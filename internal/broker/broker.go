// Package broker wraps RabbitMQ with the topology and delivery guarantees a
// service actually needs: a topic exchange, one work queue per consumer,
// publisher confirms, bounded redelivery with backoff, and a dead-letter queue.
package broker

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

// routing keys / topology names
const (
	QueueTaskEvents  = "task.events"
	QueueTaskDLQ     = "task.events.dlq"
	RoutingTaskAny   = "task.*"
	headerRetryCount = "x-retry-count"
)

// Broker owns the AMQP connection and channel used for publishing, and spawns
// dedicated channels for each consumer.
type Broker struct {
	conn     *amqp.Connection
	exchange string
	prefetch int

	mu        sync.Mutex
	pubCh     *amqp.Channel
	consumers []*Consumer
	closed    bool

	notifyClose chan *amqp.Error
}

// Options configures a Broker.
type Options struct {
	URL      string
	Exchange string
	Prefetch int
}

// New dials RabbitMQ, declares the topology and opens a confirm-mode channel.
func New(ctx context.Context, opts Options) (*Broker, error) {
	conn, err := dial(ctx, opts.URL)
	if err != nil {
		return nil, err
	}

	b := &Broker{
		conn:        conn,
		exchange:    opts.Exchange,
		prefetch:    opts.Prefetch,
		notifyClose: conn.NotifyClose(make(chan *amqp.Error, 1)),
	}

	if err := b.declareTopologyOnAll(); err != nil {
		_ = conn.Close()
		return nil, err
	}

	pubCh, err := conn.Channel()
	if err != nil {
		_ = conn.Close()
		return nil, fmt.Errorf("broker: open publish channel: %w", err)
	}
	if err := pubCh.Confirm(false); err != nil {
		_ = conn.Close()
		return nil, fmt.Errorf("broker: enable publisher confirms: %w", err)
	}
	b.pubCh = pubCh

	return b, nil
}

// dial retries with backoff so a service can start alongside a broker that is
// still booting (very common under docker-compose / k8s).
func dial(ctx context.Context, url string) (*amqp.Connection, error) {
	const maxAttempts = 10
	backoff := 500 * time.Millisecond

	var lastErr error
	for attempt := 1; attempt <= maxAttempts; attempt++ {
		conn, err := amqp.Dial(url)
		if err == nil {
			return conn, nil
		}
		lastErr = err

		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(backoff):
		}
		if backoff < 8*time.Second {
			backoff *= 2
		}
	}
	return nil, fmt.Errorf("broker: dial after %d attempts: %w", maxAttempts, lastErr)
}

// declareTopologyOnAll declares the exchange, the DLX, the DLQ and the main
// work queue. Idempotent, so it is safe to call on every replica.
func (b *Broker) declareTopologyOnAll() error {
	ch, err := b.conn.Channel()
	if err != nil {
		return fmt.Errorf("broker: topology channel: %w", err)
	}
	defer ch.Close()
	return declareTopology(ch, b.exchange)
}

func declareTopology(ch *amqp.Channel, exchange string) error {
	if err := ch.ExchangeDeclare(
		exchange, "topic", true, false, false, false, nil,
	); err != nil {
		return fmt.Errorf("broker: declare exchange %s: %w", exchange, err)
	}

	// Dead-letter exchange/queue wiring.
	if err := ch.ExchangeDeclare(
		exchange+".dlx", "fanout", true, false, false, false, nil,
	); err != nil {
		return fmt.Errorf("broker: declare dlx: %w", err)
	}
	if _, err := ch.QueueDeclare(
		QueueTaskDLQ, true, false, false, false, nil,
	); err != nil {
		return fmt.Errorf("broker: declare dlq: %w", err)
	}
	if err := ch.QueueBind(QueueTaskDLQ, "", exchange+".dlx", false, nil); err != nil {
		return fmt.Errorf("broker: bind dlq: %w", err)
	}

	// Main work queue, routing anything that fails to the DLX.
	if _, err := ch.QueueDeclare(
		QueueTaskEvents, true, false, false, false,
		amqp.Table{"x-dead-letter-exchange": exchange + ".dlx"},
	); err != nil {
		return fmt.Errorf("broker: declare queue %s: %w", QueueTaskEvents, err)
	}
	if err := ch.QueueBind(QueueTaskEvents, RoutingTaskAny, exchange, false, nil); err != nil {
		return fmt.Errorf("broker: bind queue %s: %w", QueueTaskEvents, err)
	}
	return nil
}

// Publish sends body to the exchange with the given routing key and waits for
// the broker's confirm, returning an error if the message was nacked.
func (b *Broker) Publish(ctx context.Context, routingKey string, body []byte) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	if b.closed {
		return errors.New("broker: closed")
	}

	conf, err := b.pubCh.PublishWithDeferredConfirmWithContext(ctx, b.exchange, routingKey, false, false, amqp.Publishing{
		ContentType:  "application/json",
		DeliveryMode: amqp.Persistent,
		Timestamp:    time.Now().UTC(),
		Body:         body,
	})
	if err != nil {
		return fmt.Errorf("broker: publish: %w", err)
	}

	ack, err := conf.WaitContext(ctx)
	if err != nil {
		return fmt.Errorf("broker: await confirm: %w", err)
	}
	if !ack {
		return errors.New("broker: message nacked by broker")
	}
	return nil
}

// Close drains consumers then tears down the connection.
func (b *Broker) Close() error {
	b.mu.Lock()
	if b.closed {
		b.mu.Unlock()
		return nil
	}
	b.closed = true
	consumers := b.consumers
	b.mu.Unlock()

	for _, c := range consumers {
		c.Stop()
	}
	if b.pubCh != nil {
		_ = b.pubCh.Close()
	}
	return b.conn.Close()
}

// Done reports when the underlying connection drops, so main can decide to exit
// and let the orchestrator restart the pod.
func (b *Broker) Done() <-chan *amqp.Error { return b.notifyClose }
