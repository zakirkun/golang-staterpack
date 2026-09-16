package broker

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strconv"
	"sync"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

// MaxRetries is the number of in-queue redeliveries before a message is routed
// to the dead-letter queue.
const MaxRetries = 3

// Handler processes a raw delivery. Returning an error triggers the retry /
// dead-letter path; returning nil acks the message.
type Handler func(ctx context.Context, body []byte) error

// Consumer reads from a single queue on its own channel.
type Consumer struct {
	conn     *amqp.Connection
	queue    string
	handler  Handler
	prefetch int

	mu      sync.Mutex
	stop    chan struct{}
	stopped bool
	wg      sync.WaitGroup
}

// ConsumerOptions configures a Consumer.
type ConsumerOptions struct {
	Queue    string
	Prefetch int
	Handler  Handler
}

// NewConsumer opens a channel and starts consuming in the background.
func (b *Broker) NewConsumer(opts ConsumerOptions) (*Consumer, error) {
	if opts.Handler == nil {
		return nil, errors.New("broker: consumer handler is nil")
	}
	prefetch := opts.Prefetch
	if prefetch <= 0 {
		prefetch = b.prefetch
	}

	c := &Consumer{
		conn:     b.conn,
		queue:    opts.Queue,
		handler:  opts.Handler,
		prefetch: prefetch,
		stop:     make(chan struct{}),
	}

	b.mu.Lock()
	b.consumers = append(b.consumers, c)
	b.mu.Unlock()

	c.wg.Add(1)
	go c.run(b.exchange)

	return c, nil
}

func (c *Consumer) run(exchange string) {
	defer c.wg.Done()

	const maxAttempts = 10
	backoff := 500 * time.Millisecond

	for attempt := 1; attempt <= maxAttempts; attempt++ {
		if err := c.consumeOnce(exchange); err != nil {
			slog.Error("consumer channel failed", "queue", c.queue, "attempt", attempt, "err", err)
		} else {
			return // clean shutdown
		}

		select {
		case <-c.stop:
			return
		case <-time.After(backoff):
		}
		if backoff < 8*time.Second {
			backoff *= 2
		}
	}
	slog.Error("consumer giving up after repeated failures", "queue", c.queue)
}

func (c *Consumer) consumeOnce(exchange string) error {
	ch, err := c.conn.Channel()
	if err != nil {
		return fmt.Errorf("open channel: %w", err)
	}
	defer ch.Close()

	if err := declareTopology(ch, exchange); err != nil {
		return err
	}
	if err := ch.Qos(c.prefetch, 0, false); err != nil {
		return fmt.Errorf("set qos: %w", err)
	}

	deliveries, err := ch.Consume(
		c.queue, "" /* server-named consumer */, false, /* autoAck */
		false, false, false, nil,
	)
	if err != nil {
		return fmt.Errorf("consume: %w", err)
	}

	slog.Info("consumer started", "queue", c.queue, "prefetch", c.prefetch)

	for {
		select {
		case <-c.stop:
			return nil
		case d, ok := <-deliveries:
			if !ok {
				return errors.New("delivery channel closed")
			}
			c.handle(ch, d)
		}
	}
}

func (c *Consumer) handle(ch *amqp.Channel, d amqp.Delivery) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	if err := c.handler(ctx, d.Body); err != nil {
		c.retryOrDeadLetter(ch, d, err)
		return
	}
	if err := d.Ack(false); err != nil {
		slog.Error("ack failed", "queue", c.queue, "err", err)
	}
}

// retryOrDeadLetter republishes with an incremented retry counter, or sends to
// the DLQ once MaxRetries is exhausted. Either way the original is acked so it
// does not loop forever.
func (c *Consumer) retryOrDeadLetter(ch *amqp.Channel, d amqp.Delivery, cause error) {
	retries := retryCount(d.Headers, headerRetryCount)

	if retries >= MaxRetries {
		slog.Error("message exhausted retries, dead-lettering",
			"queue", c.queue, "routing_key", d.RoutingKey, "retries", retries, "err", cause)
		// Reject without requeue -> routed to the configured DLX.
		if err := d.Nack(false, false); err != nil {
			slog.Error("nack failed", "err", err)
		}
		return
	}

	headers := amqp.Table{}
	for k, v := range d.Headers {
		headers[k] = v
	}
	headers[headerRetryCount] = int32(retries + 1)

	delay := time.Duration(1<<uint(retries)) * time.Second // 1s, 2s, 4s
	slog.Warn("retrying message", "queue", c.queue, "retry", retries+1, "delay", delay, "err", cause)

	pubCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Requeue via the default (already-declared) exchange so the same routing
	// key reaches this queue again.
	if err := ch.PublishWithContext(pubCtx, "", c.queue, false, false, amqp.Publishing{
		ContentType:  d.ContentType,
		DeliveryMode: amqp.Persistent,
		Headers:      headers,
		Timestamp:    time.Now().UTC(),
		Body:         d.Body,
	}); err != nil {
		slog.Error("republish for retry failed, dead-lettering", "err", err)
		_ = d.Nack(false, false)
		return
	}
	if err := d.Ack(false); err != nil {
		slog.Error("ack after retry failed", "err", err)
	}
}

func retryCount(h amqp.Table, key string) int {
	v, ok := h[key]
	if !ok {
		return 0
	}
	switch n := v.(type) {
	case int32:
		return int(n)
	case int64:
		return int(n)
	case int:
		return n
	case string:
		i, err := strconv.Atoi(n)
		if err != nil {
			return 0
		}
		return i
	default:
		return 0
	}
}

// Stop signals the consumer to exit and waits for it to drain.
func (c *Consumer) Stop() {
	c.mu.Lock()
	if c.stopped {
		c.mu.Unlock()
		return
	}
	c.stopped = true
	close(c.stop)
	c.mu.Unlock()

	c.wg.Wait()
}

// DecodeJSON is a small helper for handlers that expect a JSON payload.
func DecodeJSON(body []byte, dst any) error {
	if err := json.Unmarshal(body, dst); err != nil {
		return fmt.Errorf("decode json: %w", err)
	}
	return nil
}
