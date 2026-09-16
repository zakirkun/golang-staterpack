// Package worker hosts the background consumers. Each worker is a thin layer
// translating a broker message into a service call.
package worker

import (
	"context"
	"errors"
	"fmt"
	"log/slog"

	"github.com/zakirkun/golang-staterpack/internal/broker"
	"github.com/zakirkun/golang-staterpack/internal/event"
	"github.com/zakirkun/golang-staterpack/internal/llm"
	"github.com/zakirkun/golang-staterpack/internal/repository"
	"github.com/zakirkun/golang-staterpack/internal/service"
)

// TaskSummarizer consumes task.created, asks the LLM for a summary and writes
// the result back through the service.
//
// Exactly-once is not achievable over RabbitMQ, so this handler is built to be
// idempotent instead: the dedupe guard drops duplicate deliveries, and the
// conditional MarkProcessing in the service means only one worker can own a
// task even if two messages race.
type TaskSummarizer struct {
	svc    *service.TaskService
	llm    llm.Client
	dedupe *broker.Dedupe
}

// NewTaskSummarizer builds the worker. dedupe may be nil to disable duplicate
// suppression.
func NewTaskSummarizer(svc *service.TaskService, llmClient llm.Client, dedupe *broker.Dedupe) *TaskSummarizer {
	return &TaskSummarizer{svc: svc, llm: llmClient, dedupe: dedupe}
}

// Handle implements broker.Handler.
func (w *TaskSummarizer) Handle(ctx context.Context, body []byte) error {
	var env event.Envelope
	if err := broker.DecodeJSON(body, &env); err != nil {
		// A malformed message will never succeed on retry: return nil so it is
		// acked and dropped rather than poisoning the queue up to the DLQ.
		slog.Error("summarizer: undecodable envelope, dropping", "err", err)
		return nil
	}

	if env.Type != event.RoutingTaskCreated {
		slog.Debug("summarizer: ignoring event type", "type", env.Type)
		return nil
	}

	// Duplicate-suppression by event ID. Note a redelivery of the *same*
	// physical message carries the same envelope ID, which is exactly the case
	// the retry path in the broker creates.
	if w.dedupe != nil && !w.dedupe.Claim(ctx, env.ID) {
		slog.Info("summarizer: duplicate event dropped", "event_id", env.ID)
		return nil
	}

	var payload event.TaskCreatedPayload
	if err := broker.DecodeJSON(env.Payload, &payload); err != nil {
		slog.Error("summarizer: undecodable payload, dropping", "err", err)
		return nil
	}

	// Claim the task. A false result means it is already completed, failed, or
	// being handled by another worker, so there is nothing to do.
	claimed, err := w.svc.BeginProcessing(ctx, payload.TaskID)
	if err != nil {
		if errors.Is(err, repository.ErrNotFound) {
			slog.Warn("summarizer: task no longer exists", "task_id", payload.TaskID)
			return nil
		}
		return fmt.Errorf("claim task %s: %w", payload.TaskID, err)
	}
	if !claimed {
		slog.Info("summarizer: task not claimable, skipping", "task_id", payload.TaskID)
		return nil
	}

	slog.Info("summarizer: processing task", "task_id", payload.TaskID)

	text := payload.Title
	if payload.Description != "" {
		text = fmt.Sprintf("%s\n\n%s", payload.Title, payload.Description)
	}

	summary, err := w.llm.Summarize(ctx, text)
	if err != nil {
		// Transient (network, rate limit): let the broker retry with backoff.
		// Release the dedupe marker so the retry is permitted to run, and push
		// the task back to `pending` so the reconciler can also rescue it if
		// the message is lost.
		if w.dedupe != nil {
			w.dedupe.Release(ctx, env.ID)
		}
		if rerr := w.svc.MarkPending(ctx, payload.TaskID); rerr != nil {
			slog.Warn("summarizer: could not reset task to pending", "task_id", payload.TaskID, "err", rerr)
		}
		return fmt.Errorf("summarize task %s: %w", payload.TaskID, err)
	}

	if err := w.svc.ApplySummary(ctx, payload.TaskID, summary); err != nil {
		if errors.Is(err, repository.ErrNotFound) {
			slog.Warn("summarizer: task vanished before summary applied", "task_id", payload.TaskID)
			return nil
		}
		if w.dedupe != nil {
			w.dedupe.Release(ctx, env.ID)
		}
		if rerr := w.svc.MarkPending(ctx, payload.TaskID); rerr != nil {
			slog.Warn("summarizer: could not reset task to pending", "task_id", payload.TaskID, "err", rerr)
		}
		return fmt.Errorf("apply summary for %s: %w", payload.TaskID, err)
	}

	slog.Info("summarizer: task completed", "task_id", payload.TaskID, "model", w.llm.Model())
	return nil
}

// Register attaches the worker to the broker's work queue.
func (w *TaskSummarizer) Register(b *broker.Broker) (*broker.Consumer, error) {
	return b.NewConsumer(broker.ConsumerOptions{
		Queue:   broker.QueueTaskEvents,
		Handler: w.Handle,
	})
}
