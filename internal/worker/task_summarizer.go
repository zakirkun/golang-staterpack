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
type TaskSummarizer struct {
	svc *service.TaskService
	llm llm.Client
}

// NewTaskSummarizer builds the worker.
func NewTaskSummarizer(svc *service.TaskService, llmClient llm.Client) *TaskSummarizer {
	return &TaskSummarizer{svc: svc, llm: llmClient}
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

	var payload event.TaskCreatedPayload
	if err := broker.DecodeJSON(env.Payload, &payload); err != nil {
		slog.Error("summarizer: undecodable payload, dropping", "err", err)
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
		return fmt.Errorf("summarize task %s: %w", payload.TaskID, err)
	}

	if err := w.svc.ApplySummary(ctx, payload.TaskID, summary); err != nil {
		if errors.Is(err, repository.ErrNotFound) {
			slog.Warn("summarizer: task vanished before summary applied", "task_id", payload.TaskID)
			return nil
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
