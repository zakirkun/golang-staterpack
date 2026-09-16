// Package service holds the application's use cases. It depends on interfaces
// (repository, publisher, cache) rather than concrete infra.
package service

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"

	"github.com/example/golang-staterpack/internal/event"
	"github.com/example/golang-staterpack/internal/model"
	"github.com/example/golang-staterpack/internal/repository"
)

const taskCacheKeyPrefix = "task:"

// ErrInvalidInput is returned for domain-level validation failures.
var ErrInvalidInput = errors.New("service: invalid input")

// Publisher is the subset of the broker the service needs. Declared locally so
// the service does not import the broker package.
type Publisher interface {
	Publish(ctx context.Context, routingKey string, body []byte) error
}

// TaskService implements the task use cases.
type TaskService struct {
	repo  repository.TaskRepository
	pub   Publisher
	cache *redis.Client
}

// NewTaskService wires the dependencies.
func NewTaskService(repo repository.TaskRepository, pub Publisher, cache *redis.Client) *TaskService {
	return &TaskService{repo: repo, pub: pub, cache: cache}
}

// CreateTaskInput is the validated request body for creating a task.
type CreateTaskInput struct {
	Title       string `json:"title"`
	Description string `json:"description"`
}

func (in CreateTaskInput) validate() error {
	if len(in.Title) == 0 {
		return fmt.Errorf("%w: title is required", ErrInvalidInput)
	}
	if len(in.Title) > 255 {
		return fmt.Errorf("%w: title exceeds 255 characters", ErrInvalidInput)
	}
	if len(in.Description) > 20000 {
		return fmt.Errorf("%w: description exceeds 20000 characters", ErrInvalidInput)
	}
	return nil
}

// Create persists a task and publishes task.created for async summarisation.
func (s *TaskService) Create(ctx context.Context, in CreateTaskInput) (*model.Task, error) {
	if err := in.validate(); err != nil {
		return nil, err
	}

	task := &model.Task{
		Title:       in.Title,
		Description: in.Description,
		Status:      model.TaskStatusPending,
	}

	if err := s.repo.Create(ctx, task); err != nil {
		return nil, err
	}

	// Publishing after a successful commit. If the publish fails the task still
	// exists, so we log and let a reconciliation sweep (or the caller's retry)
	// pick it up rather than failing the whole request.
	body, err := event.New(event.RoutingTaskCreated, event.TaskCreatedPayload{
		TaskID:      task.ID,
		Title:       task.Title,
		Description: task.Description,
	})
	if err != nil {
		slog.Error("marshal task.created payload", "task_id", task.ID, "err", err)
		return task, nil
	}

	if err := s.pub.Publish(ctx, event.RoutingTaskCreated, body); err != nil {
		slog.Error("publish task.created", "task_id", task.ID, "err", err)
	}

	return task, nil
}

// Get returns a task, serving from Redis when warm.
func (s *TaskService) Get(ctx context.Context, id uuid.UUID) (*model.Task, error) {
	key := taskCacheKeyPrefix + id.String()

	if raw, err := s.cache.Get(ctx, key).Bytes(); err == nil {
		var cached model.Task
		if err := json.Unmarshal(raw, &cached); err == nil {
			return &cached, nil
		}
	} else if !errors.Is(err, redis.Nil) {
		slog.Warn("cache get failed", "key", key, "err", err)
	}

	task, err := s.repo.GetByID(ctx, id)
	if err != nil {
		return nil, err
	}

	s.cacheSet(ctx, key, task)
	return task, nil
}

// List returns a page of tasks plus the total count.
func (s *TaskService) List(ctx context.Context, limit, offset int) ([]model.Task, int64, error) {
	if limit <= 0 || limit > 100 {
		limit = 20
	}
	if offset < 0 {
		offset = 0
	}
	return s.repo.List(ctx, limit, offset)
}

// Delete removes a task and invalidates its cache entry.
func (s *TaskService) Delete(ctx context.Context, id uuid.UUID) error {
	if err := s.repo.Delete(ctx, id); err != nil {
		return err
	}
	if err := s.cache.Del(ctx, taskCacheKeyPrefix+id.String()).Err(); err != nil {
		slog.Warn("cache invalidate failed", "task_id", id, "err", err)
	}
	return nil
}

// ApplySummary records the LLM summary produced by the worker and refreshes the
// cache so readers see the completed state.
func (s *TaskService) ApplySummary(ctx context.Context, id uuid.UUID, summary string) error {
	if err := s.repo.UpdateStatus(ctx, id, model.TaskStatusCompleted, summary); err != nil {
		return err
	}

	if task, err := s.repo.GetByID(ctx, id); err == nil {
		s.cacheSet(ctx, taskCacheKeyPrefix+id.String(), task)
	}
	return nil
}

// MarkFailed flags a task whose processing failed terminally.
func (s *TaskService) MarkFailed(ctx context.Context, id uuid.UUID) error {
	return s.repo.UpdateStatus(ctx, id, model.TaskStatusFailed, "")
}

func (s *TaskService) cacheSet(ctx context.Context, key string, task *model.Task) {
	raw, err := json.Marshal(task)
	if err != nil {
		return
	}
	if err := s.cache.Set(ctx, key, raw, 5*time.Minute).Err(); err != nil {
		slog.Warn("cache set failed", "key", key, "err", err)
	}
}
