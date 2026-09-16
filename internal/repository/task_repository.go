// Package repository contains the persistence layer. Repositories take a
// *gorm.DB so they compose inside transactions and are trivial to fake.
package repository

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"gorm.io/gorm"

	"github.com/zakirkun/golang-staterpack/internal/model"
)

// ErrNotFound is returned when a row does not exist, mapping the storage
// detail of gorm.ErrRecordNotFound onto a domain-level sentinel.
var ErrNotFound = errors.New("repository: resource not found")

// TaskRepository is the contract the service layer depends on.
type TaskRepository interface {
	Create(ctx context.Context, t *model.Task) error
	GetByID(ctx context.Context, id uuid.UUID) (*model.Task, error)
	List(ctx context.Context, limit, offset int) ([]model.Task, int64, error)
	Update(ctx context.Context, t *model.Task) error
	UpdateStatus(ctx context.Context, id uuid.UUID, status model.TaskStatus, summary string) error
	Delete(ctx context.Context, id uuid.UUID) error

	// MarkProcessing transitions a task to `processing` only if it is currently
	// `pending`. The conditional update is what makes it safe when several
	// workers pick up duplicate deliveries of the same task: exactly one wins.
	// Returns true when this caller made the transition.
	MarkProcessing(ctx context.Context, id uuid.UUID) (bool, error)

	// FindStale returns tasks that look abandoned: `pending` rows older than
	// pendingBefore, or `processing` rows last touched before processingBefore.
	FindStale(ctx context.Context, pendingBefore, processingBefore time.Time, limit int) ([]model.Task, error)
}

type taskRepository struct {
	db *gorm.DB
}

// NewTaskRepository returns a GORM-backed TaskRepository.
func NewTaskRepository(db *gorm.DB) TaskRepository {
	return &taskRepository{db: db}
}

func (r *taskRepository) Create(ctx context.Context, t *model.Task) error {
	if err := r.db.WithContext(ctx).Create(t).Error; err != nil {
		return fmt.Errorf("create task: %w", err)
	}
	return nil
}

func (r *taskRepository) GetByID(ctx context.Context, id uuid.UUID) (*model.Task, error) {
	var t model.Task
	err := r.db.WithContext(ctx).First(&t, "id = ?", id).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("get task: %w", err)
	}
	return &t, nil
}

func (r *taskRepository) List(ctx context.Context, limit, offset int) ([]model.Task, int64, error) {
	var (
		tasks []model.Task
		total int64
	)

	q := r.db.WithContext(ctx).Model(&model.Task{})
	if err := q.Count(&total).Error; err != nil {
		return nil, 0, fmt.Errorf("count tasks: %w", err)
	}

	if err := q.Order("created_at DESC").
		Limit(limit).
		Offset(offset).
		Find(&tasks).Error; err != nil {
		return nil, 0, fmt.Errorf("list tasks: %w", err)
	}
	return tasks, total, nil
}

func (r *taskRepository) Update(ctx context.Context, t *model.Task) error {
	if err := r.db.WithContext(ctx).Save(t).Error; err != nil {
		return fmt.Errorf("update task: %w", err)
	}
	return nil
}

func (r *taskRepository) UpdateStatus(ctx context.Context, id uuid.UUID, status model.TaskStatus, summary string) error {
	updates := map[string]any{"status": status}
	if summary != "" {
		updates["summary"] = summary
	}

	res := r.db.WithContext(ctx).
		Model(&model.Task{}).
		Where("id = ?", id).
		Updates(updates)
	if res.Error != nil {
		return fmt.Errorf("update task status: %w", res.Error)
	}
	if res.RowsAffected == 0 {
		return ErrNotFound
	}
	return nil
}

func (r *taskRepository) Delete(ctx context.Context, id uuid.UUID) error {
	res := r.db.WithContext(ctx).Delete(&model.Task{}, "id = ?", id)
	if res.Error != nil {
		return fmt.Errorf("delete task: %w", res.Error)
	}
	if res.RowsAffected == 0 {
		return ErrNotFound
	}
	return nil
}

func (r *taskRepository) MarkProcessing(ctx context.Context, id uuid.UUID) (bool, error) {
	res := r.db.WithContext(ctx).
		Model(&model.Task{}).
		Where("id = ? AND status = ?", id, model.TaskStatusPending).
		Update("status", model.TaskStatusProcessing)
	if res.Error != nil {
		return false, fmt.Errorf("mark processing: %w", res.Error)
	}
	return res.RowsAffected == 1, nil
}

func (r *taskRepository) FindStale(ctx context.Context, pendingBefore, processingBefore time.Time, limit int) ([]model.Task, error) {
	if limit <= 0 {
		limit = 100
	}

	var tasks []model.Task
	// Two disjoint conditions: never-picked-up work, and work whose worker
	// died mid-flight. Both are safe to republish; the consumer's conditional
	// MarkProcessing prevents concurrent duplicates.
	err := r.db.WithContext(ctx).
		Where(
			r.db.Where("status = ? AND created_at < ?", model.TaskStatusPending, pendingBefore).
				Or("status = ? AND updated_at < ?", model.TaskStatusProcessing, processingBefore),
		).
		Order("created_at ASC").
		Limit(limit).
		Find(&tasks).Error
	if err != nil {
		return nil, fmt.Errorf("find stale tasks: %w", err)
	}
	return tasks, nil
}

// AutoMigrate applies the schema for all registered models.
func AutoMigrate(db *gorm.DB) error {
	if err := db.AutoMigrate(&model.Task{}); err != nil {
		return fmt.Errorf("auto-migrate: %w", err)
	}
	return nil
}
