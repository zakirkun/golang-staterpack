// Package repository contains the persistence layer. Repositories take a
// *gorm.DB so they compose inside transactions and are trivial to fake.
package repository

import (
	"context"
	"errors"
	"fmt"

	"github.com/google/uuid"
	"gorm.io/gorm"

	"github.com/example/golang-staterpack/internal/model"
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

// AutoMigrate applies the schema for all registered models.
func AutoMigrate(db *gorm.DB) error {
	if err := db.AutoMigrate(&model.Task{}); err != nil {
		return fmt.Errorf("auto-migrate: %w", err)
	}
	return nil
}
