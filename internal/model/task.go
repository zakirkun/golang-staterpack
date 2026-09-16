// Package model holds the GORM-mapped domain entities.
package model

import (
	"time"

	"github.com/google/uuid"
	"gorm.io/gorm"
)

// Task is the example bounded context: a unit of work that may optionally be
// summarised by the LLM asynchronously through the message broker.
type Task struct {
	ID          uuid.UUID  `gorm:"type:uuid;primaryKey"                  json:"id"`
	Title       string     `gorm:"size:255;not null;index"               json:"title"`
	Description string     `gorm:"type:text"                             json:"description"`
	Status      TaskStatus `gorm:"size:32;not null;default:pending;index" json:"status"`
	Summary     string     `gorm:"type:text"                             json:"summary,omitempty"`

	CreatedAt time.Time      `json:"created_at"`
	UpdatedAt time.Time      `json:"updated_at"`
	DeletedAt gorm.DeletedAt `gorm:"index" json:"-"`
}

// TaskStatus enumerates the lifecycle states of a Task.
type TaskStatus string

const (
	TaskStatusPending    TaskStatus = "pending"
	TaskStatusProcessing TaskStatus = "processing"
	TaskStatusCompleted  TaskStatus = "completed"
	TaskStatusFailed     TaskStatus = "failed"
)

// Valid reports whether s is a known status.
func (s TaskStatus) Valid() bool {
	switch s {
	case TaskStatusPending, TaskStatusProcessing, TaskStatusCompleted, TaskStatusFailed:
		return true
	default:
		return false
	}
}

// BeforeCreate assigns a UUID when the caller did not supply one.
func (t *Task) BeforeCreate(*gorm.DB) error {
	if t.ID == uuid.Nil {
		t.ID = uuid.New()
	}
	if t.Status == "" {
		t.Status = TaskStatusPending
	}
	return nil
}
