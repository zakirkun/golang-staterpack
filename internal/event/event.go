// Package event defines the messages exchanged over the broker. Keeping the
// contract in one place lets producers and consumers agree without importing
// each other.
package event

import (
	"encoding/json"
	"time"

	"github.com/google/uuid"
)

// Routing keys. `task.*` is the exchange topic; producers pick a specific key.
const (
	RoutingTaskCreated    = "task.created"
	RoutingTaskSummarized = "task.summarized"
)

// Envelope wraps every payload with routing metadata.
type Envelope struct {
	ID        string          `json:"id"`
	Type      string          `json:"type"`
	Timestamp time.Time       `json:"timestamp"`
	Payload   json.RawMessage `json:"payload"`
}

// New builds an envelope, marshalling the payload.
func New(eventType string, payload any) ([]byte, error) {
	raw, err := json.Marshal(payload)
	if err != nil {
		return nil, err
	}
	env := Envelope{
		ID:        uuid.NewString(),
		Type:      eventType,
		Timestamp: time.Now().UTC(),
		Payload:   raw,
	}
	return json.Marshal(env)
}

// TaskCreatedPayload is emitted when a task is persisted, and consumed by the
// summariser worker.
type TaskCreatedPayload struct {
	TaskID      uuid.UUID `json:"task_id"`
	Title       string    `json:"title"`
	Description string    `json:"description"`
}

// TaskSummarizedPayload is emitted when the LLM finishes a summary.
type TaskSummarizedPayload struct {
	TaskID  uuid.UUID `json:"task_id"`
	Summary string    `json:"summary"`
}
