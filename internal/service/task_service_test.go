package service

import (
	"context"
	"errors"
	"sync"
	"testing"

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"

	"github.com/example/golang-staterpack/internal/model"
	"github.com/example/golang-staterpack/internal/repository"
)

// --- fakes -------------------------------------------------------------------

type fakeRepo struct {
	mu    sync.Mutex
	store map[uuid.UUID]*model.Task
}

func newFakeRepo() *fakeRepo {
	return &fakeRepo{store: map[uuid.UUID]*model.Task{}}
}

func (f *fakeRepo) Create(_ context.Context, t *model.Task) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if t.ID == uuid.Nil {
		t.ID = uuid.New()
	}
	cp := *t
	f.store[t.ID] = &cp
	return nil
}

func (f *fakeRepo) GetByID(_ context.Context, id uuid.UUID) (*model.Task, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	t, ok := f.store[id]
	if !ok {
		return nil, repository.ErrNotFound
	}
	cp := *t
	return &cp, nil
}

func (f *fakeRepo) List(_ context.Context, limit, offset int) ([]model.Task, int64, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]model.Task, 0, len(f.store))
	for _, t := range f.store {
		out = append(out, *t)
	}
	total := int64(len(out))
	if offset > len(out) {
		offset = len(out)
	}
	out = out[offset:]
	if limit < len(out) {
		out = out[:limit]
	}
	return out, total, nil
}

func (f *fakeRepo) Update(_ context.Context, t *model.Task) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	cp := *t
	f.store[t.ID] = &cp
	return nil
}

func (f *fakeRepo) UpdateStatus(_ context.Context, id uuid.UUID, status model.TaskStatus, summary string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	t, ok := f.store[id]
	if !ok {
		return repository.ErrNotFound
	}
	t.Status = status
	if summary != "" {
		t.Summary = summary
	}
	return nil
}

func (f *fakeRepo) Delete(_ context.Context, id uuid.UUID) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if _, ok := f.store[id]; !ok {
		return repository.ErrNotFound
	}
	delete(f.store, id)
	return nil
}

type fakePublisher struct {
	mu     sync.Mutex
	events []string
	fail   bool
}

func (p *fakePublisher) Publish(_ context.Context, routingKey string, _ []byte) error {
	if p.fail {
		return errors.New("broker down")
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	p.events = append(p.events, routingKey)
	return nil
}

func (p *fakePublisher) published() []string {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]string(nil), p.events...)
}

// newTestService wires the service against fakes and a Redis client pointed at
// an unreachable address. Every cache call therefore fails; the service must
// degrade to the repository rather than propagate the error, which is exactly
// what these tests exercise.
func newTestService(repo *fakeRepo, pub *fakePublisher) *TaskService {
	rdb := redis.NewClient(&redis.Options{Addr: "127.0.0.1:0", MaxRetries: -1})
	return NewTaskService(repo, pub, rdb)
}

// --- tests -------------------------------------------------------------------

func TestCreatePublishesEvent(t *testing.T) {
	repo, pub := newFakeRepo(), &fakePublisher{}
	svc := newTestService(repo, pub)

	task, err := svc.Create(context.Background(), CreateTaskInput{
		Title:       "Write docs",
		Description: "Document the API",
	})
	if err != nil {
		t.Fatalf("Create() error = %v", err)
	}
	if task.ID == uuid.Nil {
		t.Error("Create() returned a nil task ID")
	}
	if task.Status != model.TaskStatusPending {
		t.Errorf("Status = %q, want pending", task.Status)
	}

	events := pub.published()
	if len(events) != 1 || events[0] != "task.created" {
		t.Errorf("published = %v, want [task.created]", events)
	}
}

func TestCreateValidatesInput(t *testing.T) {
	svc := newTestService(newFakeRepo(), &fakePublisher{})

	cases := map[string]CreateTaskInput{
		"empty title":      {Title: ""},
		"title too long":   {Title: string(make([]byte, 256))},
		"description huge": {Title: "ok", Description: string(make([]byte, 20001))},
	}

	for name, in := range cases {
		t.Run(name, func(t *testing.T) {
			if _, err := svc.Create(context.Background(), in); !errors.Is(err, ErrInvalidInput) {
				t.Errorf("Create() error = %v, want ErrInvalidInput", err)
			}
		})
	}
}

func TestCreateSucceedsWhenPublishFails(t *testing.T) {
	repo, pub := newFakeRepo(), &fakePublisher{fail: true}
	svc := newTestService(repo, pub)

	task, err := svc.Create(context.Background(), CreateTaskInput{Title: "still persisted"})
	if err != nil {
		t.Fatalf("Create() error = %v, want nil (publish failure must not fail the request)", err)
	}

	if _, err := repo.GetByID(context.Background(), task.ID); err != nil {
		t.Errorf("task was not persisted despite publish failure: %v", err)
	}
}

func TestGetFallsBackToRepoWhenCacheUnavailable(t *testing.T) {
	repo, pub := newFakeRepo(), &fakePublisher{}
	svc := newTestService(repo, pub)

	created, err := svc.Create(context.Background(), CreateTaskInput{Title: "cached?"})
	if err != nil {
		t.Fatalf("Create() error = %v", err)
	}

	got, err := svc.Get(context.Background(), created.ID)
	if err != nil {
		t.Fatalf("Get() error = %v, want nil with a dead cache", err)
	}
	if got.ID != created.ID {
		t.Errorf("Get() ID = %v, want %v", got.ID, created.ID)
	}
}

func TestGetMissingReturnsNotFound(t *testing.T) {
	svc := newTestService(newFakeRepo(), &fakePublisher{})

	if _, err := svc.Get(context.Background(), uuid.New()); !errors.Is(err, repository.ErrNotFound) {
		t.Errorf("Get() error = %v, want ErrNotFound", err)
	}
}

func TestApplySummaryCompletesTask(t *testing.T) {
	repo, pub := newFakeRepo(), &fakePublisher{}
	svc := newTestService(repo, pub)

	created, _ := svc.Create(context.Background(), CreateTaskInput{Title: "summarise me"})

	if err := svc.ApplySummary(context.Background(), created.ID, "a short summary"); err != nil {
		t.Fatalf("ApplySummary() error = %v", err)
	}

	got, err := repo.GetByID(context.Background(), created.ID)
	if err != nil {
		t.Fatalf("GetByID() error = %v", err)
	}
	if got.Status != model.TaskStatusCompleted {
		t.Errorf("Status = %q, want completed", got.Status)
	}
	if got.Summary != "a short summary" {
		t.Errorf("Summary = %q, want %q", got.Summary, "a short summary")
	}
}

func TestDeleteMissingReturnsNotFound(t *testing.T) {
	svc := newTestService(newFakeRepo(), &fakePublisher{})

	if err := svc.Delete(context.Background(), uuid.New()); !errors.Is(err, repository.ErrNotFound) {
		t.Errorf("Delete() error = %v, want ErrNotFound", err)
	}
}

func TestListClampsPaging(t *testing.T) {
	repo, pub := newFakeRepo(), &fakePublisher{}
	svc := newTestService(repo, pub)

	for i := 0; i < 5; i++ {
		if _, err := svc.Create(context.Background(), CreateTaskInput{Title: "t"}); err != nil {
			t.Fatalf("seed Create() error = %v", err)
		}
	}

	// limit 0 must fall back to the default rather than returning nothing.
	tasks, total, err := svc.List(context.Background(), 0, -10)
	if err != nil {
		t.Fatalf("List() error = %v", err)
	}
	if total != 5 {
		t.Errorf("total = %d, want 5", total)
	}
	if len(tasks) == 0 {
		t.Error("List() returned no tasks for a clamped limit")
	}
}
