package service

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"

	"github.com/zakirkun/golang-staterpack/internal/model"
	"github.com/zakirkun/golang-staterpack/internal/repository"
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

// MarkProcessing mirrors the real conditional update: only a `pending` row
// transitions, and exactly one caller wins.
func (f *fakeRepo) MarkProcessing(_ context.Context, id uuid.UUID) (bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	t, ok := f.store[id]
	if !ok {
		return false, repository.ErrNotFound
	}
	if t.Status != model.TaskStatusPending {
		return false, nil
	}
	t.Status = model.TaskStatusProcessing
	return true, nil
}

func (f *fakeRepo) FindStale(_ context.Context, pendingBefore, processingBefore time.Time, limit int) ([]model.Task, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	var out []model.Task
	for _, t := range f.store {
		switch t.Status {
		case model.TaskStatusPending:
			if t.CreatedAt.Before(pendingBefore) {
				out = append(out, *t)
			}
		case model.TaskStatusProcessing:
			if t.UpdatedAt.Before(processingBefore) {
				out = append(out, *t)
			}
		}
		if limit > 0 && len(out) >= limit {
			break
		}
	}
	return out, nil
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

func TestBeginProcessingClaimsOnlyOnce(t *testing.T) {
	repo, pub := newFakeRepo(), &fakePublisher{}
	svc := newTestService(repo, pub)

	created, err := svc.Create(context.Background(), CreateTaskInput{Title: "claim me"})
	if err != nil {
		t.Fatalf("Create() error = %v", err)
	}

	first, err := svc.BeginProcessing(context.Background(), created.ID)
	if err != nil {
		t.Fatalf("BeginProcessing() error = %v", err)
	}
	if !first {
		t.Error("first BeginProcessing() = false, want true")
	}

	// A duplicate delivery must not win the claim.
	second, err := svc.BeginProcessing(context.Background(), created.ID)
	if err != nil {
		t.Fatalf("second BeginProcessing() error = %v", err)
	}
	if second {
		t.Error("second BeginProcessing() = true, want false (already claimed)")
	}
}

func TestBeginProcessingRejectsCompletedTask(t *testing.T) {
	repo, pub := newFakeRepo(), &fakePublisher{}
	svc := newTestService(repo, pub)

	created, _ := svc.Create(context.Background(), CreateTaskInput{Title: "done"})
	if err := svc.ApplySummary(context.Background(), created.ID, "summary"); err != nil {
		t.Fatalf("ApplySummary() error = %v", err)
	}

	// Completed work must not be re-claimed by a late duplicate.
	claimed, err := svc.BeginProcessing(context.Background(), created.ID)
	if err != nil {
		t.Fatalf("BeginProcessing() error = %v", err)
	}
	if claimed {
		t.Error("BeginProcessing() on a completed task = true, want false")
	}
}

func TestMarkPendingResetsClaimedTask(t *testing.T) {
	repo, pub := newFakeRepo(), &fakePublisher{}
	svc := newTestService(repo, pub)

	created, _ := svc.Create(context.Background(), CreateTaskInput{Title: "reset me"})
	if _, err := svc.BeginProcessing(context.Background(), created.ID); err != nil {
		t.Fatalf("BeginProcessing() error = %v", err)
	}

	if err := svc.MarkPending(context.Background(), created.ID); err != nil {
		t.Fatalf("MarkPending() error = %v", err)
	}

	got, err := repo.GetByID(context.Background(), created.ID)
	if err != nil {
		t.Fatalf("GetByID() error = %v", err)
	}
	if got.Status != model.TaskStatusPending {
		t.Errorf("Status = %q, want pending", got.Status)
	}

	// Having been reset, it must be claimable again.
	claimed, err := svc.BeginProcessing(context.Background(), created.ID)
	if err != nil {
		t.Fatalf("re-claim BeginProcessing() error = %v", err)
	}
	if !claimed {
		t.Error("re-claim after MarkPending() = false, want true")
	}
}

func TestRepublishCreatedResetsProcessingTask(t *testing.T) {
	repo, pub := newFakeRepo(), &fakePublisher{}
	svc := newTestService(repo, pub)

	created, _ := svc.Create(context.Background(), CreateTaskInput{Title: "stranded"})
	if _, err := svc.BeginProcessing(context.Background(), created.ID); err != nil {
		t.Fatalf("BeginProcessing() error = %v", err)
	}

	stuck, err := repo.GetByID(context.Background(), created.ID)
	if err != nil {
		t.Fatalf("GetByID() error = %v", err)
	}
	if stuck.Status != model.TaskStatusProcessing {
		t.Fatalf("precondition: Status = %q, want processing", stuck.Status)
	}

	// The reconciler hands back the row it observed, still marked `processing`.
	// Republishing must reset it first, or the redelivered event would fail the
	// pending-only claim condition and the task would stay stuck forever.
	if err := svc.RepublishCreated(context.Background(), *stuck); err != nil {
		t.Fatalf("RepublishCreated() error = %v", err)
	}

	after, err := repo.GetByID(context.Background(), created.ID)
	if err != nil {
		t.Fatalf("GetByID() error = %v", err)
	}
	if after.Status != model.TaskStatusPending {
		t.Errorf("Status = %q after republish, want pending", after.Status)
	}

	events := pub.published()
	if len(events) < 2 { // one from Create, one from the republish
		t.Errorf("published = %v, want at least 2 events", events)
	}
}

func TestFindStaleSurfacesAbandonedWork(t *testing.T) {
	repo, pub := newFakeRepo(), &fakePublisher{}
	svc := newTestService(repo, pub)

	// A task created in the past, never picked up.
	old := &model.Task{
		Title:     "old pending",
		Status:    model.TaskStatusPending,
		CreatedAt: time.Now().Add(-time.Hour),
		UpdatedAt: time.Now().Add(-time.Hour),
	}
	if err := repo.Create(context.Background(), old); err != nil {
		t.Fatalf("seed error = %v", err)
	}

	stale, err := svc.FindStale(
		context.Background(),
		time.Now().Add(-5*time.Minute),
		time.Now().Add(-10*time.Minute),
		10,
	)
	if err != nil {
		t.Fatalf("FindStale() error = %v", err)
	}
	if len(stale) != 1 || stale[0].ID != old.ID {
		t.Errorf("FindStale() = %d rows, want the one seeded task", len(stale))
	}
}
