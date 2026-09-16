package reconciler

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/zakirkun/golang-staterpack/internal/model"
)

// --- fake ---------------------------------------------------------------------

type fakeTasks struct {
	mu sync.Mutex

	stale []model.Task
	// err, when set, is returned by FindStale.
	err error
	// republishErr is returned for the task whose ID matches the key.
	republishErr map[uuid.UUID]error

	findCalls      int
	republishedIDs []uuid.UUID
	// captured windows let tests assert the query bounds.
	gotPendingBefore    time.Time
	gotProcessingBefore time.Time
}

func (f *fakeTasks) FindStale(_ context.Context, pendingBefore, processingBefore time.Time, limit int) ([]model.Task, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.findCalls++
	f.gotPendingBefore = pendingBefore
	f.gotProcessingBefore = processingBefore
	if f.err != nil {
		return nil, f.err
	}
	if limit < len(f.stale) {
		return f.stale[:limit], nil
	}
	return f.stale, nil
}

func (f *fakeTasks) RepublishCreated(_ context.Context, task model.Task) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if err, ok := f.republishErr[task.ID]; ok {
		return err
	}
	f.republishedIDs = append(f.republishedIDs, task.ID)
	return nil
}

func (f *fakeTasks) republished() []uuid.UUID {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]uuid.UUID(nil), f.republishedIDs...)
}

// fixedClock returns a now() that always reports t.
func fixedClock(t time.Time) func() time.Time { return func() time.Time { return t } }

// --- tests --------------------------------------------------------------------

func TestSweepRepublishesStrandedPendingTask(t *testing.T) {
	now := time.Date(2026, 3, 1, 12, 0, 0, 0, time.UTC)
	stranded := model.Task{ID: uuid.New(), Status: model.TaskStatusPending, Title: "lost publish"}

	f := &fakeTasks{stale: []model.Task{stranded}}
	r := New(f, Options{now: fixedClock(now)})

	n, err := r.Sweep(context.Background())
	if err != nil {
		t.Fatalf("Sweep() error = %v", err)
	}
	if n != 1 {
		t.Errorf("Sweep() rescued = %d, want 1", n)
	}
	if got := f.republished(); len(got) != 1 || got[0] != stranded.ID {
		t.Errorf("republished = %v, want [%s]", got, stranded.ID)
	}
}

func TestSweepComputesWindowsFromGrace(t *testing.T) {
	now := time.Date(2026, 3, 1, 12, 0, 0, 0, time.UTC)
	f := &fakeTasks{}

	r := New(f, Options{
		PendingGrace:      5 * time.Minute,
		ProcessingTimeout: 20 * time.Minute,
		now:               fixedClock(now),
	})
	if _, err := r.Sweep(context.Background()); err != nil {
		t.Fatalf("Sweep() error = %v", err)
	}

	// A pending task older than 5m, and a processing task idle longer than 20m.
	if want := now.Add(-5 * time.Minute); !f.gotPendingBefore.Equal(want) {
		t.Errorf("pendingBefore = %v, want %v", f.gotPendingBefore, want)
	}
	if want := now.Add(-20 * time.Minute); !f.gotProcessingBefore.Equal(want) {
		t.Errorf("processingBefore = %v, want %v", f.gotProcessingBefore, want)
	}
}

func TestSweepProcessesABatchAndKeepsGoingOnError(t *testing.T) {
	a, b, c := uuid.New(), uuid.New(), uuid.New()
	f := &fakeTasks{
		stale: []model.Task{
			{ID: a, Status: model.TaskStatusPending},
			{ID: b, Status: model.TaskStatusPending},
			{ID: c, Status: model.TaskStatusProcessing},
		},
		republishErr: map[uuid.UUID]error{b: errors.New("broker unavailable")},
	}
	r := New(f, Options{})

	n, err := r.Sweep(context.Background())
	if err != nil {
		t.Fatalf("Sweep() error = %v, want nil (per-row errors must not abort the batch)", err)
	}
	if n != 2 {
		t.Errorf("Sweep() rescued = %d, want 2 (the failing row is skipped)", n)
	}

	got := f.republished()
	if len(got) != 2 {
		t.Fatalf("republished %d tasks, want 2", len(got))
	}
	for _, id := range got {
		if id == b {
			t.Error("task b was republished despite its error")
		}
	}
}

func TestSweepReturnsErrorWhenQueryFails(t *testing.T) {
	f := &fakeTasks{err: errors.New("db down")}
	r := New(f, Options{})

	if _, err := r.Sweep(context.Background()); err == nil {
		t.Fatal("Sweep() error = nil, want the query error surfaced")
	}
}

func TestSweepNoRowsIsNotAnError(t *testing.T) {
	r := New(&fakeTasks{}, Options{})

	n, err := r.Sweep(context.Background())
	if err != nil {
		t.Fatalf("Sweep() error = %v", err)
	}
	if n != 0 {
		t.Errorf("Sweep() rescued = %d, want 0", n)
	}
}

func TestRunSweepsImmediatelyThenStopsOnCancel(t *testing.T) {
	f := &fakeTasks{}
	// A long interval proves the first sweep is immediate rather than ticker-driven.
	r := New(f, Options{Interval: time.Hour})

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		r.Run(ctx)
		close(done)
	}()

	// Wait for the immediate sweep to land.
	deadline := time.After(2 * time.Second)
	for {
		f.mu.Lock()
		calls := f.findCalls
		f.mu.Unlock()
		if calls > 0 {
			break
		}
		select {
		case <-deadline:
			t.Fatal("Run() did not sweep immediately after start")
		default:
			time.Sleep(5 * time.Millisecond)
		}
	}

	cancel()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Run() did not return after context cancellation")
	}
}

func TestNewAppliesDefaults(t *testing.T) {
	r := New(&fakeTasks{}, Options{})
	if r.opts.Interval <= 0 {
		t.Error("Interval default not applied")
	}
	if r.opts.PendingGrace <= 0 {
		t.Error("PendingGrace default not applied")
	}
	if r.opts.ProcessingTimeout <= 0 {
		t.Error("ProcessingTimeout default not applied")
	}
	if r.opts.BatchSize <= 0 {
		t.Error("BatchSize default not applied")
	}
	if r.opts.now == nil {
		t.Error("now default not applied")
	}
}

func TestRunInvokesOnSweepWithCount(t *testing.T) {
	f := &fakeTasks{stale: []model.Task{{ID: uuid.New(), Status: model.TaskStatusPending}}}
	r := New(f, Options{Interval: time.Hour})

	got := make(chan int, 1)
	r.OnSweep = func(n int) {
		select {
		case got <- n:
		default:
		}
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go r.Run(ctx)

	select {
	case n := <-got:
		if n != 1 {
			t.Errorf("OnSweep received %d, want 1", n)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("OnSweep was never called by Run")
	}
}
