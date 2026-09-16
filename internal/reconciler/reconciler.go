// Package reconciler periodically rescues work that the message broker has
// lost track of.
//
// The failure it exists for: TaskService.Create commits to Postgres and *then*
// publishes task.created. If that publish fails the task is persisted and the
// error is only logged, by design -- losing a summary beats losing a write.
// Without this sweep, such a task sits in `pending` forever, silently, and
// nobody finds out. It also reaps tasks whose worker died mid-flight, which
// would otherwise stay in `processing` forever.
package reconciler

import (
	"context"
	"log/slog"
	"time"

	"github.com/zakirkun/golang-staterpack/internal/model"
)

// StaleTasks is the narrow dependency the reconciler needs. Declared here so
// the package can be tested with a plain fake and does not import the service.
type StaleTasks interface {
	// FindStale returns abandoned work: `pending` rows created before
	// pendingBefore, and `processing` rows last updated before processingBefore.
	FindStale(ctx context.Context, pendingBefore, processingBefore time.Time, limit int) ([]model.Task, error)
	// RepublishCreated re-emits the task.created event for a task.
	RepublishCreated(ctx context.Context, task model.Task) error
}

// Options configures a Reconciler.
type Options struct {
	// Interval between sweeps.
	Interval time.Duration
	// PendingGrace is how long a task may sit in `pending` before it is
	// considered stranded. Must comfortably exceed normal queue latency, or the
	// sweep will republish tasks that are simply still waiting.
	PendingGrace time.Duration
	// ProcessingTimeout is how long a task may sit in `processing` before its
	// worker is presumed dead.
	ProcessingTimeout time.Duration
	// BatchSize caps how many tasks one sweep republishes, to bound the burst
	// of work injected into the queue.
	BatchSize int

	// now is injectable for tests; defaults to time.Now.
	now func() time.Time
}

// Reconciler runs the sweep loop.
type Reconciler struct {
	tasks StaleTasks
	opts  Options

	// OnSweep, if set, is called after each sweep with the number of tasks
	// rescued. Used for metrics and tests.
	OnSweep func(rescued int)
}

// New builds a Reconciler, applying defaults for any unset option.
func New(tasks StaleTasks, opts Options) *Reconciler {
	if opts.Interval <= 0 {
		opts.Interval = 1 * time.Minute
	}
	if opts.PendingGrace <= 0 {
		opts.PendingGrace = 5 * time.Minute
	}
	if opts.ProcessingTimeout <= 0 {
		opts.ProcessingTimeout = 10 * time.Minute
	}
	if opts.BatchSize <= 0 {
		opts.BatchSize = 100
	}
	if opts.now == nil {
		opts.now = func() time.Time { return time.Now().UTC() }
	}

	return &Reconciler{tasks: tasks, opts: opts}
}

// Run sweeps until ctx is cancelled, then returns. It sweeps once immediately
// so a restart does not wait a full interval before rescuing stranded work.
func (r *Reconciler) Run(ctx context.Context) {
	ticker := time.NewTicker(r.opts.Interval)
	defer ticker.Stop()

	slog.Info("reconciler started",
		"interval", r.opts.Interval,
		"pending_grace", r.opts.PendingGrace,
		"processing_timeout", r.opts.ProcessingTimeout,
	)

	for {
		rescued, err := r.Sweep(ctx)
		if err != nil {
			// Transient database trouble should not kill the loop; log and let
			// the next tick retry.
			slog.Error("reconciler sweep failed", "err", err)
		} else if rescued > 0 {
			slog.Info("reconciler rescued stranded tasks", "count", rescued)
		}
		if r.OnSweep != nil {
			r.OnSweep(rescued)
		}

		select {
		case <-ctx.Done():
			slog.Info("reconciler stopped")
			return
		case <-ticker.C:
		}
	}
}

// Sweep performs a single reconciliation pass and reports how many tasks were
// republished. It is exported so it can be invoked directly (tests, admin
// endpoints) without running the loop.
func (r *Reconciler) Sweep(ctx context.Context) (int, error) {
	now := r.opts.now()
	stale, err := r.tasks.FindStale(
		ctx,
		now.Add(-r.opts.PendingGrace),
		now.Add(-r.opts.ProcessingTimeout),
		r.opts.BatchSize,
	)
	if err != nil {
		return 0, err
	}
	if len(stale) == 0 {
		return 0, nil
	}

	rescued := 0
	for _, task := range stale {
		if err := r.tasks.RepublishCreated(ctx, task); err != nil {
			// Keep going: one bad row should not block the rest of the batch.
			slog.Error("reconciler republish failed", "task_id", task.ID, "err", err)
			continue
		}
		slog.Debug("reconciler republished task", "task_id", task.ID, "status", task.Status)
		rescued++
	}
	return rescued, nil
}
