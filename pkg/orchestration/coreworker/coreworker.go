// Package coreworker is the core Temporal worker of one machine: it serves
// the session activities for the host backend on the queue local-<host>.
package coreworker

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/worker"

	"github.com/gfx-labs/protocol7/pkg/domain"
	"github.com/gfx-labs/protocol7/pkg/execution/snapshot"
	"github.com/gfx-labs/protocol7/pkg/orchestration/activity"
)

// Config configures a core worker.
type Config struct {
	// Host names this machine. The worker polls the queue local-<Host>.
	Host string
	// TaskQueue, if set, is polled in place of local-<Host>. A session's pod
	// on the kubernetes backend polls the queue of its one session.
	TaskQueue string
	// Snapshots says where the sessions' directories are kept. The zero
	// value keeps none. With it, each session's directory is copied to the
	// store after the session opens and after each of its turns, and of every
	// session on this machine when the worker stops. A session that this
	// machine has no directory for is restored from the copy when it is first
	// used.
	Snapshots snapshot.Config
	// Session names the one session that this worker serves, as the worker in
	// a session's pod does. With Snapshots, Start restores its directory
	// from the snapshot, if there is one and the directory is not there,
	// before the worker polls its queue.
	Session string
}

const (
	// restoreTimeout bounds the restore of the session when the worker starts.
	restoreTimeout = 30 * time.Minute
	// stopSnapshotTimeout bounds the snapshots written when the worker stops.
	// The process that stops the worker may give up sooner.
	stopSnapshotTimeout = 10 * time.Minute
)

// Worker is the core worker of one machine.
type Worker struct {
	taskQueue string
	worker    worker.Worker
	logger    *slog.Logger
	sessions  *activity.Sessions
	// session is the one session that Start restores.
	session   string
	snapshots bool
}

// New returns a core worker that serves sessions' activities. It does not
// poll until Start.
func New(config Config, temporalClient client.Client, sessions *activity.Sessions, logger *slog.Logger) (*Worker, error) {
	taskQueue := config.TaskQueue
	if taskQueue == "" {
		var err error
		if taskQueue, err = (domain.Placement{Backend: domain.BackendHost, Host: config.Host}).TaskQueue(); err != nil {
			return nil, err
		}
	}
	if err := config.Snapshots.Validate(); err != nil {
		return nil, err
	}
	snapshots := config.Snapshots.Enabled()
	if snapshots {
		store, err := snapshot.New(config.Snapshots, logger)
		if err != nil {
			return nil, err
		}
		sessions.Manager().SetSnapshots(store)
	}
	temporalWorker := worker.New(temporalClient, taskQueue, worker.Options{
		// Time for a running turn's activity to detach and report it, so
		// the turn is picked up again without waiting for a heartbeat timeout.
		WorkerStopTimeout: 10 * time.Second,
	})
	sessions.Register(temporalWorker)
	return &Worker{taskQueue: taskQueue, worker: temporalWorker, logger: logger, sessions: sessions, session: config.Session, snapshots: snapshots}, nil
}

// TaskQueue is the queue the worker polls.
func (w *Worker) TaskQueue() string { return w.taskQueue }

// Start begins polling. It returns at once.
//
// With snapshots and a session, it first restores the session's directory
// from its snapshot, and fails if it cannot, so that the worker does not
// open the session anew over a snapshot it could not read.
func (w *Worker) Start() error {
	if w.snapshots && w.session != "" {
		ctx, cancel := context.WithTimeout(context.Background(), restoreTimeout)
		defer cancel()
		restored, err := w.sessions.Manager().Restore(ctx, w.session)
		if err != nil {
			return fmt.Errorf("start core worker on %s: %w", w.taskQueue, err)
		}
		w.logger.Info("session snapshot checked", "session", w.session, "restored", restored)
	}
	if err := w.worker.Start(); err != nil {
		return fmt.Errorf("start core worker on %s: %w", w.taskQueue, err)
	}
	w.logger.Info("core worker started", "task_queue", w.taskQueue)
	return nil
}

// Stop stops polling and waits for running activities to return. With
// snapshots it then writes a snapshot of every session of the machine whose
// directory changed since its last.
func (w *Worker) Stop() {
	w.worker.Stop()
	if !w.snapshots {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), stopSnapshotTimeout)
	defer cancel()
	if err := w.sessions.Manager().SnapshotAll(ctx); err != nil {
		w.logger.Error("snapshots on stop failed", "error", err)
	}
}
