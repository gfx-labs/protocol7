// Package coreworker is the core Temporal worker of one machine: it serves
// the session activities for the host backend on the queue local-<host>.
package coreworker

import (
	"fmt"
	"log/slog"
	"time"

	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/worker"

	"github.com/gfx-labs/protocol7/pkg/domain"
	"github.com/gfx-labs/protocol7/pkg/orchestration/activity"
)

// Config configures a core worker.
type Config struct {
	// Host names this machine. The worker polls the queue local-<Host>.
	Host string
}

// Worker is the core worker of one machine.
type Worker struct {
	taskQueue string
	worker    worker.Worker
	logger    *slog.Logger
}

// New returns a core worker that serves sessions' activities. It does not
// poll until Start.
func New(config Config, temporalClient client.Client, sessions *activity.Sessions, logger *slog.Logger) (*Worker, error) {
	taskQueue, err := domain.Placement{Backend: domain.BackendHost, Host: config.Host}.TaskQueue()
	if err != nil {
		return nil, err
	}
	temporalWorker := worker.New(temporalClient, taskQueue, worker.Options{
		// Time for a running turn's activity to detach and report it, so
		// the turn is picked up again without waiting for a heartbeat timeout.
		WorkerStopTimeout: 10 * time.Second,
	})
	sessions.Register(temporalWorker)
	return &Worker{taskQueue: taskQueue, worker: temporalWorker, logger: logger}, nil
}

// TaskQueue is the queue the worker polls.
func (w *Worker) TaskQueue() string { return w.taskQueue }

// Start begins polling. It returns at once.
func (w *Worker) Start() error {
	if err := w.worker.Start(); err != nil {
		return fmt.Errorf("start core worker on %s: %w", w.taskQueue, err)
	}
	w.logger.Info("core worker started", "task_queue", w.taskQueue)
	return nil
}

// Stop stops polling and waits for running activities to return.
func (w *Worker) Stop() { w.worker.Stop() }
