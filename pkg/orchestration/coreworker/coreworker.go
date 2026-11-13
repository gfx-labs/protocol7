// Package coreworker is the core Temporal worker of one machine: it runs the
// session activities for the host backend on the queue local-<host>.
package coreworker

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"os/exec"
	"path/filepath"
	"time"

	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/log"
	"go.temporal.io/sdk/worker"

	"github.com/gfx-labs/protocol7/pkg/domain"
	"github.com/gfx-labs/protocol7/pkg/execution/acpshim"
	"github.com/gfx-labs/protocol7/pkg/execution/session"
	"github.com/gfx-labs/protocol7/pkg/orchestration/activity"
	"github.com/gfx-labs/protocol7/pkg/transport/stream"
)

// Config configures a core worker.
type Config struct {
	TemporalAddress   string
	TemporalNamespace string
	// NATSURL is where session frames are published. Empty publishes none.
	NATSURL string
	// Host names this machine. The worker polls the queue local-<Host>.
	Host string
	// StateDir holds the sessions of this machine.
	StateDir string
	// ShimBinary is the p7acpshim command. Empty looks beside this
	// executable and then in PATH.
	ShimBinary string
	Harness    session.Harness
	Logger     *slog.Logger
}

// Run runs the worker until ctx ends.
func Run(ctx context.Context, config Config) error {
	logger := config.Logger
	if logger == nil {
		logger = slog.Default()
	}
	taskQueue, err := domain.Placement{Backend: domain.BackendHost, Host: config.Host}.TaskQueue()
	if err != nil {
		return err
	}
	shim, err := findShim(config.ShimBinary)
	if err != nil {
		return err
	}

	managerConfig := session.Config{Root: config.StateDir, ShimBinary: shim, Harness: config.Harness, Logger: logger}
	if config.NATSURL != "" {
		frames, err := stream.Connect(ctx, config.NATSURL)
		if err != nil {
			return err
		}
		defer frames.Close()
		managerConfig.Publisher = frames
	}
	manager, err := session.NewManager(managerConfig)
	if err != nil {
		return err
	}

	temporalClient, err := client.Dial(client.Options{
		HostPort:  config.TemporalAddress,
		Namespace: config.TemporalNamespace,
		Logger:    log.NewStructuredLogger(logger),
	})
	if err != nil {
		return fmt.Errorf("connect to Temporal at %s: %w", config.TemporalAddress, err)
	}
	defer temporalClient.Close()

	temporalWorker := worker.New(temporalClient, taskQueue, worker.Options{
		// Time for a running turn's activity to detach and report it, so
		// the turn is picked up again without waiting for a heartbeat timeout.
		WorkerStopTimeout: 10 * time.Second,
	})
	activity.NewSessions(manager).Register(temporalWorker)
	if err := temporalWorker.Start(); err != nil {
		return fmt.Errorf("start core worker on %s: %w", taskQueue, err)
	}
	logger.Info("core worker started", "task_queue", taskQueue, "state_dir", config.StateDir, "shim", shim)
	<-ctx.Done()
	temporalWorker.Stop()
	return nil
}

func findShim(configured string) (string, error) {
	if configured != "" {
		return configured, nil
	}
	if executable, err := os.Executable(); err == nil {
		beside := filepath.Join(filepath.Dir(executable), acpshim.Binary)
		if info, err := os.Stat(beside); err == nil && !info.IsDir() {
			return beside, nil
		}
	}
	found, err := exec.LookPath(acpshim.Binary)
	if err != nil {
		return "", errors.New(acpshim.Binary + " was not found beside this executable or in PATH: build cmd/p7acpshim or name it with --shim")
	}
	return found, nil
}
