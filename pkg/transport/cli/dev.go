package cli

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"syscall"
	"time"

	"github.com/gfx-labs/protocol7/pkg/orchestration/coreworker"
	"github.com/gfx-labs/protocol7/pkg/transport/stream"
)

// devCommand runs everything a consumer worker needs on one machine. It has
// no ticketd: that does not exist yet.
type devCommand struct {
	DataDir        string       `name:"data-dir" env:"P7_DEV_DATA_DIR" help:"Directory for Temporal's database, NATS storage and sessions. Default: p7-dev in the XDG state directory."`
	Host           string       `name:"host" env:"P7_HOST" help:"Name of this machine. The worker polls local-<host>. Default: the hostname."`
	TemporalPort   int          `name:"temporal-port" default:"7233" help:"Port of the Temporal development server."`
	TemporalUIPort int          `name:"temporal-ui-port" default:"8233" help:"Port of Temporal's web UI."`
	NATSPort       int          `name:"nats-port" default:"4222" help:"Port of the embedded NATS server."`
	TemporalBinary string       `name:"temporal-binary" default:"temporal" help:"The Temporal CLI, which provides the development server."`
	Shim           string       `name:"shim" env:"P7_SHIM" help:"Path of p7acpshim. Default: beside this executable, then PATH."`
	Harness        harnessFlags `embed:""`
}

const loopback = "127.0.0.1"

func (c *devCommand) Run(ctx context.Context, logger *slog.Logger) error {
	dataDir := c.DataDir
	if dataDir == "" {
		state, err := defaultStateDir()
		if err != nil {
			return err
		}
		dataDir = state + "-dev"
	}
	for _, name := range []string{"temporal", "nats"} {
		if err := os.MkdirAll(filepath.Join(dataDir, name), 0o700); err != nil {
			return fmt.Errorf("create data directory: %w", err)
		}
	}

	natsServer, err := stream.StartEmbedded(loopback, c.NATSPort, filepath.Join(dataDir, "nats"))
	if err != nil {
		return err
	}
	defer natsServer.Stop()

	temporalAddress := net.JoinHostPort(loopback, strconv.Itoa(c.TemporalPort))
	temporal := exec.Command(c.TemporalBinary, "server", "start-dev",
		"--ip", loopback,
		"--port", strconv.Itoa(c.TemporalPort),
		"--ui-port", strconv.Itoa(c.TemporalUIPort),
		"--db-filename", filepath.Join(dataDir, "temporal", "temporal.db"),
		"--log-level", "warn",
	)
	temporal.Stdout, temporal.Stderr = os.Stderr, os.Stderr
	// The server must not outlive this process, and an interrupt on the
	// terminal must reach it once, from here.
	temporal.SysProcAttr = &syscall.SysProcAttr{Setpgid: true, Pdeathsig: syscall.SIGTERM}
	if err := temporal.Start(); err != nil {
		return fmt.Errorf("start the Temporal development server (%s): %w", c.TemporalBinary, err)
	}
	temporalExited := make(chan error, 1)
	go func() { temporalExited <- temporal.Wait() }()
	defer func() {
		_ = temporal.Process.Signal(syscall.SIGTERM)
		select {
		case <-temporalExited:
		case <-time.After(15 * time.Second):
			_ = temporal.Process.Kill()
		}
	}()
	if err := waitForPort(ctx, temporalAddress, temporalExited); err != nil {
		return err
	}

	flags := workerFlags{
		TemporalAddress: temporalAddress, TemporalNamespace: "default", NATSURL: natsServer.URL(),
		Host: c.Host, StateDir: filepath.Join(dataDir, "sessions"), Shim: c.Shim, Harness: c.Harness,
	}
	config, err := flags.config(logger)
	if err != nil {
		return err
	}
	logger.Info("p7 dev is up",
		"temporal", temporalAddress,
		"temporal_ui", "http://"+net.JoinHostPort(loopback, strconv.Itoa(c.TemporalUIPort)),
		"nats", natsServer.URL(), "host", config.Host, "data_dir", dataDir)

	workerCtx, stopWorker := context.WithCancel(ctx)
	defer stopWorker()
	go func() {
		select {
		case <-temporalExited:
			logger.Error("the Temporal development server exited")
			stopWorker()
		case <-workerCtx.Done():
		}
	}()
	return coreworker.Run(workerCtx, config)
}

// waitForPort returns once address accepts a connection.
func waitForPort(ctx context.Context, address string, exited <-chan error) error {
	deadline := time.After(60 * time.Second)
	retry := time.NewTicker(200 * time.Millisecond)
	defer retry.Stop()
	for {
		connection, err := net.DialTimeout("tcp", address, time.Second)
		if err == nil {
			_ = connection.Close()
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case err := <-exited:
			if err == nil {
				err = errors.New("it exited with status 0")
			}
			return fmt.Errorf("the Temporal development server exited before it listened on %s: %w", address, err)
		case <-deadline:
			return fmt.Errorf("the Temporal development server did not listen on %s within 60 seconds", address)
		case <-retry.C:
		}
	}
}
