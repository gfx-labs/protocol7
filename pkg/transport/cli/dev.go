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
	"sync/atomic"
	"syscall"
	"time"

	"go.uber.org/fx"

	"github.com/gfx-labs/protocol7/pkg/orchestration/coreworker"
	"github.com/gfx-labs/protocol7/pkg/p7/p7fx"
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

// options are the components of the command, built from its flags. The
// Temporal server and the NATS server start before the clients that use
// them, because each client's configuration is made from its server.
func (c *devCommand) options() (fx.Option, error) {
	host, err := hostName(c.Host)
	if err != nil {
		return nil, err
	}
	dataDir := c.DataDir
	if dataDir == "" {
		state, err := defaultStateDir()
		if err != nil {
			return nil, err
		}
		dataDir = state + "-dev"
	}
	server := temporalServerConfig{
		Binary: c.TemporalBinary, Port: c.TemporalPort, UIPort: c.TemporalUIPort,
		Database: filepath.Join(dataDir, "temporal", "temporal.db"),
	}
	return fx.Options(
		// A first start creates the server's database, which can be slow.
		fx.StartTimeout(60*time.Second),
		fx.StopTimeout(stopTimeout),
		fx.Supply(server, coreworker.Config{Host: host}),
		fx.Provide(
			newTemporalServer,
			func(server *temporalServer) p7fx.TemporalConfig {
				return p7fx.TemporalConfig{Address: server.Address(), Namespace: "default"}
			},
			func(lifecycle fx.Lifecycle) (*stream.Embedded, error) {
				embedded, err := stream.NewEmbedded(loopback, c.NATSPort, filepath.Join(dataDir, "nats"))
				if err != nil {
					return nil, err
				}
				lifecycle.Append(fx.StartStopHook(embedded.Start, embedded.Stop))
				return embedded, nil
			},
			func(embedded *stream.Embedded) natsURL { return natsURL(embedded.URL()) },
		),
		p7fx.TemporalClient,
		framesModule,
		coreModule(hostSessions{StateDir: filepath.Join(dataDir, "sessions"), Shim: c.Shim, Harness: c.Harness.harness()}),
		// Asking for the worker puts this hook after every other start.
		fx.Invoke(func(lifecycle fx.Lifecycle, worker *coreworker.Worker, server *temporalServer, nats natsURL, logger *slog.Logger) {
			lifecycle.Append(fx.StartHook(func() {
				logger.Info("p7 dev is up",
					"temporal", server.Address(), "temporal_ui", server.UIURL(), "nats", string(nats),
					"task_queue", worker.TaskQueue(), "data_dir", dataDir)
			}))
		}),
	), nil
}

func (c *devCommand) Run(ctx context.Context, logger *slog.Logger) error {
	options, err := c.options()
	if err != nil {
		return err
	}
	return p7fx.Run(ctx, logger, options)
}

// temporalServerConfig configures the Temporal development server.
type temporalServerConfig struct {
	// Binary is the Temporal CLI.
	Binary string
	Port   int
	UIPort int
	// Database is the file the server keeps its state in.
	Database string
}

// temporalServer is a Temporal development server run as a child process.
type temporalServer struct {
	config     temporalServerConfig
	shutdowner fx.Shutdowner
	logger     *slog.Logger

	command *exec.Cmd
	// stopping is set once stop has asked the child to exit.
	stopping atomic.Bool
	// exited is closed when the child has exited, after exitErr is set.
	exited  chan struct{}
	exitErr error
}

func newTemporalServer(lifecycle fx.Lifecycle, shutdowner fx.Shutdowner, config temporalServerConfig, logger *slog.Logger) *temporalServer {
	server := &temporalServer{config: config, shutdowner: shutdowner, logger: logger, exited: make(chan struct{})}
	lifecycle.Append(fx.StartStopHook(server.start, server.stop))
	return server
}

// Address is the server's frontend address.
func (s *temporalServer) Address() string {
	return net.JoinHostPort(loopback, strconv.Itoa(s.config.Port))
}

// UIURL is the address of Temporal's web UI.
func (s *temporalServer) UIURL() string {
	return "http://" + net.JoinHostPort(loopback, strconv.Itoa(s.config.UIPort))
}

// start runs the server and returns once it listens.
func (s *temporalServer) start(ctx context.Context) error {
	if err := os.MkdirAll(filepath.Dir(s.config.Database), 0o700); err != nil {
		return fmt.Errorf("create the Temporal data directory: %w", err)
	}
	s.command = exec.Command(s.config.Binary, "server", "start-dev",
		"--ip", loopback,
		"--port", strconv.Itoa(s.config.Port),
		"--ui-port", strconv.Itoa(s.config.UIPort),
		"--db-filename", s.config.Database,
		"--log-level", "warn",
	)
	s.command.Stdout, s.command.Stderr = os.Stderr, os.Stderr
	// The server must not outlive this process, and an interrupt on the
	// terminal must reach it once, from here.
	s.command.SysProcAttr = &syscall.SysProcAttr{Setpgid: true, Pdeathsig: syscall.SIGTERM}
	if err := s.command.Start(); err != nil {
		return fmt.Errorf("start the Temporal development server (%s): %w", s.config.Binary, err)
	}
	go func() {
		s.exitErr = s.command.Wait()
		close(s.exited)
	}()

	retry := time.NewTicker(200 * time.Millisecond)
	defer retry.Stop()
	for {
		connection, err := net.DialTimeout("tcp", s.Address(), time.Second)
		if err == nil {
			_ = connection.Close()
			break
		}
		select {
		case <-ctx.Done():
			_ = s.stop()
			return fmt.Errorf("the Temporal development server did not listen on %s: %w", s.Address(), ctx.Err())
		case <-s.exited:
			return fmt.Errorf("the Temporal development server exited before it listened on %s: %w", s.Address(), s.exitReason())
		case <-retry.C:
		}
	}

	// Nothing here works without the server. If it goes, so does the process.
	go func() {
		<-s.exited
		if s.stopping.Load() {
			return
		}
		s.logger.Error("the Temporal development server exited", "error", s.exitReason())
		_ = s.shutdowner.Shutdown(fx.ExitCode(1))
	}()
	return nil
}

func (s *temporalServer) exitReason() error {
	if s.exitErr == nil {
		return errors.New("it exited with status 0")
	}
	return s.exitErr
}

// stop ends the server and waits for it.
func (s *temporalServer) stop() error {
	if s.command == nil || s.command.Process == nil {
		return nil
	}
	s.stopping.Store(true)
	_ = s.command.Process.Signal(syscall.SIGTERM)
	select {
	case <-s.exited:
	case <-time.After(15 * time.Second):
		_ = s.command.Process.Kill()
		<-s.exited
	}
	return nil
}
