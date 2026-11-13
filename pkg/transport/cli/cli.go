// Package cli holds the commands of the p7 and p7-worker binaries. The
// entry points in cmd/ only call into it.
//
// Each command is an fx application: it turns its flags into configuration
// values and names the modules it runs. wire.go holds the modules.
package cli

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"

	"github.com/alecthomas/kong"
	"go.uber.org/fx"

	"github.com/gfx-labs/protocol7/pkg/execution/session"
	"github.com/gfx-labs/protocol7/pkg/orchestration/coreworker"
	"github.com/gfx-labs/protocol7/pkg/p7/p7fx"
)

// harnessFlags say how the agent harness is started.
type harnessFlags struct {
	Command    string   `name:"harness" default:"towa" env:"P7_HARNESS" help:"Agent harness command. It speaks ACP on standard input and output."`
	Args       []string `name:"harness-arg" env:"P7_HARNESS_ARGS" help:"Argument for the harness (repeatable)."`
	ModelsFlag string   `name:"harness-models-flag" default:"--models" env:"P7_HARNESS_MODELS_FLAG" help:"Harness flag that takes a session's model list. Empty passes no list."`
}

func (f harnessFlags) harness() session.Harness {
	return session.Harness{Command: f.Command, Args: f.Args, ModelsFlag: f.ModelsFlag}
}

// hostName is the name of this machine: named if it is set, else the
// hostname.
func hostName(named string) (string, error) {
	if named != "" {
		return named, nil
	}
	hostname, err := os.Hostname()
	if err != nil {
		return "", fmt.Errorf("read hostname, name the machine with --host: %w", err)
	}
	return hostname, nil
}

func defaultStateDir() (string, error) {
	if state := os.Getenv("XDG_STATE_HOME"); filepath.IsAbs(state) {
		return filepath.Join(state, "p7"), nil
	}
	home, err := os.UserHomeDir()
	if err != nil {
		return "", fmt.Errorf("find a state directory, name one with --state-dir: %w", err)
	}
	return filepath.Join(home, ".local", "state", "p7"), nil
}

// stopTimeout bounds stopping a command's components. The core worker alone
// waits up to ten seconds for running activities to detach from their turns.
const stopTimeout = 45 * time.Second

// workerCommand is the core worker of this machine.
type workerCommand struct {
	TemporalAddress   string       `name:"temporal-address" default:"127.0.0.1:7233" env:"TEMPORAL_ADDRESS" help:"Temporal frontend address."`
	TemporalNamespace string       `name:"temporal-namespace" default:"default" env:"TEMPORAL_NAMESPACE" help:"Temporal namespace."`
	NATSURL           string       `name:"nats-url" default:"nats://127.0.0.1:4222" env:"P7_NATS_URL" help:"NATS server that session frames are published to. Empty publishes none."`
	Host              string       `name:"host" env:"P7_HOST" help:"Name of this machine. The worker polls local-<host>. Default: the hostname."`
	StateDir          string       `name:"state-dir" env:"P7_STATE_DIR" help:"Directory that holds this machine's sessions. Default: p7 in the XDG state directory."`
	Shim              string       `name:"shim" env:"P7_SHIM" help:"Path of p7acpshim. Default: beside this executable, then PATH."`
	Harness           harnessFlags `embed:""`
}

// options are the components of the command, built from its flags.
func (c *workerCommand) options() (fx.Option, error) {
	host, err := hostName(c.Host)
	if err != nil {
		return nil, err
	}
	stateDir := c.StateDir
	if stateDir == "" {
		if stateDir, err = defaultStateDir(); err != nil {
			return nil, err
		}
	}
	options := []fx.Option{
		fx.StopTimeout(stopTimeout),
		fx.Supply(
			p7fx.TemporalConfig{Address: c.TemporalAddress, Namespace: c.TemporalNamespace},
			coreworker.Config{Host: host},
		),
		p7fx.TemporalClient,
		coreModule(hostSessions{StateDir: stateDir, Shim: c.Shim, Harness: c.Harness.harness()}),
	}
	if c.NATSURL != "" {
		options = append(options, fx.Supply(natsURL(c.NATSURL)), framesModule)
	}
	return fx.Options(options...), nil
}

func (c *workerCommand) Run(ctx context.Context, logger *slog.Logger) error {
	options, err := c.options()
	if err != nil {
		return err
	}
	return p7fx.Run(ctx, logger, options)
}

type root struct {
	Worker workerCommand `cmd:"" help:"Run the core worker of this machine: agent sessions on the host backend."`
	Dev    devCommand    `cmd:"" help:"Run a Temporal development server, an embedded NATS server and the core worker in one process."`
}

// Main runs the p7 command and returns its exit status.
func Main(args []string) int {
	var commands root
	return run("p7", "protocol7: durable agent work on Temporal.", &commands, args)
}

// WorkerMain runs the p7-worker command, which is p7 worker.
func WorkerMain(args []string) int {
	var command workerCommand
	return run("p7-worker", "The protocol7 core worker of this machine.", &command, args)
}

func run(name, description string, grammar any, args []string) int {
	parser, err := kong.New(grammar, kong.Name(name), kong.Description(description), kong.UsageOnError())
	if err != nil {
		fmt.Fprintln(os.Stderr, name+":", err)
		return 2
	}
	parsed, err := parser.Parse(args)
	if err != nil {
		parser.Errorf("%v", err)
		return 2
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	logger := slog.New(slog.NewTextHandler(os.Stderr, nil))
	parsed.BindTo(ctx, (*context.Context)(nil))
	parsed.Bind(logger)
	if err := parsed.Run(); err != nil {
		fmt.Fprintln(os.Stderr, name+":", err)
		return 1
	}
	return 0
}
