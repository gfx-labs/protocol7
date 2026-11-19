// Package cli holds the commands of the p7 and p7-worker binaries. The
// entry points in cmd/ only call into it.
//
// Each command is an fx application: it turns its flags into configuration
// values and names the modules it runs. wire.go holds the modules.
package cli

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
	"syscall"
	"time"

	"github.com/alecthomas/kong"
	"go.uber.org/fx"

	"github.com/gfx-labs/protocol7/pkg/domain"
	"github.com/gfx-labs/protocol7/pkg/execution/session"
	"github.com/gfx-labs/protocol7/pkg/execution/snapshot"
	"github.com/gfx-labs/protocol7/pkg/orchestration/coreworker"
	"github.com/gfx-labs/protocol7/pkg/orchestration/kubernetes"
	"github.com/gfx-labs/protocol7/pkg/p7"
	"github.com/gfx-labs/protocol7/pkg/p7/p7fx"
	"github.com/gfx-labs/protocol7/pkg/support/config"
)

// harnessFlags say how the agent harness is started.
type harnessFlags struct {
	Command    string   `name:"harness" default:"towa" env:"P7_HARNESS" help:"Agent harness command. It speaks ACP on standard input and output."`
	Args       []string `name:"harness-arg" env:"P7_HARNESS_ARGS" help:"Argument for the harness (repeatable)."`
	ModelsFlag string   `name:"harness-models-flag" default:"--models" env:"P7_HARNESS_MODELS_FLAG" help:"Harness flag that takes a session's model list. Empty passes no list."`
	StoreFlag  string   `name:"harness-store-flag" default:"--store.root" env:"P7_HARNESS_STORE_FLAG" help:"Harness flag that takes the directory of its conversations. With snapshots, it is given a directory in the session's, so that a snapshot holds the conversation. Empty passes none."`
}

func (f harnessFlags) harness() session.Harness {
	return session.Harness{Command: f.Command, Args: f.Args, ModelsFlag: f.ModelsFlag, StoreFlag: f.StoreFlag}
}

// snapshotFlags say where sessions' snapshots are kept, in S3-compatible
// storage. Credentials are not flags: they come from the AWS environment and
// credential chain. Without a bucket there are no snapshots.
type snapshotFlags struct {
	Endpoint  string   `name:"snapshot-endpoint" env:"P7_SNAPSHOT_ENDPOINT" help:"URL of the S3-compatible store, such as http://seaweedfs-s3.seaweedfs:8333. Empty is Amazon S3."`
	Region    string   `name:"snapshot-region" env:"P7_SNAPSHOT_REGION" help:"Region of the bucket."`
	Bucket    string   `name:"snapshot-bucket" env:"P7_SNAPSHOT_BUCKET" help:"Bucket for snapshots of session directories. Empty keeps no snapshots."`
	Prefix    string   `name:"snapshot-prefix" env:"P7_SNAPSHOT_PREFIX" help:"Prefix of every snapshot key: <prefix>/<session>/workspace.tar.zst."`
	PathStyle bool     `name:"snapshot-path-style" env:"P7_SNAPSHOT_PATH_STYLE" help:"Put the bucket in the request path, not the host name."`
	Exclude   []string `name:"snapshot-exclude" env:"P7_SNAPSHOT_EXCLUDE" default:"node_modules,go-build" help:"Names or paths that a snapshot leaves out (repeatable). A name with no slash matches at any depth, a path is from the session's directory. Go's path.Match syntax."`
}

func (f snapshotFlags) config() snapshot.Config {
	var exclude []string
	for _, pattern := range f.Exclude {
		if pattern != "" {
			exclude = append(exclude, pattern)
		}
	}
	return snapshot.Config{Endpoint: f.Endpoint, Region: f.Region, Bucket: f.Bucket, Prefix: f.Prefix, PathStyle: f.PathStyle, Exclude: exclude}
}

// temporalFlags say which Temporal a command talks to, and how.
type temporalFlags struct {
	Address      string `name:"temporal-address" default:"127.0.0.1:7233" env:"TEMPORAL_ADDRESS" help:"Temporal frontend address."`
	Namespace    string `name:"temporal-namespace" default:"default" env:"TEMPORAL_NAMESPACE" help:"Temporal namespace."`
	SPIFFESocket string `name:"temporal-spiffe-socket" env:"TEMPORAL_SPIFFE_SOCKET" help:"SPIFFE Workload API address that this process's SVID comes from, such as unix:///run/spire/agent-sockets/api.sock. Empty is SPIFFE_ENDPOINT_SOCKET. Used with --temporal-server-spiffe-id."`
	ServerID     string `name:"temporal-server-spiffe-id" env:"TEMPORAL_SERVER_SPIFFE_ID" help:"SPIFFE ID the Temporal frontend must present. Setting it connects with mutual TLS and this process's SVID. Empty connects in plaintext."`
}

func (f temporalFlags) config() p7fx.TemporalConfig {
	return p7fx.TemporalConfig{
		Address: f.Address, Namespace: f.Namespace,
		SPIFFE: p7.TemporalSPIFFE{Socket: f.SPIFFESocket, ServerID: f.ServerID},
	}
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

// snapshotStopTimeout is it for a core worker that writes snapshots when it
// stops. The pod's terminationGracePeriodSeconds, or the init system, ends
// it sooner if it must.
const snapshotStopTimeout = 12 * time.Minute

// workerCommand is the core worker of this machine.
type workerCommand struct {
	Temporal  temporalFlags `embed:""`
	NATSURL   string        `name:"nats-url" default:"nats://127.0.0.1:4222" env:"P7_NATS_URL" help:"NATS server that session frames are published to. Empty publishes none."`
	Host      string        `name:"host" env:"P7_HOST" help:"Name of this machine. The worker polls local-<host>. Default: the hostname."`
	StateDir  string        `name:"state-dir" env:"P7_STATE_DIR" help:"Directory that holds this machine's sessions. Default: p7 in the XDG state directory."`
	Shim      string        `name:"shim" env:"P7_SHIM" help:"Path of p7acpshim. Default: beside this executable, then PATH."`
	Harness   harnessFlags  `embed:""`
	Snapshots snapshotFlags `embed:""`

	// session names the session of a session's pod, whose snapshot is
	// restored before the worker polls.
	session string
	// taskQueue replaces local-<host> for a session's pod.
	taskQueue string
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
	snapshots := c.Snapshots.config()
	if err := snapshots.Validate(); err != nil {
		return nil, err
	}
	timeout := stopTimeout
	if snapshots.Enabled() {
		timeout = snapshotStopTimeout
	}
	options := []fx.Option{
		fx.StopTimeout(timeout),
		fx.Supply(
			c.Temporal.config(),
			coreworker.Config{Host: host, TaskQueue: c.taskQueue, Session: c.session, Snapshots: snapshots},
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
	config.Flag `embed:""`

	Worker  workerCommand  `cmd:"" help:"Run the core worker of this machine: agent sessions on the host backend."`
	Session sessionCommand `cmd:"" help:"Run one session in its own pod on the kubernetes backend."`
	Pods    podsCommand    `cmd:"" help:"Create and delete session pods for the kubernetes backend."`
	Dev     devCommand     `cmd:"" help:"Run a Temporal development server, an embedded NATS server and the core worker in one process."`
}

// sessionCommand is the worker inside a session's pod. It is the host
// session manager, polling the queue of its one session, or the queue of its
// slot in a pool, which many sessions share.
type sessionCommand struct {
	Worker    workerCommand `embed:""`
	SessionID string        `name:"session" env:"P7_SESSION" help:"The session this pod serves. The kubernetes backend sets it in a pod of one session. Exclusive with --pool."`
	Pool      string        `name:"pool" env:"P7_POOL" help:"The pool this pod is a worker of, which many sessions share. The kubernetes backend sets it in a pool's pods. Each session keeps its snapshot under <snapshot prefix>/pools/<pool>/<session>/, and is restored from it when it is first used. Exclusive with --session."`
	WorkerNum string        `name:"worker" env:"P7_WORKER" help:"The slot of this worker in its pool. The kubernetes backend sets it with --pool."`
	Queue     string        `name:"task-queue" required:"" env:"P7_TASK_QUEUE" help:"The queue of the session or of the worker. The kubernetes backend sets it."`
}

func (c *sessionCommand) options() (fx.Option, error) {
	switch {
	case c.SessionID != "" && c.Pool != "":
		return nil, errors.New("p7 session: name a session or a pool, not both")
	case c.SessionID == "" && c.Pool == "":
		return nil, errors.New("p7 session: name a session (--session) or a pool (--pool)")
	}
	c.Worker.taskQueue = c.Queue
	if c.Pool != "" {
		if err := domain.ValidPool(c.Pool); err != nil {
			return nil, err
		}
		slot, err := strconv.Atoi(c.WorkerNum)
		if err != nil || slot < 1 || slot > domain.MaxPoolWorkers {
			return nil, fmt.Errorf("p7 session: --worker must be a slot from 1 to %d, not %q", domain.MaxPoolWorkers, c.WorkerNum)
		}
		if want := domain.PoolQueue(c.Pool, slot); c.Queue != want {
			return nil, fmt.Errorf("p7 session: the queue of slot %d of pool %s is %s, not %s", slot, c.Pool, want, c.Queue)
		}
		// No session is restored at start: each is restored when it is
		// first used, from the prefix of the pool, on whichever worker it
		// is placed.
		if c.Worker.Snapshots.Bucket != "" {
			c.Worker.Snapshots.Prefix = domain.PoolSnapshotPrefix(c.Worker.Snapshots.Prefix, c.Pool)
		}
		if c.Worker.Host == "" {
			c.Worker.Host = "pool-" + c.Pool + "-" + c.WorkerNum
		}
		return c.Worker.options()
	}
	c.Worker.session = c.SessionID
	if c.Worker.Host == "" {
		c.Worker.Host = "session-" + c.SessionID
	}
	return c.Worker.options()
}

func (c *sessionCommand) Run(ctx context.Context, logger *slog.Logger) error {
	options, err := c.options()
	if err != nil {
		return err
	}
	if c.Pool != "" {
		logger = logger.With("pool", c.Pool, "worker", c.WorkerNum)
	} else {
		logger = logger.With("session", c.SessionID)
	}
	return p7fx.Run(ctx, logger, options)
}

// podsCommand creates and deletes session pods. It holds the right to
// create pods in one namespace and nothing else.
type podsCommand struct {
	Temporal          temporalFlags `embed:""`
	Namespace         string        `name:"namespace" required:"" env:"P7_POD_NAMESPACE" help:"Kubernetes namespace that session pods are created in."`
	Template          string        `name:"template" required:"" env:"P7_POD_TEMPLATE" help:"File holding the session pod template, a pod manifest in JSON. Read for each new pod."`
	Container         string        `name:"session-container" default:"session" env:"P7_SESSION_CONTAINER" help:"Container in the template that runs p7 session."`
	ReconcileInterval time.Duration `name:"reconcile-interval" default:"5m" env:"P7_RECONCILE_INTERVAL" help:"How often to delete session pods that nothing owns: those whose session is closed, whose owner workflow is not running, or that are not ready after their open timeout. Also done at start. Only the replica that holds the lease does it."`
	LeaseName         string        `name:"lease-name" default:"p7-pods-reconciler" env:"P7_LEASE_NAME" help:"Lease, in the pod namespace, that the replicas campaign for. Only the replica that holds it reconciles. It needs get, create and update on leases."`
	LeaseDuration     time.Duration `name:"lease-duration" default:"15s" env:"P7_LEASE_DURATION" help:"How long after the reconciling replica dies another takes over. A replica that stops cleanly hands over at once."`
	OpenTimeout       time.Duration `name:"open-timeout" default:"15m" env:"P7_OPEN_TIMEOUT" help:"How long a pod may take to become ready when its request names no open timeout."`
	// Snapshots, when set, makes deleting a session's pod delete the
	// session's snapshot. Use the same as the session pods.
	Snapshots snapshotFlags `embed:""`
}

func (c *podsCommand) options() (fx.Option, error) {
	return fx.Options(
		fx.StopTimeout(stopTimeout),
		fx.Supply(
			c.Temporal.config(),
			kubernetes.Config{
				Namespace: c.Namespace, TemplateFile: c.Template, SessionContainer: c.Container,
				ReconcileInterval: c.ReconcileInterval, OpenTimeout: c.OpenTimeout,
				LeaseName: c.LeaseName, LeaseDuration: c.LeaseDuration,
				Snapshots: c.Snapshots.config(),
			},
		),
		p7fx.TemporalClient,
		podsModule,
	), nil
}

func (c *podsCommand) Run(ctx context.Context, logger *slog.Logger) error {
	options, err := c.options()
	if err != nil {
		return err
	}
	return p7fx.Run(ctx, logger, options)
}

// Main runs the p7 command and returns its exit status.
func Main(args []string) int {
	var commands root
	return run("p7", "protocol7: durable agent work on Temporal.", &commands, args)
}

// WorkerMain runs the p7-worker command, which is p7 worker.
func WorkerMain(args []string) int {
	var command struct {
		config.Flag   `embed:""`
		workerCommand `embed:""`
	}
	return run("p7-worker", "The protocol7 core worker of this machine.", &command, args)
}

func run(name, description string, grammar any, args []string) int {
	parser, err := kong.New(grammar, kong.Name(name), kong.Description(description), kong.UsageOnError(), config.Option())
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
