package cli

import (
	"log/slog"

	"go.temporal.io/sdk/client"
	"go.uber.org/fx"

	"github.com/gfx-labs/protocol7/pkg/domain"
	"github.com/gfx-labs/protocol7/pkg/execution/acpshim"
	"github.com/gfx-labs/protocol7/pkg/execution/session"
	"github.com/gfx-labs/protocol7/pkg/orchestration/activity"
	"github.com/gfx-labs/protocol7/pkg/orchestration/coreworker"
	"github.com/gfx-labs/protocol7/pkg/orchestration/kubernetes"
	"github.com/gfx-labs/protocol7/pkg/transport/stream"
)

// This file is where the commands' components are tied together. Every
// package exposes plain constructors. The functions here give them to fx
// and attach each component's start and stop to the application's.
//
// fx starts components in the order their constructors ran, which is
// dependency order, and stops them in reverse. So a component that must
// start after another takes it, or something made from it, as a parameter.

// hostSessions says where this machine keeps its sessions and how it runs
// their processes.
type hostSessions struct {
	// StateDir holds one directory per session.
	StateDir string
	// Shim is the p7acpshim command. Empty looks beside this executable and
	// then in PATH.
	Shim    string
	Harness session.Harness
}

// natsURL is the NATS server that session frames are published to.
type natsURL string

// coreModule is the core worker of one machine: its session manager, the
// session activities and the Temporal worker that serves them. It needs a
// client.Client and a coreworker.Config. With a session.Publisher in the
// application, the journal's frames are published to it.
func coreModule(sessions hostSessions) fx.Option {
	return fx.Module("core",
		fx.Supply(sessions),
		fx.Provide(
			newSessionManager,
			activity.NewSessions,
			newCoreWorker,
		),
		// Nothing else asks for the worker, and fx builds only what is
		// asked for.
		fx.Invoke(func(*coreworker.Worker) {}),
	)
}

type sessionManagerParams struct {
	fx.In

	Sessions  hostSessions
	Temporal  client.Client
	Publisher session.Publisher `optional:"true"`
	Logger    *slog.Logger
}

func newSessionManager(params sessionManagerParams) (*session.Manager, error) {
	shim, err := acpshim.Find(params.Sessions.Shim)
	if err != nil {
		return nil, err
	}
	params.Logger.Info("sessions of this machine", "state_dir", params.Sessions.StateDir, "shim", shim)
	return session.NewManager(session.Config{
		Root: params.Sessions.StateDir, ShimBinary: shim, Harness: params.Sessions.Harness,
		Publisher: params.Publisher, Tools: coreworker.ToolCalls{Temporal: params.Temporal}, Logger: params.Logger,
	})
}

func newCoreWorker(
	lifecycle fx.Lifecycle, config coreworker.Config, temporalClient client.Client,
	sessions *activity.Sessions, logger *slog.Logger,
) (*coreworker.Worker, error) {
	worker, err := coreworker.New(config, temporalClient, sessions, logger)
	if err != nil {
		return nil, err
	}
	lifecycle.Append(fx.StartStopHook(worker.Start, worker.Stop))
	return worker, nil
}

// framesModule publishes session frames to the NATS server at the
// application's natsURL, connected from its start to its stop.
var framesModule = fx.Module("frames",
	fx.Provide(fx.Annotate(newFrames, fx.As(new(session.Publisher)))),
)

func newFrames(lifecycle fx.Lifecycle, url natsURL) *stream.Stream {
	frames := stream.New(string(url))
	lifecycle.Append(fx.StartStopHook(frames.Connect, frames.Close))
	return frames
}

// podsModule is the kubernetes backend's worker: it creates and deletes
// session pods on the queue p7-kubernetes. It needs a client.Client and a
// kubernetes.Config.
var podsModule = fx.Module("pods",
	fx.Provide(kubernetes.New),
	fx.Invoke(func(lifecycle fx.Lifecycle, temporalClient client.Client, pods *kubernetes.Pods, logger *slog.Logger) {
		temporalWorker := kubernetes.NewWorker(temporalClient, pods)
		reconciler := kubernetes.NewReconciler(pods, temporalClient)
		lifecycle.Append(fx.StartStopHook(
			func() error {
				if err := temporalWorker.Start(); err != nil {
					return err
				}
				logger.Info("kubernetes backend started", "task_queue", domain.KubernetesQueue)
				// After the worker, which runs the reconciliation's probes.
				reconciler.Start()
				return nil
			},
			func() {
				reconciler.Stop()
				temporalWorker.Stop()
			},
		))
	}),
)
