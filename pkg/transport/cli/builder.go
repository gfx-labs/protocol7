package cli

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"sync/atomic"
	"time"

	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/log"
	"go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"
	"golang.org/x/sync/errgroup"

	"github.com/gfx-labs/protocol7/pkg/orchestration/build"
	"github.com/gfx-labs/protocol7/pkg/p7"
	"github.com/gfx-labs/protocol7/pkg/transport/egress"
	"github.com/gfx-labs/protocol7/pkg/transport/githubapi"
	"github.com/gfx-labs/protocol7/pkg/transport/hooks"
)

// builderCommand runs a tenant's image builder (protocol7-selfdev
// docs/image-builder.md): the build.Build workflow and its activities on the
// tenant's build queue, and main builds for pushes from the webhook bus.
type builderCommand struct {
	TemporalAddress   string `name:"temporal-address" required:"" help:"Temporal frontend, host:port."`
	TemporalNamespace string `name:"temporal-namespace" required:"" help:"The tenant's Temporal namespace."`
	TemporalServerID  string `name:"temporal-server-spiffe-id" env:"TEMPORAL_SERVER_SPIFFE_ID" help:"SPIFFE ID the Temporal frontend must present. Empty connects in plaintext."`
	SPIFFESocket      string `name:"spiffe-socket" default:"unix:///spiffe-workload-api/spire-agent.sock" help:"SPIFFE Workload API socket."`

	PDPGateway        string `name:"pdp-gateway" required:"" help:"PDP's SPIFFE listener, host:port."`
	PDPGatewayID      string `name:"pdp-gateway-id" required:"" help:"PDP's SPIFFE ID."`
	PDPCAFile         string `name:"pdp-ca-file" help:"PEM file of PDP's interception root."`
	GitHubPlaceholder string `name:"github-placeholder" required:"" help:"Placeholder of the tenant's GitHub binding, which PDP swaps for a read token."`

	Registry       string `name:"registry" default:"zot.registry.svc:5000" help:"zot host:port that builds push to."`
	RegistryCAFile string `name:"registry-ca-file" help:"PEM file of zot's CA."`

	BuildKit            string        `name:"buildkit" required:"" help:"buildkitd address, tcp://buildkitd.<namespace>.svc:1234."`
	BuildKitStatefulSet string        `name:"buildkit-statefulset" help:"namespace/name of the buildkitd StatefulSet the builder scales between 0 and 1. Empty leaves it alone."`
	BuildKitIdleAfter   time.Duration `name:"buildkit-idle-after" default:"30m" help:"Idle time after which buildkitd is scaled to zero."`

	HooksURL      string `name:"hooks-url" help:"Webhook bus. Empty builds no pushes."`
	HooksConsumer string `name:"hooks-consumer" help:"Durable consumer granted to the builder, hooks-<tenant>-build."`
	HooksServerID string `name:"hooks-server-id" help:"The bus's SPIFFE ID."`

	Repository   string            `name:"repository" required:"" help:"owner/name of the repository this builder builds."`
	Branch       string            `name:"branch" default:"main" help:"The branch whose pushes are built to warm the cache."`
	Inputs       []string          `name:"inputs" required:"" help:"Paths whose contents determine the images."`
	Images       []build.ImageSpec `name:"image" required:"" help:"name:package:dockerfile[:target], one per image."`
	WorkDir      string            `name:"work-dir" default:"/work" help:"Directory for checkouts."`
	MaxBuilds    int               `name:"max-concurrent-builds" default:"1" help:"Builds run at once on this tenant's buildkitd."`
	BuildTimeout time.Duration     `name:"build-timeout" default:"30m" help:"Longest single build."`
}

// Run starts the builder and blocks until ctx ends.
func (c *builderCommand) Run(ctx context.Context, logger *slog.Logger) error {
	if c.HooksURL != "" && (c.HooksConsumer == "" || c.HooksServerID == "") {
		return errors.New("builder: hooks-url needs hooks-consumer and hooks-server-id")
	}
	var packages []string
	for _, image := range c.Images {
		packages = append(packages, image.Package)
	}

	pdp, err := egress.New(ctx, egress.Config{WorkloadAPI: c.SPIFFESocket, Gateway: c.PDPGateway, GatewayID: c.PDPGatewayID, CAFile: c.PDPCAFile})
	if err != nil {
		return err
	}
	defer pdp.Close()
	github := githubapi.NewThroughPDP(c.GitHubPlaceholder, "")
	github.UseHTTP(pdp.Client(30 * time.Second))
	forwarder, err := build.NewForwarder(pdp)
	if err != nil {
		return err
	}
	gitEnv := []string{"PATH=" + os.Getenv("PATH"), "HTTPS_PROXY=" + forwarder.ProxyURL(), "https_proxy=" + forwarder.ProxyURL()}
	if c.PDPCAFile != "" {
		gitEnv = append(gitEnv, "GIT_SSL_CAINFO="+c.PDPCAFile)
	}

	credential := build.WorkloadCredential(c.SPIFFESocket)
	registryTransport, err := build.TrustingTransport(c.RegistryCAFile)
	if err != nil {
		return fmt.Errorf("builder: registry CA: %w", err)
	}
	solver := build.BuildKit{Address: c.BuildKit, RegistryHost: c.Registry, Credential: credential}
	var scaler *build.Scaler
	if c.BuildKitStatefulSet != "" {
		if scaler, err = build.NewInClusterScaler(c.BuildKitStatefulSet, c.BuildKitIdleAfter); err != nil {
			return err
		}
		solver.Wake = scaler.Wake
	}
	var running atomic.Int64
	activities := &build.Activities{
		Repository: c.Repository, Inputs: c.Inputs, RegistryHost: c.Registry, Packages: packages, WorkDir: c.WorkDir,
		Source:   build.GitSource{ListTree: build.GitHubTree(github), Placeholder: c.GitHubPlaceholder, Env: gitEnv},
		Registry: build.Zot{Host: c.Registry, Credential: credential, Transport: registryTransport},
		Solver:   countingSolver{solver: solver, running: &running},
	}
	builder := &build.Builder{Repository: c.Repository, Images: c.Images, BuildTimeout: c.BuildTimeout}

	temporalClient, closeClient, err := p7.NewTemporalClient(client.Options{
		HostPort: c.TemporalAddress, Namespace: c.TemporalNamespace, Logger: log.NewStructuredLogger(logger),
	}, p7.TemporalSPIFFE{Socket: c.SPIFFESocket, ServerID: c.TemporalServerID})
	if err != nil {
		return err
	}
	defer closeClient()
	w := worker.New(temporalClient, build.TaskQueue, worker.Options{MaxConcurrentActivityExecutionSize: max(c.MaxBuilds, 1) + 3})
	w.RegisterWorkflowWithOptions(builder.Workflow, workflow.RegisterOptions{Name: build.WorkflowName})
	activities.RegisterOn(registrar{w})

	group, ctx := errgroup.WithContext(ctx)
	group.Go(func() error { return forwarder.Serve(ctx) })
	group.Go(func() error {
		<-ctx.Done()
		return forwarder.Close()
	})
	group.Go(func() error {
		if err := w.Start(); err != nil {
			return err
		}
		<-ctx.Done()
		w.Stop()
		return nil
	})
	if scaler != nil {
		group.Go(func() error { return scaler.Run(ctx, func() bool { return running.Load() > 0 }) })
	}
	if c.HooksURL != "" {
		handler := build.PushHandler{Repository: c.Repository, Branch: c.Branch, Inputs: c.Inputs, Temporal: temporalClient}
		// The bus only warms the cache for pushes. A builder that cannot reach
		// it still serves the build queue, so a failure is logged and tried
		// again, and never ends the builder.
		group.Go(func() error {
			return hooks.SubscribeRetrying(ctx, logger, hooks.SubscriberConfig{URL: c.HooksURL, Consumer: c.HooksConsumer, SPIFFESocket: c.SPIFFESocket, ServerID: c.HooksServerID},
				func(ctx context.Context, delivery hooks.Delivery) error {
					if delivery.Source != "github" {
						return nil
					}
					return handler.Handle(ctx, delivery.Event, delivery.Body)
				})
		})
	}
	logger.Info("builder started", "repository", c.Repository, "images", len(c.Images), "queue", build.TaskQueue)
	if err := group.Wait(); err != nil && !errors.Is(err, context.Canceled) {
		return err
	}
	return nil
}

// countingSolver counts running solves, so buildkitd is not scaled down
// under a build.
type countingSolver struct {
	solver  build.Solver
	running *atomic.Int64
}

func (s countingSolver) Solve(ctx context.Context, job build.SolveJob, progress func(string)) (string, error) {
	s.running.Add(1)
	defer s.running.Add(-1)
	return s.solver.Solve(ctx, job, progress)
}

type registrar struct{ worker worker.Worker }

func (r registrar) RegisterActivity(name string, function any) {
	r.worker.RegisterActivityWithOptions(function, activity.RegisterOptions{Name: name})
}
