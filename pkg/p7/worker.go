package p7

import (
	"context"
	"errors"
	"fmt"
	"log/slog"

	"github.com/nexus-rpc/sdk-go/nexus"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/log"
	"go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"
)

// WorkerConfig configures a consumer worker.
type WorkerConfig struct {
	// TaskQueue is the consumer's own queue, where its workflows run.
	TaskQueue         string
	TemporalAddress   string
	TemporalNamespace string
	// TemporalSPIFFE, when its ServerID is set, connects to Temporal with
	// mutual TLS. The zero value connects in plaintext.
	TemporalSPIFFE TemporalSPIFFE
	// Deployment and BuildID turn Worker Versioning on when both are set.
	// Workflows are then pinned to the build they started on. A versioned
	// worker gets no tasks until its build is made the deployment's current
	// version, which is the consumer's rollout code's job.
	Deployment string
	BuildID    string
	Logger     *slog.Logger
	// Limits configures the shared limits that sessions name. The worker
	// serves the workflow that keeps each limit's state.
	Limits LimitConfig
}

// Worker runs a consumer's workflows and activities.
type Worker struct {
	config   WorkerConfig
	temporal client.Client
	// closeTemporal closes the client and the source of its certificates.
	closeTemporal func() error
	worker        worker.Worker
}

// NewWorker returns a worker that is not connected and not polling.
// Register workflows and activities, then call Run, or Start and Stop.
func NewWorker(config WorkerConfig) (*Worker, error) {
	if config.TaskQueue == "" {
		return nil, errors.New("new worker: a task queue is required")
	}
	if (config.Deployment == "") != (config.BuildID == "") {
		return nil, errors.New("new worker: set both Deployment and BuildID, or neither")
	}
	logger := config.Logger
	if logger == nil {
		logger = slog.Default()
		config.Logger = logger
	}
	if err := config.Limits.validate(); err != nil {
		return nil, fmt.Errorf("new worker: %w", err)
	}
	// The client connects when the worker starts, or on its first call.
	temporalClient, closeTemporal, err := NewTemporalClient(client.Options{
		HostPort:  config.TemporalAddress,
		Namespace: config.TemporalNamespace,
		Logger:    log.NewStructuredLogger(logger),
	}, config.TemporalSPIFFE)
	if err != nil {
		return nil, fmt.Errorf("new worker: configure the Temporal client for %s: %w", config.TemporalAddress, err)
	}
	options := worker.Options{}
	if config.Deployment != "" {
		options.DeploymentOptions = worker.DeploymentOptions{
			UseVersioning:             true,
			Version:                   worker.WorkerDeploymentVersion{DeploymentName: config.Deployment, BuildID: config.BuildID},
			DefaultVersioningBehavior: workflow.VersioningBehaviorPinned,
		}
	}
	w := &Worker{config: config, temporal: temporalClient, closeTemporal: closeTemporal, worker: worker.New(temporalClient, config.TaskQueue, options)}
	limits := &limitActivities{temporal: temporalClient, config: config.Limits, queue: config.TaskQueue}
	w.RegisterWorkflow(limitWorkflowName, limitWorkflow)
	w.RegisterActivity(activityLimitStart, limits.Start)
	w.RegisterActivity(activityLimitGone, limits.Gone)
	return w, nil
}

// RegisterWorkflow registers a workflow function under a name.
func (w *Worker) RegisterWorkflow(name string, function any) {
	w.worker.RegisterWorkflowWithOptions(function, workflow.RegisterOptions{Name: name})
}

// RegisterActivities registers every exported method of a struct as an
// activity, or one function.
func (w *Worker) RegisterActivities(activities any) { w.worker.RegisterActivity(activities) }

// RegisterActivity registers one activity function under a name.
func (w *Worker) RegisterActivity(name string, function any) {
	w.worker.RegisterActivityWithOptions(function, activity.RegisterOptions{Name: name})
}

// RegisterNexusService serves Nexus operations on the worker's existing task queue.
func (w *Worker) RegisterNexusService(service *nexus.Service) {
	w.worker.RegisterNexusService(service)
}

// Temporal is the worker's Temporal client, for starting and signalling
// workflows from the same process.
func (w *Worker) Temporal() client.Client { return w.temporal }

// Start connects to Temporal and begins polling. It returns at once.
func (w *Worker) Start() error {
	if err := w.worker.Start(); err != nil {
		return fmt.Errorf("start worker on %s at %s: %w", w.config.TaskQueue, w.config.TemporalAddress, err)
	}
	return nil
}

// Stop stops polling, waits for running tasks and closes the client.
func (w *Worker) Stop() {
	w.worker.Stop()
	w.close()
}

func (w *Worker) close() {
	if err := w.closeTemporal(); err != nil {
		w.config.Logger.Warn("close the SPIFFE Workload API source", "error", err)
	}
}

// Run starts the worker, waits until ctx ends and stops it.
func (w *Worker) Run(ctx context.Context) error {
	if err := w.Start(); err != nil {
		w.close()
		return err
	}
	<-ctx.Done()
	w.Stop()
	return nil
}
