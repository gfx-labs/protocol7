package p7

import (
	"context"
	"errors"
	"fmt"
	"log/slog"

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
	// Deployment and BuildID turn Worker Versioning on when both are set.
	// Workflows are then pinned to the build they started on. A versioned
	// worker gets no tasks until its build is made the deployment's current
	// version, which is the consumer's rollout code's job.
	Deployment string
	BuildID    string
	Logger     *slog.Logger
}

// Worker runs a consumer's workflows and activities.
type Worker struct {
	config   WorkerConfig
	temporal client.Client
	worker   worker.Worker
}

// NewWorker connects to Temporal and returns a worker that is not yet
// polling. Register workflows and activities, then call Run.
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
	}
	temporalClient, err := client.Dial(client.Options{
		HostPort:  config.TemporalAddress,
		Namespace: config.TemporalNamespace,
		Logger:    log.NewStructuredLogger(logger),
	})
	if err != nil {
		return nil, fmt.Errorf("new worker: connect to Temporal at %s: %w", config.TemporalAddress, err)
	}
	options := worker.Options{}
	if config.Deployment != "" {
		options.DeploymentOptions = worker.DeploymentOptions{
			UseVersioning:             true,
			Version:                   worker.WorkerDeploymentVersion{DeploymentName: config.Deployment, BuildID: config.BuildID},
			DefaultVersioningBehavior: workflow.VersioningBehaviorPinned,
		}
	}
	return &Worker{config: config, temporal: temporalClient, worker: worker.New(temporalClient, config.TaskQueue, options)}, nil
}

// RegisterWorkflow registers a workflow function under a name.
func (w *Worker) RegisterWorkflow(name string, function any) {
	w.worker.RegisterWorkflowWithOptions(function, workflow.RegisterOptions{Name: name})
}

// RegisterActivities registers every exported method of a struct as an
// activity, or one function.
func (w *Worker) RegisterActivities(activities any) { w.worker.RegisterActivity(activities) }

// Temporal is the worker's Temporal client, for starting and signalling
// workflows from the same process.
func (w *Worker) Temporal() client.Client { return w.temporal }

// Run polls until ctx ends, then stops the worker and closes its client.
func (w *Worker) Run(ctx context.Context) error {
	defer w.temporal.Close()
	if err := w.worker.Start(); err != nil {
		return fmt.Errorf("start worker on %s: %w", w.config.TaskQueue, err)
	}
	<-ctx.Done()
	w.worker.Stop()
	return nil
}
