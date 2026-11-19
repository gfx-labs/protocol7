// Package rollout makes a worker build the deployment's current version.
//
// A versioned worker (p7.WorkerConfig.Deployment and BuildID) gets no new
// workflows until its build is the deployment's current version. selfdev's
// rollout is Git: the pod that runs the newest image promotes its own build
// once it polls, and an older build keeps running in a pod of its own until
// its pinned runs end (kubernetes/README.md, "Worker versioning").
package rollout

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"go.temporal.io/api/serviceerror"
	"go.temporal.io/sdk/client"
)

// Handle is the part of client.WorkerDeploymentHandle that Promote calls.
type Handle interface {
	Describe(ctx context.Context, options client.WorkerDeploymentDescribeOptions) (client.WorkerDeploymentDescribeResponse, error)
	SetCurrentVersion(ctx context.Context, options client.WorkerDeploymentSetCurrentVersionOptions) (client.WorkerDeploymentSetCurrentVersionResponse, error)
}

// Promoter promotes a build.
type Promoter struct {
	// Deployment returns the handle of a worker deployment.
	Deployment func(name string) Handle
	Logger     *slog.Logger
	// Retry is the wait between attempts. Default: five seconds.
	Retry time.Duration
	// Identity is recorded as the one that changed the deployment.
	Identity string
}

// FromClient returns a Promoter that uses Temporal's worker deployment API.
func FromClient(c client.Client, logger *slog.Logger) *Promoter {
	return &Promoter{
		Deployment: func(name string) Handle { return c.WorkerDeploymentClient().GetHandle(name) },
		Logger:     logger,
	}
}

// Promote makes buildID the current version of deployment, and returns when it
// is, or when ctx ends. It keeps trying while the version does not exist yet
// (its worker has not polled) or Temporal refuses for a reason that a poll
// fixes, such as a version with no pollers. Temporal refuses to make a version
// current that has no pollers, and that is what keeps a build that does not
// start from taking the traffic. A version that is current already is left
// alone.
func (p *Promoter) Promote(ctx context.Context, deployment, buildID string) error {
	retry := p.Retry
	if retry <= 0 {
		retry = 5 * time.Second
	}
	logger := p.Logger
	if logger == nil {
		logger = slog.Default()
	}
	handle := p.Deployment(deployment)
	for {
		done, err := p.attempt(ctx, handle, buildID)
		if done {
			logger.Info("worker build is the current version", "deployment", deployment, "build_id", buildID)
			return nil
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
		logger.Info("worker build is not current yet", "deployment", deployment, "build_id", buildID, "reason", err)
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(retry):
		}
	}
}

var errNotRegistered = errors.New("the deployment does not list the build yet")

func (p *Promoter) attempt(ctx context.Context, handle Handle, buildID string) (bool, error) {
	described, err := handle.Describe(ctx, client.WorkerDeploymentDescribeOptions{})
	if err != nil {
		var notFound *serviceerror.NotFound
		if errors.As(err, &notFound) {
			// The first worker of the deployment has not polled yet.
			return false, errNotRegistered
		}
		return false, fmt.Errorf("describe the deployment: %w", err)
	}
	if current := described.Info.RoutingConfig.CurrentVersion; current != nil && current.BuildID == buildID {
		return true, nil
	}
	listed := false
	for _, summary := range described.Info.VersionSummaries {
		if summary.Version.BuildID == buildID {
			listed = true
		}
	}
	if !listed {
		return false, errNotRegistered
	}
	_, err = handle.SetCurrentVersion(ctx, client.WorkerDeploymentSetCurrentVersionOptions{
		BuildID: buildID, ConflictToken: described.ConflictToken, Identity: p.Identity,
	})
	if err != nil {
		return false, fmt.Errorf("set the current version: %w", err)
	}
	return true, nil
}
