// Package build is the contract between a tenant's workflows and its image
// builder (protocol7-selfdev docs/image-builder.md).
//
// A PR maker calls Build as a child workflow for its branch head, gets the
// digests back and pins them in its own pull request. The builder registers
// the workflow under WorkflowName on TaskQueue and the activities under the
// Activity names, so callers and the builder agree on names through this
// package.
package build

import (
	"errors"
	"fmt"
	"regexp"
	"time"

	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"
)

// TaskQueue is the queue a tenant's builder polls in the tenant's Temporal namespace.
const TaskQueue = "build"

// WorkflowName is the registered name of the workflow behind Build.
const WorkflowName = "build.Build"

// Activities the builder registers for WorkflowName.
const (
	// ActivityResolveInputs computes the inputs key of a commit from the GitHub trees API.
	ActivityResolveInputs = "build.ResolveInputs"
	// ActivityFindByInputs looks for images tagged with an inputs key in zot.
	ActivityFindByInputs = "build.FindByInputs"
	// ActivityTag adds a tag to images that already exist in zot.
	ActivityTag = "build.Tag"
	// ActivitySolve builds one image on buildkitd and pushes it to zot.
	ActivitySolve = "build.Solve"
)

// Trigger says why a build was requested. It decides the tags the build pushes.
type Trigger string

const (
	// TriggerMain is a push to the configured branch. Only the builder's own push subscriber uses it.
	TriggerMain Trigger = "main"
	// TriggerPullRequest is a build of a branch head, requested by the workflow that pins it.
	TriggerPullRequest Trigger = "pull_request"
)

// Request asks the builder to build every configured image of its repository at one commit.
// The images, packages and Dockerfiles come from the builder's settings, never from the request.
type Request struct {
	// Repository is owner/name, for example "oku-trade/oku-review". It must equal the builder's repository.
	Repository string
	// Commit is the full 40 character lowercase SHA. The builder fetches it, so it must already be pushed.
	Commit string
	// Trigger is TriggerPullRequest for callers outside the builder.
	Trigger Trigger
	// PullRequest is the pull request number, or 0 when the pull request is not opened yet.
	PullRequest int
}

// Image is one image the build produced.
type Image struct {
	// Name is the image's name in the builder's settings and in the manifests' "# p7-image:" markers.
	Name string
	// Package is the repository path without a host, for example "oku-trade/oku-review".
	Package string
	// Digest is "sha256:" followed by 64 lowercase hexadecimal characters.
	Digest string
	// Tag is the tag the build pushed, "main-<commit12>-<name>" or "pr-<number>-<commit12>-<name>".
	Tag string
}

// Reference is what manifests pin. Nodes pull it from zot through the ghcr.io mirror.
func (image Image) Reference() string {
	return "ghcr.io/" + image.Package + "@" + image.Digest
}

// Result is what Build returns.
type Result struct {
	// Commit is the commit that was built.
	Commit string
	// Inputs is the inputs key of Commit.
	Inputs string
	// Images has one entry per configured image, in settings order.
	Images []Image
	// Reused is true when images with the same inputs key already existed and nothing was built.
	Reused bool
	// Duration is the time from the start of the workflow to its result.
	Duration time.Duration
}

// Error types the builder returns as non-retryable application errors.
const (
	// ErrorBuildFailed means a Dockerfile step failed. The message carries the last lines of the log.
	ErrorBuildFailed = "BuildFailed"
	// ErrorNotAllowed means the request was malformed or named another repository.
	ErrorNotAllowed = "NotAllowed"
)

var (
	commitPattern     = regexp.MustCompile(`^[0-9a-f]{40}$`)
	repositoryPattern = regexp.MustCompile(`^[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+$`)
)

// Validate rejects a malformed request before it starts a workflow.
func (request Request) Validate() error {
	if !repositoryPattern.MatchString(request.Repository) {
		return fmt.Errorf("build: repository %q is not owner/name", request.Repository)
	}
	if !commitPattern.MatchString(request.Commit) {
		return fmt.Errorf("build: commit %q is not a full lowercase SHA", request.Commit)
	}
	switch request.Trigger {
	case TriggerMain:
		if request.PullRequest != 0 {
			return errors.New("build: a main build takes no pull request")
		}
	case TriggerPullRequest:
		if request.PullRequest < 0 {
			return fmt.Errorf("build: pull request number %d is negative", request.PullRequest)
		}
	default:
		return fmt.Errorf("build: unknown trigger %q", request.Trigger)
	}
	return nil
}

// BuildOptions are the caller's limits for one build.
type BuildOptions struct {
	// Timeout bounds the child workflow, queueing behind other builds included.
	Timeout time.Duration
}

// Build runs a build as a child workflow of the calling workflow and waits for its result.
// The child's ID includes the parent's ID, so two callers never collide. The builder answers
// requests for inputs it already built from zot without a second build.
func Build(ctx workflow.Context, request Request, options BuildOptions) (Result, error) {
	if err := request.Validate(); err != nil {
		return Result{}, temporal.NewNonRetryableApplicationError(err.Error(), ErrorNotAllowed, err)
	}
	if options.Timeout <= 0 {
		return Result{}, errors.New("build: BuildOptions.Timeout must be positive")
	}
	parent := workflow.GetInfo(ctx).WorkflowExecution.ID
	ctx = workflow.WithChildOptions(ctx, workflow.ChildWorkflowOptions{
		WorkflowID:               fmt.Sprintf("build/%s/%s/%s", request.Repository, request.Commit, parent),
		TaskQueue:                TaskQueue,
		WorkflowExecutionTimeout: options.Timeout,
	})
	var result Result
	if err := workflow.ExecuteChildWorkflow(ctx, WorkflowName, request).Get(ctx, &result); err != nil {
		return Result{}, fmt.Errorf("build %s at %s: %w", request.Repository, request.Commit, err)
	}
	return result, nil
}
