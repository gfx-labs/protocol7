package build_test

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/testsuite"
	"go.temporal.io/sdk/workflow"

	"github.com/gfx-labs/protocol7/pkg/orchestration/build"
)

const (
	commit = "0123456789abcdef0123456789abcdef01234567"
	digest = "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
)

// builder stands in for the builder's workflow: it is registered under the
// contract's names and runs the Solve activity by name.
func builder(ctx workflow.Context, request build.Request) (build.Result, error) {
	ctx = workflow.WithActivityOptions(ctx, workflow.ActivityOptions{StartToCloseTimeout: time.Minute})
	var image build.Image
	if err := workflow.ExecuteActivity(ctx, build.ActivitySolve, request).Get(ctx, &image); err != nil {
		return build.Result{}, err
	}
	return build.Result{Commit: request.Commit, Images: []build.Image{image}}, nil
}

func solve(_ context.Context, request build.Request) (build.Image, error) {
	if request.Commit == strings.Repeat("f", 40) {
		return build.Image{}, temporal.NewNonRetryableApplicationError("RUN go build: exit 1", build.ErrorBuildFailed, nil)
	}
	return build.Image{Name: "reviewer", Package: "oku-trade/oku-review", Digest: digest, Tag: "pr-7-" + request.Commit[:12]}, nil
}

// caller is a PR maker. It reports the error type it sees, as a PR maker
// branches on it inside its own workflow.
func caller(ctx workflow.Context, request build.Request) (build.Result, error) {
	result, err := build.Build(ctx, request, build.BuildOptions{Timeout: 30 * time.Minute})
	var applicationError *temporal.ApplicationError
	if errors.As(err, &applicationError) {
		return build.Result{}, temporal.NewNonRetryableApplicationError("caller saw", applicationError.Type(), nil)
	}
	return result, err
}

func run(t *testing.T, request build.Request) (build.Result, error) {
	t.Helper()
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	env.RegisterWorkflow(caller)
	env.RegisterWorkflowWithOptions(builder, workflow.RegisterOptions{Name: build.WorkflowName})
	env.RegisterActivityWithOptions(solve, activity.RegisterOptions{Name: build.ActivitySolve})
	env.ExecuteWorkflow(caller, request)
	if !env.IsWorkflowCompleted() {
		t.Fatal("caller did not complete")
	}
	var result build.Result
	err := env.GetWorkflowError()
	if err == nil {
		if decodeErr := env.GetWorkflowResult(&result); decodeErr != nil {
			t.Fatal(decodeErr)
		}
	}
	return result, err
}

func TestCallerGetsDigest(t *testing.T) {
	result, err := run(t, build.Request{Repository: "oku-trade/oku-review", Commit: commit, Trigger: build.TriggerPullRequest, PullRequest: 7})
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Images) != 1 || result.Images[0].Digest != digest || result.Commit != commit {
		t.Fatalf("result %+v", result)
	}
	if got, want := result.Images[0].Reference(), "ghcr.io/oku-trade/oku-review@"+digest; got != want {
		t.Fatalf("reference %q, want %q", got, want)
	}
}

// A failed Dockerfile step reaches the caller with its error type, so a PR
// maker can tell a broken build from an outage.
func TestCallerSeesBuildFailure(t *testing.T) {
	_, err := run(t, build.Request{Repository: "oku-trade/oku-review", Commit: strings.Repeat("f", 40), Trigger: build.TriggerPullRequest})
	var applicationError *temporal.ApplicationError
	if !errors.As(err, &applicationError) || applicationError.Type() != build.ErrorBuildFailed {
		t.Fatalf("error %v, want %s", err, build.ErrorBuildFailed)
	}
}

// A malformed request fails in the caller without starting the builder.
func TestMalformedRequestIsNotAllowed(t *testing.T) {
	_, err := run(t, build.Request{Repository: "oku-trade/oku-review", Commit: "HEAD", Trigger: build.TriggerPullRequest})
	var applicationError *temporal.ApplicationError
	if !errors.As(err, &applicationError) || applicationError.Type() != build.ErrorNotAllowed {
		t.Fatalf("error %v, want %s", err, build.ErrorNotAllowed)
	}
}
