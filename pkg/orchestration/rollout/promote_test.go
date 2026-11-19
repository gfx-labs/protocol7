package rollout_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"go.temporal.io/api/serviceerror"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/worker"

	"github.com/gfx-labs/protocol7/pkg/orchestration/rollout"
)

type fake struct {
	describes []func() (client.WorkerDeploymentDescribeResponse, error)
	calls     int
	set       []client.WorkerDeploymentSetCurrentVersionOptions
	setErr    error
}

func (f *fake) Describe(context.Context, client.WorkerDeploymentDescribeOptions) (client.WorkerDeploymentDescribeResponse, error) {
	index := min(f.calls, len(f.describes)-1)
	f.calls++
	return f.describes[index]()
}

func (f *fake) SetCurrentVersion(_ context.Context, options client.WorkerDeploymentSetCurrentVersionOptions) (client.WorkerDeploymentSetCurrentVersionResponse, error) {
	f.set = append(f.set, options)
	return client.WorkerDeploymentSetCurrentVersionResponse{}, f.setErr
}

func described(current string, builds ...string) func() (client.WorkerDeploymentDescribeResponse, error) {
	return func() (client.WorkerDeploymentDescribeResponse, error) {
		var response client.WorkerDeploymentDescribeResponse
		response.ConflictToken = []byte("token")
		for _, build := range builds {
			response.Info.VersionSummaries = append(response.Info.VersionSummaries, client.WorkerDeploymentVersionSummary{Version: worker.WorkerDeploymentVersion{DeploymentName: "selfdev", BuildID: build}})
		}
		if current != "" {
			response.Info.RoutingConfig.CurrentVersion = &worker.WorkerDeploymentVersion{DeploymentName: "selfdev", BuildID: current}
		}
		return response, nil
	}
}

func promoter(f *fake) *rollout.Promoter {
	return &rollout.Promoter{Deployment: func(string) rollout.Handle { return f }, Retry: time.Millisecond}
}

// The build is made current once the deployment lists it, and the call carries
// the token that Describe returned, so another change in between is refused.
func TestPromoteWaitsForTheBuildToPoll(t *testing.T) {
	f := &fake{describes: []func() (client.WorkerDeploymentDescribeResponse, error){
		func() (client.WorkerDeploymentDescribeResponse, error) {
			return client.WorkerDeploymentDescribeResponse{}, serviceerror.NewNotFound("no deployment")
		},
		described("old", "old"),
		described("old", "old", "new"),
	}}
	if err := promoter(f).Promote(context.Background(), "selfdev", "new"); err != nil {
		t.Fatal(err)
	}
	if len(f.set) != 1 || f.set[0].BuildID != "new" || string(f.set[0].ConflictToken) != "token" {
		t.Errorf("set current %+v, want one call for new with the token", f.set)
	}
}

func TestPromoteLeavesTheCurrentBuildAlone(t *testing.T) {
	f := &fake{describes: []func() (client.WorkerDeploymentDescribeResponse, error){described("new", "old", "new")}}
	if err := promoter(f).Promote(context.Background(), "selfdev", "new"); err != nil {
		t.Fatal(err)
	}
	if len(f.set) != 0 {
		t.Errorf("set current %+v, want no call", f.set)
	}
}

// Temporal's refusal (no pollers, a changed token) is tried again until the
// context ends.
func TestPromoteRetriesARefusalUntilCancelled(t *testing.T) {
	f := &fake{describes: []func() (client.WorkerDeploymentDescribeResponse, error){described("old", "old", "new")}, setErr: errors.New("no pollers")}
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	if err := promoter(f).Promote(ctx, "selfdev", "new"); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Promote returned %v, want the context's error", err)
	}
	if len(f.set) < 2 {
		t.Errorf("tried %d times, want it to retry", len(f.set))
	}
}
