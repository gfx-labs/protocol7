package upgrade_test

import (
	"errors"
	"testing"

	"go.temporal.io/sdk/testsuite"
	"go.temporal.io/sdk/workflow"

	"github.com/gfx-labs/protocol7/pkg/orchestration/upgrade"
)

type state struct{ N int }

// next continues as new when it is due, with the next number.
func next(ctx workflow.Context, s state) (state, error) {
	if upgrade.Due(ctx) {
		s.N++
		return s, upgrade.ContinueAsNew(ctx, "next", s)
	}
	return s, nil
}

func TestContinueAsNewUpgradesOnlyWhenTheTargetChanged(t *testing.T) {
	for _, test := range []struct {
		name      string
		suggested bool
		changed   bool
		continues bool
		behavior  workflow.ContinueAsNewVersioningBehavior
	}{
		{"nothing changed", false, false, false, 0},
		{"temporal suggests it", true, false, true, workflow.ContinueAsNewVersioningBehaviorUnspecified},
		{"the target version changed", false, true, true, workflow.ContinueAsNewVersioningBehaviorAutoUpgrade},
		{"both", true, true, true, workflow.ContinueAsNewVersioningBehaviorAutoUpgrade},
	} {
		t.Run(test.name, func(t *testing.T) {
			var suite testsuite.WorkflowTestSuite
			env := suite.NewTestWorkflowEnvironment()
			env.RegisterWorkflowWithOptions(next, workflow.RegisterOptions{Name: "next"})
			env.SetContinueAsNewSuggested(test.suggested)
			env.SetTargetWorkerDeploymentVersionChanged(test.changed)
			env.ExecuteWorkflow("next", state{})
			err := env.GetWorkflowError()
			var continued *workflow.ContinueAsNewError
			if got := errors.As(err, &continued); got != test.continues {
				t.Fatalf("continued as new = %v (%v), want %v", got, err, test.continues)
			}
			if !test.continues {
				return
			}
			if continued.InitialVersioningBehavior != test.behavior {
				t.Errorf("versioning behavior %v, want %v", continued.InitialVersioningBehavior, test.behavior)
			}
			if continued.WorkflowType.Name != "next" {
				t.Errorf("continues as %q, want next", continued.WorkflowType.Name)
			}
		})
	}
}
