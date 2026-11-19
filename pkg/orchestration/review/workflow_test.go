package review_test

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

	"github.com/gfx-labs/protocol7/pkg/domain"
	"github.com/gfx-labs/protocol7/pkg/orchestration/review"
)

// fakes stand in for GitHub and for the core worker's session activities.
type fakes struct {
	// pulls is what the next reads return. The last one repeats.
	pulls []review.Pull
	reads int
	// verdicts is the reviewer's answer to each review, in order.
	verdicts []string
	// answers is the text of each verdict turn, when verdicts does not decide it.
	opened    []domain.SessionRequest
	turns     []domain.TurnRequest
	closed    int
	published []review.PublishInput
	merges    []review.MergeInput
	// mergeErr is what the first merge fails with.
	mergeErr  error
	verdictAt int
}

func (f *fakes) pull(_ context.Context, _ string, _ int) (review.Pull, error) {
	index := f.reads
	if index >= len(f.pulls) {
		index = len(f.pulls) - 1
	}
	f.reads++
	return f.pulls[index], nil
}

func (f *fakes) publish(_ context.Context, input review.PublishInput) error {
	f.published = append(f.published, input)
	return nil
}

func (f *fakes) merge(_ context.Context, input review.MergeInput) error {
	f.merges = append(f.merges, input)
	if len(f.merges) == 1 {
		return f.mergeErr
	}
	return nil
}

func (f *fakes) open(_ context.Context, request domain.SessionRequest) (domain.SessionHandle, error) {
	f.opened = append(f.opened, request)
	return domain.SessionHandle{ID: request.ID, TaskQueue: "local-test"}, nil
}

func (f *fakes) turn(_ context.Context, request domain.TurnRequest) (domain.TurnResult, error) {
	f.turns = append(f.turns, request)
	text := "I read it."
	if strings.HasPrefix(request.Prompt, "Now give your verdict") || strings.HasPrefix(request.Prompt, "That was not") {
		text = f.verdicts[f.verdictAt]
		f.verdictAt++
	}
	return domain.TurnResult{Turn: request.Turn, StopReason: "end_turn", Text: text}, nil
}

func (f *fakes) close(context.Context, domain.CloseRequest) error { f.closed++; return nil }

func verdict(decision string) string {
	return "```review-verdict\n{\"verdict\":\"" + decision + "\",\"summary\":\"because\"}\n```"
}

func environment(t *testing.T, f *fakes) *testsuite.TestWorkflowEnvironment {
	t.Helper()
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	env.RegisterWorkflowWithOptions(review.Workflow, workflow.RegisterOptions{Name: review.WorkflowName})
	env.RegisterActivityWithOptions(f.pull, activity.RegisterOptions{Name: review.ActivityPull})
	env.RegisterActivityWithOptions(f.publish, activity.RegisterOptions{Name: review.ActivityPublish})
	env.RegisterActivityWithOptions(f.merge, activity.RegisterOptions{Name: review.ActivityMerge})
	env.RegisterActivityWithOptions(f.open, activity.RegisterOptions{Name: domain.ActivityOpenSession})
	env.RegisterActivityWithOptions(f.turn, activity.RegisterOptions{Name: domain.ActivityRunTurn})
	env.RegisterActivityWithOptions(f.close, activity.RegisterOptions{Name: domain.ActivityCloseSession})
	return env
}

func request(merge bool) review.State {
	return review.State{Request: review.Request{Repo: "gfx-labs/r", Number: 3, Merge: merge, Host: "test"}}
}

func final(t *testing.T, env *testsuite.TestWorkflowEnvironment) review.State {
	t.Helper()
	value, err := env.QueryWorkflow(review.QueryState)
	if err != nil {
		t.Fatal(err)
	}
	var state review.State
	if err := value.Get(&state); err != nil {
		t.Fatal(err)
	}
	return state
}

// An approved pull request that passes the criteria is reviewed once, the
// verdict is published, and the pull request is merged at the reviewed head.
func TestAnApprovedPullRequestIsPublishedAndMerged(t *testing.T) {
	green := chatPull()
	f := &fakes{pulls: []review.Pull{green}, verdicts: []string{verdict("approve")}}
	env := environment(t, f)
	env.ExecuteWorkflow(review.WorkflowName, request(true))

	if err := env.GetWorkflowError(); err != nil {
		t.Fatal(err)
	}
	if len(f.opened) != 1 || f.opened[0].Inputs[0].Ref != "h1" || f.opened[0].Inputs[0].Destination != "/repo" {
		t.Errorf("opened %+v, want one session with /repo at h1", f.opened)
	}
	if len(f.turns) != 2 || f.turns[0].Context == "" || f.turns[1].Context != "" {
		t.Errorf("turns %+v, want a review with instructions and a verdict turn", f.turns)
	}
	if len(f.published) != 1 || f.published[0].Head != "h1" || f.published[0].Verdict.Decision != review.Approve {
		t.Errorf("published %+v, want the approval of h1", f.published)
	}
	if len(f.merges) != 1 || f.merges[0].Head != "h1" {
		t.Errorf("merges %+v, want one at h1", f.merges)
	}
	if state := final(t, env); state.Status != review.StatusMerged || f.closed != 1 {
		t.Errorf("status %s, closed %d times, want merged and once", state.Status, f.closed)
	}
}

// Without the merge request the review stops at the approval.
func TestWithoutMergeTheReviewStopsAtTheVerdict(t *testing.T) {
	f := &fakes{pulls: []review.Pull{chatPull()}, verdicts: []string{verdict("approve")}}
	env := environment(t, f)
	env.ExecuteWorkflow(review.WorkflowName, request(false))

	if err := env.GetWorkflowError(); err != nil {
		t.Fatal(err)
	}
	if state := final(t, env); state.Status != review.StatusApproved || len(f.merges) != 0 {
		t.Errorf("status %s with %d merges, want approved and none", state.Status, len(f.merges))
	}
}

// A change request waits. A push makes a new head, which is reviewed, and the
// approval of that head merges. Nothing merges on the first verdict.
func TestAPushAfterChangesAreRequestedIsReviewedAgain(t *testing.T) {
	first, second := chatPull(), chatPull()
	second.Head = "h2"
	f := &fakes{pulls: []review.Pull{first, first, second}, verdicts: []string{verdict("request_changes"), verdict("approve")}}
	env := environment(t, f)
	env.ExecuteWorkflow(review.WorkflowName, request(true))

	if err := env.GetWorkflowError(); err != nil {
		t.Fatal(err)
	}
	if len(f.published) != 2 || f.published[0].Verdict.Decision != review.RequestChanges || f.published[1].Head != "h2" {
		t.Errorf("published %+v, want the change request at h1 and then the approval at h2", f.published)
	}
	if len(f.merges) != 1 || f.merges[0].Head != "h2" {
		t.Errorf("merges %+v, want one at h2", f.merges)
	}
	if len(f.opened) != 2 || f.opened[1].Inputs[0].Ref != "h2" {
		t.Errorf("sessions %+v, want one per head", f.opened)
	}
}

// An approved pull request waits while a check runs, and merges when it ends.
func TestMergeWaitsForChecks(t *testing.T) {
	running, done := chatPull(), chatPull()
	running.Checks.Pending = 1
	f := &fakes{pulls: []review.Pull{running, running, done}, verdicts: []string{verdict("approve")}}
	env := environment(t, f)
	env.ExecuteWorkflow(review.WorkflowName, request(true))

	if err := env.GetWorkflowError(); err != nil {
		t.Fatal(err)
	}
	if len(f.merges) != 1 || len(f.opened) != 1 {
		t.Errorf("%d merges and %d reviews, want one of each: a wait does not review again", len(f.merges), len(f.opened))
	}
}

// A failed check blocks the merge, and the workflow says why and waits for a
// push or a person.
func TestAFailedCheckBlocksTheMerge(t *testing.T) {
	failed := chatPull()
	failed.Checks.Failed = 1
	f := &fakes{pulls: []review.Pull{failed}, verdicts: []string{verdict("approve")}}
	env := environment(t, f)
	env.SetTestTimeout(time.Hour)
	env.RegisterDelayedCallback(func() { env.CancelWorkflow() }, 30*time.Minute)
	env.ExecuteWorkflow(review.WorkflowName, request(true))

	if len(f.merges) != 0 {
		t.Errorf("merged %+v, want no merge", f.merges)
	}
	if state := final(t, env); state.Status != review.StatusBlocked || !strings.Contains(state.Blocked, "failed") {
		t.Errorf("status %s, blocked %q, want blocked with the failed check as the reason", state.Status, state.Blocked)
	}
}

// A merge that GitHub refuses because the head moved is not a failure: the
// next pass reads the new head and reviews it.
func TestAMergeRefusedForAMovedHeadIsReviewedAgain(t *testing.T) {
	first, second := chatPull(), chatPull()
	second.Head = "h2"
	f := &fakes{pulls: []review.Pull{first, second}, verdicts: []string{verdict("approve"), verdict("approve")}}
	f.mergeErr = temporal.NewNonRetryableApplicationError("moved", review.ErrorHeadChanged, nil)
	env := environment(t, f)
	env.ExecuteWorkflow(review.WorkflowName, request(true))

	if err := env.GetWorkflowError(); err != nil {
		t.Fatal(err)
	}
	if len(f.opened) != 2 {
		t.Errorf("%d reviews, want two: the new head is reviewed", len(f.opened))
	}
}

// A closed pull request ends the review without a session.
func TestAClosedPullRequestEndsTheReview(t *testing.T) {
	closed := chatPull()
	closed.Open = false
	f := &fakes{pulls: []review.Pull{closed}}
	env := environment(t, f)
	env.ExecuteWorkflow(review.WorkflowName, request(true))

	if err := env.GetWorkflowError(); err != nil {
		t.Fatal(err)
	}
	if state := final(t, env); state.Status != review.StatusClosed || len(f.opened) != 0 {
		t.Errorf("status %s with %d sessions, want closed and none", state.Status, len(f.opened))
	}
}

// A reviewer that does not give a valid verdict twice is not trusted: no
// review is published and nothing merges.
func TestAReviewerWithoutAVerdictPublishesNothing(t *testing.T) {
	f := &fakes{pulls: []review.Pull{chatPull()}, verdicts: []string{"looks fine", "yes", "looks fine", "yes", "a", "b", "c", "d", "e", "f", "g", "h"}}
	env := environment(t, f)
	env.RegisterDelayedCallback(func() { env.CancelWorkflow() }, 2*time.Hour)
	env.ExecuteWorkflow(review.WorkflowName, request(true))

	if len(f.published) != 0 || len(f.merges) != 0 {
		t.Errorf("published %d and merged %d, want neither", len(f.published), len(f.merges))
	}
	if state := final(t, env); state.Rounds != review.MaxRounds && state.Error == "" {
		t.Errorf("rounds %d, error %q, want the error kept", state.Rounds, state.Error)
	}
}

// A run continues as new when the deployment's target version changed, and
// asks to start on it. It does not when nothing changed, and Temporal's own
// suggestion continues as new without the upgrade.
func TestTheReviewContinuesAsNewWhenTheTargetVersionChanges(t *testing.T) {
	for _, test := range []struct {
		name               string
		suggested, changed bool
		want               workflow.ContinueAsNewVersioningBehavior
		continues          bool
	}{
		{"unchanged", false, false, 0, false},
		{"suggested", true, false, workflow.ContinueAsNewVersioningBehaviorUnspecified, true},
		{"target changed", false, true, workflow.ContinueAsNewVersioningBehaviorAutoUpgrade, true},
	} {
		t.Run(test.name, func(t *testing.T) {
			f := &fakes{pulls: []review.Pull{chatPull()}, verdicts: []string{verdict("approve")}}
			env := environment(t, f)
			env.SetContinueAsNewSuggested(test.suggested)
			env.SetTargetWorkerDeploymentVersionChanged(test.changed)
			env.ExecuteWorkflow(review.WorkflowName, request(true))

			err := env.GetWorkflowError()
			var continued *workflow.ContinueAsNewError
			if got := errors.As(err, &continued); got != test.continues {
				t.Fatalf("continued as new = %v (%v), want %v", got, err, test.continues)
			}
			if !test.continues {
				if len(f.merges) != 1 {
					t.Errorf("merges %+v, want the review to run to the merge", f.merges)
				}
				return
			}
			if continued.InitialVersioningBehavior != test.want {
				t.Errorf("versioning behavior %v, want %v", continued.InitialVersioningBehavior, test.want)
			}
			if len(f.merges) != 0 || len(f.opened) != 0 {
				t.Errorf("opened %d sessions and merged %d times, want the run to continue before any work", len(f.opened), len(f.merges))
			}
		})
	}
}
