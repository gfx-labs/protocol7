package review

import (
	"errors"
	"fmt"
	"time"

	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"

	"github.com/gfx-labs/protocol7/pkg/orchestration/upgrade"
	"github.com/gfx-labs/protocol7/pkg/p7"
	"github.com/gfx-labs/protocol7/pkg/transport/githubapi"
)

// Names of the review activities. They are methods of Activities, which the
// selfdev worker registers.
const (
	ActivityPull    = "ReviewPull"
	ActivityPublish = "ReviewPublish"
	ActivityMerge   = "ReviewMerge"
)

// Error types of the activities. The workflow reads them to tell a merge that
// GitHub refused from a call that failed.
const (
	ErrorNotMergeable = "NotMergeable"
	ErrorHeadChanged  = "HeadChanged"
)

// Intervals of a waiting review. A pull request waiting for a person is read
// less often than one waiting for its checks.
const (
	slowPoll = 5 * time.Minute
	// Lifetime is how long a review lasts before it ends as blocked.
	Lifetime = 7 * 24 * time.Hour
)

// PublishInput is the input of ActivityPublish.
type PublishInput struct {
	Repo    string  `json:"repo"`
	Number  int     `json:"number"`
	Head    string  `json:"head"`
	Verdict Verdict `json:"verdict"`
}

// MergeInput is the input of ActivityMerge. GitHub merges only if the head is
// still Head.
type MergeInput struct {
	Repo   string `json:"repo"`
	Number int    `json:"number"`
	Head   string `json:"head"`
}

// Workflow is the review of one pull request. Each pass reads the pull request
// and does the one thing its state calls for: review a head that has no
// verdict, publish a verdict, merge, or wait. A wake only means look again, so
// a push that lands during a review is simply the next pass.
func Workflow(ctx workflow.Context, state State) error {
	if err := workflow.SetQueryHandler(ctx, QueryState, func() (State, error) { return state, nil }); err != nil {
		return err
	}
	workflow.Go(ctx, func(ctx workflow.Context) {
		signals := workflow.GetSignalChannel(ctx, SignalRereview)
		for {
			signals.Receive(ctx, nil)
			state.Rereview = true
		}
	})
	if state.Started.IsZero() {
		state.Started = workflow.Now(ctx)
	}
	if state.Status == "" {
		state.Status = StatusReading
	}

	for {
		if upgrade.Due(ctx) {
			return upgrade.ContinueAsNew(ctx, WorkflowName, state)
		}
		if workflow.Now(ctx).Sub(state.Started) > Lifetime {
			state.Status, state.Blocked = StatusBlocked, "the review ran for a week without the pull request merging or closing"
			return nil
		}

		var pull Pull
		pullCtx := workflow.WithActivityOptions(ctx, readOptions)
		if err := workflow.ExecuteActivity(pullCtx, ActivityPull, state.Request.Repo, state.Request.Number).Get(ctx, &pull); err != nil {
			if ctx.Err() != nil {
				// Cancelled: the review ends with the status it had.
				return ctx.Err()
			}
			state.Status, state.Error = StatusFailed, cause(err)
			return err
		}
		switch {
		case pull.Merged:
			state.Status = StatusMerged
			return nil
		case !pull.Open:
			state.Status = StatusClosed
			return nil
		}
		if pull.Head != state.Head {
			state.Head, state.HeadSeen = pull.Head, workflow.Now(ctx)
		}
		if pull.Draft {
			state.Status = StatusDraft
			wait(ctx, &state, slowPoll)
			continue
		}

		// A head with no verdict gets a review. So does a head that a person
		// asked about again.
		if state.Verdict == nil || state.Reviewed != pull.Head || state.Rereview {
			state.Rereview = false
			if state.Rounds >= MaxRounds {
				state.Status, state.Blocked = StatusBlocked, fmt.Sprintf("%d reviews were made and the pull request still has no approval", state.Rounds)
				return nil
			}
			state.Rounds++
			verdict, err := reviewHead(ctx, &state, pull)
			if err != nil {
				// The next pass tries again, until the rounds are used up.
				state.Error = cause(err)
				wait(ctx, &state, slowPoll)
				continue
			}
			state.Error, state.Verdict, state.Reviewed = "", &verdict, pull.Head
			state.MergeFailures = 0
		}

		verdict := *state.Verdict
		if published := pull.Head + ":" + verdict.Decision; state.Published != published {
			state.Status = StatusPublish
			publishCtx := workflow.WithActivityOptions(ctx, writeOptions)
			input := PublishInput{Repo: state.Request.Repo, Number: state.Request.Number, Head: pull.Head, Verdict: verdict}
			if err := workflow.ExecuteActivity(publishCtx, ActivityPublish, input).Get(ctx, nil); err != nil {
				state.Status, state.Error = StatusFailed, cause(err)
				return err
			}
			state.Published = published
		}

		if verdict.Decision != Approve {
			state.Status, state.Blocked = StatusChanges, ""
			if verdict.Decision == Comment {
				state.Status, state.Blocked = StatusBlocked, "the reviewer could not judge the change: "+verdict.Summary
			}
			// A push gives a new head, which the next pass reviews.
			wait(ctx, &state, slowPoll)
			continue
		}
		if !state.Request.Merge {
			state.Status = StatusApproved
			return nil
		}
		if !CanMerge(state.Request) {
			// The person who filed it may not merge here, whatever the review
			// says. The pull request stays approved for someone who may.
			state.Status, state.Blocked = StatusApproved, "not merged: the scope of whoever filed the review does not allow a merge into "+state.Request.Repo
			return nil
		}

		action, reason := Decide(pull, workflow.Now(ctx).Sub(state.HeadSeen))
		switch action {
		case ActionWait:
			state.Status, state.Blocked = StatusWaiting, reason
			wait(ctx, &state, PollEvery)
		case ActionBlock:
			state.Status, state.Blocked = StatusBlocked, reason
			wait(ctx, &state, slowPoll)
		case ActionMerge:
			state.Blocked = ""
			mergeCtx := workflow.WithActivityOptions(ctx, writeOptions)
			input := MergeInput{Repo: state.Request.Repo, Number: state.Request.Number, Head: pull.Head}
			err := workflow.ExecuteActivity(mergeCtx, ActivityMerge, input).Get(ctx, nil)
			switch {
			case err == nil:
				state.Status = StatusMerged
				return nil
			case isRefusal(err) && state.MergeFailures < MaxMergeFailures:
				// A push or a conflict landed between the read and the merge.
				// The next pass reads again.
				state.MergeFailures++
				state.Status, state.Error = StatusWaiting, cause(err)
				wait(ctx, &state, PollEvery)
			case isRefusal(err):
				state.Status, state.Blocked = StatusBlocked, "GitHub refused the merge: "+cause(err)
				return nil
			default:
				state.Status, state.Error = StatusFailed, cause(err)
				return err
			}
		}
	}
}

// wait sleeps until the interval ends or a rereview signal comes. A cancelled
// workflow stops sleeping, and the next pass of the loop ends the workflow
// through its first activity.
func wait(ctx workflow.Context, state *State, interval time.Duration) {
	_, _ = workflow.AwaitWithTimeout(ctx, interval, func() bool { return state.Rereview })
}

// placement is where the reviewer's session runs: in a pod of its own on the
// kubernetes backend, and otherwise on the host's core worker (chat.Placement
// does the same for a chat).
func placement(backend, host string) p7.Placement {
	if backend == p7.BackendKubernetes {
		return p7.Placement{Backend: p7.BackendKubernetes}
	}
	return p7.Placement{Backend: p7.BackendHost, Host: host}
}

// reviewHead runs one reviewer session on the pull request's head and returns
// its verdict.
func reviewHead(ctx workflow.Context, state *State, pull Pull) (Verdict, error) {
	state.Status = StatusReviewing
	request := state.Request
	session, err := p7.OpenSession(ctx, p7.SessionSpec{
		Models:    request.Models,
		Placement: placement(request.Backend, request.Host),
		Inputs: []p7.Input{{
			Source:      p7.GitClone{Repository: "https://github.com/" + request.Repo + ".git", Ref: pull.Head},
			Destination: "/repo",
		}},
		WorkingDirectory: "/repo",
		OpenRetry:        &temporal.RetryPolicy{InitialInterval: 5 * time.Second, BackoffCoefficient: 2, MaximumAttempts: 3},
		IdleLimit:        10 * time.Minute,
	})
	if err != nil {
		return Verdict{}, err
	}
	// The review may be ending because the workflow was cancelled. The
	// session is closed all the same.
	defer func() {
		closeCtx, _ := workflow.NewDisconnectedContext(ctx)
		_ = session.Close(closeCtx)
	}()

	turn := func(id, text, context string) (p7.Turn, error) {
		result, err := p7.PromptText(ctx, session, p7.TextPrompt{
			Text: text, Context: context, TurnID: fmt.Sprintf("review-%d-%s", state.Rounds, id),
			Retry: &temporal.RetryPolicy{
				InitialInterval: 2 * time.Second, BackoffCoefficient: 2, MaximumInterval: time.Minute, MaximumAttempts: 10,
				NonRetryableErrorTypes: []string{p7.ErrorHarnessStopped},
			},
		})
		if err == nil && result.StopReason != "end_turn" {
			err = fmt.Errorf("the reviewer's turn ended as %q", result.StopReason)
		}
		return result, err
	}
	if _, err := turn("read", reviewPrompt(pull), instructions(request, pull)); err != nil {
		return Verdict{}, err
	}
	prompt := verdictPrompt
	var parseErr error
	for attempt := 1; attempt <= 2; attempt++ {
		answer, err := turn(fmt.Sprintf("verdict-%d", attempt), prompt, "")
		if err != nil {
			return Verdict{}, err
		}
		verdict, err := ParseVerdict(answer.Text)
		if err == nil {
			return verdict, nil
		}
		parseErr, prompt = err, verdictRetryPrompt
	}
	return Verdict{}, parseErr
}

var (
	// Reads are retried, a bad request is not.
	readOptions = workflow.ActivityOptions{
		TaskQueue:           TaskQueue,
		StartToCloseTimeout: time.Minute,
		RetryPolicy: &temporal.RetryPolicy{
			InitialInterval: 2 * time.Second, BackoffCoefficient: 2, MaximumInterval: time.Minute, MaximumAttempts: 6,
			NonRetryableErrorTypes: []string{ErrorRefused},
		},
	}
	// A write is retried a few times. Publishing checks for its own review
	// first, so a retry does not post twice.
	writeOptions = workflow.ActivityOptions{
		TaskQueue:           TaskQueue,
		StartToCloseTimeout: time.Minute,
		RetryPolicy: &temporal.RetryPolicy{
			InitialInterval: 2 * time.Second, BackoffCoefficient: 2, MaximumInterval: 30 * time.Second, MaximumAttempts: 4,
			NonRetryableErrorTypes: []string{ErrorRefused, ErrorNotMergeable, ErrorHeadChanged},
		},
	}
)

// ErrorRefused is the error type of a request that GitHub or the activity's
// own rules refused, and that trying again cannot change.
const ErrorRefused = githubapi.ErrorRefused

func isRefusal(err error) bool {
	application, ok := errors.AsType[*temporal.ApplicationError](err)
	return ok && (application.Type() == ErrorNotMergeable || application.Type() == ErrorHeadChanged || application.Type() == ErrorRefused)
}

// cause is the message of the error the activity returned, without
// Temporal's description of the activity.
func cause(err error) string {
	if application, ok := errors.AsType[*temporal.ApplicationError](err); ok {
		return application.Message()
	}
	return err.Error()
}
