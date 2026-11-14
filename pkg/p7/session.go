package p7

import (
	"errors"
	"fmt"
	"time"

	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"

	"github.com/gfx-labs/protocol7/pkg/domain"
	"github.com/gfx-labs/protocol7/pkg/orchestration/activity"
	"github.com/gfx-labs/protocol7/pkg/support/ids"
)

// SessionSpec says what session to open.
type SessionSpec struct {
	// Models is the ordered list the harness takes, each entry in its own
	// form, such as "claude:work/claude-opus-5-5#medium". Empty leaves the
	// choice to the harness's configuration on the machine.
	Models []string
	Inputs []Input
	// WorkingDirectory is where the agent starts, a path in the workspace.
	// Empty is the workspace root.
	WorkingDirectory string
	Placement        Placement
	// IdleLimit is how long the session's processes are kept with no turn
	// running. After it they stop, and the next prompt starts them and
	// loads the conversation again.
	IdleLimit time.Duration
	// OpenTimeout bounds opening, which clones the inputs. Zero is 15 minutes.
	OpenTimeout time.Duration
	// OpenRetry is the retry policy of opening. Nil is ten attempts over
	// about five minutes.
	OpenRetry *temporal.RetryPolicy
}

// ErrorHarnessStopped is the type of the application error that opening or
// a turn fails with when the harness exited while the session was made
// ready, for example because it has no login. Its message ends with the
// harness's last output.
const ErrorHarnessStopped = activity.ErrorHarnessStopped

// Session is a handle to an open session. It is plain data: a workflow can
// keep it in its state and pass it across Continue-As-New.
type Session struct {
	ID string `json:"id"`
	// TaskQueue is the core queue of the machine that holds the session.
	TaskQueue string          `json:"task_queue"`
	Inputs    []ResolvedInput `json:"inputs,omitempty"`
}

var sessionRetry = &temporal.RetryPolicy{
	InitialInterval:        2 * time.Second,
	BackoffCoefficient:     2,
	MaximumInterval:        time.Minute,
	MaximumAttempts:        10,
	NonRetryableErrorTypes: []string{activity.ErrorSessionClosed, activity.ErrorSessionNotFound},
}

// OpenSession opens a session on the machine its placement names and returns
// once its inputs are in place and the agent harness runs.
func OpenSession(ctx workflow.Context, spec SessionSpec) (*Session, error) {
	taskQueue, err := spec.Placement.TaskQueue()
	if err != nil {
		return nil, fmt.Errorf("open session: %w", err)
	}
	request := domain.SessionRequest{
		Models: spec.Models, WorkingDirectory: spec.WorkingDirectory, IdleLimit: spec.IdleLimit,
	}
	for _, input := range spec.Inputs {
		if input.Source == nil {
			return nil, fmt.Errorf("open session: input %q has no source", input.Destination)
		}
		request.Inputs = append(request.Inputs, input.Source.input(input.Destination))
	}
	// The ID is recorded in history, so every attempt opens the same session.
	if err := newID(ctx, &request.ID); err != nil {
		return nil, fmt.Errorf("open session: %w", err)
	}

	timeout := spec.OpenTimeout
	if timeout <= 0 {
		timeout = 15 * time.Minute
	}
	retry := spec.OpenRetry
	if retry == nil {
		retry = sessionRetry
	}
	activityCtx := workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		TaskQueue:           taskQueue,
		StartToCloseTimeout: timeout,
		RetryPolicy:         retry,
	})
	var handle domain.SessionHandle
	if err := workflow.ExecuteActivity(activityCtx, domain.ActivityOpenSession, request).Get(ctx, &handle); err != nil {
		return nil, fmt.Errorf("open session %s: %w", request.ID, err)
	}
	return &Session{ID: handle.ID, TaskQueue: handle.TaskQueue, Inputs: handle.Inputs}, nil
}

// TextPrompt is one prompt whose answer is the agent's message text.
type TextPrompt struct {
	Text string
	// Context is sent in front of Text. A viewer of the session does not see
	// it as part of the person's message.
	Context string
	// TurnID identifies the turn. Empty makes one. A caller that names it
	// can find the turn among the session's events.
	TurnID string
	// Timeout bounds the turn. Zero is 2 hours.
	Timeout time.Duration
}

// PromptTextFuture starts a turn and returns a future of its Turn. Combine
// it with timers and signal channels in a workflow.Selector.
//
// Cancelling ctx asks the agent to stop the turn. The future then still
// yields the turn's result, with the stop reason the agent gave, which is
// normally StopCancelled.
func PromptTextFuture(ctx workflow.Context, session *Session, prompt TextPrompt) workflow.Future {
	future, settable := workflow.NewFuture(ctx)
	if session == nil {
		settable.SetError(errors.New("prompt: no session"))
		return future
	}
	request := domain.TurnRequest{Session: session.ID, Turn: prompt.TurnID, Context: prompt.Context, Prompt: prompt.Text}
	if request.Turn == "" {
		// Recorded in history, so a retried activity runs the same turn and
		// never prompts twice.
		if err := newID(ctx, &request.Turn); err != nil {
			settable.SetError(fmt.Errorf("prompt session %s: %w", session.ID, err))
			return future
		}
	}
	timeout := prompt.Timeout
	if timeout <= 0 {
		timeout = 2 * time.Hour
	}
	activityCtx := workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		TaskQueue:           session.TaskQueue,
		StartToCloseTimeout: timeout,
		// A cancel request reaches a running activity in the answer to a
		// heartbeat, and the SDK sends one at most every 0.8 of this. A
		// cancelled turn therefore stops within about eight seconds.
		HeartbeatTimeout: 10 * time.Second,
		// A cancelled turn still ends with a result. Wait for it.
		WaitForCancellation: true,
		RetryPolicy:         sessionRetry,
	})
	return workflow.ExecuteActivity(activityCtx, domain.ActivityRunTurn, request)
}

// PromptText runs one turn and returns it. A turn that the agent ended with
// an error is returned with StopReason StopFailed and its Failure set, and
// no error: the workflow decides what a failed turn means.
func PromptText(ctx workflow.Context, session *Session, prompt TextPrompt) (Turn, error) {
	var turn Turn
	if err := PromptTextFuture(ctx, session, prompt).Get(ctx, &turn); err != nil {
		return turn, fmt.Errorf("prompt session: %w", err)
	}
	return turn, nil
}

// Close stops the session's processes. Its workspace and journal are kept.
// Closing twice is harmless. To close when the workflow itself is being
// cancelled, pass a context from workflow.NewDisconnectedContext.
func (s *Session) Close(ctx workflow.Context) error {
	activityCtx := workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		TaskQueue:           s.TaskQueue,
		StartToCloseTimeout: 2 * time.Minute,
		RetryPolicy:         sessionRetry,
	})
	request := domain.CloseRequest{Session: s.ID}
	if err := workflow.ExecuteActivity(activityCtx, domain.ActivityCloseSession, request).Get(ctx, nil); err != nil {
		return fmt.Errorf("close session %s: %w", s.ID, err)
	}
	return nil
}

func newID(ctx workflow.Context, id *string) error {
	return workflow.SideEffect(ctx, func(workflow.Context) any { return ids.New() }).Get(id)
}
