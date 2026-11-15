package p7

import (
	"errors"
	"fmt"
	"slices"
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
	// Tools are offered to the agent. Their calls reach this workflow, which
	// must answer them with HandleTools.
	Tools []Tool
	// Limit names a shared limit that the session counts against. Opening
	// waits until the limit has room, and the place is given back by
	// Session.Close. The worker's WorkerConfig.Limits must give the name a
	// capacity. Empty counts against no limit.
	Limit string
	// Priority orders the sessions that wait for the same limit: a higher
	// number goes first, and equals go in the order they asked. It has no
	// effect without Limit.
	Priority int
	// Permissions decides the agent's permission requests. Nil allows every
	// request. Requests reach this workflow, which must answer them with
	// HandlePermissions and this same function. An error from it, and a
	// request that cannot reach the workflow, deny the request.
	Permissions func(ctx workflow.Context, request PermissionRequest) (PermissionDecision, error)
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
	// TaskQueue is the core queue of the machine or pod that holds the
	// session.
	TaskQueue string          `json:"task_queue"`
	Inputs    []ResolvedInput `json:"inputs,omitempty"`
	// Lease is the place the session holds in its limit, if it has one.
	Lease *Lease `json:"lease,omitempty"`
	// Reports is true when the session offers ReportTool, which PromptTyped needs.
	Reports bool `json:"reports,omitempty"`
	// Pod is the session's pod on the kubernetes backend, which Close
	// deletes. Empty on the host backend.
	Pod string `json:"pod,omitempty"`
}

var sessionRetry = &temporal.RetryPolicy{
	InitialInterval:        2 * time.Second,
	BackoffCoefficient:     2,
	MaximumInterval:        time.Minute,
	MaximumAttempts:        10,
	NonRetryableErrorTypes: []string{activity.ErrorSessionClosed, activity.ErrorSessionNotFound},
}

// OpenSession opens a session on the machine its placement names and returns
// once its inputs are in place and the agent harness runs. When the spec
// names a limit, it first waits for a place in it. Cancelling ctx stops the
// wait.
func OpenSession(ctx workflow.Context, spec SessionSpec) (*Session, error) {
	taskQueue, err := spec.Placement.TaskQueue()
	if err != nil {
		return nil, fmt.Errorf("open session: %w", err)
	}
	if spec.Limit == "" {
		return openSession(ctx, spec, taskQueue)
	}
	lease, err := acquireLease(ctx, spec.Limit, spec.Priority)
	if err != nil {
		return nil, fmt.Errorf("open session: %w", err)
	}
	session, err := openSession(ctx, spec, taskQueue)
	if err != nil {
		releaseLease(ctx, *lease)
		return nil, err
	}
	session.Lease = lease
	return session, nil
}

func openSession(ctx workflow.Context, spec SessionSpec, taskQueue string) (*Session, error) {
	request := domain.SessionRequest{
		Models: spec.Models, WorkingDirectory: spec.WorkingDirectory, IdleLimit: spec.IdleLimit,
	}
	box := reportsOf(ctx)
	if box != nil && !box.handled {
		return nil, errors.New("open session: the context has reports, and HandleTools was not called with it")
	}
	for _, tool := range spec.Tools {
		if tool.Name == ReportTool {
			return nil, fmt.Errorf("open session: the name %q is reserved for typed reports", ReportTool)
		}
	}
	if spec.Permissions != nil {
		request.Permissions = true
	}
	if len(spec.Tools) > 0 || box != nil || spec.Permissions != nil {
		request.Tools = toolSpecs(spec.Tools)
		if box != nil {
			request.Tools = append(request.Tools, reportSpec)
		}
	}
	request.Owner = workflow.GetInfo(ctx).WorkflowExecution.ID
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
	var pod domain.PodHandle
	if spec.Placement.Backend == domain.BackendKubernetes {
		// The pod is created first, and the session is opened by the worker
		// inside it, on the pod's own queue. The open waits on that queue
		// while the pod is scheduled and its image is pulled.
		podCtx := workflow.WithActivityOptions(ctx, podOptions)
		podRequest := domain.PodRequest{Session: request.ID, Owner: request.Owner}
		if err := workflow.ExecuteActivity(podCtx, domain.ActivityStartSessionPod, podRequest).Get(ctx, &pod); err != nil {
			return nil, fmt.Errorf("open session %s: start its pod: %w", request.ID, err)
		}
		activityCtx = workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
			TaskQueue:              pod.TaskQueue,
			ScheduleToStartTimeout: podStartLimit,
			StartToCloseTimeout:    timeout,
			RetryPolicy:            retry,
		})
	}
	var handle domain.SessionHandle
	if err := workflow.ExecuteActivity(activityCtx, domain.ActivityOpenSession, request).Get(ctx, &handle); err != nil {
		if pod.Name != "" {
			// A session that could not open leaves no pod behind.
			deleteCtx, _ := workflow.NewDisconnectedContext(ctx)
			deleteCtx = workflow.WithActivityOptions(deleteCtx, podDeleteOptions)
			_ = workflow.ExecuteActivity(deleteCtx, domain.ActivityDeleteSessionPod, domain.PodRequest{Session: request.ID}).Get(deleteCtx, nil)
		}
		return nil, fmt.Errorf("open session %s: %w", request.ID, err)
	}
	return &Session{ID: handle.ID, TaskQueue: handle.TaskQueue, Inputs: handle.Inputs, Reports: box != nil, Pod: pod.Name}, nil
}

// podOptions are those of the activities that create and delete session
// pods. Both are idempotent.
var podOptions = workflow.ActivityOptions{
	TaskQueue:           domain.KubernetesQueue,
	StartToCloseTimeout: time.Minute,
	RetryPolicy:         &temporal.RetryPolicy{InitialInterval: 2 * time.Second, BackoffCoefficient: 2, MaximumInterval: 30 * time.Second, MaximumAttempts: 10},
}

// podDeleteOptions are those of the activity that deletes a session pod.
// With snapshots on, it waits for the pod's worker to write its last
// snapshot and stop, which can take minutes, and it heartbeats while it
// waits. Without them it returns at once.
var podDeleteOptions = workflow.ActivityOptions{
	TaskQueue:           domain.KubernetesQueue,
	StartToCloseTimeout: 20 * time.Minute,
	HeartbeatTimeout:    30 * time.Second,
	RetryPolicy:         podOptions.RetryPolicy,
}

// podStartLimit bounds the wait for a new session pod's worker: scheduling,
// pulling the image and starting.
const podStartLimit = 10 * time.Minute

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
	// Retry is the retry policy of the turn's activity. Nil is ten attempts
	// over about five minutes. A session that is closed or does not exist
	// is never retried.
	Retry *temporal.RetryPolicy
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
	retry := sessionRetry
	if prompt.Retry != nil {
		copied := *prompt.Retry
		copied.NonRetryableErrorTypes = append(slices.Clone(copied.NonRetryableErrorTypes), sessionRetry.NonRetryableErrorTypes...)
		retry = &copied
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
		RetryPolicy:         retry,
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

// Close stops the session's processes and gives back its place in its limit.
// Its workspace and journal are kept. Closing twice is harmless. A session
// that fails to close keeps its place. To close when the workflow itself is being
// cancelled, pass a context from workflow.NewDisconnectedContext.
func (s *Session) Close(ctx workflow.Context) error {
	activityCtx := workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		TaskQueue:           s.TaskQueue,
		StartToCloseTimeout: 2 * time.Minute,
		RetryPolicy:         sessionRetry,
	})
	if s.Pod != "" {
		// A pod that is gone has no worker to close the session in, and
		// deleting the pod ends the session all the same.
		activityCtx = workflow.WithActivityOptions(activityCtx, workflow.ActivityOptions{
			TaskQueue:              s.TaskQueue,
			ScheduleToStartTimeout: time.Minute,
			StartToCloseTimeout:    2 * time.Minute,
			RetryPolicy:            &temporal.RetryPolicy{MaximumAttempts: 1},
		})
	}
	request := domain.CloseRequest{Session: s.ID}
	closeErr := workflow.ExecuteActivity(activityCtx, domain.ActivityCloseSession, request).Get(ctx, nil)
	if s.Pod == "" {
		if closeErr != nil {
			return fmt.Errorf("close session %s: %w", s.ID, closeErr)
		}
		return nil
	}
	podCtx := workflow.WithActivityOptions(ctx, podDeleteOptions)
	if err := workflow.ExecuteActivity(podCtx, domain.ActivityDeleteSessionPod, domain.PodRequest{Session: s.ID}).Get(ctx, nil); err != nil {
		return fmt.Errorf("close session %s: delete its pod: %w", s.ID, err)
	}
	if s.Lease != nil {
		releaseLease(ctx, *s.Lease)
	}
	return nil
}

func newID(ctx workflow.Context, id *string) error {
	return workflow.SideEffect(ctx, func(workflow.Context) any { return ids.New() }).Get(id)
}
