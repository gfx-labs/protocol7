package p7

import (
	"errors"
	"fmt"
	"slices"
	"time"

	enumspb "go.temporal.io/api/enums/v1"
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
	// Pool, on the kubernetes backend, puts the session on a worker of a pool
	// instead of a pod of its own: up to Pool.SessionsPerWorker sessions on each
	// of up to Pool.MaxWorkers pods, started on demand. Opening waits when the
	// pool is full. Nil gives the session a pod of its own.
	//
	// Sessions on one worker see each other's files, processes and queue, and
	// a pool has one identity. The caller names the pool after everything that
	// decides what a session may do, so that only sessions with identical
	// permissions share a pool.
	Pool *SessionPool
}

// SessionPool says how a pool of workers is made. See domain.PoolSpec.
type SessionPool = domain.PoolSpec

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
	// Pool is the pool whose worker holds the session, if it is in one. The
	// session's TaskQueue and Pod are those of its worker, and change when the
	// worker is lost and the session is placed on another.
	Pool *SessionPool `json:"pool,omitempty"`
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
	if spec.Pool != nil {
		if spec.Placement.Backend != domain.BackendKubernetes {
			return nil, errors.New("open session: a pool needs the kubernetes backend")
		}
		if err := spec.Pool.Validate(); err != nil {
			return nil, fmt.Errorf("open session: %w", err)
		}
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
		if spec.Pool != nil {
			// The pool says which worker, and waits for room if it is full.
			placement, err := placeSession(ctx, *spec.Pool, request.ID, request.Owner)
			if err != nil {
				return nil, fmt.Errorf("open session %s: place it in pool %s: %w", request.ID, spec.Pool.Name, err)
			}
			pod = domain.PodHandle{Name: placement.Pod, TaskQueue: placement.TaskQueue}
		} else {
			// The pod is created first, and the session is opened by the worker
			// inside it, on the pod's own queue. The open waits on that queue
			// while the pod is scheduled and its image is pulled.
			podCtx := workflow.WithActivityOptions(ctx, podOptions)
			podRequest := domain.PodRequest{Session: request.ID, Owner: request.Owner, OpenTimeout: timeout}
			if err := workflow.ExecuteActivity(podCtx, domain.ActivityStartSessionPod, podRequest).Get(ctx, &pod); err != nil {
				return nil, fmt.Errorf("open session %s: start its pod: %w", request.ID, err)
			}
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
			// A session that could not open leaves no pod behind, and no
			// place in its pool.
			deleteCtx, _ := workflow.NewDisconnectedContext(ctx)
			if spec.Pool != nil {
				releaseSession(deleteCtx, *spec.Pool, request.ID)
			} else {
				deleteCtx = workflow.WithActivityOptions(deleteCtx, podDeleteOptions)
				_ = workflow.ExecuteActivity(deleteCtx, domain.ActivityDeleteSessionPod, domain.PodRequest{Session: request.ID}).Get(deleteCtx, nil)
			}
		}
		return nil, fmt.Errorf("open session %s: %w", request.ID, err)
	}
	return &Session{ID: handle.ID, TaskQueue: handle.TaskQueue, Inputs: handle.Inputs, Reports: box != nil, Pod: pod.Name, Pool: spec.Pool}, nil
}

// podOptions are those of the activities that create and delete session
// pods. Both are idempotent.
var podOptions = workflow.ActivityOptions{
	TaskQueue:           domain.KubernetesQueue,
	StartToCloseTimeout: time.Minute,
	RetryPolicy:         &temporal.RetryPolicy{InitialInterval: 2 * time.Second, BackoffCoefficient: 2, MaximumInterval: 30 * time.Second, MaximumAttempts: 10},
}

// placeOptions are those of the activity that places a session in a pool. It
// returns when a worker has room, which can take as long as the sessions that
// fill the pool, and it heartbeats while it waits.
var placeOptions = workflow.ActivityOptions{
	TaskQueue:           domain.KubernetesQueue,
	StartToCloseTimeout: 12 * time.Hour,
	HeartbeatTimeout:    time.Minute,
	RetryPolicy:         &temporal.RetryPolicy{InitialInterval: 2 * time.Second, BackoffCoefficient: 2, MaximumInterval: 30 * time.Second, MaximumAttempts: 10},
}

// placeSession asks a pool where a session runs. Each call has an attempt ID
// of its own, in history, which makes the activity's retries ask once.
func placeSession(ctx workflow.Context, pool domain.PoolSpec, session, owner string) (domain.PoolPlacement, error) {
	var attempt string
	if err := newID(ctx, &attempt); err != nil {
		return domain.PoolPlacement{}, err
	}
	var placement domain.PoolPlacement
	request := domain.PlaceRequest{Pool: pool, Session: session, Owner: owner, Attempt: attempt}
	err := workflow.ExecuteActivity(workflow.WithActivityOptions(ctx, placeOptions), domain.ActivityPlaceSession, request).Get(ctx, &placement)
	return placement, err
}

// releaseSession gives back a session's place in its pool, and deletes its
// snapshot, which no worker writes once the session is closed or lost. It
// does not fail: a pool that cannot be told closes the sessions whose owner
// is not running.
func releaseSession(ctx workflow.Context, pool domain.PoolSpec, session string) {
	podCtx := workflow.WithActivityOptions(ctx, podOptions)
	_ = workflow.ExecuteActivity(podCtx, domain.ActivityDeletePoolSnapshots, domain.PoolSnapshotsRequest{Pool: pool.Name, Session: session}).Get(ctx, nil)
	_ = workflow.ExecuteActivity(podCtx, domain.ActivityReleaseSession, domain.ReleaseRequest{Pool: pool, Session: session}).Get(ctx, nil)
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
	options := workflow.ActivityOptions{
		TaskQueue:           session.TaskQueue,
		StartToCloseTimeout: timeout,
		// A cancel request reaches a running activity in the answer to a
		// heartbeat, and the SDK sends one at most every 0.8 of this. A
		// cancelled turn therefore stops within about eight seconds.
		HeartbeatTimeout: 10 * time.Second,
		// A cancelled turn still ends with a result. Wait for it.
		WaitForCancellation: true,
		RetryPolicy:         retry,
	}
	if session.Pod != "" {
		return runTurnInPod(ctx, session, request, options)
	}
	return workflow.ExecuteActivity(workflow.WithActivityOptions(ctx, options), domain.ActivityRunTurn, request)
}

// podLostAfter is how long a turn waits for the worker in its session's pod
// to take it before the pod is made again. The worker of a pod that exists
// takes it at once, so a longer wait means that the pod is gone, was deleted
// and is ending, or is starting, which the new request for it tells apart.
var podLostAfter = time.Minute

// podRecoveries is how many times a turn makes its pod again before it
// gives up. With podLostAfter that is about 40 minutes, which covers a pod
// that ends slowly because its worker is writing a last snapshot (12
// minutes) and the new pod's image pull and restore.
const podRecoveries = 40

// runTurnInPod runs a turn on the queue of a session's pod. A pod that is
// deleted mid-turn, or while the session is idle, leaves nobody to poll
// the queue, and the activity's retries would wait for ever. So each
// attempt may wait podLostAfter to be taken. An attempt that is not taken
// fails with a schedule-to-start timeout, which the retry policy does not
// retry, and the pod is requested again: that creates it when it is gone,
// and finds it when it exists. The new pod restores the session's snapshot
// before its worker polls (see SnapshotStore), and the turn, which has the
// same ID, runs again in it. A turn the agent ended in any other way is
// returned as it is.
func runTurnInPod(ctx workflow.Context, session *Session, request domain.TurnRequest, options workflow.ActivityOptions) workflow.Future {
	future, settable := workflow.NewFuture(ctx)
	options.ScheduleToStartTimeout = podLostAfter
	workflow.Go(ctx, func(ctx workflow.Context) {
		owner := workflow.GetInfo(ctx).WorkflowExecution.ID
		for recoveries := 0; ; recoveries++ {
			var turn domain.TurnResult
			err := workflow.ExecuteActivity(workflow.WithActivityOptions(ctx, options), domain.ActivityRunTurn, request).Get(ctx, &turn)
			if !isScheduleToStartTimeout(err) || ctx.Err() != nil || recoveries == podRecoveries {
				settable.Set(turn, err)
				return
			}
			if session.Pool != nil {
				// Another worker of the pool, or the same if it is there.
				placement, err := placeSession(ctx, *session.Pool, session.ID, owner)
				if err != nil {
					settable.Set(turn, fmt.Errorf("place session %s in its pool again: %w", session.ID, err))
					return
				}
				session.TaskQueue, session.Pod = placement.TaskQueue, placement.Pod
				options.TaskQueue = placement.TaskQueue
				continue
			}
			podCtx := workflow.WithActivityOptions(ctx, podOptions)
			var pod domain.PodHandle
			if err := workflow.ExecuteActivity(podCtx, domain.ActivityStartSessionPod, domain.PodRequest{Session: session.ID, Owner: owner}).Get(ctx, &pod); err != nil {
				settable.Set(turn, fmt.Errorf("make the pod of session %s again: %w", session.ID, err))
				return
			}
		}
	})
	return future
}

func isScheduleToStartTimeout(err error) bool {
	var timeout *temporal.TimeoutError
	return errors.As(err, &timeout) && timeout.TimeoutType() == enumspb.TIMEOUT_TYPE_SCHEDULE_TO_START
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
	if s.Pool != nil {
		// The worker stays for the pool's other sessions and goes when it has
		// had none for the pool's IdleTimeout.
		releaseSession(ctx, *s.Pool, s.ID)
	} else {
		podCtx := workflow.WithActivityOptions(ctx, podDeleteOptions)
		if err := workflow.ExecuteActivity(podCtx, domain.ActivityDeleteSessionPod, domain.PodRequest{Session: s.ID}).Get(ctx, nil); err != nil {
			return fmt.Errorf("close session %s: delete its pod: %w", s.ID, err)
		}
	}
	if s.Lease != nil {
		releaseLease(ctx, *s.Lease)
	}
	return nil
}

func newID(ctx workflow.Context, id *string) error {
	return workflow.SideEffect(ctx, func(workflow.Context) any { return ids.New() }).Get(id)
}
