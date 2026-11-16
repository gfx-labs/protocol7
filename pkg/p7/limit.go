package p7

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"sort"
	"strings"
	"time"

	"go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"
)

// A limit is a named count of leases that workflows hold at once. Its state
// is one Temporal workflow per name, which grants a lease to the waiter of
// the highest priority whenever one is free and takes it back when the holder
// releases it or is gone. A session names a limit in SessionSpec.Limit.
//
// The messages are signals in both directions, so that a waiting workflow
// holds no activity and the limit's history grows only by requests.
//
//	holder -> limit  p7.limit.request   a lease wants a place
//	limit  -> holder p7.limit.granted/<lease>
//	holder -> limit  p7.limit.release   the lease is done, or no longer wanted
const (
	limitWorkflowName   = "p7.Limit"
	activityLimitStart  = "p7.limit.Start"
	activityLimitGone   = "p7.limit.Gone"
	signalLimitRequest  = "p7.limit.request"
	signalLimitRelease  = "p7.limit.release"
	signalLimitGranted  = "p7.limit.granted/"
	queryLimitState     = "p7.limit.state"
	errorLimitNotSet    = "LimitNotConfigured"
	limitWorkflowPrefix = "p7.limit/"

	// DefaultLimitReconcile is how often a limit checks that its holders and
	// waiters still exist, when LimitConfig.Reconcile is zero.
	DefaultLimitReconcile = time.Minute

	// A limit continues as new after this many messages.
	limitMessagesPerRun = 500
)

// ErrorLimitNotConfigured is the type of the application error that opening
// a session fails with when the limit it names has no capacity in the
// worker's LimitConfig.
const ErrorLimitNotConfigured = errorLimitNotSet

// LimitConfig is a worker's configuration of shared limits. Every worker of
// a deployment that serves workflows naming a limit should be given the same.
type LimitConfig struct {
	// Capacity maps a limit's name to how many leases it grants at once. A
	// name may hold "*", which matches any run of characters, so
	// "repo:*" gives every repository the same number. An exact name wins
	// over a pattern, and the longest pattern over a shorter one.
	Capacity map[string]int
	// Aging is how long a waiter must wait to gain one priority point, so
	// that a stream of high priority work does not starve it. Zero turns
	// aging off.
	Aging time.Duration
	// Reconcile is how often a limit looks for holders and waiters whose
	// workflow has ended without releasing, and frees them. Zero is
	// DefaultLimitReconcile.
	Reconcile time.Duration
	// TaskQueue is where limit workflows run. Empty is the queue of the
	// worker whose workflow first asks. Several consumer queues that share a
	// limit must name one queue here, and a worker must serve it.
	TaskQueue string
}

func (c LimitConfig) validate() error {
	for name, capacity := range c.Capacity {
		if name == "" || capacity < 1 {
			return fmt.Errorf("limit %q: the capacity must be at least 1 and the name must not be empty", name)
		}
	}
	if c.Aging < 0 || c.Reconcile < 0 {
		return errors.New("limits: aging and reconcile must not be negative")
	}
	return nil
}

// capacityOf is the capacity of the limit called name.
func (c LimitConfig) capacityOf(name string) (int, bool) {
	if capacity, ok := c.Capacity[name]; ok {
		return capacity, true
	}
	best, bestLength, found := 0, -1, false
	patterns := make([]string, 0, len(c.Capacity))
	for pattern := range c.Capacity {
		patterns = append(patterns, pattern)
	}
	sort.Strings(patterns) // so that two patterns of one length always give the same answer
	for _, pattern := range patterns {
		if wildcard(pattern, name) && len(pattern) > bestLength {
			best, bestLength, found = c.Capacity[pattern], len(pattern), true
		}
	}
	return best, found
}

// wildcard reports whether name matches pattern, where "*" matches any run
// of characters, including none.
func wildcard(pattern, name string) bool {
	head, rest, star := strings.Cut(pattern, "*")
	if !star {
		return pattern == name
	}
	if !strings.HasPrefix(name, head) {
		return false
	}
	name = name[len(head):]
	for i := 0; i <= len(name); i++ {
		if wildcard(rest, name[i:]) {
			return true
		}
	}
	return false
}

// Lease is a place in a limit that a workflow holds.
type Lease struct {
	Limit string `json:"limit"`
	ID    string `json:"id"`
}

// LimitEntry is a holder or a waiter of a limit.
type LimitEntry struct {
	Lease string `json:"lease"`
	// Holder is the ID of the workflow that asked.
	Holder   string    `json:"holder"`
	Priority int       `json:"priority"`
	Since    time.Time `json:"since"`
}

// LimitStatus is what a limit holds now. Waiters are in arrival order.
type LimitStatus struct {
	Name     string       `json:"name"`
	Capacity int          `json:"capacity"`
	Holders  []LimitEntry `json:"holders"`
	Waiters  []LimitEntry `json:"waiters"`
}

// QueryLimit reads the state of a limit. A limit that nothing has asked for
// yet has no workflow, and the error then wraps a not-found error.
func QueryLimit(ctx context.Context, c client.Client, name string) (LimitStatus, error) {
	var status LimitStatus
	value, err := c.QueryWorkflow(ctx, limitWorkflowID(name), "", queryLimitState)
	if err != nil {
		return status, fmt.Errorf("query limit %q: %w", name, err)
	}
	if err := value.Get(&status); err != nil {
		return status, fmt.Errorf("query limit %q: %w", name, err)
	}
	return status, nil
}

func limitWorkflowID(name string) string { return limitWorkflowPrefix + name }

// limitState is the limit workflow's input and its whole state, which it
// carries across Continue-As-New.
type limitState struct {
	Name      string        `json:"name"`
	Capacity  int           `json:"capacity"`
	Aging     time.Duration `json:"aging,omitempty"`
	Reconcile time.Duration `json:"reconcile,omitempty"`
	Holders   []LimitEntry  `json:"holders,omitempty"`
	Waiters   []LimitEntry  `json:"waiters,omitempty"`
}

// limitRequest is the signal that asks for a lease. It also carries the
// configuration of the asking worker, so that a change of capacity reaches
// a limit that is already running.
type limitRequest struct {
	Lease     string        `json:"lease"`
	Holder    string        `json:"holder"`
	Priority  int           `json:"priority"`
	Capacity  int           `json:"capacity"`
	Aging     time.Duration `json:"aging,omitempty"`
	Reconcile time.Duration `json:"reconcile,omitempty"`
}

// limitStart is the input of the activity that sends the first request.
type limitStart struct {
	Limit    string `json:"limit"`
	Lease    string `json:"lease"`
	Holder   string `json:"holder"`
	Priority int    `json:"priority"`
}

// limitActivities start limit workflows and look up holders. A worker
// registers them with its configuration.
type limitActivities struct {
	temporal client.Client
	config   LimitConfig
	queue    string
}

// Start signals the limit workflow with a lease's request, and starts the
// workflow first when it is not running.
func (a *limitActivities) Start(ctx context.Context, start limitStart) error {
	capacity, ok := a.config.capacityOf(start.Limit)
	if !ok {
		return temporal.NewNonRetryableApplicationError(
			fmt.Sprintf("limit %q has no capacity in this worker's configuration", start.Limit), errorLimitNotSet, nil)
	}
	request := limitRequest{
		Lease: start.Lease, Holder: start.Holder, Priority: start.Priority,
		Capacity: capacity, Aging: a.config.Aging, Reconcile: a.config.Reconcile,
	}
	queue := a.config.TaskQueue
	if queue == "" {
		queue = a.queue
	}
	initial := limitState{Name: start.Limit, Capacity: capacity, Aging: a.config.Aging, Reconcile: a.config.Reconcile}
	_, err := a.temporal.SignalWithStartWorkflow(ctx, limitWorkflowID(start.Limit), signalLimitRequest, request,
		client.StartWorkflowOptions{ID: limitWorkflowID(start.Limit), TaskQueue: queue}, limitWorkflowName, initial)
	if err != nil {
		return fmt.Errorf("request a lease of limit %q: %w", start.Limit, err)
	}
	return nil
}

// Gone returns the workflows among ids that are not running.
func (a *limitActivities) Gone(ctx context.Context, ids []string) ([]string, error) {
	var gone []string
	for _, id := range ids {
		described, err := a.temporal.DescribeWorkflowExecution(ctx, id, "")
		var notFound *serviceerror.NotFound
		switch {
		case errors.As(err, &notFound):
			gone = append(gone, id)
		case err != nil:
			return nil, fmt.Errorf("describe workflow %s: %w", id, err)
		case described.GetWorkflowExecutionInfo().GetStatus() != enums.WORKFLOW_EXECUTION_STATUS_RUNNING:
			gone = append(gone, id)
		}
	}
	return gone, nil
}

// limitWorkflow grants the leases of one limit. It never ends.
func limitWorkflow(ctx workflow.Context, state limitState) error {
	if err := workflow.SetQueryHandler(ctx, queryLimitState, func() (LimitStatus, error) {
		return LimitStatus{Name: state.Name, Capacity: state.Capacity, Holders: state.Holders, Waiters: state.Waiters}, nil
	}); err != nil {
		return err
	}
	requests := workflow.GetSignalChannel(ctx, signalLimitRequest)
	releases := workflow.GetSignalChannel(ctx, signalLimitRelease)

	var timer workflow.Future
	var stopTimer workflow.CancelFunc
	handled := 0
	handle := func(request limitRequest) {
		handled++
		state.admit(workflow.Now(ctx), request)
	}
	release := func(lease string) {
		handled++
		state.remove(func(entry LimitEntry) bool { return entry.Lease == lease })
	}
	for {
		if busy := len(state.Holders)+len(state.Waiters) > 0; busy && timer == nil {
			var timerCtx workflow.Context
			timerCtx, stopTimer = workflow.WithCancel(ctx)
			every := state.Reconcile
			if every <= 0 {
				every = DefaultLimitReconcile
			}
			timer = workflow.NewTimer(timerCtx, every)
		} else if !busy && timer != nil {
			stopTimer()
			timer = nil
		}

		reconcile := false
		selector := workflow.NewSelector(ctx)
		selector.AddReceive(requests, func(c workflow.ReceiveChannel, _ bool) {
			var request limitRequest
			c.Receive(ctx, &request)
			handle(request)
		})
		selector.AddReceive(releases, func(c workflow.ReceiveChannel, _ bool) {
			var lease string
			c.Receive(ctx, &lease)
			release(lease)
		})
		if timer != nil {
			selector.AddFuture(timer, func(workflow.Future) {
				timer, reconcile = nil, true
			})
		}
		selector.Select(ctx)
		// Take what else arrived, so that one pass grants in the order of
		// priority and not of arrival.
		for {
			var request limitRequest
			if !requests.ReceiveAsync(&request) {
				break
			}
			handle(request)
		}
		for {
			var lease string
			if !releases.ReceiveAsync(&lease) {
				break
			}
			release(lease)
		}

		if reconcile {
			state.reconcile(ctx)
		}
		state.grant(ctx)

		if handled >= limitMessagesPerRun || workflow.GetInfo(ctx).GetContinueAsNewSuggested() {
			return workflow.NewContinueAsNewError(ctx, limitWorkflow, state)
		}
	}
}

// admit queues a request, and takes its configuration. A lease the limit
// already knows is not queued twice.
func (s *limitState) admit(now time.Time, request limitRequest) {
	if request.Capacity > 0 {
		s.Capacity = request.Capacity
	}
	s.Aging, s.Reconcile = request.Aging, request.Reconcile
	known := func(entries []LimitEntry) bool {
		return slices.ContainsFunc(entries, func(e LimitEntry) bool { return e.Lease == request.Lease })
	}
	if known(s.Holders) || known(s.Waiters) {
		return
	}
	s.Waiters = append(s.Waiters, LimitEntry{Lease: request.Lease, Holder: request.Holder, Priority: request.Priority, Since: now})
}

func (s *limitState) remove(match func(LimitEntry) bool) {
	s.Holders = slices.DeleteFunc(s.Holders, match)
	s.Waiters = slices.DeleteFunc(s.Waiters, match)
}

// next is the index of the waiter to serve first: the highest priority, where
// each Aging that a waiter has waited adds one, and the earliest arrival
// among equals. Waiters are in arrival order.
func (s *limitState) next(now time.Time) int {
	best, bestPriority := -1, 0
	for i, waiter := range s.Waiters {
		priority := waiter.Priority
		if s.Aging > 0 && now.After(waiter.Since) {
			priority += int(now.Sub(waiter.Since) / s.Aging)
		}
		if best < 0 || priority > bestPriority {
			best, bestPriority = i, priority
		}
	}
	return best
}

// grant gives free places to waiters. A waiter whose workflow has gone is
// dropped and does not take one.
func (s *limitState) grant(ctx workflow.Context) {
	for len(s.Holders) < s.Capacity && len(s.Waiters) > 0 {
		i := s.next(workflow.Now(ctx))
		waiter := s.Waiters[i]
		s.Waiters = slices.Delete(s.Waiters, i, i+1)
		err := workflow.SignalExternalWorkflow(ctx, waiter.Holder, "", signalLimitGranted+waiter.Lease, true).Get(ctx, nil)
		if err != nil {
			workflow.GetLogger(ctx).Warn("a waiter of the limit is gone", "limit", s.Name, "holder", waiter.Holder, "error", err)
			continue
		}
		s.Holders = append(s.Holders, waiter)
	}
}

// reconcile frees the holders and waiters whose workflow is no longer running.
func (s *limitState) reconcile(ctx workflow.Context) {
	var holders []string
	for _, entry := range slices.Concat(s.Holders, s.Waiters) {
		if !slices.Contains(holders, entry.Holder) {
			holders = append(holders, entry.Holder)
		}
	}
	if len(holders) == 0 {
		return
	}
	ctx = workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		StartToCloseTimeout: 30 * time.Second,
		RetryPolicy:         &temporal.RetryPolicy{MaximumAttempts: 3},
	})
	var gone []string
	if err := workflow.ExecuteActivity(ctx, activityLimitGone, holders).Get(ctx, &gone); err != nil {
		workflow.GetLogger(ctx).Warn("could not check the holders of the limit", "limit", s.Name, "error", err)
		return
	}
	s.remove(func(entry LimitEntry) bool {
		freed := slices.Contains(gone, entry.Holder)
		if freed {
			workflow.GetLogger(ctx).Warn("freeing a lease whose workflow has ended", "limit", s.Name, "holder", entry.Holder)
		}
		return freed
	})
}

// AcquireLimit waits for a lease in a worker-configured shared limit. It can
// bound work shorter than a session, such as a single model turn. The caller
// must release the returned lease, including when its context is cancelled.
func AcquireLimit(ctx workflow.Context, name string, priority int) (*Lease, error) {
	if name == "" {
		return nil, errors.New("acquire limit: a name is required")
	}
	return acquireLease(ctx, name, priority)
}

// Release gives this lease back. Releasing it more than once is harmless.
func (l Lease) Release(ctx workflow.Context) { releaseLease(ctx, l) }

// acquireLease waits for a place in the limit. A cancelled ctx gives the
// place up.
func acquireLease(ctx workflow.Context, name string, priority int) (*Lease, error) {
	lease := Lease{Limit: name}
	if err := newID(ctx, &lease.ID); err != nil {
		return nil, fmt.Errorf("acquire limit %q: %w", name, err)
	}
	asked := workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		StartToCloseTimeout: 30 * time.Second,
		// Without a wait, a cancelled request could reach the limit after
		// the release that follows it.
		WaitForCancellation: true,
		RetryPolicy: &temporal.RetryPolicy{
			InitialInterval: time.Second, MaximumAttempts: 5,
			NonRetryableErrorTypes: []string{errorLimitNotSet},
		},
	})
	start := limitStart{Limit: name, Lease: lease.ID, Holder: workflow.GetInfo(ctx).WorkflowExecution.ID, Priority: priority}
	if err := workflow.ExecuteActivity(asked, activityLimitStart, start).Get(ctx, nil); err != nil {
		// The request may have landed before the error.
		releaseLease(ctx, lease)
		return nil, fmt.Errorf("acquire limit %q: %w", name, err)
	}
	granted := false
	selector := workflow.NewSelector(ctx)
	selector.AddReceive(workflow.GetSignalChannel(ctx, signalLimitGranted+lease.ID), func(c workflow.ReceiveChannel, _ bool) {
		c.Receive(ctx, nil)
		granted = true
	})
	selector.AddReceive(ctx.Done(), func(workflow.ReceiveChannel, bool) {})
	selector.Select(ctx)
	if !granted {
		releaseLease(ctx, lease)
		return nil, fmt.Errorf("acquire limit %q: %w", name, ctx.Err())
	}
	return &lease, nil
}

// releaseLease gives the lease back. It works on a cancelled workflow, and a
// limit that cannot be reached is not an error: its reconciliation frees
// the lease once this workflow ends.
func releaseLease(ctx workflow.Context, lease Lease) {
	ctx, _ = workflow.NewDisconnectedContext(ctx)
	err := workflow.SignalExternalWorkflow(ctx, limitWorkflowID(lease.Limit), "", signalLimitRelease, lease.ID).Get(ctx, nil)
	if err != nil {
		workflow.GetLogger(ctx).Warn("could not release a lease", "limit", lease.Limit, "lease", lease.ID, "error", err)
	}
}
