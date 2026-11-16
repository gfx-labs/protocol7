package kubernetes

import (
	"errors"
	"fmt"
	"slices"
	"time"

	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"

	"github.com/gfx-labs/protocol7/pkg/domain"
)

// The workflow of a pool.
//
// One workflow owns a pool and all its state: which workers run, which
// session is on which, and which sessions wait. It is the only thing that
// starts or deletes the pool's pods, and it does so in one place, its loop,
// with activities, so no two changes race. The workflows that open sessions
// reach it by an update (place, release) that the backend's activities send.
// An update never waits: a session without room is queued and told so, and the
// activity asks again, so that any number of sessions may wait (Temporal
// allows ten in-flight updates in a workflow). It ends when it holds nothing,
// and the next place starts it again.

// Timings of a pool. Variables so that a test can shorten them.
var (
	// poolCheckEvery is how often a pool looks at its pods and its sessions'
	// owners while it has workers.
	poolCheckEvery = 30 * time.Second
	// poolOrphanAge is how old a pod of the pool that its workflow does not
	// know must be before it is deleted, so that a pod just made is not.
	poolOrphanAge = 2 * time.Minute
	// poolMissingAfter is how long after a worker was made its pod may be
	// missing from a list: the list can lag the creation.
	poolMissingAfter = 10 * time.Second
	// poolReadyTimeout is how long a pod may stay not ready before the pool
	// takes it for lost.
	poolReadyTimeout = 15 * time.Minute
	// poolWaiterTTL is how long a queued session, or a failure that is
	// waiting to be read, is kept without its caller asking again. Callers
	// ask every few seconds.
	poolWaiterTTL = time.Minute
)

// PoolInput is the input of the pool workflow, and what it hands to the next
// run on Continue-As-New.
type PoolInput struct {
	Spec    domain.PoolSpec `json:"spec"`
	Workers []PoolWorker    `json:"workers,omitempty"`
	Placed  []PoolSession   `json:"placed,omitempty"`
	Waiting []PoolWaiter    `json:"waiting,omitempty"`
	// Failed are sessions that could not be placed, with the reason, until
	// their callers have read it.
	Failed []PoolWaiter `json:"failed,omitempty"`
	// Checks counts the checks that were made.
	Checks int `json:"checks,omitempty"`
}

// PoolWorker is a worker of a pool.
type PoolWorker struct {
	Slot int `json:"slot"`
	// Since is when the worker was made.
	Since time.Time `json:"since"`
	// EmptyFrom is when it last had no session. Zero while it has one.
	EmptyFrom time.Time `json:"empty_from,omitzero"`
}

// PoolSession is a session placed on a worker.
type PoolSession struct {
	ID    string `json:"id"`
	Owner string `json:"owner,omitempty"`
	Slot  int    `json:"slot"`
}

// PoolWaiter is a session that waits for room, or one that could not be
// placed, with the reason in Error.
type PoolWaiter struct {
	Session string `json:"session"`
	Owner   string `json:"owner,omitempty"`
	Error   string `json:"error,omitempty"`
	// Seen is when its caller last asked, or for an error when it was made.
	// A caller that stops asking (its activity was cancelled) is dropped.
	Seen time.Time `json:"seen,omitzero"`
}

var poolActivityOptions = workflow.ActivityOptions{
	TaskQueue:           domain.KubernetesQueue,
	StartToCloseTimeout: 20 * time.Minute,
	HeartbeatTimeout:    time.Minute,
	RetryPolicy:         &temporal.RetryPolicy{InitialInterval: 2 * time.Second, BackoffCoefficient: 2, MaximumInterval: 30 * time.Second, MaximumAttempts: 5},
}

func registerPoolWorkflow(w worker.Worker) {
	w.RegisterWorkflowWithOptions(Pool, workflow.RegisterOptions{Name: domain.WorkflowPool})
}

type pool struct {
	PoolInput
	dirty bool
	// wantCheck asks the loop to check at once. checks counts them, for the
	// handlers that wait for one.
	wantCheck bool
	nextCheck time.Time
}

// Pool is the workflow of one pool. See the comment of this file.
func Pool(ctx workflow.Context, input PoolInput) error {
	p := &pool{PoolInput: input}
	if err := p.Spec.Validate(); err != nil {
		return temporal.NewNonRetryableApplicationError(err.Error(), "PoolSpec", err)
	}
	if err := workflow.SetQueryHandler(ctx, domain.QueryPoolState, func() (domain.PoolState, error) { return p.state(), nil }); err != nil {
		return err
	}
	if err := workflow.SetUpdateHandlerWithOptions(ctx, domain.UpdatePoolPlace, p.place, workflow.UpdateHandlerOptions{Validator: p.validatePlace}); err != nil {
		return err
	}
	if err := workflow.SetUpdateHandler(ctx, domain.UpdatePoolRelease, p.release); err != nil {
		return err
	}
	for {
		p.dirty = false
		p.schedule(ctx)
		if p.empty() && workflow.AllHandlersFinished(ctx) {
			return nil
		}
		if workflow.GetInfo(ctx).GetContinueAsNewSuggested() && workflow.AllHandlersFinished(ctx) {
			return workflow.NewContinueAsNewError(ctx, Pool, p.PoolInput)
		}
		wake := p.wakeIn(ctx)
		if wake <= 0 {
			continue
		}
		_, _ = workflow.AwaitWithTimeout(ctx, wake, func() bool { return p.dirty })
	}
}

func (p *pool) empty() bool {
	return len(p.Workers) == 0 && len(p.Placed) == 0 && len(p.Waiting) == 0 && len(p.Failed) == 0
}

func (p *pool) state() domain.PoolState {
	state := domain.PoolState{Spec: p.Spec}
	for _, w := range p.Workers {
		entry := domain.PoolWorkerState{
			Slot: w.Slot, Pod: domain.PoolPodName(p.Spec.Name, w.Slot), TaskQueue: domain.PoolQueue(p.Spec.Name, w.Slot), EmptyFrom: w.EmptyFrom,
		}
		for _, s := range p.Placed {
			if s.Slot == w.Slot {
				entry.Sessions = append(entry.Sessions, s.ID)
			}
		}
		state.Workers = append(state.Workers, entry)
	}
	for _, w := range p.Waiting {
		state.Waiting = append(state.Waiting, w.Session)
	}
	return state
}

func (p *pool) placement(session string) (domain.PoolPlacement, bool) {
	for _, s := range p.Placed {
		if s.ID == session {
			return domain.PoolPlacement{Worker: s.Slot, Pod: domain.PoolPodName(p.Spec.Name, s.Slot), TaskQueue: domain.PoolQueue(p.Spec.Name, s.Slot)}, true
		}
	}
	return domain.PoolPlacement{}, false
}

func (p *pool) waiter(session string) (int, bool) {
	index := slices.IndexFunc(p.Waiting, func(w PoolWaiter) bool { return w.Session == session })
	return index, index >= 0
}

func (p *pool) worker(slot int) (*PoolWorker, bool) {
	index := slices.IndexFunc(p.Workers, func(w PoolWorker) bool { return w.Slot == slot })
	if index < 0 {
		return nil, false
	}
	return &p.Workers[index], true
}

func (p *pool) load(slot int) int {
	count := 0
	for _, s := range p.Placed {
		if s.Slot == slot {
			count++
		}
	}
	return count
}

func (p *pool) validatePlace(_ workflow.Context, request domain.PlaceRequest) error {
	if err := request.Pool.Validate(); err != nil {
		return err
	}
	if request.Pool.Name != p.Spec.Name {
		return fmt.Errorf("this is pool %s, not %s", p.Spec.Name, request.Pool.Name)
	}
	if request.Session == "" {
		return errors.New("place: no session")
	}
	return nil
}

// place answers where a session runs, or that it does not run anywhere yet.
// It never waits: a session that has no room is queued, in order, and its
// caller asks again.
//
// A session that is placed is checked against the pool's pods first, because
// the worker it was placed on may be gone, and then it is placed anew: this is
// how a session whose pod was lost gets another worker. The first ask of such
// a session asks for the check and is answered Pending with the number of the
// last one, the next carries that number in After.
func (p *pool) place(ctx workflow.Context, request domain.PlaceRequest) (domain.PoolPlacement, error) {
	p.Spec = request.Pool
	session := request.Session
	if index := slices.IndexFunc(p.Failed, func(w PoolWaiter) bool { return w.Session == session }); index >= 0 {
		failure := p.Failed[index].Error
		p.Failed = slices.Delete(p.Failed, index, index+1)
		return domain.PoolPlacement{}, temporal.NewNonRetryableApplicationError(failure, "PoolWorker", nil)
	}
	if placement, placed := p.placement(session); placed {
		switch {
		case request.Queued:
			return placement, nil
		case request.After != nil && p.Checks > *request.After:
			return placement, nil
		case request.After != nil:
			p.wantCheck, p.dirty = true, true
			return domain.PoolPlacement{Pending: true, Checks: *request.After}, nil
		}
		p.wantCheck, p.dirty = true, true
		return domain.PoolPlacement{Pending: true, Checks: p.Checks}, nil
	}
	if index, waiting := p.waiter(session); waiting {
		p.Waiting[index].Seen = workflow.Now(ctx)
	} else {
		p.Waiting = append(p.Waiting, PoolWaiter{Session: session, Owner: request.Owner, Seen: workflow.Now(ctx)})
		p.dirty = true
	}
	return domain.PoolPlacement{Pending: true, Queued: true}, nil
}

func (p *pool) release(ctx workflow.Context, request domain.ReleaseRequest) error {
	p.removeSession(ctx, request.Session)
	p.dirty = true
	return nil
}

// removeSession takes a session out of the pool's placements and queue, and
// notes the time a worker became empty.
func (p *pool) removeSession(ctx workflow.Context, session string) {
	if index, ok := p.waiter(session); ok {
		p.Waiting = slices.Delete(p.Waiting, index, index+1)
	}
	index := slices.IndexFunc(p.Placed, func(s PoolSession) bool { return s.ID == session })
	if index < 0 {
		return
	}
	slot := p.Placed[index].Slot
	p.Placed = slices.Delete(p.Placed, index, index+1)
	if w, ok := p.worker(slot); ok && p.load(slot) == 0 {
		w.EmptyFrom = workflow.Now(ctx)
	}
}

// wakeIn is how long until the loop has something to do without being told.
func (p *pool) wakeIn(ctx workflow.Context) time.Duration {
	now := workflow.Now(ctx)
	wake := time.Hour
	for _, w := range slices.Concat(p.Waiting, p.Failed) {
		wake = min(wake, w.Seen.Add(poolWaiterTTL).Sub(now))
	}
	if len(p.Workers) == 0 {
		return wake
	}
	wake = min(wake, p.nextCheck.Sub(now))
	for _, w := range p.Workers {
		if !w.EmptyFrom.IsZero() {
			wake = min(wake, w.EmptyFrom.Add(p.Spec.Idle()).Sub(now))
		}
	}
	return wake
}

// schedule does what the pool owes: a check when it is due, places for the
// sessions that wait, and the removal of workers that were empty long enough.
func (p *pool) schedule(ctx workflow.Context) {
	now := workflow.Now(ctx)
	// Callers that stopped asking are not placed: nobody would use the place.
	alive := func(w PoolWaiter) bool { return now.Sub(w.Seen) < poolWaiterTTL }
	p.Waiting = slices.DeleteFunc(p.Waiting, func(w PoolWaiter) bool { return !alive(w) })
	p.Failed = slices.DeleteFunc(p.Failed, func(w PoolWaiter) bool { return !alive(w) })
	if len(p.Workers) > 0 && (p.wantCheck || !now.Before(p.nextCheck)) {
		p.check(ctx)
	}
	for len(p.Waiting) > 0 {
		slot, ok := p.freeWorker()
		if !ok {
			if len(p.Workers) >= p.Spec.MaxWorkers {
				break
			}
			var failure string
			slot, failure = p.newWorker(ctx)
			// The activity blocked, and a release or a check may have taken
			// the waiting session out of the queue meanwhile. The worker
			// stays, empty, and goes after its idle timeout.
			if len(p.Waiting) == 0 {
				break
			}
			if failure != "" {
				// This session cannot be placed. The next one may be.
				failed := p.Waiting[0]
				failed.Error, failed.Seen = failure, workflow.Now(ctx)
				p.Waiting = p.Waiting[1:]
				p.Failed = append(p.Failed, failed)
				continue
			}
		}
		waiter := p.Waiting[0]
		p.Waiting = p.Waiting[1:]
		p.Placed = append(p.Placed, PoolSession{ID: waiter.Session, Owner: waiter.Owner, Slot: slot})
		if w, found := p.worker(slot); found {
			w.EmptyFrom = time.Time{}
		}
	}
	if len(p.Waiting) == 0 {
		p.removeIdle(ctx)
	}
}

// freeWorker is the fullest worker that has room, so that the others empty and
// go.
func (p *pool) freeWorker() (int, bool) {
	best, bestLoad := 0, -1
	for _, w := range p.Workers {
		if load := p.load(w.Slot); load < p.Spec.SessionsPerWorker && load > bestLoad {
			best, bestLoad = w.Slot, load
		}
	}
	return best, bestLoad >= 0
}

// newWorker makes a worker in the lowest free slot, and returns the slot, or
// why it could not.
func (p *pool) newWorker(ctx workflow.Context) (int, string) {
	slot := 1
	for slices.ContainsFunc(p.Workers, func(w PoolWorker) bool { return w.Slot == slot }) {
		slot++
	}
	var placement domain.PoolPlacement
	activityCtx := workflow.WithActivityOptions(ctx, poolActivityOptions)
	err := workflow.ExecuteActivity(activityCtx, domain.ActivityStartPoolWorker, domain.PoolWorkerRequest{Pool: p.Spec, Slot: slot}).Get(ctx, &placement)
	if err != nil {
		return 0, fmt.Sprintf("start a worker of pool %s: %v", p.Spec.Name, err)
	}
	p.Workers = append(p.Workers, PoolWorker{Slot: slot, Since: workflow.Now(ctx), EmptyFrom: workflow.Now(ctx)})
	return slot, ""
}

func (p *pool) deleteWorkerPod(ctx workflow.Context, slot int) {
	activityCtx := workflow.WithActivityOptions(ctx, poolActivityOptions)
	if err := workflow.ExecuteActivity(activityCtx, domain.ActivityDeletePoolWorker, domain.PoolWorkerRequest{Pool: p.Spec, Slot: slot}).Get(ctx, nil); err != nil {
		workflow.GetLogger(ctx).Warn("delete a pool worker", "pool", p.Spec.Name, "slot", slot, "error", err)
	}
}

// removeIdle deletes the workers that have had no session for IdleTimeout.
func (p *pool) removeIdle(ctx workflow.Context) {
	now := workflow.Now(ctx)
	for _, w := range slices.Clone(p.Workers) {
		if p.load(w.Slot) > 0 || w.EmptyFrom.IsZero() || now.Sub(w.EmptyFrom) < p.Spec.Idle() {
			continue
		}
		p.deleteWorkerPod(ctx, w.Slot)
		p.Workers = slices.DeleteFunc(p.Workers, func(other PoolWorker) bool { return other.Slot == w.Slot })
	}
}

// check looks at the pool's pods and at the owners of its sessions. A worker
// whose pod is gone, ending, failed or not ready for too long is lost: its
// sessions lose their place and are placed again when their callers ask. A pod
// that no worker of the pool has is deleted. A session whose owner is gone is
// closed and released.
func (p *pool) check(ctx workflow.Context) {
	now := workflow.Now(ctx)
	p.wantCheck = false
	request := domain.PoolCheckRequest{Pool: p.Spec.Name, Sessions: map[string]string{}}
	for _, s := range p.Placed {
		request.Sessions[s.ID] = s.Owner
	}
	var result domain.PoolCheckResult
	activityCtx := workflow.WithActivityOptions(ctx, poolActivityOptions)
	if err := workflow.ExecuteActivity(activityCtx, domain.ActivityCheckPool, request).Get(ctx, &result); err != nil {
		workflow.GetLogger(ctx).Warn("check a pool", "pool", p.Spec.Name, "error", err)
		p.nextCheck = now.Add(poolCheckEvery)
		// A caller that waits for a check must not wait for ever.
		p.Checks++
		return
	}
	pods := map[int]domain.PoolPod{}
	for _, pod := range result.Pods {
		pods[pod.Slot] = pod
	}
	for _, w := range slices.Clone(p.Workers) {
		pod, exists := pods[w.Slot]
		var reason string
		switch {
		case !exists && now.Sub(w.Since) > poolMissingAfter:
			reason = "its pod is gone"
		case exists && pod.Terminating:
			reason = "its pod is ending"
		case exists && pod.Failed:
			reason = "its pod failed"
		case exists && !pod.Ready && !pod.NotReadySince.IsZero() && now.Sub(pod.NotReadySince) > poolReadyTimeout:
			reason = "its pod is not ready"
		}
		if reason == "" {
			continue
		}
		workflow.GetLogger(ctx).Info("pool worker lost", "pool", p.Spec.Name, "slot", w.Slot, "reason", reason)
		p.Workers = slices.DeleteFunc(p.Workers, func(other PoolWorker) bool { return other.Slot == w.Slot })
		p.Placed = slices.DeleteFunc(p.Placed, func(s PoolSession) bool { return s.Slot == w.Slot })
		if exists && !pod.Terminating {
			p.deleteWorkerPod(ctx, w.Slot)
		}
	}
	for _, pod := range result.Pods {
		if _, known := p.worker(pod.Slot); known || pod.Terminating || now.Sub(pod.Created) < poolOrphanAge {
			continue
		}
		workflow.GetLogger(ctx).Info("pool pod of no worker deleted", "pool", p.Spec.Name, "pod", pod.Name)
		p.deleteWorkerPod(ctx, pod.Slot)
	}
	for _, session := range result.OwnersGone {
		p.closeOrphan(ctx, session)
	}
	p.nextCheck = now.Add(poolCheckEvery)
	p.Checks++
}

// closeOrphan closes a session whose owner is not running, on its worker, so
// that the worker writes no more snapshots of it, and deletes its snapshot.
func (p *pool) closeOrphan(ctx workflow.Context, session string) {
	index := slices.IndexFunc(p.Placed, func(s PoolSession) bool { return s.ID == session })
	if index < 0 {
		return
	}
	slot := p.Placed[index].Slot
	workflow.GetLogger(ctx).Info("pool session closed, its owner is not running", "pool", p.Spec.Name, "session", session)
	closeCtx := workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		TaskQueue: domain.PoolQueue(p.Spec.Name, slot), ScheduleToStartTimeout: 30 * time.Second, StartToCloseTimeout: 2 * time.Minute,
		RetryPolicy: &temporal.RetryPolicy{MaximumAttempts: 1},
	})
	if err := workflow.ExecuteActivity(closeCtx, domain.ActivityCloseSession, domain.CloseRequest{Session: session}).Get(ctx, nil); err != nil {
		workflow.GetLogger(ctx).Warn("close the session of a pool", "pool", p.Spec.Name, "session", session, "error", err)
	}
	activityCtx := workflow.WithActivityOptions(ctx, poolActivityOptions)
	if err := workflow.ExecuteActivity(activityCtx, domain.ActivityDeletePoolSnapshots, domain.PoolSnapshotsRequest{Pool: p.Spec.Name, Session: session}).Get(ctx, nil); err != nil {
		workflow.GetLogger(ctx).Warn("delete the snapshot of a pool's session", "pool", p.Spec.Name, "session", session, "error", err)
	}
	p.removeSession(ctx, session)
}
