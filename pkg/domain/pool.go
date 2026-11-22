package domain

import (
	"fmt"
	"strconv"
	"strings"
	"time"
)

// Session pools.
//
// A pool is a set of worker pods on the kubernetes backend, started on demand,
// that run the sessions of one workflow's choosing: up to SessionsPerWorker
// sessions on each of up to MaxWorkers pods. A Temporal workflow owns each pool
// (WorkflowPool). It starts a worker when every running one is full, places
// each session on a worker, queues a session when the pool is full, and removes
// a worker that has been empty for IdleTimeout. A session in a pod of its own
// is a pool whose workers hold one session each, and the host backend one
// worker that never ends; both keep their own paths for now.

const (
	// ActivityPlaceSession and ActivityReleaseSession run on the kubernetes
	// backend's queue. They ask a session's pool, by an update to its workflow,
	// where the session runs and give its place back.
	ActivityPlaceSession   = "p7.PlaceSession"
	ActivityReleaseSession = "p7.ReleaseSession"
	// The activities the pool's workflow runs for its workers, on the same
	// queue.
	ActivityStartPoolWorker     = "p7.StartPoolWorker"
	ActivityDeletePoolWorker    = "p7.DeletePoolWorker"
	ActivityCheckPool           = "p7.CheckPool"
	ActivityDeletePoolSnapshots = "p7.DeletePoolSnapshots"

	// WorkflowPool is the workflow that owns a pool. Its ID is PoolWorkflowID.
	WorkflowPool = "p7.Pool"
	// UpdatePoolPlace asks the pool where a session runs. It waits until a worker
	// has room. UpdatePoolRelease gives the place back. QueryPoolState says
	// what the pool holds.
	UpdatePoolPlace   = "p7.pool.place"
	UpdatePoolRelease = "p7.pool.release"
	QueryPoolState    = "p7.pool.state"

	// MaxPoolWorkers bounds MaxWorkers. A worker's queue is named for its slot
	// in the pool, 1 to MaxPoolWorkers, so the queues of a pool are known
	// before it runs, and a deployment can list them in the pool identity's
	// grants.
	MaxPoolWorkers = 16
	// DefaultPoolIdleTimeout is IdleTimeout when it is zero.
	DefaultPoolIdleTimeout = 5 * time.Minute
)

// PodSpec is what a pool changes in the pod template of its workers. The
// zero value is the template as it is.
type PodSpec struct {
	// Image replaces the image of the session container.
	Image string `json:"image,omitempty"`
	// Requests and Limits replace the same resources of the session container,
	// such as {"cpu": "500m", "memory": "2Gi"}.
	Requests map[string]string `json:"requests,omitempty"`
	Limits   map[string]string `json:"limits,omitempty"`
	// Env replaces the value of environment variables that the template's
	// session container already has, such as the placeholder of a token that
	// a pool of another reach sends. A variable the template lacks, and the
	// variables of the pool itself, are refused.
	Env map[string]string `json:"env,omitempty"`
}

// PoolSpec says how a pool is made. The workflow that opens a session
// chooses it in Go and passes it with the session. Every session of a pool
// must have identical permissions: they share a worker's files, processes and
// queue, and the pool's identity.
type PoolSpec struct {
	// Name identifies the pool, 8 to 32 lower case letters and digits. It
	// becomes part of pod and queue names, the pool's SPIFFE ID and its
	// snapshot prefix.
	Name string `json:"name"`
	// MaxWorkers bounds the worker pods that run at once, 1 to MaxPoolWorkers.
	MaxWorkers int `json:"max_workers"`
	// SessionsPerWorker is how many sessions one worker runs.
	SessionsPerWorker int `json:"sessions_per_worker"`
	// IdleTimeout is how long a worker stays with no session before it is
	// removed. Zero is DefaultPoolIdleTimeout.
	IdleTimeout time.Duration `json:"idle_timeout,omitempty"`
	// Pod changes the workers' pod.
	Pod PodSpec `json:"pod,omitzero"`
}

// Validate checks a pool's spec.
func (s PoolSpec) Validate() error {
	if err := ValidPool(s.Name); err != nil {
		return err
	}
	switch {
	case s.MaxWorkers < 1 || s.MaxWorkers > MaxPoolWorkers:
		return fmt.Errorf("pool %s: MaxWorkers must be 1 to %d, not %d", s.Name, MaxPoolWorkers, s.MaxWorkers)
	case s.SessionsPerWorker < 1:
		return fmt.Errorf("pool %s: SessionsPerWorker must be at least 1, not %d", s.Name, s.SessionsPerWorker)
	case s.IdleTimeout < 0:
		return fmt.Errorf("pool %s: IdleTimeout must not be negative", s.Name)
	}
	return nil
}

// Idle is IdleTimeout, or its default.
func (s PoolSpec) Idle() time.Duration {
	if s.IdleTimeout == 0 {
		return DefaultPoolIdleTimeout
	}
	return s.IdleTimeout
}

// ValidPool checks a pool's name.
func ValidPool(name string) error {
	if len(name) < 8 || len(name) > 32 {
		return fmt.Errorf("pool %q: the name must be 8 to 32 characters", name)
	}
	for _, c := range name {
		if (c < 'a' || c > 'z') && (c < '0' || c > '9') {
			return fmt.Errorf("pool %q: the name may hold only a-z and 0-9", name)
		}
	}
	return nil
}

// PoolWorkflowID is the ID of a pool's workflow.
func PoolWorkflowID(pool string) string { return "p7.pool/" + pool }

// PoolQueue is the queue of the worker in a slot of a pool. The same name is
// the worker's pod.
func PoolQueue(pool string, slot int) string { return "p7-pool-" + pool + "-" + strconv.Itoa(slot) }

// PoolPodName is the name of the pod of a pool's worker.
func PoolPodName(pool string, slot int) string { return PoolQueue(pool, slot) }

// PoolSnapshotPrefix is the prefix under which a pool's sessions keep their
// snapshots: <prefix>/pools/<pool>/<session>/workspace.tar.zst. A session
// that moves to another worker of its pool finds its snapshot where it was,
// and the pool's identity may reach that prefix and no other.
func PoolSnapshotPrefix(prefix, pool string) string {
	return strings.Trim(strings.Trim(prefix, "/")+"/pools/"+pool, "/")
}

// PlaceRequest is the input of ActivityPlaceSession and of the update
// UpdatePoolPlace. Placing a session that is placed returns its place.
type PlaceRequest struct {
	Pool    PoolSpec `json:"pool"`
	Session string   `json:"session"`
	// Owner is the ID of the workflow that opened the session. The pool
	// closes the sessions of an owner that is not running.
	Owner string `json:"owner,omitempty"`
	// Attempt is unique to one call of the activity.
	Attempt string `json:"attempt,omitempty"`
	// The update does not wait. A session that must wait for room is queued
	// and answered at once with Pending, and asks again every few seconds:
	// a waiting session holds no update open, which Temporal limits to ten
	// at a time in a workflow.
	//
	// Queued is set by a caller that was told it is queued: if the session
	// has a place by now it is returned as it is.
	Queued bool `json:"queued,omitempty"`
	// After is set by a caller that asked for a check of the pool's pods
	// (Pending with Checks): the number of the check it was told about. The
	// session's place is returned once a later check has been made.
	After *int `json:"after,omitempty"`
}

// PoolPlacement says where a session of a pool runs.
type PoolPlacement struct {
	// Worker is the slot of the worker.
	Worker int `json:"worker"`
	// Pod is the worker's pod and TaskQueue the queue its worker polls.
	Pod       string `json:"pod"`
	TaskQueue string `json:"task_queue"`
	// Pending is set by the update, and never by the activity, when the
	// session has no place yet: it is in the queue (Queued), or the pool
	// checks its pods first, and Checks is the number of the last check.
	Pending bool `json:"pending,omitempty"`
	Queued  bool `json:"queued,omitempty"`
	Checks  int  `json:"checks,omitempty"`
}

// ReleaseRequest is the input of ActivityReleaseSession and of UpdatePoolRelease.
type ReleaseRequest struct {
	Pool    PoolSpec `json:"pool"`
	Session string   `json:"session"`
}

// PoolState is what QueryPoolState returns.
type PoolState struct {
	Spec    PoolSpec          `json:"spec"`
	Workers []PoolWorkerState `json:"workers"`
	// Waiting are the sessions that wait for room, in order.
	Waiting []string `json:"waiting,omitempty"`
}

// PoolWorkerState is one worker of a pool.
type PoolWorkerState struct {
	Slot      int       `json:"slot"`
	Pod       string    `json:"pod"`
	TaskQueue string    `json:"task_queue"`
	Sessions  []string  `json:"sessions,omitempty"`
	EmptyFrom time.Time `json:"empty_from,omitzero"`
}

// PoolWorkerRequest is the input of ActivityStartPoolWorker and
// ActivityDeletePoolWorker: one worker of a pool, by its slot.
type PoolWorkerRequest struct {
	Pool PoolSpec `json:"pool"`
	Slot int      `json:"slot"`
}

// PoolSnapshotsRequest is the input of ActivityDeletePoolSnapshots.
type PoolSnapshotsRequest struct {
	Pool    string `json:"pool"`
	Session string `json:"session"`
}

// PoolCheckRequest is the input of ActivityCheckPool: the pool, and the
// sessions it has placed with the workflow that owns each.
type PoolCheckRequest struct {
	Pool     string            `json:"pool"`
	Sessions map[string]string `json:"sessions,omitempty"`
}

// PoolCheckResult is the result of ActivityCheckPool.
type PoolCheckResult struct {
	// Pods are the pool's worker pods that exist.
	Pods []PoolPod `json:"pods,omitempty"`
	// OwnersGone are the sessions whose owner workflow is not running.
	OwnersGone []string `json:"owners_gone,omitempty"`
}

// PoolPod is the state of one worker pod of a pool.
type PoolPod struct {
	Slot        int       `json:"slot"`
	Name        string    `json:"name"`
	Created     time.Time `json:"created,omitzero"`
	Terminating bool      `json:"terminating,omitempty"`
	// Failed says the pod's phase is Failed or Succeeded: it does not run.
	Failed bool `json:"failed,omitempty"`
	Ready  bool `json:"ready,omitempty"`
	// NotReadySince is when the pod was last ready, or created, if it is not
	// ready now.
	NotReadySince time.Time `json:"not_ready_since,omitzero"`
}
