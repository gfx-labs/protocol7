package kubernetes

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"time"

	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/worker"

	"github.com/gfx-labs/protocol7/pkg/domain"
	"github.com/gfx-labs/protocol7/pkg/execution/snapshot"
)

// Pools: the pods of a pool's workers, and the activities by which the pool's
// workflow (poolworkflow.go) and the workflows that open sessions use them.
//
// A worker is the pod p7-pool-<pool>-<slot>, running `p7 session --pool
// <pool> --worker <slot>`: the host session manager on the queue of its slot.
// The pod carries the label of its pool and none of a session, so it has the
// pool's SPIFFE ID, which Temporal limits to the queues of the pool's slots
// and PDP to the pool's snapshot prefix.

// poolActivities are the activities of pools. They run on the backend's
// queue.
type poolActivities struct {
	pods     *Pods
	temporal client.Client
}

// registerPools adds the pool workflow and its activities to a worker.
func registerPools(w worker.Worker, pods *Pods, temporalClient client.Client) {
	a := &poolActivities{pods: pods, temporal: temporalClient}
	register := func(name string, fn any) { w.RegisterActivityWithOptions(fn, activity.RegisterOptions{Name: name}) }
	register(domain.ActivityPlaceSession, a.PlaceSession)
	register(domain.ActivityReleaseSession, a.ReleaseSession)
	register(domain.ActivityStartPoolWorker, a.StartWorker)
	register(domain.ActivityDeletePoolWorker, a.DeleteWorker)
	register(domain.ActivityCheckPool, a.Check)
	register(domain.ActivityDeletePoolSnapshots, a.DeleteSnapshots)
	registerPoolWorkflow(w)
}

func refusedRequest(err error) error {
	return temporal.NewNonRetryableApplicationError(err.Error(), "PoolRequest", err)
}

func (a *poolActivities) poolsSelector(pool string) string {
	return url.Values{"labelSelector": {LabelPool + "=" + pool}}.Encode()
}

// StartWorker makes the pod of a worker, unless it exists. A pod of the
// slot that is ending is waited for, because it keeps the name until it is
// gone, and the wait heartbeats.
func (a *poolActivities) StartWorker(ctx context.Context, request domain.PoolWorkerRequest) (domain.PoolPlacement, error) {
	placement := domain.PoolPlacement{Worker: request.Slot, Pod: domain.PoolPodName(request.Pool.Name, request.Slot), TaskQueue: domain.PoolQueue(request.Pool.Name, request.Slot)}
	if err := request.Pool.Validate(); err != nil {
		return placement, refusedRequest(err)
	}
	if request.Slot < 1 || request.Slot > domain.MaxPoolWorkers {
		return placement, refusedRequest(fmt.Errorf("pool %s: slot %d is not 1 to %d", request.Pool.Name, request.Slot, domain.MaxPoolWorkers))
	}
	path := a.pods.podsPath() + "/" + placement.Pod
	for attempt := 0; attempt < 20; attempt++ {
		var existing podObject
		err := a.pods.do(ctx, http.MethodGet, path, nil, &existing)
		switch {
		case isStatus(err, http.StatusNotFound):
			pod, err := a.pods.renderWorker(request, placement)
			if err != nil {
				return placement, temporal.NewNonRetryableApplicationError(err.Error(), "PodTemplate", err)
			}
			err = a.pods.do(ctx, http.MethodPost, a.pods.podsPath(), pod, nil)
			switch {
			case err == nil:
				a.pods.logger.Info("pool worker created", "pool", request.Pool.Name, "slot", request.Slot, "pod", placement.Pod)
				return placement, nil
			case isStatus(err, http.StatusConflict):
				continue
			}
			return placement, err
		case err != nil:
			return placement, err
		}
		if existing.Metadata.DeletionTimestamp != nil {
			if err := a.pods.waitGone(ctx, placement.Pod); err != nil {
				return placement, err
			}
			continue
		}
		return placement, nil
	}
	return placement, fmt.Errorf("start the pool worker %s: its pod kept changing", placement.Pod)
}

// renderWorker makes a worker's pod from the template and the pool's changes
// to it.
func (p *Pods) renderWorker(request domain.PoolWorkerRequest, placement domain.PoolPlacement) (map[string]any, error) {
	pod, container, err := p.template()
	if err != nil {
		return nil, err
	}
	metadata := object(pod, "metadata")
	metadata["name"] = placement.Pod
	labels := object(metadata, "labels")
	labels[LabelManaged] = "true"
	labels[LabelPool] = request.Pool.Name
	labels[LabelWorker] = strconv.Itoa(request.Slot)
	setEnv(container, map[string]string{
		EnvPool: request.Pool.Name, EnvWorker: strconv.Itoa(request.Slot), EnvTaskQueue: placement.TaskQueue,
	})
	if image := request.Pool.Pod.Image; image != "" {
		container["image"] = image
	}
	if len(request.Pool.Pod.Requests) > 0 || len(request.Pool.Pod.Limits) > 0 {
		resources := object(container, "resources")
		for field, values := range map[string]map[string]string{"requests": request.Pool.Pod.Requests, "limits": request.Pool.Pod.Limits} {
			if len(values) == 0 {
				continue
			}
			target := object(resources, field)
			for name, quantity := range values {
				target[name] = quantity
			}
		}
	}
	return pod, nil
}

// DeleteWorker deletes a worker's pod. A pod that is gone is not an error.
// It does not wait for the pod to end: the worker writes a last snapshot of
// its sessions when it stops, which takes minutes, and StartWorker waits for
// a pod of the same slot.
func (a *poolActivities) DeleteWorker(ctx context.Context, request domain.PoolWorkerRequest) error {
	if err := domain.ValidPool(request.Pool.Name); err != nil {
		return refusedRequest(err)
	}
	name := domain.PoolPodName(request.Pool.Name, request.Slot)
	if err := a.pods.deletePod(ctx, name, ""); err != nil {
		return err
	}
	a.pods.logger.Info("pool worker deleted", "pool", request.Pool.Name, "slot", request.Slot, "pod", name)
	return nil
}

// DeleteSnapshots deletes a session's snapshot under its pool's prefix, for a
// session that was closed on its worker, which wrote no more, or whose worker
// is gone.
func (a *poolActivities) DeleteSnapshots(ctx context.Context, request domain.PoolSnapshotsRequest) error {
	if a.pods.snapshots == nil {
		return nil
	}
	if err := domain.ValidPool(request.Pool); err != nil {
		return refusedRequest(err)
	}
	config := a.pods.config.Snapshots
	config.Prefix = domain.PoolSnapshotPrefix(config.Prefix, request.Pool)
	store, err := snapshot.New(config, a.pods.logger)
	if err != nil {
		return err
	}
	if err := store.Delete(ctx, request.Session); err != nil {
		return fmt.Errorf("delete the snapshot of session %s in pool %s: %w", request.Session, request.Pool, err)
	}
	return nil
}

// Check lists the pool's worker pods, and the placed sessions whose owner is
// not running.
func (a *poolActivities) Check(ctx context.Context, request domain.PoolCheckRequest) (domain.PoolCheckResult, error) {
	var result domain.PoolCheckResult
	if err := domain.ValidPool(request.Pool); err != nil {
		return result, refusedRequest(err)
	}
	var list podList
	if err := a.pods.do(ctx, http.MethodGet, a.pods.podsPath()+"?"+a.poolsSelector(request.Pool), nil, &list); err != nil {
		return result, fmt.Errorf("list the pods of pool %s: %w", request.Pool, err)
	}
	for index := range list.Items {
		pod := &list.Items[index]
		slot, err := strconv.Atoi(pod.Metadata.Labels[LabelWorker])
		if err != nil || pod.Metadata.Labels[LabelManaged] != "true" {
			continue
		}
		entry := domain.PoolPod{
			Slot: slot, Name: pod.Metadata.Name, Created: pod.Metadata.CreationTimestamp,
			Terminating: pod.Metadata.DeletionTimestamp != nil,
			Failed:      pod.Status.Phase == "Failed" || pod.Status.Phase == "Succeeded",
		}
		since, notReady := pod.notReadySince()
		entry.Ready, entry.NotReadySince = !notReady, since
		result.Pods = append(result.Pods, entry)
	}
	for session, owner := range request.Sessions {
		if owner == "" {
			continue
		}
		running, err := workflowRunning(ctx, a.temporal, owner)
		if err != nil {
			// Not knowing is not a reason to close a session.
			a.pods.logger.Warn("check a pool session's owner", "pool", request.Pool, "session", session, "error", err)
			continue
		}
		if !running {
			result.OwnersGone = append(result.OwnersGone, session)
		}
	}
	return result, nil
}

// beats records a heartbeat every ten seconds until the returned function is
// called.
func beats(ctx context.Context) func() {
	done := make(chan struct{})
	stopped := make(chan struct{})
	go func() {
		defer close(stopped)
		ticker := time.NewTicker(10 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				activity.RecordHeartbeat(ctx)
			case <-done:
				return
			}
		}
	}()
	return func() { close(done); <-stopped }
}

// PlaceSession asks the session's pool where the session runs, and starts the
// pool's workflow if it is not running. It returns when a worker has room for
// the session, which can take as long as the sessions that fill the pool,
// and it heartbeats while it waits. A retry with the same Attempt gets the
// answer of the first.
func (a *poolActivities) PlaceSession(ctx context.Context, request domain.PlaceRequest) (domain.PoolPlacement, error) {
	var placement domain.PoolPlacement
	if err := request.Pool.Validate(); err != nil {
		return placement, refusedRequest(err)
	}
	if request.Session == "" || request.Attempt == "" {
		return placement, refusedRequest(errors.New("place a session: the session and the attempt are required"))
	}
	defer beats(ctx)()
	operation := a.temporal.NewWithStartWorkflowOperation(client.StartWorkflowOptions{
		ID:                       domain.PoolWorkflowID(request.Pool.Name),
		TaskQueue:                domain.KubernetesQueue,
		WorkflowIDConflictPolicy: enumspb.WORKFLOW_ID_CONFLICT_POLICY_USE_EXISTING,
		WorkflowIDReusePolicy:    enumspb.WORKFLOW_ID_REUSE_POLICY_ALLOW_DUPLICATE,
	}, domain.WorkflowPool, PoolInput{Spec: request.Pool})
	handle, err := a.temporal.UpdateWithStartWorkflow(ctx, client.UpdateWithStartWorkflowOptions{
		StartWorkflowOperation: operation,
		UpdateOptions: client.UpdateWorkflowOptions{
			UpdateID: request.Attempt, UpdateName: domain.UpdatePoolPlace, Args: []any{request},
			WaitForStage: client.WorkflowUpdateStageCompleted,
		},
	})
	if err != nil {
		return placement, fmt.Errorf("place session %s in pool %s: %w", request.Session, request.Pool.Name, err)
	}
	if err := handle.Get(ctx, &placement); err != nil {
		var application *temporal.ApplicationError
		if errors.As(err, &application) && application.NonRetryable() {
			return placement, err
		}
		return placement, fmt.Errorf("place session %s in pool %s: %w", request.Session, request.Pool.Name, err)
	}
	return placement, nil
}

// ReleaseSession gives back a session's place in its pool. A pool that is not
// running holds none.
func (a *poolActivities) ReleaseSession(ctx context.Context, request domain.ReleaseRequest) error {
	if err := domain.ValidPool(request.Pool.Name); err != nil {
		return refusedRequest(err)
	}
	handle, err := a.temporal.UpdateWorkflow(ctx, client.UpdateWorkflowOptions{
		WorkflowID: domain.PoolWorkflowID(request.Pool.Name), UpdateID: "release-" + request.Session + "-" + strconv.FormatInt(time.Now().UnixNano(), 36),
		UpdateName: domain.UpdatePoolRelease, Args: []any{request}, WaitForStage: client.WorkflowUpdateStageCompleted,
	})
	var notFound *serviceerror.NotFound
	if errors.As(err, &notFound) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("release session %s of pool %s: %w", request.Session, request.Pool.Name, err)
	}
	if err := handle.Get(ctx, nil); err != nil {
		return fmt.Errorf("release session %s of pool %s: %w", request.Session, request.Pool.Name, err)
	}
	return nil
}

// reconcilePool judges a pool's pod in the reconciliation: the pool's own
// workflow keeps its workers, so this only deletes the pods of a pool whose
// workflow is not running, which nothing would delete.
func (r *Reconciler) reconcilePool(ctx context.Context, pod *podObject, pool string) error {
	running, err := workflowRunning(ctx, r.temporal, domain.PoolWorkflowID(pool))
	if err != nil {
		r.logger.Warn("check a pool", "pool", pool, "pod", pod.Metadata.Name, "error", err)
		return nil
	}
	// A pod made a moment before its workflow started has none yet.
	if running || r.now().Sub(pod.Metadata.CreationTimestamp) < poolGrace {
		return nil
	}
	if err := r.pods.deletePod(ctx, pod.Metadata.Name, pod.Metadata.UID); err != nil {
		return fmt.Errorf("delete the pod of pool %s: %w", pool, err)
	}
	r.logger.Info("pool pod deleted by reconciliation", "pool", pool, "pod", pod.Metadata.Name, "reason", ReasonPoolGone)
	return nil
}

// poolGrace is how long a pool's pod may exist before its pool's workflow
// is expected to be running.
const poolGrace = 5 * time.Minute
