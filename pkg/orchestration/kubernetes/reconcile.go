package kubernetes

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"net/url"
	"sync"
	"time"

	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"

	"github.com/gfx-labs/protocol7/pkg/domain"
)

// Reasons a pod is deleted. They are logged with the session's ID.
const (
	ReasonOwnerGone    = "its owner workflow is not running"
	ReasonSessionClose = "its session is closed"
	ReasonNotReady     = "it is not ready after its open timeout"
	ReasonPoolGone     = "its pool is not running"
)

const (
	defaultReconcileInterval = 5 * time.Minute
	defaultOpenTimeout       = 15 * time.Minute
	// probeTimeout bounds the question to a session's pod. A pod that does
	// not answer is not deleted for that.
	probeTimeout = 30 * time.Second
)

// ProbeSessionPod asks the worker in a session's pod, on the session's
// queue, whether the session is closed. The reconciliation starts it as a
// workflow because only a workflow can run an activity.
func ProbeSessionPod(ctx workflow.Context, session string) (domain.SessionState, error) {
	var state domain.SessionState
	ctx = workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		TaskQueue:              domain.SessionQueue(session),
		ScheduleToStartTimeout: probeTimeout,
		StartToCloseTimeout:    probeTimeout,
		RetryPolicy:            &temporal.RetryPolicy{MaximumAttempts: 1},
	})
	err := workflow.ExecuteActivity(ctx, domain.ActivitySessionState, domain.CloseRequest{Session: session}).Get(ctx, &state)
	return state, err
}

// Reconciler deletes session pods that nothing owns: a workflow that failed,
// was terminated or lost its CloseSession leaves its session's pod running.
//
// Every `p7 pods` replica has one. Start makes them campaign for a
// Kubernetes Lease (see lease.go): only the replica that holds it runs the
// passes, and another takes over when it stops or dies.
type Reconciler struct {
	pods     *Pods
	temporal client.Client
	logger   *slog.Logger
	now      func() time.Time

	mu     sync.Mutex
	cancel context.CancelFunc
	done   chan struct{}
}

// NewReconciler returns a reconciler of pods' session pods. It does nothing
// until Start.
func NewReconciler(pods *Pods, temporalClient client.Client) *Reconciler {
	return &Reconciler{pods: pods, temporal: temporalClient, logger: pods.logger, now: time.Now}
}

// Start joins the election for the lease and returns at once. While this
// replica holds the lease it reconciles at once and then every
// Config.ReconcileInterval. It stops reconciling when it loses the lease or
// at Stop, and a replica that does not hold the lease does nothing.
func (r *Reconciler) Start() {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.cancel != nil {
		return
	}
	ctx, cancel := context.WithCancel(context.Background())
	r.cancel, r.done = cancel, make(chan struct{})
	go func() {
		defer close(r.done)
		r.pods.newElector().campaign(ctx, r.passes)
	}()
}

// Stop ends the reconciliation, gives the lease back when this replica holds
// it, and waits for a pass in progress.
func (r *Reconciler) Stop() {
	r.mu.Lock()
	cancel, done := r.cancel, r.done
	r.cancel = nil
	r.mu.Unlock()
	if cancel != nil {
		cancel()
		<-done
	}
}

// passes runs a pass now and then each interval, until ctx ends: the lease is
// lost or the reconciler stops.
func (r *Reconciler) passes(ctx context.Context) {
	interval := r.pods.config.ReconcileInterval
	if interval <= 0 {
		interval = defaultReconcileInterval
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for ctx.Err() == nil {
		if err := r.Reconcile(ctx); err != nil && ctx.Err() == nil {
			r.logger.Error("reconcile session pods", "error", err)
		}
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

// podObject is the part of a pod that reconciliation reads.
type podObject struct {
	Metadata struct {
		Name              string            `json:"name"`
		UID               string            `json:"uid"`
		ResourceVersion   string            `json:"resourceVersion"`
		Labels            map[string]string `json:"labels"`
		Annotations       map[string]string `json:"annotations"`
		CreationTimestamp time.Time         `json:"creationTimestamp"`
		DeletionTimestamp *time.Time        `json:"deletionTimestamp"`
	} `json:"metadata"`
	Status struct {
		Phase      string `json:"phase"`
		Conditions []struct {
			Type               string    `json:"type"`
			Status             string    `json:"status"`
			LastTransitionTime time.Time `json:"lastTransitionTime"`
		} `json:"conditions"`
	} `json:"status"`
}

type podList struct {
	Metadata struct {
		Continue string `json:"continue"`
	} `json:"metadata"`
	Items []podObject `json:"items"`
}

// notReadySince says since when a pod has not been ready. It is false for a
// ready pod.
func (p *podObject) notReadySince() (time.Time, bool) {
	for _, condition := range p.Status.Conditions {
		if condition.Type == "Ready" {
			if condition.Status == "True" {
				return time.Time{}, false
			}
			if !condition.LastTransitionTime.IsZero() {
				return condition.LastTransitionTime, true
			}
		}
	}
	return p.Metadata.CreationTimestamp, true
}

// snapshotDeleteWait bounds the wait for a pod to end before its session's
// snapshot is deleted by reconciliation.
const snapshotDeleteWait = 15 * time.Minute

// list returns the pods this backend created.
func (p *Pods) list(ctx context.Context) ([]podObject, error) {
	var pods []podObject
	var next string
	for {
		query := url.Values{"labelSelector": {LabelManaged + "=true"}, "limit": {"500"}}
		if next != "" {
			query.Set("continue", next)
		}
		var page podList
		if err := p.do(ctx, http.MethodGet, p.podsPath()+"?"+query.Encode(), nil, &page); err != nil {
			return nil, fmt.Errorf("list session pods: %w", err)
		}
		pods = append(pods, page.Items...)
		if next = page.Metadata.Continue; next == "" {
			return pods, nil
		}
	}
}

// Reconcile makes one pass over the session pods this backend created and
// deletes those that nothing owns. A check that cannot be made, for example
// because Temporal does not answer, leaves its pod for the next pass.
func (r *Reconciler) Reconcile(ctx context.Context) error {
	pods, err := r.pods.list(ctx)
	if err != nil {
		return err
	}
	var failed []error
	// Sessions whose snapshot goes once their pod has ended.
	var abandoned []string
	for index := range pods {
		pod := &pods[index]
		if pod.Metadata.DeletionTimestamp != nil {
			continue
		}
		if pool := pod.Metadata.Labels[LabelPool]; pool != "" {
			if err := r.reconcilePool(ctx, pod, pool); err != nil {
				failed = append(failed, err)
			}
			continue
		}
		session := pod.Metadata.Labels[LabelSession]
		if session == "" {
			continue
		}
		reason, err := r.unowned(ctx, pod, session)
		if err != nil {
			r.logger.Warn("check a session pod", "session", session, "pod", pod.Metadata.Name, "error", err)
			continue
		}
		if reason == "" {
			continue
		}
		if err := r.pods.deletePod(ctx, pod.Metadata.Name, pod.Metadata.UID); err != nil {
			failed = append(failed, fmt.Errorf("delete the pod of session %s: %w", session, err))
			continue
		}
		r.logger.Info("session pod deleted by reconciliation", "session", session, "pod", pod.Metadata.Name, "reason", reason)
		// A session whose owner is gone or that is closed will not be
		// used again. One whose pod was only slow to start may still be
		// retried by its owner, so its snapshot stays.
		if reason != ReasonNotReady {
			abandoned = append(abandoned, session)
		}
	}
	for _, session := range abandoned {
		// The pods end at the same time, so the waits overlap.
		waitCtx, cancel := context.WithTimeout(ctx, snapshotDeleteWait)
		err := r.pods.deleteSnapshotWhenGone(waitCtx, session)
		cancel()
		if err != nil {
			failed = append(failed, fmt.Errorf("delete the snapshot of session %s: %w", session, err))
		}
	}
	return errors.Join(failed...)
}

// unowned returns why a pod should be deleted, or "" when it should stay.
func (r *Reconciler) unowned(ctx context.Context, pod *podObject, session string) (string, error) {
	if owner := pod.Metadata.Annotations[AnnotationOwner]; owner != "" {
		running, err := r.ownerRunning(ctx, owner)
		if err != nil {
			return "", err
		}
		if !running {
			return ReasonOwnerGone, nil
		}
	}
	if since, notReady := pod.notReadySince(); notReady {
		timeout := r.pods.config.OpenTimeout
		if timeout <= 0 {
			timeout = defaultOpenTimeout
		}
		if annotated, err := time.ParseDuration(pod.Metadata.Annotations[AnnotationOpenTimeout]); err == nil && annotated > 0 {
			timeout = annotated
		}
		if r.now().Sub(since) > timeout {
			return ReasonNotReady, nil
		}
		// It cannot answer yet.
		return "", nil
	}
	state, err := r.probe(ctx, session)
	if err != nil {
		return "", fmt.Errorf("ask the pod whether its session is closed: %w", err)
	}
	if state.Closed {
		return ReasonSessionClose, nil
	}
	return "", nil
}

// ownerRunning says whether the latest run of a workflow is running.
func (r *Reconciler) ownerRunning(ctx context.Context, workflowID string) (bool, error) {
	return workflowRunning(ctx, r.temporal, workflowID)
}

// workflowRunning says whether the latest run of a workflow is running.
func workflowRunning(ctx context.Context, temporalClient client.Client, workflowID string) (bool, error) {
	described, err := temporalClient.DescribeWorkflowExecution(ctx, workflowID, "")
	var notFound *serviceerror.NotFound
	switch {
	case errors.As(err, &notFound):
		return false, nil
	case err != nil:
		return false, fmt.Errorf("look up workflow %s: %w", workflowID, err)
	}
	return described.GetWorkflowExecutionInfo().GetStatus() == enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING, nil
}

func (r *Reconciler) probe(ctx context.Context, session string) (domain.SessionState, error) {
	var state domain.SessionState
	ctx, cancel := context.WithTimeout(ctx, 2*probeTimeout)
	defer cancel()
	run, err := r.temporal.ExecuteWorkflow(ctx, client.StartWorkflowOptions{
		// One entry in the history of workflows for each session.
		ID:                       "p7.probe/" + session,
		TaskQueue:                domain.KubernetesQueue,
		WorkflowExecutionTimeout: 2 * probeTimeout,
		WorkflowIDReusePolicy:    enumspb.WORKFLOW_ID_REUSE_POLICY_ALLOW_DUPLICATE,
	}, domain.WorkflowProbeSessionPod, session)
	if err != nil {
		return state, err
	}
	err = run.Get(ctx, &state)
	return state, err
}
