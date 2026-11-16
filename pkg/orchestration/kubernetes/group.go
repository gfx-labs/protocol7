package kubernetes

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"sort"
	"strings"
	"time"

	"go.temporal.io/sdk/temporal"

	"github.com/gfx-labs/protocol7/pkg/domain"
	"github.com/gfx-labs/protocol7/pkg/execution/snapshot"
)

// Group pods.
//
// A deployment that says sessions may share a pod (a group key it derived
// from everything that decides their permissions, see domain.PodRequest.Group)
// gets one pod for the group instead of one for each session. The pod runs
// the host session manager on the group's queue, with the group's identity,
// and holds each session's directory under the session's own ID.
//
// The members of a pod are annotations on it, one for each session
// (AnnotationMemberPrefix). Nothing else keeps state: starting joins the
// group's pod or makes it, and deleting leaves it. The pod ends with its last
// member. An update of the annotations names the pod's resourceVersion, so
// that a join that races the last member's leave either lands before the
// delete or finds the pod gone, and a pod is never deleted under a session
// that was just added to it.

const (
	mergePatch = "application/merge-patch+json"
	// groupJoinAttempts bounds the passes of a join or a leave that lose a
	// race for the pod.
	groupJoinAttempts = 20
)

// memberAnnotation is the annotation that makes a session a member.
func memberAnnotation(session string) (string, error) {
	if session == "" || len(session) > 40 {
		return "", fmt.Errorf("session %q: the ID must be 1 to 40 characters", session)
	}
	for _, c := range session {
		if (c < 'a' || c > 'z') && (c < 'A' || c > 'Z') && (c < '0' || c > '9') && c != '-' && c != '_' {
			return "", fmt.Errorf("session %q: the ID may hold only letters, digits, - and _", session)
		}
	}
	return AnnotationMemberPrefix + session, nil
}

// members returns the sessions that are members of a pod, sorted, with the
// owner of each.
func (p *podObject) members() map[string]string {
	members := map[string]string{}
	for key, owner := range p.Metadata.Annotations {
		if session, ok := strings.CutPrefix(key, AnnotationMemberPrefix); ok && session != "" {
			members[session] = owner
		}
	}
	return members
}

// memberSessions lists the member sessions in order.
func (p *podObject) memberSessions() []string {
	var sessions []string
	for session := range p.members() {
		sessions = append(sessions, session)
	}
	sort.Strings(sessions)
	return sessions
}

func refused(err error, kind string) error {
	return temporal.NewNonRetryableApplicationError(err.Error(), kind, err)
}

// startGroup makes a session a member of its group's pod, and makes the pod
// if there is none. It waits while the pod of an earlier generation is
// ending, and heartbeats while it does, because the pod keeps its name until
// it is gone.
func (p *Pods) startGroup(ctx context.Context, request domain.PodRequest) (domain.PodHandle, error) {
	handle := domain.PodHandle{Name: domain.GroupPodName(request.Group), TaskQueue: domain.GroupQueue(request.Group)}
	if err := domain.ValidGroup(request.Group); err != nil {
		return handle, refused(err, "PodRequest")
	}
	key, err := memberAnnotation(request.Session)
	if err != nil {
		return handle, refused(err, "PodRequest")
	}
	path := p.podsPath() + "/" + handle.Name
	for attempt := 0; attempt < groupJoinAttempts; attempt++ {
		var pod podObject
		err := p.do(ctx, http.MethodGet, path, nil, &pod)
		switch {
		case isStatus(err, http.StatusNotFound):
			rendered, err := p.render(request, handle)
			if err != nil {
				return handle, refused(err, "PodTemplate")
			}
			err = p.do(ctx, http.MethodPost, p.podsPath(), rendered, nil)
			switch {
			case err == nil:
				p.logger.Info("group pod created", "group", request.Group, "pod", handle.Name, "session", request.Session)
				return handle, nil
			case isStatus(err, http.StatusConflict):
				// Another session made it first: join it.
				continue
			}
			return handle, err
		case err != nil:
			return handle, err
		}
		if pod.Metadata.DeletionTimestamp != nil {
			if err := p.waitGone(ctx, handle.Name); err != nil {
				return handle, err
			}
			continue
		}
		if owner, member := pod.members()[request.Session]; member && owner == request.Owner {
			return handle, nil
		}
		patch := map[string]any{"metadata": map[string]any{
			"resourceVersion": pod.Metadata.ResourceVersion,
			"annotations":     map[string]any{key: request.Owner},
		}}
		err = p.doAs(ctx, http.MethodPatch, path, mergePatch, patch, nil)
		switch {
		case err == nil:
			p.logger.Info("session joined a group pod", "group", request.Group, "pod", handle.Name, "session", request.Session)
			return handle, nil
		case isStatus(err, http.StatusConflict), isStatus(err, http.StatusNotFound):
			// The pod changed, or ended, since it was read.
			continue
		}
		return handle, err
	}
	return handle, fmt.Errorf("join the group pod %s: it kept changing", handle.Name)
}

// leaveGroup removes a session from its group's pod, deletes the pod when
// the session was its last member, and then deletes the session's snapshot
// once the pod can no longer write it. A pod or a membership that is gone is
// not an error.
func (p *Pods) leaveGroup(ctx context.Context, request domain.PodRequest) error {
	if err := domain.ValidGroup(request.Group); err != nil {
		return refused(err, "PodRequest")
	}
	key, err := memberAnnotation(request.Session)
	if err != nil {
		return refused(err, "PodRequest")
	}
	name := domain.GroupPodName(request.Group)
	deleted, err := p.removeMember(ctx, name, key, request.Session)
	if err != nil {
		return err
	}
	if deleted {
		p.logger.Info("group pod deleted with its last session", "group", request.Group, "pod", name, "session", request.Session)
		if err := p.waitGone(ctx, name); err != nil {
			return err
		}
	} else {
		p.logger.Info("session left a group pod", "group", request.Group, "pod", name, "session", request.Session)
	}
	return p.deleteGroupSnapshot(ctx, request.Group, request.Session)
}

// removeMember takes a session out of a pod's members and deletes the pod if
// none is left. It reports whether it deleted the pod.
func (p *Pods) removeMember(ctx context.Context, name, key, session string) (bool, error) {
	path := p.podsPath() + "/" + name
	for attempt := 0; attempt < groupJoinAttempts; attempt++ {
		var pod podObject
		err := p.do(ctx, http.MethodGet, path, nil, &pod)
		if isStatus(err, http.StatusNotFound) {
			return false, nil
		}
		if err != nil {
			return false, err
		}
		members := pod.members()
		delete(members, session)
		if pod.Metadata.DeletionTimestamp != nil {
			return false, nil
		}
		if len(members) == 0 {
			body := map[string]any{"preconditions": map[string]any{"uid": pod.Metadata.UID, "resourceVersion": pod.Metadata.ResourceVersion}}
			err := p.do(ctx, http.MethodDelete, path, body, nil)
			switch {
			case err == nil:
				return true, nil
			case isStatus(err, http.StatusNotFound):
				return false, nil
			case isStatus(err, http.StatusConflict):
				continue
			}
			return false, err
		}
		if _, member := pod.members()[session]; !member {
			return false, nil
		}
		patch := map[string]any{"metadata": map[string]any{
			"resourceVersion": pod.Metadata.ResourceVersion,
			"annotations":     map[string]any{key: nil},
		}}
		err = p.doAs(ctx, http.MethodPatch, path, mergePatch, patch, nil)
		switch {
		case err == nil:
			return false, nil
		case isStatus(err, http.StatusNotFound):
			return false, nil
		case isStatus(err, http.StatusConflict):
			continue
		}
		return false, err
	}
	return false, fmt.Errorf("leave the group pod %s: it kept changing", name)
}

// groupStore is the snapshot store of a group's sessions.
func (p *Pods) groupStore(group string) (*snapshot.Store, error) {
	if p.snapshots == nil {
		return nil, nil
	}
	config := p.config.Snapshots
	config.Prefix = domain.GroupSnapshotPrefix(config.Prefix, group)
	return snapshot.New(config, p.logger)
}

// deleteGroupSnapshot deletes a session's snapshot under its group's prefix.
// The worker that closed the session deleted it already: this is for one
// that could not.
func (p *Pods) deleteGroupSnapshot(ctx context.Context, group, session string) error {
	store, err := p.groupStore(group)
	if err != nil || store == nil {
		return err
	}
	if err := store.Delete(ctx, session); err != nil {
		return fmt.Errorf("delete the snapshot of session %s in group %s: %w", session, group, err)
	}
	return nil
}

// reconcileGroup judges a group's pod: it removes the members whose owner is
// not running or whose session is closed, which deletes the pod with the last
// of them, and deletes a pod with no member or one that is not ready after
// its open timeout.
func (r *Reconciler) reconcileGroup(ctx context.Context, pod *podObject, group string) error {
	var failed []error
	if since, notReady := pod.notReadySince(); notReady {
		timeout := r.pods.config.OpenTimeout
		if timeout <= 0 {
			timeout = defaultOpenTimeout
		}
		if annotated, err := time.ParseDuration(pod.Metadata.Annotations[AnnotationOpenTimeout]); err == nil && annotated > 0 {
			timeout = annotated
		}
		if r.now().Sub(since) > timeout {
			if err := r.pods.deletePod(ctx, pod.Metadata.Name, pod.Metadata.UID); err != nil {
				return fmt.Errorf("delete the pod of group %s: %w", group, err)
			}
			r.logger.Info("group pod deleted by reconciliation", "group", group, "pod", pod.Metadata.Name, "reason", ReasonNotReady)
		}
		// A pod that cannot answer yet is asked later.
		return nil
	}
	sessions := pod.memberSessions()
	if len(sessions) == 0 {
		if err := r.pods.deletePod(ctx, pod.Metadata.Name, pod.Metadata.UID); err != nil {
			return fmt.Errorf("delete the pod of group %s: %w", group, err)
		}
		r.logger.Info("group pod deleted by reconciliation", "group", group, "pod", pod.Metadata.Name, "reason", ReasonNoMembers)
		return nil
	}
	members := pod.members()
	for _, session := range sessions {
		reason, err := r.memberOrphaned(ctx, group, session, members[session])
		if err != nil {
			r.logger.Warn("check a group member", "group", group, "session", session, "pod", pod.Metadata.Name, "error", err)
			continue
		}
		if reason == "" {
			continue
		}
		r.logger.Info("group member removed by reconciliation", "group", group, "session", session, "pod", pod.Metadata.Name, "reason", reason)
		leaveCtx, cancel := context.WithTimeout(ctx, snapshotDeleteWait)
		err = r.pods.leaveGroup(leaveCtx, domain.PodRequest{Session: session, Group: group})
		cancel()
		if err != nil {
			failed = append(failed, fmt.Errorf("remove session %s from group %s: %w", session, group, err))
		}
	}
	return errors.Join(failed...)
}

// memberOrphaned returns why a member should go, or "" when it should stay.
func (r *Reconciler) memberOrphaned(ctx context.Context, group, session, owner string) (string, error) {
	if owner != "" {
		running, err := r.ownerRunning(ctx, owner)
		if err != nil {
			return "", err
		}
		if !running {
			return ReasonOwnerGone, nil
		}
	}
	state, err := r.probeGroup(ctx, group, session)
	if err != nil {
		return "", fmt.Errorf("ask the pod whether its session is closed: %w", err)
	}
	if state.Closed {
		return ReasonSessionClose, nil
	}
	return "", nil
}
