package domain

import (
	"errors"
	"fmt"
	"strings"
	"time"
)

// Activity names of the core worker.
const (
	ActivityOpenSession  = "p7.OpenSession"
	ActivityRunTurn      = "p7.RunTurn"
	ActivityCloseSession = "p7.CloseSession"
	// ActivityStartSessionPod and ActivityDeleteSessionPod run on the
	// kubernetes backend's queue. The session activities above then run in
	// the session's pod, on its own queue.
	ActivityStartSessionPod  = "p7.StartSessionPod"
	ActivityDeleteSessionPod = "p7.DeleteSessionPod"
	// ActivitySessionState runs on a session's own queue and says whether the
	// session is closed. The kubernetes backend's reconciliation asks it.
	ActivitySessionState = "p7.SessionState"
)

// WorkflowProbeSessionPod is the workflow of the kubernetes backend's queue
// that runs ActivitySessionState on a session's queue for the reconciliation,
// which is not a workflow itself and so cannot run an activity.
const WorkflowProbeSessionPod = "p7.ProbeSessionPod"

// WorkflowProbeGroupSession is WorkflowProbeSessionPod for a session in a
// group's pod. Its input has the group and the session.
const WorkflowProbeGroupSession = "p7.ProbeGroupSession"

// BackendHost runs a session as a process on a named machine.
const BackendHost = "host"

// BackendKubernetes runs each session in a pod of its own, made from a pod
// template that the deployment supplies.
const BackendKubernetes = "kubernetes"

// KubernetesQueue is the queue of the worker that creates and deletes
// session pods.
const KubernetesQueue = "p7-kubernetes"

// SessionQueue is the queue of the worker in a session's pod.
func SessionQueue(session string) string { return "p7-session-" + session }

// GroupQueue is the queue of the worker in a group's pod. Every session of the
// group runs its activities on it.
func GroupQueue(group string) string { return "p7-group-" + group }

// GroupPodName is the name of a group's pod.
func GroupPodName(group string) string { return "p7-group-" + group }

// ValidGroup checks a group key: 8 to 40 lower case letters and digits, such
// as the start of a hash. It becomes part of a pod name, a queue name, a
// SPIFFE ID and an object path, so nothing else is allowed in it.
func ValidGroup(group string) error {
	if len(group) < 8 || len(group) > 40 {
		return fmt.Errorf("group %q: the key must be 8 to 40 characters", group)
	}
	for _, c := range group {
		if (c < 'a' || c > 'z') && (c < '0' || c > '9') {
			return fmt.Errorf("group %q: the key may hold only a-z and 0-9", group)
		}
	}
	return nil
}

// GroupSnapshotPrefix is the prefix under which a group's sessions keep their
// snapshots: <prefix>/groups/<group>/<session>/workspace.tar.zst. The group's
// identity may touch that prefix and no other, so a group cannot read the
// snapshots of another.
func GroupSnapshotPrefix(prefix, group string) string {
	return strings.Trim(strings.Trim(prefix, "/")+"/groups/"+group, "/")
}

// Placement says where a session or step runs.
type Placement struct {
	Backend string `json:"backend"`
	// Host names the machine. The host backend requires it.
	Host string `json:"host,omitempty"`
}

// TaskQueue is the core queue a placement's activities are sent to.
func (p Placement) TaskQueue() (string, error) {
	switch p.Backend {
	case BackendHost:
		if p.Host == "" {
			return "", errors.New("placement: the host backend needs a host name")
		}
		return "local-" + p.Host, nil
	case BackendKubernetes:
		return KubernetesQueue, nil
	case "":
		return "", errors.New("placement: no backend named, and there is no default backend yet")
	default:
		return "", fmt.Errorf("placement: backend %q is not implemented", p.Backend)
	}
}

// InputGitClone is a working clone of a repository.
const InputGitClone = "git_clone"

// Input is one thing placed in a session's workspace before it starts.
type Input struct {
	Kind string `json:"kind"`
	// Destination is a path relative to the workspace root.
	Destination string `json:"destination"`
	Repository  string `json:"repository,omitempty"`
	// Ref is a commit, tag or branch. Empty means the default branch.
	Ref string `json:"ref,omitempty"`
	// Branch, if set, is checked out at Ref so the session can commit.
	Branch string `json:"branch,omitempty"`
}

// ResolvedInput records what an input resolved to when the session opened.
type ResolvedInput struct {
	Destination string `json:"destination"`
	Repository  string `json:"repository,omitempty"`
	Ref         string `json:"ref,omitempty"`
	Commit      string `json:"commit,omitempty"`
}

// SessionRequest is the input of ActivityOpenSession.
type SessionRequest struct {
	// ID is chosen by the caller and makes opening idempotent: a session
	// that exists under the ID is attached to, not created again.
	ID     string   `json:"id"`
	Models []string `json:"models,omitempty"`
	Inputs []Input  `json:"inputs,omitempty"`
	// WorkingDirectory is relative to the workspace root.
	WorkingDirectory string `json:"working_directory,omitempty"`
	// IdleLimit is how long the session's processes are kept with nobody
	// connected. The conversation outlives them.
	IdleLimit time.Duration `json:"idle_limit,omitempty"`
	// Tools are offered to the agent as one MCP server over the ACP
	// connection. A call is sent as the update UpdateToolCall to Owner.
	Tools []ToolSpec `json:"tools,omitempty"`
	// Owner is the ID of the workflow that opened the session.
	Owner string `json:"owner,omitempty"`
	// Permissions makes the owner decide the agent's permission requests,
	// each sent as the update UpdatePermission to Owner. False allows them all.
	Permissions bool `json:"permissions,omitempty"`
}

// SessionHandle is the result of ActivityOpenSession.
type SessionHandle struct {
	ID        string          `json:"id"`
	TaskQueue string          `json:"task_queue"`
	Inputs    []ResolvedInput `json:"inputs,omitempty"`
}

// PodRequest is the input of ActivityStartSessionPod and
// ActivityDeleteSessionPod.
type PodRequest struct {
	Session string `json:"session"`
	// Group, if set, puts the session in the pod of that group, which a
	// session of the same group already made or which is made for this one,
	// and not in a pod of its own. Starting adds the session to the pod's
	// members and deleting removes it: the pod is deleted with its last
	// member.
	Group string `json:"group,omitempty"`
	// Owner is the ID of the workflow that opened the session. It is kept on
	// the pod, so that a pod nobody owns can be found.
	Owner string `json:"owner,omitempty"`
	// OpenTimeout is how long the pod may take to become ready. It is kept
	// on the pod, and a pod that is still not ready after it is deleted.
	// Zero is the backend's default.
	OpenTimeout time.Duration `json:"open_timeout,omitempty"`
}

// PodHandle is the result of ActivityStartSessionPod.
type PodHandle struct {
	Name string `json:"name"`
	// TaskQueue is the queue the worker in the pod polls.
	TaskQueue string `json:"task_queue"`
}

// TurnRequest is the input of ActivityRunTurn.
type TurnRequest struct {
	Session string `json:"session"`
	// Turn identifies the turn. A repeated request with the same value
	// returns the first outcome and never prompts twice.
	Turn string `json:"turn"`
	// Context is sent in front of the prompt and is not shown as the
	// person's message.
	Context string `json:"context,omitempty"`
	Prompt  string `json:"prompt"`
}

// Stop reasons protocol7 adds to those of ACP.
const (
	// StopInterrupted: the session's processes went away during the turn.
	StopInterrupted = "interrupted"
	// StopFailed: the agent answered the prompt with an error.
	StopFailed = "failed"
)

// TurnFailure says why a turn did not end normally.
type TurnFailure struct {
	// Kind is the harness's typed stop where it gave one (rate_limit, auth,
	// overload, context_full, limit, network, request, denied), else empty.
	Kind    string    `json:"kind,omitempty"`
	Message string    `json:"message"`
	ResetAt time.Time `json:"reset_at,omitzero"`
	Account string    `json:"account,omitempty"`
	Model   string    `json:"model,omitempty"`
}

// TurnResult is the result of ActivityRunTurn.
type TurnResult struct {
	Turn       string `json:"turn"`
	StopReason string `json:"stop_reason"`
	// Text is the agent's message text of the turn, cut to TextLimit bytes.
	Text      string       `json:"text,omitempty"`
	Truncated bool         `json:"truncated,omitempty"`
	FirstSeq  int64        `json:"first_seq"`
	LastSeq   int64        `json:"last_seq"`
	Failure   *TurnFailure `json:"failure,omitempty"`
}

// TextLimit bounds TurnResult.Text so a result stays far below Temporal's
// payload limit. The whole conversation is in the session's journal.
const TextLimit = 32 << 10

// SessionState is the result of ActivitySessionState.
type SessionState struct {
	Session string `json:"session"`
	// Exists is false for a session that was never opened on this machine.
	Exists bool `json:"exists"`
	Closed bool `json:"closed"`
}

// CloseRequest is the input of ActivityCloseSession.
type CloseRequest struct {
	Session string `json:"session"`
}
