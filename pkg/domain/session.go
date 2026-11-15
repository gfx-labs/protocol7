package domain

import (
	"errors"
	"fmt"
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
)

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
	// Owner is the ID of the workflow that opened the session. It is kept on
	// the pod, so that a pod nobody owns can be found.
	Owner string `json:"owner,omitempty"`
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

// CloseRequest is the input of ActivityCloseSession.
type CloseRequest struct {
	Session string `json:"session"`
}
