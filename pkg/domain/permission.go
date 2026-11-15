package domain

import "encoding/json"

// UpdatePermission is the name of the Temporal update that carries a
// permission request of a session's agent to the workflow that owns the
// session.
const UpdatePermission = "p7.Permission"

// PermissionOption is one answer the agent offers to a permission request.
type PermissionOption struct {
	ID string `json:"id"`
	// Kind is ACP's: allow_once, allow_always, reject_once or reject_always.
	Kind string `json:"kind"`
	Name string `json:"name,omitempty"`
}

// PermissionRequest is an agent asking to run a tool call that its harness
// holds back until the client allows it.
type PermissionRequest struct {
	// Session is the ID of the session whose agent asks. The core worker
	// fills it in.
	Session    string `json:"session"`
	ToolCallID string `json:"tool_call_id"`
	// Title and Kind describe the call as the agent does. Kind is ACP's
	// tool kind, such as "execute" or "edit". Both may be empty.
	Title string `json:"title,omitempty"`
	Kind  string `json:"kind,omitempty"`
	// RawInput is the call's input as the agent gave it, if it did.
	RawInput json.RawMessage    `json:"raw_input,omitempty"`
	Options  []PermissionOption `json:"options,omitempty"`
}

// PermissionDecision is the owner workflow's answer to a PermissionRequest.
type PermissionDecision struct {
	Allow bool `json:"allow"`
	// Reason says why, for the workflow's own history. The agent does not
	// receive it: ACP's answer to a permission request carries none.
	Reason string `json:"reason,omitempty"`
}
