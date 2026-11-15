package domain

import "encoding/json"

// UpdateToolCall is the name of the Temporal update that carries a session's
// tool call to the workflow that owns the session.
const UpdateToolCall = "p7.ToolCall"

// ToolSpec describes one tool offered to a session's agent.
type ToolSpec struct {
	Name        string `json:"name"`
	Description string `json:"description,omitempty"`
	// InputSchema is the JSON Schema of the arguments, an object schema.
	// Empty accepts an object with any members.
	InputSchema json.RawMessage `json:"input_schema,omitempty"`
}

// ToolCall is one call of a tool by a session's agent.
type ToolCall struct {
	Session string `json:"session"`
	Tool    string `json:"tool"`
	// Arguments is a JSON object.
	Arguments json.RawMessage `json:"arguments,omitempty"`
}

// ToolContent is one block of a tool's result: text, or an image when
// MIMEType is set.
type ToolContent struct {
	Text     string `json:"text,omitempty"`
	Data     []byte `json:"data,omitempty"`
	MIMEType string `json:"mime_type,omitempty"`
}

// ToolResult is what a tool call returns to the agent. IsError marks a
// result the agent should read as the tool failing, not as its output.
type ToolResult struct {
	Content []ToolContent `json:"content"`
	IsError bool          `json:"is_error,omitempty"`
}
