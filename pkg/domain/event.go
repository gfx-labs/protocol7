package domain

import (
	"encoding/json"
	"time"
)

// Frame is one line of a session's journal: a JSON-RPC message that crossed
// the ACP connection, as the shim recorded it.
type Frame struct {
	Seq  int64           `json:"seq"`
	T    int64           `json:"t"`    // unix milliseconds
	From string          `json:"from"` // "agent" or "controller"
	Msg  json.RawMessage `json:"msg"`
}

// EventKind names what a session event reports.
type EventKind string

// Kinds of session event.
const (
	EventPrompt     EventKind = "prompt"      // a prompt was sent to the agent
	EventMessage    EventKind = "message"     // a piece of the agent's answer
	EventThought    EventKind = "thought"     // a piece of the agent's reasoning, as ACP exposes it
	EventToolCall   EventKind = "tool_call"   // a tool call started
	EventToolUpdate EventKind = "tool_update" // a tool call changed or finished
	EventPlan       EventKind = "plan"        // the agent's plan, whole
	EventUsage      EventKind = "usage"       // a model request of the turn was counted
	EventTurnEnd    EventKind = "turn_end"    // the turn ended
)

// SessionEvent is one decoded journal frame.
type SessionEvent struct {
	Session string    `json:"session"`
	Seq     int64     `json:"seq"`
	Time    time.Time `json:"time"`
	Kind    EventKind `json:"kind"`
	// Turn is the ID of the turn a prompt starts.
	Turn string `json:"turn,omitempty"`
	// Text is the prompt, or a piece of a message or thought.
	Text string      `json:"text,omitempty"`
	Tool *ToolEvent  `json:"tool,omitempty"`
	Plan []PlanEntry `json:"plan,omitempty"`
	End  *TurnEnd    `json:"end,omitempty"`
	// Usage is what the harness reported for one model request.
	Usage *UsageEvent `json:"usage,omitempty"`
}

// UsageEvent is the harness's usage update for one model request.
type UsageEvent struct {
	// Model is the model that served the request, when the harness says.
	Model string `json:"model,omitempty"`
	// Tokens is how much of the model's context the request filled: what it
	// read, from the cache or not, and what it wrote. Summed over a turn it
	// is the tokens the turn processed. It is not split by kind.
	Tokens int64 `json:"tokens"`
	// Window is the model's context window, in tokens.
	Window int64 `json:"window,omitempty"`
	// Cost is the session's cost so far, cumulative, when the harness knows
	// every price. Nil when it does not.
	Cost *Cost `json:"cost,omitempty"`
}

// Cost is an amount of money in an ISO 4217 currency, or another unit the
// harness names.
type Cost struct {
	Amount   float64 `json:"amount"`
	Currency string  `json:"currency"`
}

// ToolEvent is a tool call or an update to one. An update carries only the
// fields that changed.
type ToolEvent struct {
	ID     string `json:"id"`
	Title  string `json:"title,omitempty"`
	Kind   string `json:"kind,omitempty"`
	Status string `json:"status,omitempty"`
	// Input is the call's arguments as JSON, cut to ToolTextLimit bytes.
	Input string `json:"input,omitempty"`
	// Output is the text the tool produced, cut to ToolTextLimit bytes.
	Output string `json:"output,omitempty"`
}

// ToolTextLimit bounds the input and output text of one tool event. The
// journal has all of it.
const ToolTextLimit = 16 << 10

// PlanEntry is one item of the agent's plan.
type PlanEntry struct {
	Content  string `json:"content"`
	Status   string `json:"status"`
	Priority string `json:"priority,omitempty"`
}

// TurnEnd is how a turn ended.
type TurnEnd struct {
	StopReason string       `json:"stop_reason"`
	Failure    *TurnFailure `json:"failure,omitempty"`
}
