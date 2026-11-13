// Package journal decodes a session's journal, the ordered record of the
// JSON-RPC messages that crossed its ACP connection, into session events.
//
// A decoder keeps state between frames, so it has to see a journal from its
// first frame. It decodes leniently: a frame it does not understand yields
// no event, because a newer harness may send updates this code predates.
package journal

import (
	"encoding/json"
	"strings"
	"time"

	"github.com/gfx-labs/protocol7/pkg/domain"
)

// MetaKey is the key protocol7 uses in the _meta of what it sends.
const MetaKey = "p7"

// PromptMeta is protocol7's _meta on a session/prompt request.
type PromptMeta struct {
	// Turn identifies the turn the prompt starts.
	Turn string `json:"turn"`
}

// BlockMeta is protocol7's _meta on a content block of a prompt.
type BlockMeta struct {
	// Context marks a block the workflow added in front of the prompt. It is
	// not part of what the person wrote.
	Context bool `json:"context,omitempty"`
}

// Decoder turns the frames of one session's journal into events.
type Decoder struct {
	session string
	// prompting is set from a prompt until the turn it started has ended.
	prompting bool
	// loading is set while the harness replays a conversation it is loading.
	// The replay repeats what the journal already has.
	loading bool
}

// NewDecoder returns a decoder for the journal of the named session.
func NewDecoder(session string) *Decoder { return &Decoder{session: session} }

type rpcMessage struct {
	ID     json.RawMessage `json:"id"`
	Method string          `json:"method"`
	Params json.RawMessage `json:"params"`
	Result json.RawMessage `json:"result"`
	Error  *rpcError       `json:"error"`
}

type rpcError struct {
	Code    int             `json:"code"`
	Message string          `json:"message"`
	Data    json.RawMessage `json:"data"`
}

func (m rpcMessage) isResponse() bool { return m.Method == "" && len(m.ID) > 0 }

// Decode returns the events of one frame, in order. Most frames yield one
// event or none.
func (d *Decoder) Decode(frame domain.Frame) []domain.SessionEvent {
	var message rpcMessage
	if err := json.Unmarshal(frame.Msg, &message); err != nil {
		return nil
	}
	if frame.From == "controller" {
		return d.fromController(frame, message)
	}
	return d.fromAgent(frame, message)
}

func (d *Decoder) event(frame domain.Frame, kind domain.EventKind) domain.SessionEvent {
	return domain.SessionEvent{Session: d.session, Seq: frame.Seq, Time: time.UnixMilli(frame.T).UTC(), Kind: kind}
}

func (d *Decoder) fromController(frame domain.Frame, message rpcMessage) []domain.SessionEvent {
	var events []domain.SessionEvent
	switch message.Method {
	case "initialize", "session/new", "session/load", "session/prompt":
		// The client sends none of these while a turn runs. One that follows
		// an unanswered prompt means the harness went away during the turn.
		if d.prompting {
			d.prompting = false
			end := d.event(frame, domain.EventTurnEnd)
			end.End = &domain.TurnEnd{
				StopReason: domain.StopInterrupted,
				Failure:    &domain.TurnFailure{Message: "the session's processes stopped during the turn"},
			}
			events = append(events, end)
		}
	}
	switch message.Method {
	case "session/load":
		d.loading = true
	case "session/prompt":
		var params struct {
			Meta   map[string]json.RawMessage `json:"_meta"`
			Prompt []contentBlock             `json:"prompt"`
		}
		if err := json.Unmarshal(message.Params, &params); err != nil {
			return events
		}
		d.prompting = true
		prompt := d.event(frame, domain.EventPrompt)
		var meta PromptMeta
		if raw, ok := params.Meta[MetaKey]; ok {
			_ = json.Unmarshal(raw, &meta)
		}
		prompt.Turn = meta.Turn
		var text strings.Builder
		for _, block := range params.Prompt {
			if block.isContext() {
				continue
			}
			text.WriteString(block.Text)
		}
		prompt.Text = text.String()
		events = append(events, prompt)
	}
	return events
}

func (d *Decoder) fromAgent(frame domain.Frame, message rpcMessage) []domain.SessionEvent {
	if message.isResponse() {
		return d.response(frame, message)
	}
	if message.Method != "session/update" || d.loading {
		return nil
	}
	var params struct {
		Update sessionUpdate `json:"update"`
	}
	if err := json.Unmarshal(message.Params, &params); err != nil {
		return nil
	}
	update := params.Update
	switch update.Kind {
	case "agent_message_chunk", "agent_thought_chunk":
		var block contentBlock
		if err := json.Unmarshal(update.Content, &block); err != nil || block.Text == "" {
			return nil
		}
		kind := domain.EventMessage
		if update.Kind == "agent_thought_chunk" {
			kind = domain.EventThought
		}
		event := d.event(frame, kind)
		event.Text = block.Text
		return []domain.SessionEvent{event}
	case "tool_call", "tool_call_update":
		kind := domain.EventToolCall
		if update.Kind == "tool_call_update" {
			kind = domain.EventToolUpdate
		}
		event := d.event(frame, kind)
		event.Tool = &domain.ToolEvent{
			ID:     update.ToolCallID,
			Title:  update.Title,
			Kind:   update.ToolKind,
			Status: update.Status,
			Input:  cut(compactJSON(update.RawInput), domain.ToolTextLimit),
			Output: cut(toolOutput(update.Content), domain.ToolTextLimit),
		}
		return []domain.SessionEvent{event}
	case "plan":
		event := d.event(frame, domain.EventPlan)
		event.Plan = update.Entries
		return []domain.SessionEvent{event}
	}
	return nil
}

// response handles an answer of the agent to a request of the client. The
// shim rewrites request IDs, so an answer is matched by what it contains and
// by what the client had outstanding, not by its ID.
func (d *Decoder) response(frame domain.Frame, message rpcMessage) []domain.SessionEvent {
	if d.loading {
		// The answer to session/load ends the replay.
		d.loading = false
		return nil
	}
	var result struct {
		StopReason string `json:"stopReason"`
	}
	if len(message.Result) > 0 {
		_ = json.Unmarshal(message.Result, &result)
	}
	switch {
	case result.StopReason != "":
		d.prompting = false
		end := d.event(frame, domain.EventTurnEnd)
		end.End = &domain.TurnEnd{StopReason: result.StopReason}
		return []domain.SessionEvent{end}
	case message.Error != nil && d.prompting:
		d.prompting = false
		end := d.event(frame, domain.EventTurnEnd)
		end.End = &domain.TurnEnd{StopReason: domain.StopFailed, Failure: failure(*message.Error)}
		return []domain.SessionEvent{end}
	}
	return nil
}

// failure reads the harness's typed stop from an error's data. towa puts it
// under "towa"."stop". Another harness gets the message alone.
func failure(rpc rpcError) *domain.TurnFailure {
	out := &domain.TurnFailure{Message: rpc.Message}
	var data struct {
		Error string `json:"error"`
		Towa  struct {
			Stop *struct {
				Kind    string    `json:"kind"`
				ResetAt time.Time `json:"resetAt"`
				Account string    `json:"account"`
				Model   string    `json:"model"`
				Message string    `json:"message"`
			} `json:"stop"`
		} `json:"towa"`
	}
	if len(rpc.Data) == 0 || json.Unmarshal(rpc.Data, &data) != nil {
		return out
	}
	if data.Error != "" {
		out.Message = data.Error
	}
	if stop := data.Towa.Stop; stop != nil {
		out.Kind, out.ResetAt, out.Account, out.Model = stop.Kind, stop.ResetAt, stop.Account, stop.Model
		if stop.Message != "" {
			out.Message = stop.Message
		}
	}
	return out
}

type contentBlock struct {
	Meta map[string]json.RawMessage `json:"_meta"`
	Type string                     `json:"type"`
	Text string                     `json:"text"`
}

func (b contentBlock) isContext() bool {
	raw, ok := b.Meta[MetaKey]
	if !ok {
		return false
	}
	var meta BlockMeta
	return json.Unmarshal(raw, &meta) == nil && meta.Context
}

// sessionUpdate holds the fields of the session/update kinds this package
// reads. Content is one block for a message chunk and a list for a tool call.
type sessionUpdate struct {
	Kind       string             `json:"sessionUpdate"`
	Content    json.RawMessage    `json:"content"`
	ToolCallID string             `json:"toolCallId"`
	Title      string             `json:"title"`
	ToolKind   string             `json:"kind"`
	Status     string             `json:"status"`
	RawInput   json.RawMessage    `json:"rawInput"`
	Entries    []domain.PlanEntry `json:"entries"`
}

// toolOutput is the text of a tool call's content: its text blocks, and for
// an edit the path and the new text.
func toolOutput(raw json.RawMessage) string {
	if len(raw) == 0 {
		return ""
	}
	var items []struct {
		Type    string       `json:"type"`
		Content contentBlock `json:"content"`
		Path    string       `json:"path"`
		NewText string       `json:"newText"`
	}
	if err := json.Unmarshal(raw, &items); err != nil {
		return ""
	}
	var out strings.Builder
	for _, item := range items {
		switch item.Type {
		case "content":
			out.WriteString(item.Content.Text)
		case "diff":
			out.WriteString(item.Path)
			out.WriteString("\n")
			out.WriteString(item.NewText)
		}
	}
	return out.String()
}

func compactJSON(raw json.RawMessage) string {
	if len(raw) == 0 || string(raw) == "null" {
		return ""
	}
	return string(raw)
}

// cut shortens text to at most limit bytes without splitting a character.
func cut(text string, limit int) string {
	if len(text) <= limit {
		return text
	}
	end := limit
	for end > 0 && !isRuneStart(text[end]) {
		end--
	}
	return text[:end]
}

func isRuneStart(b byte) bool { return b&0xC0 != 0x80 }
