package session

import (
	"context"

	"github.com/gfx-labs/protocol7/pkg/domain"
	"github.com/gfx-labs/protocol7/pkg/execution/acpshim"
)

// ToolCaller runs a session's tool call in the workflow that owns the
// session. The core worker's is a Temporal update.
type ToolCaller interface {
	CallTool(ctx context.Context, owner, callID string, call domain.ToolCall) (domain.ToolResult, error)
}

// sessionTools are the tools of one session, as the shim client offers them.
type sessionTools struct {
	caller  ToolCaller
	session string
	owner   string
	specs   []domain.ToolSpec
}

func (t sessionTools) List() []domain.ToolSpec { return t.specs }

func (t sessionTools) Call(ctx context.Context, callID string, call domain.ToolCall) (domain.ToolResult, error) {
	call.Session = t.session
	return t.caller.CallTool(ctx, t.owner, t.session+"-"+callID, call)
}

// tools are what a connection to rec's shim offers the agent. A session
// with no tools, or a manager with no caller, offers none.
func (m *Manager) tools(rec record) acpshim.Tools {
	if m.config.Tools == nil || len(rec.Request.Tools) == 0 || rec.Request.Owner == "" {
		return nil
	}
	return sessionTools{caller: m.config.Tools, session: rec.ID, owner: rec.Request.Owner, specs: rec.Request.Tools}
}
