package session

import (
	"context"
	"errors"

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

// tools are what a connection to rec's shim offers the agent, and who
// decides its permission requests. A session with neither, or a manager with
// no caller, gets nil: no tools, and every request allowed.
func (m *Manager) tools(rec record) acpshim.Tools {
	if m.config.Tools == nil || rec.Request.Owner == "" {
		return nil
	}
	tools := sessionTools{caller: m.config.Tools, session: rec.ID, owner: rec.Request.Owner, specs: rec.Request.Tools}
	if rec.Request.Permissions {
		// The owner asked to decide. A caller that cannot reach it must not
		// turn that into allowing everything, so its requests are denied.
		decider, _ := m.config.Tools.(PermissionCaller)
		return sessionPermissions{sessionTools: tools, decider: decider}
	}
	if len(rec.Request.Tools) == 0 {
		return nil
	}
	return tools
}

// PermissionCaller decides a session's permission requests in the workflow
// that owns the session. The core worker's is a Temporal update. A
// ToolCaller that also implements it is used for permissions: no field of
// Config is needed.
type PermissionCaller interface {
	CallPermission(ctx context.Context, owner, callID string, request domain.PermissionRequest) (domain.PermissionDecision, error)
}

// sessionPermissions are the tools of one session, and its owner's decisions.
type sessionPermissions struct {
	sessionTools
	decider PermissionCaller
}

func (p sessionPermissions) Decide(ctx context.Context, callID string, request domain.PermissionRequest) (domain.PermissionDecision, error) {
	request.Session = p.session
	if p.decider == nil {
		return domain.PermissionDecision{}, errors.New("this worker cannot send a permission request to its owner")
	}
	return p.decider.CallPermission(ctx, p.owner, p.session+"-"+callID, request)
}

// offersTools reports whether a connection to rec's shim offers the agent
// tools, which needs a harness that takes MCP servers over ACP. A session
// that only has permissions decided does not.
func (m *Manager) offersTools(rec record) bool {
	return m.tools(rec) != nil && len(rec.Request.Tools) > 0
}
