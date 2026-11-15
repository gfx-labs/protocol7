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
	if t.caller == nil {
		return domain.ToolResult{}, errors.New("this session has no tools")
	}
	call.Session = t.session
	return t.caller.CallTool(ctx, t.owner, t.session+"-"+callID, call)
}

// tools are what a connection to rec's shim offers the agent, and who
// decides its permission requests. A session with neither gets nil: no
// tools, and every request allowed.
//
// A session whose owner asked to decide its permission requests never gets
// nil. When nobody can be asked, because the manager has no caller, the
// caller cannot send permission requests or the request has no owner, every
// request is denied: a request must not be allowed because the owner could
// not be reached.
func (m *Manager) tools(rec record) acpshim.Tools {
	owned := m.config.Tools != nil && rec.Request.Owner != ""
	tools := sessionTools{session: rec.ID, owner: rec.Request.Owner, specs: rec.Request.Tools}
	if owned {
		tools.caller = m.config.Tools
	}
	if rec.Request.Permissions {
		var decider PermissionCaller
		if owned {
			decider, _ = m.config.Tools.(PermissionCaller)
		}
		if !owned {
			// Without a caller or an owner no tool can be called either.
			tools.specs = nil
		}
		return sessionPermissions{sessionTools: tools, decider: decider}
	}
	if !owned || len(rec.Request.Tools) == 0 {
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
	tools := m.tools(rec)
	return tools != nil && len(tools.List()) > 0
}
