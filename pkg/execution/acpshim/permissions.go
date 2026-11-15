package acpshim

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	jsonv2 "encoding/json/v2"
	"strconv"

	acp "github.com/gfx-labs/go-acp"

	"github.com/gfx-labs/protocol7/pkg/domain"
)

// Permissions decides the agent's permission requests. A value passed to
// Connect as the Tools that also implements it is asked about every request
// of the session. Without it, every request is allowed.
type Permissions interface {
	// Decide returns the answer to one request. callID is the same when the
	// shim sends the same request again to a later connection, so a caller
	// can decide it once. The request's session is filled in by the caller.
	// An error is treated as a denial.
	Decide(ctx context.Context, callID string, request domain.PermissionRequest) (domain.PermissionDecision, error)
}

// RequestPermission answers the agent's permission request. With a
// Permissions, it is the decision of the session's owner, and a request that
// cannot be decided is denied. Without one the request is allowed: what a
// session may do is then set by its environment, not by a prompt to a person
// who is not there.
func (a agentRequests) RequestPermission(ctx context.Context, request acp.RequestPermissionRequest) (acp.RequestPermissionResponse, error) {
	if a.permissions == nil {
		return chooseOption(request, true), nil
	}
	call := permissionRequest(request)
	decision, err := a.permissions.Decide(ctx, permissionCallID(a.shimPid, request), call)
	if err != nil {
		decision = domain.PermissionDecision{Allow: false, Reason: err.Error()}
	}
	return chooseOption(request, decision.Allow), nil
}

// chooseOption answers with the agent's own option of the wanted kind, once
// before always. When the agent offers none, it answers cancelled, which the
// agent reads as not allowed.
func chooseOption(request acp.RequestPermissionRequest, allow bool) acp.RequestPermissionResponse {
	kinds := []acp.PermissionOptionKind{acp.PermissionOptionKindAllowOnce, acp.PermissionOptionKindAllowAlways}
	if !allow {
		kinds = []acp.PermissionOptionKind{acp.PermissionOptionKindRejectOnce, acp.PermissionOptionKindRejectAlways}
	}
	for _, kind := range kinds {
		for _, option := range request.Options {
			if option.Kind == kind {
				return acp.RequestPermissionResponse{Outcome: acp.NewRequestPermissionOutcomeSelected(option.OptionId)}
			}
		}
	}
	return acp.RequestPermissionResponse{Outcome: acp.NewRequestPermissionOutcomeCancelled()}
}

func permissionRequest(request acp.RequestPermissionRequest) domain.PermissionRequest {
	call := domain.PermissionRequest{Session: string(request.SessionId), ToolCallID: string(request.ToolCall.ToolCallId)}
	if request.ToolCall.Title != nil {
		call.Title = *request.ToolCall.Title
	}
	if request.ToolCall.Kind != nil {
		call.Kind = string(*request.ToolCall.Kind)
	}
	if request.ToolCall.RawInput != nil {
		call.RawInput, _ = jsonv2.Marshal(request.ToolCall.RawInput)
	}
	for _, option := range request.Options {
		call.Options = append(call.Options, domain.PermissionOption{ID: string(option.OptionId), Kind: string(option.Kind), Name: option.Name})
	}
	return call
}

// permissionCallID names a request. ACP's request ID is not visible to a
// client's handler, so the ID is the shim's process and a digest of the
// request: the same request sent again to a later connection of this shim
// gets the first decision.
func permissionCallID(shimPid int, request acp.RequestPermissionRequest) string {
	content, _ := jsonv2.Marshal(request, jsonv2.Deterministic(true))
	sum := sha256.Sum256(content)
	return strconv.Itoa(shimPid) + "-permission-" + hex.EncodeToString(sum[:12])
}
