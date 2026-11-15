package p7

import (
	"fmt"
	"slices"

	"go.temporal.io/sdk/workflow"

	"github.com/gfx-labs/protocol7/pkg/domain"
)

// PermissionRequest is an agent asking to run a tool call that its harness
// holds back until its client allows it. Session is the ID of the session
// whose agent asks.
type PermissionRequest = domain.PermissionRequest

// PermissionOption is one answer the agent offers to a PermissionRequest.
type PermissionOption = domain.PermissionOption

// PermissionDecision is a workflow's answer to a PermissionRequest.
type PermissionDecision = domain.PermissionDecision

// Allow allows the request.
func Allow() PermissionDecision { return PermissionDecision{Allow: true} }

// Deny refuses the request. The reason is kept in the workflow's history. The
// agent is told only that the call was refused.
func Deny(reason string) PermissionDecision { return PermissionDecision{Reason: reason} }

// HandlePermissions makes the workflow answer the permission requests of the
// sessions it opened with SessionSpec.Permissions, with decide. Call it at the
// start of every run, before the first OpenSession and after Continue-As-New,
// with the function the sessions were opened with. A request from a session
// not in sessions is refused before it enters history.
//
// decide runs in the workflow, and may run activities or wait for a signal
// to ask a person. An error from decide denies the request. The agent waits
// for the answer, so a decision that takes long holds up its turn.
//
// Before Continue-As-New, wait for workflow.AllHandlersFinished so that no
// request is left without an answer.
func HandlePermissions(ctx workflow.Context, decide func(ctx workflow.Context, request PermissionRequest) (PermissionDecision, error), sessions func() []string) error {
	if decide == nil {
		return fmt.Errorf("handle permissions: no decision function")
	}
	return workflow.SetUpdateHandlerWithOptions(ctx, domain.UpdatePermission,
		func(ctx workflow.Context, request PermissionRequest) (PermissionDecision, error) {
			decision, err := decide(ctx, request)
			if err != nil {
				return Deny(err.Error()), nil
			}
			return decision, nil
		},
		workflow.UpdateHandlerOptions{
			Validator: func(ctx workflow.Context, request PermissionRequest) error {
				if !slices.Contains(sessions(), request.Session) {
					return fmt.Errorf("session %q is not one this workflow opened", request.Session)
				}
				return nil
			},
			Description: "A permission request from the agent of a session this workflow opened.",
		})
}
