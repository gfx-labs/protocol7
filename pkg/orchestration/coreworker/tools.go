package coreworker

import (
	"context"
	"fmt"

	"go.temporal.io/sdk/client"

	"github.com/gfx-labs/protocol7/pkg/domain"
)

// ToolCalls sends sessions' tool calls to the workflows that own them, as
// the Temporal update domain.UpdateToolCall.
type ToolCalls struct {
	Temporal client.Client
}

// CallTool runs one call and waits for its result. The call ID is the
// update ID, so a call that the shim sends again after a reconnect gets the
// first call's result and does not run twice. With no run ID the update
// reaches the owner's current run, also after Continue-As-New.
func (t ToolCalls) CallTool(ctx context.Context, owner, callID string, call domain.ToolCall) (domain.ToolResult, error) {
	var result domain.ToolResult
	handle, err := t.Temporal.UpdateWorkflow(ctx, client.UpdateWorkflowOptions{
		UpdateID:     callID,
		WorkflowID:   owner,
		UpdateName:   domain.UpdateToolCall,
		Args:         []any{call},
		WaitForStage: client.WorkflowUpdateStageCompleted,
	})
	if err != nil {
		return result, fmt.Errorf("call tool %s of session %s in workflow %s: %w", call.Tool, call.Session, owner, err)
	}
	if err := handle.Get(ctx, &result); err != nil {
		return result, fmt.Errorf("call tool %s of session %s in workflow %s: %w", call.Tool, call.Session, owner, err)
	}
	return result, nil
}
