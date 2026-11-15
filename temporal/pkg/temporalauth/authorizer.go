package temporalauth

import (
	"context"
	"strings"

	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/common/api"
	"go.temporal.io/server/common/authorization"
)

// Authorizer adds task-only worker roles and namespace-scoped API grants.
// Writer, reader and admin permissions retain the server's normal semantics.
type Authorizer struct {
	defaults authorization.Authorizer
}

func NewAuthorizer() *Authorizer {
	return &Authorizer{defaults: authorization.NewDefaultAuthorizer()}
}

func (a *Authorizer) Authorize(ctx context.Context, claims *authorization.Claims, target *authorization.CallTarget) (authorization.Result, error) {
	if target == nil || claims == nil {
		return authorization.Result{Decision: authorization.DecisionDeny}, nil
	}
	if target.APIName == api.WorkflowServicePrefix+"RespondWorkflowTaskCompleted" {
		if request, ok := target.Request.(*workflowservice.RespondWorkflowTaskCompletedRequest); ok {
			for _, command := range request.GetCommands() {
				var namespace, method string
				switch command.GetCommandType() {
				case enumspb.COMMAND_TYPE_START_CHILD_WORKFLOW_EXECUTION:
					namespace, method = command.GetStartChildWorkflowExecutionCommandAttributes().GetNamespace(), "StartWorkflowExecution"
				case enumspb.COMMAND_TYPE_SIGNAL_EXTERNAL_WORKFLOW_EXECUTION:
					namespace, method = command.GetSignalExternalWorkflowExecutionCommandAttributes().GetNamespace(), "SignalWorkflowExecution"
				case enumspb.COMMAND_TYPE_REQUEST_CANCEL_EXTERNAL_WORKFLOW_EXECUTION:
					namespace, method = command.GetRequestCancelExternalWorkflowExecutionCommandAttributes().GetNamespace(), "RequestCancelWorkflowExecution"
				default:
					continue
				}
				if namespace == "" {
					namespace = target.Namespace
				}
				result, err := a.Authorize(ctx, claims, &authorization.CallTarget{Namespace: namespace, APIName: api.WorkflowServicePrefix + method})
				if err != nil || result.Decision != authorization.DecisionAllow {
					return result, err
				}
			}
		}
	}
	permissions, _ := claims.Extensions.(namespaceAPIs)
	allowedAPI := target.Namespace != "" && permissions[target.Namespace][target.APIName]
	allowedTask := target.Namespace != "" && claims.Namespaces[target.Namespace]&authorization.RoleWorker != 0 && workerMethod(target.APIName)
	if allowedAPI || allowedTask {
		return authorization.Result{Decision: authorization.DecisionAllow, Principal: &commonpb.Principal{Type: claims.AuthType, Name: claims.Subject}}, nil
	}
	return a.defaults.Authorize(ctx, claims, target)
}

func workerMethod(method string) bool {
	if !strings.HasPrefix(method, api.WorkflowServicePrefix) {
		return false
	}
	switch strings.TrimPrefix(method, api.WorkflowServicePrefix) {
	case "PollWorkflowTaskQueue", "RespondWorkflowTaskCompleted", "RespondWorkflowTaskFailed",
		"PollActivityTaskQueue", "RecordActivityTaskHeartbeat", "RecordActivityTaskHeartbeatById",
		"RespondActivityTaskCompleted", "RespondActivityTaskCompletedById",
		"RespondActivityTaskFailed", "RespondActivityTaskFailedById",
		"RespondActivityTaskCanceled", "RespondActivityTaskCanceledById",
		"RespondQueryTaskCompleted", "ResetStickyTaskQueue", "ShutdownWorker",
		"GetWorkflowExecutionHistory", "DescribeNamespace", "DescribeTaskQueue",
		"GetWorkerBuildIdCompatibility", "GetWorkerVersioningRules", "RecordWorkerHeartbeat":
		return true
	default:
		return false
	}
}
