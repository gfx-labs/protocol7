package chat

import (
	"encoding/json"
	"errors"
	"fmt"
	"time"

	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"

	"github.com/gfx-labs/protocol7/pkg/execution/artifact"
	"github.com/gfx-labs/protocol7/pkg/execution/kubeop"
	"github.com/gfx-labs/protocol7/pkg/p7"
)

// operatorSubject is what the decision to give a chat the operator credential
// is made from. The workflow's state is the source: nothing the agent says
// reaches it.
func operatorSubject(state *State) kubeop.Subject {
	request := state.Request
	subject := kubeop.Subject{
		Chat: request.ID, Project: request.Project, Restricted: request.Restricted, Parent: request.Parent,
		Escalation: request.Escalation != nil, Handler: request.Handler, Kubernetes: request.Backend == p7.BackendKubernetes,
		MessageRestricted: state.Current != nil && state.Current.Restricted,
	}
	if request.StartedBy != nil {
		subject.StartedBy = request.StartedBy.Login
	}
	for _, repository := range request.Repositories {
		repo := githubRepo(repository.URL)
		if repo == "" {
			repo = repository.URL
		}
		subject.Repositories = append(subject.Repositories, repo)
	}
	if request.Issue != nil {
		subject.Issue = &kubeop.Issue{Repo: request.Issue.Repo, Number: request.Issue.Number}
	}
	return subject
}

const operatorDescription = `Get a Kubernetes credential for the cluster that lives for one hour, to verify an infrastructure change yourself, such as running scripts/verify-isolation and scripts/verify-session-pods. Only a chat that an admin started, or whose issue an admin labelled, on the core repositories gets it. Call it again to renew it.
The credential is the service account selfdev-operator. It creates and deletes only the labelled, isotest-named namespaces, pods, network policies and SPIFFE registrations that those scripts make, and it may run as the service accounts they check. It reads no Secrets other than the image pull secret and changes no RBAC, Flux or Helm object. It changes no existing workload either, with one exception: it may exec into and delete the session pods named p7-session-* in protocol7-selfdev, which are other chats' live sessions (verify-session-pods needs it). Do not touch a session pod that is not the one you started. The result names a kubeconfig: use it as KUBECONFIG=<path> kubectl --context selfdev-operator ..., or pass the context name selfdev-operator to the scripts.`

// operatorTools gives an eligible chat the kube_operator tool. A chat that is
// not eligible does not see it, and the call checks again.
func operatorTools(state *State) []p7.Tool {
	if state.operatorOff {
		return nil
	}
	if _, err := kubeop.Check(operatorSubject(state)); err != nil {
		return nil
	}
	request := state.Request
	issue := workflow.ActivityOptions{TaskQueue: TaskQueue, StartToCloseTimeout: 30 * time.Second, RetryPolicy: &temporal.RetryPolicy{MaximumAttempts: 3, NonRetryableErrorTypes: []string{kubeop.ErrorRefused}}}
	write := workflow.ActivityOptions{
		TaskQueue: artifact.Queue(request.Host), ScheduleToStartTimeout: 30 * time.Second, StartToCloseTimeout: time.Minute,
		RetryPolicy: &temporal.RetryPolicy{MaximumAttempts: 3, NonRetryableErrorTypes: []string{kubeop.ErrorRefused}},
	}
	return []p7.Tool{{
		Name:        "kube_operator",
		Description: operatorDescription,
		InputSchema: json.RawMessage(`{"type":"object","properties":{},"additionalProperties":false}`),
		Run: func(ctx workflow.Context, call p7.ToolCall) (p7.ToolResult, error) {
			subject := operatorSubject(state)
			if _, err := kubeop.Check(subject); err != nil {
				return p7.ToolResult{}, err
			}
			var credentials kubeop.Credentials
			if err := workflow.ExecuteActivity(workflow.WithActivityOptions(ctx, issue), kubeop.ActivityIssue, subject).Get(ctx, &credentials); err != nil {
				return p7.ToolResult{}, fmt.Errorf("%s", cause(err))
			}
			if state.Operator == nil {
				state.Operator = &kubeop.Access{}
			}
			state.Operator.Record(kubeop.Issued{At: workflow.Now(ctx), Expires: credentials.Expires, By: credentials.By, Basis: credentials.Basis})
			var result kubeop.WriteResult
			err := workflow.ExecuteActivity(workflow.WithActivityOptions(ctx, write), kubeop.ActivityWrite, kubeop.WriteRequest{Session: call.Session, Credentials: credentials}).Get(ctx, &result)
			if err != nil {
				if timeout, ok := errors.AsType[*temporal.TimeoutError](err); ok && timeout.TimeoutType() == enumspb.TIMEOUT_TYPE_SCHEDULE_TO_START {
					return p7.ToolResult{}, fmt.Errorf("the credential could not be placed: nothing on this host answered within 30 seconds")
				}
				return p7.ToolResult{}, fmt.Errorf("the credential could not be placed: %s", cause(err))
			}
			left := credentials.Expires.Sub(workflow.Now(ctx)).Round(time.Minute)
			return p7.TextResult(fmt.Sprintf("The credential is placed. It expires at %s (in %s). Call kube_operator again to renew it.\nkubeconfig: %s\nUse: KUBECONFIG=%s kubectl --context %s ...\nThe scripts take the context name: KUBECONFIG=%s scripts/verify-isolation %s --network-only",
				credentials.Expires.UTC().Format(time.RFC3339), left, result.Kubeconfig, result.Kubeconfig, kubeop.Context, result.Kubeconfig, kubeop.Context)), nil
		},
	}}
}
