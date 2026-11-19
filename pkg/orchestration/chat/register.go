package chat

import (
	"errors"

	"github.com/gfx-labs/protocol7/pkg/execution/design"
	"github.com/gfx-labs/protocol7/pkg/orchestration/learning"
	"github.com/gfx-labs/protocol7/pkg/p7"
)

// Options are what an app's worker sets for its chats. The zero value is a
// tenant's chat: no integrations, no operator credential, no extra tools.
type Options struct {
	// Tools are tools the app adds to every chat's session, after the
	// built-in ones. A handler's chat does not get them.
	Tools []p7.Tool
	// Integrations say which outside services the chat's tools reach.
	Integrations Integrations
	// OperatorCredentials offers the kube_operator tool to eligible chats
	// (kubeop.Check). The app registers the broker activity
	// (kubeop.ActivityIssue) itself. Off for tenants.
	OperatorCredentials bool
}

// RegisterConfig is what Register needs besides the Options.
type RegisterConfig struct {
	Options Options
	// Handler is the escalation handler's model choice.
	Handler HandlerConfig
	// Linear and Web are the clients of the Linear and web tools. They are
	// registered only when Options.Integrations turns the tools on.
	Linear LinearConfig
	Web    WebConfig
	// GitHubTokenFile and GitHubAPIURL are the GitHub client of the issue and
	// pull request tools. An empty API URL is https://api.github.com.
	GitHubTokenFile string
	GitHubAPIURL    string
	// Files is the session file server that the design tools read.
	Files design.FilesClient
}

// Register registers the chat, escalation, handler and learning workflows
// on the worker, with the activities they run in the worker's process.
func Register(worker *p7.Worker, config RegisterConfig) error {
	if worker == nil {
		return errors.New("chat: register on a nil worker")
	}
	integrations := config.Options.Integrations
	worker.RegisterWorkflow(WorkflowName, newWorkflow(config.Options))
	if integrations.Linear {
		worker.RegisterActivities(NewLinearActivities(config.Linear))
	}
	if integrations.Web {
		worker.RegisterActivities(NewWebActivities(config.Web))
	}
	worker.RegisterWorkflow(EscalationWorkflowName, EscalationWorkflow)
	worker.RegisterWorkflow(HandlerWorkflowName, HandlerWorkflow)
	worker.RegisterActivities(&HandlerActivities{Client: worker.Temporal(), Config: config.Handler})
	learning.Register(worker)
	worker.RegisterActivities(NewIssueActivities(config.GitHubTokenFile, config.GitHubAPIURL))
	designActivities := design.Activities{Files: config.Files}
	worker.RegisterActivity(design.ActivityCheck, designActivities.Check)
	worker.RegisterActivity(design.ActivityShot, designActivities.Shot)
	return nil
}
