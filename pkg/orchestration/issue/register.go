package issue

import (
	"errors"

	"github.com/gfx-labs/protocol7/pkg/p7"
)

// Options are the issue workflows' GitHub client.
type Options struct {
	// GitHubTokenFile holds an installation token, read on each call.
	GitHubTokenFile string
	// GitHubAPIURL is GitHub's API. Empty is https://api.github.com.
	GitHubAPIURL string
}

// Register registers the issue, intake, scanner and old scan workflows and
// their activities on the worker. The issue workflow files reviews and
// starts chats, so the app registers those too.
func Register(worker *p7.Worker, options Options) error {
	if worker == nil {
		return errors.New("issue: register on a nil worker")
	}
	worker.RegisterWorkflow(WorkflowName, Workflow)
	worker.RegisterWorkflow(ScanWorkflowName, ScanWorkflow)
	worker.RegisterWorkflow(ScannerWorkflowName, ScannerWorkflow)
	worker.RegisterWorkflow(IntakeWorkflowName, IntakeWorkflow)
	worker.RegisterActivities(NewActivities(options.GitHubTokenFile, options.GitHubAPIURL))
	return nil
}
