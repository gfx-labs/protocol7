package review

import (
	"errors"

	"github.com/gfx-labs/protocol7/pkg/p7"
)

// Options are the review workflow's settings in an app's worker. They are the
// GitHub client and what it may do (GitHubConfig): Repositories, a review may
// be filed for, and MergeMethod.
type Options = GitHubConfig

// Register registers the review workflow and its GitHub activities on the
// worker.
func Register(worker *p7.Worker, options Options) error {
	if worker == nil {
		return errors.New("review: register on a nil worker")
	}
	worker.RegisterWorkflow(WorkflowName, Workflow)
	worker.RegisterActivities(NewActivities(options))
	return nil
}
