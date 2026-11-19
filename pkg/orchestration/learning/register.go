package learning

import "github.com/gfx-labs/protocol7/pkg/p7"

// Register registers the learning workflow and its activities on the worker.
func Register(worker *p7.Worker) {
	worker.RegisterWorkflow(WorkflowName, Workflow)
	activities := &Activities{Store: Store{Client: worker.Temporal()}}
	worker.RegisterActivity(ActivityRecord, activities.Record)
	worker.RegisterActivity(ActivitySearch, activities.Search)
	worker.RegisterActivity(ActivityBrief, activities.Brief)
	worker.RegisterActivity(ActivityDecide, activities.Decide)
}
