// Package upgrade is how the long-lived workflows (chat, issue and review)
// leave a worker build that is being replaced.
//
// The worker declares a Worker Deployment Version (see the --worker-deployment
// and --worker-build-id flags), so every run is pinned to the build it
// started on, and an old build keeps polling until its pinned runs end. A
// workflow that runs for days would hold that build for as long, so at its
// safe point it continues as new when the deployment's target version is no
// longer its own, and asks to start the new run on the target (auto-upgrade).
// protocol7/docs/plan.md, "Worker Versioning" and "Long-lived flow workflows",
// describes the design.
//
// Replay: GetTargetWorkerDeploymentVersionChanged is read from the workflow
// task's started event, so it is part of the history, and a history written
// before the field existed reads false. A check that follows it adds no
// command to such a history.
package upgrade

import "go.temporal.io/sdk/workflow"

// Changed is true when the deployment's target version is not the version this
// run is on.
func Changed(ctx workflow.Context) bool {
	return workflow.GetInfo(ctx).GetTargetWorkerDeploymentVersionChanged()
}

// Due is true when the workflow should continue as new at its next safe point:
// Temporal suggests it because the history is large, or a newer build is the
// target.
func Due(ctx workflow.Context) bool {
	info := workflow.GetInfo(ctx)
	return info.GetContinueAsNewSuggested() || info.GetTargetWorkerDeploymentVersionChanged()
}

// ContinueAsNew returns the error that continues the workflow as a new run of
// the workflow type name with the given arguments. When the target version
// changed, the run starts on the target version (auto-upgrade). Otherwise it
// is an ordinary continue-as-new, which keeps the run's own version. Without
// worker versioning the target never changes.
func ContinueAsNew(ctx workflow.Context, name string, args ...any) error {
	if !Changed(ctx) {
		return workflow.NewContinueAsNewError(ctx, name, args...)
	}
	return workflow.NewContinueAsNewErrorWithOptions(ctx, workflow.ContinueAsNewErrorOptions{
		InitialVersioningBehavior: workflow.ContinueAsNewVersioningBehaviorAutoUpgrade,
	}, name, args...)
}
