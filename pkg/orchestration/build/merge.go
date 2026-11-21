package build

import (
	"context"
	"encoding/base64"
	"fmt"
	"net/url"
	"path"
	"slices"
	"strings"
	"time"

	"github.com/gfx-labs/protocol7/pkg/transport/githubapi"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"
)

const (
	ActivityPinMergeEnabled = "build.PinMergeEnabled"
	ActivityMergePinPull    = "build.MergePinPull"
)

// PullRequest identifies a PR opened by the calling PR maker at its pinned head.
type PullRequest struct {
	BranchRequest
	Number int
	Head   string
}

// MergePinRequest carries a build result obtained by the workflow, not an agent.
type MergePinRequest struct {
	PullRequest
	Result Result
}

// PinMergeStatus leaves rejected PRs on their normal review path.
type PinMergeStatus struct {
	Merged  bool
	Pending bool
	Reason  string
}

// AutoMergePull builds the final pin commit, then durably waits for exact-head CI.
// Feature PRs stay open. No result or merge authority is exposed as an agent tool.
func AutoMergePull(ctx workflow.Context, request PullRequest, timeout time.Duration) (PinMergeStatus, error) {
	ctx = workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		StartToCloseTimeout: 5 * time.Minute,
		RetryPolicy:         &temporal.RetryPolicy{MaximumAttempts: 3},
	})
	var enabled bool
	if err := workflow.ExecuteActivity(ctx, ActivityPinMergeEnabled, request.Repository).Get(ctx, &enabled); err != nil {
		return PinMergeStatus{}, err
	}
	if !enabled || request.Head == "" {
		return PinMergeStatus{Reason: "pin-only auto-merge is not enabled"}, nil
	}
	// PinFiles does not change image inputs, so this normally reuses the source
	// build while binding its result to the final PR head in Temporal history.
	result, err := Build(ctx, Request{Repository: request.Repository, Commit: request.Head, Trigger: TriggerPullRequest, PullRequest: request.Number}, BuildOptions{Timeout: time.Hour})
	if err != nil {
		return PinMergeStatus{}, err
	}
	deadline := workflow.Now(ctx).Add(timeout)
	for {
		var status PinMergeStatus
		if err := workflow.ExecuteActivity(ctx, ActivityMergePinPull, MergePinRequest{PullRequest: request, Result: result}).Get(ctx, &status); err != nil {
			return PinMergeStatus{}, err
		}
		if !status.Pending {
			return status, nil
		}
		if !workflow.Now(ctx).Before(deadline) {
			return PinMergeStatus{Reason: "timed out waiting for exact-head CI, normal review still applies"}, nil
		}
		if err := workflow.Sleep(ctx, 15*time.Second); err != nil {
			return PinMergeStatus{}, err
		}
	}
}

// PinMergeEnabled is separate from the builder allowlist. Opt-in is fail-closed.
func (a *PullActivities) PinMergeEnabled(_ context.Context, repository string) (bool, error) {
	return a.enabled(repository) && a.CIWorkflow != "" && slices.ContainsFunc(a.AutoMergeRepositories, func(allowed string) bool {
		return strings.EqualFold(allowed, repository)
	}), nil
}

// MergePinPull rechecks the whole live diff and CI on every attempt, then asks
// GitHub to merge only the expected head, without bypassing repository rules.
func (a *PullActivities) MergePinPull(ctx context.Context, request MergePinRequest) (PinMergeStatus, error) {
	reject := func(reason string) (PinMergeStatus, error) { return PinMergeStatus{Reason: reason}, nil }
	enabled, _ := a.PinMergeEnabled(ctx, request.Repository)
	if !enabled || request.Number <= 0 || !strings.HasPrefix(request.Branch, "chat/") || !commitPattern.MatchString(request.Head) || request.Result.Commit != request.Head || len(request.Result.Images) == 0 {
		return reject("not an enabled PR maker pull request with an exact-head build")
	}
	prefix := "/repos/" + request.Repository
	var repository struct {
		DefaultBranch string `json:"default_branch"`
	}
	if err := a.GitHub.Do(ctx, "GET", prefix, nil, &repository); err != nil {
		return PinMergeStatus{}, githubapi.Settle(err)
	}
	var pull struct {
		State         string
		Draft, Merged bool
		Head, Base    struct {
			SHA, Ref string
			Repo     struct {
				FullName string `json:"full_name"`
			}
		}
	}
	pullPath := fmt.Sprintf("%s/pulls/%d", prefix, request.Number)
	if err := a.GitHub.Do(ctx, "GET", pullPath, nil, &pull); err != nil {
		return PinMergeStatus{}, githubapi.Settle(err)
	}
	if pull.Head.SHA != request.Head || pull.Head.Ref != request.Branch || !strings.EqualFold(pull.Head.Repo.FullName, request.Repository) || !strings.EqualFold(pull.Base.Repo.FullName, request.Repository) || pull.Base.Ref != repository.DefaultBranch || pull.Draft {
		return reject("PR moved, is a draft, or does not target this repository's default branch")
	}
	if pull.Merged {
		return PinMergeStatus{Merged: true, Reason: "already merged at the expected head"}, nil
	}
	if pull.State != "open" {
		return reject("PR is not open")
	}
	before, after, err := a.pinTrees(ctx, prefix, pull.Base.SHA, request.Head)
	if err != nil {
		return PinMergeStatus{}, err
	}
	if err := PinOnly(before, after, a.Registry, request.Result.Images); err != nil {
		return reject(err.Error())
	}
	// The workflow is selected by its configured filename, not a check name
	// another App could publish. Only its newest PR run for this head counts.
	var runs struct {
		WorkflowRuns []struct {
			HeadSHA                   string `json:"head_sha"`
			Status, Conclusion, Event string
		} `json:"workflow_runs"`
	}
	runsPath := prefix + "/actions/workflows/" + url.PathEscape(a.CIWorkflow) + "/runs?" + url.Values{"head_sha": {request.Head}, "event": {"pull_request"}, "per_page": {"1"}}.Encode()
	if err := a.GitHub.Do(ctx, "GET", runsPath, nil, &runs); err != nil {
		return PinMergeStatus{}, githubapi.Settle(err)
	}
	if len(runs.WorkflowRuns) == 0 {
		return PinMergeStatus{Pending: true, Reason: "waiting for exact-head CI"}, nil
	}
	run := runs.WorkflowRuns[0]
	if run.HeadSHA != request.Head || run.Event != "pull_request" {
		return reject("CI run does not match the PR head")
	}
	if run.Status != "completed" {
		return PinMergeStatus{Pending: true, Reason: "waiting for exact-head CI"}, nil
	}
	if run.Conclusion != "success" {
		return reject("exact-head CI did not succeed")
	}
	var merged struct {
		Merged  bool
		Message string
	}
	if err := a.GitHub.Do(ctx, "PUT", pullPath+"/merge", map[string]any{"sha": request.Head, "merge_method": "squash"}, &merged); err != nil {
		return PinMergeStatus{}, githubapi.Settle(err)
	}
	return PinMergeStatus{Merged: merged.Merged, Reason: merged.Message}, nil
}

// pinTrees includes non-manifest blobs as opaque Git object identities, so
// additions, deletions, mode changes and changes outside marked lines fail closed.
func (a *PullActivities) pinTrees(ctx context.Context, prefix, base, head string) (map[string]string, map[string]string, error) {
	read := func(commit string) (map[string]string, error) {
		var tree struct {
			Truncated bool
			Tree      []struct{ Path, SHA, Mode, Type string }
		}
		if err := a.GitHub.Do(ctx, "GET", prefix+"/git/trees/"+commit+"?recursive=1", nil, &tree); err != nil {
			return nil, githubapi.Settle(err)
		}
		if tree.Truncated {
			return nil, fmt.Errorf("build: pin merge tree is truncated")
		}
		files := make(map[string]string)
		for _, entry := range tree.Tree {
			if entry.Type == "tree" {
				continue
			}
			value := entry.Mode + ":" + entry.Type + ":" + entry.SHA
			if strings.HasPrefix(entry.Path, "kubernetes/") && (path.Ext(entry.Path) == ".yaml" || path.Ext(entry.Path) == ".yml") && entry.Type == "blob" && entry.Mode == "100644" {
				var blob struct{ Content, Encoding string }
				if err := a.GitHub.Do(ctx, "GET", prefix+"/git/blobs/"+entry.SHA, nil, &blob); err != nil {
					return nil, githubapi.Settle(err)
				}
				if blob.Encoding != "base64" {
					return nil, fmt.Errorf("build: unsupported pin blob encoding %q", blob.Encoding)
				}
				content, err := base64.StdEncoding.DecodeString(strings.ReplaceAll(blob.Content, "\n", ""))
				if err != nil {
					return nil, err
				}
				value = string(content)
			}
			files[entry.Path] = value
		}
		return files, nil
	}
	before, err := read(base)
	if err != nil {
		return nil, nil, err
	}
	after, err := read(head)
	return before, after, err
}
