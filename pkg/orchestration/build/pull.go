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
	ActivityBranchHead = "build.BranchHead"
	ActivityPinBranch  = "build.PinBranch"
)

// BranchRequest names the PR maker's already-pushed branch.
type BranchRequest struct {
	Repository string
	Branch     string
}

// PinRequest commits a completed build's pins to the branch that was built.
type PinRequest struct {
	BranchRequest
	Result Result
}

// PullActivities run as the PR maker, never as the read-only builder.
type PullActivities struct {
	GitHub       *githubapi.Client
	Registry     string
	Repositories []string
}

func (a *PullActivities) enabled(repository string) bool {
	return slices.ContainsFunc(a.Repositories, func(allowed string) bool { return strings.EqualFold(allowed, repository) })
}

// BranchHead returns no commit for repositories without a configured builder.
func (a *PullActivities) BranchHead(ctx context.Context, request BranchRequest) (string, error) {
	if !a.enabled(request.Repository) {
		return "", nil
	}
	var branch struct {
		Commit struct{ SHA string } `json:"commit"`
	}
	if err := a.GitHub.Do(ctx, "GET", "/repos/"+request.Repository+"/branches/"+url.PathEscape(request.Branch), nil, &branch); err != nil {
		return "", githubapi.Settle(err)
	}
	if !commitPattern.MatchString(branch.Commit.SHA) {
		return "", fmt.Errorf("build: branch %s returned no valid commit", request.Branch)
	}
	return branch.Commit.SHA, nil
}

// BuildBranch waits for a branch-head build and the PR maker's pin commit.
// The returned head lets the caller refuse to open a PR if the branch moved.
func BuildBranch(ctx workflow.Context, request BranchRequest, options BuildOptions) (string, error) {
	activityContext := workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		StartToCloseTimeout: 5 * time.Minute,
		RetryPolicy:         &temporal.RetryPolicy{MaximumAttempts: 3},
	})
	var head string
	if err := workflow.ExecuteActivity(activityContext, ActivityBranchHead, request).Get(ctx, &head); err != nil {
		return "", err
	}
	if head == "" {
		return "", nil
	}
	result, err := Build(ctx, Request{Repository: request.Repository, Commit: head, Trigger: TriggerPullRequest}, options)
	if err != nil {
		return "", err
	}
	if result.Commit != head || len(result.Images) == 0 {
		return "", fmt.Errorf("build: result does not contain images for branch head %s", head)
	}
	var pinned string
	if err := workflow.ExecuteActivity(activityContext, ActivityPinBranch, PinRequest{BranchRequest: request, Result: result}).Get(ctx, &pinned); err != nil {
		return "", err
	}
	return pinned, nil
}

// PinBranch writes only marked kubernetes files, with a non-forced fast-forward
// of the built head. A concurrent branch push cannot be overwritten.
func (a *PullActivities) PinBranch(ctx context.Context, request PinRequest) (string, error) {
	if !a.enabled(request.Repository) || !commitPattern.MatchString(request.Result.Commit) || len(request.Result.Images) == 0 {
		return "", temporal.NewNonRetryableApplicationError("build: pin request is not enabled or has no valid build", ErrorNotAllowed, nil)
	}
	head, err := a.BranchHead(ctx, request.BranchRequest)
	if err != nil {
		return "", err
	}
	prefix := "/repos/" + request.Repository + "/git/"
	// A lost response after the update is safe to retry if the branch is exactly
	// our pin commit, with the built commit as its only parent.
	var previousPinTree string
	if head != request.Result.Commit {
		var commit struct {
			Message string                 `json:"message"`
			Tree    struct{ SHA string }   `json:"tree"`
			Parents []struct{ SHA string } `json:"parents"`
		}
		if err := a.GitHub.Do(ctx, "GET", prefix+"commits/"+head, nil, &commit); err != nil {
			return "", githubapi.Settle(err)
		}
		if len(commit.Parents) != 1 || commit.Parents[0].SHA != request.Result.Commit || commit.Message != pinMessage(request.Result.Commit) {
			return "", temporal.NewNonRetryableApplicationError("build: branch moved while its images were building; retry pr_create for the new head", ErrorNotAllowed, nil)
		}
		previousPinTree = commit.Tree.SHA
	}
	var tree struct {
		SHA       string `json:"sha"`
		Truncated bool   `json:"truncated"`
		Tree      []struct {
			Path string `json:"path"`
			Mode string `json:"mode"`
			Type string `json:"type"`
			SHA  string `json:"sha"`
		} `json:"tree"`
	}
	if err := a.GitHub.Do(ctx, "GET", prefix+"trees/"+request.Result.Commit+"?recursive=1", nil, &tree); err != nil {
		return "", githubapi.Settle(err)
	}
	if tree.Truncated {
		return "", fmt.Errorf("build: manifest tree is truncated")
	}
	files := make(map[string]string)
	modes := make(map[string]string)
	for _, entry := range tree.Tree {
		if entry.Type != "blob" || (entry.Mode != "100644" && entry.Mode != "100755") || !strings.HasPrefix(entry.Path, "kubernetes/") || (path.Ext(entry.Path) != ".yaml" && path.Ext(entry.Path) != ".yml") {
			continue
		}
		var blob struct{ Content, Encoding string }
		if err := a.GitHub.Do(ctx, "GET", prefix+"blobs/"+entry.SHA, nil, &blob); err != nil {
			return "", githubapi.Settle(err)
		}
		if blob.Encoding != "base64" {
			return "", fmt.Errorf("build: unsupported blob encoding %q", blob.Encoding)
		}
		content, err := base64.StdEncoding.DecodeString(strings.ReplaceAll(blob.Content, "\n", ""))
		if err != nil {
			return "", fmt.Errorf("build: decode %s: %w", entry.Path, err)
		}
		files[entry.Path], modes[entry.Path] = string(content), entry.Mode
	}
	changed, err := PinFiles(files, a.Registry, request.Result.Images)
	if err != nil {
		return "", temporal.NewNonRetryableApplicationError(err.Error(), ErrorNotAllowed, err)
	}
	if len(changed) == 0 && head == request.Result.Commit {
		return head, nil
	}
	var entries []map[string]string
	for filename, content := range changed {
		entries = append(entries, map[string]string{"path": filename, "mode": modes[filename], "type": "blob", "content": content})
	}
	slices.SortFunc(entries, func(a, b map[string]string) int { return strings.Compare(a["path"], b["path"]) })
	var updated struct{ SHA string }
	if err := a.GitHub.Do(ctx, "POST", prefix+"trees", map[string]any{"base_tree": tree.SHA, "tree": entries}, &updated); err != nil {
		return "", githubapi.Settle(err)
	}
	if head != request.Result.Commit {
		if previousPinTree == updated.SHA {
			return head, nil
		}
		return "", temporal.NewNonRetryableApplicationError("build: branch moved and its tree differs from the pin commit", ErrorNotAllowed, nil)
	}
	var commit struct{ SHA string }
	if err := a.GitHub.Do(ctx, "POST", prefix+"commits", map[string]any{"message": pinMessage(head), "tree": updated.SHA, "parents": []string{head}}, &commit); err != nil {
		return "", githubapi.Settle(err)
	}
	if err := a.GitHub.Do(ctx, "PATCH", prefix+"refs/heads/"+url.PathEscape(request.Branch), map[string]any{"sha": commit.SHA, "force": false}, nil); err != nil {
		return "", githubapi.Settle(err)
	}
	return commit.SHA, nil
}

func pinMessage(commit string) string { return "pin: builder images for " + commit }
