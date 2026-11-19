package navi

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"

	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/converter"
	"go.temporal.io/sdk/temporal"

	"github.com/gfx-labs/protocol7/pkg/domain/scope"
)

// MaxPicks bounds the picked elements of one Message.
const MaxPicks = 8

// IssueIntakeRunTimeout is the run timeout navi starts IssueIntakeWorkflow
// with: the worst case of the intake's label write and its retries, and a
// minute for the workflow's own tasks.
const IssueIntakeRunTimeout = 6*time.Minute + 30*time.Second

// LearningListLimit bounds the learnings one ListLearnings returns.
const LearningListLimit = 500

// ChatMemo is the memo a chat's workflow starts with.
func ChatMemo(request ChatRequest) map[string]any {
	return map[string]any{MemoTitle: request.Title, MemoContractVersion: ContractVersion}
}

// ChatSearchAttributes are the search attributes a chat's workflow starts
// with.
func ChatSearchAttributes(request ChatRequest) temporal.SearchAttributes {
	updates := []temporal.SearchAttributeUpdate{AttributeKind.ValueSet(string(request.Kind()))}
	if request.StartedBy != nil {
		updates = append(updates, AttributeStartedBy.ValueSet(request.StartedBy.Login))
	}
	if request.Owner != "" {
		updates = append(updates, AttributeOwner.ValueSet(request.Owner))
	}
	if request.Issue != nil {
		updates = append(updates, AttributeIssue.ValueSet(fmt.Sprintf("%s#%d", request.Issue.Repo, request.Issue.Number)))
	}
	if request.Project != "" {
		updates = append(updates, AttributeProject.ValueSet(request.Project))
	}
	if request.Parent != "" {
		updates = append(updates, AttributeParent.ValueSet(request.Parent))
	}
	return temporal.NewSearchAttributes(updates...)
}

// ChatBranch is the branch every clone of a chat is checked out on.
func ChatBranch(chatID string) string { return "chat/" + chatID }

// GitHubRepo is owner/name of a github.com clone URL, or false for a URL
// that is not one.
func GitHubRepo(cloneURL string) (string, bool) {
	for _, prefix := range []string{"https://github.com/", "git@github.com:", "ssh://git@github.com/"} {
		if rest, ok := strings.CutPrefix(cloneURL, prefix); ok {
			rest = strings.TrimSuffix(strings.TrimSuffix(rest, "/"), ".git")
			if owner, name, ok := strings.Cut(rest, "/"); ok && owner != "" && name != "" && !strings.Contains(name, "/") {
				return owner + "/" + name, true
			}
		}
	}
	return "", false
}

// ScopedRepositories are the repositories of a chat that its scope reaches. A
// repository that is not on github.com, or is not in the scope, is not
// cloned. Without a scope, every repository is kept.
func ScopedRepositories(repositories []Repository, held *scope.Scope) []Repository {
	if held == nil {
		return repositories
	}
	kept := make([]Repository, 0, len(repositories))
	for _, repository := range repositories {
		if repo, ok := GitHubRepo(repository.URL); ok && held.HasRepository(repo) {
			kept = append(kept, repository)
		}
	}
	return kept
}

// ReviewCanMerge says whether the review's scope lets it merge: the pull
// request's repository with write on pull requests and contents. A review
// without a scope may.
func ReviewCanMerge(request ReviewRequest) bool {
	held := request.Scope
	return held == nil || (held.HasRepository(request.Repo) && held.Allows("pull_requests", "write") && held.Allows("contents", "write"))
}

// ParseReviewURL reads a pull request's address,
// https://github.com/owner/name/pull/N.
func ParseReviewURL(address string) (repo string, number int, err error) {
	parsed, err := url.Parse(strings.TrimSpace(address))
	if err != nil || parsed.Host != "github.com" {
		return "", 0, fmt.Errorf("%q is not a github.com pull request address", address)
	}
	parts := strings.Split(strings.Trim(parsed.Path, "/"), "/")
	if len(parts) < 4 || parts[2] != "pull" {
		return "", 0, fmt.Errorf("%q is not a pull request address, want https://github.com/owner/name/pull/N", address)
	}
	number, err = strconv.Atoi(parts[3])
	if err != nil || number <= 0 {
		return "", 0, fmt.Errorf("%q has no pull request number", address)
	}
	return parts[0] + "/" + parts[1], number, nil
}

// FileReview files the review of a pull request. A pull request that already
// has a running review gets SignalRereview instead of a second review, so
// filing it again after a push asks for another look.
func FileReview(ctx context.Context, temporalClient client.Client, request ReviewRequest) (workflowID string, err error) {
	if request.Repo == "" || request.Number <= 0 {
		return "", errors.New("a review needs a repository and a pull request number")
	}
	workflowID = ReviewWorkflowID(request.Repo, request.Number)
	_, err = temporalClient.SignalWithStartWorkflow(ctx, workflowID, SignalRereview, nil, client.StartWorkflowOptions{
		ID:        workflowID,
		TaskQueue: TaskQueue,
		Memo:      map[string]any{"repo": request.Repo, "number": request.Number, MemoContractVersion: ContractVersion},
	}, ReviewWorkflow, ReviewState{Request: request})
	if err != nil {
		return "", fmt.Errorf("file the review of %s#%d: %w", request.Repo, request.Number, err)
	}
	return workflowID, nil
}

var learningIDPattern = regexp.MustCompile(`^[a-z0-9][a-z0-9-]{0,62}$`)

// ValidLearningID says whether id can be a learning's ID.
func ValidLearningID(id string) bool { return learningIDPattern.MatchString(id) }

// learningListQuery lists the proposed and the accepted learnings, the ones
// whose workflow runs.
const learningListQuery = "WorkflowType = '" + LearningWorkflow + "' AND ExecutionStatus = 'Running'"

// ListLearnings reads the learnings that wait or are accepted, newest first,
// at most LearningListLimit. Each one's state is the search attribute's,
// which a decision changes, and its text is the memo's.
func ListLearnings(ctx context.Context, temporalClient client.Client) ([]LearningEntry, error) {
	data := converter.GetDefaultDataConverter()
	var entries []LearningEntry
	var token []byte
	for len(entries) < LearningListLimit {
		response, err := temporalClient.ListWorkflow(ctx, &workflowservice.ListWorkflowExecutionsRequest{PageSize: 100, NextPageToken: token, Query: learningListQuery})
		if err != nil {
			return nil, err
		}
		for _, execution := range response.GetExecutions() {
			var entry LearningEntry
			if err := data.FromPayload(execution.GetMemo().GetFields()[MemoLearningEntry], &entry); err != nil || entry.ID == "" {
				continue
			}
			if payload, ok := execution.GetSearchAttributes().GetIndexedFields()[AttributeLearningState.GetName()]; ok {
				var state string
				if data.FromPayload(payload, &state) == nil && state != "" {
					entry.State = LearningState(state)
				}
			}
			entries = append(entries, entry)
		}
		token = response.GetNextPageToken()
		if len(token) == 0 {
			break
		}
	}
	if len(entries) > LearningListLimit {
		entries = entries[:LearningListLimit]
	}
	sort.SliceStable(entries, func(i, j int) bool { return entries[i].Recorded.After(entries[j].Recorded) })
	return entries, nil
}

// DecideLearning accepts, rejects or retires a learning through
// UpdateLearningDecide, which the workflow refuses when the state does not
// allow it. It returns the learning as it is after.
func DecideLearning(ctx context.Context, temporalClient client.Client, id string, decision LearningDecision) (LearningEntry, error) {
	ctx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()
	handle, err := temporalClient.UpdateWorkflow(ctx, client.UpdateWorkflowOptions{
		WorkflowID:   LearningWorkflowID(id),
		UpdateName:   UpdateLearningDecide,
		Args:         []any{decision},
		WaitForStage: client.WorkflowUpdateStageCompleted,
	})
	if err != nil {
		return LearningEntry{}, err
	}
	var entry LearningEntry
	if err := handle.Get(ctx, &entry); err != nil {
		return LearningEntry{}, err
	}
	return entry, nil
}
