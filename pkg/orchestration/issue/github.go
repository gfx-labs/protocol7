package issue

import (
	"context"
	"fmt"
	"net/http"
	"net/url"
	"slices"
	"sort"
	"strings"

	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/client"

	"github.com/gfx-labs/protocol7/pkg/orchestration/chat"
	"github.com/gfx-labs/protocol7/pkg/orchestration/review"
	"github.com/gfx-labs/protocol7/pkg/transport/githubapi"
)

// Names of the activities. They are methods of Activities.
const (
	ActivityIntake     = "IssueIntake"
	ActivityScan       = "IssueScan"
	ActivityPoll       = "IssuePoll"
	ActivityComment    = "IssueComment"
	ActivityFinish     = "IssueFinish"
	ActivityFileReview = "IssueFileReview"
)

// trusted are the author associations whose comments and reviews reach the
// agent. Anyone who can read the repository can comment, and what they write
// becomes the agent's instructions.
var trusted = []string{"OWNER", "MEMBER", "COLLABORATOR"}

// writers are the permissions of a collaborator who may steer the agent.
var writers = []string{"admin", "maintain", "write"}

// trustedAuthor says whether a comment or review by login reaches the agent.
// GitHub fills author_association from what the caller may see. The app's
// token sees an organisation member whose membership is private as a
// CONTRIBUTOR, so a person with write access that the association does not
// show is looked up by their permission on the repository. The answers are
// kept in known for the length of one poll.
func (a *Activities) trustedAuthor(ctx context.Context, repo, login, association string, known map[string]bool) (bool, error) {
	if slices.Contains(trusted, association) {
		return true, nil
	}
	if login == "" || strings.HasSuffix(login, "[bot]") {
		return false, nil
	}
	if allowed, ok := known[login]; ok {
		return allowed, nil
	}
	var permission struct {
		Permission string `json:"permission"`
	}
	err := a.github.Do(ctx, http.MethodGet, fmt.Sprintf("/repos/%s/collaborators/%s/permission", repo, url.PathEscape(login)), nil, &permission)
	if githubapi.IsStatus(err, http.StatusNotFound) {
		known[login] = false
		return false, nil
	}
	if err != nil {
		return false, githubapi.Settle(err)
	}
	known[login] = slices.Contains(writers, permission.Permission)
	return known[login], nil
}

// Activities are the GitHub and Temporal calls of the issue workflows. The
// selfdev worker registers them, and they run in its process, which holds the
// token file. The token needs issues write besides what a review needs.
type Activities struct {
	github *githubapi.Client
}

// NewActivities makes the activities. tokenFile and apiURL are as in
// review.GitHubConfig.
func NewActivities(tokenFile, apiURL string) *Activities {
	return &Activities{github: githubapi.New(tokenFile, apiURL)}
}

// IssueIntake labels a newly opened issue by a trusted human. Reading GitHub
// here keeps its token and the permission check out of the webhook process.
func (a *Activities) IssueIntake(ctx context.Context, input IntakeInput) error {
	path := fmt.Sprintf("/repos/%s/issues/%d", input.Issue.Repo, input.Issue.Number)
	var raw struct {
		State       string                  `json:"state"`
		User        rawUser                 `json:"user"`
		Association string                  `json:"author_association"`
		PullRequest *struct{}               `json:"pull_request"`
		Labels      []struct{ Name string } `json:"labels"`
	}
	if err := a.github.Do(ctx, http.MethodGet, path, nil, &raw); err != nil {
		return githubapi.Settle(err)
	}
	if raw.State != "open" || raw.PullRequest != nil || raw.User.Type == "Bot" || raw.User.Login == "" || strings.HasSuffix(raw.User.Login, "[bot]") {
		return nil
	}
	for _, label := range raw.Labels {
		if label.Name == input.Label {
			return nil
		}
	}
	allowed, err := a.trustedAuthor(ctx, input.Issue.Repo, raw.User.Login, raw.Association, map[string]bool{})
	if err != nil || !allowed {
		return err
	}
	// A retry after a successful label write must not undo a person's stop.
	for page := 1; ; page++ {
		var events []struct {
			Event string                `json:"event"`
			Label struct{ Name string } `json:"label"`
		}
		if err := a.github.Do(ctx, http.MethodGet, fmt.Sprintf("%s/events?per_page=100&page=%d", path, page), nil, &events); err != nil {
			return githubapi.Settle(err)
		}
		for _, event := range events {
			if event.Event == "unlabeled" && event.Label.Name == input.Label {
				return nil
			}
		}
		if len(events) < 100 {
			break
		}
	}
	return githubapi.Settle(a.github.Do(ctx, http.MethodPost, path+"/labels", map[string][]string{"labels": {input.Label}}, nil))
}

// scanPageSize is the page size of the issue list, the most GitHub gives.
const scanPageSize = 100

// labelled lists the open issues with the label, oldest first, that are no
// pull request and not blocked. It reads each repository page by page until a
// short one.
func (a *Activities) labelled(ctx context.Context, template Template) ([]Ref, error) {
	var labelled []Ref
	for _, repo := range template.Repos {
		for page := 1; ; page++ {
			var issues []struct {
				Number       int       `json:"number"`
				PullRequest  *struct{} `json:"pull_request"`
				Dependencies struct {
					// BlockedBy counts the open issues this one is blocked by.
					BlockedBy int `json:"blocked_by"`
				} `json:"issue_dependencies_summary"`
			}
			path := fmt.Sprintf("/repos/%s/issues?state=open&per_page=%d&page=%d&sort=created&direction=asc&labels=%s", repo, scanPageSize, page, url.QueryEscape(template.Label))
			if err := a.github.Do(ctx, http.MethodGet, path, nil, &issues); err != nil {
				return nil, githubapi.Settle(err)
			}
			for _, issue := range issues {
				if issue.PullRequest == nil && issue.Dependencies.BlockedBy == 0 {
					labelled = append(labelled, Ref{Repo: repo, Number: issue.Number})
				}
			}
			if len(issues) < scanPageSize {
				break
			}
		}
	}
	return labelled, nil
}

// IssueScan starts the workflow of each open issue that has the label, up to
// the template's limit of running ones, oldest issue first. An issue whose
// workflow runs is left alone, and so is one that GitHub lists as blocked by
// an open issue, until the blocker closes. It returns the issues it started.
func (a *Activities) IssueScan(ctx context.Context, template Template) ([]Ref, error) {
	labelled, err := a.labelled(ctx, template)
	if err != nil {
		return nil, err
	}
	if len(labelled) == 0 {
		return nil, nil
	}
	temporalClient := activity.GetClient(ctx)
	running, err := temporalClient.ListWorkflow(ctx, &workflowservice.ListWorkflowExecutionsRequest{
		PageSize: 1000,
		Query:    fmt.Sprintf("WorkflowType = '%s' AND ExecutionStatus = 'Running'", WorkflowName),
	})
	if err != nil {
		return nil, fmt.Errorf("list the running issues: %w", err)
	}
	busy := map[string]bool{}
	for _, execution := range running.GetExecutions() {
		busy[execution.GetExecution().GetWorkflowId()] = true
	}
	var started []Ref
	for _, ref := range labelled {
		id := WorkflowID(ref.Repo, ref.Number)
		if busy[id] {
			continue
		}
		if len(busy) >= max(template.Limit, 1) {
			break
		}
		_, err := temporalClient.ExecuteWorkflow(ctx, client.StartWorkflowOptions{
			ID: id, TaskQueue: TaskQueue,
			Memo: map[string]any{"repo": ref.Repo, "number": ref.Number},
			// The list of running workflows may lag a start. Then this finds
			// the one that runs.
			WorkflowIDConflictPolicy: enumspb.WORKFLOW_ID_CONFLICT_POLICY_USE_EXISTING,
		}, WorkflowName, State{Request: Request{Ref: ref, Template: template}})
		if err != nil {
			return started, fmt.Errorf("start the workflow of %s: %w", ref, err)
		}
		busy[id] = true
		started = append(started, ref)
		activity.GetLogger(ctx).Info("issue taken", "issue", ref.String(), "workflow", id)
	}
	return started, nil
}

type rawUser struct {
	Login string `json:"login"`
	Type  string `json:"type"`
}

// IssuePoll reads an issue, its new comments, and the pull requests from the
// chat's branch with their reviews and checks.
func (a *Activities) IssuePoll(ctx context.Context, input PollInput) (Poll, error) {
	var raw struct {
		Title  string                  `json:"title"`
		Body   string                  `json:"body"`
		State  string                  `json:"state"`
		User   rawUser                 `json:"user"`
		Labels []struct{ Name string } `json:"labels"`
	}
	if err := a.github.Do(ctx, http.MethodGet, fmt.Sprintf("/repos/%s/issues/%d", input.Issue.Repo, input.Issue.Number), nil, &raw); err != nil {
		return Poll{}, githubapi.Settle(err)
	}
	poll := Poll{Title: raw.Title, Body: raw.Body, Author: raw.User.Login, Open: raw.State == "open", LastComment: input.LastComment}
	people := map[string]bool{}
	for _, label := range raw.Labels {
		poll.Labeled = poll.Labeled || label.Name == input.Label
	}

	for page := 1; ; page++ {
		var comments []struct {
			ID          int64   `json:"id"`
			Body        string  `json:"body"`
			User        rawUser `json:"user"`
			Association string  `json:"author_association"`
		}
		path := fmt.Sprintf("/repos/%s/issues/%d/comments?per_page=100&page=%d", input.Issue.Repo, input.Issue.Number, page)
		if err := a.github.Do(ctx, http.MethodGet, path, nil, &comments); err != nil {
			return Poll{}, githubapi.Settle(err)
		}
		for _, comment := range comments {
			if comment.ID <= input.LastComment {
				continue
			}
			poll.LastComment = max(poll.LastComment, comment.ID)
			if strings.Contains(comment.Body, CommentMarker) {
				continue
			}
			allowed, err := a.trustedAuthor(ctx, input.Issue.Repo, comment.User.Login, comment.Association, people)
			if err != nil {
				return Poll{}, err
			}
			if !allowed {
				continue
			}
			poll.Comments = append(poll.Comments, Comment{ID: comment.ID, Author: comment.User.Login, Body: comment.Body})
		}
		if len(comments) < 100 {
			break
		}
	}

	if input.Branch == "" {
		return poll, nil
	}
	known := map[Ref]Pull{}
	for _, pull := range input.Pulls {
		known[pullRef(pull)] = pull
	}
	for _, repo := range input.PullRepos {
		owner, _, _ := strings.Cut(repo, "/")
		var pulls []struct {
			Number   int     `json:"number"`
			HTMLURL  string  `json:"html_url"`
			State    string  `json:"state"`
			MergedAt *string `json:"merged_at"`
			Head     struct {
				SHA string `json:"sha"`
			} `json:"head"`
			Base struct {
				Ref string `json:"ref"`
			} `json:"base"`
		}
		path := fmt.Sprintf("/repos/%s/pulls?state=all&per_page=100&head=%s", repo, url.QueryEscape(owner+":"+input.Branch))
		if err := a.github.Do(ctx, http.MethodGet, path, nil, &pulls); err != nil {
			return Poll{}, githubapi.Settle(err)
		}
		for _, raw := range pulls {
			status := PullStatus{Pull: known[Ref{Repo: repo, Number: raw.Number}]}
			status.Repo, status.Number, status.URL, status.Head, status.Base = repo, raw.Number, raw.HTMLURL, raw.Head.SHA, raw.Base.Ref
			status.State = raw.State
			if raw.MergedAt != nil {
				status.State = "merged"
			}
			if err := a.pullDetails(ctx, &status, people); err != nil {
				return Poll{}, err
			}
			poll.Pulls = append(poll.Pulls, status)
		}
	}
	sort.Slice(poll.Pulls, func(i, j int) bool {
		return poll.Pulls[i].Repo < poll.Pulls[j].Repo || poll.Pulls[i].Repo == poll.Pulls[j].Repo && poll.Pulls[i].Number < poll.Pulls[j].Number
	})
	return poll, nil
}

// pullDetails reads the new reviews of a pull request and, if it is open,
// whether it merges and its failed checks.
func (a *Activities) pullDetails(ctx context.Context, status *PullStatus, people map[string]bool) error {
	for page := 1; ; page++ {
		var reviews []struct {
			ID          int64   `json:"id"`
			User        rawUser `json:"user"`
			State       string  `json:"state"`
			Body        string  `json:"body"`
			CommitID    string  `json:"commit_id"`
			Association string  `json:"author_association"`
		}
		path := fmt.Sprintf("/repos/%s/pulls/%d/reviews?per_page=100&page=%d", status.Repo, status.Number, page)
		if err := a.github.Do(ctx, http.MethodGet, path, nil, &reviews); err != nil {
			return githubapi.Settle(err)
		}
		for _, raw := range reviews {
			if raw.ID <= status.LastReview {
				continue
			}
			status.LastReview = max(status.LastReview, raw.ID)
			// The review workflow posts as the app, which is no member.
			if !strings.Contains(raw.Body, review.ReviewMarker) {
				allowed, err := a.trustedAuthor(ctx, status.Repo, raw.User.Login, raw.Association, people)
				if err != nil {
					return err
				}
				if !allowed {
					continue
				}
			}
			status.Reviews = append(status.Reviews, Review{ID: raw.ID, Author: raw.User.Login, State: raw.State, Body: raw.Body, Head: raw.CommitID})
		}
		if len(reviews) < 100 {
			break
		}
	}
	if status.State != "open" {
		return nil
	}
	var pull struct {
		Mergeable *bool `json:"mergeable"`
	}
	if err := a.github.Do(ctx, http.MethodGet, fmt.Sprintf("/repos/%s/pulls/%d", status.Repo, status.Number), nil, &pull); err != nil {
		return githubapi.Settle(err)
	}
	status.Mergeable = pull.Mergeable
	var runs struct {
		Runs []struct {
			Name       string `json:"name"`
			Status     string `json:"status"`
			Conclusion string `json:"conclusion"`
			HTMLURL    string `json:"html_url"`
		} `json:"check_runs"`
	}
	if err := a.github.Do(ctx, http.MethodGet, fmt.Sprintf("/repos/%s/commits/%s/check-runs?per_page=100", status.Repo, status.Head), nil, &runs); err != nil {
		return githubapi.Settle(err)
	}
	for _, run := range runs.Runs {
		switch {
		case run.Status != "completed":
			status.Pending++
		case run.Conclusion == "success" || run.Conclusion == "neutral" || run.Conclusion == "skipped":
		default:
			status.Failed = append(status.Failed, Check{Name: run.Name, URL: run.HTMLURL})
		}
	}
	return nil
}

// IssueComment posts a comment on the issue, unless one with the same key is
// there already.
func (a *Activities) IssueComment(ctx context.Context, input CommentInput) error {
	marker := CommentMarker + input.Key + " -->"
	path := fmt.Sprintf("/repos/%s/issues/%d/comments", input.Issue.Repo, input.Issue.Number)
	for page := 1; ; page++ {
		var comments []struct {
			Body string `json:"body"`
		}
		if err := a.github.Do(ctx, http.MethodGet, fmt.Sprintf("%s?per_page=100&page=%d", path, page), nil, &comments); err != nil {
			return githubapi.Settle(err)
		}
		for _, comment := range comments {
			if strings.Contains(comment.Body, marker) {
				return nil
			}
		}
		if len(comments) < 100 {
			break
		}
	}
	body := input.Body
	if len(body) > 60000 {
		body = strings.ToValidUTF8(body[:60000], "") + "\n\n(cut)"
	}
	return githubapi.Settle(a.github.Do(ctx, http.MethodPost, path, map[string]string{"body": body + "\n\n" + marker}, nil))
}

// IssueFinish closes the issue as completed, or removes the label.
func (a *Activities) IssueFinish(ctx context.Context, input FinishInput) error {
	if input.Action == FinishClose {
		path := fmt.Sprintf("/repos/%s/issues/%d", input.Issue.Repo, input.Issue.Number)
		return githubapi.Settle(a.github.Do(ctx, http.MethodPatch, path, map[string]string{"state": "closed", "state_reason": "completed"}, nil))
	}
	path := fmt.Sprintf("/repos/%s/issues/%d/labels/%s", input.Issue.Repo, input.Issue.Number, url.PathEscape(input.Label))
	err := a.github.Do(ctx, http.MethodDelete, path, nil, nil)
	if githubapi.IsStatus(err, http.StatusNotFound) {
		return nil
	}
	return githubapi.Settle(err)
}

// IssueFileReview files the review ticket of a pull request, or wakes the one
// that runs.
func (a *Activities) IssueFileReview(ctx context.Context, request review.Request) error {
	_, err := review.File(ctx, activity.GetClient(ctx), request)
	return err
}

// IssueStartChat starts an issue's chat. A retry finds the chat it started.
func (a *Activities) IssueStartChat(ctx context.Context, start chat.State) error {
	_, err := activity.GetClient(ctx).ExecuteWorkflow(ctx, client.StartWorkflowOptions{
		ID: chat.WorkflowID(start.Request.ID), TaskQueue: chat.TaskQueue,
		Memo:                     chat.Memo(start.Request),
		TypedSearchAttributes:    chat.SearchAttributes(start.Request),
		WorkflowIDConflictPolicy: enumspb.WORKFLOW_ID_CONFLICT_POLICY_USE_EXISTING,
	}, chat.WorkflowName, start)
	if err != nil {
		return fmt.Errorf("start the chat %s: %w", start.Request.ID, err)
	}
	return nil
}
