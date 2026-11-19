package review

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"strings"

	"go.temporal.io/sdk/temporal"

	"github.com/gfx-labs/protocol7/pkg/transport/githubapi"
)

// GitHubConfig configures the activities that talk to GitHub.
type GitHubConfig struct {
	// TokenFile holds an installation token, which another process replaces
	// before it expires (selfdev github-token). It is read on every call.
	TokenFile string
	// Repositories are the owner/name pairs a review may be filed for.
	Repositories []string
	// APIURL is GitHub's API. Empty is https://api.github.com.
	APIURL string
	// MergeMethod is merge, squash or rebase. Empty is merge.
	MergeMethod string
}

// Activities are the GitHub calls of a review. The selfdev worker registers
// them, and they run in its process, which holds the token file.
type Activities struct {
	config GitHubConfig
	github *githubapi.Client
}

// NewActivities makes the activities. It reads nothing until a call.
func NewActivities(config GitHubConfig) *Activities {
	if config.MergeMethod == "" {
		config.MergeMethod = "merge"
	}
	return &Activities{config: config, github: githubapi.New(config.TokenFile, config.APIURL)}
}

func (a *Activities) do(ctx context.Context, method, path string, body, result any) error {
	return a.github.Do(ctx, method, path, body, result)
}

var (
	settle   = githubapi.Settle
	isStatus = githubapi.IsStatus
)

func (a *Activities) allowed(repo string) error {
	for _, candidate := range a.config.Repositories {
		if candidate == repo {
			return nil
		}
	}
	return temporal.NewNonRetryableApplicationError(
		fmt.Sprintf("the repository %s is not one that reviews are filed for (%s)", repo, strings.Join(a.config.Repositories, ", ")), ErrorRefused, nil)
}

// ReviewPull reads a pull request, and the check runs of its head.
func (a *Activities) ReviewPull(ctx context.Context, repo string, number int) (Pull, error) {
	if err := a.allowed(repo); err != nil {
		return Pull{}, err
	}
	var raw struct {
		Title          string                  `json:"title"`
		State          string                  `json:"state"`
		Draft          bool                    `json:"draft"`
		Merged         bool                    `json:"merged"`
		User           struct{ Login string }  `json:"user"`
		Labels         []struct{ Name string } `json:"labels"`
		Mergeable      *bool                   `json:"mergeable"`
		MergeableState string                  `json:"mergeable_state"`
		Base           struct{ Ref string }    `json:"base"`
		Head           struct {
			Ref  string `json:"ref"`
			SHA  string `json:"sha"`
			Repo *struct {
				FullName string `json:"full_name"`
			} `json:"repo"`
		} `json:"head"`
	}
	if err := a.do(ctx, http.MethodGet, fmt.Sprintf("/repos/%s/pulls/%d", repo, number), nil, &raw); err != nil {
		return Pull{}, settle(err)
	}
	pull := Pull{
		Repo: repo, Number: number, Title: raw.Title, Author: raw.User.Login, Base: raw.Base.Ref,
		HeadRef: raw.Head.Ref, Head: raw.Head.SHA, Open: raw.State == "open", Merged: raw.Merged, Draft: raw.Draft,
		Mergeable: raw.Mergeable, MergeState: raw.MergeableState,
	}
	if raw.Head.Repo != nil {
		pull.HeadRepo = raw.Head.Repo.FullName
	}
	for _, label := range raw.Labels {
		pull.Labels = append(pull.Labels, label.Name)
	}
	if !pull.Open {
		return pull, nil
	}

	var runs struct {
		Runs []struct {
			Status     string `json:"status"`
			Conclusion string `json:"conclusion"`
		} `json:"check_runs"`
	}
	if err := a.do(ctx, http.MethodGet, fmt.Sprintf("/repos/%s/commits/%s/check-runs?per_page=100", repo, pull.Head), nil, &runs); err != nil {
		return Pull{}, settle(err)
	}
	for _, run := range runs.Runs {
		pull.Checks.Total++
		switch {
		case run.Status != "completed":
			pull.Checks.Pending++
		case run.Conclusion == "success" || run.Conclusion == "neutral" || run.Conclusion == "skipped":
		default:
			pull.Checks.Failed++
		}
	}
	return pull, nil
}

// ReviewPublish posts a verdict as a review of one head, unless the pull
// request already has it. GitHub refuses an approval or a change request
// from the author of the pull request, and the app that wrote the pull
// request may be the one that reviews it. Then the review is posted as a
// comment that says which verdict it is.
func (a *Activities) ReviewPublish(ctx context.Context, input PublishInput) error {
	if err := a.allowed(input.Repo); err != nil {
		return err
	}
	marker := fmt.Sprintf("%s%s:%s -->", ReviewMarker, input.Head, input.Verdict.Decision)
	var existing []struct {
		Body string `json:"body"`
	}
	if err := a.do(ctx, http.MethodGet, fmt.Sprintf("/repos/%s/pulls/%d/reviews?per_page=100", input.Repo, input.Number), nil, &existing); err != nil {
		return settle(err)
	}
	for _, review := range existing {
		if strings.Contains(review.Body, marker) {
			return nil
		}
	}

	event := map[string]string{Approve: "APPROVE", RequestChanges: "REQUEST_CHANGES", Comment: "COMMENT"}[input.Verdict.Decision]
	body := reviewBody(input.Verdict, input.Head)
	path := fmt.Sprintf("/repos/%s/pulls/%d/reviews", input.Repo, input.Number)
	err := a.do(ctx, http.MethodPost, path, map[string]string{"commit_id": input.Head, "event": event, "body": body}, nil)
	if event != "COMMENT" && isStatus(err, http.StatusUnprocessableEntity) {
		body = "**Verdict: " + input.Verdict.Decision + "**\n\n" + body
		err = a.do(ctx, http.MethodPost, path, map[string]string{"commit_id": input.Head, "event": "COMMENT", "body": body}, nil)
	}
	return settle(err)
}

// ReviewMerge merges a pull request if its head is still the one that was
// reviewed. GitHub checks that.
func (a *Activities) ReviewMerge(ctx context.Context, input MergeInput) error {
	if err := a.allowed(input.Repo); err != nil {
		return err
	}
	err := a.do(ctx, http.MethodPut, fmt.Sprintf("/repos/%s/pulls/%d/merge", input.Repo, input.Number),
		map[string]string{"sha": input.Head, "merge_method": a.config.MergeMethod}, nil)
	var status *githubapi.StatusError
	if errors.As(err, &status) {
		switch status.Code {
		case http.StatusConflict:
			return temporal.NewNonRetryableApplicationError("the head changed before the merge: "+status.Message, ErrorHeadChanged, nil)
		case http.StatusMethodNotAllowed:
			return temporal.NewNonRetryableApplicationError("GitHub will not merge it: "+status.Message, ErrorNotMergeable, nil)
		}
	}
	return settle(err)
}
