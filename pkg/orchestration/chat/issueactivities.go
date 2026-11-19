package chat

import (
	"context"
	"fmt"
	"net/http"
	"net/url"
	"strings"

	"go.temporal.io/sdk/temporal"

	"github.com/gfx-labs/protocol7/pkg/transport/githubapi"
)

// Names of the activities behind the issue tools. They are methods of
// IssueActivities, which the selfdev worker registers: they run in the pod
// that holds the GitHub token with issues write.
const (
	ActivityIssueCreate  = "ChatIssueCreate"
	ActivityIssueComment = "ChatIssueComment"
	ActivityIssueLabels  = "ChatIssueLabels"
	ActivityIssueClose   = "ChatIssueClose"
	ActivityIssueEdit    = "ChatIssueEdit"
)

// HandoffLabel is the label that hands an issue to selfdev. Removing it
// stops the work on an issue, so an agent may remove it only from an issue it
// owns.
const HandoffLabel = "selfdev"

// ErrorNotOwner is the type of the error of an action that the agent may
// take only on an issue it owns.
const ErrorNotOwner = "NotOwner"

// OwnerMarker starts the hidden marker that issue_create puts in the body of
// each issue it opens. The marker with a chat's ID is how a later call knows
// the chat opened the issue.
const OwnerMarker = "<!-- selfdev-chat "

// IssueRequest is the input of the issue activities. Which fields an
// activity reads is in its method's comment.
type IssueRequest struct {
	// Chat is the ID of the chat whose agent calls.
	Chat string `json:"chat"`
	// Own is the issue the chat works on, if it works on one.
	Own *Issue `json:"own,omitempty"`
	// Repos are the repositories, as owner/name, that the chat's tools may
	// touch.
	Repos  []string `json:"repos"`
	Repo   string   `json:"repo"`
	Number int      `json:"number,omitempty"`
	Title  string   `json:"title,omitempty"`
	Body   string   `json:"body,omitempty"`
	Labels []string `json:"labels,omitempty"`
	Add    []string `json:"add,omitempty"`
	Remove []string `json:"remove,omitempty"`
	// Base is the branch a pull request is for. Empty is the default branch.
	Base string `json:"base,omitempty"`
	// Draft opens a pull request as a draft.
	Draft bool `json:"draft,omitempty"`
	// Comments is how many of the last comments of a pull request to show.
	Comments int `json:"comments,omitempty"`
	// Reason is "completed" or "not_planned".
	Reason string `json:"reason,omitempty"`
	// ReviewHint is the chat's Request.ReviewHint, for pr_create's answer.
	ReviewHint string `json:"review_hint,omitempty"`
}

// IssueResult names the issue an action touched.
type IssueResult struct {
	Repo   string `json:"repo"`
	Number int    `json:"number"`
	URL    string `json:"url,omitempty"`
}

// IssueActivities are the GitHub calls of the chat's issue tools.
type IssueActivities struct {
	github *githubapi.Client
}

// NewIssueActivities makes the activities. tokenFile and apiURL are as in
// githubapi.New.
func NewIssueActivities(tokenFile, apiURL string) *IssueActivities {
	return &IssueActivities{github: githubapi.New(tokenFile, apiURL)}
}

func refuse(format string, arguments ...any) error {
	return temporal.NewNonRetryableApplicationError(fmt.Sprintf(format, arguments...), githubapi.ErrorRefused, nil)
}

func (r IssueRequest) check() error {
	if !allowedRepo(r.Repos, r.Repo) {
		return refuse("%q is not a repository this chat may use issues of: %s", r.Repo, strings.Join(r.Repos, ", "))
	}
	return nil
}

func allowedRepo(repos []string, repo string) bool {
	for _, allowed := range repos {
		if strings.EqualFold(allowed, repo) {
			return true
		}
	}
	return false
}

// owns says whether the chat opened the issue or works on it. A chat does not
// own an issue that someone else opened, even in a repository it may use.
func (a *IssueActivities) owns(ctx context.Context, r IssueRequest) (bool, error) {
	if r.Own != nil && strings.EqualFold(r.Own.Repo, r.Repo) && r.Own.Number == r.Number {
		return true, nil
	}
	var issue struct {
		Body string `json:"body"`
	}
	if err := a.github.Do(ctx, http.MethodGet, fmt.Sprintf("/repos/%s/issues/%d", r.Repo, r.Number), nil, &issue); err != nil {
		return false, githubapi.Settle(err)
	}
	return strings.Contains(issue.Body, OwnerMarker+r.Chat+" -->"), nil
}

// ChatIssueCreate opens an issue with Repo, Title, Body and Labels. The body
// ends with the owner marker.
func (a *IssueActivities) ChatIssueCreate(ctx context.Context, r IssueRequest) (IssueResult, error) {
	if err := r.check(); err != nil {
		return IssueResult{}, err
	}
	if strings.TrimSpace(r.Title) == "" {
		return IssueResult{}, refuse("an issue needs a title")
	}
	body := map[string]any{"title": r.Title, "body": strings.TrimSpace(r.Body) + "\n\n" + OwnerMarker + r.Chat + " -->"}
	if len(r.Labels) > 0 {
		body["labels"] = r.Labels
	}
	var created struct {
		Number  int    `json:"number"`
		HTMLURL string `json:"html_url"`
	}
	if err := a.github.Do(ctx, http.MethodPost, fmt.Sprintf("/repos/%s/issues", r.Repo), body, &created); err != nil {
		return IssueResult{}, githubapi.Settle(err)
	}
	return IssueResult{Repo: r.Repo, Number: created.Number, URL: created.HTMLURL}, nil
}

// ChatIssueComment comments on the issue Repo#Number, which may be anyone's.
func (a *IssueActivities) ChatIssueComment(ctx context.Context, r IssueRequest) (IssueResult, error) {
	if err := r.check(); err != nil {
		return IssueResult{}, err
	}
	if strings.TrimSpace(r.Body) == "" {
		return IssueResult{}, refuse("a comment needs a body")
	}
	var created struct {
		HTMLURL string `json:"html_url"`
	}
	path := fmt.Sprintf("/repos/%s/issues/%d/comments", r.Repo, r.Number)
	if err := a.github.Do(ctx, http.MethodPost, path, map[string]string{"body": r.Body}, &created); err != nil {
		return IssueResult{}, githubapi.Settle(err)
	}
	return IssueResult{Repo: r.Repo, Number: r.Number, URL: created.HTMLURL}, nil
}

// ChatIssueLabels adds Add to the labels of the issue Repo#Number and then
// removes Remove. The handoff label comes off only an issue the chat owns.
func (a *IssueActivities) ChatIssueLabels(ctx context.Context, r IssueRequest) (IssueResult, error) {
	if err := r.check(); err != nil {
		return IssueResult{}, err
	}
	for _, name := range r.Remove {
		if !strings.EqualFold(name, HandoffLabel) {
			continue
		}
		owned, err := a.owns(ctx, r)
		if err != nil {
			return IssueResult{}, err
		}
		if !owned {
			return IssueResult{}, temporal.NewNonRetryableApplicationError(
				fmt.Sprintf("the label %q stays on %s#%d: this chat did not open the issue and does not work on it, so it may not take the issue away from selfdev", HandoffLabel, r.Repo, r.Number),
				ErrorNotOwner, nil)
		}
	}
	base := fmt.Sprintf("/repos/%s/issues/%d/labels", r.Repo, r.Number)
	if len(r.Add) > 0 {
		if err := a.github.Do(ctx, http.MethodPost, base, map[string][]string{"labels": r.Add}, nil); err != nil {
			return IssueResult{}, githubapi.Settle(err)
		}
	}
	for _, name := range r.Remove {
		err := a.github.Do(ctx, http.MethodDelete, base+"/"+url.PathEscape(name), nil, nil)
		if err != nil && !githubapi.IsStatus(err, http.StatusNotFound) {
			return IssueResult{}, githubapi.Settle(err)
		}
	}
	return IssueResult{Repo: r.Repo, Number: r.Number}, nil
}

// ChatIssueClose closes the issue Repo#Number as Reason, if the chat owns it.
func (a *IssueActivities) ChatIssueClose(ctx context.Context, r IssueRequest) (IssueResult, error) {
	if err := r.check(); err != nil {
		return IssueResult{}, err
	}
	owned, err := a.owns(ctx, r)
	if err != nil {
		return IssueResult{}, err
	}
	if !owned {
		return IssueResult{}, temporal.NewNonRetryableApplicationError(
			fmt.Sprintf("%s#%d stays open: this chat did not open the issue and does not work on it. Comment on it instead", r.Repo, r.Number),
			ErrorNotOwner, nil)
	}
	reason := r.Reason
	if reason == "" {
		reason = "completed"
	}
	path := fmt.Sprintf("/repos/%s/issues/%d", r.Repo, r.Number)
	if err := a.github.Do(ctx, http.MethodPatch, path, map[string]string{"state": "closed", "state_reason": reason}, nil); err != nil {
		return IssueResult{}, githubapi.Settle(err)
	}
	return IssueResult{Repo: r.Repo, Number: r.Number}, nil
}

// ChatIssueEdit sets the title and the body of the issue Repo#Number to Title
// and Body, whichever is not empty, if the chat owns the issue. A new body
// that lacks the owner marker gets it appended, so the chat keeps the right
// to edit and close an issue it opened.
func (a *IssueActivities) ChatIssueEdit(ctx context.Context, r IssueRequest) (IssueResult, error) {
	if err := r.check(); err != nil {
		return IssueResult{}, err
	}
	edit := map[string]string{}
	if title := strings.TrimSpace(r.Title); title != "" {
		edit["title"] = title
	}
	if body := strings.TrimSpace(r.Body); body != "" {
		if !strings.Contains(body, OwnerMarker+r.Chat+" -->") {
			body += "\n\n" + OwnerMarker + r.Chat + " -->"
		}
		edit["body"] = body
	}
	if len(edit) == 0 {
		return IssueResult{}, refuse("an edit needs a title or a body")
	}
	owned, err := a.owns(ctx, r)
	if err != nil {
		return IssueResult{}, err
	}
	if !owned {
		return IssueResult{}, temporal.NewNonRetryableApplicationError(
			fmt.Sprintf("%s#%d stays as it is: this chat did not open the issue and does not work on it. Comment on it instead", r.Repo, r.Number),
			ErrorNotOwner, nil)
	}
	var edited struct {
		HTMLURL string `json:"html_url"`
	}
	path := fmt.Sprintf("/repos/%s/issues/%d", r.Repo, r.Number)
	if err := a.github.Do(ctx, http.MethodPatch, path, edit, &edited); err != nil {
		return IssueResult{}, githubapi.Settle(err)
	}
	return IssueResult{Repo: r.Repo, Number: r.Number, URL: edited.HTMLURL}, nil
}
