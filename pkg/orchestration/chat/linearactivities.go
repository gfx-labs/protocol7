package chat

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"regexp"
	"slices"
	"strings"

	"github.com/gfx-labs/protocol7/pkg/transport/linear"
)

// Names of the activities behind the Linear tools. They are methods of
// LinearActivities, which the selfdev worker registers when it is given a
// Linear placeholder.
const (
	ActivityLinearIssueView   = "ChatLinearIssueView"
	ActivityLinearIssueSearch = "ChatLinearIssueSearch"
	ActivityLinearComment     = "ChatLinearComment"
	ActivityLinearIssueCreate = "ChatLinearIssueCreate"
)

// The bounds of what the Linear tools return.
const (
	// DefaultLinearResults is how many issues linear_issue_search returns
	// when the call gives no number. MaxLinearResults is the most.
	DefaultLinearResults = 10
	MaxLinearResults     = 25
	// DefaultLinearComments is how many of the last comments
	// linear_issue_view shows when the call gives no number.
	DefaultLinearComments = 10
	// DescriptionBytes bounds an issue's description.
	DescriptionBytes = 4000
)

// LinearConfig configures the Linear activities.
type LinearConfig struct {
	// Placeholder is what the client sends as its Authorization header. PDP
	// swaps it for the Linear app's token. Empty offers no Linear tool.
	Placeholder string
	// Teams are the keys of the teams the tools may touch. None allows every
	// team the app can see.
	Teams []string
	// URL is Linear's GraphQL endpoint. Empty is linear.DefaultURL.
	URL string
	// HTTP sends the requests, through PDP. Nil is http.DefaultClient.
	HTTP *http.Client
}

// LinearRequest is one call of a Linear tool.
type LinearRequest struct {
	Chat string `json:"chat"`
	// Identifier names an issue, such as ABC-123.
	Identifier  string   `json:"identifier,omitempty"`
	Query       string   `json:"query,omitempty"`
	Team        string   `json:"team,omitempty"`
	State       string   `json:"state,omitempty"`
	Limit       int      `json:"limit,omitempty"`
	Comments    int      `json:"comments,omitempty"`
	Body        string   `json:"body,omitempty"`
	Title       string   `json:"title,omitempty"`
	Description string   `json:"description,omitempty"`
	Labels      []string `json:"labels,omitempty"`
}

// LinearResult is the text given to the agent.
type LinearResult struct {
	Text string `json:"text"`
}

// LinearActivities are the Linear calls of the chat's Linear tools.
type LinearActivities struct {
	client *linear.Client
	teams  []string
}

// NewLinearActivities makes the activities.
func NewLinearActivities(config LinearConfig) *LinearActivities {
	teams := make([]string, 0, len(config.Teams))
	for _, team := range config.Teams {
		teams = append(teams, strings.ToUpper(strings.TrimSpace(team)))
	}
	return &LinearActivities{
		client: &linear.Client{URL: config.URL, Authorization: config.Placeholder, HTTP: config.HTTP},
		teams:  teams,
	}
}

var linearIdentifier = regexp.MustCompile(`^([A-Za-z][A-Za-z0-9_]*)-[0-9]+$`)

// teamOf is the team key of an issue identifier.
func teamOf(identifier string) (string, error) {
	match := linearIdentifier.FindStringSubmatch(strings.TrimSpace(identifier))
	if match == nil {
		return "", refuse("%q is not a Linear issue identifier such as ABC-123", identifier)
	}
	return strings.ToUpper(match[1]), nil
}

// allowTeam refuses a team the worker was not configured for.
func (a *LinearActivities) allowTeam(team string) error {
	if len(a.teams) == 0 || slices.Contains(a.teams, strings.ToUpper(team)) {
		return nil
	}
	return refuse("the Linear team %s is not one this worker may use: %s", team, strings.Join(a.teams, ", "))
}

// settleLinear makes an answer that trying again cannot change final.
func settleLinear(err error) error {
	if status, ok := errors.AsType[*linear.StatusError](err); ok && !status.Retryable() {
		return refuse("%s", status.Error())
	}
	if refused, ok := errors.AsType[*linear.GraphQLError](err); ok {
		return refuse("%s", refused.Error())
	}
	return err
}

type linearIssue struct {
	ID            string `json:"id"`
	Identifier    string `json:"identifier"`
	Title         string `json:"title"`
	URL           string `json:"url"`
	Description   string `json:"description"`
	PriorityLabel string `json:"priorityLabel"`
	UpdatedAt     string `json:"updatedAt"`
	State         struct {
		Name string `json:"name"`
	} `json:"state"`
	Assignee *struct {
		Name string `json:"name"`
	} `json:"assignee"`
	Team struct {
		Key string `json:"key"`
	} `json:"team"`
	Labels struct {
		Nodes []struct {
			Name string `json:"name"`
		} `json:"nodes"`
	} `json:"labels"`
	Comments struct {
		Nodes []struct {
			Body      string `json:"body"`
			CreatedAt string `json:"createdAt"`
			User      *struct {
				Name string `json:"name"`
			} `json:"user"`
		} `json:"nodes"`
	} `json:"comments"`
}

func (issue linearIssue) assignee() string {
	if issue.Assignee == nil {
		return "nobody"
	}
	return issue.Assignee.Name
}

func (issue linearIssue) labels() string {
	names := make([]string, 0, len(issue.Labels.Nodes))
	for _, label := range issue.Labels.Nodes {
		names = append(names, label.Name)
	}
	if len(names) == 0 {
		return "none"
	}
	return strings.Join(names, ", ")
}

const linearIssueQuery = `query Issue($id: String!, $comments: Int!) {
  issue(id: $id) {
    id identifier title url description priorityLabel updatedAt
    state { name }
    assignee { name }
    team { key }
    labels { nodes { name } }
    comments(last: $comments) { nodes { body createdAt user { name } } }
  }
}`

// issue reads an issue of an allowed team with its last n comments.
func (a *LinearActivities) issue(ctx context.Context, identifier string, comments int) (linearIssue, error) {
	team, err := teamOf(identifier)
	if err != nil {
		return linearIssue{}, err
	}
	if err := a.allowTeam(team); err != nil {
		return linearIssue{}, err
	}
	var data struct {
		Issue *linearIssue `json:"issue"`
	}
	variables := map[string]any{"id": strings.ToUpper(strings.TrimSpace(identifier)), "comments": max(comments, 0)}
	if err := a.client.Do(ctx, linearIssueQuery, variables, &data); err != nil {
		return linearIssue{}, settleLinear(err)
	}
	if data.Issue == nil {
		return linearIssue{}, refuse("there is no Linear issue %s", identifier)
	}
	// The identifier's prefix could be an old team key of a moved issue.
	if err := a.allowTeam(data.Issue.Team.Key); err != nil {
		return linearIssue{}, err
	}
	return *data.Issue, nil
}

// ChatLinearIssueView describes an issue: its title, state, assignee, labels,
// description and last comments, all bounded.
func (a *LinearActivities) ChatLinearIssueView(ctx context.Context, r LinearRequest) (LinearResult, error) {
	n := r.Comments
	if n <= 0 {
		n = DefaultLinearComments
	}
	issue, err := a.issue(ctx, r.Identifier, min(n, MaxViewComments))
	if err != nil {
		return LinearResult{}, err
	}
	var out strings.Builder
	fmt.Fprintf(&out, "%s %s\nTitle: %s\nState: %s\nAssignee: %s\nLabels: %s\n", issue.Identifier, issue.URL, issue.Title, issue.State.Name, issue.assignee(), issue.labels())
	if issue.PriorityLabel != "" {
		fmt.Fprintf(&out, "Priority: %s\n", issue.PriorityLabel)
	}
	if description := strings.TrimSpace(issue.Description); description != "" {
		out.WriteString("\nDescription:\n" + clip(description, DescriptionBytes) + "\n")
	}
	fmt.Fprintf(&out, "\nComments (last %d):\n", len(issue.Comments.Nodes))
	for _, comment := range issue.Comments.Nodes {
		author := "unknown"
		if comment.User != nil {
			author = comment.User.Name
		}
		fmt.Fprintf(&out, "- %s, %s:\n%s\n", author, comment.CreatedAt, indent(clip(comment.Body, CommentBytes)))
	}
	return LinearResult{Text: strings.TrimRight(out.String(), "\n")}, nil
}

const linearSearchQuery = `query Search($filter: IssueFilter, $first: Int!) {
  issues(filter: $filter, first: $first, orderBy: updatedAt) {
    nodes { identifier title url updatedAt state { name } assignee { name } team { key } }
  }
}`

// ChatLinearIssueSearch lists the issues whose title or description holds
// Query, newest change first, in Team and State when they are given. With
// teams configured and no Team, it searches those teams only.
func (a *LinearActivities) ChatLinearIssueSearch(ctx context.Context, r LinearRequest) (LinearResult, error) {
	query := strings.TrimSpace(r.Query)
	team := strings.ToUpper(strings.TrimSpace(r.Team))
	state := strings.TrimSpace(r.State)
	if query == "" && team == "" && state == "" {
		return LinearResult{}, refuse("give a query, a team or a state")
	}
	var filters []map[string]any
	if query != "" {
		filters = append(filters, map[string]any{"or": []map[string]any{
			{"title": map[string]any{"containsIgnoreCase": query}},
			{"description": map[string]any{"containsIgnoreCase": query}},
		}})
	}
	switch {
	case team != "":
		if err := a.allowTeam(team); err != nil {
			return LinearResult{}, err
		}
		filters = append(filters, map[string]any{"team": map[string]any{"key": map[string]any{"eq": team}}})
	case len(a.teams) > 0:
		filters = append(filters, map[string]any{"team": map[string]any{"key": map[string]any{"in": a.teams}}})
	}
	if state != "" {
		filters = append(filters, map[string]any{"state": map[string]any{"name": map[string]any{"eqIgnoreCase": state}}})
	}
	limit := r.Limit
	if limit <= 0 {
		limit = DefaultLinearResults
	}
	limit = min(limit, MaxLinearResults)
	var data struct {
		Issues struct {
			Nodes []linearIssue `json:"nodes"`
		} `json:"issues"`
	}
	if err := a.client.Do(ctx, linearSearchQuery, map[string]any{"filter": map[string]any{"and": filters}, "first": limit}, &data); err != nil {
		return LinearResult{}, settleLinear(err)
	}
	if len(data.Issues.Nodes) == 0 {
		return LinearResult{Text: "No Linear issue matches."}, nil
	}
	var out strings.Builder
	fmt.Fprintf(&out, "%d issues, most recently updated first:\n", len(data.Issues.Nodes))
	for _, issue := range data.Issues.Nodes {
		fmt.Fprintf(&out, "- %s [%s] %s (assignee: %s, updated %s) %s\n", issue.Identifier, issue.State.Name, clip(issue.Title, 200), issue.assignee(), issue.UpdatedAt, issue.URL)
	}
	return LinearResult{Text: strings.TrimRight(out.String(), "\n")}, nil
}

const linearCommentMutation = `mutation Comment($issue: String!, $body: String!) {
  commentCreate(input: { issueId: $issue, body: $body }) { success comment { url } }
}`

// ChatLinearComment comments on an issue of an allowed team.
func (a *LinearActivities) ChatLinearComment(ctx context.Context, r LinearRequest) (LinearResult, error) {
	if strings.TrimSpace(r.Body) == "" {
		return LinearResult{}, refuse("a comment needs a body")
	}
	issue, err := a.issue(ctx, r.Identifier, 0)
	if err != nil {
		return LinearResult{}, err
	}
	var data struct {
		CommentCreate struct {
			Success bool `json:"success"`
			Comment struct {
				URL string `json:"url"`
			} `json:"comment"`
		} `json:"commentCreate"`
	}
	if err := a.client.Do(ctx, linearCommentMutation, map[string]any{"issue": issue.ID, "body": r.Body}, &data); err != nil {
		return LinearResult{}, settleLinear(err)
	}
	if !data.CommentCreate.Success {
		return LinearResult{}, refuse("Linear did not create the comment on %s", issue.Identifier)
	}
	return LinearResult{Text: fmt.Sprintf("Commented on %s: %s", issue.Identifier, data.CommentCreate.Comment.URL)}, nil
}

const linearTeamQuery = `query Team($key: String!, $labels: [String!]) {
  teams(filter: { key: { eq: $key } }) { nodes { id key } }
  issueLabels(filter: { name: { in: $labels } }, first: 100) { nodes { id name team { key } } }
}`

const linearCreateMutation = `mutation Create($input: IssueCreateInput!) {
  issueCreate(input: $input) { success issue { identifier url } }
}`

// ChatLinearIssueCreate opens an issue in an allowed team. Each label must
// exist in the team or the workspace.
func (a *LinearActivities) ChatLinearIssueCreate(ctx context.Context, r LinearRequest) (LinearResult, error) {
	team := strings.ToUpper(strings.TrimSpace(r.Team))
	if team == "" {
		return LinearResult{}, refuse("an issue needs a team key")
	}
	if err := a.allowTeam(team); err != nil {
		return LinearResult{}, err
	}
	title := strings.TrimSpace(r.Title)
	if title == "" {
		return LinearResult{}, refuse("an issue needs a title")
	}
	var found struct {
		Teams struct {
			Nodes []struct {
				ID  string `json:"id"`
				Key string `json:"key"`
			} `json:"nodes"`
		} `json:"teams"`
		IssueLabels struct {
			Nodes []struct {
				ID   string `json:"id"`
				Name string `json:"name"`
				Team *struct {
					Key string `json:"key"`
				} `json:"team"`
			} `json:"nodes"`
		} `json:"issueLabels"`
	}
	labels := r.Labels
	if labels == nil {
		labels = []string{}
	}
	if err := a.client.Do(ctx, linearTeamQuery, map[string]any{"key": team, "labels": labels}, &found); err != nil {
		return LinearResult{}, settleLinear(err)
	}
	if len(found.Teams.Nodes) == 0 {
		return LinearResult{}, refuse("there is no Linear team %s", team)
	}
	var labelIDs []string
	for _, name := range r.Labels {
		id := ""
		for _, label := range found.IssueLabels.Nodes {
			if strings.EqualFold(label.Name, name) && (label.Team == nil || strings.EqualFold(label.Team.Key, team)) {
				id = label.ID
				break
			}
		}
		if id == "" {
			return LinearResult{}, refuse("there is no label %q in the Linear team %s or the workspace", name, team)
		}
		labelIDs = append(labelIDs, id)
	}
	input := map[string]any{"teamId": found.Teams.Nodes[0].ID, "title": title}
	if description := strings.TrimSpace(r.Description); description != "" {
		input["description"] = description
	}
	if len(labelIDs) > 0 {
		input["labelIds"] = labelIDs
	}
	var created struct {
		IssueCreate struct {
			Success bool `json:"success"`
			Issue   struct {
				Identifier string `json:"identifier"`
				URL        string `json:"url"`
			} `json:"issue"`
		} `json:"issueCreate"`
	}
	if err := a.client.Do(ctx, linearCreateMutation, map[string]any{"input": input}, &created); err != nil {
		return LinearResult{}, settleLinear(err)
	}
	if !created.IssueCreate.Success {
		return LinearResult{}, refuse("Linear did not create the issue in %s", team)
	}
	return LinearResult{Text: fmt.Sprintf("%s %s", created.IssueCreate.Issue.Identifier, created.IssueCreate.Issue.URL)}, nil
}
