package chat

import (
	"context"
	"fmt"
	"net/http"
	"net/url"
	"regexp"
	"strings"

	"github.com/gfx-labs/protocol7/pkg/transport/githubapi"
)

// Names of the activities behind the pull request tools. They are methods of
// IssueActivities too, so the worker registers them with the issue ones.
const (
	ActivityPullCreate  = "ChatPullCreate"
	ActivityPullComment = "ChatPullComment"
	ActivityPullView    = "ChatPullView"
)

// The bounds of what pr_view returns, so that a long thread or a huge log
// does not fill the agent's context.
const (
	// DefaultViewComments is how many of the last comments pr_view shows, of
	// each kind, when the call gives no number. MaxViewComments is the most.
	DefaultViewComments = 10
	MaxViewComments     = 30
	// CommentBytes bounds the text of a comment, a review or the pull
	// request's body.
	CommentBytes = 2000
	// LogLines and LogLineBytes bound the tail of a failed check's log.
	LogLines     = 200
	LogLineBytes = 400
	// LogChecks is how many failed checks show a log. The others show where
	// the log is.
	LogChecks = 3
)

// PullResult names the pull request an action touched, with the text to give
// the agent.
type PullResult struct {
	Repo   string `json:"repo"`
	Number int    `json:"number"`
	URL    string `json:"url,omitempty"`
	Text   string `json:"text"`
}

func owner(repo string) string { return strings.SplitN(repo, "/", 2)[0] }

// marker is the hidden text that makes a pull request the chat's.
func marker(chat string) string { return OwnerMarker + chat + " -->" }

// ChatPullCreate opens a pull request in Repo from the chat's own branch to
// Base, or to the default branch. The head cannot be given. The branch must be
// pushed and have commits that Base lacks. If a pull request for the head is
// open, it is returned and no other is made. The body ends with the owner
// marker.
func (a *IssueActivities) ChatPullCreate(ctx context.Context, r IssueRequest) (PullResult, error) {
	if err := r.check(); err != nil {
		return PullResult{}, err
	}
	if strings.TrimSpace(r.Title) == "" {
		return PullResult{}, refuse("a pull request needs a title")
	}
	head := "chat/" + r.Chat

	var open []struct {
		Number  int    `json:"number"`
		HTMLURL string `json:"html_url"`
	}
	query := url.Values{"head": {owner(r.Repo) + ":" + head}, "state": {"open"}}
	if err := a.github.Do(ctx, http.MethodGet, fmt.Sprintf("/repos/%s/pulls?%s", r.Repo, query.Encode()), nil, &open); err != nil {
		return PullResult{}, githubapi.Settle(err)
	}
	if len(open) > 0 {
		return PullResult{Repo: r.Repo, Number: open[0].Number, URL: open[0].HTMLURL,
			Text: fmt.Sprintf("%s#%d is already open for %s, so no other was made: %s", r.Repo, open[0].Number, head, open[0].HTMLURL)}, nil
	}

	base := strings.TrimSpace(r.Base)
	if base == "" {
		var repository struct {
			DefaultBranch string `json:"default_branch"`
		}
		if err := a.github.Do(ctx, http.MethodGet, "/repos/"+r.Repo, nil, &repository); err != nil {
			return PullResult{}, githubapi.Settle(err)
		}
		base = repository.DefaultBranch
	}
	if base == head {
		return PullResult{}, refuse("the base %q is the chat's own branch", base)
	}

	if err := a.github.Do(ctx, http.MethodGet, fmt.Sprintf("/repos/%s/branches/%s", r.Repo, head), nil, nil); err != nil {
		if githubapi.IsStatus(err, http.StatusNotFound) {
			return PullResult{}, refuse("the branch %s is not pushed to %s: push it first with `git push -u origin %s`", head, r.Repo, head)
		}
		return PullResult{}, githubapi.Settle(err)
	}
	var comparison struct {
		AheadBy int `json:"ahead_by"`
	}
	if err := a.github.Do(ctx, http.MethodGet, fmt.Sprintf("/repos/%s/compare/%s...%s", r.Repo, base, head), nil, &comparison); err != nil {
		if githubapi.IsStatus(err, http.StatusNotFound) {
			return PullResult{}, refuse("the base branch %q does not exist in %s", base, r.Repo)
		}
		return PullResult{}, githubapi.Settle(err)
	}
	if comparison.AheadBy == 0 {
		return PullResult{}, refuse("%s has no commits ahead of %s in %s: commit and push your work first", head, base, r.Repo)
	}

	body := map[string]any{
		"title": strings.TrimSpace(r.Title), "head": head, "base": base,
		"body": strings.TrimSpace(r.Body) + "\n\n" + marker(r.Chat),
	}
	if r.Draft {
		body["draft"] = true
	}
	var created struct {
		Number  int    `json:"number"`
		HTMLURL string `json:"html_url"`
	}
	if err := a.github.Do(ctx, http.MethodPost, fmt.Sprintf("/repos/%s/pulls", r.Repo), body, &created); err != nil {
		return PullResult{}, githubapi.Settle(err)
	}
	text := fmt.Sprintf("%s#%d %s", r.Repo, created.Number, created.HTMLURL)
	switch {
	case r.Own != nil:
		text += fmt.Sprintf("\nWait for its checks with `gh pr checks %d --watch` and fix a failure first. A reviewer reviews it by itself once the checks pass: do not file the review.", created.Number)
	case r.ReviewHint != "":
		text += fmt.Sprintf("\nWait for its checks with `gh pr checks %d --watch` and fix a failure first. %s", created.Number, r.ReviewHint)
	default:
		text += fmt.Sprintf("\nWait for its checks with `gh pr checks %d --watch` and fix a failure first. Then file its review with `selfdev review %s`.", created.Number, created.HTMLURL)
	}
	return PullResult{Repo: r.Repo, Number: created.Number, URL: created.HTMLURL, Text: text}, nil
}

// ChatPullComment comments on the pull request Repo#Number, which may be
// anyone's. An issue that is no pull request is refused: issue_comment
// comments on it.
func (a *IssueActivities) ChatPullComment(ctx context.Context, r IssueRequest) (PullResult, error) {
	if err := r.check(); err != nil {
		return PullResult{}, err
	}
	if strings.TrimSpace(r.Body) == "" {
		return PullResult{}, refuse("a comment needs a body")
	}
	if err := a.github.Do(ctx, http.MethodGet, fmt.Sprintf("/repos/%s/pulls/%d", r.Repo, r.Number), nil, nil); err != nil {
		if githubapi.IsStatus(err, http.StatusNotFound) {
			return PullResult{}, refuse("%s#%d is not a pull request: use issue_comment for an issue", r.Repo, r.Number)
		}
		return PullResult{}, githubapi.Settle(err)
	}
	var created struct {
		HTMLURL string `json:"html_url"`
	}
	path := fmt.Sprintf("/repos/%s/issues/%d/comments", r.Repo, r.Number)
	if err := a.github.Do(ctx, http.MethodPost, path, map[string]string{"body": r.Body}, &created); err != nil {
		return PullResult{}, githubapi.Settle(err)
	}
	return PullResult{Repo: r.Repo, Number: r.Number, URL: created.HTMLURL, Text: fmt.Sprintf("%s#%d %s", r.Repo, r.Number, created.HTMLURL)}, nil
}

// clip bounds text to n bytes, on a character boundary, and says how much it
// left out.
func clip(text string, n int) string {
	text = strings.TrimSpace(text)
	if len(text) <= n {
		return text
	}
	cut := n
	for cut > 0 && !isStart(text[cut]) {
		cut--
	}
	return fmt.Sprintf("%s\n[truncated: %d more bytes]", text[:cut], len(text)-cut)
}

func isStart(b byte) bool { return b&0xC0 != 0x80 }

// indent puts text under a bullet.
func indent(text string) string { return "  " + strings.ReplaceAll(text, "\n", "\n  ") }

var logTimestamp = regexp.MustCompile(`^\d{4}-\d\d-\d\dT[\d:.]+Z `)

type viewComment struct {
	User struct {
		Login string `json:"login"`
	} `json:"user"`
	Body      string `json:"body"`
	CreatedAt string `json:"created_at"`
	Path      string `json:"path"`
	Line      int    `json:"line"`
}

// lastComments reads the last n entries of a list that GitHub pages in
// order, given how many entries it has.
func (a *IssueActivities) lastComments(ctx context.Context, path string, total, n int) ([]viewComment, error) {
	if total == 0 {
		return nil, nil
	}
	const per = 100
	page := (total + per - 1) / per
	var found []viewComment
	for ; page >= 1 && len(found) < n; page-- {
		var got []viewComment
		if err := a.github.Do(ctx, http.MethodGet, fmt.Sprintf("%s?per_page=%d&page=%d", path, per, page), nil, &got); err != nil {
			return nil, err
		}
		found = append(got, found...)
	}
	if len(found) > n {
		found = found[len(found)-n:]
	}
	return found, nil
}

// ChatPullView describes the pull request Repo#Number, which may be anyone's:
// its title, state, head and base, the state of its reviews, its checks with
// the tail of the log of a failed one, and the last Comments comments. All of
// it is bounded.
func (a *IssueActivities) ChatPullView(ctx context.Context, r IssueRequest) (PullResult, error) {
	if err := r.check(); err != nil {
		return PullResult{}, err
	}
	n := r.Comments
	if n <= 0 {
		n = DefaultViewComments
	}
	n = min(n, MaxViewComments)

	var pull struct {
		HTMLURL        string `json:"html_url"`
		Title          string `json:"title"`
		Body           string `json:"body"`
		State          string `json:"state"`
		Draft          bool   `json:"draft"`
		Merged         bool   `json:"merged"`
		MergeableState string `json:"mergeable_state"`
		Comments       int    `json:"comments"`
		ReviewComments int    `json:"review_comments"`
		User           struct {
			Login string `json:"login"`
		} `json:"user"`
		Head struct {
			Ref string `json:"ref"`
			SHA string `json:"sha"`
		} `json:"head"`
		Base struct {
			Ref string `json:"ref"`
		} `json:"base"`
	}
	if err := a.github.Do(ctx, http.MethodGet, fmt.Sprintf("/repos/%s/pulls/%d", r.Repo, r.Number), nil, &pull); err != nil {
		if githubapi.IsStatus(err, http.StatusNotFound) {
			return PullResult{}, refuse("%s#%d is not a pull request", r.Repo, r.Number)
		}
		return PullResult{}, githubapi.Settle(err)
	}
	state := pull.State
	switch {
	case pull.Merged:
		state = "merged"
	case pull.Draft:
		state = "open, draft"
	}
	var out strings.Builder
	fmt.Fprintf(&out, "%s#%d %s\nTitle: %s\nState: %s\nHead: %s (%.12s)\nBase: %s\nAuthor: %s\n", r.Repo, r.Number, pull.HTMLURL, pull.Title, state, pull.Head.Ref, pull.Head.SHA, pull.Base.Ref, pull.User.Login)
	if pull.State == "open" && pull.MergeableState != "" {
		fmt.Fprintf(&out, "Merge state: %s\n", pull.MergeableState)
	}
	if pull.Head.Ref == "chat/"+r.Chat || strings.Contains(pull.Body, marker(r.Chat)) {
		out.WriteString("Opened by this chat: yes\n")
	} else {
		out.WriteString("Opened by this chat: no (you may comment on it, and nothing else)\n")
	}
	if body := strings.TrimSpace(strings.ReplaceAll(pull.Body, marker(r.Chat), "")); body != "" {
		out.WriteString("\nBody:\n" + clip(body, CommentBytes) + "\n")
	}

	if err := a.viewReviews(ctx, r, &out, n); err != nil {
		return PullResult{}, err
	}
	if err := a.viewChecks(ctx, r, pull.Head.SHA, &out); err != nil {
		return PullResult{}, err
	}

	comments, err := a.lastComments(ctx, fmt.Sprintf("/repos/%s/issues/%d/comments", r.Repo, r.Number), pull.Comments, n)
	if err != nil {
		return PullResult{}, githubapi.Settle(err)
	}
	fmt.Fprintf(&out, "\nComments (last %d of %d):\n", len(comments), pull.Comments)
	for _, comment := range comments {
		fmt.Fprintf(&out, "- %s, %s:\n%s\n", comment.User.Login, comment.CreatedAt, indent(clip(comment.Body, CommentBytes)))
	}
	inline, err := a.lastComments(ctx, fmt.Sprintf("/repos/%s/pulls/%d/comments", r.Repo, r.Number), pull.ReviewComments, n)
	if err != nil {
		return PullResult{}, githubapi.Settle(err)
	}
	if len(inline) > 0 {
		fmt.Fprintf(&out, "\nComments on the diff (last %d of %d):\n", len(inline), pull.ReviewComments)
		for _, comment := range inline {
			fmt.Fprintf(&out, "- %s on %s:%d, %s:\n%s\n", comment.User.Login, comment.Path, comment.Line, comment.CreatedAt, indent(clip(comment.Body, CommentBytes)))
		}
	}
	return PullResult{Repo: r.Repo, Number: r.Number, URL: pull.HTMLURL, Text: strings.TrimRight(out.String(), "\n")}, nil
}

// viewReviews writes the review state: the last decisive review of each
// person decides, as in navi's GitHub page, and the last n reviews with text.
func (a *IssueActivities) viewReviews(ctx context.Context, r IssueRequest, out *strings.Builder, n int) error {
	var reviews []struct {
		User struct {
			Login string `json:"login"`
		} `json:"user"`
		State       string `json:"state"`
		Body        string `json:"body"`
		SubmittedAt string `json:"submitted_at"`
		CommitID    string `json:"commit_id"`
	}
	if err := a.github.Do(ctx, http.MethodGet, fmt.Sprintf("/repos/%s/pulls/%d/reviews?per_page=100", r.Repo, r.Number), nil, &reviews); err != nil {
		return githubapi.Settle(err)
	}
	decisive := map[string]string{}
	var order []string
	for _, review := range reviews {
		switch review.State {
		case "APPROVED", "CHANGES_REQUESTED":
			if _, seen := decisive[review.User.Login]; !seen {
				order = append(order, review.User.Login)
			}
			decisive[review.User.Login] = strings.ToLower(strings.ReplaceAll(review.State, "_", " "))
		case "DISMISSED":
			delete(decisive, review.User.Login)
		}
	}
	overall := "none"
	if len(reviews) > 0 {
		overall = "commented"
	}
	var by []string
	for _, login := range order {
		state, ok := decisive[login]
		if !ok {
			continue
		}
		by = append(by, login+": "+state)
		if state == "changes requested" {
			overall = state
		} else if overall != "changes requested" {
			overall = state
		}
	}
	fmt.Fprintf(out, "\nReview state: %s", overall)
	if len(by) > 0 {
		fmt.Fprintf(out, " (%s)", strings.Join(by, ", "))
	}
	out.WriteString("\n")
	var texts []int
	for i, review := range reviews {
		if strings.TrimSpace(review.Body) != "" {
			texts = append(texts, i)
		}
	}
	if len(texts) > n {
		texts = texts[len(texts)-n:]
	}
	for _, i := range texts {
		review := reviews[i]
		fmt.Fprintf(out, "- review by %s, %s, on %.12s, %s:\n%s\n", review.User.Login, strings.ToLower(strings.ReplaceAll(review.State, "_", " ")), review.CommitID, review.SubmittedAt, indent(clip(review.Body, CommentBytes)))
	}
	return nil
}

// viewChecks writes the checks of the head, and for a failed one the tail of
// its log, if GitHub Actions ran it, or else where to read it.
func (a *IssueActivities) viewChecks(ctx context.Context, r IssueRequest, head string, out *strings.Builder) error {
	var runs struct {
		TotalCount int `json:"total_count"`
		CheckRuns  []struct {
			ID         int64  `json:"id"`
			Name       string `json:"name"`
			Status     string `json:"status"`
			Conclusion string `json:"conclusion"`
			HTMLURL    string `json:"html_url"`
			DetailsURL string `json:"details_url"`
			App        struct {
				Slug string `json:"slug"`
			} `json:"app"`
			Output struct {
				Title   string `json:"title"`
				Summary string `json:"summary"`
			} `json:"output"`
		} `json:"check_runs"`
	}
	if err := a.github.Do(ctx, http.MethodGet, fmt.Sprintf("/repos/%s/commits/%s/check-runs?per_page=100", r.Repo, head), nil, &runs); err != nil {
		return githubapi.Settle(err)
	}
	if len(runs.CheckRuns) == 0 {
		out.WriteString("\nChecks: none\n")
		return nil
	}
	out.WriteString("\nChecks:\n")
	var failed []int
	for i, run := range runs.CheckRuns {
		result := run.Conclusion
		if run.Status != "completed" {
			result = run.Status
		}
		fmt.Fprintf(out, "- %s: %s\n", run.Name, result)
		switch run.Conclusion {
		case "failure", "timed_out", "cancelled", "action_required", "startup_failure":
			failed = append(failed, i)
		}
	}
	for shown, i := range failed {
		run := runs.CheckRuns[i]
		link := run.HTMLURL
		if link == "" {
			link = run.DetailsURL
		}
		fmt.Fprintf(out, "\nLog of the failed check %s (%s):\n", run.Name, link)
		if run.App.Slug == "github-actions" && shown < LogChecks {
			lines, cut, err := a.github.Tail(ctx, fmt.Sprintf("/repos/%s/actions/jobs/%d/logs", r.Repo, run.ID), LogLines, LogLineBytes)
			if err == nil {
				if cut {
					out.WriteString("[earlier lines left out]\n")
				}
				for _, line := range lines {
					out.WriteString(logTimestamp.ReplaceAllString(line, "") + "\n")
				}
				continue
			}
			fmt.Fprintf(out, "(the log could not be read: %v) Read it with `gh run view --job %d --log-failed`.\n", err, run.ID)
			continue
		}
		if summary := strings.TrimSpace(run.Output.Title + "\n" + run.Output.Summary); summary != "" {
			out.WriteString(clip(summary, CommentBytes) + "\n")
		}
		fmt.Fprintf(out, "(no log here: read it at %s)\n", link)
	}
	return nil
}
