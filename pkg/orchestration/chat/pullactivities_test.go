package chat_test

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"unicode/utf8"

	"github.com/gfx-labs/protocol7/pkg/orchestration/chat"
)

// pulls stands in for the REST API of the repository gfx-labs/r: the default
// branch is main, the chat c1's branch chat/c1 is what the fields say, pull
// request 5 is someone else's and pull request 7 is the chat's.
type pulls struct {
	mu sync.Mutex
	// pushed says whether chat/c1 exists, ahead is its commits ahead of main
	// and open the number of the open pull request for it, or 0.
	pushed bool
	ahead  int
	open   int
	// created is the body of the last POST of a pull request, and comment
	// that of the last comment.
	created map[string]any
	comment string
	posts   []string
	// comments are the issue comments of pull request 7, log its failed job's
	// log.
	comments []string
	log      string
	// logStatus is the status of the log, if not 200.
	logStatus int
}

func (p *pulls) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if r.Method == http.MethodPost {
		p.posts = append(p.posts, r.URL.Path)
	}
	path := r.URL.Path
	switch {
	case r.Method == http.MethodGet && path == "/repos/gfx-labs/r/pulls" && r.URL.Query().Get("head") == "gfx-labs:chat/c1":
		if p.open != 0 {
			fmt.Fprintf(w, `[{"number":%d,"html_url":"https://github.com/gfx-labs/r/pull/%[1]d"}]`, p.open)
			return
		}
		_, _ = w.Write([]byte(`[]`))
	case path == "/repos/gfx-labs/r":
		_, _ = w.Write([]byte(`{"default_branch":"main"}`))
	case path == "/repos/gfx-labs/r/branches/chat/c1":
		if !p.pushed {
			http.Error(w, `{"message":"Branch not found"}`, http.StatusNotFound)
			return
		}
		_, _ = w.Write([]byte(`{}`))
	case path == "/repos/gfx-labs/r/compare/main...chat/c1":
		fmt.Fprintf(w, `{"ahead_by":%d}`, p.ahead)
	case r.Method == http.MethodPost && path == "/repos/gfx-labs/r/pulls":
		p.created = map[string]any{}
		_ = json.NewDecoder(r.Body).Decode(&p.created)
		w.WriteHeader(http.StatusCreated)
		_, _ = w.Write([]byte(`{"number":9,"html_url":"https://github.com/gfx-labs/r/pull/9"}`))
	case path == "/repos/gfx-labs/r/pulls/5":
		_, _ = w.Write([]byte(`{"number":5,"body":"someone else's"}`))
	case path == "/repos/gfx-labs/r/pulls/6":
		http.Error(w, `{"message":"Not Found"}`, http.StatusNotFound)
	case r.Method == http.MethodPost && strings.HasPrefix(path, "/repos/gfx-labs/r/issues/") && strings.HasSuffix(path, "/comments"):
		var body struct{ Body string }
		_ = json.NewDecoder(r.Body).Decode(&body)
		p.comment = body.Body
		w.WriteHeader(http.StatusCreated)
		_, _ = w.Write([]byte(`{"html_url":"https://github.com/gfx-labs/r/pull/5#issuecomment-1"}`))
	case path == "/repos/gfx-labs/r/pulls/7":
		fmt.Fprintf(w, `{"html_url":"https://github.com/gfx-labs/r/pull/7","title":"A title","body":"text\n\n<!-- selfdev-chat c1 -->","state":"open","comments":%d,"review_comments":0,
			"user":{"login":"bot"},"head":{"ref":"chat/c1","sha":"abcdef0123456789"},"base":{"ref":"main"}}`, len(p.comments))
	case path == "/repos/gfx-labs/r/pulls/7/reviews":
		_, _ = w.Write([]byte(`[{"user":{"login":"rev"},"state":"CHANGES_REQUESTED","body":"fix it","submitted_at":"t1","commit_id":"abcdef0123456789"}]`))
	case path == "/repos/gfx-labs/r/pulls/7/comments":
		_, _ = w.Write([]byte(`[]`))
	case path == "/repos/gfx-labs/r/issues/7/comments":
		var items []string
		for _, text := range p.comments {
			encoded, _ := json.Marshal(text)
			items = append(items, `{"user":{"login":"u"},"created_at":"t","body":`+string(encoded)+`}`)
		}
		_, _ = w.Write([]byte("[" + strings.Join(items, ",") + "]"))
	case path == "/repos/gfx-labs/r/commits/abcdef0123456789/check-runs":
		_, _ = w.Write([]byte(`{"total_count":2,"check_runs":[
			{"id":11,"name":"test","status":"completed","conclusion":"failure","html_url":"https://github.com/gfx-labs/r/runs/11","app":{"slug":"github-actions"}},
			{"id":12,"name":"lint","status":"completed","conclusion":"success","app":{"slug":"github-actions"}}]}`))
	case path == "/repos/gfx-labs/r/actions/jobs/11/logs":
		if p.logStatus != 0 {
			http.Error(w, `{"message":"Resource not accessible by integration"}`, p.logStatus)
			return
		}
		_, _ = w.Write([]byte(p.log))
	default:
		http.NotFound(w, r)
	}
}

func pullActivities(t *testing.T, fake *pulls) *chat.IssueActivities {
	t.Helper()
	server := httptest.NewServer(fake)
	t.Cleanup(server.Close)
	token := filepath.Join(t.TempDir(), "token")
	if err := os.WriteFile(token, []byte("tok"), 0o600); err != nil {
		t.Fatal(err)
	}
	return chat.NewIssueActivities(token, server.URL)
}

func pullRequest(number int) chat.IssueRequest {
	return chat.IssueRequest{Chat: "c1", Repos: []string{"gfx-labs/r"}, Repo: "gfx-labs/r", Number: number, Title: "t", Body: "b"}
}

// pr_create refuses a branch that is not pushed and one with nothing new, and
// writes nothing.
func TestPullCreateNeedsAPushedBranchWithCommits(t *testing.T) {
	ctx := context.Background()
	fake := &pulls{}
	a := pullActivities(t, fake)

	if _, err := a.ChatPullCreate(ctx, pullRequest(0)); err == nil || !strings.Contains(err.Error(), "is not pushed") {
		t.Fatalf("an unpushed branch: got %v, want a refusal", err)
	}
	fake.pushed = true
	if _, err := a.ChatPullCreate(ctx, pullRequest(0)); err == nil || !strings.Contains(err.Error(), "no commits ahead of main") {
		t.Fatalf("no commits: got %v, want a refusal", err)
	}
	if len(fake.posts) != 0 {
		t.Fatalf("posted %v, want nothing", fake.posts)
	}
	r := pullRequest(0)
	r.Title = " "
	if _, err := a.ChatPullCreate(ctx, r); err == nil {
		t.Error("a pull request with no title was opened")
	}
	r = pullRequest(0)
	r.Repo = "gfx-labs/other"
	if _, err := a.ChatPullCreate(ctx, r); err == nil {
		t.Error("a pull request in a repository the chat does not clone was opened")
	}
}

// An open pull request for the head is returned, not duplicated.
func TestPullCreateReturnsTheOpenPullRequest(t *testing.T) {
	fake := &pulls{pushed: true, ahead: 2, open: 3}
	a := pullActivities(t, fake)
	got, err := a.ChatPullCreate(context.Background(), pullRequest(0))
	if err != nil || got.Number != 3 || !strings.Contains(got.Text, "already open") {
		t.Fatalf("got %+v, %v, want the open pull request 3", got, err)
	}
	if len(fake.posts) != 0 {
		t.Errorf("posted %v, want nothing", fake.posts)
	}
}

// The body gets the owner marker, the head is the chat's branch, the base is
// the default branch, and the result says what to do next.
func TestPullCreateMarksTheBodyAndTakesTheChatsBranch(t *testing.T) {
	fake := &pulls{pushed: true, ahead: 1}
	a := pullActivities(t, fake)
	r := pullRequest(0)
	r.Draft = true
	got, err := a.ChatPullCreate(context.Background(), r)
	if err != nil || got.Number != 9 {
		t.Fatalf("got %+v, %v, want pull request 9", got, err)
	}
	if body := fake.created["body"]; body != "b\n\n<!-- selfdev-chat c1 -->" {
		t.Errorf("body %q, want the marker appended", body)
	}
	if fake.created["head"] != "chat/c1" || fake.created["base"] != "main" || fake.created["draft"] != true {
		t.Errorf("created %v, want head chat/c1, base main, draft", fake.created)
	}
	for _, want := range []string{"gh pr checks 9 --watch", "selfdev review https://github.com/gfx-labs/r/pull/9"} {
		if !strings.Contains(got.Text, want) {
			t.Errorf("result %q lacks %q", got.Text, want)
		}
	}
	// A chat that works on an issue does not file the review itself.
	r.Own = &chat.Issue{Repo: "gfx-labs/r", Number: 1}
	got, err = a.ChatPullCreate(context.Background(), r)
	if err != nil || strings.Contains(got.Text, "selfdev review") || !strings.Contains(got.Text, "gh pr checks 9 --watch") {
		t.Errorf("issue chat result %q, %v: want the checks and no review to file", got.Text, err)
	}
}

// A comment on a pull request somebody else opened is allowed. An issue is
// no pull request.
func TestPullCommentOnAForeignPullRequest(t *testing.T) {
	fake := &pulls{}
	a := pullActivities(t, fake)
	got, err := a.ChatPullComment(context.Background(), pullRequest(5))
	if err != nil || fake.comment != "b" || !strings.Contains(got.URL, "issuecomment-1") {
		t.Fatalf("got %+v, %v, comment %q, want the comment posted", got, err, fake.comment)
	}
	if _, err := a.ChatPullComment(context.Background(), pullRequest(6)); err == nil || !strings.Contains(err.Error(), "not a pull request") {
		t.Errorf("a comment on an issue: got %v, want a refusal", err)
	}
	r := pullRequest(5)
	r.Repo = "gfx-labs/other"
	if _, err := a.ChatPullComment(context.Background(), r); err == nil {
		t.Error("a comment in a repository the chat does not clone was allowed")
	}
}

// pr_view cuts a long comment and a long log, and keeps the end of the log.
func TestPullViewTruncatesCommentsAndLogs(t *testing.T) {
	var log strings.Builder
	for i := 1; i <= 1000; i++ {
		fmt.Fprintf(&log, "2026-01-01T00:00:00.0000000Z line %d %s\n", i, strings.Repeat("x", 1000))
	}
	long := strings.Repeat("é", 5000)
	fake := &pulls{comments: []string{"first", "second", long}, log: log.String()}
	a := pullActivities(t, fake)

	r := pullRequest(7)
	r.Comments = 2
	got, err := a.ChatPullView(context.Background(), r)
	if err != nil {
		t.Fatal(err)
	}
	text := got.Text
	for _, want := range []string{
		"Title: A title", "State: open", "Head: chat/c1", "Base: main", "Opened by this chat: yes",
		"Review state: changes requested (rev: changes requested)", "fix it",
		"- test: failure", "- lint: success",
		"Comments (last 2 of 3)", "second",
		"line 1000 ", "[earlier lines left out]", "[truncated:",
	} {
		if !strings.Contains(text, want) {
			t.Errorf("the view lacks %q:\n%.3000s", want, text)
		}
	}
	for _, unwanted := range []string{"first", "line 800 ", "<!-- selfdev-chat", "2026-01-01T"} {
		if strings.Contains(text, unwanted) {
			t.Errorf("the view has %q", unwanted)
		}
	}
	if strings.Count(text, "é") > chat.CommentBytes/2 {
		t.Errorf("the long comment has %d characters, want at most %d bytes", strings.Count(text, "é"), chat.CommentBytes)
	}
	if lines := strings.Count(text, "\nline "); lines != chat.LogLines {
		t.Errorf("the log shows %d lines, want %d", lines, chat.LogLines)
	}
	if len(text) > 200*(chat.LogLineBytes+10)+3*chat.CommentBytes {
		t.Errorf("the view is %d bytes, want it bounded", len(text))
	}
	if _, err := a.ChatPullView(context.Background(), pullRequest(6)); err == nil {
		t.Error("viewing a number that is no pull request was allowed")
	}
}

// A log that GitHub refuses, such as for a token without actions read, does
// not fail the view: it says where to read the log.
func TestPullViewPointsToALogItCannotRead(t *testing.T) {
	for _, status := range []int{http.StatusForbidden, http.StatusNotFound, http.StatusGone} {
		a := pullActivities(t, &pulls{logStatus: status})
		got, err := a.ChatPullView(context.Background(), pullRequest(7))
		if err != nil {
			t.Fatalf("status %d: %v", status, err)
		}
		for _, want := range []string{"- test: failure", "the log could not be read", fmt.Sprint(status), "gh run view --job 11 --log-failed"} {
			if !strings.Contains(got.Text, want) {
				t.Errorf("status %d: the view lacks %q:\n%s", status, want, got.Text)
			}
		}
	}
}

// A long line of multi-byte characters is cut on a character boundary.
func TestPullViewCutsLogLinesOnCharacters(t *testing.T) {
	a := pullActivities(t, &pulls{log: strings.Repeat("é", 1000) + "\nlast\n"})
	got, err := a.ChatPullView(context.Background(), pullRequest(7))
	if err != nil {
		t.Fatal(err)
	}
	if !utf8.ValidString(got.Text) || !strings.Contains(got.Text, "é...") || !strings.Contains(got.Text, "\nlast\n") {
		t.Errorf("the log is not cut on characters, or lacks its end:\n%.2000s", got.Text)
	}
}
