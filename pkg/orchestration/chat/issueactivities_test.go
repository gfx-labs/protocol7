package chat_test

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/gfx-labs/protocol7/pkg/orchestration/chat"
)

// github stands in for the REST API: issue 5 is someone else's, issue 6 is
// the chat's own work, issue 7 carries the chat's marker.
type github struct {
	mu    sync.Mutex
	calls []string
	// patched is the body of the last PATCH.
	patched map[string]string
}

func (g *github) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	g.mu.Lock()
	g.calls = append(g.calls, r.Method+" "+r.URL.Path)
	if r.Method == http.MethodPatch {
		g.patched = map[string]string{}
		_ = json.NewDecoder(r.Body).Decode(&g.patched)
	}
	g.mu.Unlock()
	switch {
	case r.Method == http.MethodGet && r.URL.Path == "/repos/gfx-labs/r/issues/5":
		_, _ = w.Write([]byte(`{"body":"someone else's"}`))
	case r.Method == http.MethodGet && r.URL.Path == "/repos/gfx-labs/r/issues/7":
		_, _ = w.Write([]byte(`{"body":"mine\n\n<!-- selfdev-chat c1 -->"}`))
	case r.Method == http.MethodPost && r.URL.Path == "/repos/gfx-labs/r/issues":
		var body struct{ Body string }
		_ = json.NewDecoder(r.Body).Decode(&body)
		if !strings.Contains(body.Body, "<!-- selfdev-chat c1 -->") {
			http.Error(w, `{"message":"no marker"}`, http.StatusBadRequest)
			return
		}
		w.WriteHeader(http.StatusCreated)
		_, _ = w.Write([]byte(`{"number":9,"html_url":"https://github.com/gfx-labs/r/issues/9"}`))
	default:
		_, _ = w.Write([]byte(`{}`))
	}
}

func (g *github) wrote() []string {
	g.mu.Lock()
	defer g.mu.Unlock()
	var writes []string
	for _, call := range g.calls {
		if !strings.HasPrefix(call, "GET ") {
			writes = append(writes, call)
		}
	}
	return writes
}

func activities(t *testing.T) (*chat.IssueActivities, *github) {
	t.Helper()
	fake := &github{}
	server := httptest.NewServer(fake)
	t.Cleanup(server.Close)
	token := filepath.Join(t.TempDir(), "token")
	if err := os.WriteFile(token, []byte("tok"), 0o600); err != nil {
		t.Fatal(err)
	}
	return chat.NewIssueActivities(token, server.URL), fake
}

func request(number int) chat.IssueRequest {
	return chat.IssueRequest{
		Chat: "c1", Own: &chat.Issue{Repo: "gfx-labs/r", Number: 6},
		Repos: []string{"gfx-labs/r"}, Repo: "gfx-labs/r", Number: number,
	}
}

// The label that hands an issue to selfdev stays on an issue the chat did not
// open and does not work on. Nothing is written, and other labels still move.
func TestTheHandoffLabelStaysOnAnotherPersonsIssue(t *testing.T) {
	a, fake := activities(t)
	r := request(5)
	r.Remove = []string{"selfdev"}
	if _, err := a.ChatIssueLabels(context.Background(), r); err == nil || !strings.Contains(err.Error(), "may not take the issue away") {
		t.Fatalf("removing the label from issue 5: got %v, want a refusal", err)
	}
	if writes := fake.wrote(); len(writes) != 0 {
		t.Fatalf("wrote %v, want nothing", writes)
	}

	r.Remove, r.Add = []string{"bug"}, []string{"triage"}
	if _, err := a.ChatIssueLabels(context.Background(), r); err != nil {
		t.Fatalf("other labels: %v", err)
	}
	if got := strings.Join(fake.wrote(), ","); got != "POST /repos/gfx-labs/r/issues/5/labels,DELETE /repos/gfx-labs/r/issues/5/labels/bug" {
		t.Errorf("wrote %s, want the label added and bug removed", got)
	}
}

// The issue the chat works on, and one it opened, may be unlabelled and
// closed. One it did not open may not be closed.
func TestAChatClosesOnlyItsOwnIssues(t *testing.T) {
	a, fake := activities(t)
	if _, err := a.ChatIssueClose(context.Background(), request(5)); err == nil || !strings.Contains(err.Error(), "stays open") {
		t.Fatalf("closing issue 5: got %v, want a refusal", err)
	}
	for _, number := range []int{6, 7} {
		r := request(number)
		r.Remove = []string{"selfdev"}
		if _, err := a.ChatIssueLabels(context.Background(), r); err != nil {
			t.Errorf("unlabelling %d: %v", number, err)
		}
		if _, err := a.ChatIssueClose(context.Background(), request(number)); err != nil {
			t.Errorf("closing %d: %v", number, err)
		}
	}
	want := "DELETE /repos/gfx-labs/r/issues/6/labels/selfdev,PATCH /repos/gfx-labs/r/issues/6,DELETE /repos/gfx-labs/r/issues/7/labels/selfdev,PATCH /repos/gfx-labs/r/issues/7"
	if got := strings.Join(fake.wrote(), ","); got != want {
		t.Errorf("wrote %s, want %s", got, want)
	}
}

// An issue the chat opens carries its marker, which is what makes it the
// chat's later. A repository the chat does not clone is refused.
func TestCreatedIssuesAreMarkedAndRepositoriesAreLimited(t *testing.T) {
	a, _ := activities(t)
	created, err := a.ChatIssueCreate(context.Background(), chat.IssueRequest{Chat: "c1", Repos: []string{"gfx-labs/r"}, Repo: "gfx-labs/r", Title: "t", Body: "b"})
	if err != nil || created.Number != 9 {
		t.Fatalf("created %+v, %v, want issue 9", created, err)
	}
	if _, err := a.ChatIssueComment(context.Background(), chat.IssueRequest{Chat: "c1", Repos: []string{"gfx-labs/r"}, Repo: "gfx-labs/other", Number: 1, Body: "x"}); err == nil {
		t.Error("commenting in a repository the chat does not clone was allowed")
	}
}

// A chat edits an issue it opened or works on, and the owner marker stays in
// the body. Someone else's issue is refused, and so is an edit that changes
// nothing. Nothing is written in either refusal.
func TestAChatEditsOnlyItsOwnIssues(t *testing.T) {
	ctx := context.Background()
	a, fake := activities(t)

	r := request(5)
	r.Title, r.Body = "t", "b"
	if _, err := a.ChatIssueEdit(ctx, r); err == nil || !strings.Contains(err.Error(), "Comment on it instead") {
		t.Fatalf("editing issue 5: got %v, want a refusal", err)
	}
	for _, empty := range []chat.IssueRequest{request(7), func() chat.IssueRequest { r := request(7); r.Title, r.Body = " ", "\n"; return r }()} {
		if _, err := a.ChatIssueEdit(ctx, empty); err == nil || !strings.Contains(err.Error(), "title or a body") {
			t.Errorf("empty edit: got %v, want a refusal", err)
		}
	}
	if writes := fake.wrote(); len(writes) != 0 {
		t.Fatalf("wrote %v, want nothing", writes)
	}

	// The issue the chat opened: a body without the marker gets it back.
	r = request(7)
	r.Body = "new text"
	if _, err := a.ChatIssueEdit(ctx, r); err != nil {
		t.Fatalf("editing issue 7: %v", err)
	}
	if got, want := fake.patched["body"], "new text\n\n<!-- selfdev-chat c1 -->"; got != want {
		t.Errorf("body %q, want %q", got, want)
	}
	if _, ok := fake.patched["title"]; ok {
		t.Error("the title was sent though none was given")
	}

	// A body that has the marker is not given a second one.
	r.Body = "again\n\n<!-- selfdev-chat c1 -->"
	r.Title = "new title"
	if _, err := a.ChatIssueEdit(ctx, r); err != nil {
		t.Fatalf("editing issue 7: %v", err)
	}
	if got := fake.patched["body"]; strings.Count(got, "<!-- selfdev-chat") != 1 || fake.patched["title"] != "new title" {
		t.Errorf("patched %v, want one marker and the new title", fake.patched)
	}

	// The issue the chat works on, and a repository it does not clone.
	r = request(6)
	r.Title = "t"
	if _, err := a.ChatIssueEdit(ctx, r); err != nil {
		t.Errorf("editing the issue the chat works on: %v", err)
	}
	r.Repo = "gfx-labs/other"
	if _, err := a.ChatIssueEdit(ctx, r); err == nil {
		t.Error("editing in a repository the chat does not clone was allowed")
	}
	want := "PATCH /repos/gfx-labs/r/issues/7,PATCH /repos/gfx-labs/r/issues/7,PATCH /repos/gfx-labs/r/issues/6"
	if got := strings.Join(fake.wrote(), ","); got != want {
		t.Errorf("wrote %s, want %s", got, want)
	}
}
