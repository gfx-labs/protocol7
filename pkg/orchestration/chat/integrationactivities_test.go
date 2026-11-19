package chat_test

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"github.com/gfx-labs/protocol7/pkg/orchestration/chat"
)

// linearAPI stands in for Linear's GraphQL endpoint, with the shapes Linear
// answers: the issue ENG-7 with two comments, and an error for any other.
type linearAPI struct {
	mu        sync.Mutex
	auth      []string
	variables []map[string]any
	queries   []string
}

func (l *linearAPI) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	l.mu.Lock()
	defer l.mu.Unlock()
	var body struct {
		Query     string         `json:"query"`
		Variables map[string]any `json:"variables"`
	}
	_ = json.NewDecoder(r.Body).Decode(&body)
	l.auth = append(l.auth, r.Header.Get("Authorization"))
	l.queries = append(l.queries, body.Query)
	l.variables = append(l.variables, body.Variables)
	switch {
	case strings.HasPrefix(body.Query, "query Issue") && body.Variables["id"] == "ENG-7":
		_, _ = w.Write([]byte(`{"data":{"issue":{"id":"9f1c","identifier":"ENG-7","title":"Retry the export","url":"https://linear.app/x/issue/ENG-7",
			"description":"` + strings.Repeat("d", chat.DescriptionBytes+100) + `","priorityLabel":"High","state":{"name":"In Progress"},"assignee":null,
			"team":{"key":"ENG"},"labels":{"nodes":[{"name":"bug"}]},
			"comments":{"nodes":[{"body":"first","createdAt":"2026-11-01T00:00:00.000Z","user":{"name":"Ann"}},{"body":"second","createdAt":"2026-11-02T00:00:00.000Z","user":null}]}}}}`))
	case strings.HasPrefix(body.Query, "query Issue"):
		_, _ = w.Write([]byte(`{"data":null,"errors":[{"message":"Entity not found: Issue"}]}`))
	case strings.HasPrefix(body.Query, "mutation Comment"):
		_, _ = w.Write([]byte(`{"data":{"commentCreate":{"success":true,"comment":{"url":"https://linear.app/x/issue/ENG-7#comment-1"}}}}`))
	default:
		http.Error(w, `{"errors":[{"message":"unexpected"}]}`, http.StatusBadRequest)
	}
}

// The Linear tools send the placeholder as it is, bound what they show, and
// refuse a team the worker was not given before they call Linear.
func TestLinearToolsSendThePlaceholderAndKeepToTheirTeams(t *testing.T) {
	fake := &linearAPI{}
	server := httptest.NewServer(fake)
	t.Cleanup(server.Close)
	a := chat.NewLinearActivities(chat.LinearConfig{Placeholder: "pdp-proxied-linear-protocol7", Teams: []string{"eng"}, URL: server.URL, HTTP: server.Client()})
	ctx := context.Background()

	got, err := a.ChatLinearIssueView(ctx, chat.LinearRequest{Identifier: "eng-7"})
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{"ENG-7", "State: In Progress", "Assignee: nobody", "Labels: bug", "[truncated: ", "- Ann, ", "- unknown, "} {
		if !strings.Contains(got.Text, want) {
			t.Errorf("the view lacks %q:\n%s", want, got.Text)
		}
	}
	if _, err := a.ChatLinearComment(ctx, chat.LinearRequest{Identifier: "ENG-7", Body: "done"}); err != nil {
		t.Fatal(err)
	}
	if id := fake.variables[len(fake.variables)-1]["issue"]; id != "9f1c" {
		t.Errorf("commented on %v, want the issue's ID 9f1c", id)
	}
	for _, auth := range fake.auth {
		if auth != "pdp-proxied-linear-protocol7" {
			t.Errorf("Authorization was %q, want the placeholder alone", auth)
		}
	}

	calls := len(fake.queries)
	for name, call := range map[string]func() error{
		"view": func() error {
			_, err := a.ChatLinearIssueView(ctx, chat.LinearRequest{Identifier: "OPS-1"})
			return err
		},
		"search": func() error { _, err := a.ChatLinearIssueSearch(ctx, chat.LinearRequest{Team: "OPS"}); return err },
		"create": func() error {
			_, err := a.ChatLinearIssueCreate(ctx, chat.LinearRequest{Team: "OPS", Title: "t"})
			return err
		},
	} {
		if err := call(); err == nil || !strings.Contains(err.Error(), "not one this worker may use") {
			t.Errorf("%s in another team: got %v, want a refusal", name, err)
		}
	}
	if len(fake.queries) != calls {
		t.Errorf("a refused call reached Linear")
	}
	if _, err := a.ChatLinearIssueView(ctx, chat.LinearRequest{Identifier: "ENG-8"}); err == nil || !strings.Contains(err.Error(), "Entity not found") {
		t.Errorf("an unknown issue: got %v, want Linear's error", err)
	}
}

// web_search sends the script's body with the placeholder and renders the
// results. web_extract refuses more than the limit of URLs.
func TestWebToolsSendTheScriptsRequest(t *testing.T) {
	var header string
	var body map[string]any
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		header = r.Header.Get("x-api-key")
		content, _ := io.ReadAll(r.Body)
		_ = json.Unmarshal(content, &body)
		if r.URL.Path != "/v1/search" {
			http.Error(w, `{"error":"Forbidden"}`, http.StatusForbidden)
			return
		}
		_, _ = w.Write([]byte(`{"search_id":"s1","results":[{"url":"https://go.dev/doc/devel/release","title":"Release History","publish_date":null,"excerpts":["go1.26.3 (released 2026-11-04)"]}],"warnings":null}`))
	}))
	t.Cleanup(server.Close)
	a := chat.NewWebActivities(chat.WebConfig{Placeholder: "towa-proxied-parallel", URL: server.URL + "/v1", HTTP: server.Client()})
	ctx := context.Background()

	got, err := a.ChatWebSearch(ctx, chat.WebRequest{Objective: "The latest Go release"})
	if err != nil {
		t.Fatal(err)
	}
	if header != "towa-proxied-parallel" {
		t.Errorf("x-api-key was %q", header)
	}
	if queries, _ := body["search_queries"].([]any); len(queries) != 1 || queries[0] != "The latest Go release" || body["mode"] != "basic" || body["max_chars_total"] != float64(chat.SearchChars) {
		t.Errorf("the body was %v, want the objective as the only query, basic mode and the character bound", body)
	}
	if !strings.Contains(got.Text, "https://go.dev/doc/devel/release") || !strings.Contains(got.Text, "go1.26.3") {
		t.Errorf("the result lacks the page:\n%s", got.Text)
	}

	urls := make([]string, chat.MaxExtractURLs+1)
	for i := range urls {
		urls[i] = "https://example.com/"
	}
	body = nil
	if _, err := a.ChatWebExtract(ctx, chat.WebRequest{Objective: "o", URLs: urls}); err == nil || body != nil {
		t.Errorf("%d URLs: got %v, sent %v, want a refusal and no request", len(urls), err, body)
	}
	if _, err := a.ChatWebExtract(ctx, chat.WebRequest{Objective: "o", URLs: urls[:1]}); err == nil || !strings.Contains(err.Error(), "403") {
		t.Errorf("a refused extract: got %v, want the 403", err)
	}
}
