package review_test

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"go.temporal.io/sdk/temporal"

	"github.com/gfx-labs/protocol7/pkg/orchestration/review"
)

// github is a stand-in for the parts of GitHub's API that a review uses.
type github struct {
	t        *testing.T
	requests []string
	bodies   map[string]string
	handle   func(w http.ResponseWriter, r *http.Request) bool
}

func (g *github) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	body, _ := io.ReadAll(r.Body)
	key := r.Method + " " + r.URL.Path
	g.requests = append(g.requests, key)
	if g.bodies == nil {
		g.bodies = map[string]string{}
	}
	g.bodies[key] = string(body)
	if r.Header.Get("Authorization") != "Bearer tok" {
		g.t.Errorf("%s sent authorization %q, want the token in the file", key, r.Header.Get("Authorization"))
	}
	if g.handle != nil && g.handle(w, r) {
		return
	}
	switch key {
	case "GET /repos/gfx-labs/r/pulls/3":
		_, _ = w.Write([]byte(`{"title":"t","state":"open","draft":false,"merged":false,"user":{"login":"bot"},
			"labels":[{"name":"x"}],"mergeable":true,"mergeable_state":"clean","base":{"ref":"master"},
			"head":{"ref":"chat/a","sha":"h1","repo":{"full_name":"gfx-labs/r"}}}`))
	case "GET /repos/gfx-labs/r/commits/h1/check-runs":
		_, _ = w.Write([]byte(`{"check_runs":[{"status":"completed","conclusion":"success"},{"status":"completed","conclusion":"skipped"},
			{"status":"in_progress"},{"status":"completed","conclusion":"failure"}]}`))
	case "GET /repos/gfx-labs/r/pulls/3/reviews":
		_, _ = w.Write([]byte(`[]`))
	default:
		_, _ = w.Write([]byte(`{}`))
	}
}

func activities(t *testing.T, g *github) *review.Activities {
	t.Helper()
	g.t = t
	server := httptest.NewServer(g)
	t.Cleanup(server.Close)
	file := filepath.Join(t.TempDir(), "token")
	if err := os.WriteFile(file, []byte("tok\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	return review.NewActivities(review.GitHubConfig{TokenFile: file, Repositories: []string{"gfx-labs/r"}, APIURL: server.URL})
}

func TestReadingAPullRequestCountsItsCheckRuns(t *testing.T) {
	pull, err := activities(t, &github{}).ReviewPull(context.Background(), "gfx-labs/r", 3)
	if err != nil {
		t.Fatal(err)
	}
	if pull.Head != "h1" || pull.HeadRef != "chat/a" || pull.HeadRepo != "gfx-labs/r" || !pull.Open || pull.Author != "bot" {
		t.Errorf("pull %+v", pull)
	}
	if pull.Checks != (review.Checks{Total: 4, Pending: 1, Failed: 1}) {
		t.Errorf("checks %+v, want 4 runs, 1 running and 1 failed (skipped counts as passed)", pull.Checks)
	}
}

func TestARepositoryThatIsNotConfiguredIsRefusedWithoutACall(t *testing.T) {
	g := &github{}
	_, err := activities(t, g).ReviewPull(context.Background(), "someone/else", 1)
	var application *temporal.ApplicationError
	if err == nil || len(g.requests) != 0 || !errorsAs(err, &application) || application.Type() != review.ErrorRefused {
		t.Fatalf("err %v after %v, want a refusal and no request", err, g.requests)
	}
}

func TestPublishingPostsTheReviewAtTheHead(t *testing.T) {
	g := &github{}
	input := review.PublishInput{Repo: "gfx-labs/r", Number: 3, Head: "h1", Verdict: review.Verdict{Decision: review.Approve, Summary: "fine"}}
	if err := activities(t, g).ReviewPublish(context.Background(), input); err != nil {
		t.Fatal(err)
	}
	var sent map[string]string
	_ = json.Unmarshal([]byte(g.bodies["POST /repos/gfx-labs/r/pulls/3/reviews"]), &sent)
	if sent["event"] != "APPROVE" || sent["commit_id"] != "h1" || !strings.Contains(sent["body"], review.ReviewMarker+"h1:approve -->") {
		t.Errorf("sent %v, want an approval at h1 that carries its marker", sent)
	}
}

// A retried activity must not post the review twice.
func TestPublishingTwiceDoesNotPostTwice(t *testing.T) {
	g := &github{handle: func(w http.ResponseWriter, r *http.Request) bool {
		if r.Method == http.MethodGet && strings.HasSuffix(r.URL.Path, "/reviews") {
			_, _ = w.Write([]byte(`[{"body":"x\n` + review.ReviewMarker + `h1:approve -->"}]`))
			return true
		}
		return false
	}}
	input := review.PublishInput{Repo: "gfx-labs/r", Number: 3, Head: "h1", Verdict: review.Verdict{Decision: review.Approve, Summary: "fine"}}
	if err := activities(t, g).ReviewPublish(context.Background(), input); err != nil {
		t.Fatal(err)
	}
	for _, request := range g.requests {
		if strings.HasPrefix(request, "POST") {
			t.Errorf("posted %s, want no post", request)
		}
	}
}

// GitHub refuses an approval from the author of the pull request. The review
// is then a comment that names its verdict.
func TestAnApprovalFromTheAuthorFallsBackToAComment(t *testing.T) {
	posts := 0
	g := &github{handle: func(w http.ResponseWriter, r *http.Request) bool {
		if r.Method == http.MethodPost {
			posts++
			if posts == 1 {
				w.WriteHeader(http.StatusUnprocessableEntity)
				_, _ = w.Write([]byte(`{"message":"Can not approve your own pull request"}`))
				return true
			}
		}
		return false
	}}
	input := review.PublishInput{Repo: "gfx-labs/r", Number: 3, Head: "h1", Verdict: review.Verdict{Decision: review.Approve, Summary: "fine"}}
	if err := activities(t, g).ReviewPublish(context.Background(), input); err != nil {
		t.Fatal(err)
	}
	var sent map[string]string
	_ = json.Unmarshal([]byte(g.bodies["POST /repos/gfx-labs/r/pulls/3/reviews"]), &sent)
	if posts != 2 || sent["event"] != "COMMENT" || !strings.HasPrefix(sent["body"], "**Verdict: approve**") {
		t.Errorf("%d posts, last %v, want a second post as a comment that names the verdict", posts, sent)
	}
}

func TestMergeNamesTheReviewedHeadAndReadsGitHubsRefusals(t *testing.T) {
	status := 0
	g := &github{handle: func(w http.ResponseWriter, r *http.Request) bool {
		if r.Method == http.MethodPut && status != 0 {
			w.WriteHeader(status)
			_, _ = w.Write([]byte(`{"message":"no"}`))
			return true
		}
		return false
	}}
	a := activities(t, g)
	input := review.MergeInput{Repo: "gfx-labs/r", Number: 3, Head: "h1"}
	if err := a.ReviewMerge(context.Background(), input); err != nil {
		t.Fatal(err)
	}
	var sent map[string]string
	_ = json.Unmarshal([]byte(g.bodies["PUT /repos/gfx-labs/r/pulls/3/merge"]), &sent)
	if sent["sha"] != "h1" || sent["merge_method"] != "merge" {
		t.Errorf("sent %v, want the merge at h1", sent)
	}

	for code, want := range map[int]string{http.StatusConflict: review.ErrorHeadChanged, http.StatusMethodNotAllowed: review.ErrorNotMergeable, http.StatusUnprocessableEntity: review.ErrorRefused} {
		status = code
		err := a.ReviewMerge(context.Background(), input)
		var application *temporal.ApplicationError
		if !errorsAs(err, &application) || application.Type() != want || !application.NonRetryable() {
			t.Errorf("status %d gave %v, want a non-retryable %s", code, err, want)
		}
	}
	// A server error is retried.
	status = http.StatusBadGateway
	err := a.ReviewMerge(context.Background(), input)
	var application *temporal.ApplicationError
	if err == nil || errorsAs(err, &application) {
		t.Errorf("a 502 gave %v, want a plain error that Temporal retries", err)
	}
}
