package issue_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/mock"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/converter"
	"go.temporal.io/sdk/testsuite"
	"go.temporal.io/sdk/workflow"

	"github.com/gfx-labs/protocol7/pkg/orchestration/chat"
	"github.com/gfx-labs/protocol7/pkg/orchestration/issue"
	"github.com/gfx-labs/protocol7/pkg/orchestration/review"
)

// fakes stand in for GitHub and Temporal.
type fakes struct {
	// polls is what the polls return in order. The last one repeats.
	polls    []issue.Poll
	pollAt   int
	inputs   []issue.PollInput
	chats    []chat.State
	comments []issue.CommentInput
	finished []issue.FinishInput
	reviews  []review.Request
	// onPoll runs after each poll, with its index.
	onPoll func(int)
}

func (f *fakes) poll(_ context.Context, input issue.PollInput) (issue.Poll, error) {
	f.inputs = append(f.inputs, input)
	index := min(f.pollAt, len(f.polls)-1)
	f.pollAt++
	if f.onPoll != nil {
		f.onPoll(index)
	}
	return f.polls[index], nil
}

func (f *fakes) comment(_ context.Context, input issue.CommentInput) error {
	f.comments = append(f.comments, input)
	return nil
}

func (f *fakes) finish(_ context.Context, input issue.FinishInput) error {
	f.finished = append(f.finished, input)
	return nil
}

func (f *fakes) fileReview(_ context.Context, request review.Request) error {
	f.reviews = append(f.reviews, request)
	return nil
}

func (f *fakes) startChat(_ context.Context, start chat.State) error {
	f.chats = append(f.chats, start)
	return nil
}

func environment(t *testing.T, f *fakes) *testsuite.TestWorkflowEnvironment {
	t.Helper()
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	env.RegisterWorkflowWithOptions(issue.Workflow, workflow.RegisterOptions{Name: issue.WorkflowName})
	env.RegisterActivityWithOptions(f.poll, activity.RegisterOptions{Name: issue.ActivityPoll})
	env.RegisterActivityWithOptions(f.comment, activity.RegisterOptions{Name: issue.ActivityComment})
	env.RegisterActivityWithOptions(f.finish, activity.RegisterOptions{Name: issue.ActivityFinish})
	env.RegisterActivityWithOptions(f.fileReview, activity.RegisterOptions{Name: issue.ActivityFileReview})
	env.RegisterActivityWithOptions(f.startChat, activity.RegisterOptions{Name: issue.ActivityStartChat})
	return env
}

func request() issue.State {
	return issue.State{Request: issue.Request{
		Ref: issue.Ref{Repo: "gfx-labs/r", Number: 7},
		Template: issue.Template{
			Repos: []string{"gfx-labs/r"}, Label: "selfdev", Host: "h", Models: []string{"m"},
			Repositories: []chat.Repository{{Name: "r", URL: "https://github.com/gfx-labs/r.git"}}, PullRepos: []string{"gfx-labs/r"},
		},
	}}
}

func pull(state string) issue.PullStatus {
	return issue.PullStatus{Pull: issue.Pull{Repo: "gfx-labs/r", Number: 9, Head: "h1", Base: "master", State: state}}
}

// An issue starts a chat in issue mode with the issue as its first message.
// The pull request from the chat's branch gets a review ticket, a member's
// comment and a requested change reach the chat, each answer is posted to the
// issue, and when the pull request merges and the chat has answered, the
// issue is closed and the chat too.
func TestAnIssueRunsFromLabelToMerge(t *testing.T) {
	open := issue.Poll{Title: "Add a thing", Body: "Please add it.", Author: "alice", Open: true, Labeled: true, LastComment: 5}
	withPull := open
	withPull.Pulls = []issue.PullStatus{pull("open")}
	withFeedback := withPull
	withFeedback.Comments = []issue.Comment{{ID: 6, Author: "alice", Body: "Also rename it."}}
	withFeedback.LastComment = 6
	changes := pull("open")
	changes.Reviews = []issue.Review{{ID: 1, Author: "app", State: "COMMENTED", Body: "**Verdict: request_changes**\nfix x\n<!-- selfdev-review h1:request_changes -->", Head: "h1"}}
	withFeedback.Pulls = []issue.PullStatus{changes}
	merged := withPull
	merged.Pulls = []issue.PullStatus{pull("merged")}
	f := &fakes{polls: []issue.Poll{open, withPull, withFeedback, withPull, merged}}

	env := environment(t, f)
	var signals []chat.Message
	var closes int
	report := func(message string) {
		env.SignalWorkflow(chat.SignalReport, chat.Report{Message: message, StopReason: "end_turn", Text: "Did " + message, Idle: true})
	}
	f.onPoll = func(index int) {
		switch index {
		case 1:
			// The first turn ends while the pull request is open.
			env.RegisterDelayedCallback(func() { report("issue0") }, time.Second)
		case 3:
			env.RegisterDelayedCallback(func() { report("issue1") }, time.Second)
		}
	}
	env.OnSignalExternalWorkflow(mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil).Run(func(args mock.Arguments) {
		switch args.String(3) {
		case chat.SignalMessage:
			signals = append(signals, args.Get(4).(chat.Message))
		case chat.SignalClose:
			closes++
		}
	})
	env.ExecuteWorkflow(issue.WorkflowName, request())

	if err := env.GetWorkflowError(); err != nil {
		t.Fatal(err)
	}
	if len(f.chats) != 1 {
		t.Fatalf("started %d chats, want 1", len(f.chats))
	}
	started := f.chats[0]
	if started.Request.Issue == nil || started.Request.Issue.Number != 7 || started.Request.Owner != "default-test-workflow-id" {
		t.Errorf("chat request %+v, want issue mode with this workflow as owner", started.Request)
	}
	if len(started.Pending) != 1 || !strings.Contains(started.Pending[0].Text, "Please add it.") {
		t.Errorf("first message %+v, want the issue's body", started.Pending)
	}
	branch := "chat/" + started.Request.ID
	if f.inputs[1].Branch != branch {
		t.Errorf("polled branch %q, want %q", f.inputs[1].Branch, branch)
	}
	if len(f.reviews) != 1 || f.reviews[0].Number != 9 || !f.reviews[0].Merge {
		t.Errorf("filed reviews %+v, want one for #9 with merge", f.reviews)
	}
	if len(signals) != 1 || !strings.Contains(signals[0].Text, "Also rename it.") || !strings.Contains(signals[0].Text, "fix x") || signals[0].ID != "issue1" {
		t.Errorf("messages to the chat %+v, want one with the comment and the change request", signals)
	}
	var bodies []string
	for _, comment := range f.comments {
		bodies = append(bodies, comment.Key)
	}
	want := []string{"start-" + started.Request.ID, "turn-" + started.Request.ID + "-issue0", "turn-" + started.Request.ID + "-issue1", "finish-" + started.Request.ID}
	if strings.Join(bodies, " ") != strings.Join(want, " ") {
		t.Errorf("comments %v, want %v", bodies, want)
	}
	if len(f.finished) != 1 || f.finished[0].Action != issue.FinishClose {
		t.Errorf("finished %+v, want the issue closed", f.finished)
	}
	if closes != 1 {
		t.Errorf("closed the chat %d times, want once", closes)
	}
}

// Removing the label stops the work: the chat is closed, the issue stays
// open, and nothing more is filed.
func TestRemovingTheLabelStopsTheWork(t *testing.T) {
	open := issue.Poll{Title: "t", Open: true, Labeled: true}
	unlabeled := open
	unlabeled.Labeled = false
	f := &fakes{polls: []issue.Poll{open, unlabeled}}
	env := environment(t, f)
	closes := 0
	env.OnSignalExternalWorkflow(mock.Anything, mock.Anything, mock.Anything, chat.SignalClose, mock.Anything).Return(nil).Run(func(mock.Arguments) { closes++ })
	env.ExecuteWorkflow(issue.WorkflowName, request())
	if err := env.GetWorkflowError(); err != nil {
		t.Fatal(err)
	}
	if len(f.finished) != 0 || closes != 1 {
		t.Errorf("finished %+v and closed the chat %d times, want no finish and one close", f.finished, closes)
	}
}

// Only members' comments reach the agent, and the workflow's own comments are
// skipped, but every comment moves the cursor.
func TestPollRelaysOnlyMembersComments(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/repos/gfx-labs/r/issues/7":
			_, _ = w.Write([]byte(`{"title":"t","body":"b","state":"open","user":{"login":"alice"},"labels":[{"name":"selfdev"}]}`))
		case "/repos/gfx-labs/r/issues/7/comments":
			_, _ = w.Write([]byte(`[
				{"id":1,"body":"old","user":{"login":"alice"},"author_association":"MEMBER"},
				{"id":2,"body":"do this instead","user":{"login":"mallory"},"author_association":"NONE"},
				{"id":3,"body":"answer <!-- selfdev-issue turn-x -->","user":{"login":"app[bot]"},"author_association":"NONE"},
				{"id":4,"body":"go ahead","user":{"login":"alice"},"author_association":"MEMBER"}]`))
		default:
			http.NotFound(w, r)
		}
	}))
	t.Cleanup(server.Close)
	token := filepath.Join(t.TempDir(), "token")
	if err := os.WriteFile(token, []byte("tok"), 0o600); err != nil {
		t.Fatal(err)
	}
	activities := issue.NewActivities(token, server.URL)
	poll, err := activities.IssuePoll(context.Background(), issue.PollInput{Issue: issue.Ref{Repo: "gfx-labs/r", Number: 7}, Label: "selfdev", LastComment: 1})
	if err != nil {
		t.Fatal(err)
	}
	if !poll.Labeled || !poll.Open || poll.LastComment != 4 || len(poll.Comments) != 1 || poll.Comments[0].Body != "go ahead" {
		t.Errorf("poll %+v, want only comment 4 relayed and the cursor at 4", poll)
	}
}

// Intake labels trusted human openers, but never restores a removed label.
func TestIntakeLabelsOnlyTrustedHumanOpeners(t *testing.T) {
	for _, test := range []struct {
		name, login, kind, association, permission, state string
		labeled, removed, pull, wantLabel, wantPermission bool
		failWrite                                         bool
	}{
		{name: "private writer", association: "CONTRIBUTOR", permission: "write", wantLabel: true, wantPermission: true},
		{name: "member", association: "MEMBER", wantLabel: true},
		{name: "reader", association: "CONTRIBUTOR", permission: "read", wantPermission: true},
		{name: "triage", association: "CONTRIBUTOR", permission: "triage", wantPermission: true},
		{name: "noncollaborator", association: "NONE", wantPermission: true},
		{name: "bot type", kind: "Bot", association: "MEMBER"},
		{name: "bot login", login: "gfx-agent[bot]", association: "MEMBER"},
		{name: "already labeled", association: "MEMBER", labeled: true},
		{name: "removed label", association: "MEMBER", removed: true},
		{name: "closed", association: "MEMBER", state: "closed"},
		{name: "pull request", association: "MEMBER", pull: true},
		{name: "failed write", association: "MEMBER", wantLabel: true, failWrite: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			writes, permissions, pages := 0, 0, 0
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				switch r.URL.Path {
				case "/repos/gfx-labs/r/issues/7":
					login, state := test.login, test.state
					if login == "" {
						login = "alice"
					}
					if state == "" {
						state = "open"
					}
					raw := map[string]any{"state": state, "user": map[string]string{"login": login, "type": test.kind}, "author_association": test.association}
					if test.labeled {
						raw["labels"] = []map[string]string{{"name": "work"}}
					}
					if test.pull {
						raw["pull_request"] = map[string]any{}
					}
					_ = json.NewEncoder(w).Encode(raw)
				case "/repos/gfx-labs/r/collaborators/alice/permission":
					permissions++
					if test.permission == "" {
						http.NotFound(w, r)
						return
					}
					_ = json.NewEncoder(w).Encode(map[string]string{"permission": test.permission})
				case "/repos/gfx-labs/r/issues/7/events":
					pages++
					if test.removed && r.URL.Query().Get("page") == "1" {
						_, _ = w.Write([]byte("[" + strings.Repeat(`{"event":"labeled","label":{"name":"other"}},`, 99) + `{"event":"labeled","label":{"name":"other"}}]`))
					} else if test.removed {
						_, _ = w.Write([]byte(`[{"event":"unlabeled","label":{"name":"work"}}]`))
					} else {
						_, _ = w.Write([]byte(`[]`))
					}
				case "/repos/gfx-labs/r/issues/7/labels":
					if r.Method != http.MethodPost {
						t.Errorf("label method %s", r.Method)
					}
					var labels struct {
						Labels []string `json:"labels"`
					}
					if err := json.NewDecoder(r.Body).Decode(&labels); err != nil || len(labels.Labels) != 1 || labels.Labels[0] != "work" {
						t.Errorf("label body %+v, error %v", labels, err)
					}
					writes++
					if test.failWrite {
						http.Error(w, "unavailable", http.StatusServiceUnavailable)
					} else {
						_, _ = w.Write([]byte(`[]`))
					}
				default:
					t.Errorf("unexpected request %s", r.URL)
					http.NotFound(w, r)
				}
			}))
			t.Cleanup(server.Close)
			token := filepath.Join(t.TempDir(), "token")
			if err := os.WriteFile(token, []byte("tok"), 0o600); err != nil {
				t.Fatal(err)
			}
			err := issue.NewActivities(token, server.URL).IssueIntake(context.Background(), issue.IntakeInput{Issue: issue.Ref{Repo: "gfx-labs/r", Number: 7}, Label: "work"})
			if (err != nil) != test.failWrite {
				t.Fatalf("intake error %v", err)
			}
			if (writes == 1) != test.wantLabel || writes > 1 {
				t.Errorf("label writes %d, want label %v", writes, test.wantLabel)
			}
			if (permissions == 1) != test.wantPermission || permissions > 1 {
				t.Errorf("permission reads %d, want %v", permissions, test.wantPermission)
			}
			if test.removed && pages != 2 {
				t.Errorf("event pages %d, want 2", pages)
			}
		})
	}
}

// An issue that GitHub lists as blocked by an open issue is not taken, and
// neither is a pull request.
func TestScanSkipsBlockedIssues(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/repos/gfx-labs/r/issues" {
			http.NotFound(w, r)
			return
		}
		_, _ = w.Write([]byte(`[
			{"number":5,"issue_dependencies_summary":{"blocked_by":1,"total_blocked_by":1}},
			{"number":6,"pull_request":{},"issue_dependencies_summary":{"blocked_by":0}}]`))
	}))
	t.Cleanup(server.Close)
	token := filepath.Join(t.TempDir(), "token")
	if err := os.WriteFile(token, []byte("tok"), 0o600); err != nil {
		t.Fatal(err)
	}
	started, err := issue.NewActivities(token, server.URL).IssueScan(context.Background(), issue.Template{Repos: []string{"gfx-labs/r"}, Label: "selfdev", Limit: 3})
	if err != nil {
		t.Fatal(err)
	}
	if len(started) != 0 {
		t.Errorf("started %v, want none", started)
	}
}

// The scan pages through the labelled issues until a short page, so the ones
// past the first hundred are taken too, and a pull request on a page counts
// toward its length.
func TestScanPagesThroughLabelledIssues(t *testing.T) {
	const total = 250
	var pages []string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/repos/gfx-labs/r/issues" {
			http.NotFound(w, r)
			return
		}
		page, _ := strconv.Atoi(r.URL.Query().Get("page"))
		pages = append(pages, r.URL.Query().Get("page"))
		var items []string
		for n := (page-1)*100 + 1; n <= min(page*100, total); n++ {
			switch {
			case n == 7:
				items = append(items, `{"number":7,"pull_request":{}}`)
			case n == 8:
				items = append(items, `{"number":8,"issue_dependencies_summary":{"blocked_by":1}}`)
			default:
				items = append(items, fmt.Sprintf(`{"number":%d}`, n))
			}
		}
		_, _ = w.Write([]byte("[" + strings.Join(items, ",") + "]"))
	}))
	t.Cleanup(server.Close)
	token := filepath.Join(t.TempDir(), "token")
	if err := os.WriteFile(token, []byte("tok"), 0o600); err != nil {
		t.Fatal(err)
	}
	refs, err := issue.Labelled(issue.NewActivities(token, server.URL), context.Background(), issue.Template{Repos: []string{"gfx-labs/r"}, Label: "selfdev"})
	if err != nil {
		t.Fatal(err)
	}
	if want := total - 2; len(refs) != want {
		t.Fatalf("got %d issues, want %d", len(refs), want)
	}
	if last := refs[len(refs)-1]; last.Number != total {
		t.Errorf("last issue is %v, want number %d", last, total)
	}
	if got := strings.Join(pages, ","); got != "1,2,3" {
		t.Errorf("read pages %s, want 1,2,3", got)
	}
}

// The schedule lets a scan run as long as its retries can take.
func TestScheduleTimeoutCoversTheScanRetries(t *testing.T) {
	if issue.ScanRunTimeout < issue.ScanWorstCase() {
		t.Errorf("run timeout %s is shorter than the worst case of the scan %s", issue.ScanRunTimeout, issue.ScanWorstCase())
	}
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	started := env.Now()
	read := false
	env.RegisterActivityWithOptions(func(context.Context, issue.Template) ([]issue.Ref, error) {
		read = true
		if !env.Now().After(started) {
			t.Error("the scan read GitHub before allowing newly written labels to become visible")
		}
		return nil, nil
	}, activity.RegisterOptions{Name: issue.ActivityScan})
	env.ExecuteWorkflow(issue.ScanWorkflow, issue.Template{})
	if err := env.GetWorkflowError(); err != nil {
		t.Fatal(err)
	}
	if !read {
		t.Error("the scan never read GitHub")
	}
}

// A chat that stops, fails or gives up resuming says why on the issue, with a
// link to the chat in navi's session page of the core project.
func TestTheIssueSaysWhyTheChatStopped(t *testing.T) {
	open := issue.Poll{Title: "t", Open: true, Labeled: true}
	f := &fakes{polls: []issue.Poll{open}}
	env := environment(t, f)
	env.OnSignalExternalWorkflow(mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	f.onPoll = func(index int) {
		if index != 0 {
			return
		}
		send := func(delay time.Duration, report chat.Report) {
			env.RegisterDelayedCallback(func() { env.SignalWorkflow(chat.SignalReport, report) }, delay)
		}
		send(time.Second, chat.Report{Message: "issue0", Stopped: true, StoppedBy: "alice@example.com", StopReason: "cancelled", Idle: true})
		send(2*time.Second, chat.Report{Message: "issue0", GaveUp: true, Resumes: 3, StopReason: "interrupted", Error: "the turn was cut off 4 times", Idle: true})
		send(3*time.Second, chat.Report{Idle: true, Ended: true, Error: "no login"})
	}
	state := request()
	state.Request.Template.PageURL = "https://navi.tuna-carp.ts.net/"
	env.ExecuteWorkflow(issue.WorkflowName, state)
	if err := env.GetWorkflowError(); err != nil {
		t.Fatal(err)
	}

	var bodies []string
	for _, comment := range f.comments {
		bodies = append(bodies, comment.Body)
	}
	all := strings.Join(bodies, "\n---\n")
	link := "https://navi.tuna-carp.ts.net/#/selfdev/s/" + f.chats[0].Request.ID
	for _, want := range []string{"A person stopped the selfdev agent's turn (alice@example.com)", "cut off 4 times while it worked", "The selfdev chat ended: no login", link} {
		if !strings.Contains(all, want) {
			t.Errorf("comments %q do not contain %q", all, want)
		}
	}
	if strings.Contains(all, "/#/c/") {
		t.Errorf("comments %q still link the page's old route", all)
	}
}

// GitHub shows the app a member whose membership is private as a
// contributor. A comment from such a person with write access reaches the
// agent. One from a person with no access does not.
func TestPollRelaysCommentsOfPeopleWithWriteAccess(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/repos/gfx-labs/r/issues/7":
			_, _ = w.Write([]byte(`{"title":"t","state":"open","user":{"login":"alice"},"labels":[{"name":"selfdev"}]}`))
		case "/repos/gfx-labs/r/issues/7/comments":
			_, _ = w.Write([]byte(`[
				{"id":1,"body":"private member","user":{"login":"alice"},"author_association":"CONTRIBUTOR"},
				{"id":2,"body":"stranger","user":{"login":"mallory"},"author_association":"CONTRIBUTOR"},
				{"id":3,"body":"the app","user":{"login":"gfx-agent[bot]"},"author_association":"NONE"}]`))
		case "/repos/gfx-labs/r/collaborators/alice/permission":
			_, _ = w.Write([]byte(`{"permission":"admin"}`))
		case "/repos/gfx-labs/r/collaborators/mallory/permission":
			_, _ = w.Write([]byte(`{"permission":"read"}`))
		default:
			http.NotFound(w, r)
		}
	}))
	t.Cleanup(server.Close)
	token := filepath.Join(t.TempDir(), "token")
	if err := os.WriteFile(token, []byte("tok"), 0o600); err != nil {
		t.Fatal(err)
	}
	poll, err := issue.NewActivities(token, server.URL).IssuePoll(context.Background(), issue.PollInput{Issue: issue.Ref{Repo: "gfx-labs/r", Number: 7}, Label: "selfdev"})
	if err != nil {
		t.Fatal(err)
	}
	if poll.LastComment != 3 || len(poll.Comments) != 1 || poll.Comments[0].Body != "private member" {
		t.Errorf("poll %+v, want only alice's comment relayed", poll)
	}
}

// The issue's run continues as new when the deployment's target version
// changed, once the chat is started and no report waits, and asks to start on
// the target. Unchanged, it runs on.
func TestTheIssueContinuesAsNewWhenTheTargetVersionChanges(t *testing.T) {
	for _, test := range []struct {
		name               string
		suggested, changed bool
		want               workflow.ContinueAsNewVersioningBehavior
		continues          bool
	}{
		{"unchanged", false, false, 0, false},
		{"suggested", true, false, workflow.ContinueAsNewVersioningBehaviorUnspecified, true},
		{"target changed", false, true, workflow.ContinueAsNewVersioningBehaviorAutoUpgrade, true},
	} {
		t.Run(test.name, func(t *testing.T) {
			open := issue.Poll{Title: "t", Open: true, Labeled: true}
			unlabeled := open
			unlabeled.Labeled = false
			f := &fakes{polls: []issue.Poll{open, unlabeled}}
			env := environment(t, f)
			env.OnSignalExternalWorkflow(mock.Anything, mock.Anything, mock.Anything, chat.SignalClose, mock.Anything).Return(nil)
			env.SetContinueAsNewSuggested(test.suggested)
			env.SetTargetWorkerDeploymentVersionChanged(test.changed)
			env.ExecuteWorkflow(issue.WorkflowName, request())

			err := env.GetWorkflowError()
			var continued *workflow.ContinueAsNewError
			if got := errors.As(err, &continued); got != test.continues {
				t.Fatalf("continued as new = %v (%v), want %v", got, err, test.continues)
			}
			if !test.continues {
				return
			}
			if continued.InitialVersioningBehavior != test.want {
				t.Errorf("versioning behavior %v, want %v", continued.InitialVersioningBehavior, test.want)
			}
			// The chat was started by this run, and the next run goes on with it.
			if len(f.chats) != 1 {
				t.Errorf("started %d chats, want 1", len(f.chats))
			}
			var next issue.State
			if err := converter.GetDefaultDataConverter().FromPayloads(continued.Input, &next); err != nil {
				t.Fatal(err)
			}
			if next.Chat == "" {
				t.Errorf("the next run's state has no chat: %+v", next)
			}
		})
	}
}

// With reports not yet posted, the run does not continue as new: it posts them
// first, and the next pass continues.
func TestTheIssueDoesNotContinueWhileReportsWait(t *testing.T) {
	open := issue.Poll{Title: "t", Open: true, Labeled: true}
	f := &fakes{polls: []issue.Poll{open}}
	env := environment(t, f)
	env.OnSignalExternalWorkflow(mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	env.SetTargetWorkerDeploymentVersionChanged(true)
	state := request()
	state.Chat = "c1"
	state.Reports = []chat.Report{{Message: "m1", Text: "Did it", StopReason: "end_turn", Idle: true}}
	env.ExecuteWorkflow(issue.WorkflowName, state)

	var continued *workflow.ContinueAsNewError
	if err := env.GetWorkflowError(); !errors.As(err, &continued) {
		t.Fatalf("workflow ended with %v, want a continue as new", err)
	}
	if len(f.comments) == 0 {
		t.Fatal("the run continued as new with a report that was not posted")
	}
	var next issue.State
	if err := converter.GetDefaultDataConverter().FromPayloads(continued.Input, &next); err != nil {
		t.Fatal(err)
	}
	if len(next.Reports) != 0 {
		t.Errorf("the next run carries %d reports, want none", len(next.Reports))
	}
}

// A wake from GitHub's webhook makes the workflow poll at once, and without one the workflow polls on its slow timer.
func TestAWakePollsAtOnce(t *testing.T) {
	open := issue.Poll{Title: "t", Open: true, Labeled: true}
	unlabeled := open
	unlabeled.Labeled = false
	f := &fakes{polls: []issue.Poll{open, open, open, unlabeled}}
	env := environment(t, f)
	var times []time.Time
	f.onPoll = func(index int) {
		times = append(times, env.Now())
		if index == 1 {
			env.RegisterDelayedCallback(func() { env.SignalWorkflow(issue.SignalWake, nil) }, time.Minute)
		}
	}
	env.OnSignalExternalWorkflow(mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	env.ExecuteWorkflow(issue.WorkflowName, request())
	if err := env.GetWorkflowError(); err != nil {
		t.Fatal(err)
	}
	if len(times) != 4 {
		t.Fatalf("polled %d times, want 4 (start, first pass, the wake, the timer)", len(times))
	}
	if woke := times[2].Sub(times[1]); woke > 2*time.Minute {
		t.Errorf("the wake polled after %s, want about a minute", woke)
	}
	if fallback := times[3].Sub(times[2]); fallback != issue.PollEvery {
		t.Errorf("with no wake the poll came after %s, want %s", fallback, issue.PollEvery)
	}
}

// A chat that raises an escalation is reported on the issue at once, with the
// link to answer it in navi. The comment is keyed by the escalation, so one
// that is reported twice is posted once by the comment activity.
func TestTheIssueCommentsAnEscalationOfTheChat(t *testing.T) {
	open := issue.Poll{Title: "t", Open: true, Labeled: true}
	f := &fakes{polls: []issue.Poll{open}}
	env := environment(t, f)
	env.OnSignalExternalWorkflow(mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	f.onPoll = func(index int) {
		if index != 0 {
			return
		}
		ref := &chat.EscalationRef{ID: "esc1", Title: "A registry token", Status: chat.EscalationOpen}
		for delay := range 2 {
			env.RegisterDelayedCallback(func() { env.SignalWorkflow(chat.SignalReport, chat.Report{Escalation: ref}) }, time.Duration(delay+1)*time.Second)
		}
		env.RegisterDelayedCallback(func() { env.SignalWorkflow(chat.SignalReport, chat.Report{Idle: true, Ended: true}) }, 3*time.Second)
	}
	state := request()
	state.Request.Template.PageURL = "https://navi.tuna-carp.ts.net/"
	env.ExecuteWorkflow(issue.WorkflowName, state)
	if err := env.GetWorkflowError(); err != nil {
		t.Fatal(err)
	}
	found := 0
	for _, comment := range f.comments {
		if !strings.Contains(comment.Body, "A registry token") {
			continue
		}
		found++
		if !strings.Contains(comment.Body, "https://navi.tuna-carp.ts.net/#/selfdev/e/esc1") {
			t.Errorf("comment %q has no link to the escalation", comment.Body)
		}
	}
	if found == 0 {
		t.Errorf("no comment tells of the escalation: %+v", f.comments)
	}
}
