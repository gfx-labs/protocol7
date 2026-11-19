package review_test

import (
	"errors"
	"testing"
	"time"

	"github.com/gfx-labs/protocol7/pkg/domain/scope"
	"github.com/gfx-labs/protocol7/pkg/orchestration/review"
)

func ptr(b bool) *bool { return &b }

func chatPull() review.Pull {
	return review.Pull{
		Repo: "gfx-labs/r", Number: 3, HeadRef: "chat/abc", HeadRepo: "gfx-labs/r", Head: "h1",
		Open: true, Mergeable: ptr(true), MergeState: "clean", Checks: review.Checks{Total: 2},
	}
}

func TestDecide(t *testing.T) {
	for _, test := range []struct {
		name   string
		change func(*review.Pull)
		age    time.Duration
		want   review.Action
	}{
		{"clean and green", func(*review.Pull) {}, 0, review.ActionMerge},
		{"another branch is a person's", func(p *review.Pull) { p.HeadRef = "feature/x" }, 0, review.ActionBlock},
		{"a fork", func(p *review.Pull) { p.HeadRepo = "someone/r" }, 0, review.ActionBlock},
		{"the opt-out label", func(p *review.Pull) { p.Labels = []string{review.NoMergeLabel} }, 0, review.ActionBlock},
		{"a conflict", func(p *review.Pull) { p.Mergeable, p.MergeState = ptr(false), "dirty" }, 0, review.ActionBlock},
		{"a failed check", func(p *review.Pull) { p.Checks.Failed = 1 }, 0, review.ActionBlock},
		{"a check that runs", func(p *review.Pull) { p.Checks.Pending = 1 }, 0, review.ActionWait},
		{"mergeability not known", func(p *review.Pull) { p.Mergeable, p.MergeState = nil, "unknown" }, 0, review.ActionWait},
		{"no checks yet on a new head", func(p *review.Pull) { p.Checks = review.Checks{} }, time.Second, review.ActionWait},
		{"no checks on an old head", func(p *review.Pull) { p.Checks = review.Checks{} }, 5 * time.Minute, review.ActionMerge},
		{"branch protection unmet", func(p *review.Pull) { p.MergeState = "blocked" }, 0, review.ActionBlock},
		{"behind the base", func(p *review.Pull) { p.MergeState = "behind" }, 0, review.ActionBlock},
	} {
		t.Run(test.name, func(t *testing.T) {
			pull := chatPull()
			test.change(&pull)
			if got, reason := review.Decide(pull, test.age); got != test.want {
				t.Errorf("Decide = %s (%s), want %s", got, reason, test.want)
			}
		})
	}
}

func TestParseVerdictTakesTheLastBlock(t *testing.T) {
	text := "```review-verdict\n{\"verdict\":\"comment\",\"summary\":\"first\"}\n```\nthen\n```review-verdict\n{\"verdict\":\"approve\",\"summary\":\" ok \"}\n```"
	got, err := review.ParseVerdict(text)
	if err != nil || got.Decision != review.Approve || got.Summary != "ok" {
		t.Fatalf("got %+v, %v, want the approval", got, err)
	}
	for _, bad := range []string{
		"", "approve", "```review-verdict\n{\"verdict\":\"merge\",\"summary\":\"x\"}\n```",
		"```review-verdict\n{\"verdict\":\"approve\"}\n```", "```review-verdict\n{nope}\n```",
	} {
		if _, err := review.ParseVerdict(bad); err == nil {
			t.Errorf("%q parsed, want an error", bad)
		}
	}
}

func TestParseURL(t *testing.T) {
	repo, number, err := review.ParseURL("https://github.com/gfx-labs/protocol7/pull/12/files")
	if err != nil || repo != "gfx-labs/protocol7" || number != 12 {
		t.Fatalf("got %s %d %v", repo, number, err)
	}
	for _, bad := range []string{"", "https://example.com/a/b/pull/1", "https://github.com/a/b/issues/1", "https://github.com/a/b/pull/x"} {
		if _, _, err := review.ParseURL(bad); err == nil {
			t.Errorf("%q parsed, want an error", bad)
		}
	}
}

func errorsAs[T any](err error, target *T) bool { return errors.As(err, target) }

// A review filed by someone whose scope does not reach the repository, or does
// not write pull requests and contents, is not merged by the workflow. One with
// no scope, filed from a session, is not limited here.
func TestCanMergeFollowsTheScope(t *testing.T) {
	write := map[string]string{"contents": "write", "pull_requests": "write", "metadata": "read"}
	for name, test := range map[string]struct {
		scope *scope.Scope
		want  bool
	}{
		"no scope":         {nil, true},
		"write":            {scopeOf([]string{"o/a"}, write), true},
		"other repository": {scopeOf([]string{"o/b"}, write), false},
		"read only":        {scopeOf([]string{"o/a"}, map[string]string{"contents": "read", "pull_requests": "read"}), false},
		"no pull requests": {scopeOf([]string{"o/a"}, map[string]string{"contents": "write"}), false},
		"no repository":    {scopeOf(nil, write), false},
	} {
		if got := review.CanMerge(review.Request{Repo: "O/A", Scope: test.scope}); got != test.want {
			t.Errorf("%s: CanMerge = %v, want %v", name, got, test.want)
		}
	}
}

func scopeOf(repositories []string, permissions map[string]string) *scope.Scope {
	scope := scope.NewScope("p", repositories, permissions)
	return &scope
}
