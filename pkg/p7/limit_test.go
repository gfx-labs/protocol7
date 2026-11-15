package p7_test

import (
	"context"
	"strings"
	"testing"
	"time"

	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/workflow"

	"github.com/gfx-labs/protocol7/pkg/p7"
)

// holdWorkflow opens a session in the limit, and holds it until it gets the
// signal "finish". Its query "opened" says whether the session has opened.
func holdWorkflow(ctx workflow.Context, host, limit string, priority int) error {
	opened := false
	if err := workflow.SetQueryHandler(ctx, "opened", func() (bool, error) { return opened, nil }); err != nil {
		return err
	}
	session, err := p7.OpenSession(ctx, p7.SessionSpec{
		Placement: p7.Placement{Backend: p7.BackendHost, Host: host},
		Limit:     limit, Priority: priority,
	})
	if err != nil {
		return err
	}
	opened = true
	workflow.GetSignalChannel(ctx, "finish").Receive(ctx, nil)
	return session.Close(ctx)
}

// A session over the limit waits and opens when another closes, and the
// waiter of the highest priority goes first.
func TestASessionOverTheLimitWaitsForAPlace(t *testing.T) {
	temporalClient, address := startStack(t)
	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Minute)
	defer cancel()

	consumer, err := p7.NewWorker(p7.WorkerConfig{
		TaskQueue: "consumer", TemporalAddress: address,
		Limits: p7.LimitConfig{Capacity: map[string]int{"model:*": 2}, Reconcile: time.Second},
	})
	if err != nil {
		t.Fatal(err)
	}
	consumer.RegisterWorkflow("hold", holdWorkflow)
	if err := consumer.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(consumer.Stop)

	const limit = "model:test"
	start := func(name string, priority int) client.WorkflowRun {
		run, err := temporalClient.ExecuteWorkflow(ctx, client.StartWorkflowOptions{ID: name, TaskQueue: "consumer"},
			"hold", "test", limit, priority)
		if err != nil {
			t.Fatal(err)
		}
		return run
	}
	opened := func(name string) bool {
		value, err := temporalClient.QueryWorkflow(ctx, name, "", "opened")
		if err != nil {
			return false
		}
		var opened bool
		return value.Get(&opened) == nil && opened
	}
	waitFor := func(what string, condition func() bool) {
		t.Helper()
		for deadline := time.Now().Add(time.Minute); !condition(); time.Sleep(100 * time.Millisecond) {
			if time.Now().After(deadline) {
				t.Fatalf("timed out waiting for %s", what)
			}
		}
	}
	waiting := func(want int) func() bool {
		return func() bool {
			status, err := p7.QueryLimit(ctx, temporalClient, limit)
			return err == nil && len(status.Waiters) == want
		}
	}
	finish := func(run client.WorkflowRun) {
		t.Helper()
		if err := temporalClient.SignalWorkflow(ctx, run.GetID(), "", "finish", nil); err != nil {
			t.Fatal(err)
		}
		if err := run.Get(ctx, nil); err != nil {
			t.Fatalf("%s: %v", run.GetID(), err)
		}
	}

	// Two sessions take both places. The third and the fourth wait, the
	// fourth with the higher priority, and arrive after the third.
	first, second := start("first", 0), start("second", 0)
	waitFor("the first two sessions to open", func() bool { return opened("first") && opened("second") })
	third := start("third", 1)
	waitFor("the third session to wait", waiting(1))
	fourth := start("fourth", 5)
	waitFor("the fourth session to wait", waiting(2))
	if opened("third") || opened("fourth") {
		t.Fatal("a third session opened with the limit full")
	}

	finish(first)
	waitFor("the high priority waiter to open", func() bool { return opened("fourth") })
	if opened("third") {
		t.Error("the lower priority waiter opened before the higher one")
	}
	status, err := p7.QueryLimit(ctx, temporalClient, limit)
	if err != nil {
		t.Fatal(err)
	}
	if len(status.Holders) != 2 || len(status.Waiters) != 1 {
		t.Errorf("the limit has %d holders and %d waiters, want 2 and 1", len(status.Holders), len(status.Waiters))
	}

	finish(second)
	waitFor("the last waiter to open", func() bool { return opened("third") })
	finish(fourth)
	finish(third)
	waitFor("every place to be free", func() bool {
		status, err := p7.QueryLimit(ctx, temporalClient, limit)
		return err == nil && len(status.Holders) == 0 && len(status.Waiters) == 0
	})

	// A holder that ends without closing its session is found and its place
	// freed.
	lost := start("lost", 0)
	waitFor("the lost session to open", func() bool { return opened("lost") })
	other := start("other", 0)
	waitFor("the other session to open", func() bool { return opened("other") })
	last := start("last", 0)
	waitFor("the last session to wait", waiting(1))
	if err := temporalClient.TerminateWorkflow(ctx, lost.GetID(), "", "the test ends it"); err != nil {
		t.Fatal(err)
	}
	waitFor("the place of the ended workflow to be freed", func() bool { return opened("last") })
	finish(other)
	finish(last)

	// A cancelled waiter gives up its place in the queue.
	one, two := start("one", 0), start("two", 0)
	waitFor("two sessions to open", func() bool { return opened("one") && opened("two") })
	cancelled := start("cancelled", 0)
	waitFor("a session to wait", waiting(1))
	if err := temporalClient.CancelWorkflow(ctx, cancelled.GetID(), ""); err != nil {
		t.Fatal(err)
	}
	if err := cancelled.Get(ctx, nil); err == nil {
		t.Error("the cancelled waiter returned no error")
	}
	waitFor("the cancelled waiter to leave the queue", waiting(0))
	finish(one)
	finish(two)

	// A limit with no capacity in the configuration is an error and not a wait.
	unconfigured, err := temporalClient.ExecuteWorkflow(ctx, client.StartWorkflowOptions{TaskQueue: "consumer"},
		"hold", "test", "nothing", 0)
	if err != nil {
		t.Fatal(err)
	}
	err = unconfigured.Get(ctx, nil)
	if err == nil || !strings.Contains(err.Error(), "has no capacity") {
		t.Errorf("a limit with no capacity gave %v, want an error that says so", err)
	}
}

func TestLimitConfig(t *testing.T) {
	_, err := p7.NewWorker(p7.WorkerConfig{TaskQueue: "q", Limits: p7.LimitConfig{Capacity: map[string]int{"a": 0}}})
	if err == nil {
		t.Error("a capacity of zero was accepted")
	}
	for _, test := range []struct {
		name string
		want int
	}{
		{"model:fast", 3}, // exact over pattern
		{"model:slow", 2},
		{"model:", 2},
		{"repo:gfx-labs/protocol7", 4},
		{"repo:gfx-labs/protocol7-selfdev", 5}, // the longer pattern
		{"device:android", 0},
	} {
		got, _ := p7.LimitCapacity(p7.LimitConfig{Capacity: map[string]int{
			"model:*": 2, "model:fast": 3, "repo:*": 4, "repo:*-selfdev": 5,
		}}, test.name)
		if got != test.want {
			t.Errorf("capacity of %q is %d, want %d", test.name, got, test.want)
		}
	}
}

func TestWaitersAreServedByPriorityThenAge(t *testing.T) {
	now := time.Now()
	waiters := []p7.LimitEntry{
		{Lease: "old", Priority: 0, Since: now.Add(-10 * time.Minute)},
		{Lease: "new", Priority: 3, Since: now.Add(-time.Minute)},
		{Lease: "later", Priority: 3, Since: now},
	}
	if got := p7.NextWaiter(0, now, waiters); got != 1 {
		t.Errorf("without aging the waiter %d goes first, want 1: the highest priority that came first", got)
	}
	if got := p7.NextWaiter(time.Minute, now, waiters); got != 0 {
		t.Errorf("with aging the waiter %d goes first, want 0: ten minutes outweigh three points", got)
	}
}
