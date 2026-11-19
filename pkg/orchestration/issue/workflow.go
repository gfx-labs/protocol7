package issue

import (
	"errors"
	"fmt"
	"strings"
	"time"

	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"

	"github.com/gfx-labs/protocol7/pkg/orchestration/chat"
	"github.com/gfx-labs/protocol7/pkg/orchestration/review"
	"github.com/gfx-labs/protocol7/pkg/orchestration/upgrade"
	"github.com/gfx-labs/protocol7/pkg/support/ids"
)

// ActivityStartChat starts the chat of an issue. It is a method of
// Activities.
const ActivityStartChat = "IssueStartChat"

var (
	readOptions = workflow.ActivityOptions{
		TaskQueue:           TaskQueue,
		StartToCloseTimeout: 2 * time.Minute,
		RetryPolicy: &temporal.RetryPolicy{
			InitialInterval: 2 * time.Second, BackoffCoefficient: 2, MaximumInterval: time.Minute, MaximumAttempts: 6,
			NonRetryableErrorTypes: []string{review.ErrorRefused},
		},
	}
	// scanOptions are the scan's own, smaller budget: three attempts of two
	// minutes with 2s and 4s between them, so six minutes and six seconds at
	// the worst. The schedule's run timeout is that plus a margin, see
	// ScanRunTimeout. A scan that fails is tried again by the next one anyway.
	scanOptions = workflow.ActivityOptions{
		TaskQueue:           TaskQueue,
		StartToCloseTimeout: 2 * time.Minute,
		RetryPolicy: &temporal.RetryPolicy{
			InitialInterval: 2 * time.Second, BackoffCoefficient: 2, MaximumInterval: 30 * time.Second, MaximumAttempts: 3,
			NonRetryableErrorTypes: []string{review.ErrorRefused},
		},
	}
	writeOptions = workflow.ActivityOptions{
		TaskQueue:           TaskQueue,
		StartToCloseTimeout: time.Minute,
		RetryPolicy: &temporal.RetryPolicy{
			InitialInterval: 2 * time.Second, BackoffCoefficient: 2, MaximumInterval: 30 * time.Second, MaximumAttempts: 5,
			NonRetryableErrorTypes: []string{review.ErrorRefused},
		},
	}
)

// worstCase is the longest an activity with these options can take: every
// attempt runs to its timeout and the retries wait their full backoff.
func worstCase(options workflow.ActivityOptions) time.Duration {
	policy := options.RetryPolicy
	total := time.Duration(policy.MaximumAttempts) * options.StartToCloseTimeout
	wait := policy.InitialInterval
	for range policy.MaximumAttempts - 1 {
		total += min(wait, policy.MaximumInterval)
		wait = time.Duration(float64(wait) * policy.BackoffCoefficient)
	}
	return total
}

// scanSettle lets GitHub's label-filtered issue list catch up with a label write.
const scanSettle = 5 * time.Second

// ScanRunTimeout is how long the old schedule let a scan run: label visibility,
// the worst case of its retries and a minute for the workflow's own tasks.
// The schedule skips a tick while a scan runs, so scans do not stack.
var ScanRunTimeout = scanSettle + worstCase(scanOptions) + time.Minute

// IntakeRunTimeout bounds the one-time opener check and its label write.
var IntakeRunTimeout = worstCase(writeOptions) + time.Minute

// IntakeWorkflow runs only for an opened delivery, never later issue events.
func IntakeWorkflow(ctx workflow.Context, input IntakeInput) error {
	return workflow.ExecuteActivity(workflow.WithActivityOptions(ctx, writeOptions), ActivityIntake, input).Get(ctx, nil)
}

// ScanWorkflow is what the old schedule ran: one scan for labelled issues. It
// is unchanged, so that the runs it left replay. The scanner replaced it.
func ScanWorkflow(ctx workflow.Context, template Template) ([]Ref, error) {
	if workflow.GetVersion(ctx, "issue-label-list-settle", workflow.DefaultVersion, 1) != workflow.DefaultVersion {
		if err := workflow.Sleep(ctx, scanSettle); err != nil {
			return nil, err
		}
	}
	var started []Ref
	err := workflow.ExecuteActivity(workflow.WithActivityOptions(ctx, scanOptions), ActivityScan, template).Get(ctx, &started)
	return started, err
}

// Workflow follows one issue from the label to the merge. Each pass posts what
// the chat reported, reads the issue and the chat's pull requests, files the
// reviews of new heads, relays what is new to the chat, and decides whether
// the issue is done.
func Workflow(ctx workflow.Context, state State) error {
	if err := workflow.SetQueryHandler(ctx, QueryState, func() (State, error) { return state, nil }); err != nil {
		return err
	}
	reports := workflow.GetSignalChannel(ctx, chat.SignalReport)
	takeReports := func() {
		for {
			var report chat.Report
			if !reports.ReceiveAsync(&report) {
				return
			}
			state.Reports = append(state.Reports, report)
		}
	}
	wakes := workflow.GetSignalChannel(ctx, SignalWake)
	// takeWakes drops the wakes that are waiting. The poll that follows reads
	// everything they announced, and a wake that arrives during the poll is
	// kept for the next wait. It is also what lets Continue-As-New go: the
	// next run polls first.
	takeWakes := func() {
		for wakes.ReceiveAsync(nil) {
		}
	}
	ref := state.Request.Ref
	template := state.Request.Template
	if state.Started.IsZero() {
		state.Started = workflow.Now(ctx)
	}
	if state.Status == "" {
		state.Status = StatusStarting
	}
	readCtx := workflow.WithActivityOptions(ctx, readOptions)
	writeCtx := workflow.WithActivityOptions(ctx, writeOptions)
	comment := func(key, body string) error {
		return workflow.ExecuteActivity(writeCtx, ActivityComment, CommentInput{Issue: ref, Key: key, Body: body}).Get(ctx, nil)
	}
	closeChat := func() {
		if state.Chat == "" || state.ChatEnded {
			return
		}
		closeCtx, _ := workflow.NewDisconnectedContext(ctx)
		_ = workflow.SignalExternalWorkflow(closeCtx, chat.WorkflowID(state.Chat), "", chat.SignalClose, chat.Action{}).Get(closeCtx, nil)
	}
	finish := func(status Status, action FinishAction, note string) error {
		closeChat()
		state.Status = status
		if note != "" {
			if err := comment("finish-"+state.Chat, note); err != nil {
				state.Error = cause(err)
			}
		}
		if action == "" {
			return nil
		}
		input := FinishInput{Issue: ref, Label: template.Label, Action: action}
		if err := workflow.ExecuteActivity(writeCtx, ActivityFinish, input).Get(ctx, nil); err != nil {
			state.Error = cause(err)
			return err
		}
		return nil
	}

	if state.Chat == "" {
		var poll Poll
		if err := workflow.ExecuteActivity(readCtx, ActivityPoll, PollInput{Issue: ref, Label: template.Label}).Get(ctx, &poll); err != nil {
			state.Error = cause(err)
			return err
		}
		if !poll.Open || !poll.Labeled {
			state.Status = StatusStopped
			return nil
		}
		if err := workflow.SideEffect(ctx, func(workflow.Context) any { return ids.New() }).Get(&state.Chat); err != nil {
			return err
		}
		message := chat.Message{ID: messageID(0), Text: firstMessage(ref, poll)}
		start := chat.State{
			Request: chat.Request{
				ID: state.Chat, Title: chatTitle(ref, poll.Title), Host: template.Host, Backend: template.Backend, PoolScope: template.PoolScope,
				Model: template.ModelName, Models: template.Models, Repositories: template.Repositories, Scope: template.Scope,
				Issue: &chat.Issue{Repo: ref.Repo, Number: ref.Number}, Owner: workflow.GetInfo(ctx).WorkflowExecution.ID,
			},
			Pending: []chat.Message{message},
		}
		if err := workflow.ExecuteActivity(writeCtx, ActivityStartChat, start).Get(ctx, nil); err != nil {
			state.Error = cause(err)
			return err
		}
		state.Sent, state.LastSent, state.LastComment = 1, message.ID, poll.LastComment
		state.Status = StatusWorking
		body := fmt.Sprintf("selfdev is working on this issue in the chat `%s`, on the branch `%s`.", state.Chat, branch(state))
		if link := chatLink(template, state.Chat); link != "" {
			body += " " + link
		}
		body += "\n\nComments here from members of the repository reach the agent. Remove the label `" + template.Label + "` to stop it."
		if err := comment("start-"+state.Chat, body); err != nil {
			state.Error = cause(err)
		}
	}

	for {
		takeReports()
		takeWakes()
		if upgrade.Due(ctx) && len(state.Reports) == 0 {
			return upgrade.ContinueAsNew(ctx, WorkflowName, state)
		}
		if ctx.Err() != nil {
			closeChat()
			return ctx.Err()
		}
		if err := postReports(ctx, &state, comment); err != nil {
			// GitHub refused or failed for minutes. The reports stay, and the
			// next pass posts them. A failed workflow would be restarted by
			// the scan with a new chat.
			state.Error = cause(err)
			wait(ctx, reports, wakes, &state)
			continue
		}

		var poll Poll
		input := PollInput{Issue: ref, Label: template.Label, Branch: branch(state), PullRepos: template.PullRepos, LastComment: state.LastComment, Pulls: state.Pulls}
		if err := workflow.ExecuteActivity(readCtx, ActivityPoll, input).Get(ctx, &poll); err != nil {
			state.Error = cause(err)
			wait(ctx, reports, wakes, &state)
			continue
		}
		state.Error = ""
		merged, open := 0, 0
		for _, pull := range poll.Pulls {
			switch pull.State {
			case "merged":
				merged++
			case "open":
				open++
			}
		}
		switch {
		case !poll.Open && merged > 0:
			return finish(StatusDone, "", "")
		case !poll.Open:
			return finish(StatusClosed, "", "")
		case !poll.Labeled:
			return finish(StatusStopped, "", "The label `"+template.Label+"` was removed, so selfdev stopped working on this issue. The branch and any open pull request are left as they are.")
		case merged > 0 && open == 0 && state.Answered:
			return finish(StatusDone, FinishClose, fmt.Sprintf("The pull requests of the branch `%s` are merged, so this issue is done.", branch(state)))
		case state.ChatEnded:
			return finish(StatusGaveUp, FinishUnlabel, "The selfdev chat ended before the issue was done, so the label `"+template.Label+"` is removed. Add it again to start over in a new chat.")
		case workflow.Now(ctx).Sub(state.Started) > Lifetime:
			return finish(StatusGaveUp, FinishUnlabel, fmt.Sprintf("selfdev worked on this issue for %s without finishing, so the label `%s` is removed.", Lifetime, template.Label))
		case state.Sent >= MaxMessages && state.Answered:
			return finish(StatusGaveUp, FinishUnlabel, fmt.Sprintf("The selfdev chat answered %d messages without finishing, so the label `%s` is removed.", state.Sent, template.Label))
		}

		// The workflow's own marks come from its state, and what GitHub says
		// comes from the poll.
		known := map[Ref]*Pull{}
		var pulls []Pull
		for _, status := range poll.Pulls {
			pull := Pull{}
			for _, previous := range state.Pulls {
				if pullRef(previous) == pullRef(status.Pull) {
					pull = previous
				}
			}
			pull.Repo, pull.Number, pull.URL, pull.Head, pull.Base, pull.State = status.Repo, status.Number, status.URL, status.Head, status.Base, status.State
			pull.LastReview = max(pull.LastReview, status.LastReview)
			pulls = append(pulls, pull)
		}
		for index := range pulls {
			known[pullRef(pulls[index])] = &pulls[index]
		}
		for index := range poll.Pulls {
			status := &poll.Pulls[index]
			pull := known[pullRef(status.Pull)]
			if status.State != "open" || pull.FiledHead == status.Head {
				continue
			}
			// A new head of a pull request whose review ended, or one that
			// was never filed. A running review reads the head by itself,
			// and filing again wakes it.
			request := review.Request{Repo: status.Repo, Number: status.Number, Merge: true, Host: template.Host, Backend: template.Backend, Models: template.Models}
			if err := workflow.ExecuteActivity(writeCtx, ActivityFileReview, request).Get(ctx, nil); err != nil {
				state.Error = cause(err)
				continue
			}
			pull.FiledHead = status.Head
		}
		message := relayMessage(ref, poll.Comments, poll.Pulls, known)
		state.Pulls, state.LastComment = pulls, poll.LastComment
		if message != "" {
			id := messageID(state.Sent)
			err := workflow.SignalExternalWorkflow(ctx, chat.WorkflowID(state.Chat), "", chat.SignalMessage, chat.Message{ID: id, Text: message}).Get(ctx, nil)
			if err != nil {
				// The chat is gone, which the next pass handles.
				state.ChatEnded = true
				continue
			}
			state.Sent, state.LastSent, state.Answered = state.Sent+1, id, false
		}
		state.Status = StatusWaiting
		if !state.Answered {
			state.Status = StatusWorking
		}
		wait(ctx, reports, wakes, &state)
	}
}

// postReports posts the chat's reports to the issue, oldest first, and notes
// what they say. A report that could not be posted stays.
func postReports(ctx workflow.Context, state *State, comment func(key, body string) error) error {
	for len(state.Reports) > 0 {
		report := state.Reports[0]
		key := "end-" + state.Chat
		switch {
		case report.Escalation != nil:
			key = "escalation-" + report.Escalation.ID
		case !report.Ended:
			key = "turn-" + state.Chat + "-" + report.Message
		}
		if body := reportComment(*state, report); body != "" {
			if err := comment(key, body); err != nil {
				return err
			}
		}
		state.Reports = state.Reports[1:]
		if report.Message == state.LastSent && report.Idle {
			state.Answered = true
		}
		if report.Ended {
			state.ChatEnded = true
		}
	}
	return nil
}

// wait sleeps until the next poll, a report from the chat, or a wake from
// GitHub's webhook.
func wait(ctx workflow.Context, reports, wakes workflow.ReceiveChannel, state *State) {
	timerCtx, cancel := workflow.WithCancel(ctx)
	defer cancel()
	selector := workflow.NewSelector(ctx)
	selector.AddFuture(workflow.NewTimer(timerCtx, PollEvery), func(workflow.Future) {})
	selector.AddReceive(reports, func(channel workflow.ReceiveChannel, _ bool) {
		var report chat.Report
		channel.Receive(ctx, &report)
		state.Reports = append(state.Reports, report)
	})
	selector.AddReceive(wakes, func(channel workflow.ReceiveChannel, _ bool) {
		channel.Receive(ctx, nil)
	})
	selector.AddReceive(ctx.Done(), func(workflow.ReceiveChannel, bool) {})
	selector.Select(ctx)
}

func messageID(sent int) string { return fmt.Sprintf("issue%d", sent) }

func chatTitle(ref Ref, title string) string {
	text := fmt.Sprintf("#%d %s", ref.Number, strings.TrimSpace(title))
	if runes := []rune(text); len(runes) > 80 {
		text = string(runes[:80])
	}
	return text
}

// cause is the message of the error an activity returned, without Temporal's
// description of the activity.
func cause(err error) string {
	if application, ok := errors.AsType[*temporal.ApplicationError](err); ok {
		return application.Message()
	}
	return err.Error()
}
