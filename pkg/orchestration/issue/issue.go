// Package issue is selfdev's path from a GitHub issue to merged code.
// A person labels an issue, or opening one with write access adds the label.
// The scanner, one long-lived workflow, finds it and starts one
// workflow per issue, which starts a chat whose agent works on the issue by
// itself. The workflow relays the issue's comments, the reviews of the chat's
// pull requests and their failed checks to the agent, posts the agent's
// answers to the issue, files each pull request's review, and closes the
// issue when its pull requests are merged.
package issue

import (
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/gfx-labs/protocol7/pkg/domain/navi"

	"github.com/gfx-labs/protocol7/pkg/orchestration/chat"
)

const (
	// WorkflowName is the registered name of the workflow of one issue.
	WorkflowName = navi.IssueWorkflow
	// IntakeWorkflowName is the one-time check of an issue's opener.
	IntakeWorkflowName = navi.IssueIntakeWorkflow
	// ScanWorkflowName is the registered name of the workflow the old
	// schedule ran, one scan for each start. It stays registered until the
	// runs it left are gone. The scanner (ScannerWorkflowName) replaced it.
	ScanWorkflowName = navi.IssueScanWorkflow
	// ScheduleID is the ID of the schedule the scanner replaced. The worker
	// deletes it at startup.
	ScheduleID = "selfdev-issues"
	// TaskQueue is the queue of the selfdev worker.
	TaskQueue = navi.TaskQueue

	// QueryState returns the issue's State.
	QueryState = navi.QueryState
	// SignalWake wakes the workflow's wait, so that it polls at once. The
	// web server sends it when GitHub's webhook says that something changed
	// on the issue or on a pull request of the chat's branch. It carries no
	// data: a poll reads the state from GitHub.
	SignalWake = navi.SignalIssueWake

	// DefaultLabel is the label that hands an issue to selfdev.
	DefaultLabel = "selfdev"
	// CommentMarker starts the hidden marker in each comment the workflow
	// posts. Posting looks for it first, so a retried activity does not post
	// twice, and comments with it are not relayed back to the agent.
	CommentMarker = "<!-- selfdev-issue "
)

// Limits.
const (
	// ScanEvery is how often the scanner looks for labelled issues when no
	// wake signal made it look sooner.
	ScanEvery = 2 * time.Minute
	// PollEvery is how often an issue's workflow reads the issue and its
	// pull requests when no signal woke it. It is the fallback for a lost
	// webhook: GitHub's events (SignalWake) and the chat's reports wake it
	// sooner.
	PollEvery = 10 * time.Minute
	// Lifetime is how long an issue is worked on before the workflow gives
	// it back to a person.
	Lifetime = 14 * 24 * time.Hour
	// DefaultLimit is how many issues are worked on at once.
	DefaultLimit = 3
	// MaxMessages bounds the messages relayed to one issue's chat, and so its
	// turns, for an agent and a reviewer that do not settle.
	MaxMessages = 40
	// textLimit bounds one comment or review that is relayed to the agent.
	textLimit = 8 << 10
)

// WorkflowPrefix starts the workflow ID of every issue's workflow.
const WorkflowPrefix = "issue-"

// WorkflowID is the workflow ID of one issue's workflow.
func WorkflowID(repo string, number int) string {
	return WorkflowPrefix + strings.ReplaceAll(repo, "/", "-") + "-" + strconv.Itoa(number)
}

// IntakeWorkflowID deduplicates opened deliveries, even after intake completes.
func IntakeWorkflowID(repo string, number int) string {
	return "intake-" + WorkflowID(repo, number)
}

// IntakeInput asks the worker to label a newly opened issue if its author is trusted.
type IntakeInput = navi.IntakeInput

// Template is what every issue's chat is started with. The worker
// sets it when it ensures the scanner, and the scan hands it to each issue.
type Template = navi.Template

// Ref names an issue.
type Ref = navi.IssueRef

// Request is the input of a new issue workflow.
type Request = navi.IssueRequest

// Status says what an issue's workflow is doing.
type Status = navi.IssueStatus

// Statuses of an issue.
const (
	StatusStarting = navi.IssueStarting // reading the issue and starting the chat
	StatusWorking  = navi.IssueWorking  // the agent has a message to answer
	StatusWaiting  = navi.IssueWaiting  // for a person, a review, checks or a merge
	StatusDone     = navi.IssueDone     // the pull requests merged and the issue is closed
	StatusClosed   = navi.IssueClosed   // a person closed the issue
	StatusStopped  = navi.IssueStopped  // the label was removed
	StatusGaveUp   = navi.IssueGaveUp   // the chat ended or the lifetime ran out, the label was removed
)

// Pull is a pull request from the chat's branch, as the workflow tracks it.
type Pull = navi.IssuePull

// Ref is the pull request's name.
func pullRef(p Pull) Ref { return Ref{Repo: p.Repo, Number: p.Number} }

// State is everything an issue's workflow keeps. It is the workflow's input,
// so it is also what one run hands to the next on Continue-As-New.
type State = navi.IssueState

// Branch is the chat's branch.
func branch(s State) string { return chat.Branch(s.Chat) }

// Comment is a comment on the issue.
type Comment struct {
	ID     int64  `json:"id"`
	Author string `json:"author"`
	Body   string `json:"body"`
}

// Review is a review of a pull request.
type Review struct {
	ID     int64  `json:"id"`
	Author string `json:"author"`
	// State is GitHub's: APPROVED, CHANGES_REQUESTED or COMMENTED.
	State string `json:"state"`
	Body  string `json:"body"`
	Head  string `json:"head"`
}

// Check is a check run that failed.
type Check struct {
	Name string `json:"name"`
	URL  string `json:"url,omitempty"`
}

// PullStatus is a pull request as one poll read it.
type PullStatus struct {
	Pull
	// Mergeable is nil while GitHub has not worked it out.
	Mergeable *bool `json:"mergeable,omitempty"`
	// Failed are the head's failed check runs, and Pending counts the ones
	// that still run.
	Failed  []Check `json:"failed,omitempty"`
	Pending int     `json:"pending,omitempty"`
	// Reviews are the reviews newer than the pull request's LastReview.
	Reviews []Review `json:"reviews,omitempty"`
}

// PollInput is the input of ActivityPoll.
type PollInput struct {
	Issue Ref    `json:"issue"`
	Label string `json:"label"`
	// Branch is the chat's branch. Empty looks for no pull requests.
	Branch      string   `json:"branch,omitempty"`
	PullRepos   []string `json:"pull_repos,omitempty"`
	LastComment int64    `json:"last_comment"`
	// Pulls are the pull requests already known, for their LastReview.
	Pulls []Pull `json:"pulls,omitempty"`
}

// Poll is what one poll read.
type Poll struct {
	Title   string `json:"title"`
	Body    string `json:"body"`
	Author  string `json:"author"`
	Open    bool   `json:"open"`
	Labeled bool   `json:"labeled"`
	// Comments are the comments newer than LastComment, oldest first, from
	// the repository's members and not posted by this workflow.
	Comments []Comment `json:"comments,omitempty"`
	// LastComment is the newest comment's ID, also of the ones skipped.
	LastComment int64        `json:"last_comment"`
	Pulls       []PullStatus `json:"pulls,omitempty"`
}

// CommentInput is the input of ActivityComment.
type CommentInput struct {
	Issue Ref `json:"issue"`
	// Key makes the comment's marker. A comment with the marker is posted
	// once.
	Key  string `json:"key"`
	Body string `json:"body"`
}

// FinishInput is the input of ActivityFinish.
type FinishInput struct {
	Issue  Ref          `json:"issue"`
	Label  string       `json:"label"`
	Action FinishAction `json:"action"`
}

// FinishAction is what is done to an issue whose work ends.
type FinishAction string

const (
	// FinishClose closes the issue as completed.
	FinishClose FinishAction = "close"
	// FinishUnlabel removes the label, which gives the issue back to a
	// person.
	FinishUnlabel FinishAction = "unlabel"
)

// firstMessage is the chat's first message: the issue and its comments.
func firstMessage(ref Ref, poll Poll) string {
	var text strings.Builder
	fmt.Fprintf(&text, "Work on the GitHub issue %s by @%s: %s\n\n", ref, poll.Author, poll.Title)
	body := strings.TrimSpace(poll.Body)
	if body == "" {
		body = "(The issue has no description.)"
	}
	text.WriteString(cut(body))
	if len(poll.Comments) > 0 {
		text.WriteString("\n\nIts comments:\n")
		for _, comment := range poll.Comments {
			fmt.Fprintf(&text, "\n@%s:\n%s\n", comment.Author, quote(comment.Body))
		}
	}
	return text.String()
}

// relayMessage is what a poll found that the agent has to know, or "".
// It marks what it relays in the pulls.
func relayMessage(issue Ref, comments []Comment, pulls []PullStatus, known map[Ref]*Pull) string {
	var parts []string
	for _, comment := range comments {
		parts = append(parts, fmt.Sprintf("@%s commented on the issue %s:\n%s", comment.Author, issue, quote(comment.Body)))
	}
	for _, status := range pulls {
		pull := known[pullRef(status.Pull)]
		name := pullRef(status.Pull).String()
		for _, review := range status.Reviews {
			switch {
			case strings.Contains(review.Body, "<!-- selfdev-review ") && strings.Contains(review.Body, ":approve -->"):
			case strings.TrimSpace(review.Body) == "" && review.State != "CHANGES_REQUESTED":
			default:
				parts = append(parts, fmt.Sprintf("A review of %s at %s by @%s (%s):\n%s", name, short(review.Head), review.Author, strings.ToLower(review.State), quote(review.Body)))
			}
		}
		switch status.State {
		case "open":
			if len(status.Failed) > 0 && status.Pending == 0 && pull.ChecksRelayed != status.Head {
				pull.ChecksRelayed = status.Head
				var names []string
				for _, check := range status.Failed {
					names = append(names, fmt.Sprintf("- %s %s", check.Name, check.URL))
				}
				parts = append(parts, fmt.Sprintf("Checks failed on %s at %s. `gh run view --log-failed` in the clone shows the logs of a workflow run.\n%s", name, short(status.Head), strings.Join(names, "\n")))
			}
			if status.Mergeable != nil && !*status.Mergeable && pull.ConflictRelayed != status.Head {
				pull.ConflictRelayed = status.Head
				parts = append(parts, fmt.Sprintf("%s does not merge cleanly into %s. Merge or rebase onto the base branch, resolve the conflicts and push.", name, status.Base))
			}
		case "closed":
			if !pull.ClosedRelayed {
				pull.ClosedRelayed = true
				parts = append(parts, fmt.Sprintf("%s was closed without being merged.", name))
			}
		}
	}
	if len(parts) == 0 {
		return ""
	}
	return "New on the issue and its pull requests:\n\n" + strings.Join(parts, "\n\n")
}

// reportComment is the issue comment for one of the chat's reports, or "".
// A stop, a failure and a turn that the chat gave up resuming each say why,
// with the link to the chat, so that nobody has to look in Temporal.
func reportComment(state State, report chat.Report) string {
	var text strings.Builder
	switch {
	case report.Escalation != nil:
		// The chat raised an escalation in the middle of a turn. It waits for
		// the answer, which a person gives in navi.
		fmt.Fprintf(&text, "The selfdev agent cannot go on without a person: **%s**", report.Escalation.Title)
		if link := escalationLink(state.Request.Template, report.Escalation.ID); link != "" {
			text.WriteString("\n\nAnswer it in navi: " + link)
		} else {
			text.WriteString("\n\nAnswer it in navi, on the Escalations list (escalation `" + report.Escalation.ID + "`).")
		}
		text.WriteString("\n\nThe chat waits for the answer and goes on from it.")
	case report.Error != "" && report.Ended:
		fmt.Fprintf(&text, "The selfdev chat ended: %s", report.Error)
	case report.GaveUp:
		fmt.Fprintf(&text, "The selfdev agent was cut off %d times while it worked, and the chat stopped resuming it: %s", report.Resumes+1, report.Error)
		if t := strings.TrimSpace(report.Text); t != "" {
			text.WriteString("\n\nWhat it had said before it was cut off:\n\n" + t)
		}
		text.WriteString("\n\nA comment on this issue starts it again.")
	case report.Error != "":
		fmt.Fprintf(&text, "A turn of the selfdev chat failed: %s", report.Error)
	case report.Stopped:
		text.WriteString("A person stopped the selfdev agent's turn")
		if report.StoppedBy != "" {
			text.WriteString(" (" + report.StoppedBy + ")")
		}
		text.WriteString(". The chat is open and waits for a comment.")
	case strings.TrimSpace(report.Text) != "":
		text.WriteString(strings.TrimSpace(report.Text))
		if report.Resumes > 0 {
			fmt.Fprintf(&text, "\n\n(The agent was cut off %d times by restarts and carried on.)", report.Resumes)
		}
	default:
		return ""
	}
	text.WriteString("\n\n<sub>selfdev chat `" + state.Chat + "`")
	if link := chatLink(state.Request.Template, state.Chat); link != "" {
		text.WriteString(", " + link)
	}
	text.WriteString("</sub>")
	return text.String()
}

func quote(text string) string {
	lines := strings.Split(cut(strings.TrimSpace(text)), "\n")
	for index, line := range lines {
		lines[index] = "> " + line
	}
	return strings.Join(lines, "\n")
}

func cut(text string) string {
	if len(text) <= textLimit {
		return text
	}
	return strings.ToValidUTF8(text[:textLimit], "") + "\n(cut)"
}

func short(sha string) string {
	if len(sha) > 7 {
		return sha[:7]
	}
	return sha
}
