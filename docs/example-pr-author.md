# Example: following our own pull request

The author side of `example-pr-review.md`. That workflow reviews pull requests. This one follows a pull request that one of our own agents opened: it answers review comments, fixes failing checks, resolves conflicts, updates a branch that is behind, requests review and merges.

It is a sketch against the library proposed in `plan.md` section 7. Nothing here compiles or has run. It is not a port: jobcity has no flow like this.

Both workflows can be open on the same pull request at once, one as author and one as reviewer, beside any number of people. The second half of this file is about how they avoid working against each other.

## The flow

```mermaid
flowchart TD
  R[Read the record's revision, then read GitHub] --> D{Decide}
  D -->|merged or closed| F[Finish]
  D -->|unanswered threads, failed checks or conflicts| X[Fix round: one coding session, one push]
  D -->|the same problem is still there after a fix, or the round limit| Q[Ask a person]
  D -->|only behind, and otherwise ready| U[Update the branch]
  D -->|this head has no review request yet| V[Request review]
  D -->|approved and nothing holds the merge| M[Merge]
  D -->|anything else| W[Wait for a change or the recheck timer]
  X --> R
  Q --> R
  U --> R
  V --> R
  M --> R
  W --> R
```

The loop is the same as the reviewer's: events only wake it, every decision comes from a fresh read of GitHub, and `Decide` is one pure function.

## Who does what

Each kind of write to a pull request has one owner. A workflow never does another owner's write, and never undoes one.

| Write | Owner |
|---|---|
| Commits on the branch, branch updates | The author workflow |
| Replies on review threads, and resolving a thread it answered | The author workflow |
| Requesting review | The author workflow |
| Reviews: approve, request changes, comment | The reviewer workflow |
| Opening review threads | The reviewer workflow |
| Merging a pull request one of our agents wrote | The author workflow |
| Merging a pull request someone else wrote | The reviewer workflow, where the repository allows it |
| Anything at all | A person. Both workflows read the result and carry on |

## What stops ping-pong

| Risk | Rule |
|---|---|
| A workflow wakes on its own write and acts again | Nothing is decided from an event. After its own write the workflow reads GitHub, and `Decide` finds nothing new to do |
| The author fixes, the same check fails, it fixes again, without end | An action is tried at most `MaxAttemptsPerProblem` times for the same problem. The problem's key does not contain the head, so a new push does not reset the count. After that a person is asked |
| Reviewer and author trade rounds without end | Each side has its own round limit and asks a person at the limit. The reviewer's later rounds are delta reviews, and its prompt says not to raise new minor points on code that did not change |
| Every fix push triggers a review of a half-finished head | The reviewer only reviews on a request. The author requests review once per head, and only when no problem is left and checks are not failing |
| The author pushes while a review of the old head is running | The reviewer cancels that round and waits for the new request. One fix round handles every open problem in one push, so there are few heads |
| A branch update makes a new head, which needs a new approval, while the base moves again | The branch is updated only when nothing else is missing. This still costs one review per update. A merge queue on the repository removes the problem, and is the better answer where it is available |
| A person pushes to the branch during a fix | The fix round is cancelled. Events carry their actor, so the workflow can tell its own push from someone else's |
| Both workflows try to merge | Only one of them owns the merge of a given pull request, decided by who the author is |
| Both workflows ask a person about the same pull request | Questions name the pull request as their subject. The attention list groups them |

## Types

```proto
// Ticket fields for kind pr_author. One ticket follows one pull request of ours.
message PRAuthor {
  string repo = 1;
  int32 pr = 2;
  string branch = 3;
  string issue = 4;                       // Linear key, may be empty
  string image = 5;
  repeated string reviewers = 6;          // who is asked to review
  repeated string required_approvers = 7; // whose approval the merge waits for
  int32 fix_rounds = 8;                   // fix rounds that finished
  int32 extra_rounds = 9;                 // rounds a person allowed beyond MaxFixRounds
  map<string, int32> attempts = 10;       // problem key to times a fix was tried
  repeated string requested_heads = 11;   // heads review was requested for
  repeated string cleared = 12;           // problem keys a person said to try again
}

// What the coding session submits at the end of a fix round.
message FixReport {
  FixOutcome outcome = 1;          // PUSHED, NOTHING_TO_DO or NEEDS_PERSON
  string head = 2;                 // the commit it pushed, with PUSHED
  repeated ThreadReply replies = 3; // thread, text, and whether to resolve it
  string summary = 4;
  string question = 5;             // with NEEDS_PERSON: what it needs to know
}
```

`Decide` reads this from the `github` package:

```go
// In package github.

// AuthorState is what GitHub says about one of our own pull requests now.
type AuthorState struct {
	Open   bool
	Merged bool
	Draft  bool
	Head   string
	// UnansweredThreads are unresolved threads whose last comment is not ours.
	UnansweredThreads []Thread
	// ChangesRequestedBy lists reviewers whose change request stands.
	ChangesRequestedBy []string
	// FailedChecks are finished checks on the head that did not pass.
	FailedChecks  []Check
	ChecksPending bool
	Mergeable     Mergeable
	// PendingReviewers have been asked for a review of this head and have not given one.
	PendingReviewers []string
	// ApprovedBy lists everyone whose approval stands for the current head.
	ApprovedBy []string
}
```

## The workflow

```go
// Package prauthor follows pull requests that our own agents opened.
package prauthor

import (
	"errors"
	"fmt"
	"slices"
	"strings"
	"time"

	p7 "github.com/gfx-labs/protocol7"
	"go.temporal.io/sdk/workflow"

	authorv1 "example.com/gfx/gen/author/v1"
	"example.com/gfx/github"
	"example.com/gfx/prompts"
)

const (
	// MaxFixRounds is the number of fix rounds a pull request may get before a person is asked.
	MaxFixRounds = 6
	// MaxAttemptsPerProblem is the number of times a fix is tried for the same problem.
	MaxAttemptsPerProblem = 2
	// WorkDirectory is where the coding session finds its clone.
	WorkDirectory = "/work"
)

// Config is everything that differs between installations.
type Config struct {
	// AuthorLogin is the GitHub login our agents open pull requests as.
	AuthorLogin string
	Coder       p7.Model
	Guidance    []p7.Input
	// Owners are asked the questions and told when something is overdue.
	Owners []p7.Recipient
	// ReviewOverdue says who is told, and when, if a requested review does not arrive.
	ReviewOverdue []p7.EscalationStep
	// QuestionOverdue is the same for a question nobody has answered.
	QuestionOverdue []p7.EscalationStep
	// Recheck is how often a waiting workflow reads GitHub when no event arrives.
	Recheck time.Duration
}

// Workflows holds the configuration that the workflows read.
type Workflows struct {
	config Config
}

// Follow follows one of our own pull requests until it is merged or closed.
func (w *Workflows) Follow(
	ctx workflow.Context,
	ticket p7.Ticket[*authorv1.PRAuthor],
) (*authorv1.Result, error) {
	pull := ticket.Fields
	pullRequestRef := github.PullRequest(pull.Repo, pull.Pr)

	for {
		object, err := p7.GetObject(ctx, pullRequestRef)
		if err != nil {
			return nil, fmt.Errorf("read pull request object: %w", err)
		}
		state, err := github.GetAuthorState(ctx, pull.Repo, pull.Pr)
		if err != nil {
			return nil, fmt.Errorf("read pull request: %w", err)
		}
		watched := p7.Since{Record: pullRequestRef, Revision: object.Revision, Kinds: github.AuthorRelevantEvents}

		action := Decide(state, pull)
		workflow.SetCurrentDetails(ctx, action.Reason)
		if err := p7.SetEscalation(ctx, ticket, w.escalationFor(ticket.ID, action)); err != nil {
			return nil, fmt.Errorf("set escalation: %w", err)
		}

		switch action.Kind {
		case ActionFinish:
			return &authorv1.Result{Outcome: action.Outcome}, nil

		case ActionFix:
			// Count the attempt before it runs, so a fix that fails halfway
			// still counts against its problems.
			for _, problem := range action.Problems {
				pull.Attempts[problem.Key]++
			}
			if err := p7.UpdateFields(ctx, ticket, pull); err != nil {
				return nil, fmt.Errorf("record fix attempt: %w", err)
			}
			interrupted, err := w.fixRound(ctx, ticket.ID, pull, state, action.Problems, watched)
			if err != nil {
				return nil, err
			}
			if !interrupted {
				pull.FixRounds++
				if err := p7.UpdateFields(ctx, ticket, pull); err != nil {
					return nil, fmt.Errorf("record fix round: %w", err)
				}
			}
			continue

		case ActionAsk:
			if err := w.askAndApply(ctx, ticket, pull, state, action, watched); err != nil {
				return nil, err
			}
			continue

		case ActionUpdateBranch:
			// No agent. GitHub merges the base into the branch, or refuses.
			err := github.UpdateBranch(ctx, pull.Repo, pull.Pr, state.Head)
			if err != nil && !errors.Is(err, github.ErrHeadMoved) {
				return nil, fmt.Errorf("update branch: %w", err)
			}

		case ActionRequestReview:
			if err := github.RequestReview(ctx, pull.Repo, pull.Pr, pull.Reviewers); err != nil {
				return nil, fmt.Errorf("request review: %w", err)
			}
			pull.RequestedHeads = append(pull.RequestedHeads, state.Head)
			if err := p7.UpdateFields(ctx, ticket, pull); err != nil {
				return nil, fmt.Errorf("record review request: %w", err)
			}
			continue

		case ActionMerge:
			err := github.Merge(ctx, github.MergeRequest{
				Repo:         pull.Repo,
				PullRequest:  pull.Pr,
				Method:       github.Squash,
				MatchHead:    state.Head,
				OperationKey: fmt.Sprintf("merge:%s:%s", ticket.ID, state.Head),
			})
			if err != nil && !errors.Is(err, github.ErrHeadMoved) && !errors.Is(err, github.ErrNotMergeable) {
				return nil, fmt.Errorf("merge pull request: %w", err)
			}

		case ActionWait:
		}

		info := workflow.GetInfo(ctx)
		if info.GetContinueAsNewSuggested() || info.GetTargetWorkerDeploymentVersionChanged() {
			return nil, p7.ContinueAsNew(ctx, ticket)
		}
		// waitForChange is the function of the same name in example-pr-review.md.
		if err := waitForChange(ctx, w.config.Recheck, watched); err != nil {
			return nil, err
		}
	}
}

// fixRound runs one coding session on every open problem and posts its
// replies. It reports interrupted when someone else pushed to the branch
// first. Then nothing was posted, and the next pass starts from their head.
func (w *Workflows) fixRound(
	ctx workflow.Context,
	ticketID string,
	pull *authorv1.PRAuthor,
	state github.AuthorState,
	problems []Problem,
	watched p7.Since,
) (interrupted bool, err error) {
	roundCtx, cancelRound := workflow.WithCancel(ctx)
	defer cancelRound()

	// Cancel the round when anyone but this workflow's own login moves the
	// head. The session's own push arrives here too, and is ignored.
	since := watched
	since.Kinds = github.SupersedingEvents
	workflow.Go(roundCtx, func(watchCtx workflow.Context) {
		for {
			var events []p7.Event
			if err := p7.Watch(watchCtx, since).Get(watchCtx, &events); err != nil {
				return
			}
			for _, event := range events {
				if event.Actor != w.config.AuthorLogin && github.Supersedes(event, state.Head) {
					cancelRound()
					return
				}
				since.Revision = event.Revision
			}
		}
	})

	// The clone is writable. The session may push to this branch and nowhere else.
	clone := p7.GitClone{Repository: pull.Repo, Ref: state.Head, Branch: pull.Branch}
	report, err := p7.Agent[*authorv1.FixReport](roundCtx, p7.AgentSpec{
		Model:    w.config.Coder,
		Image:    pull.Image,
		Prompt:   prompts.Fix(pull, state, problems),
		Inputs:   slices.Concat([]p7.Input{{Source: clone, Destination: WorkDirectory}}, w.config.Guidance),
		Egress:   []p7.EgressRef{github.PushBranch(pull.Repo, pull.Branch)},
		Limits:   []string{"model:" + w.config.Coder.Name, "repo:" + pull.Repo},
		Priority: p7.PriorityNormal,
		Timeout:  2 * time.Hour,
	})
	if err != nil {
		if roundCtx.Err() != nil && ctx.Err() == nil {
			return true, nil
		}
		return false, fmt.Errorf("fix round: %w", err)
	}

	// The workflow posts the replies, each under its own key. The session
	// cannot comment. A thread is resolved only when the reply says so.
	for _, reply := range report.Replies {
		err := github.ReplyToThread(ctx, github.ThreadReply{
			Repo:         pull.Repo,
			PullRequest:  pull.Pr,
			Thread:       reply.Thread,
			Body:         reply.Body,
			Resolve:      reply.Resolve,
			OperationKey: fmt.Sprintf("reply:%s:%s:%d", ticketID, reply.Thread, pull.FixRounds),
		})
		if err != nil {
			return false, fmt.Errorf("reply on thread %s: %w", reply.Thread, err)
		}
	}
	if report.Outcome == authorv1.FixOutcome_NEEDS_PERSON {
		// Mark every problem as out of attempts. The next pass asks a person,
		// with the session's question in the ticket's history.
		for _, problem := range problems {
			pull.Attempts[problem.Key] = MaxAttemptsPerProblem
		}
	}
	return false, nil
}
```

Left out: `askAndApply`, which asks the owners with `p7.Ask` the way `askAboutRoundLimit` does in the reviewer and applies the answer (try again, which adds the problem's key to `cleared`, allow more rounds, or stop following), and `escalationFor`, which is the reviewer's with one clock: review requested and not received.

## Deciding

```go
// Problem is one thing wrong with the pull request that a coding session can fix.
type Problem struct {
	// Key names the problem without naming the head, so it stays the same
	// across pushes. Attempts are counted against it.
	Key  string
	Text string
}

// ActionKind is the one thing the Follow workflow does on a pass.
type ActionKind int

const (
	ActionWait ActionKind = iota
	ActionFix
	ActionAsk
	ActionUpdateBranch
	ActionRequestReview
	ActionMerge
	ActionFinish
)

// Action is what Decide returns.
type Action struct {
	Kind     ActionKind
	Outcome  authorv1.Outcome
	Reason   string
	Problems []Problem
	// ReviewOverdue is set while a requested review has not arrived.
	ReviewOverdue bool
}

// Decide returns what to do for one of our own pull requests.
func Decide(state github.AuthorState, pull *authorv1.PRAuthor) Action {
	switch {
	case state.Merged:
		return Action{Kind: ActionFinish, Outcome: authorv1.Outcome_MERGED, Reason: "merged"}
	case !state.Open:
		return Action{Kind: ActionFinish, Outcome: authorv1.Outcome_CLOSED, Reason: "closed"}
	case state.Draft:
		return Action{Kind: ActionWait, Reason: "waiting: draft"}
	}

	if problems := Problems(state); len(problems) > 0 {
		exhausted := func(problem Problem) bool {
			return pull.Attempts[problem.Key] >= MaxAttemptsPerProblem && !slices.Contains(pull.Cleared, problem.Key)
		}
		switch {
		case slices.ContainsFunc(problems, exhausted):
			return Action{Kind: ActionAsk, Reason: "asking a person: a fix did not clear the problem", Problems: problems}
		case pull.FixRounds >= MaxFixRounds+pull.ExtraRounds:
			return Action{Kind: ActionAsk, Reason: "asking a person: fix round limit reached", Problems: problems}
		}
		return Action{Kind: ActionFix, Reason: fmt.Sprintf("fixing %d problems", len(problems)), Problems: problems}
	}

	approved := func(approver string) bool { return slices.Contains(state.ApprovedBy, approver) }
	switch {
	case state.ChecksPending:
		return Action{Kind: ActionWait, Reason: "waiting: checks running"}
	case len(state.PendingReviewers) > 0:
		return Action{Kind: ActionWait, Reason: "waiting: review requested", ReviewOverdue: true}
	case len(state.ChangesRequestedBy) > 0:
		// Every thread is answered and the change request still stands, so
		// the reviewer has to look again.
		return requestReviewOnce(state, pull, "waiting: changes requested, replies posted")
	case !slices.ContainsFunc(pull.Reviewers, approved):
		return requestReviewOnce(state, pull, "waiting: no approval for this head")
	case state.Mergeable == github.MergeableBehind:
		// Only now, when nothing else is missing. An update makes a new head.
		return Action{Kind: ActionUpdateBranch, Reason: "updating the branch"}
	case state.Mergeable != github.MergeableClean:
		return Action{Kind: ActionWait, Reason: "waiting to merge: GitHub says it is not mergeable"}
	case slices.ContainsFunc(pull.RequiredApprovers, func(approver string) bool { return !approved(approver) }):
		return Action{Kind: ActionWait, Reason: "waiting to merge: required approval missing", ReviewOverdue: true}
	}
	return Action{Kind: ActionMerge, Reason: "merging " + state.Head}
}

// requestReviewOnce asks for review of a head the first time and waits after that.
func requestReviewOnce(state github.AuthorState, pull *authorv1.PRAuthor, waiting string) Action {
	if slices.Contains(pull.RequestedHeads, state.Head) {
		return Action{Kind: ActionWait, Reason: waiting, ReviewOverdue: true}
	}
	return Action{Kind: ActionRequestReview, Reason: "requesting review of " + state.Head}
}

// Problems lists what a coding session should fix, in one round.
func Problems(state github.AuthorState) []Problem {
	var problems []Problem
	for _, thread := range state.UnansweredThreads {
		last := thread.Comments[len(thread.Comments)-1]
		problems = append(problems, Problem{
			// The last comment is in the key, so a new reply from the reviewer
			// on the same thread is a new problem.
			Key:  fmt.Sprintf("thread:%s:%d", thread.ID, last.ID),
			Text: fmt.Sprintf("%s:%d: %s", thread.Path, thread.Line, last.Body),
		})
	}
	if len(state.FailedChecks) > 0 {
		names := make([]string, 0, len(state.FailedChecks))
		for _, check := range state.FailedChecks {
			names = append(names, check.Name)
		}
		slices.Sort(names)
		problems = append(problems, Problem{
			Key:  "checks:" + strings.Join(names, ","),
			Text: "failed checks: " + strings.Join(names, ", "),
		})
	}
	if state.Mergeable == github.MergeableConflicts {
		problems = append(problems, Problem{Key: "conflicts", Text: "the branch conflicts with its base"})
	}
	return problems
}
```

## What this needs from the library

Everything `example-pr-review.md` lists, and three more:

1. **The actor on every event.** `fixRound` tells its own push from a person's by `event.Actor`. Without it the workflow cancels its own fix round when its own push arrives.
2. **A branch on a `p7.GitClone` input.** The clone is checked out on a named branch, so the session can commit and push without setting anything up.
3. **A push grant limited to one branch.** `github.PushBranch` is this repo's egress reference. PDP needs a rule that allows a push to one ref of one repository (plan section 9 says push-ref restrictions are possible, not that they exist).

## Open

- **How this ticket is filed.** The workflow that opened the pull request would file it, as its child or as independent work. Not shown.
- **The first failed check is treated like any other.** A flaky check costs a coding session. Rerunning a failed check once before fixing would be cheaper.
- **The reviewer sees every head the author requests review for.** With a reviewer that takes twenty minutes a round, six fix rounds is two hours of reviews. The limits on both sides bound it, but nothing makes the two sides converge faster.
- **Two agents can agree with each other and be wrong.** Our author and our reviewer are both ours. `required_approvers` is the gate that puts a person in the loop.
- **A merge refusal is not remembered here.** The reviewer example saves a refusal and does not retry until the state changes. This workflow should do the same and does not yet.
- None of this has run. Every `p7`, `github` and `prompts` call is proposed.
