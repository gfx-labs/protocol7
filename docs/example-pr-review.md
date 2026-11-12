# Example: the jobcity PR review flow on protocol7

A port of jobcity's `pr-review-v2` and `review-capture-v2` to the library proposed in `plan.md` section 7. It is a sketch to test the design. Nothing here compiles, because the library does not exist yet. `example-small-review.md` shows the review stage alone.

Source read: `elee1766/jobcity` at `7894440` (`packs/gfx/pack.cue`, `packs/gfx/workflows.star`, `packs/gfx/gh.star`, `packs/automation/events.star`, the step prompts and the review tools under `tools/`).

## The flow today

1. Trigger `review-requests` fires on six GitHub events and every 15 minutes. It lists eligible PRs, skips subjects that already had a run, and starts at most 10 open runs.
2. Step `route` (script, gate pool) adds the eyes reaction and picks the reviewer: Terra for small low-risk diffs, Sol otherwise.
3. Step `review-gate` (script) ends the run as `stale` if the head moved, and reports which runtime evidence the repo uses.
4. Step `review` (agent) reviews. For missing runtime evidence it starts `review-capture-v2` child runs and waits with its step open. It posts one GitHub review and closes with `merge`, `approved`, `changes`, `commented` or `stale`.
5. Step `merge` (agent, gate pool) runs only after `merge`. It rechecks the PR and squash-merges, or closes `held`.

## Where each part goes

| Today | Here |
|---|---|
| Workflow and trigger blocks in `pack.cue`, and the per-repo instance vars | `Register`, and the `Config` value in `main.go` |
| The relay and the six-event trigger | `cmd/gfx-webhooks`, an ordinary HTTP handler in this repo. It starts one `IngestDelivery` workflow per delivery |
| The 15-minute trigger | A timer inside `Workflows.Intake`. One Intake workflow per repo wakes on a webhook or on the timer, whichever comes first |
| `events.star:review_requests` (dedupe by run key, cap of 10) | `Workflows.fileEligibleReviews`. The store refuses a second ticket with the same `Once` key |
| `tools/lib/review-discovery.ts` (247 lines) | `Discover`, an activity. Same rules, ported to Go |
| `route` step and `tools/lib/review-route.ts` (71 lines) | `RiskReasons`, a plain Go function called in the workflow. No step and no sandbox |
| `review-gate` step and `gh.star:evidence_gate` | Three lines in `Workflows.Review` plus `EvidencePlatforms` |
| `pr_review_completed` in `workflows.star` | The body of `Workflows.Review`, read top to bottom |
| `review` step | One session per ticket, so each review starts with an empty conversation |
| `tools/automation/start_review_capture.ts` (217 lines: checks that the caller holds the review step, that the run and subject match, that the platform fits the repo, then checks the run it started) | The reviewer returns capture requests in its typed `Turn`. `Workflows.runCaptures` checks the platform and the attempt limit in Go and starts each capture as a Temporal child. The agent files nothing, so the identity checks go away |
| A head that moves while captures run is noticed at the next step | `runCaptures` waits on the captures and on a head change together, cancels the captures and ends the review as `STALE` |
| `review_capture_start` and the `_CAPTURES` table | `Workflows.Capture` and `EvidencePlatforms` |
| `merge` step, an agent session | `MergeWhenReady`: `github.GetMergeState` and `github.Merge`. No agent. It waits and retries where the agent closed `held` |
| The reviewer posts one GitHub review with `gh` | `github.PostReview` in the workflow, under an operation key. The session can only read GitHub. This is the example's choice, not a rule of the library |
| `jcx image upload` | `PublishEvidence`, an activity in this worker |
| `resources` in `pack.cue` with `bin/emulator-provider` and `bin/ios-provider` | `devices.Leases.Reserve` and `devices.Leases.Release`, activities on the device worker. protocol7 has no resource model, so the lease is this repo's code |
| Pool session limits (`sol_review_sessions` and the rest) | `MaxOpenReviews`, and the capacity of the executors that run the sessions |
| The policy repo checked out beside the code | `SessionSpec.Inputs`: two git directories mounted read-only under `/guidance` |
| `jcx totp` | `p7.Tool` in the device worker. It returns a code for a designated test account, which the plan allows (section 9) |
| `bin/namespace-login` | Not ported. A session is sent no login for the iOS devbox |
| "Read-only: never push" in the prompt | The session's egress grant. PDP refuses a push |
| `jobshop close --outcome <word>` | The agent submits a `Turn` message. An unknown word fails the turn |

Unchanged: the policy documents, the discovery rules, the risk rules, and most of the prompt text.

## Packages

Only `p7` is protocol7. The rest is code in this repo.

| Package | What it is |
|---|---|
| `p7` | The proposed protocol7 library |
| `github` | This repo's GitHub package. protocol7 has no GitHub types. Each function called with a workflow context runs one activity that calls GitHub through PDP, and turns a known failure into a Go error such as `github.ErrHeadMoved`. `github.PullRequest` builds the provider-neutral `p7.ObjectRef` for a pull request |
| `devices` | Device leases and test-account tools. Runs only in the device worker |
| `prompts` | Functions that build prompt text from typed values |
| `reviewv1` | Generated from the protobuf below |

## Types

```proto
// Ticket fields for kind pr_review. The last three are copied from the repo's
// configuration at intake, so a ticket describes its own review.
message PRReview {
  string repo = 1;
  int32 pr = 2;
  string head = 3;
  int64 request_comment = 4; // the comment that asked, 0 when there is none
  string issue = 5;          // Linear key, may be empty
  Evidence evidence = 6;     // NONE, ANDROID or BROWSER
  bool merge_others = 7;
  string image = 8;
}

// What the reviewer submits at the end of each turn.
message Turn {
  Outcome outcome = 1; // WAITING, MERGE, APPROVED, CHANGES, COMMENTED or STALE
  string body = 2;                      // the review text, naming each evidence file where it belongs
  repeated LineComment comments = 3;    // path, line and text
  repeated string evidence = 4;         // names of the files the session saved as artifacts
  repeated CaptureRequest captures = 5; // set with WAITING: the captures the reviewer needs
}

// The result stored on the pr_review ticket. Adds HELD and MERGED.
message Result {
  Outcome outcome = 1;
  HeldReason held = 2; // set with HELD: HEAD_MOVED, APPROVAL_MISSING, CHECKS, BLOCKING_REVIEW or OPEN_THREADS
}

// Ticket fields for kind review_capture. The reviewer fills in platform and
// criteria. The workflow adds repo and head, so a capture describes its own work.
message CaptureRequest {
  Platform platform = 1; // ANDROID, IOS or BROWSER
  string criteria = 2;
  string repo = 3;
  string head = 4;
}

message CaptureReport {
  CaptureResult result = 1; // PASS, FAIL, STALE, REFUSED or ERROR
  string report = 2;
  repeated string evidence = 3; // artifact names the reviewer can put in its review
}
```

## Workflows

`Review` and `Capture` are ticket workflows. `IngestDelivery` and `Intake` are plain Temporal workflows with no ticket.

```go
// Package prreview reviews pull requests that other people wrote.
package prreview

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"time"

	p7 "github.com/gfx-labs/protocol7"
	"go.temporal.io/api/enums/v1"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"

	"example.com/gfx/devices"
	reviewv1 "example.com/gfx/gen/review/v1"
	"example.com/gfx/github"
	"example.com/gfx/prompts"
)

const (
	// MaxCaptureAttempts is the number of captures one review may start per platform.
	MaxCaptureAttempts = 2
	// IntakeWakeSignal tells a repo's Intake workflow that one of its pull requests changed.
	IntakeWakeSignal = "pull-request-activity"
	// IntakePassesPerRun bounds the history of one Intake run before it continues as new.
	IntakePassesPerRun = 200
)

// Workflow type names. They are registered explicitly, so renaming a Go method
// does not change the type that running executions and other binaries refer to.
const (
	// IngestDeliveryWorkflow is started by the webhook receiver.
	IngestDeliveryWorkflow = "prreview.IngestDelivery"
	// IntakeWorkflow is started by wakeIntake and continued as new by Intake.
	IntakeWorkflow = "prreview.Intake"
)

// Config is everything that differs between installations.
type Config struct {
	// TaskQueue is the queue this worker polls. Intake workflows are started on it.
	TaskQueue string
	// AgentLogin is the GitHub login reviews are posted as. Its own PRs are never merged here.
	AgentLogin       string
	MaxOpenReviews   int
	HighRiskReviewer p7.Model
	LowRiskReviewer  p7.Model
	CaptureAgent     p7.Model
	// CaptureImage holds the device tools a capture uses, such as adb.
	CaptureImage string
	// CaptureTools are the tools a capture agent may call. Each is registered in the device worker.
	CaptureTools []string
	// DeviceQueue is the task queue of the device worker, which owns the device leases.
	DeviceQueue string
	// DeviceWait is how long a capture may wait for a free device.
	DeviceWait time.Duration
	// Guidance is the skills and policy documents mounted read-only into every
	// session. A source may name a tag. It is resolved to a commit once, when the
	// session starts, and the commit is recorded on the run.
	Guidance []p7.Input
	// SessionIdleLimit closes a reviewer session that has waited this long.
	SessionIdleLimit time.Duration
	// IntakeInterval is how often a repo is rescanned when no webhook arrives.
	IntakeInterval time.Duration
	// MergeWait is how long an approved pull request may wait for checks and threads.
	MergeWait time.Duration
	// MergeRecheck is how often a waiting merge rereads GitHub when no event arrives.
	MergeRecheck time.Duration
	Repos        []Repo
}

// Repo is one repository whose pull requests are reviewed.
type Repo struct {
	Name        string
	Image       string // the image the reviewer runs the repo's checks in
	Evidence    reviewv1.Evidence
	MergeOthers bool
}

// Candidate is one pull request that Discover found eligible for review.
type Candidate struct {
	PullRequest int32
	Head        string
	// Subject is <pr>@<head>, or <pr>@<head>~<comment> for a new request on a head already reviewed.
	Subject        string
	RequestComment int64
	Issue          string
	Title          string
}

// Workflows holds the configuration that the workflows read.
type Workflows struct {
	config Config
}

// Activities holds what the activities in this package need.
type Activities struct {
	config   Config
	temporal client.Client
}

// Register adds the review workflows and activities to a worker. p7.Worker
// embeds Temporal's worker, so plain workflows and activities register on it too.
func Register(worker *p7.Worker, config Config) {
	workflows := &Workflows{config: config}
	activities := &Activities{config: config, temporal: worker.Client()}
	p7.Register(worker, "pr_review", workflows.Review)
	p7.Register(worker, "review_capture", workflows.Capture)
	worker.RegisterWorkflowWithOptions(workflows.IngestDelivery, workflow.RegisterOptions{Name: IngestDeliveryWorkflow})
	worker.RegisterWorkflowWithOptions(workflows.Intake, workflow.RegisterOptions{Name: IntakeWorkflow})
	worker.RegisterActivity(activities.RecordDelivery)
	worker.RegisterActivity(activities.WakeIntake)
	worker.RegisterActivity(Discover)
	worker.RegisterActivity(PublishEvidence)
}

// StartIntakes makes sure every configured repo has a running Intake workflow.
// The worker calls it at startup, so a repo that receives no webhook is still scanned.
func StartIntakes(ctx context.Context, temporalClient client.Client, config Config) error {
	for _, repo := range config.Repos {
		if err := wakeIntake(ctx, temporalClient, config.TaskQueue, repo); err != nil {
			return err
		}
	}
	return nil
}

// wakeIntake signals the repo's Intake workflow, and starts it first if it is not running.
func wakeIntake(ctx context.Context, temporalClient client.Client, taskQueue string, repo Repo) error {
	workflowID := "review-intake/" + repo.Name
	options := client.StartWorkflowOptions{ID: workflowID, TaskQueue: taskQueue}
	_, err := temporalClient.SignalWithStartWorkflow(ctx, workflowID, IntakeWakeSignal, nil, options, IntakeWorkflow, repo)
	if err != nil {
		return fmt.Errorf("wake intake for %s: %w", repo.Name, err)
	}
	return nil
}

// WakeIntake is wakeIntake as an activity, because a workflow cannot signal-with-start.
func (a *Activities) WakeIntake(ctx context.Context, repo Repo) error {
	return wakeIntake(ctx, a.temporal, a.config.TaskQueue, repo)
}

// IngestDelivery handles one GitHub webhook delivery. The receiver starts it
// with the delivery as the workflow ID and answers GitHub once Temporal has
// accepted the start. The receiver passes parsed fields, not the raw body.
func (w *Workflows) IngestDelivery(ctx workflow.Context, delivery github.Delivery) error {
	ctx = workflow.WithActivityOptions(ctx, workflow.ActivityOptions{StartToCloseTimeout: time.Minute})

	// RecordDelivery appends the change to the pull request's timeline through
	// ticketd. The timeline entry is keyed by the delivery, so a redelivery
	// after Temporal has dropped this workflow's history appends nothing.
	var activities *Activities
	if err := workflow.ExecuteActivity(ctx, activities.RecordDelivery, delivery).Get(ctx, nil); err != nil {
		return fmt.Errorf("record delivery %s: %w", delivery.ID, err)
	}
	for _, repo := range w.config.Repos {
		if repo.Name != delivery.Repo {
			continue
		}
		if err := workflow.ExecuteActivity(ctx, activities.WakeIntake, repo).Get(ctx, nil); err != nil {
			return fmt.Errorf("wake intake after delivery %s: %w", delivery.ID, err)
		}
	}
	return nil
}

// Intake files pr_review tickets for one repo. It is one long-lived workflow
// per repo, so two scans of a repo never run at once and the open-review cap holds.
func (w *Workflows) Intake(ctx workflow.Context, repo Repo) error {
	logger := workflow.GetLogger(ctx)
	wake := workflow.GetSignalChannel(ctx, IntakeWakeSignal)

	for pass := 0; pass < IntakePassesPerRun; pass++ {
		// A failed pass is logged, not returned. Returning would stop this
		// repo's intake until the next webhook started it again.
		if err := w.fileEligibleReviews(ctx, repo); err != nil {
			logger.Warn("intake pass failed", "repo", repo.Name, "error", err)
		}

		// Sleep until a webhook arrives or the interval passes.
		timerCtx, cancelTimer := workflow.WithCancel(ctx)
		selector := workflow.NewSelector(ctx)
		selector.AddReceive(wake, func(channel workflow.ReceiveChannel, more bool) {
			channel.Receive(ctx, nil)
		})
		selector.AddFuture(workflow.NewTimer(timerCtx, w.config.IntakeInterval), func(timer workflow.Future) {})
		selector.Select(ctx)
		cancelTimer()
		if ctx.Err() != nil {
			return ctx.Err()
		}
		// Webhooks that arrived together become one pass.
		for wake.ReceiveAsync(nil) {
		}
	}

	// The next run scans first, so a wake signal that arrives during the switch
	// loses nothing. A pinned workflow moves to a new build only here.
	options := workflow.ContinueAsNewErrorOptions{}
	if workflow.GetInfo(ctx).GetTargetWorkerDeploymentVersionChanged() {
		options.InitialVersioningBehavior = workflow.ContinueAsNewVersioningBehaviorAutoUpgrade
	}
	return workflow.NewContinueAsNewErrorWithOptions(ctx, options, IntakeWorkflow, repo)
}

// fileEligibleReviews files a pr_review ticket for each eligible pull request.
// It ignores what any event said and lists what is eligible now.
func (w *Workflows) fileEligibleReviews(ctx workflow.Context, repo Repo) error {
	activityCtx := workflow.WithActivityOptions(ctx, workflow.ActivityOptions{StartToCloseTimeout: 2 * time.Minute})
	var candidates []Candidate
	if err := workflow.ExecuteActivity(activityCtx, Discover, repo.Name).Get(ctx, &candidates); err != nil {
		return fmt.Errorf("discover reviews in %s: %w", repo.Name, err)
	}
	open, err := p7.Count(ctx, p7.Query{Kind: "pr_review", Status: p7.Open, Where: p7.Eq("repo", repo.Name)})
	if err != nil {
		return fmt.Errorf("count open reviews in %s: %w", repo.Name, err)
	}
	for _, candidate := range candidates {
		if open >= w.config.MaxOpenReviews {
			break
		}
		filed, err := p7.File(ctx, p7.NewTicket{
			Kind:    "pr_review",
			Once:    "review:" + repo.Name + "#" + candidate.Subject,
			Title:   candidate.Title,
			Objects: []p7.ObjectRef{github.PullRequest(repo.Name, candidate.PullRequest)},
			Fields: &reviewv1.PRReview{
				Repo:           repo.Name,
				Pr:             candidate.PullRequest,
				Head:           candidate.Head,
				RequestComment: candidate.RequestComment,
				Issue:          candidate.Issue,
				Evidence:       repo.Evidence,
				MergeOthers:    repo.MergeOthers,
				Image:          repo.Image,
			},
		})
		if err != nil {
			return fmt.Errorf("file review for %s#%d: %w", repo.Name, candidate.PullRequest, err)
		}
		if filed {
			open++
		}
	}
	return nil
}

// Review handles one pr_review ticket: route, review, post, then merge when allowed.
func (w *Workflows) Review(
	ctx workflow.Context,
	ticket p7.Ticket[*reviewv1.PRReview],
) (result *reviewv1.Result, err error) {
	review := ticket.Fields
	pullRequestRef := github.PullRequest(review.Repo, review.Pr)

	// Read the timeline position before reading GitHub. Every wait below asks
	// for changes after this position, so a change that lands between a read
	// and a wait still wakes the workflow.
	object, err := p7.GetObject(ctx, pullRequestRef)
	if err != nil {
		return nil, fmt.Errorf("read pull request object: %w", err)
	}
	cursor := object.Sequence

	// Route. Was a script step in the gate pool.
	MarkSeen(ctx, review)
	pullRequest, err := github.GetPullRequest(ctx, review.Repo, review.Pr)
	if err != nil {
		return nil, fmt.Errorf("read pull request: %w", err)
	}
	if !pullRequest.Open || pullRequest.Head != review.Head {
		return &reviewv1.Result{Outcome: reviewv1.Outcome_STALE}, nil
	}
	files, err := github.ListFiles(ctx, review.Repo, review.Pr)
	if err != nil {
		return nil, fmt.Errorf("list changed files: %w", err)
	}
	reviewer := w.config.LowRiskReviewer
	if len(RiskReasons(files, pullRequest.Labels)) > 0 {
		reviewer = w.config.HighRiskReviewer
	}

	// Review. One session, kept open while captures run. It can read GitHub and cannot write to it.
	session, err := p7.OpenSession(ctx, p7.SessionSpec{
		Model:     reviewer,
		Image:     review.Image,
		Repos:     []p7.Repo{{Name: review.Repo, Ref: review.Head}},
		Inputs:    w.config.Guidance,
		Egress:    []p7.EgressRef{github.ReadOnly(review.Repo)},
		IdleLimit: w.config.SessionIdleLimit,
	})
	if err != nil {
		return nil, fmt.Errorf("open reviewer: %w", err)
	}
	defer func() {
		err = errors.Join(err, closeSession(ctx, session))
	}()

	platforms := EvidencePlatforms(review.Evidence, files, pullRequest.Labels)
	turn, err := p7.Prompt[*reviewv1.Turn](ctx, session, prompts.Review(review, platforms))
	if err != nil {
		return nil, fmt.Errorf("review turn: %w", err)
	}
	captureAttempts := map[reviewv1.Platform]int{}
	for turn.Outcome == reviewv1.Outcome_WAITING {
		if len(turn.Captures) == 0 {
			return nil, temporal.NewNonRetryableApplicationError(
				"reviewer is waiting and requested no capture", "InvalidTurn", nil)
		}
		round, err := w.runCaptures(ctx, ticket.ID, review, turn.Captures, platforms, captureAttempts, cursor)
		if err != nil {
			return nil, err
		}
		if round.HeadMoved {
			return &reviewv1.Result{Outcome: reviewv1.Outcome_STALE}, nil
		}
		turn, err = p7.Prompt[*reviewv1.Turn](ctx, session, prompts.CaptureReports(round.Reports))
		if err != nil {
			return nil, fmt.Errorf("review turn after captures: %w", err)
		}
	}
	if turn.Outcome == reviewv1.Outcome_STALE {
		return &reviewv1.Result{Outcome: reviewv1.Outcome_STALE}, nil
	}
	// The pod should not stay up while the merge waits.
	if err := closeSession(ctx, session); err != nil {
		return nil, fmt.Errorf("close reviewer: %w", err)
	}

	// Post. The reviewer used to do this itself with gh. PostReview names the
	// head it reviewed, and GitHub refuses the review if the head has moved.
	// The operation key stops a second review when Temporal runs the activity twice.
	activityCtx := workflow.WithActivityOptions(ctx, workflow.ActivityOptions{StartToCloseTimeout: 5 * time.Minute})
	var body string
	err = workflow.ExecuteActivity(activityCtx, PublishEvidence, EvidenceRequest{
		Repo:        review.Repo,
		PullRequest: review.Pr,
		Head:        review.Head,
		Body:        turn.Body,
		Artifacts:   turn.Evidence,
	}).Get(ctx, &body)
	if err != nil {
		return nil, fmt.Errorf("publish evidence: %w", err)
	}
	err = github.PostReview(ctx, github.Review{
		Repo:         review.Repo,
		PullRequest:  review.Pr,
		Head:         review.Head,
		Event:        ReviewEvent(turn.Outcome),
		Body:         body,
		Comments:     LineComments(turn.Comments),
		OperationKey: "review:" + ticket.ID,
	})
	if errors.Is(err, github.ErrHeadMoved) {
		return &reviewv1.Result{Outcome: reviewv1.Outcome_STALE}, nil
	}
	if err != nil {
		return nil, fmt.Errorf("post review: %w", err)
	}
	if turn.Outcome != reviewv1.Outcome_MERGE {
		return &reviewv1.Result{Outcome: turn.Outcome}, nil
	}

	// Merge. Was a second agent session in the gate pool.
	if !review.MergeOthers || pullRequest.Author == w.config.AgentLogin {
		return &reviewv1.Result{Outcome: reviewv1.Outcome_APPROVED}, nil
	}
	return w.MergeWhenReady(ctx, ticket.ID, review, cursor)
}

// closeSession closes a session even when the workflow was cancelled. Closing
// a closed session does nothing.
func closeSession(ctx workflow.Context, session *p7.Session) error {
	cleanupCtx, cancel := workflow.NewDisconnectedContext(ctx)
	defer cancel()
	return session.Close(cleanupCtx)
}

// captureRound is the result of one set of captures the reviewer asked for.
type captureRound struct {
	// Reports has one report per request, in the order the reviewer asked.
	Reports []*reviewv1.CaptureReport
	// HeadMoved is set when the pull request head changed before the captures finished.
	HeadMoved bool
}

// runCaptures starts the captures the reviewer asked for and waits until all
// of them finish or the pull request head moves, whichever comes first.
func (w *Workflows) runCaptures(
	ctx workflow.Context,
	reviewTicket string,
	review *reviewv1.PRReview,
	requests []*reviewv1.CaptureRequest,
	allowed []reviewv1.Platform,
	attempts map[reviewv1.Platform]int,
	cursor int64,
) (captureRound, error) {
	// Leaving this function cancels the head watch and any capture still running.
	captureCtx, cancelCaptures := workflow.WithCancel(ctx)
	defer cancelCaptures()

	round := captureRound{Reports: make([]*reviewv1.CaptureReport, len(requests))}
	selector := workflow.NewSelector(ctx)
	pending := 0
	for index, request := range requests {
		attempts[request.Platform]++
		attempt := attempts[request.Platform]
		if !slices.Contains(allowed, request.Platform) || attempt > MaxCaptureAttempts {
			round.Reports[index] = &reviewv1.CaptureReport{Result: reviewv1.CaptureResult_REFUSED}
			continue
		}
		capture, err := p7.Child(captureCtx, p7.NewTicket{
			Kind:  "review_capture",
			Once:  fmt.Sprintf("capture:%s:%s:%d", reviewTicket, request.Platform, attempt),
			Title: "Capture on " + request.Platform.String(),
			Fields: &reviewv1.CaptureRequest{
				Platform: request.Platform,
				Criteria: request.Criteria,
				Repo:     review.Repo,
				Head:     review.Head,
			},
		}, p7.ChildOptions{ParentClosePolicy: enums.PARENT_CLOSE_POLICY_REQUEST_CANCEL})
		if err != nil {
			return captureRound{}, fmt.Errorf("start %s capture: %w", request.Platform, err)
		}
		pending++
		selector.AddFuture(capture, func(finished workflow.Future) {
			pending--
			report := &reviewv1.CaptureReport{}
			if err := finished.Get(ctx, report); err != nil {
				// A capture that broke is reported to the reviewer, which
				// decides what the missing evidence means for the review.
				report = &reviewv1.CaptureReport{Result: reviewv1.CaptureResult_ERROR, Report: err.Error()}
			}
			round.Reports[index] = report
		})
	}

	// If this watch fails the captures still finish. PostReview checks the head again.
	headChange := p7.NextEvent(captureCtx, github.PullRequest(review.Repo, review.Pr), github.HeadChanged, cursor)
	selector.AddFuture(headChange, func(changed workflow.Future) {
		if err := changed.Get(ctx, nil); err == nil {
			round.HeadMoved = true
		}
	})

	for pending > 0 && !round.HeadMoved {
		selector.Select(ctx)
		if ctx.Err() != nil {
			return captureRound{}, ctx.Err()
		}
	}
	return round, nil
}

// MergeWhenReady merges an approved pull request once nothing holds it. It
// wakes when a check, review or thread changes, and rereads GitHub on a timer
// in case a webhook was missed. It stops when the head moves or MergeWait has passed.
func (w *Workflows) MergeWhenReady(
	ctx workflow.Context,
	reviewTicket string,
	review *reviewv1.PRReview,
	cursor int64,
) (*reviewv1.Result, error) {
	deadline := workflow.Now(ctx).Add(w.config.MergeWait)
	pullRequestRef := github.PullRequest(review.Repo, review.Pr)
	for {
		state, err := github.GetMergeState(ctx, review.Repo, review.Pr)
		if err != nil {
			return nil, fmt.Errorf("read merge state: %w", err)
		}
		reason := WhyHeld(state, review.Head)
		if reason == reviewv1.HeldReason_NOT_HELD {
			err = github.Merge(ctx, github.MergeRequest{
				Repo:         review.Repo,
				PullRequest:  review.Pr,
				Method:       github.Squash,
				MatchHead:    review.Head,
				OperationKey: "merge:" + reviewTicket,
			})
			if errors.Is(err, github.ErrHeadMoved) {
				return &reviewv1.Result{Outcome: reviewv1.Outcome_HELD, Held: reviewv1.HeldReason_HEAD_MOVED}, nil
			}
			if err != nil {
				return nil, fmt.Errorf("merge pull request: %w", err)
			}
			return &reviewv1.Result{Outcome: reviewv1.Outcome_MERGED}, nil
		}
		remaining := deadline.Sub(workflow.Now(ctx))
		if reason == reviewv1.HeldReason_HEAD_MOVED || remaining <= 0 {
			return &reviewv1.Result{Outcome: reviewv1.Outcome_HELD, Held: reason}, nil
		}

		// Wait for the next change to the pull request or for the recheck
		// timer, then read GitHub again.
		waitCtx, cancelWait := workflow.WithCancel(ctx)
		var waitErr error
		selector := workflow.NewSelector(ctx)
		blockerChange := p7.NextEvent(waitCtx, pullRequestRef, github.MergeBlockerEvents, cursor)
		selector.AddFuture(blockerChange, func(changed workflow.Future) {
			var event p7.Event
			if waitErr = changed.Get(ctx, &event); waitErr == nil {
				cursor = event.Sequence
			}
		})
		recheck := workflow.NewTimer(waitCtx, min(remaining, w.config.MergeRecheck))
		selector.AddFuture(recheck, func(fired workflow.Future) {
			waitErr = fired.Get(ctx, nil)
		})
		selector.Select(ctx)
		cancelWait()
		if waitErr != nil {
			return nil, fmt.Errorf("wait for merge blockers: %w", waitErr)
		}
	}
}

// Capture handles one review_capture ticket that Review started.
func (w *Workflows) Capture(
	ctx workflow.Context,
	ticket p7.Ticket[*reviewv1.CaptureRequest],
) (report *reviewv1.CaptureReport, err error) {
	request := ticket.Fields

	// Hold a device. The lease is this repo's own reservation, kept by the
	// device worker. Reserve waits for a free device and heartbeats while it
	// waits. It is keyed by the holder, so a retry returns the same lease.
	reserveCtx := workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		TaskQueue:              w.config.DeviceQueue,
		ScheduleToCloseTimeout: w.config.DeviceWait,
		HeartbeatTimeout:       time.Minute,
	})
	// A nil receiver only names the activity. The device worker runs it.
	var leases *devices.Leases
	var lease devices.Lease
	err = workflow.ExecuteActivity(reserveCtx, leases.Reserve, devices.ReserveRequest{
		Platform: request.Platform,
		Holder:   ticket.ID,
	}).Get(ctx, &lease)
	if err != nil {
		return nil, fmt.Errorf("reserve %s device: %w", request.Platform, err)
	}
	defer func() {
		releaseCtx, cancel := workflow.NewDisconnectedContext(ctx)
		defer cancel()
		releaseCtx = workflow.WithActivityOptions(releaseCtx, workflow.ActivityOptions{
			TaskQueue:           w.config.DeviceQueue,
			StartToCloseTimeout: time.Minute,
		})
		released := workflow.ExecuteActivity(releaseCtx, leases.Release, lease.ID)
		err = errors.Join(err, released.Get(releaseCtx, nil))
	}()

	// The session runs on the host that has the device. Placement picks the
	// core worker's queue for that host.
	report, err = p7.Agent[*reviewv1.CaptureReport](ctx, p7.AgentSpec{
		Model:       w.config.CaptureAgent,
		Image:       w.config.CaptureImage,
		Prompt:      prompts.Capture(request),
		Repos:       []p7.Repo{{Name: request.Repo, Ref: request.Head}},
		Inputs:      w.config.Guidance,
		Placement:   p7.Placement{Backend: p7.BackendHost, Host: lease.Host},
		Environment: lease.Environment,
		Tools:       w.config.CaptureTools,
		Timeout:     time.Hour,
	})
	if err != nil {
		return nil, fmt.Errorf("capture on %s: %w", lease.Host, err)
	}
	return report, nil
}
```

## Helpers

Plain Go with no library calls, apart from the two GitHub calls in `MarkSeen`.

```go
// MarkSeen adds the eyes reaction to the review request. A failure is logged
// and does not stop the review, as today.
func MarkSeen(ctx workflow.Context, review *reviewv1.PRReview) {
	var err error
	if review.RequestComment != 0 {
		err = github.ReactToComment(ctx, review.Repo, review.RequestComment, github.ReactionEyes)
	} else {
		err = github.ReactToPullRequest(ctx, review.Repo, review.Pr, github.ReactionEyes)
	}
	if err != nil {
		workflow.GetLogger(ctx).Warn("eyes reaction failed", "error", err)
	}
}

const (
	LowRiskMaxLines = 600
	LowRiskMaxFiles = 30
)

// RiskReasons returns why a diff needs the high-risk reviewer. It returns nil
// for a small low-risk diff. Ported from tools/lib/review-route.ts.
func RiskReasons(files []github.File, labels []string) []string {
	var reasons []string
	fileCount, lineCount := 0, 0
	for _, file := range files {
		if IsLockfile(file.Path) {
			continue
		}
		fileCount++
		lineCount += file.Additions + file.Deletions
		if IsSensitivePath(file.Path) {
			reasons = append(reasons, "sensitive path: "+file.Path)
		}
	}
	if fileCount == 0 {
		reasons = append(reasons, "no reviewable files")
	}
	if lineCount > LowRiskMaxLines {
		reasons = append(reasons, fmt.Sprintf("%d changed lines > %d", lineCount, LowRiskMaxLines))
	}
	if fileCount > LowRiskMaxFiles {
		reasons = append(reasons, fmt.Sprintf("%d files > %d", fileCount, LowRiskMaxFiles))
	}
	for _, label := range labels {
		if IsSensitiveLabel(label) {
			reasons = append(reasons, "label: "+label)
		}
	}
	return reasons
}

// EvidencePlatforms lists the platforms a capture may run on for this pull request.
func EvidencePlatforms(evidence reviewv1.Evidence, files []github.File, labels []string) []reviewv1.Platform {
	switch evidence {
	case reviewv1.Evidence_BROWSER:
		return []reviewv1.Platform{reviewv1.Platform_BROWSER}
	case reviewv1.Evidence_ANDROID:
		platforms := []reviewv1.Platform{reviewv1.Platform_ANDROID}
		touchesIOS := slices.ContainsFunc(files, func(file github.File) bool { return IsIOSPath(file.Path) })
		if touchesIOS || slices.Contains(labels, "evidence:ios") {
			platforms = append(platforms, reviewv1.Platform_IOS)
		}
		return platforms
	default:
		return nil
	}
}

// ReviewEvent maps the reviewer's outcome to the kind of GitHub review to post.
func ReviewEvent(outcome reviewv1.Outcome) github.ReviewEvent {
	switch outcome {
	case reviewv1.Outcome_MERGE, reviewv1.Outcome_APPROVED:
		return github.Approve
	case reviewv1.Outcome_CHANGES:
		return github.RequestChanges
	default:
		return github.Comment
	}
}

// WhyHeld returns what stops a merge of this head, or NOT_HELD. These are the
// four preconditions the merge agent's prompt listed.
func WhyHeld(state github.MergeState, head string) reviewv1.HeldReason {
	switch {
	case state.Head != head:
		return reviewv1.HeldReason_HEAD_MOVED
	case state.ApprovedHead != head:
		return reviewv1.HeldReason_APPROVAL_MISSING
	case !state.ChecksGreen:
		return reviewv1.HeldReason_CHECKS
	case state.ChangesRequested:
		return reviewv1.HeldReason_BLOCKING_REVIEW
	case state.OpenThreads > 0:
		return reviewv1.HeldReason_OPEN_THREADS
	default:
		return reviewv1.HeldReason_NOT_HELD
	}
}
```

Left out:
- `Discover`, and the four path and label matchers `IsLockfile`, `IsSensitivePath`, `IsSensitiveLabel` and `IsIOSPath`. Each matcher is a table of regular expressions copied from `review-route.ts` and `gh.star`.
- `Activities.RecordDelivery`. It writes the pull request as a p7 external object (source `github`, kind `pull_request`, with this repo's schema) and appends one timeline entry keyed by the delivery.
- `PublishEvidence` and its `EvidenceRequest`. It is an activity that uploads the named artifacts to the image host and returns the review body with their links in place. It replaces `jcx image upload`. If Temporal runs it twice the files are uploaded twice, and the review uses the links from the run that finished.
- `LineComments`, which copies the reviewer's line comments into the GitHub package's type.
- The `github`, `prompts` and `devices` packages.

## The binaries

Three binaries from this repo. Image names, tags, paths, host names, queue names and the time limits are placeholders. A core `p7-worker` also runs on the device host and polls `local-<host>`, so sessions placed there run beside the devices.

The workflow worker holds no upstream credentials. Its GitHub activities go through PDP.

```go
// cmd/gfx-workflows

// buildID is set at link time. Worker Versioning pins each review to the build that started it.
var buildID string

func main() {
	worker, err := p7.NewWorker(p7.WorkerConfig{
		TaskQueue:         "gfx",
		Deployment:        "gfx-workflows",
		BuildID:           buildID,
		TemporalAddress:   os.Getenv("TEMPORAL_ADDRESS"),
		TemporalNamespace: os.Getenv("TEMPORAL_NAMESPACE"),
		TicketdURL:        os.Getenv("P7_TICKETD_URL"),
	})
	if err != nil {
		log.Fatalf("create worker: %v", err)
	}

	config := prreview.Config{
		TaskQueue:        "gfx",
		AgentLogin:       "elee-agent[bot]",
		MaxOpenReviews:   10,
		HighRiskReviewer: p7.Model{Name: "openai-oauth:gpt-6.1-sol", Effort: "high"},
		LowRiskReviewer:  p7.Model{Name: "openai-oauth:gpt-6.1-terra", Effort: "high"},
		CaptureAgent:     p7.Model{Name: "claude-sonnet-5-5", Effort: "medium"},
		CaptureImage:     "review-capture@sha256:...",
		CaptureTools:     []string{"qa_authenticator_code"},
		DeviceQueue:      "gfx-devices",
		DeviceWait:       2 * time.Hour,
		Guidance: []p7.Input{
			{
				Source:      p7.GitTree{Repository: "elee1766/jobcity", Ref: "guidance-v1", Path: "docs/policy"},
				Destination: "/guidance/policy",
				ReadOnly:    true,
			},
			{
				Source:      p7.GitTree{Repository: "elee1766/jobcity", Ref: "guidance-v1", Path: "dot-agents/skills"},
				Destination: "/guidance/skills",
				ReadOnly:    true,
			},
		},
		SessionIdleLimit: 2 * time.Hour,
		IntakeInterval:   15 * time.Minute,
		MergeWait:        24 * time.Hour,
		MergeRecheck:     10 * time.Minute,
		Repos: []prreview.Repo{
			{Name: "oku-trade/retail", Image: "review-retail@sha256:...", Evidence: reviewv1.Evidence_ANDROID, MergeOthers: true},
			{Name: "oku-trade/trade", Image: "review-trade@sha256:...", Evidence: reviewv1.Evidence_BROWSER},
			{Name: "oku-trade/account", Image: "review-account@sha256:...", MergeOthers: true},
			// The other five repos with reviews enabled are left out here.
		},
	}
	prreview.Register(worker, config)

	if err := prreview.StartIntakes(context.Background(), worker.Client(), config); err != nil {
		log.Fatalf("start intakes: %v", err)
	}
	if err := worker.Run(); err != nil {
		log.Fatalf("run worker: %v", err)
	}
}
```

The receiver is a Temporal client and not a worker. It answers GitHub once Temporal has accepted the start, and never waits for the review. The webhook secret is a mounted secret on this binary only.

```go
// cmd/gfx-webhooks

// MaxDeliveryBytes is the largest webhook body the receiver reads.
const MaxDeliveryBytes = 5 << 20

// Receiver turns each relevant GitHub delivery into one IngestDelivery workflow.
type Receiver struct {
	temporal      client.Client
	taskQueue     string
	webhookSecret []byte
}

func (r *Receiver) ServeHTTP(response http.ResponseWriter, request *http.Request) {
	body, err := io.ReadAll(http.MaxBytesReader(response, request.Body, MaxDeliveryBytes))
	if err != nil {
		http.Error(response, "body too large or unreadable", http.StatusRequestEntityTooLarge)
		return
	}
	if !github.ValidSignature(r.webhookSecret, request.Header.Get("X-Hub-Signature-256"), body) {
		http.Error(response, "bad signature", http.StatusUnauthorized)
		return
	}
	// ParseDelivery keeps the few fields the timeline needs and drops the rest,
	// so the workflow input stays small. It reports false for events this repo ignores.
	delivery, relevant, err := github.ParseDelivery(request.Header, body)
	if err != nil {
		http.Error(response, "unreadable delivery", http.StatusBadRequest)
		return
	}
	if !relevant {
		response.WriteHeader(http.StatusNoContent)
		return
	}

	_, err = r.temporal.ExecuteWorkflow(request.Context(), client.StartWorkflowOptions{
		ID:                       fmt.Sprintf("github-delivery/%d/%s", delivery.Installation, delivery.ID),
		TaskQueue:                r.taskQueue,
		WorkflowIDConflictPolicy: enums.WORKFLOW_ID_CONFLICT_POLICY_USE_EXISTING,
		WorkflowIDReusePolicy:    enums.WORKFLOW_ID_REUSE_POLICY_REJECT_DUPLICATE,
	}, prreview.IngestDeliveryWorkflow, delivery)
	var alreadyStarted *serviceerror.WorkflowExecutionAlreadyStarted
	if err != nil && !errors.As(err, &alreadyStarted) {
		// GitHub redelivers. The same delivery ID gives the same workflow ID.
		http.Error(response, "not accepted, retry", http.StatusServiceUnavailable)
		return
	}
	response.WriteHeader(http.StatusAccepted)
}
```

The device worker runs where the devices are. It owns the leases and the test-account tool. The authenticator seeds are a mounted secret on this worker; an agent gets a code for a designated test account and never a seed.

```go
// cmd/gfx-devices

// buildID is set at link time.
var buildID string

func main() {
	config, err := devices.LoadConfig()
	if err != nil {
		log.Fatalf("load device config: %v", err)
	}
	worker, err := p7.NewWorker(p7.WorkerConfig{
		TaskQueue:         "gfx-devices",
		Deployment:        "gfx-devices",
		BuildID:           buildID,
		TemporalAddress:   os.Getenv("TEMPORAL_ADDRESS"),
		TemporalNamespace: os.Getenv("TEMPORAL_NAMESPACE"),
		TicketdURL:        os.Getenv("P7_TICKETD_URL"),
	})
	if err != nil {
		log.Fatalf("create worker: %v", err)
	}

	// The limits are the ones in jobcity's pack.cue today: five Android
	// emulators, two iOS simulators and four browsers.
	leases, err := devices.NewLeases(config)
	if err != nil {
		log.Fatalf("open device leases: %v", err)
	}
	worker.RegisterActivity(leases.Reserve)
	worker.RegisterActivity(leases.Release)

	accounts := &devices.TestAccounts{SeedDirectory: config.AuthenticatorSeedDirectory}
	p7.Tool(worker, "qa_authenticator_code", accounts.AuthenticatorCode)

	if err := worker.Run(); err != nil {
		log.Fatalf("run worker: %v", err)
	}
}
```

Left out: the `devices` package. `Leases.Reserve` waits for a free device of the platform, boots it with jobcity's provider scripts, and returns the host and the environment a session needs to reach it, such as `ANDROID_SERIAL`. `Leases.Release` resets the device and frees it. A capture workflow that is terminated never runs its release, so `Leases` must also free a lease whose holder ticket has closed. `TestAccounts.AuthenticatorCode` computes a code for a test account from its seed.

## What the port needs from the library

Calls used here that `plan.md` section 7 does not have yet, or has in a different shape:

1. **`p7.NextEvent(ctx, object, kinds, after)` returning a `workflow.Future`.** The plan has `p7.WaitEvent`, which blocks with a timeout. A blocking call cannot sit in a `workflow.Selector` beside capture futures or a timer, and this flow needs both: captures or head change, and blocker change or recheck timer.
2. **`p7.GetObject` and a timeline `Sequence`.** A wait must name the position it starts from. Without it, a change that lands between reading GitHub and starting the wait is missed.
3. **`p7.Child` returning the child's future, with `p7.ChildOptions`.** The plan says the future is retained and the Parent Close Policy is explicit. Here `p7.Child` blocks until the child ticket is persisted and the child is started, then returns a `workflow.ChildWorkflowFuture`.
4. **`SessionSpec.Inputs`, `p7.Input` and `p7.GitTree`.** Agreed in discussion, with a tag resolved to a commit when the session starts. Not in the plan text at the time of writing.
5. **`Placement` and `Environment` on `p7.AgentSpec`,** filled from a lease that this repo's own activity returned.
6. **`p7.NewTicket.Objects`,** the link from a ticket to a provider-neutral external object.

Calls in the plan that this port no longer uses:

- `p7.OnEvent` and `p7.Every`. Webhook intake is this repo's receiver plus `SignalWithStart`, and the 15-minute rescan is a workflow timer. If no other flow needs them, the plan row can go.
- `p7.WaitChildren`, `p7.Parent`, and agents filing child tickets. The reviewer returns typed capture requests and the workflow starts the children.

Open in this port:

- A head that moves during a reviewer turn does not interrupt the turn. `p7.Prompt` blocks, so the selector pattern does not reach it. The reviewer can still answer `STALE`, and `PostReview` refuses a moved head.
- `Review` and `Capture` are pinned to the build that started them. `Intake` moves to a new build only at Continue-As-New, so a fix to intake waits up to `IntakePassesPerRun` passes unless an operator moves it.
- A capture session on a host backend makes no enforced-egress claim (plan section 5).
- iOS captures need a login to the Namespace devbox. Nothing here supplies one.
- None of this has run. The Temporal calls are real API. Every `p7`, `github` and `devices` call is proposed.
