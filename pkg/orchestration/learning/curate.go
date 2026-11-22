package learning

import (
	"fmt"
	"strings"
	"time"

	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"
	"google.golang.org/protobuf/proto"

	"github.com/gfx-labs/protocol7/pkg/domain/navi"
	base "github.com/gfx-labs/protocol7/pkg/orchestration/chat"
)

// The curator turns open learnings into changes without a person accepting
// them. The SDK holds its mechanics and no words:
//
//   - A Temporal schedule per namespace (EnsureSchedule) starts CurateWorkflow
//     every few hours, with an overlap policy that skips a run while one runs.
//   - A pass opens only when at least one open entry was recorded since the
//     last pass, and never while another pass runs (ActivityCurateCheck).
//   - The pass runs one chat of the project, the curator's session, with the
//     repositories and the tools the app gives it. The first message comes from
//     the app's activity (ActivityCurateBrief): the app writes the prompt, its
//     lanes and its tools. The curator settles each entry with LearningDecide
//     (apply, cover or reject), or leaves it open to join a later recurrence.
//   - The pass reads back what became of each entry (ActivityCurateOutcome)
//     and keeps it as its result, with the curator's last message as the
//     pass note.
//
// Lanes (lanes.go) is the mechanism that says whether a change may be merged
// by the curator or needs review or a person. Which paths belong to which lane
// is each app's.
const (
	// CurateWorkflowName is the registered name of the pass.
	CurateWorkflowName = "selfdev.LearningCurate"
	// CurateWorkflowPrefix starts the workflow ID of every pass, and so the
	// Owner of the curator's chat (see IsCuratorChat).
	CurateWorkflowPrefix = "learning-curate-"
	// CurateScheduleID is the ID of the namespace's schedule. Its runs have
	// the IDs CurateScheduleID-<time>.
	CurateScheduleID = "learning-curate"

	// ActivityCurateCheck and ActivityCurateOutcome are the SDK's activities
	// (CurateActivities). ActivityCurateBrief is the app's: it takes a
	// BriefRequest and returns a Brief.
	ActivityCurateCheck   = "LearningCurateCheck"
	ActivityCurateBrief   = "LearningCurateBrief"
	ActivityCurateOutcome = "LearningCurateOutcome"

	// QueryCurate returns the pass's CurateResult so far.
	QueryCurate = "curate"
	// MemoCurate is the memo field that holds the result of a pass that
	// ran. A pass that skipped or failed has none, and so is not the last
	// pass (Plan.Since).
	MemoCurate = "learning_curate_pass"

	// TriggerSchedule is a run of the schedule, which the gate may skip.
	// TriggerManual is one that a person or a tool started, which runs
	// whenever an entry is open and no pass runs.
	TriggerSchedule = "schedule"
	TriggerManual   = "manual"
)

// CurateConfig is how often a pass may run and how much it takes.
type CurateConfig struct {
	// Every is the interval of the schedule. Default 4 hours.
	Every time.Duration `json:"every,omitempty"`
	// MinNew is how many entries must be recorded since the last pass for a
	// scheduled run to open a pass. Default 1.
	MinNew int `json:"min_new,omitempty"`
	// MaxEntries bounds the entries one pass is given, oldest first.
	// Default 30.
	MaxEntries int `json:"max_entries,omitempty"`
	// TurnBudget is how long the curator's chat has for its turn. Default
	// 3 hours: it opens pull requests and waits for their checks.
	TurnBudget time.Duration `json:"turn_budget,omitempty"`
}

// WithDefaults fills the settings that are zero.
func (c CurateConfig) WithDefaults() CurateConfig {
	if c.Every <= 0 {
		c.Every = 4 * time.Hour
	}
	if c.MinNew <= 0 {
		c.MinNew = 1
	}
	if c.MaxEntries <= 0 {
		c.MaxEntries = 30
	}
	if c.TurnBudget <= 0 {
		c.TurnBudget = 3 * time.Hour
	}
	return c
}

// CurateInput is the input of a pass, and the action of the schedule.
type CurateInput struct {
	Config CurateConfig `json:"config"`
	// Session is the curator chat's request but for what the pass sets: the
	// host, backend, models, repositories, scope, project and the rest of
	// what a chat of the project is started with. The app builds it from its
	// own configuration when its worker starts, and hands it to the schedule.
	Session *navi.ChatRequest `json:"session,omitempty"`
	// Trigger is TriggerSchedule (the default) or TriggerManual.
	Trigger string `json:"trigger,omitempty"`
}

// Plan is what the check says of a pass about to start.
type Plan struct {
	// At is when the check read the log.
	At time.Time `json:"at"`
	// Since is the cursor of the last pass that ran, and zero when none did.
	Since time.Time `json:"since,omitzero"`
	// Open are the entries the pass is given, oldest first, at most
	// Config.MaxEntries.
	Open []Entry `json:"open,omitempty"`
	// OpenTotal counts every open entry, and New those recorded after Since.
	OpenTotal int `json:"open_total"`
	New       int `json:"new"`
	// Cursor is where this pass leaves the log: At, or when the entries
	// were cut at MaxEntries, the time the last one given was recorded, so
	// that the rest count as new for the next pass.
	Cursor time.Time `json:"cursor"`
	// Skip is why there is no pass, or empty.
	Skip string `json:"skip,omitempty"`
}

// CheckInput is the input of ActivityCurateCheck.
type CheckInput struct {
	// Pass is the workflow ID of the pass that asks, and Started when it
	// started. A pass that started earlier, and runs, makes this one skip.
	Pass    string       `json:"pass"`
	Started time.Time    `json:"started"`
	Config  CurateConfig `json:"config"`
	Trigger string       `json:"trigger,omitempty"`
}

// BriefRequest is the input of ActivityCurateBrief, which the app answers
// with the curator's first message.
type BriefRequest struct {
	Pass string `json:"pass"`
	// Chat is the ID the curator's chat will have, so that the brief can
	// name its branch.
	Chat    string    `json:"chat"`
	Trigger string    `json:"trigger,omitempty"`
	Since   time.Time `json:"since,omitzero"`
	// Entries are the open entries the pass is given, oldest first. Their
	// text is a model's own words from another chat: the app fences it.
	Entries []Entry `json:"entries"`
	// Left counts the open entries the pass is not given.
	Left int `json:"left,omitempty"`
}

// Brief is what the app's activity returns: a title for the chat and its
// first message.
type Brief struct {
	Title   string `json:"title"`
	Message string `json:"message"`
}

// Settlement is what became of one entry in a pass.
type Settlement struct {
	ID    string `json:"id"`
	Title string `json:"title"`
	// State is Applied, Covered or Rejected for an entry that the pass
	// settled, and Open for one it left, to join a later recurrence.
	State State  `json:"state"`
	PR    string `json:"pr,omitempty"`
	Note  string `json:"note,omitempty"`
}

// CurateResult is what a pass keeps and returns.
type CurateResult struct {
	Pass    string    `json:"pass"`
	Trigger string    `json:"trigger,omitempty"`
	Started time.Time `json:"started"`
	Ended   time.Time `json:"ended,omitzero"`
	// Skipped is why no pass ran, and the other fields are then empty.
	Skipped string `json:"skipped,omitempty"`
	// Cursor is the plan's: entries recorded after it are new to the next
	// pass.
	Cursor time.Time `json:"cursor,omitzero"`
	// Chat is the curator's chat.
	Chat string `json:"chat,omitempty"`
	// Settlements are the entries the pass was given, as it left them.
	Settlements []Settlement `json:"settlements,omitempty"`
	// Note is the curator's last message: the pass note, with its reasons.
	Note string `json:"note,omitempty"`
	// Error is why the pass failed or ran out of time.
	Error string `json:"error,omitempty"`
}

// Count is how many of the pass's entries ended in the state. Open counts
// those it left.
func (r CurateResult) Count(state State) int {
	n := 0
	for _, settled := range r.Settlements {
		if settled.State == state || (state == Open && settled.State.IsOpen()) {
			n++
		}
	}
	return n
}

// OutcomeRequest is the input of ActivityCurateOutcome.
type OutcomeRequest struct {
	IDs []string `json:"ids"`
}

// IsCuratorChat says whether a chat was started by a pass: its Owner is the
// pass's workflow. An app gives such a chat the curator's tools and
// instructions.
func IsCuratorChat(request *base.Request) bool {
	return request != nil && strings.HasPrefix(request.GetOwner(), CurateWorkflowPrefix)
}

var (
	checkOptions = workflow.ActivityOptions{
		TaskQueue: base.TaskQueue, StartToCloseTimeout: time.Minute,
		RetryPolicy: &temporal.RetryPolicy{InitialInterval: 2 * time.Second, BackoffCoefficient: 2, MaximumAttempts: 4},
	}
	// briefOptions are those of the app's brief, which may read more than
	// the log.
	briefOptions = workflow.ActivityOptions{
		TaskQueue: base.TaskQueue, StartToCloseTimeout: 2 * time.Minute,
		RetryPolicy: &temporal.RetryPolicy{InitialInterval: 2 * time.Second, BackoffCoefficient: 2, MaximumAttempts: 3},
	}
)

const (
	// curateGrace is how long a chat that was told to close has to do it,
	// and how long the pass allows its chat beyond the turn budget.
	curateGrace = 30 * time.Minute
)

// CurateWorkflow is one pass. It checks that a pass is due, runs the
// curator's chat for one turn, closes it, and keeps what became of each
// entry. A pass that has nothing to do returns at once with the reason.
func CurateWorkflow(ctx workflow.Context, input CurateInput) (CurateResult, error) {
	config := input.Config.WithDefaults()
	info := workflow.GetInfo(ctx)
	result := CurateResult{Pass: info.WorkflowExecution.ID, Trigger: input.Trigger, Started: workflow.Now(ctx)}
	if result.Trigger == "" {
		result.Trigger = TriggerSchedule
	}
	if err := workflow.SetQueryHandler(ctx, QueryCurate, func() (CurateResult, error) { return result, nil }); err != nil {
		return result, err
	}
	if input.Session == nil {
		return result, temporal.NewNonRetryableApplicationError("the pass has no session: the app gives the schedule the curator chat's request", ErrorRefused, nil)
	}

	var plan Plan
	check := CheckInput{Pass: result.Pass, Started: info.WorkflowStartTime, Config: config, Trigger: result.Trigger}
	if err := workflow.ExecuteActivity(workflow.WithActivityOptions(ctx, checkOptions), ActivityCurateCheck, check).Get(ctx, &plan); err != nil {
		return result, fmt.Errorf("check for a pass: %s", base.Cause(err))
	}
	if plan.Skip != "" {
		result.Skipped, result.Ended = plan.Skip, workflow.Now(ctx)
		return result, nil
	}
	result.Cursor = plan.Cursor

	chatID, err := base.NewID(ctx)
	if err != nil {
		return result, err
	}
	var brief Brief
	request := BriefRequest{Pass: result.Pass, Chat: chatID, Trigger: result.Trigger, Since: plan.Since, Entries: plan.Open, Left: plan.OpenTotal - len(plan.Open)}
	if err := workflow.ExecuteActivity(workflow.WithActivityOptions(ctx, briefOptions), ActivityCurateBrief, request).Get(ctx, &brief); err != nil {
		return result, fmt.Errorf("write the curator's brief: %s", base.Cause(err))
	}
	result.Chat = chatID
	chat := proto.Clone(input.Session).(*base.Request)
	chat.Id, chat.Title, chat.Owner, chat.Handler = chatID, brief.Title, result.Pass, false
	chat.Parent, chat.Depth, chat.Serves, chat.ProjectRequest, chat.RequestTargets = "", 0, nil, nil, nil

	childCtx := workflow.WithChildOptions(ctx, workflow.ChildWorkflowOptions{
		WorkflowID: base.WorkflowID(chatID), TaskQueue: base.TaskQueue,
		// The chat is closed below. It must not die with a pass that ends
		// first.
		ParentClosePolicy:        enumspb.PARENT_CLOSE_POLICY_ABANDON,
		TypedSearchAttributes:    base.SearchAttributes(chat),
		Memo:                     base.Memo(chat),
		WorkflowExecutionTimeout: config.TurnBudget + curateGrace,
	})
	future := workflow.ExecuteChildWorkflow(childCtx, base.WorkflowName, &navi.ChatState{Request: chat, Pending: []*base.Message{{Id: chatID, Text: brief.Message}}})
	if err := future.GetChildWorkflowExecution().Get(ctx, nil); err != nil {
		result.Error = "the curator's chat could not be started: " + base.Cause(err)
		return finishPass(ctx, result, plan, false)
	}

	// One turn: the chat reports when it ends it, or when it ends.
	reports := workflow.GetSignalChannel(ctx, base.SignalReport)
	timerCtx, stopTimer := workflow.WithCancel(ctx)
	selector := workflow.NewSelector(ctx)
	done, timedOut := false, false
	selector.AddReceive(reports, func(channel workflow.ReceiveChannel, _ bool) {
		var report *base.Report
		channel.Receive(ctx, &report)
		if report.GetChatId() != chatID || report.GetStarted() || (!report.GetEnded() && report.GetMessage() == "") {
			return
		}
		done = true
		result.Note, result.Error = strings.TrimSpace(report.GetText()), report.GetError()
	})
	selector.AddFuture(workflow.NewTimer(timerCtx, config.TurnBudget), func(workflow.Future) { timedOut, done = true, true })
	selector.AddFuture(future, func(f workflow.Future) {
		// The chat workflow ended without a report that this loop took.
		if !done {
			done = true
			if err := f.Get(ctx, nil); err != nil {
				result.Error = "the curator's chat failed: " + base.Cause(err)
			}
		}
	})
	for !done {
		selector.Select(ctx)
		if ctx.Err() != nil {
			break
		}
	}
	stopTimer()
	if timedOut && result.Error == "" {
		result.Error = fmt.Sprintf("the curator did not end its turn within %s", config.TurnBudget)
	}
	closeCtx, _ := workflow.NewDisconnectedContext(ctx)
	_ = workflow.SignalExternalWorkflow(closeCtx, base.WorkflowID(chatID), "", base.SignalClose, &base.Action{}).Get(closeCtx, nil)
	return finishPass(closeCtx, result, plan, true)
}

// finishPass reads what became of each entry, and keeps the pass in the memo
// when it ran without an error. A failed pass returns its error, so that it
// shows as one, and is not the last pass: the next run takes its entries
// again.
func finishPass(ctx workflow.Context, result CurateResult, plan Plan, ran bool) (CurateResult, error) {
	if ran {
		ids := make([]string, 0, len(plan.Open))
		for _, entry := range plan.Open {
			ids = append(ids, entry.ID)
		}
		var settlements []Settlement
		if err := workflow.ExecuteActivity(workflow.WithActivityOptions(ctx, checkOptions), ActivityCurateOutcome, OutcomeRequest{IDs: ids}).Get(ctx, &settlements); err != nil {
			if result.Error == "" {
				result.Error = "the outcome of the entries could not be read: " + base.Cause(err)
			}
		}
		result.Settlements = settlements
		if result.Error == "" && result.Note == "" && len(settlements) > 0 && result.Count(Open) == len(settlements) {
			result.Error = "the curator ended its turn with no pass note and settled nothing"
		}
	}
	result.Ended = workflow.Now(ctx)
	if result.Error != "" {
		return result, temporal.NewApplicationError("curator pass failed: "+result.Error, "LearningCurateFailed", result)
	}
	if err := workflow.UpsertMemo(ctx, map[string]any{MemoCurate: result}); err != nil {
		return result, err
	}
	return result, nil
}
