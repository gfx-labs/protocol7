package chat_test

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/converter"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/testsuite"
	"go.temporal.io/sdk/workflow"

	"github.com/gfx-labs/protocol7/pkg/orchestration/chat"
	"github.com/gfx-labs/protocol7/pkg/orchestration/learning"
	"github.com/gfx-labs/protocol7/pkg/orchestration/learning/learningtest"
	"github.com/gfx-labs/protocol7/pkg/p7"
)

// handledEscalation is an escalation workflow with the handler in front of
// it: the hand-off is an activity that records what it was given.
type handledEscalation struct {
	env      *testsuite.TestWorkflowEnvironment
	notices  *[]chat.EscalationNotice
	handoffs []chat.EscalationRequest
	// handoffErr makes the hand-off fail.
	handoffErr error
}

func newHandledEscalation(t *testing.T) *handledEscalation {
	t.Helper()
	var suite testsuite.WorkflowTestSuite
	h := &handledEscalation{env: suite.NewTestWorkflowEnvironment()}
	h.env.RegisterWorkflowWithOptions(chat.EscalationWorkflow, workflow.RegisterOptions{Name: chat.EscalationWorkflowName})
	h.env.RegisterActivityWithOptions(func(_ context.Context, request chat.EscalationRequest) error {
		h.handoffs = append(h.handoffs, request)
		return h.handoffErr
	}, activity.RegisterOptions{Name: chat.ActivityEscalationHandoff})
	h.notices = &[]chat.EscalationNotice{}
	h.env.OnSignalExternalWorkflow(mock.Anything, "chat-c1", "", chat.SignalEscalation, mock.Anything).Return(nil).Run(func(args mock.Arguments) {
		*h.notices = append(*h.notices, args.Get(4).(chat.EscalationNotice))
	})
	return h
}

func (h *handledEscalation) update(t *testing.T, name string, arg any, done func(chat.EscalationState, error)) {
	t.Helper()
	h.env.UpdateWorkflow(name, "", &testsuite.TestUpdateCallback{
		OnAccept: func() {},
		OnReject: func(err error) { done(chat.EscalationState{}, err) },
		OnComplete: func(value any, err error) {
			state, _ := value.(chat.EscalationState)
			done(state, err)
		},
	}, arg)
}

// A new escalation is handed to the handler and waits, open, for it. The handler
// takes it, resolves it with a learning, and the chat gets the answer, said to
// come from the handler. The resolution stays open for a person to override,
// and the override reaches the chat in place of it, once.
func TestTheHandlerResolvesAnEscalationAndAPersonCanOverrideIt(t *testing.T) {
	h := newHandledEscalation(t)
	var refusedNoLearning, refusedAgain, refusedEmptyReview, resolved, reviewed error
	var midState, afterResolve, afterReview chat.EscalationState
	resolution := chat.EscalationDecision{Outcome: chat.OutcomeResolved, Text: "Verify it yourself with go test.", Reasoning: "the issue says to verify", Learning: "l1", Handler: "h1"}
	h.env.RegisterDelayedCallback(func() {
		midState = queryEscalation(t, h.env)
		h.env.SignalWorkflow(chat.SignalEscalationHandling, chat.EscalationClaim{Handler: "h1"})
	}, time.Minute)
	h.env.RegisterDelayedCallback(func() {
		noLearning := resolution
		noLearning.Learning = ""
		h.update(t, chat.UpdateEscalationDecide, noLearning, func(_ chat.EscalationState, err error) { refusedNoLearning = err })
		h.update(t, chat.UpdateEscalationDecide, resolution, func(state chat.EscalationState, err error) { afterResolve, resolved = state, err })
	}, 2*time.Minute)
	h.env.RegisterDelayedCallback(func() {
		h.update(t, chat.UpdateEscalationDecide, resolution, func(_ chat.EscalationState, err error) { refusedAgain = err })
		h.update(t, chat.UpdateEscalationReview, chat.EscalationReview{By: &chat.Person{Login: "alice@example.com"}}, func(_ chat.EscalationState, err error) { refusedEmptyReview = err })
	}, 3*time.Minute)
	h.env.RegisterDelayedCallback(func() {
		h.update(t, chat.UpdateEscalationReview, chat.EscalationReview{Text: " Ask the operator after all ", By: &chat.Person{Login: "alice@example.com"}}, func(state chat.EscalationState, err error) { afterReview, reviewed = state, err })
	}, 4*time.Minute)
	h.env.ExecuteWorkflow(chat.EscalationWorkflowName, newEscalation())
	require.NoError(t, h.env.GetWorkflowError())

	require.Len(t, h.handoffs, 1)
	assert.Equal(t, "e1", h.handoffs[0].ID)
	assert.Equal(t, chat.EscalationOpen, midState.Status)
	require.Error(t, refusedNoLearning)
	assert.Contains(t, refusedNoLearning.Error(), "learning")
	require.NoError(t, resolved)
	assert.Equal(t, chat.EscalationResolved, afterResolve.Status)
	assert.Equal(t, "h1", afterResolve.Handler)
	require.NotNil(t, afterResolve.Decision)
	assert.Equal(t, "the issue says to verify", afterResolve.Decision.Reasoning)
	assert.Equal(t, "l1", afterResolve.Decision.Learning)
	require.Error(t, refusedAgain)
	assert.Contains(t, refusedAgain.Error(), "resolved already")
	require.Error(t, refusedEmptyReview)
	require.NoError(t, reviewed)
	require.NotNil(t, afterReview.Review)
	assert.Equal(t, "Ask the operator after all", afterReview.Review.Text)

	notices := *h.notices
	require.Len(t, notices, 3, "%+v", notices)
	assert.Equal(t, chat.EscalationHandling, notices[0].Status)
	assert.Equal(t, chat.EscalationResolved, notices[1].Status)
	assert.False(t, notices[1].Override)
	assert.Equal(t, chat.HandlerLogin, notices[1].Answer.By.Login)
	assert.Equal(t, "Verify it yourself with go test.", notices[1].Answer.Text)
	assert.True(t, notices[2].Override)
	assert.Equal(t, "Ask the operator after all", notices[2].Answer.Text)

	var final chat.EscalationState
	require.NoError(t, h.env.GetWorkflowResult(&final))
	assert.True(t, final.Delivered)
}

// An escalation the handler resolved ends when the review window passes with
// nobody reviewing it.
func TestAResolutionEndsWhenNobodyReviewsIt(t *testing.T) {
	h := newHandledEscalation(t)
	h.env.RegisterDelayedCallback(func() {
		h.update(t, chat.UpdateEscalationDecide, chat.EscalationDecision{Outcome: chat.OutcomeResolved, Text: "a", Reasoning: "b", Learning: "l"}, func(chat.EscalationState, error) {})
	}, time.Minute)
	h.env.ExecuteWorkflow(chat.EscalationWorkflowName, newEscalation())
	require.True(t, h.env.IsWorkflowCompleted())
	require.NoError(t, h.env.GetWorkflowError())
	var final chat.EscalationState
	require.NoError(t, h.env.GetWorkflowResult(&final))
	assert.Equal(t, chat.EscalationResolved, final.Status)
	assert.Nil(t, final.Review)
}

// A forward carries a summary for a person and the escalation then waits for
// a person's answer, as any forwarded escalation does. A person cannot review
// a forward: they answer it.
func TestTheHandlerForwardsWithASummary(t *testing.T) {
	h := newHandledEscalation(t)
	var forwarded, review error
	var answered chat.EscalationState
	h.env.RegisterDelayedCallback(func() {
		h.update(t, chat.UpdateEscalationDecide, chat.EscalationDecision{Outcome: chat.OutcomeForwarded, Text: "Add the registry token to the chat's secret. Why: the handler cannot grant credentials.", Reasoning: "needs a credential", Handler: "h1"},
			func(_ chat.EscalationState, err error) { forwarded = err })
	}, time.Minute)
	h.env.RegisterDelayedCallback(func() {
		h.update(t, chat.UpdateEscalationReview, chat.EscalationReview{Text: "x"}, func(_ chat.EscalationState, err error) { review = err })
		h.update(t, chat.UpdateEscalationResolve, chat.EscalationAnswer{Text: "done", By: &chat.Person{Login: "alice@example.com"}}, func(state chat.EscalationState, _ error) { answered = state })
	}, time.Hour)
	var mid chat.EscalationState
	h.env.RegisterDelayedCallback(func() { mid = queryEscalation(t, h.env) }, 30*time.Minute)
	h.env.ExecuteWorkflow(chat.EscalationWorkflowName, newEscalation())
	require.NoError(t, h.env.GetWorkflowError())
	require.NoError(t, forwarded)
	require.Error(t, review)
	assert.Equal(t, chat.EscalationForwarded, mid.Status)
	assert.Contains(t, mid.Note, "registry token")
	assert.Equal(t, chat.OutcomeForwarded, mid.Decision.Outcome)
	assert.Equal(t, chat.EscalationResolved, answered.Status)
}

// A declined escalation tells the chat why and waits, listed, for a person: they
// acknowledge it, or override it. An answer is refused: reviewing is what
// they do.
func TestTheHandlerDeclinesAndAPersonAcknowledges(t *testing.T) {
	h := newHandledEscalation(t)
	var answerRefused error
	var declined, acknowledged chat.EscalationState
	h.env.RegisterDelayedCallback(func() {
		h.update(t, chat.UpdateEscalationDecide, chat.EscalationDecision{Outcome: chat.OutcomeDeclined, Text: "Force-pushing master is not allowed.", Reasoning: "branch rules"}, func(state chat.EscalationState, _ error) { declined = state })
	}, time.Minute)
	h.env.RegisterDelayedCallback(func() {
		h.update(t, chat.UpdateEscalationResolve, chat.EscalationAnswer{Text: "ok"}, func(_ chat.EscalationState, err error) { answerRefused = err })
		h.update(t, chat.UpdateEscalationReview, chat.EscalationReview{By: &chat.Person{Login: "alice@example.com"}}, func(state chat.EscalationState, _ error) { acknowledged = state })
	}, time.Hour)
	h.env.ExecuteWorkflow(chat.EscalationWorkflowName, newEscalation())
	require.NoError(t, h.env.GetWorkflowError())
	assert.Equal(t, chat.EscalationDeclined, declined.Status)
	require.Error(t, answerRefused)
	assert.Contains(t, answerRefused.Error(), "declined")
	assert.Equal(t, chat.EscalationResolved, acknowledged.Status)
	notices := *h.notices
	require.Len(t, notices, 1)
	assert.Equal(t, chat.EscalationDeclined, notices[0].Status)
	assert.Equal(t, "Force-pushing master is not allowed.", notices[0].Answer.Text)
}

// An escalation that the handler cannot be given, or that it does not decide
// in time, goes to a person, with a note that says so. The handler's own
// forward signal does too.
func TestAnEscalationTheHandlerDoesNotTakeIsForwarded(t *testing.T) {
	t.Run("hand-off fails", func(t *testing.T) {
		h := newHandledEscalation(t)
		h.handoffErr = errors.New("temporal is down")
		var end chat.EscalationState
		h.env.RegisterDelayedCallback(func() { end = queryEscalation(t, h.env) }, time.Hour)
		h.env.ExecuteWorkflow(chat.EscalationWorkflowName, newEscalation())
		assert.Equal(t, chat.EscalationForwarded, end.Status)
		assert.Contains(t, end.Note, "could not take")
	})
	t.Run("deadline", func(t *testing.T) {
		h := newHandledEscalation(t)
		var end chat.EscalationState
		h.env.RegisterDelayedCallback(func() { end = queryEscalation(t, h.env) }, 2*time.Hour)
		h.env.ExecuteWorkflow(chat.EscalationWorkflowName, newEscalation())
		assert.Equal(t, chat.EscalationForwarded, end.Status)
		assert.Contains(t, end.Note, "did not decide")
	})
	t.Run("handler gives up", func(t *testing.T) {
		h := newHandledEscalation(t)
		var end chat.EscalationState
		h.env.RegisterDelayedCallback(func() {
			h.env.SignalWorkflow(chat.SignalEscalationHandling, chat.EscalationClaim{Handler: "h1"})
		}, time.Minute)
		h.env.RegisterDelayedCallback(func() {
			h.env.SignalWorkflow(chat.SignalEscalationForward, chat.EscalationForward{Note: "ended its turn", Reasoning: "one attempt"})
		}, 2*time.Minute)
		h.env.RegisterDelayedCallback(func() { end = queryEscalation(t, h.env) }, 3*time.Minute)
		h.env.ExecuteWorkflow(chat.EscalationWorkflowName, newEscalation())
		assert.Equal(t, chat.EscalationForwarded, end.Status)
		require.NotNil(t, end.Decision)
		assert.Equal(t, "one attempt", end.Decision.Reasoning)
		assert.Equal(t, "h1", end.Decision.Handler)
	})
}

// The chat takes what the escalation tells it: a refusal and an override
// reach it as messages, an override only once, and a resolution of the handler
// says it comes from the handler.
func TestTheChatTakesTheHandlersDecisionsAndAnOverride(t *testing.T) {
	env, fake := newEnvironment(t)
	state := newChat("first")
	state.Escalations = []chat.EscalationRef{{ID: "e1", Title: "A token", Status: chat.EscalationOpen}, {ID: "e2", Title: "Merge", Status: chat.EscalationOpen}}
	env.RegisterDelayedCallback(func() {
		env.SignalWorkflow(chat.SignalEscalation, chat.EscalationNotice{ID: "e1", Status: chat.EscalationResolved, Answer: &chat.EscalationAnswer{Text: "use the vault", By: &chat.Person{Login: chat.HandlerLogin}}})
		env.SignalWorkflow(chat.SignalEscalation, chat.EscalationNotice{ID: "e2", Status: chat.EscalationDeclined, Answer: &chat.EscalationAnswer{Text: "no force pushes", By: &chat.Person{Login: chat.HandlerLogin}}})
	}, time.Minute)
	env.RegisterDelayedCallback(func() {
		override := chat.EscalationNotice{ID: "e1", Status: chat.EscalationResolved, Override: true, Answer: &chat.EscalationAnswer{Text: "ask ops", By: &chat.Person{Login: "alice@example.com"}}}
		env.SignalWorkflow(chat.SignalEscalation, override)
		env.SignalWorkflow(chat.SignalEscalation, override)
	}, 2*time.Minute)
	env.RegisterDelayedCallback(func() { env.SignalWorkflow(chat.SignalClose, nil) }, 3*time.Minute)
	env.ExecuteWorkflow(chat.WorkflowName, state)
	require.NoError(t, env.GetWorkflowError())
	var prompts []string
	for _, turn := range fake.turns {
		prompts = append(prompts, turn.Prompt)
	}
	require.Len(t, prompts, 4, "%q", prompts)
	assert.Contains(t, prompts[1], "the escalation handler (an agent, not a person)")
	assert.Contains(t, prompts[1], "use the vault")
	assert.Contains(t, prompts[2], "declined")
	assert.Contains(t, prompts[2], "do not escalate the same request again")
	assert.Contains(t, prompts[3], "alice@example.com")
	assert.Contains(t, prompts[3], "overrides")
}

// handlerEnvironment runs the handler workflow with the chat it starts, its
// activities and its signals mocked. The test says how each handler chat ends.
type handlerEnvironment struct {
	env *testsuite.TestWorkflowEnvironment
	// started are the handler chats' states, in order.
	started []chat.State
	// forwards are the signals sent to the escalations to forward them.
	forwards map[string][]chat.EscalationForward
	claims   []string
	// outcome says what each escalation's state is when its handler chat's turn
	// has ended. Missing is still handling.
	outcome map[string]chat.EscalationState
	// brief is what the brief activity answers.
	briefs int
}

func newHandlerEnvironment(t *testing.T) *handlerEnvironment {
	t.Helper()
	var suite testsuite.WorkflowTestSuite
	h := &handlerEnvironment{env: suite.NewTestWorkflowEnvironment(), forwards: map[string][]chat.EscalationForward{}, outcome: map[string]chat.EscalationState{}}
	h.env.RegisterWorkflowWithOptions(chat.HandlerWorkflow, workflow.RegisterOptions{Name: chat.HandlerWorkflowName})
	h.env.RegisterWorkflowWithOptions(chat.Workflow, workflow.RegisterOptions{Name: chat.WorkflowName})
	h.env.RegisterActivityWithOptions(func(_ context.Context, request chat.EscalationRequest) (string, error) {
		h.briefs++
		return "Chat " + request.Chat + " is stuck on a thing.", nil
	}, activity.RegisterOptions{Name: chat.ActivityEscalationBrief})
	h.env.RegisterActivityWithOptions(func(_ context.Context, id string) (chat.EscalationState, error) {
		if state, ok := h.outcome[id]; ok {
			return state, nil
		}
		return chat.EscalationState{Request: chat.EscalationRequest{ID: id}, Status: chat.EscalationHandling}, nil
	}, activity.RegisterOptions{Name: chat.ActivityEscalationOutcome})
	// The mocked chat ends at once. The test env takes a signal to a finished
	// child itself, so the handler's close of its chat is not seen here.
	h.env.OnWorkflow(chat.WorkflowName, mock.Anything, mock.Anything).Return(chat.Report{}, nil)
	h.env.SetOnChildWorkflowStartedListener(func(_ *workflow.Info, _ workflow.Context, args converter.EncodedValues) {
		var state chat.State
		_ = args.Get(&state)
		h.started = append(h.started, state)
	})
	h.env.OnSignalExternalWorkflow(mock.Anything, mock.Anything, "", chat.SignalEscalationForward, mock.Anything).Return(nil).Run(func(args mock.Arguments) {
		id := strings.TrimPrefix(args.Get(1).(string), "escalation-")
		h.forwards[id] = append(h.forwards[id], args.Get(4).(chat.EscalationForward))
	})
	h.env.OnSignalExternalWorkflow(mock.Anything, mock.Anything, "", chat.SignalEscalationHandling, mock.Anything).Return(nil).Run(func(args mock.Arguments) {
		h.claims = append(h.claims, strings.TrimPrefix(args.Get(1).(string), "escalation-"))
	})
	return h
}

func intake(id, chatID, need string) chat.HandlerIntake {
	return chat.HandlerIntake{Request: chat.EscalationRequest{
		ID: id, Chat: chatID, Need: need, Why: "w", Title: need,
		Issue:   &chat.Issue{Repo: "gfx-labs/protocol7-selfdev", Number: 79},
		Project: "tenant",
		Session: &chat.EscalationSession{
			Host: "host1", Backend: "kubernetes", Model: "fast", Models: []string{"a/b"}, Restricted: true,
			Repositories: []chat.Repository{{Name: "protocol7-selfdev", URL: "git@github.com:gfx-labs/protocol7-selfdev.git"}},
		},
	}}
}

func queryHandler(t *testing.T, env *testsuite.TestWorkflowEnvironment) chat.HandlerState {
	t.Helper()
	value, err := env.QueryWorkflow(chat.QueryHandler)
	require.NoError(t, err)
	var state chat.HandlerState
	require.NoError(t, value.Get(&state))
	return state
}

// endTurn reports that the handler chat of the escalation ended its turn.
func (h *handlerEnvironment) endTurn(t *testing.T, index int, errText string) {
	t.Helper()
	require.Greater(t, len(h.started), index)
	id := h.started[index].Request.ID
	h.env.SignalWorkflow(chat.SignalReport, chat.Report{ChatID: id, Message: id, Idle: true, Error: errText})
}

// The handler takes an escalation, claims it, and starts a chat that has the
// reach of the chat it serves and no more, with the escalation, its issue and
// the chat's state in the first message. When the chat's turn ends having
// resolved it, the handler sends no forward and remembers it.
func TestTheHandlerStartsAChatWithTheReachOfTheChatItServes(t *testing.T) {
	h := newHandlerEnvironment(t)
	h.env.SetStartWorkflowOptions(client.StartWorkflowOptions{ID: chat.HandlerWorkflowID})
	request := intake("e1", "c1", "How do I verify the flag without the operator script")
	request.Request.Tried, request.Request.Next = "ran the script", "run the checks"
	request.Models, request.Model = []string{"deepseek/flash"}, "flash"
	h.env.RegisterDelayedCallback(func() { h.env.SignalWorkflow(chat.SignalEscalationIntake, request) }, time.Minute)
	h.env.RegisterDelayedCallback(func() {
		require.Len(t, h.started, 1)
		h.outcome["e1"] = chat.EscalationState{Status: chat.EscalationResolved, Decision: &chat.EscalationDecision{Outcome: chat.OutcomeResolved, Learning: "l1"}}
		h.endTurn(t, 0, "")
	}, 2*time.Minute)
	var after chat.HandlerState
	h.env.RegisterDelayedCallback(func() { after = queryHandler(t, h.env) }, 3*time.Minute)
	h.env.RegisterDelayedCallback(func() { h.env.CancelWorkflow() }, 4*time.Minute)
	h.env.ExecuteWorkflow(chat.HandlerWorkflowName, chat.HandlerState{})

	require.Len(t, h.started, 1)
	started := h.started[0]
	assert.Equal(t, []string{"e1"}, h.claims)
	assert.Equal(t, 1, h.briefs)
	assert.Empty(t, h.forwards)

	r := started.Request
	assert.True(t, r.Handler)
	assert.Equal(t, &chat.Serving{Escalation: "e1", Chat: "c1"}, r.Serves)
	assert.Equal(t, chat.HandlerWorkflowID, r.Owner)
	assert.Equal(t, "tenant", r.Project)
	assert.True(t, r.Restricted, "a restricted chat's handler is restricted too")
	assert.Equal(t, "host1", r.Host)
	assert.Equal(t, "kubernetes", r.Backend)
	assert.Equal(t, request.Request.Session.Repositories, r.Repositories)
	assert.Equal(t, &chat.Issue{Repo: "gfx-labs/protocol7-selfdev", Number: 79}, r.Issue)
	assert.Equal(t, []string{"deepseek/flash"}, r.Models, "the handler's own model choice")
	assert.Equal(t, "flash", r.Model)
	require.Len(t, started.Pending, 1)
	task := started.Pending[0].Text
	for _, want := range []string{"escalation e1", "How do I verify the flag", "ran the script", "run the checks", "gfx-labs/protocol7-selfdev", "Chat c1 is stuck"} {
		assert.Contains(t, task, want)
	}
	require.Len(t, after.Resolved, 1)
	assert.Equal(t, "c1", after.Resolved[0].Chat)
	assert.Equal(t, chat.OutcomeResolved, after.Resolved[0].Outcome)
}

// Without its own model choice the handler runs on the models of the chat it
// serves, so it draws on the same provider limits.
func TestTheHandlerUsesTheModelsOfTheChatItServesByDefault(t *testing.T) {
	h := newHandlerEnvironment(t)
	h.env.RegisterDelayedCallback(func() { h.env.SignalWorkflow(chat.SignalEscalationIntake, intake("e1", "c1", "need")) }, time.Minute)
	h.env.RegisterDelayedCallback(func() { h.env.CancelWorkflow() }, 2*time.Minute)
	h.env.ExecuteWorkflow(chat.HandlerWorkflowName, chat.HandlerState{})
	require.Len(t, h.started, 1)
	assert.Equal(t, []string{"a/b"}, h.started[0].Request.Models)
	assert.Equal(t, "fast", h.started[0].Request.Model)
}

// One failed attempt forwards: a turn that ends with nothing decided, a chat
// that fails and a turn that does not end in time are each forwarded once,
// with the reason.
func TestTheHandlerForwardsAfterOneFailedAttempt(t *testing.T) {
	for name, test := range map[string]struct {
		errText string
		stall   bool
		want    string
	}{
		"no decision": {want: "without deciding"},
		"failure":     {errText: "the harness stopped", want: "failed: the harness stopped"},
		"timeout":     {stall: true, want: "did not decide within"},
	} {
		t.Run(name, func(t *testing.T) {
			h := newHandlerEnvironment(t)
			h.env.RegisterDelayedCallback(func() { h.env.SignalWorkflow(chat.SignalEscalationIntake, intake("e1", "c1", "need")) }, time.Minute)
			if !test.stall {
				h.env.RegisterDelayedCallback(func() { h.endTurn(t, 0, test.errText) }, 2*time.Minute)
			}
			h.env.RegisterDelayedCallback(func() { h.env.CancelWorkflow() }, 2*time.Hour)
			h.env.ExecuteWorkflow(chat.HandlerWorkflowName, chat.HandlerState{})
			require.Len(t, h.forwards["e1"], 1, "forwarded once")
			assert.Contains(t, h.forwards["e1"][0].Note, test.want)
			assert.Contains(t, h.forwards["e1"][0].Reasoning, "One attempt")
			assert.Len(t, h.started, 1, "the handler does not try twice")
		})
	}
}

// A chat that escalates the same thing again after the handler resolved it is
// forwarded at once: no second handler chat. The words of the need are what is
// compared, so a different need of the same chat, and the same need of another
// chat, are handled.
func TestTheLoopGuardForwardsARepeatAtOnce(t *testing.T) {
	h := newHandlerEnvironment(t)
	h.env.RegisterDelayedCallback(func() {
		h.env.SignalWorkflow(chat.SignalEscalationIntake, intake("e1", "c1", "How do I verify the feature flag state without the operator script"))
	}, time.Minute)
	h.env.RegisterDelayedCallback(func() {
		h.outcome["e1"] = chat.EscalationState{Status: chat.EscalationResolved, Decision: &chat.EscalationDecision{Outcome: chat.OutcomeResolved, Learning: "l1"}}
		h.endTurn(t, 0, "")
	}, 2*time.Minute)
	h.env.RegisterDelayedCallback(func() {
		// The same thing, said a little differently, by the same chat.
		h.env.SignalWorkflow(chat.SignalEscalationIntake, intake("e2", "c1", "How do I verify the feature flag state without the operator script?"))
		// A different thing from the same chat, and the same thing from another chat.
		h.env.SignalWorkflow(chat.SignalEscalationIntake, intake("e3", "c1", "Please merge the pull request although the checks are red"))
		h.env.SignalWorkflow(chat.SignalEscalationIntake, intake("e4", "c2", "How do I verify the feature flag state without the operator script"))
	}, 3*time.Minute)
	h.env.RegisterDelayedCallback(func() { h.env.CancelWorkflow() }, 4*time.Minute)
	h.env.ExecuteWorkflow(chat.HandlerWorkflowName, chat.HandlerState{})

	require.Len(t, h.forwards["e2"], 1)
	assert.Contains(t, h.forwards["e2"][0].Note, "again after the handler resolved escalation e1")
	assert.Contains(t, h.forwards["e2"][0].Reasoning, "Loop guard")
	assert.Empty(t, h.forwards["e3"])
	assert.Empty(t, h.forwards["e4"])
	var chats []string
	for _, state := range h.started {
		chats = append(chats, state.Request.Serves.Escalation)
	}
	assert.ElementsMatch(t, []string{"e1", "e3", "e4"}, chats, "e2 never got a handler chat")
}

// An escalation handed over twice (a retried hand-off) is handled once.
func TestTheHandlerTakesAnEscalationOnce(t *testing.T) {
	h := newHandlerEnvironment(t)
	h.env.RegisterDelayedCallback(func() {
		h.env.SignalWorkflow(chat.SignalEscalationIntake, intake("e1", "c1", "need"))
		h.env.SignalWorkflow(chat.SignalEscalationIntake, intake("e1", "c1", "need"))
	}, time.Minute)
	h.env.RegisterDelayedCallback(func() { h.env.CancelWorkflow() }, 2*time.Minute)
	h.env.ExecuteWorkflow(chat.HandlerWorkflowName, chat.HandlerState{})
	assert.Len(t, h.started, 1)
}

// At most chat.MaxHandlers handler chats run at once. The rest wait.
func TestTheHandlerRunsAtMostFourChats(t *testing.T) {
	h := newHandlerEnvironment(t)
	h.env.RegisterDelayedCallback(func() {
		for i := range 6 {
			h.env.SignalWorkflow(chat.SignalEscalationIntake, intake("e"+string(rune('a'+i)), "c"+string(rune('a'+i)), "need number "+strings.Repeat("x", i)))
		}
	}, time.Minute)
	var running, queued int
	h.env.RegisterDelayedCallback(func() {
		state := queryHandler(t, h.env)
		running, queued = state.Active, len(state.Queue)
		h.env.CancelWorkflow()
	}, 2*time.Minute)
	h.env.ExecuteWorkflow(chat.HandlerWorkflowName, chat.HandlerState{})
	assert.Equal(t, chat.MaxHandlers, running)
	assert.Equal(t, 2, queued)
}

// The handler's chat decides with three tools and has none that escalates,
// spawns chats, runs probes or draws on a board. A resolution is refused
// until a learning is recorded, and carries the learning's ID. The chat's
// refusal of the decision is the escalation's, and is final.
func TestTheHandlersChatToolsAndResolution(t *testing.T) {
	log := learningtest.New(t)
	env, fake := newEnvironment(t)
	registerLearnings(env, log)
	var decisions []chat.DecideRequest
	env.RegisterActivityWithOptions(func(_ context.Context, request chat.DecideRequest) (chat.EscalationState, error) {
		decisions = append(decisions, request)
		return chat.EscalationState{Status: chat.EscalationResolved}, nil
	}, activity.RegisterOptions{Name: chat.ActivityEscalationDecide})
	var early, recorded, resolved p7.ToolResult
	fake.onSlow = func() {
		fake.mu.Lock()
		session := fake.opened[0].ID
		fake.mu.Unlock()
		toolCall(t, env, session, "escalation_resolve", map[string]string{"answer": "Run go test.", "reasoning": "docs say so"}, func(result p7.ToolResult) {
			early = result
			toolCall(t, env, session, "learning_record", map[string]any{"title": "Verify with go test", "problem": "asked for the operator script", "cause": "did not know go test verifies it", "fix": "run go test", "scope": "project"}, func(result p7.ToolResult) {
				recorded = result
				toolCall(t, env, session, "escalation_resolve", map[string]string{"answer": "Run go test.", "reasoning": "docs say so"}, func(result p7.ToolResult) {
					resolved = result
					close(fake.release)
				})
			})
		})
	}
	state := newChat("slow")
	state.Request.Handler = true
	state.Request.Serves = &chat.Serving{Escalation: "e1", Chat: "c9"}
	state.Request.Issue = &chat.Issue{Repo: "gfx-labs/protocol7-selfdev", Number: 79}
	env.ExecuteWorkflow(chat.WorkflowName, state)
	_ = env.GetWorkflowError()

	fake.mu.Lock()
	var tools []string
	for _, tool := range fake.opened[0].Tools {
		tools = append(tools, tool.Name)
	}
	fake.mu.Unlock()
	assert.Subset(t, tools, []string{"escalation_resolve", "escalation_forward", "escalation_decline", "learning_record", "learning_search", "learning_decide", "issue_edit", "issue_comment"})
	for _, banned := range []string{"escalate", "probe_escalate", "chat_spawn", "probe_create", "design_show"} {
		assert.NotContains(t, tools, banned, "the handler has no %s", banned)
	}

	assert.True(t, early.IsError)
	assert.Contains(t, early.Content[0].Text, "learning_record")
	assert.Contains(t, recorded.Content[0].Text, "accepted")
	assert.False(t, resolved.IsError, "%+v", resolved)
	require.Len(t, decisions, 1)
	assert.Equal(t, "e1", decisions[0].Escalation)
	assert.Equal(t, chat.OutcomeResolved, decisions[0].Decision.Outcome)
	assert.Equal(t, "c1", decisions[0].Decision.Handler)
	assert.NotEmpty(t, decisions[0].Decision.Learning)
	assert.Equal(t, learning.Accepted, log.Entries()[0].State)
	assert.Equal(t, "e1", log.Entries()[0].Source.Escalation, "the learning names the escalation without being told")
	// The first prompt is the handler's rules.
	fake.mu.Lock()
	context := fake.turns[0].Context
	fake.mu.Unlock()
	assert.Contains(t, context, "escalation handler for escalation e1")
	assert.Contains(t, context, "never grant it any")
}

// A forward and a refusal carry what the handler gives, and a refusal of the
// decision by the escalation reaches the agent. A decline on an issue's chat
// comments on the issue.
func TestTheHandlersForwardAndDecline(t *testing.T) {
	env, fake := newEnvironment(t)
	var decisions []chat.DecideRequest
	refuse := false
	env.RegisterActivityWithOptions(func(_ context.Context, request chat.DecideRequest) (chat.EscalationState, error) {
		decisions = append(decisions, request)
		if refuse {
			return chat.EscalationState{}, temporal.NewNonRetryableApplicationError("this escalation is resolved already", chat.ErrorRefused, nil)
		}
		return chat.EscalationState{Status: chat.EscalationDeclined}, nil
	}, activity.RegisterOptions{Name: chat.ActivityEscalationDecide})
	var comments []chat.IssueRequest
	env.RegisterActivityWithOptions(func(_ context.Context, request chat.IssueRequest) (chat.IssueResult, error) {
		comments = append(comments, request)
		return chat.IssueResult{}, nil
	}, activity.RegisterOptions{Name: chat.ActivityIssueComment})
	var forwardResult, declineResult p7.ToolResult
	fake.onSlow = func() {
		fake.mu.Lock()
		session := fake.opened[0].ID
		fake.mu.Unlock()
		toolCall(t, env, session, "escalation_decline", map[string]string{"reason": "force-pushing master", "reasoning": "branch rules"}, func(result p7.ToolResult) {
			declineResult = result
			refuse = true
			toolCall(t, env, session, "escalation_forward", map[string]string{"summary": "Add the token", "reasoning": "credential"}, func(result p7.ToolResult) {
				forwardResult = result
				close(fake.release)
			})
		})
	}
	state := newChat("slow")
	state.Request.Handler = true
	state.Request.Serves = &chat.Serving{Escalation: "e1", Chat: "c9"}
	state.Request.Issue = &chat.Issue{Repo: "gfx-labs/protocol7-selfdev", Number: 79}
	state.Request.Repositories = []chat.Repository{{Name: "protocol7-selfdev", URL: "git@github.com:gfx-labs/protocol7-selfdev.git"}}
	env.ExecuteWorkflow(chat.WorkflowName, state)
	_ = env.GetWorkflowError()
	assert.False(t, declineResult.IsError, "%+v", declineResult)
	assert.True(t, forwardResult.IsError)
	assert.Contains(t, forwardResult.Content[0].Text, "resolved already")
	require.Len(t, decisions, 2)
	assert.Equal(t, chat.OutcomeDeclined, decisions[0].Decision.Outcome)
	assert.Equal(t, "force-pushing master", decisions[0].Decision.Text)
	assert.Equal(t, chat.OutcomeForwarded, decisions[1].Decision.Outcome)
	require.Len(t, comments, 1)
	assert.Equal(t, 79, comments[0].Number)
	assert.Contains(t, comments[0].Body, "declined")
	assert.Contains(t, comments[0].Body, "force-pushing master")
}

// The reports of a chat that fails to open, and of one that is closed, name
// the chat, so that an owner that runs several tells them apart: the handler
// workflow notices a handler chat that failed at once.
func TestAnOwnersReportsNameTheChatThatFailedToOpenOrClosed(t *testing.T) {
	env, fake := newEnvironment(t)
	fake.openErr = errors.New("no login")
	var reports []chat.Report
	env.OnSignalExternalWorkflow(mock.Anything, "owner", "", chat.SignalReport, mock.Anything).Return(nil).Run(func(args mock.Arguments) {
		reports = append(reports, args.Get(4).(chat.Report))
	})
	state := newChat("first")
	state.Request.Owner = "owner"
	env.ExecuteWorkflow(chat.WorkflowName, state)
	require.Error(t, env.GetWorkflowError())
	require.NotEmpty(t, reports)
	for _, report := range reports {
		assert.Equal(t, "c1", report.ChatID)
	}
	assert.True(t, reports[len(reports)-1].Ended)
}
