package panel_test

import (
	"errors"
	"slices"
	"strings"
	"testing"
	"time"

	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/testsuite"
	"go.temporal.io/sdk/workflow"

	"github.com/gfx-labs/protocol7/pkg/orchestration/panel"
)

// Answer is what a test participant says: a decision, and what it saw of the
// others when it said it.
type Answer struct {
	Decision string   `json:"decision"`
	Saw      []string `json:"saw"`
}

// Say is a participant's script for one round.
type Say struct {
	Decision string      `json:"decision"`
	Fail     bool        `json:"fail,omitempty"`
	Hang     bool        `json:"hang,omitempty"`
	Spend    panel.Money `json:"spend,omitempty"`
}

// Plan is the scripted panel: for each participant and round what it says.
type Plan struct {
	Participants []panel.Participant `json:"participants"`
	Script       map[string][]Say    `json:"script"`
	MinVendors   int                 `json:"min_vendors"`
	MaxRounds    int                 `json:"max_rounds"`
	Budget       panel.Budget        `json:"budget"`
	Ceiling      panel.Money         `json:"ceiling"`
	Settle       bool                `json:"settle"`
	SameAnswer   bool                `json:"same_answer"`
}

type childInput struct {
	Say  Say      `json:"say"`
	Saw  []string `json:"saw"`
	Name string   `json:"name"`
}

// participant is the child workflow of every participant. It answers as its
// script says.
func participant(ctx workflow.Context, input childInput) (panel.Outcome[Answer], error) {
	if input.Say.Hang {
		if err := workflow.Sleep(ctx, 24*time.Hour); err != nil {
			return panel.Outcome[Answer]{}, err
		}
	}
	if input.Say.Fail {
		return panel.Outcome[Answer]{}, temporal.NewNonRetryableApplicationError("the participant's session failed", "SessionFailed", nil)
	}
	return panel.Outcome[Answer]{Position: Answer{Decision: input.Say.Decision, Saw: input.Saw}, Spent: input.Say.Spend}, nil
}

func scripted(plan Plan, name string, round int) Say {
	script := plan.Script[name]
	if round < len(script) {
		return script[round]
	}
	return script[len(script)-1]
}

// agree is the test's rule: the panel agrees when every live participant's
// newest decision is the same.
func agree(state panel.State[Answer]) (string, bool) {
	if len(state.Latest) < 2 {
		return "", false
	}
	for _, position := range state.Latest[1:] {
		if position.Value.Decision != state.Latest[0].Value.Decision {
			return "", false
		}
	}
	return "all say " + state.Latest[0].Value.Decision, true
}

func runPanel(ctx workflow.Context, plan Plan) (panel.Result[Answer], error) {
	config := panel.Config[Answer]{
		Participants: plan.Participants,
		MinVendors:   plan.MinVendors,
		MaxRounds:    plan.MaxRounds,
		Budget:       plan.Budget,
		Child: func(p panel.Participant, round int, others []panel.Position[Answer]) (panel.Child, error) {
			var saw []string
			for _, other := range others {
				saw = append(saw, other.Participant+":"+other.Value.Decision)
			}
			return panel.Child{Workflow: participant, Input: childInput{Say: scripted(plan, p.Name, round), Saw: saw, Name: p.Name}}, nil
		},
		Ceiling: func(panel.Participant, int) panel.Money { return plan.Ceiling },
		Rules:   []panel.Rule[Answer]{agree},
	}
	if plan.SameAnswer {
		config.Same = func(a, b Answer) bool { return a.Decision == b.Decision }
	}
	return panel.Run(ctx, config)
}

func environment(t *testing.T) *testsuite.TestWorkflowEnvironment {
	t.Helper()
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	env.RegisterWorkflow(runPanel)
	env.RegisterWorkflow(participant)
	return env
}

func execute(t *testing.T, env *testsuite.TestWorkflowEnvironment, plan Plan) (panel.Result[Answer], error) {
	t.Helper()
	env.ExecuteWorkflow(runPanel, plan)
	if !env.IsWorkflowCompleted() {
		t.Fatal("the panel did not finish")
	}
	var result panel.Result[Answer]
	if err := env.GetWorkflowError(); err != nil {
		return result, err
	}
	if err := env.GetWorkflowResult(&result); err != nil {
		t.Fatal(err)
	}
	return result, nil
}

var twoVendors = []panel.Participant{{Name: "A", Vendor: "anthropic"}, {Name: "B", Vendor: "openai"}}

func decisions(round []panel.Position[Answer]) string {
	var out []string
	for _, position := range round {
		out = append(out, position.Participant+"="+position.Value.Decision)
	}
	return strings.Join(out, " ")
}

// Participants that agree after the first round end the panel there, with
// the rule's reason, and the first round is independent: nobody saw anybody.
func TestAgreementInTheFirstRoundEndsThePanel(t *testing.T) {
	result, err := execute(t, environment(t), Plan{
		Participants: twoVendors, MinVendors: 2, MaxRounds: 3,
		Budget: panel.Budget{Cap: 100e6, Reserve: 10e6}, Ceiling: 5e6,
		Script: map[string][]Say{"A": {{Decision: "build", Spend: 2e6}}, "B": {{Decision: "build", Spend: 3e6}}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if result.Stop != panel.StopAgreement || result.Detail != "all say build" {
		t.Errorf("stop = %q (%q), want agreement on build", result.Stop, result.Detail)
	}
	if len(result.Rounds) != 1 {
		t.Fatalf("%d rounds, want 1", len(result.Rounds))
	}
	for _, position := range result.Rounds[0] {
		if len(position.Value.Saw) != 0 {
			t.Errorf("%s saw %v in the first round, want nothing", position.Participant, position.Value.Saw)
		}
	}
	if result.Spent != 5e6 {
		t.Errorf("spent = %d, want 5000000", result.Spent)
	}
}

// Participants that keep disagreeing go through every round. In each later
// round a participant sees the others' newest positions as of the end of the
// round before and never its own or the round's.
func TestDisagreementRunsEveryRoundAndEachRoundSeesTheLastOne(t *testing.T) {
	result, err := execute(t, environment(t), Plan{
		Participants: twoVendors, MinVendors: 2, MaxRounds: 2,
		Budget: panel.Budget{Cap: 100e6, Reserve: 10e6}, Ceiling: 5e6,
		Script: map[string][]Say{
			"A": {{Decision: "build"}, {Decision: "build?"}, {Decision: "build!"}},
			"B": {{Decision: "decline"}, {Decision: "decline?"}, {Decision: "decline!"}},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if result.Stop != panel.StopRoundLimit || len(result.Rounds) != 3 {
		t.Fatalf("stop = %q after %d rounds, want the round limit after 3", result.Stop, len(result.Rounds))
	}
	byName := func(round []panel.Position[Answer], name string) Answer {
		for _, position := range round {
			if position.Participant == name {
				return position.Value
			}
		}
		t.Fatalf("%s has no position in %s", name, decisions(round))
		return Answer{}
	}
	if got := byName(result.Rounds[1], "A").Saw; !slices.Equal(got, []string{"B:decline"}) {
		t.Errorf("A saw %v in round 1, want B's round 0 answer", got)
	}
	if got := byName(result.Rounds[2], "B").Saw; !slices.Equal(got, []string{"A:build?"}) {
		t.Errorf("B saw %v in round 2, want A's round 1 answer", got)
	}
}

// A panel agrees once the participants converge, and says in which round.
func TestAParticipantThatChangesItsMindEndsInAgreement(t *testing.T) {
	result, err := execute(t, environment(t), Plan{
		Participants: twoVendors, MinVendors: 2, MaxRounds: 3,
		Budget: panel.Budget{Cap: 100e6, Reserve: 10e6}, Ceiling: 5e6,
		Script: map[string][]Say{"A": {{Decision: "build"}}, "B": {{Decision: "decline"}, {Decision: "build"}}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if result.Stop != panel.StopAgreement || len(result.Rounds) != 2 {
		t.Errorf("stop = %q after %d rounds, want agreement after 2", result.Stop, len(result.Rounds))
	}
}

// A round in which nobody changed anything ends the panel, with the
// disagreement left as it is.
func TestARoundThatChangesNothingEndsThePanel(t *testing.T) {
	result, err := execute(t, environment(t), Plan{
		Participants: twoVendors, MinVendors: 2, MaxRounds: 5, SameAnswer: true,
		Budget: panel.Budget{Cap: 100e6, Reserve: 10e6}, Ceiling: 5e6,
		Script: map[string][]Say{"A": {{Decision: "build"}}, "B": {{Decision: "decline"}}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if result.Stop != panel.StopNoProgress || len(result.Rounds) != 2 {
		t.Errorf("stop = %q after %d rounds, want no progress after 2", result.Stop, len(result.Rounds))
	}
}

// A participant that fails takes no further part, and its ceiling is charged,
// because what it spent is not known. The panel goes on while enough vendors
// remain and stops when they do not.
func TestFailedParticipantsAreDroppedWhileEnoughVendorsRemain(t *testing.T) {
	plan := Plan{
		Participants: []panel.Participant{{Name: "A", Vendor: "anthropic"}, {Name: "B", Vendor: "openai"}, {Name: "C", Vendor: "anthropic"}},
		MinVendors:   2, MaxRounds: 2,
		Budget: panel.Budget{Cap: 100e6, Reserve: 10e6}, Ceiling: 5e6,
		Script: map[string][]Say{
			"A": {{Decision: "build"}},
			"B": {{Decision: "decline"}},
			"C": {{Fail: true}},
		},
	}
	result, err := execute(t, environment(t), plan)
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Failed) != 1 || result.Failed[0].Participant != "C" || result.Failed[0].Charged != 5e6 {
		t.Fatalf("failed = %+v, want C charged its ceiling", result.Failed)
	}
	if got := decisions(result.Rounds[1]); got != "A=build B=decline" {
		t.Errorf("round 1 = %q, want C gone", got)
	}

	plan.Script["B"] = []Say{{Decision: "decline"}, {Fail: true}}
	result, err = execute(t, environment(t), plan)
	if err != nil {
		t.Fatal(err)
	}
	if result.Stop != panel.StopTooFewVendors {
		t.Errorf("stop = %q, want too few vendors once B failed and only anthropic remains", result.Stop)
	}
}

// A round whose ceiling does not fit the budget outside the reserve is not
// started, so the caller still has its reserve for the last step.
func TestARoundThatDoesNotFitTheBudgetIsNotStarted(t *testing.T) {
	result, err := execute(t, environment(t), Plan{
		Participants: twoVendors, MinVendors: 2, MaxRounds: 3,
		Budget: panel.Budget{Cap: 10e6, Reserve: 1.5e6}, Ceiling: 4e6,
		Script: map[string][]Say{"A": {{Decision: "build", Spend: 3e6}}, "B": {{Decision: "decline", Spend: 3e6}}},
	})
	if err != nil {
		t.Fatal(err)
	}
	// Round 0 may cost 8 and fits in 8.5. Round 1 may cost 8 more and 6 are spent.
	if result.Stop != panel.StopCost || len(result.Rounds) != 1 || result.Spent != 6e6 {
		t.Errorf("stop = %q after %d rounds with %d spent, want cost after 1 round with 6000000", result.Stop, len(result.Rounds), result.Spent)
	}
}

// A panel whose first round does not fit does not start, and spends nothing.
func TestAPanelThatCannotAffordItsFirstRoundSpendsNothing(t *testing.T) {
	result, err := execute(t, environment(t), Plan{
		Participants: twoVendors, MinVendors: 2, MaxRounds: 1,
		Budget: panel.Budget{Cap: 10e6, Reserve: 5e6}, Ceiling: 4e6,
		Script: map[string][]Say{"A": {{Decision: "build"}}, "B": {{Decision: "build"}}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if result.Stop != panel.StopCost || len(result.Rounds) != 0 || result.Spent != 0 {
		t.Errorf("stop = %q, %d rounds, %d spent, want cost with nothing run", result.Stop, len(result.Rounds), result.Spent)
	}
}

// A stop signal cancels the round that runs, and the children's
// cancellations are not counted as their failures.
func TestStopSignalCancelsTheRound(t *testing.T) {
	env := environment(t)
	env.RegisterDelayedCallback(func() { env.SignalWorkflow(panel.SignalStop, "a person stopped it") }, time.Hour)
	result, err := execute(t, env, Plan{
		Participants: twoVendors, MinVendors: 2, MaxRounds: 2,
		Budget: panel.Budget{Cap: 100e6, Reserve: 10e6}, Ceiling: 5e6,
		Script: map[string][]Say{"A": {{Decision: "build", Hang: true}}, "B": {{Decision: "build", Hang: true}}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if result.Stop != panel.StopSignal || len(result.Failed) != 0 {
		t.Errorf("stop = %q with failures %+v, want the signal and no failures", result.Stop, result.Failed)
	}
}

// A configuration that cannot work is refused before any child starts.
func TestAConfigurationWithTooFewVendorsIsRefused(t *testing.T) {
	_, err := execute(t, environment(t), Plan{
		Participants: []panel.Participant{{Name: "A", Vendor: "anthropic"}, {Name: "B", Vendor: "anthropic"}}, MinVendors: 2,
		Budget: panel.Budget{Cap: 10e6}, Ceiling: 1e6,
		Script: map[string][]Say{"A": {{Decision: "x"}}, "B": {{Decision: "x"}}},
	})
	var application *temporal.ApplicationError
	if err == nil || !errors.As(err, &application) || !strings.Contains(application.Error(), "2 vendors are needed") {
		t.Errorf("error = %v, want a refusal that names the vendors", err)
	}
}

// The query reports where the panel is, and the state the caller sees after
// each phase is the same one.
func TestQueryReportsTheFinishedPanel(t *testing.T) {
	env := environment(t)
	result, err := execute(t, env, Plan{
		Participants: twoVendors, MinVendors: 2, MaxRounds: 1,
		Budget: panel.Budget{Cap: 100e6, Reserve: 10e6}, Ceiling: 5e6,
		Script: map[string][]Say{"A": {{Decision: "build", Spend: 1e6}}, "B": {{Decision: "build", Spend: 1e6}}},
	})
	if err != nil {
		t.Fatal(err)
	}
	value, err := env.QueryWorkflow(panel.QueryState)
	if err != nil {
		t.Fatal(err)
	}
	var status panel.Status
	if err := value.Get(&status); err != nil {
		t.Fatal(err)
	}
	if status.Phase != "done" || status.Stop != result.Stop || status.Round != 0 || status.Spent != 2e6 || !slices.Equal(status.Live, []string{"A", "B"}) {
		t.Errorf("status = %+v, want done at round 0 with 2000000 spent and both live", status)
	}
}
