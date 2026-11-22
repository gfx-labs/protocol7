// Package panel runs a panel of participants through rounds of independent
// and then mutual work, inside one workflow.
//
// Each participant of a round is a child workflow that the caller names. The
// panel starts the round's children together and waits for all of them
// before it calls the caller again, so that no participant's answer reaches
// another before every answer of the round exists. It then applies the stop
// rules, a budget that holds a reserve back, and a floor on how many vendors
// must still take part.
//
// The package holds the mechanics and no text. What a participant is asked,
// how its answer is read, when the panel agrees and what a round costs are
// the caller's, through Config. Temporal does the rest: retry, resume and
// cancellation are those of child workflows, and the history is the record of
// each round.
package panel

import (
	"errors"
	"fmt"
	"slices"
	"strconv"

	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"
)

const (
	// SignalStop asks the panel to end at once. The round that runs is
	// cancelled, and the result says StopSignal.
	SignalStop = "panel.stop"
	// QueryState returns the panel's Status.
	QueryState = "panel.state"
)

// Why a panel stopped.
const (
	// StopAgreement: a rule of the caller said the panel agrees.
	StopAgreement = "agreement"
	// StopNoProgress: a round changed no participant's answer.
	StopNoProgress = "no_progress"
	// StopRoundLimit: the panel ran every round it may.
	StopRoundLimit = "round_limit"
	// StopCost: the next round's ceiling does not fit what the budget has
	// left after its reserve.
	StopCost = "cost"
	// StopSignal: SignalStop arrived.
	StopSignal = "signal"
	// StopTooFewVendors: fewer vendors than Config.MinVendors still have a
	// participant.
	StopTooFewVendors = "too_few_vendors"
)

// Money is an amount in millionths of a dollar, so that sums are exact.
type Money int64

// Dollars is the amount in dollars, for a person to read.
func (m Money) Dollars() float64 { return float64(m) / 1e6 }

// Budget bounds what a panel may spend.
type Budget struct {
	// Cap is the most the panel and its caller may spend in all.
	Cap Money
	// Reserve is the part of Cap that the panel does not spend on rounds, so
	// that the caller can still do its last step, such as writing the record,
	// when the rounds used up the rest. It is at most Cap.
	Reserve Money
}

// Fits reports whether a step that costs at most ceiling can run after spent
// has been spent, without touching the reserve.
func (b Budget) Fits(spent, ceiling Money) bool { return spent+ceiling <= b.Cap-b.Reserve }

// Participant is one voice of the panel.
type Participant struct {
	// Name is unique in the panel.
	Name string `json:"name"`
	// Vendor says who makes the participant's model. The panel needs
	// Config.MinVendors different ones to go on.
	Vendor string `json:"vendor"`
}

// Position is what a participant said in a round.
type Position[P any] struct {
	Participant string `json:"participant"`
	Vendor      string `json:"vendor"`
	Round       int    `json:"round"`
	Value       P      `json:"value"`
	Spent       Money  `json:"spent"`
}

// Outcome is the result of a participant's child workflow.
type Outcome[P any] struct {
	Position P     `json:"position"`
	Spent    Money `json:"spent"`
}

// Failure is a participant's round that ended with an error. The participant
// takes no further part.
type Failure struct {
	Participant string `json:"participant"`
	Round       int    `json:"round"`
	Error       string `json:"error"`
	// Charged is what the panel counted for it: its ceiling, because what a
	// failed child spent is not known.
	Charged Money `json:"charged"`
}

// Child says which child workflow runs a participant's round.
type Child struct {
	// Workflow is the registered name or function of the child workflow. It
	// returns an Outcome of the panel's position type.
	Workflow any
	// Input is the child's argument.
	Input any
	// Options are the child's options. An empty WorkflowID becomes
	// <parent ID>/panel/<round>/<participant>, and a zero ParentClosePolicy
	// becomes REQUEST_CANCEL, so that the panel's end ends its children.
	Options workflow.ChildWorkflowOptions
}

// State is what a rule and the caller see after a round.
type State[P any] struct {
	// Round is the round that just ended, from 0.
	Round int
	// Rounds are the rounds so far, each with the positions that came in.
	Rounds [][]Position[P]
	// Latest is each live participant's newest position, in participant order.
	Latest []Position[P]
	Failed []Failure
	Spent  Money
}

// Rule decides whether the panel stops after a round. It runs in workflow
// code, so it must be deterministic.
type Rule[P any] func(State[P]) (reason string, stop bool)

// Config says how a panel runs. Child is required.
type Config[P any] struct {
	Participants []Participant
	// MinVendors is how many different vendors must still have a live
	// participant for the panel to go on. Zero is one.
	MinVendors int
	// MaxRounds is the rounds that follow the first round, in which each
	// participant answers alone. Zero runs the first round only.
	MaxRounds int
	Budget    Budget
	// Child returns the child workflow of a participant's round. others are
	// the other live participants' newest positions as of the end of the
	// previous round, empty in round 0. The caller decides what of them the
	// participant sees.
	Child func(p Participant, round int, others []Position[P]) (Child, error)
	// Ceiling is the most a participant's round may cost. The panel does not
	// start a round whose ceilings do not fit the budget.
	Ceiling func(p Participant, round int) Money
	// Rules are the caller's tests of agreement, checked in order after each
	// round. The first to stop ends the panel with StopAgreement, and its
	// reason is the result's Detail.
	Rules []Rule[P]
	// Same says whether two positions of one participant are the same, which
	// lets the panel see a round that changed nothing. Nil turns the check off.
	Same func(a, b P) bool
	// Spent is what the caller has spent before the panel, which counts
	// against the budget.
	Spent Money
	// OnState is called, in workflow code, after each phase change, so that
	// the caller can set search attributes or record progress.
	OnState func(ctx workflow.Context, status Status)
}

// Status is the panel's progress, which the query returns.
type Status struct {
	// Phase is "running", "waiting" between rounds, or "done".
	Phase string `json:"phase"`
	// Round is the round that runs or just ended. It is -1 before the first.
	Round int `json:"round"`
	// Live are the participants that still take part.
	Live   []string  `json:"live"`
	Failed []Failure `json:"failed,omitempty"`
	Spent  Money     `json:"spent"`
	// Stop is why the panel ended, once it did.
	Stop string `json:"stop,omitempty"`
}

// Result is how a panel ended.
type Result[P any] struct {
	// Rounds are the positions of each round, in participant order.
	Rounds [][]Position[P] `json:"rounds"`
	Stop   string          `json:"stop"`
	// Detail is the rule's reason, when a rule stopped the panel.
	Detail string    `json:"detail,omitempty"`
	Failed []Failure `json:"failed,omitempty"`
	Spent  Money     `json:"spent"`
}

// Latest is each participant's newest position, in the order of the
// participants. A participant that never answered has none.
func (r Result[P]) Latest() []Position[P] {
	var latest []Position[P]
	seen := make(map[string]bool)
	for index := len(r.Rounds) - 1; index >= 0; index-- {
		for _, position := range r.Rounds[index] {
			if !seen[position.Participant] {
				seen[position.Participant] = true
				latest = append(latest, position)
			}
		}
	}
	slices.Reverse(latest)
	return latest
}

func (c Config[P]) validate() error {
	if len(c.Participants) == 0 {
		return errors.New("panel: no participants")
	}
	if c.Child == nil || c.Ceiling == nil {
		return errors.New("panel: Child and Ceiling are required")
	}
	if c.Budget.Reserve < 0 || c.Budget.Reserve > c.Budget.Cap {
		return fmt.Errorf("panel: the reserve %d is outside the cap %d", c.Budget.Reserve, c.Budget.Cap)
	}
	names := make(map[string]bool)
	vendors := make(map[string]bool)
	for _, p := range c.Participants {
		if p.Name == "" || p.Vendor == "" {
			return errors.New("panel: a participant has no name or no vendor")
		}
		if names[p.Name] {
			return fmt.Errorf("panel: two participants are named %q", p.Name)
		}
		names[p.Name] = true
		vendors[p.Vendor] = true
	}
	if len(vendors) < c.minVendors() {
		return fmt.Errorf("panel: %d vendors are needed and the participants have %d", c.minVendors(), len(vendors))
	}
	return nil
}

func (c Config[P]) minVendors() int { return max(c.MinVendors, 1) }

// Run runs the panel and returns how it ended. An error is returned only for
// a configuration that is invalid, a Child callback that fails, or the
// workflow's own cancellation. A participant that fails is recorded in the
// result.
func Run[P any](ctx workflow.Context, config Config[P]) (Result[P], error) {
	var result Result[P]
	if err := config.validate(); err != nil {
		return result, err
	}
	run := &runner[P]{config: config, status: Status{Phase: "running", Round: -1, Spent: config.Spent}}
	run.live = slices.Clone(config.Participants)
	run.stops = workflow.GetSignalChannel(ctx, SignalStop)
	if err := workflow.SetQueryHandler(ctx, QueryState, func() (Status, error) { return run.status, nil }); err != nil {
		return result, fmt.Errorf("panel: set the query handler: %w", err)
	}
	run.publish(ctx)
	return run.run(ctx)
}

type runner[P any] struct {
	config Config[P]
	status Status
	live   []Participant
	stops  workflow.ReceiveChannel
	state  State[P]
	// signalled is set once a stop signal was taken.
	signalled bool
}

func (r *runner[P]) publish(ctx workflow.Context) {
	r.status.Live = r.status.Live[:0]
	for _, p := range r.live {
		r.status.Live = append(r.status.Live, p.Name)
	}
	r.status.Failed = r.state.Failed
	r.status.Spent = r.state.Spent
	if r.config.OnState != nil {
		r.config.OnState(ctx, r.status)
	}
}

func (r *runner[P]) vendors() int {
	seen := make(map[string]bool)
	for _, p := range r.live {
		seen[p.Vendor] = true
	}
	return len(seen)
}

func (r *runner[P]) ceiling(round int) Money {
	var total Money
	for _, p := range r.live {
		total += r.config.Ceiling(p, round)
	}
	return total
}

func (r *runner[P]) finish(ctx workflow.Context, stop, detail string) (Result[P], error) {
	r.status.Phase, r.status.Stop = "done", stop
	r.publish(ctx)
	return Result[P]{Rounds: r.state.Rounds, Stop: stop, Detail: detail, Failed: r.state.Failed, Spent: r.state.Spent}, nil
}

func (r *runner[P]) run(ctx workflow.Context) (Result[P], error) {
	r.state.Spent = r.config.Spent
	for round := 0; ; round++ {
		if r.stopped() {
			return r.finish(ctx, StopSignal, "")
		}
		if !r.config.Budget.Fits(r.state.Spent, r.ceiling(round)) {
			return r.finish(ctx, StopCost, fmt.Sprintf("round %d needs up to %.2f dollars and %.2f remain before the reserve",
				round, r.ceiling(round).Dollars(), (r.config.Budget.Cap-r.config.Budget.Reserve-r.state.Spent).Dollars()))
		}
		r.status.Phase, r.status.Round = "running", round
		r.publish(ctx)
		positions, err := r.round(ctx, round)
		if err != nil {
			return Result[P]{}, err
		}
		r.state.Round = round
		r.state.Rounds = append(r.state.Rounds, positions)
		r.state.Latest = r.latest()
		r.status.Phase = "waiting"
		r.publish(ctx)
		if r.stopped() {
			return r.finish(ctx, StopSignal, "")
		}
		if r.vendors() < r.config.minVendors() {
			return r.finish(ctx, StopTooFewVendors, fmt.Sprintf("%d vendors remain and %d are needed", r.vendors(), r.config.minVendors()))
		}
		for _, rule := range r.config.Rules {
			if reason, stop := rule(r.state); stop {
				return r.finish(ctx, StopAgreement, reason)
			}
		}
		if round > 0 && r.unchanged(round) {
			return r.finish(ctx, StopNoProgress, "")
		}
		if round >= r.config.MaxRounds {
			return r.finish(ctx, StopRoundLimit, "")
		}
	}
}

// stopped reports whether a stop signal came, taking one that waits.
func (r *runner[P]) stopped() bool {
	if !r.signalled {
		var reason string
		r.signalled = r.stops.ReceiveAsync(&reason)
	}
	return r.signalled
}

// latest is each live participant's newest position.
func (r *runner[P]) latest() []Position[P] {
	var latest []Position[P]
	for _, p := range r.live {
		for index := len(r.state.Rounds) - 1; index >= 0; index-- {
			found := false
			for _, position := range r.state.Rounds[index] {
				if position.Participant == p.Name {
					latest = append(latest, position)
					found = true
					break
				}
			}
			if found {
				break
			}
		}
	}
	return latest
}

// unchanged reports whether every participant that answered in round also
// answered in the round before and said the same.
func (r *runner[P]) unchanged(round int) bool {
	if r.config.Same == nil || len(r.state.Rounds) < 2 {
		return false
	}
	current, before := r.state.Rounds[len(r.state.Rounds)-1], r.state.Rounds[len(r.state.Rounds)-2]
	if len(current) == 0 {
		return false
	}
	for _, position := range current {
		index := slices.IndexFunc(before, func(p Position[P]) bool { return p.Participant == position.Participant })
		if index < 0 || !r.config.Same(before[index].Value, position.Value) {
			return false
		}
	}
	return true
}

type pending[P any] struct {
	participant Participant
	future      workflow.ChildWorkflowFuture
	ceiling     Money
}

// round starts every live participant's child and waits for all of them. A
// stop signal that arrives meanwhile cancels the children.
func (r *runner[P]) round(ctx workflow.Context, round int) ([]Position[P], error) {
	others := r.state.Latest
	childCtx, cancel := workflow.WithCancel(ctx)
	defer cancel()
	var started []pending[P]
	for _, p := range r.live {
		var visible []Position[P]
		for _, other := range others {
			if other.Participant != p.Name {
				visible = append(visible, other)
			}
		}
		child, err := r.config.Child(p, round, visible)
		if err != nil {
			return nil, fmt.Errorf("panel: child of %s in round %d: %w", p.Name, round, err)
		}
		options := child.Options
		if options.WorkflowID == "" {
			options.WorkflowID = workflow.GetInfo(ctx).WorkflowExecution.ID + "/panel/" + strconv.Itoa(round) + "/" + p.Name
		}
		if options.ParentClosePolicy == enumspb.PARENT_CLOSE_POLICY_UNSPECIFIED {
			options.ParentClosePolicy = enumspb.PARENT_CLOSE_POLICY_REQUEST_CANCEL
		}
		future := workflow.ExecuteChildWorkflow(workflow.WithChildOptions(childCtx, options), child.Workflow, child.Input)
		started = append(started, pending[P]{participant: p, future: future, ceiling: r.config.Ceiling(p, round)})
	}

	// Wait for the children, or for a stop signal that ends them.
	done := 0
	selector := workflow.NewSelector(ctx)
	for index := range started {
		selector.AddFuture(started[index].future, func(workflow.Future) { done++ })
	}
	selector.AddReceive(r.stops, func(channel workflow.ReceiveChannel, _ bool) {
		var reason string
		channel.Receive(ctx, &reason)
		r.signalled = true
		cancel()
	})
	for done < len(started) {
		selector.Select(ctx)
	}

	var positions []Position[P]
	var lost []Participant
	for _, item := range started {
		var outcome Outcome[P]
		err := item.future.Get(ctx, &outcome)
		switch {
		case err == nil:
			r.state.Spent += outcome.Spent
			positions = append(positions, Position[P]{Participant: item.participant.Name, Vendor: item.participant.Vendor, Round: round, Value: outcome.Position, Spent: outcome.Spent})
		case ctx.Err() != nil:
			return nil, ctx.Err()
		case r.signalled && temporal.IsCanceledError(err):
			// Cancelled by the stop signal: not the participant's failure.
		default:
			r.state.Spent += item.ceiling
			r.state.Failed = append(r.state.Failed, Failure{Participant: item.participant.Name, Round: round, Error: err.Error(), Charged: item.ceiling})
			lost = append(lost, item.participant)
		}
	}
	for _, p := range lost {
		r.live = slices.DeleteFunc(r.live, func(q Participant) bool { return q.Name == p.Name })
	}
	return positions, nil
}
