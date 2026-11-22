package chat

import (
	"errors"
	"fmt"
	"slices"
	"strings"

	"go.temporal.io/sdk/workflow"

	"github.com/gfx-labs/protocol7/pkg/domain/navi"
	"github.com/gfx-labs/protocol7/pkg/domain/scope"
	"github.com/gfx-labs/protocol7/pkg/p7"
	"github.com/gfx-labs/protocol7/pkg/support/prompt"
)

// Registry is where Register puts the workflows and activities. *p7.Worker
// is one.
type Registry interface {
	RegisterWorkflow(name string, function any)
	RegisterActivity(name string, function any)
}

// Options are what an app adds to the base chat. The zero value is a chat
// with the base tools, DefaultInstructions and escalations that go to a
// person at once.
type Options struct {
	// Tools returns the app's tools for one run of a chat. It is called at
	// the start of each run, after Continue-As-New too, and must return the
	// same tools for the same State, because a session keeps the tools it
	// was opened with. The base adds escalate and the child chat tools to
	// every chat that is not an escalation handler's (Request.Handler). It also
	// adds project_request, where navi lets the chat's project ask others, and
	// project_request_reply to a chat that a request started.
	Tools func(chat *Chat) []p7.Tool
	// Instructions is the hidden context of a chat's first turn. Nil uses
	// DefaultInstructions. An error fails the turn and leaves the chat open.
	Instructions func(request *Request) (string, error)
	// Brief is added to the first turn's context after the instructions. It
	// may run activities. An empty result adds nothing.
	Brief func(ctx workflow.Context, chat *Chat) string
	// TurnContext is added to the context of every turn, after the picks and
	// a person's guidance. An empty result adds nothing.
	TurnContext func(ctx workflow.Context, chat *Chat, message *Message) string
	// EscalationHandoff names the activity that gives a new escalation to the
	// app's handler, which then has handlerDeadline to decide before a person
	// gets it. The activity takes an EscalationRequest and runs on TaskQueue.
	// Empty sends every escalation to a person at once.
	EscalationHandoff string
	// ValidateDecision, when set, refuses a decision of the app's handler
	// (UpdateEscalationDecide) that the base accepts. An app may require a
	// resolution to name a record of its own, for example.
	ValidateDecision func(decision *EscalationDecision) error
	// Activities are the app's activities, by name. Register registers them
	// with the workflows.
	Activities map[string]any
}

// Chat is one run of a chat workflow, as the app's tools and hooks see it.
type Chat struct {
	// State is the run's state. Tools may change the fields kept for
	// extensions (Board, Probes, Operator, Learnings, LastLearning).
	State *State
}

// Escalate raises an escalation for the chat and returns its ID, as the
// escalate tool does.
func (c *Chat) Escalate(ctx workflow.Context, request *EscalationRequest) (string, error) {
	return raiseEscalation(ctx, c.State, request)
}

// Spawn starts a child chat with a task and returns its ID, as chat_spawn
// does. A non-nil escalation makes the child an escalation chat.
func (c *Chat) Spawn(ctx workflow.Context, task, title string, escalation *Escalation) (string, error) {
	return spawnChild(ctx, c.State, task, title, escalation)
}

// Register registers the chat workflow (WorkflowName) and the escalation
// workflow (EscalationWorkflowName) with the options, and the options'
// activities, on registry.
func Register(registry Registry, options Options) error {
	if registry == nil {
		return errors.New("register chat: no registry")
	}
	names := make([]string, 0, len(options.Activities))
	for name, function := range options.Activities {
		if name == "" || function == nil {
			return fmt.Errorf("register chat: activity %q needs a name and a function", name)
		}
		names = append(names, name)
	}
	if options.EscalationHandoff != "" && options.Activities != nil {
		if _, ok := options.Activities[options.EscalationHandoff]; !ok {
			return fmt.Errorf("register chat: the escalation handoff %q is not among the activities", options.EscalationHandoff)
		}
	}
	registry.RegisterWorkflow(WorkflowName, NewWorkflow(options))
	registry.RegisterWorkflow(EscalationWorkflowName, NewEscalationWorkflow(options))
	registry.RegisterWorkflow(ProjectRequestWorkflowName, ProjectRequestWorkflow)
	// In name order, so that two workers register alike.
	slices.Sort(names)
	for _, name := range names {
		registry.RegisterActivity(name, options.Activities[name])
	}
	return nil
}

// NewWorkflow is the chat workflow with the options. Register registers it
// under WorkflowName. A replayer registers it the same way.
func NewWorkflow(options Options) func(workflow.Context, *navi.ChatState) (*Report, error) {
	return func(ctx workflow.Context, start *navi.ChatState) (*Report, error) {
		state := NewState(start)
		state.options = &options
		return run(ctx, *state)
	}
}

// NewEscalationWorkflow is the escalation workflow with the options. Register
// registers it under EscalationWorkflowName.
func NewEscalationWorkflow(options Options) func(workflow.Context, *EscalationState) (*EscalationState, error) {
	return func(ctx workflow.Context, state *EscalationState) (*EscalationState, error) {
		return runEscalation(ctx, state, options.EscalationHandoff, options.ValidateDecision)
	}
}

// ProjectRequestWorkflow is the project request workflow. Register registers
// it under ProjectRequestWorkflowName.
func ProjectRequestWorkflow(ctx workflow.Context, state *ProjectRequestState) (*ProjectRequestState, error) {
	return runProjectRequest(ctx, state)
}

// optionsOf are the worker's options of a run. A State that did not come
// through NewWorkflow has the zero options.
func optionsOf(state *State) Options {
	if state.options == nil {
		return Options{}
	}
	return *state.options
}

// chatTools are the tools of a chat's session: the app's, then the base's.
// An escalation handler's chat gets only the app's, so that it never
// escalates to itself or starts chats.
func chatTools(state *State) []p7.Tool {
	var tools []p7.Tool
	if extend := optionsOf(state).Tools; extend != nil {
		tools = append(tools, extend(&Chat{State: state})...)
	}
	if state.Request.Handler {
		return tools
	}
	tools = append(tools, coordinationTools(state)...)
	tools = append(tools, escalationTools(state)...)
	return append(tools, requestTools(state)...)
}

// firstContext is the hidden context of a chat's first turn: the
// instructions and the app's brief.
func firstContext(ctx workflow.Context, state *State) (string, error) {
	options := optionsOf(state)
	instructions := DefaultInstructions
	if options.Instructions != nil {
		instructions = options.Instructions
	}
	text, err := instructions(state.Request)
	if err != nil {
		return "", fmt.Errorf("build the chat's instructions: %w", err)
	}
	if extra := requestInstructions(state.Request); extra != "" {
		text += "\n\n" + extra
	}
	if options.Brief != nil {
		if brief := options.Brief(ctx, &Chat{State: state}); brief != "" {
			text += "\n\n" + brief
		}
	}
	return text, nil
}

// EscalateInstructions tell the agent when to escalate. DefaultInstructions
// include them, and an app's instructions may.
const EscalateInstructions = "- If you cannot go on without something outside your reach (a credential, access, a decision, a review or a deployment by a person), call the tool escalate instead of ending the turn and waiting for something to change. Say what you need, why you cannot do it yourself, what you already tried and what you will do once you have it. Then end your turn: the answer arrives in this chat as a message, and you go on from it.\n"

// ScopeInstructions tell the agent what its GitHub scope is, or are empty
// without one. The scope itself is enforced where the agent cannot change
// it: by what is cloned, by the credential and by the pool of the session.
func ScopeInstructions(held *scope.Scope) string {
	if held == nil {
		return ""
	}
	permissions := make([]string, 0, len(held.Permissions))
	for name, level := range held.Permissions {
		permissions = append(permissions, name+": "+level)
	}
	slices.Sort(permissions)
	return fmt.Sprintf("\nYour GitHub access is the lower of the project's and your person's. Repositories: %s. Permissions: %s (%s). It cannot be widened from here: do not try to reach any other repository, and say that an admin has to change it if you need more.\n",
		strings.Join(held.Repositories, ", "), strings.Join(permissions, ", "), scope.LevelOf(held))
}

// DefaultInstructions are the instructions of a chat whose app gives none:
// where the clones are, the branch, the scope, and when to escalate.
func DefaultInstructions(request *Request) (string, error) {
	repositories := make([]string, 0, len(request.Repositories))
	for _, repository := range ScopedRepositories(request.Repositories, request.Scope) {
		repositories = append(repositories, fmt.Sprintf("%s/ (%s)", repository.Name, repository.Url))
	}
	text := prompt.New(prompt.Vars{"Branch": Branch(request.Id), "Project": request.Project}).
		P("This is a p7 chat. A person talks to you through a web page and sees your messages, tool calls and plan.").
		P("Your working directory holds a clone of each repository you work on:").
		List(repositories...).
		P("Each clone is on the branch {{.Branch}}, made for this chat.").
		Raw(ScopeInstructions(request.Scope)).
		When(request.Restricted, func(b *prompt.Builder) {
			b.P(`The person in this chat may change the project "{{.Project}}" and nothing else. Work only in the repositories listed above.`)
		}).
		When(request.Parent != "", func(b *prompt.Builder) {
			b.P("You are a child chat carrying out a task for a parent chat. Complete this task and end the turn with a short result summary. If you need guidance, call chat_attention and end the turn with the question.")
		}).
		Raw(EscalateInstructions).
		Bullets(`
			Commit your work on that branch, in small commits with plain messages.
			Push the branch only when the person asks. Never push to the default branch.
			Run the repository's own checks before you say a change works, and say what you ran.
		`)
	if err := text.Err(); err != nil {
		return "", err
	}
	return text.String(), nil
}
