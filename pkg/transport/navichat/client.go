// Package navichat drives navi chats over A2A from a person's own machine.
//
// navi serves each project's chats as an A2A 1.0 agent at
// <navi>/a2a/<project>/. A context is a chat and a task is one turn of it:
// a message with no contextId starts a chat, a message with one continues
// that chat, and the turn's task completes when the agent's turn ends or
// becomes input-required when it waits on a person. Client wraps a2a-go's
// client with those operations, and NewMCPServer serves them as MCP tools.
package navichat

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/a2aproject/a2a-go/v2/a2a"
	"github.com/a2aproject/a2a-go/v2/a2aclient"
	"github.com/a2aproject/a2a-go/v2/a2aclient/agentcard"
)

// LoginHeader is the header the tailnet proxy in front of navi sets to the
// caller's login. Only a client that reaches navi through a port forward
// sets it itself.
const LoginHeader = "Tailscale-User-Login"

// Config says which navi to talk to.
type Config struct {
	// NaviURL is navi's base URL, such as https://navi.example.ts.net.
	NaviURL string
	// Project is the project used when an operation names none.
	Project string
	// Projects are further projects that Projects reports.
	Projects []string
	// Login, when set, is sent in LoginHeader on each request. Only for a
	// port forward, where no tailnet proxy sets it.
	Login string
	// HTTPClient makes the requests. Nil is a client with no timeout, since
	// a wait streams for as long as a turn runs.
	HTTPClient *http.Client
}

// Client drives the chats of one navi. It is safe for concurrent use.
type Client struct {
	config     Config
	base       *url.URL
	httpClient *http.Client

	mu      sync.Mutex
	clients map[string]*a2aclient.Client
}

// New returns a client of the navi in config. It does not connect until an
// operation needs a project's agent.
func New(config Config) (*Client, error) {
	if config.NaviURL == "" {
		return nil, errors.New("no navi URL: set navi-url in the config file, such as https://navi.<tailnet>.ts.net")
	}
	base, err := url.Parse(strings.TrimSuffix(config.NaviURL, "/"))
	if err != nil || (base.Scheme != "http" && base.Scheme != "https") || base.Host == "" {
		return nil, fmt.Errorf("navi-url %q is not an http or https URL", config.NaviURL)
	}
	httpClient := config.HTTPClient
	if httpClient == nil {
		httpClient = &http.Client{}
	}
	if config.Login != "" {
		transport := httpClient.Transport
		if transport == nil {
			transport = http.DefaultTransport
		}
		withLogin := *httpClient
		withLogin.Transport = loginTransport{login: config.Login, next: transport}
		httpClient = &withLogin
	}
	return &Client{config: config, base: base, httpClient: httpClient, clients: map[string]*a2aclient.Client{}}, nil
}

type loginTransport struct {
	login string
	next  http.RoundTripper
}

func (t loginTransport) RoundTrip(request *http.Request) (*http.Response, error) {
	request = request.Clone(request.Context())
	request.Header.Set(LoginHeader, t.login)
	return t.next.RoundTrip(request)
}

// project is name, or the default project when name is empty.
func (c *Client) project(name string) (string, error) {
	if name != "" {
		return name, nil
	}
	if c.config.Project == "" {
		return "", errors.New("no project named and no default project: pass a project, or set project in the config file (chat_projects lists the known ones)")
	}
	return c.config.Project, nil
}

// cardURL is the agent card URL of project.
func (c *Client) cardURL(project string) string {
	return c.base.JoinPath("a2a", project, ".well-known", "agent-card.json").String()
}

func (c *Client) resolve(ctx context.Context, project string) (*a2a.AgentCard, error) {
	card, err := agentcard.NewResolver(c.httpClient).Resolve(ctx, c.cardURL(project))
	if err != nil {
		var status *agentcard.ErrStatusNotOK
		if errors.As(err, &status) && status.StatusCode == http.StatusNotFound {
			return nil, fmt.Errorf("navi has no project %q at %s: check the name with chat_projects", project, c.config.NaviURL)
		}
		if errors.As(err, &status) && (status.StatusCode == http.StatusUnauthorized || status.StatusCode == http.StatusForbidden) {
			return nil, fmt.Errorf("navi denied access to project %q (%s): reach navi through the tailnet, or set login-header when using a port forward", project, status.Status)
		}
		return nil, fmt.Errorf("read the agent card of project %q from %s: %w", project, c.cardURL(project), err)
	}
	return card, nil
}

// agent is the A2A client of project, made once from its card.
func (c *Client) agent(ctx context.Context, project string) (*a2aclient.Client, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if client, ok := c.clients[project]; ok {
		return client, nil
	}
	card, err := c.resolve(ctx, project)
	if err != nil {
		return nil, err
	}
	client, err := a2aclient.NewFromCard(ctx, card,
		a2aclient.WithDefaultsDisabled(),
		a2aclient.WithJSONRPCTransport(c.httpClient),
		a2aclient.WithRESTTransport(c.httpClient),
	)
	if err != nil {
		return nil, fmt.Errorf("connect to the agent of project %q: %w", project, err)
	}
	c.clients[project] = client
	return client, nil
}

// Close releases the agents' clients.
func (c *Client) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	var errs []error
	for project, client := range c.clients {
		if err := client.Destroy(); err != nil {
			errs = append(errs, fmt.Errorf("close the client of project %q: %w", project, err))
		}
		delete(c.clients, project)
	}
	return errors.Join(errs...)
}

// Project is a project as Projects reports it.
type Project struct {
	Name string `json:"name"`
	// Default says operations that name no project use this one.
	Default bool `json:"default,omitempty"`
	// Reachable says its agent card was read.
	Reachable   bool   `json:"reachable"`
	Description string `json:"description,omitempty"`
	// Error says why the card was not read.
	Error string `json:"error,omitempty"`
}

// Projects reports the default project and the configured projects, each
// checked by reading its agent card.
func (c *Client) Projects(ctx context.Context) []Project {
	var names []string
	seen := map[string]bool{}
	for _, name := range append([]string{c.config.Project}, c.config.Projects...) {
		if name == "" || seen[name] {
			continue
		}
		seen[name] = true
		names = append(names, name)
	}
	projects := make([]Project, len(names))
	var wg sync.WaitGroup
	for i, name := range names {
		wg.Add(1)
		go func() {
			defer wg.Done()
			project := Project{Name: name, Default: name == c.config.Project}
			card, err := c.resolve(ctx, name)
			if err != nil {
				project.Error = err.Error()
			} else {
				project.Reachable = true
				project.Description = card.Description
			}
			projects[i] = project
		}()
	}
	wg.Wait()
	return projects
}

// State is a turn's state, A2A's task state without its prefix:
// submitted, working, input-required, completed, failed, canceled, rejected
// or auth-required.
type State string

func stateOf(state a2a.TaskState) State {
	return State(strings.ReplaceAll(strings.ToLower(strings.TrimPrefix(string(state), "TASK_STATE_")), "_", "-"))
}

// Turn is one turn of a chat: the agent's answer to one message.
type Turn struct {
	Project string `json:"project"`
	// Chat is the chat's ID, the A2A contextId.
	Chat string `json:"chat"`
	// ID is the turn's ID, the A2A task ID.
	ID    string `json:"turn"`
	State State  `json:"state"`
	// Done says the turn has ended: completed, failed, canceled or rejected.
	Done bool `json:"done"`
	// NeedsInput says the agent waits on a person. Answer it with Send.
	NeedsInput bool `json:"needs_input,omitempty"`
	// Text is the agent's result or question, or the failure. navi keeps the
	// text of a chat's last turn only.
	Text string `json:"text,omitempty"`
}

func turnOf(project string, task *a2a.Task) Turn {
	turn := Turn{
		Project:    project,
		Chat:       task.ContextID,
		ID:         string(task.ID),
		State:      stateOf(task.Status.State),
		Done:       task.Status.State.Terminal(),
		NeedsInput: task.Status.State == a2a.TaskStateInputRequired,
	}
	if task.Status.Message != nil {
		turn.Text = partsText(task.Status.Message.Parts)
	}
	if turn.Text == "" {
		for _, artifact := range task.Artifacts {
			if text := partsText(artifact.Parts); text != "" {
				turn.Text = text
			}
		}
	}
	return turn
}

func partsText(parts a2a.ContentParts) string {
	var texts []string
	for _, part := range parts {
		if part == nil {
			continue
		}
		if text := part.Text(); text != "" {
			texts = append(texts, text)
		}
	}
	return strings.Join(texts, "\n")
}

// stops says a wait for a turn ends at state.
func stops(state a2a.TaskState) bool {
	return state.Terminal() || state == a2a.TaskStateInputRequired
}

// Start starts a chat in project with its first message and returns the
// turn that answers it, without waiting for the turn.
func (c *Client) Start(ctx context.Context, project, text string) (Turn, error) {
	return c.send(ctx, project, "", text)
}

// Send sends a message to a chat and returns the turn that answers it,
// without waiting for the turn. A chat runs one turn at a time, so a
// message sent while one runs waits in the chat's queue.
func (c *Client) Send(ctx context.Context, project, chat, text string) (Turn, error) {
	if chat == "" {
		return Turn{}, errors.New("no chat named: pass the chat ID that chat_start returned, or call chat_start to begin a new chat")
	}
	return c.send(ctx, project, chat, text)
}

func (c *Client) send(ctx context.Context, project, chat, text string) (Turn, error) {
	if strings.TrimSpace(text) == "" {
		return Turn{}, errors.New("the message has no text: pass the text to send")
	}
	project, err := c.project(project)
	if err != nil {
		return Turn{}, err
	}
	agent, err := c.agent(ctx, project)
	if err != nil {
		return Turn{}, err
	}
	message := a2a.NewMessage(a2a.MessageRoleUser, a2a.NewTextPart(text))
	message.ContextID = chat
	result, err := agent.SendMessage(ctx, &a2a.SendMessageRequest{
		Message: message,
		Config:  &a2a.SendMessageConfig{ReturnImmediately: true},
	})
	if err != nil {
		return Turn{}, operationError("send the message", project, chat, err)
	}
	task, ok := result.(*a2a.Task)
	if !ok {
		return Turn{}, fmt.Errorf("navi answered the message in project %q with a %T, not a turn", project, result)
	}
	return turnOf(project, task), nil
}

// Get reads a turn.
func (c *Client) Get(ctx context.Context, project, turn string) (Turn, error) {
	project, err := c.project(project)
	if err != nil {
		return Turn{}, err
	}
	agent, err := c.agent(ctx, project)
	if err != nil {
		return Turn{}, err
	}
	task, err := agent.GetTask(ctx, &a2a.GetTaskRequest{ID: a2a.TaskID(turn)})
	if err != nil {
		return Turn{}, turnError("read", project, turn, err)
	}
	return turnOf(project, task), nil
}

// Cancel stops a running turn, which is the chat's stop. The chat stays
// open and takes further messages.
func (c *Client) Cancel(ctx context.Context, project, turn string) (Turn, error) {
	project, err := c.project(project)
	if err != nil {
		return Turn{}, err
	}
	agent, err := c.agent(ctx, project)
	if err != nil {
		return Turn{}, err
	}
	task, err := agent.CancelTask(ctx, &a2a.CancelTaskRequest{ID: a2a.TaskID(turn)})
	if err != nil {
		if errors.Is(err, a2a.ErrTaskNotCancelable) {
			return Turn{}, fmt.Errorf("turn %s has already ended, so there is nothing to cancel: read it with chat_get", turn)
		}
		return Turn{}, turnError("cancel", project, turn, err)
	}
	return turnOf(project, task), nil
}

// MaxList is the most turns List returns at once.
const MaxList = 100

// ListRequest says which turns List returns.
type ListRequest struct {
	Project string
	// Chat, when set, lists the chat's turns. Otherwise List returns the
	// latest turn of each chat in the project, newest first.
	Chat string
	// Limit is the most turns returned. Zero is 20, and it is at most
	// MaxList.
	Limit int
	// PageToken continues an earlier list, when the server pages.
	PageToken string
}

// TurnList is a page of turns.
type TurnList struct {
	Turns []Turn `json:"turns"`
	// Total is the number of turns before the limit.
	Total int `json:"total"`
	// NextPageToken continues the list, when the server pages and more remain.
	NextPageToken string `json:"next_page_token,omitempty"`
}

// List lists the turns of a chat, or the latest turn of each chat in a
// project.
func (c *Client) List(ctx context.Context, request ListRequest) (TurnList, error) {
	project, err := c.project(request.Project)
	if err != nil {
		return TurnList{}, err
	}
	agent, err := c.agent(ctx, project)
	if err != nil {
		return TurnList{}, err
	}
	limit := request.Limit
	if limit <= 0 {
		limit = 20
	}
	limit = min(limit, MaxList)
	response, err := agent.ListTasks(ctx, &a2a.ListTasksRequest{ContextID: request.Chat, PageSize: limit, PageToken: request.PageToken})
	if err != nil {
		return TurnList{}, operationError("list turns", project, request.Chat, err)
	}
	list := TurnList{Turns: []Turn{}, Total: response.TotalSize, NextPageToken: response.NextPageToken}
	for _, task := range response.Tasks {
		if len(list.Turns) == limit {
			break
		}
		list.Turns = append(list.Turns, turnOf(project, task))
	}
	return list, nil
}

// Waited is what Wait returns.
type Waited struct {
	// Turn is the first turn that ended or needs input. It is nil when the
	// wait timed out.
	Turn *Turn `json:"turn,omitempty"`
	// TimedOut says no turn ended or needed input in time.
	TimedOut bool `json:"timed_out"`
	// Others are the other turns as they were when the wait ended.
	Others []Turn `json:"others,omitempty"`
}

// Wait waits up to timeout for the first of turns, all of project, to end
// or need input. It follows each turn's stream rather than polling, and
// subscribes again when a stream breaks.
func (c *Client) Wait(ctx context.Context, project string, turns []string, timeout time.Duration) (Waited, error) {
	if len(turns) == 0 {
		return Waited{}, errors.New("no turns named: pass the turn IDs that chat_start or chat_send returned")
	}
	project, err := c.project(project)
	if err != nil {
		return Waited{}, err
	}
	agent, err := c.agent(ctx, project)
	if err != nil {
		return Waited{}, err
	}
	waitCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	type outcome struct {
		turn Turn
		err  error
	}
	outcomes := make(chan outcome, len(turns))
	latest := make([]Turn, len(turns))
	var mu sync.Mutex
	var wg sync.WaitGroup
	for i, id := range turns {
		wg.Add(1)
		go func() {
			defer wg.Done()
			task, err := c.follow(waitCtx, agent, a2a.TaskID(id), func(task *a2a.Task) {
				mu.Lock()
				latest[i] = turnOf(project, task)
				mu.Unlock()
			})
			if err != nil {
				if waitCtx.Err() == nil {
					outcomes <- outcome{err: turnError("wait for", project, id, err)}
				}
				return
			}
			outcomes <- outcome{turn: turnOf(project, task)}
		}()
	}

	var waited Waited
	select {
	case first := <-outcomes:
		if first.err != nil {
			cancel()
			wg.Wait()
			return Waited{}, first.err
		}
		waited.Turn = &first.turn
	case <-waitCtx.Done():
		if ctx.Err() != nil {
			return Waited{}, ctx.Err()
		}
		waited.TimedOut = true
	}
	cancel()
	wg.Wait()
	mu.Lock()
	defer mu.Unlock()
	for i, id := range turns {
		if waited.Turn != nil && waited.Turn.ID == id {
			continue
		}
		turn := latest[i]
		if turn.ID == "" {
			turn = Turn{Project: project, ID: id, State: "unknown"}
		}
		waited.Others = append(waited.Others, turn)
	}
	return waited, nil
}

// follow returns the task once it ends or needs input. seen is called with
// each state of it.
func (c *Client) follow(ctx context.Context, agent *a2aclient.Client, id a2a.TaskID, seen func(*a2a.Task)) (*a2a.Task, error) {
	// A chat can take a moment to take in a message that was just sent, and
	// until then navi does not know the turn.
	unknownUntil := time.Now().Add(unknownTurnGrace)
	for {
		task, err := agent.GetTask(ctx, &a2a.GetTaskRequest{ID: id})
		if errors.Is(err, a2a.ErrTaskNotFound) && time.Now().Before(unknownUntil) {
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-time.After(resubscribeDelay):
			}
			continue
		}
		if err != nil {
			return nil, err
		}
		seen(task)
		if stops(task.Status.State) {
			return task, nil
		}
		ended := false
		for event, err := range agent.SubscribeToTask(ctx, &a2a.SubscribeToTaskRequest{ID: id}) {
			if err != nil {
				break
			}
			var status a2a.TaskStatus
			switch event := event.(type) {
			case *a2a.Task:
				status = event.Status
			case *a2a.TaskStatusUpdateEvent:
				status = event.Status
			default:
				continue
			}
			if ended = stops(status.State); ended {
				break
			}
		}
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		if ended {
			// The stream's last update carries the state. Read the task for
			// its result text as well.
			continue
		}
		// The stream ended or broke: read the task again, and subscribe
		// again if it still runs. A short pause keeps a server that ends
		// streams at once from being called in a tight loop.
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(resubscribeDelay):
		}
	}
}

// resubscribeDelay is the pause before following a turn again after its
// stream ended.
var resubscribeDelay = 500 * time.Millisecond

// unknownTurnGrace is how long a wait takes a turn navi does not know for
// one it has not taken in yet.
const unknownTurnGrace = 10 * time.Second

// operationError describes a failed operation on a project or a chat and
// what to do next.
func operationError(operation, project, chat string, err error) error {
	where := fmt.Sprintf("project %q", project)
	if chat != "" {
		where = fmt.Sprintf("chat %s in project %q", chat, project)
	}
	switch {
	case errors.Is(err, a2a.ErrUnauthorized), errors.Is(err, a2a.ErrUnauthenticated):
		return fmt.Errorf("%s: navi denied it in %s (%v): ask a navi admin for modify access to the project", operation, where, err)
	case errors.Is(err, a2a.ErrTaskNotFound):
		return fmt.Errorf("%s: navi has no %s (%v): check the chat ID with chat_list", operation, where, err)
	case errors.Is(err, a2a.ErrInvalidParams):
		return fmt.Errorf("%s in %s: %v", operation, where, err)
	}
	return fmt.Errorf("%s in %s: %w", operation, where, err)
}

// turnError describes a failed operation on a turn and what to do next.
func turnError(operation, project, turn string, err error) error {
	switch {
	case errors.Is(err, a2a.ErrTaskNotFound):
		return fmt.Errorf("%s turn %s: project %q has no such turn: use the turn ID that chat_start or chat_send returned, or find it with chat_list", operation, turn, project)
	case errors.Is(err, a2a.ErrUnauthorized), errors.Is(err, a2a.ErrUnauthenticated):
		return fmt.Errorf("%s turn %s: navi denied it (%v): ask a navi admin for access to project %q", operation, turn, err, project)
	}
	return fmt.Errorf("%s turn %s in project %q: %w", operation, turn, project, err)
}
