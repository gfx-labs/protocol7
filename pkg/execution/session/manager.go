package session

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"strings"
	"time"

	acp "github.com/gfx-labs/go-acp"

	"github.com/gfx-labs/protocol7/pkg/domain"
	"github.com/gfx-labs/protocol7/pkg/execution/acpshim"
	"github.com/gfx-labs/protocol7/pkg/execution/journal"
)

// Version is sent to the harness as the client version.
const Version = "0.0.0"

// DefaultIdleLimit is how long a session's processes are kept with no turn
// running when its request names no limit.
const DefaultIdleLimit = 15 * time.Minute

const (
	// answerGrace is how long the journal may trail the answer to a prompt.
	// The shim journals an answer before it forwards it, so this only covers
	// the journal reader waking up.
	answerGrace = 10 * time.Second
	// cancelGrace bounds how long a harness may take to end a cancelled turn.
	cancelGrace = 2 * time.Minute
)

// ErrDetached is returned by RunTurn when its caller went away. The turn
// keeps running, and a repeated RunTurn with the same turn ID picks it up.
var ErrDetached = errors.New("detached from a running turn")

// ErrClosed is returned for a session that was closed.
var ErrClosed = errors.New("the session is closed")

// Harness says how the agent harness is started. It speaks ACP on its
// standard input and output.
type Harness struct {
	Command string
	Args    []string
	// Env is added to the worker's own environment (KEY=VALUE).
	Env []string
	// ModelsFlag is the flag that takes a session's model list, joined with
	// commas. towa's is "--models". Empty leaves the list out, and the
	// harness uses its own configuration.
	ModelsFlag string
}

// Config configures a Manager.
type Config struct {
	// Root holds one directory per session. It should be short: the shim's
	// socket path is under it and may be at most 107 bytes.
	Root string
	// ShimBinary is the p7acpshim command.
	ShimBinary string
	Harness    Harness
	// Publisher receives the journal's frames. Nil publishes nothing.
	Publisher Publisher
	Logger    *slog.Logger
}

// Manager runs sessions on this machine.
type Manager struct {
	config Config
	log    *slog.Logger
}

// NewManager returns a manager, creating its root directory.
func NewManager(config Config) (*Manager, error) {
	if config.Root == "" || config.ShimBinary == "" || config.Harness.Command == "" {
		return nil, errors.New("session manager: Root, ShimBinary and Harness.Command are required")
	}
	if err := os.MkdirAll(config.Root, 0o700); err != nil {
		return nil, fmt.Errorf("session manager: create root: %w", err)
	}
	logger := config.Logger
	if logger == nil {
		logger = slog.New(slog.DiscardHandler)
	}
	return &Manager{config: config, log: logger}, nil
}

// Open creates a session: its workspace with every input placed in it, its
// shim and a conversation in the harness. Opening an ID that exists returns
// the existing session.
func (m *Manager) Open(ctx context.Context, request domain.SessionRequest) (domain.SessionHandle, error) {
	var handle domain.SessionHandle
	if err := checkID(request.ID); err != nil {
		return handle, err
	}
	rec, err := m.load(request.ID)
	switch {
	case isMissing(err):
		if err := os.MkdirAll(m.workspace(request.ID), 0o755); err != nil {
			return handle, fmt.Errorf("create workspace of session %s: %w", request.ID, err)
		}
		rec = record{ID: request.ID, Request: request}
		if err := m.save(rec); err != nil {
			return handle, err
		}
	case err != nil:
		return handle, err
	case rec.Closed:
		return handle, fmt.Errorf("open session %s: %w", request.ID, ErrClosed)
	}

	// An earlier attempt may have placed some inputs. Each is recorded as it
	// is placed, so its commit is resolved once.
	for index := len(rec.Inputs); index < len(rec.Request.Inputs); index++ {
		resolved, err := materialize(ctx, m.workspace(rec.ID), rec.Request.Inputs[index])
		if err != nil {
			return handle, fmt.Errorf("open session %s: %w", rec.ID, err)
		}
		rec.Inputs = append(rec.Inputs, resolved)
		if err := m.save(rec); err != nil {
			return handle, err
		}
	}

	p := newPump(ctx, m, rec.ID, "")
	client, _, err := m.ensure(ctx, &rec)
	if err != nil {
		return handle, fmt.Errorf("open session %s: %w", rec.ID, err)
	}
	client.Close()
	if err := p.catchUp(ctx); err != nil {
		m.log.Warn("publish the journal after opening", "session", rec.ID, "error", err)
	}
	return domain.SessionHandle{ID: rec.ID, Inputs: rec.Inputs}, nil
}

// workingDirectory is where the harness starts and where its conversation's
// working directory is.
func (m *Manager) workingDirectory(rec record) (string, error) {
	if rec.Request.WorkingDirectory == "" {
		return m.workspace(rec.ID), nil
	}
	return insideWorkspace(m.workspace(rec.ID), rec.Request.WorkingDirectory)
}

// ensure makes the session ready for a prompt: its shim running, and its
// conversation open in the harness behind that shim. A shim that was started
// since the conversation was last open runs a new harness process, which has
// to load the conversation. If it cannot, a new conversation is opened and
// seed is a summary of the journal to put in front of the next prompt.
func (m *Manager) ensure(ctx context.Context, rec *record) (client *acpshim.Client, seed string, err error) {
	dir := m.shimDir(rec.ID)
	workingDirectory, err := m.workingDirectory(*rec)
	if err != nil {
		return nil, "", err
	}
	if !acpshim.Alive(dir) {
		lease := rec.Request.IdleLimit
		if lease <= 0 {
			lease = DefaultIdleLimit
		}
		args := append([]string(nil), m.config.Harness.Args...)
		if m.config.Harness.ModelsFlag != "" && len(rec.Request.Models) > 0 {
			args = append(args, m.config.Harness.ModelsFlag+"="+strings.Join(rec.Request.Models, ","))
		}
		spec := acpshim.Spec{
			Session: rec.ID, Command: m.config.Harness.Command, Args: args, Env: m.config.Harness.Env,
			WorkingDirectory: workingDirectory, Lease: lease,
		}
		if err := acpshim.WriteSpec(dir, spec); err != nil {
			return nil, "", err
		}
		if err := acpshim.Start(ctx, m.config.ShimBinary, dir); err != nil {
			return nil, "", err
		}
	}

	client, err = acpshim.Connect(ctx, dir)
	if err != nil {
		return nil, "", err
	}
	defer func() {
		if err != nil {
			client.Close()
			client = nil
		}
	}()
	initialized, err := client.Initialize(ctx, acp.Implementation{Name: "protocol7", Version: Version})
	if err != nil {
		return client, "", err
	}

	switch {
	case rec.ACPSession == "":
		// A new conversation.
	case rec.ShimPid == client.ShimPid():
		return client, "", nil
	case initialized.AgentCapabilities.LoadSession:
		err = client.LoadSession(ctx, rec.ACPSession, workingDirectory)
		if err == nil {
			rec.ShimPid = client.ShimPid()
			return client, "", m.save(*rec)
		}
		if _, refused := errors.AsType[*acp.RequestError](err); !refused {
			return client, "", err
		}
		// A load that the harness refuses would be refused on every retry.
		m.log.Warn("the harness could not load the conversation, starting a new one from a summary", "session", rec.ID, "error", err)
		seed = acpshim.Summarize(dir)
	default:
		seed = acpshim.Summarize(dir)
	}

	rec.ACPSession, err = client.NewSession(ctx, workingDirectory)
	if err != nil {
		return client, "", err
	}
	rec.ShimPid = client.ShimPid()
	err = m.save(*rec)
	return client, seed, err
}

// TurnControl lets the caller of RunTurn watch a turn and leave it.
type TurnControl struct {
	// Detach, when closed, makes RunTurn return ErrDetached and leave the
	// turn running. A worker closes it when it shuts down.
	Detach <-chan struct{}
	// Beat is called every few seconds while the turn runs.
	Beat func()
}

// RunTurn sends one prompt and returns when the turn has ended. The turn ID
// makes it repeatable: a turn that ended returns its recorded result, and a
// turn that is running is waited for, never prompted again.
//
// Cancelling ctx asks the agent to stop the turn. RunTurn then still waits
// for the turn to end and returns its result, with the stop reason the
// agent gave.
func (m *Manager) RunTurn(ctx context.Context, request domain.TurnRequest, control TurnControl) (domain.TurnResult, error) {
	var none domain.TurnResult
	if request.Turn == "" {
		return none, errors.New("run turn: a turn ID is required")
	}
	rec, err := m.load(request.Session)
	if err != nil {
		return none, err
	}
	if rec.Closed {
		return none, fmt.Errorf("run turn in session %s: %w", rec.ID, ErrClosed)
	}
	dir := m.shimDir(rec.ID)

	// The journal outlives this call. Reading and publishing it must not
	// stop when ctx is cancelled, because a cancelled turn still has to be
	// followed to its end.
	background := context.WithoutCancel(ctx)
	p := newPump(background, m, rec.ID, request.Turn)
	if err := p.catchUp(background); err != nil {
		return none, err
	}
	if p.ended() {
		return p.result(), nil
	}

	var client *acpshim.Client
	answered := make(chan error, 1)
	if p.started() {
		// An earlier attempt sent the prompt. The turn is running, or the
		// session's processes stopped during it.
		if !acpshim.Alive(dir) {
			return interrupted(p), nil
		}
		if client, err = acpshim.Connect(ctx, dir); err != nil {
			return none, err
		}
		defer client.Close()
		if err := p.catchUp(background); err != nil {
			return none, err
		}
		if p.ended() {
			return p.result(), nil
		}
		if !client.Busy() {
			// This shim was started after the prompt was sent.
			return interrupted(p), nil
		}
	} else {
		var seed string
		if client, seed, err = m.ensure(ctx, &rec); err != nil {
			return none, err
		}
		defer client.Close()
		if err := p.catchUp(background); err != nil {
			return none, err
		}
		prompt, err := promptRequest(rec.ACPSession, request, seed)
		if err != nil {
			return none, err
		}
		go func() {
			_, err := client.Prompt(prompt)
			answered <- err
		}()
	}

	followCtx, stopFollowing := context.WithCancel(background)
	ended := make(chan struct{})
	followed := make(chan error, 1)
	go func() { followed <- p.follow(followCtx, func() { close(ended) }) }()
	defer func() {
		stopFollowing()
		<-followed
	}()

	beat := time.NewTicker(5 * time.Second)
	defer beat.Stop()
	cancelled := ctx.Done()
	connectionClosed := client.Closed()
	var deadline <-chan time.Time
	var pending error
	for {
		select {
		case <-ended:
			return p.result(), nil

		case err := <-answered:
			// The answer is in the journal before it reaches this client, so
			// the journal reader ends the turn next. An answer that is an
			// error is a turn that failed, and it is in the journal too.
			answered = nil
			connectionClosed = nil
			if errors.Is(err, acpshim.ErrShimGone) {
				pending = m.connectionLost(rec.ID, err)
			} else if err != nil {
				pending = fmt.Errorf("turn %s of session %s: %w", request.Turn, rec.ID, err)
			} else {
				pending = fmt.Errorf("turn %s of session %s: the prompt was answered and the journal does not show it", request.Turn, rec.ID)
			}
			deadline = time.After(answerGrace)

		case <-connectionClosed:
			// Waiting for a turn another attempt started, and the shim closed
			// the connection.
			connectionClosed = nil
			pending = m.connectionLost(rec.ID, acpshim.ErrShimGone)
			deadline = time.After(answerGrace)

		case <-deadline:
			if errors.Is(pending, errInterrupted) {
				return interrupted(p), nil
			}
			return none, pending

		case <-cancelled:
			cancelled = nil
			if err := client.Cancel(background, rec.ACPSession); err != nil {
				return none, fmt.Errorf("cancel turn %s of session %s: %w", request.Turn, rec.ID, err)
			}
			if deadline == nil {
				pending = fmt.Errorf("turn %s of session %s: the harness did not end the turn within %s of a cancel", request.Turn, rec.ID, cancelGrace)
				deadline = time.After(cancelGrace)
			}

		case <-control.Detach:
			return none, fmt.Errorf("turn %s of session %s: %w", request.Turn, rec.ID, ErrDetached)

		case <-beat.C:
			if control.Beat != nil {
				control.Beat()
			}
		}
	}
}

var errInterrupted = errors.New("the session's processes stopped during the turn")

// connectionLost says what a closed connection means. A shim that is gone
// took the turn with it. A shim that still runs closed this connection for a
// newer one, and the turn goes on.
func (m *Manager) connectionLost(session string, cause error) error {
	if !acpshim.Alive(m.shimDir(session)) {
		return errInterrupted
	}
	return fmt.Errorf("session %s: another client took the shim's connection: %w", session, cause)
}

// interrupted is the result of a turn whose processes stopped before it
// ended. What the agent did until then is in the journal.
func interrupted(p *pump) domain.TurnResult {
	result := p.result()
	result.StopReason = domain.StopInterrupted
	result.Failure = &domain.TurnFailure{Message: errInterrupted.Error()}
	return result
}

// promptRequest builds the session/prompt of a turn. The turn ID travels in
// the request's _meta, which the journal keeps, so a later attempt can tell
// that the prompt was sent.
func promptRequest(acpSession string, request domain.TurnRequest, seed string) (acp.PromptRequest, error) {
	contextMeta := map[string]any{journal.MetaKey: journal.BlockMeta{Context: true}}
	var blocks []acp.ContentBlock
	for _, text := range []string{seed, request.Context} {
		if text != "" {
			blocks = append(blocks, acp.ContentBlock{Text: &acp.ContentBlockText{Type: "text", Text: text + "\n\n", Meta: contextMeta}})
		}
	}
	if request.Prompt == "" {
		return acp.PromptRequest{}, errors.New("run turn: the prompt is empty")
	}
	blocks = append(blocks, acp.ContentBlock{Text: &acp.ContentBlockText{Type: "text", Text: request.Prompt}})
	return acp.PromptRequest{
		SessionId: acp.SessionId(acpSession),
		Prompt:    blocks,
		Meta:      map[string]any{journal.MetaKey: journal.PromptMeta{Turn: request.Turn}},
	}, nil
}

// Close stops the session's processes and marks it closed. Its workspace and
// journal stay on disk. Closing a closed session does nothing.
func (m *Manager) Close(ctx context.Context, request domain.CloseRequest) error {
	rec, err := m.load(request.Session)
	if isMissing(err) {
		return nil
	}
	if err != nil {
		return err
	}
	if err := acpshim.Stop(ctx, m.shimDir(rec.ID)); err != nil {
		return fmt.Errorf("close session %s: %w", rec.ID, err)
	}
	if rec.Closed {
		return nil
	}
	rec.Closed = true
	return m.save(rec)
}
