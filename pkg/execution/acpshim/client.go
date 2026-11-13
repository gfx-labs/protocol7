package acpshim

import (
	"context"
	"encoding/json/jsontext"
	jsonv2 "encoding/json/v2"
	"errors"
	"fmt"
	"net"

	acp "github.com/gfx-labs/go-acp"

	"github.com/gfx-labs/acphost/pkg/acphost"
)

// ErrShimGone is returned by calls on a connection the shim closed: it
// stopped, or another client connected to the session.
var ErrShimGone = errors.New("the shim closed the connection")

// Client is one connection to a session's shim. The shim serves one client
// at a time: a new connection replaces the one before it.
//
// What the agent does is read from the journal, not from this connection.
// A journal frame has a sequence number, so a reader knows what it has seen.
type Client struct {
	connection net.Conn
	acp        *acp.ClientSideConnection
	welcome    acphost.Welcome
	served     chan struct{}
}

// Connect dials the shim of dir.
func Connect(ctx context.Context, dir string) (*Client, error) {
	connection, reader, welcome, err := acphost.Connect(ctx, dir, acphost.WithMethodPrefix(methodPrefix))
	if err != nil {
		if reason := ExitReason(dir); reason != "" {
			return nil, fmt.Errorf("connect to shim in %s: %w (the shim stopped: %s)", dir, err, reason)
		}
		return nil, fmt.Errorf("connect to shim in %s: %w", dir, err)
	}
	client := &Client{connection: connection, welcome: welcome, served: make(chan struct{})}
	client.acp = acp.NewClientSideConnection(agentRequests{}, connection, reader, nil)
	// The shim reports these to whoever is connected. Nothing here needs them.
	for _, name := range []string{"turn_end", "export_stats", "cgroup_stats"} {
		method := acphost.MethodName(methodPrefix, name)
		if err := client.acp.RegisterExtensionNotification(method, ignoreNotification); err != nil {
			_ = connection.Close()
			return nil, fmt.Errorf("register shim method %s: %w", method, err)
		}
	}
	go func() {
		defer close(client.served)
		_ = client.acp.Serve(context.Background())
	}()
	return client, nil
}

func ignoreNotification(context.Context, string, jsontext.Value) (any, *acp.RequestError) {
	return nil, nil
}

// ShimPid is the process ID of the shim. It changes when the shim is
// started again, and the harness behind a new shim has no session open.
func (c *Client) ShimPid() int { return c.welcome.Pid }

// Busy reports whether a prompt was running when this client connected.
func (c *Client) Busy() bool { return c.welcome.Busy }

// Closed is closed when the connection has ended.
func (c *Client) Closed() <-chan struct{} { return c.served }

// Close ends the connection. The shim and the harness keep running.
func (c *Client) Close() {
	_ = c.connection.Close()
	<-c.served
}

// Initialize returns the harness's answer to initialize. The shim keeps the
// first answer, so only the first client of a shim sends the request.
func (c *Client) Initialize(ctx context.Context, clientInfo acp.Implementation) (acp.InitializeResponse, error) {
	var response acp.InitializeResponse
	if len(c.welcome.Initialize) > 0 {
		if err := jsonv2.Unmarshal(c.welcome.Initialize, &response); err != nil {
			return response, fmt.Errorf("decode the shim's stored initialize result: %w", err)
		}
		return response, nil
	}
	response, err := c.acp.Initialize(ctx, acp.InitializeRequest{
		ProtocolVersion: acp.ProtocolVersion(acp.ProtocolVersionNumber),
		ClientInfo:      &clientInfo,
	})
	if err != nil {
		return response, c.explain("initialize", err)
	}
	return response, nil
}

// NewSession opens a new conversation in the harness and returns its ID.
func (c *Client) NewSession(ctx context.Context, workingDirectory string) (string, error) {
	response, err := c.acp.NewSession(ctx, acp.NewSessionRequest{Cwd: workingDirectory, McpServers: []acp.McpServer{}})
	if err != nil {
		return "", c.explain("session/new", err)
	}
	if response.SessionId == "" {
		return "", errors.New("session/new: the harness returned no session ID")
	}
	return string(response.SessionId), nil
}

// LoadSession opens an earlier conversation in a harness process that does
// not have it open. The harness replays the conversation as it loads.
func (c *Client) LoadSession(ctx context.Context, sessionID, workingDirectory string) error {
	_, err := c.acp.LoadSession(ctx, acp.LoadSessionRequest{
		SessionId: acp.SessionId(sessionID), Cwd: workingDirectory, McpServers: []acp.McpServer{},
	})
	if err != nil {
		return c.explain("session/load", err)
	}
	return nil
}

// Prompt sends one prompt and returns when the turn has ended. It does not
// take a context: ending the wait must not cancel the turn, because a turn
// outlives the worker that started it. Close ends the wait and leaves the
// turn running. Cancel asks the agent to stop it.
func (c *Client) Prompt(request acp.PromptRequest) (acp.PromptResponse, error) {
	response, err := c.acp.Prompt(context.Background(), request)
	if err != nil {
		return response, c.explain("session/prompt", err)
	}
	return response, nil
}

// Cancel asks the agent to stop the running turn of a session. The agent
// then answers the prompt with the stop reason cancelled.
func (c *Client) Cancel(ctx context.Context, sessionID string) error {
	if err := c.acp.Cancel(ctx, acp.CancelNotification{SessionId: acp.SessionId(sessionID)}); err != nil {
		return c.explain("session/cancel", err)
	}
	return nil
}

func (c *Client) explain(method string, err error) error {
	if errors.Is(err, acp.ErrClosed) {
		return fmt.Errorf("%s: %w", method, ErrShimGone)
	}
	return fmt.Errorf("%s: %w", method, err)
}

// agentRequests answers what the agent asks of its client. The client offers
// no file, terminal or elicitation capability: the agent does that work
// itself, in its own environment.
type agentRequests struct{}

func (agentRequests) SessionUpdate(context.Context, acp.SessionNotification) error { return nil }

// RequestPermission allows the call. What a session may do is set by its
// environment and grants, not by a prompt to a person who is not there.
func (agentRequests) RequestPermission(_ context.Context, request acp.RequestPermissionRequest) (acp.RequestPermissionResponse, error) {
	for _, kind := range []acp.PermissionOptionKind{acp.PermissionOptionKindAllowOnce, acp.PermissionOptionKindAllowAlways} {
		for _, option := range request.Options {
			if option.Kind == kind {
				return acp.RequestPermissionResponse{Outcome: acp.NewRequestPermissionOutcomeSelected(option.OptionId)}, nil
			}
		}
	}
	return acp.RequestPermissionResponse{Outcome: acp.NewRequestPermissionOutcomeCancelled()}, nil
}

func (agentRequests) CompleteElicitation(context.Context, acp.CompleteElicitationNotification) error {
	return nil
}

func (agentRequests) CreateElicitation(context.Context, acp.CreateElicitationRequest) (acp.CreateElicitationResponse, error) {
	return acp.CreateElicitationResponse{}, acp.NewMethodNotFound(acp.ClientMethodElicitationCreate)
}

func (agentRequests) ReadTextFile(context.Context, acp.ReadTextFileRequest) (acp.ReadTextFileResponse, error) {
	return acp.ReadTextFileResponse{}, acp.NewMethodNotFound(acp.ClientMethodFsReadTextFile)
}

func (agentRequests) WriteTextFile(context.Context, acp.WriteTextFileRequest) (acp.WriteTextFileResponse, error) {
	return acp.WriteTextFileResponse{}, acp.NewMethodNotFound(acp.ClientMethodFsWriteTextFile)
}

func (agentRequests) CreateTerminal(context.Context, acp.CreateTerminalRequest) (acp.CreateTerminalResponse, error) {
	return acp.CreateTerminalResponse{}, acp.NewMethodNotFound(acp.ClientMethodTerminalCreate)
}

func (agentRequests) KillTerminal(context.Context, acp.KillTerminalRequest) (acp.KillTerminalResponse, error) {
	return acp.KillTerminalResponse{}, acp.NewMethodNotFound(acp.ClientMethodTerminalKill)
}

func (agentRequests) TerminalOutput(context.Context, acp.TerminalOutputRequest) (acp.TerminalOutputResponse, error) {
	return acp.TerminalOutputResponse{}, acp.NewMethodNotFound(acp.ClientMethodTerminalOutput)
}

func (agentRequests) ReleaseTerminal(context.Context, acp.ReleaseTerminalRequest) (acp.ReleaseTerminalResponse, error) {
	return acp.ReleaseTerminalResponse{}, acp.NewMethodNotFound(acp.ClientMethodTerminalRelease)
}

func (agentRequests) WaitForTerminalExit(context.Context, acp.WaitForTerminalExitRequest) (acp.WaitForTerminalExitResponse, error) {
	return acp.WaitForTerminalExitResponse{}, acp.NewMethodNotFound(acp.ClientMethodTerminalWaitForExit)
}
