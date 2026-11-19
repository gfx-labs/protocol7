package navichat

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/modelcontextprotocol/go-sdk/mcp"
)

// DefaultWait and MaxWait bound how long chat_wait blocks. An MCP client
// often times out a tool call after a few minutes, so a long wait is several
// calls.
const (
	DefaultWait = 60 * time.Second
	MaxWait     = 10 * time.Minute
)

const chatModel = "A navi chat is a conversation with a selfdev agent that works on the project's repositories. Each message starts one turn, the agent's answer to it, and tools that send return at once with the chat ID and the turn ID."

// ProjectsInput is the input of chat_projects.
type ProjectsInput struct{}

// ProjectsOutput is the output of chat_projects.
type ProjectsOutput struct {
	Projects []Project `json:"projects"`
}

// StartInput is the input of chat_start.
type StartInput struct {
	Project string `json:"project,omitempty" jsonschema:"navi project to start the chat in. Empty is the default project."`
	Text    string `json:"text" jsonschema:"the first message: the task for the agent, with the context it needs"`
}

// SendInput is the input of chat_send.
type SendInput struct {
	Project string `json:"project,omitempty" jsonschema:"navi project of the chat. Empty is the default project."`
	Chat    string `json:"chat" jsonschema:"chat ID that chat_start returned"`
	Text    string `json:"text" jsonschema:"the message: a follow-up, or the answer to a turn that needs input"`
}

// WaitInput is the input of chat_wait.
type WaitInput struct {
	Project        string   `json:"project,omitempty" jsonschema:"navi project of the turns. Empty is the default project."`
	Turns          []string `json:"turns" jsonschema:"turn IDs to wait for, from chat_start or chat_send. The first to end or need input is returned."`
	TimeoutSeconds int      `json:"timeout_seconds,omitempty" jsonschema:"how long to wait, in seconds. Default 60, at most 600."`
}

// TurnInput is the input of chat_get and chat_cancel.
type TurnInput struct {
	Project string `json:"project,omitempty" jsonschema:"navi project of the turn. Empty is the default project."`
	Turn    string `json:"turn" jsonschema:"turn ID from chat_start, chat_send or chat_list"`
}

// ListInput is the input of chat_list.
type ListInput struct {
	Project   string `json:"project,omitempty" jsonschema:"navi project. Empty is the default project."`
	Chat      string `json:"chat,omitempty" jsonschema:"chat ID to list the turns of. Empty lists the latest turn of each chat in the project, newest first."`
	Limit     int    `json:"limit,omitempty" jsonschema:"most turns to return. Default 20, at most 100."`
	PageToken string `json:"page_token,omitempty" jsonschema:"next_page_token of an earlier chat_list, to continue it"`
}

func boolPointer(value bool) *bool { return &value }

// NewMCPServer returns an MCP server with the chat tools on client.
func NewMCPServer(client *Client, version string) *mcp.Server {
	server := mcp.NewServer(&mcp.Implementation{Name: "p7-chat", Title: "navi chats", Version: version}, &mcp.ServerOptions{
		Instructions: chatModel + " Start a chat per independent task, continue a chat with chat_send, and use chat_wait to block until any of several turns finishes or needs input.",
	})
	readOnly := func(title string) *mcp.ToolAnnotations {
		return &mcp.ToolAnnotations{Title: title, ReadOnlyHint: true, OpenWorldHint: boolPointer(true)}
	}

	mcp.AddTool(server, &mcp.Tool{
		Name:        "chat_projects",
		Description: "List the navi projects this client knows, the default first, and whether each one's agent answers. A chat belongs to one project, which decides its repositories.",
		Annotations: readOnly("List navi projects"),
	}, func(ctx context.Context, _ *mcp.CallToolRequest, _ ProjectsInput) (*mcp.CallToolResult, ProjectsOutput, error) {
		projects := client.Projects(ctx)
		var lines []string
		for _, project := range projects {
			line := project.Name
			if project.Default {
				line += " (default)"
			}
			if project.Reachable {
				line += ": reachable"
			} else {
				line += ": " + project.Error
			}
			lines = append(lines, line)
		}
		if len(lines) == 0 {
			lines = append(lines, "No projects are configured: set project in the p7 config file.")
		}
		return textResult(strings.Join(lines, "\n")), ProjectsOutput{Projects: projects}, nil
	})

	mcp.AddTool(server, &mcp.Tool{
		Name:        "chat_start",
		Description: chatModel + " chat_start begins a new chat with its first message and does not wait: pass the returned turn to chat_wait. Start a new chat for each independent task, and continue an existing one with chat_send.",
		Annotations: &mcp.ToolAnnotations{Title: "Start a navi chat", DestructiveHint: boolPointer(false), OpenWorldHint: boolPointer(true)},
	}, func(ctx context.Context, _ *mcp.CallToolRequest, input StartInput) (*mcp.CallToolResult, Turn, error) {
		turn, err := client.Start(ctx, input.Project, input.Text)
		if err != nil {
			return nil, Turn{}, err
		}
		return textResult(fmt.Sprintf("Started chat %s in project %s. Turn %s is %s. Call chat_wait with this turn for the answer.", turn.Chat, turn.Project, turn.ID, turn.State)), turn, nil
	})

	mcp.AddTool(server, &mcp.Tool{
		Name:        "chat_send",
		Description: "Send a message to an existing chat, to follow up or to answer a turn that needs input. It returns the new turn without waiting. A chat runs one turn at a time, so a message sent while a turn runs waits in its queue.",
		Annotations: &mcp.ToolAnnotations{Title: "Send to a navi chat", DestructiveHint: boolPointer(false), OpenWorldHint: boolPointer(true)},
	}, func(ctx context.Context, _ *mcp.CallToolRequest, input SendInput) (*mcp.CallToolResult, Turn, error) {
		turn, err := client.Send(ctx, input.Project, input.Chat, input.Text)
		if err != nil {
			return nil, Turn{}, err
		}
		return textResult(fmt.Sprintf("Sent to chat %s. Turn %s is %s. Call chat_wait with this turn for the answer.", turn.Chat, turn.ID, turn.State)), turn, nil
	})

	mcp.AddTool(server, &mcp.Tool{
		Name:        "chat_wait",
		Description: "Wait until the first of several turns ends or needs input, and return it with the others' current state. Pass every running turn to drive several chats at once. On timed_out, call it again.",
		Annotations: readOnly("Wait for navi turns"),
	}, func(ctx context.Context, _ *mcp.CallToolRequest, input WaitInput) (*mcp.CallToolResult, Waited, error) {
		timeout := DefaultWait
		if input.TimeoutSeconds > 0 {
			timeout = min(time.Duration(input.TimeoutSeconds)*time.Second, MaxWait)
		}
		waited, err := client.Wait(ctx, input.Project, input.Turns, timeout)
		if err != nil {
			return nil, Waited{}, err
		}
		if waited.TimedOut {
			return textResult(fmt.Sprintf("No turn ended or needed input within %s. Call chat_wait again to keep waiting.", timeout)), waited, nil
		}
		return textResult(turnSummary(*waited.Turn)), waited, nil
	})

	mcp.AddTool(server, &mcp.Tool{
		Name:        "chat_get",
		Description: "Read one turn: its state and the agent's result or question. navi keeps the text of a chat's last turn only, so earlier turns have a state and no text.",
		Annotations: readOnly("Read a navi turn"),
	}, func(ctx context.Context, _ *mcp.CallToolRequest, input TurnInput) (*mcp.CallToolResult, Turn, error) {
		turn, err := client.Get(ctx, input.Project, input.Turn)
		if err != nil {
			return nil, Turn{}, err
		}
		return textResult(turnSummary(turn)), turn, nil
	})

	mcp.AddTool(server, &mcp.Tool{
		Name:        "chat_list",
		Description: fmt.Sprintf("List the turns of one chat, or with no chat the latest turn of each chat in the project, newest first. At most %d turns per call.", MaxList),
		Annotations: readOnly("List navi turns"),
	}, func(ctx context.Context, _ *mcp.CallToolRequest, input ListInput) (*mcp.CallToolResult, TurnList, error) {
		list, err := client.List(ctx, ListRequest{Project: input.Project, Chat: input.Chat, Limit: input.Limit, PageToken: input.PageToken})
		if err != nil {
			return nil, TurnList{}, err
		}
		summary := fmt.Sprintf("%d of %d turns.", len(list.Turns), list.Total)
		if list.NextPageToken != "" {
			summary += " Pass next_page_token as page_token for more."
		} else if len(list.Turns) < list.Total {
			summary += " Raise limit to see more."
		}
		for _, turn := range list.Turns {
			summary += fmt.Sprintf("\n%s chat %s turn %s", turn.State, turn.Chat, turn.ID)
		}
		return textResult(summary), list, nil
	})

	mcp.AddTool(server, &mcp.Tool{
		Name:        "chat_cancel",
		Description: "Stop a running turn. This is the chat's stop button: the agent stops working on it, and the chat stays open for further messages.",
		Annotations: &mcp.ToolAnnotations{Title: "Stop a navi turn", DestructiveHint: boolPointer(true), IdempotentHint: true, OpenWorldHint: boolPointer(true)},
	}, func(ctx context.Context, _ *mcp.CallToolRequest, input TurnInput) (*mcp.CallToolResult, Turn, error) {
		turn, err := client.Cancel(ctx, input.Project, input.Turn)
		if err != nil {
			return nil, Turn{}, err
		}
		return textResult(fmt.Sprintf("Turn %s of chat %s is %s.", turn.ID, turn.Chat, turn.State)), turn, nil
	})
	return server
}

func textResult(text string) *mcp.CallToolResult {
	return &mcp.CallToolResult{Content: []mcp.Content{&mcp.TextContent{Text: text}}}
}

// turnSummary says what a turn's state means for the caller.
func turnSummary(turn Turn) string {
	var summary string
	switch {
	case turn.NeedsInput:
		summary = fmt.Sprintf("Turn %s of chat %s needs input. Answer with chat_send to chat %s, or ask the user.", turn.ID, turn.Chat, turn.Chat)
	case turn.Done:
		summary = fmt.Sprintf("Turn %s of chat %s is %s.", turn.ID, turn.Chat, turn.State)
	default:
		summary = fmt.Sprintf("Turn %s of chat %s is %s. Call chat_wait to wait for it.", turn.ID, turn.Chat, turn.State)
	}
	if turn.Text != "" {
		summary += "\n\n" + turn.Text
	}
	return summary
}
