package cli

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"runtime/debug"
	"strings"
	"time"

	"github.com/modelcontextprotocol/go-sdk/mcp"

	"github.com/gfx-labs/protocol7/pkg/transport/navichat"
)

// chatCommand drives navi chats over A2A from this machine. Its settings
// usually come from a --config file.
type chatCommand struct {
	NaviURL     string   `name:"navi-url" help:"navi's base URL, such as https://navi.<tailnet>.ts.net, or http://127.0.0.1:18788 through a port forward."`
	Project     string   `name:"project" help:"navi project used when a command names none."`
	Projects    []string `name:"projects" help:"Further navi projects that chat projects and chat_projects report (repeatable)."`
	LoginHeader string   `name:"login-header" help:"Login to send in the Tailscale-User-Login header, such as you@example.com. Only for a port forward: on the tailnet the proxy sets it. Empty sends none."`

	ProjectsList chatProjectsCommand `cmd:"" name:"projects" help:"List the known projects and whether each one's agent answers."`
	Start        chatStartCommand    `cmd:"" help:"Start a chat with its first message. Prints the turn without waiting for it."`
	Send         chatSendCommand     `cmd:"" help:"Send a message to a chat. Prints the turn without waiting for it."`
	Wait         chatWaitCommand     `cmd:"" help:"Wait for the first of several turns to end or need input."`
	Get          chatGetCommand      `cmd:"" help:"Read a turn."`
	List         chatListCommand     `cmd:"" help:"List the turns of a chat, or the latest turn of each chat in the project."`
	Cancel       chatCancelCommand   `cmd:"" help:"Stop a running turn. The chat stays open."`
	MCP          chatMCPCommand      `cmd:"" name:"mcp" help:"Serve the chat tools to an MCP client over standard input and output."`
}

func (c *chatCommand) client() (*navichat.Client, error) {
	return navichat.New(navichat.Config{NaviURL: c.NaviURL, Project: c.Project, Projects: c.Projects, Login: c.LoginHeader})
}

// use runs operation with a client and prints its result as JSON.
func (c *chatCommand) use(operation func(*navichat.Client) (any, error)) (err error) {
	client, err := c.client()
	if err != nil {
		return err
	}
	defer func() {
		if closeErr := client.Close(); err == nil {
			err = closeErr
		}
	}()
	result, err := operation(client)
	if err != nil {
		return err
	}
	encoder := json.NewEncoder(os.Stdout)
	encoder.SetIndent("", "  ")
	return encoder.Encode(result)
}

type chatProjectsCommand struct{}

func (*chatProjectsCommand) Run(ctx context.Context, chat *chatCommand) error {
	return chat.use(func(client *navichat.Client) (any, error) {
		return client.Projects(ctx), nil
	})
}

type chatStartCommand struct {
	Text []string `arg:"" help:"The first message. Several arguments are joined with spaces."`
}

func (s *chatStartCommand) Run(ctx context.Context, chat *chatCommand) error {
	return chat.use(func(client *navichat.Client) (any, error) {
		return client.Start(ctx, "", strings.Join(s.Text, " "))
	})
}

type chatSendCommand struct {
	Chat string   `arg:"" help:"The chat's ID."`
	Text []string `arg:"" help:"The message. Several arguments are joined with spaces."`
}

func (s *chatSendCommand) Run(ctx context.Context, chat *chatCommand) error {
	return chat.use(func(client *navichat.Client) (any, error) {
		return client.Send(ctx, "", s.Chat, strings.Join(s.Text, " "))
	})
}

type chatWaitCommand struct {
	Turns   []string      `arg:"" help:"Turn IDs to wait for."`
	Timeout time.Duration `name:"timeout" default:"10m" help:"How long to wait."`
}

func (w *chatWaitCommand) Run(ctx context.Context, chat *chatCommand) error {
	return chat.use(func(client *navichat.Client) (any, error) {
		return client.Wait(ctx, "", w.Turns, w.Timeout)
	})
}

type chatGetCommand struct {
	Turn string `arg:"" help:"The turn's ID."`
}

func (g *chatGetCommand) Run(ctx context.Context, chat *chatCommand) error {
	return chat.use(func(client *navichat.Client) (any, error) {
		return client.Get(ctx, "", g.Turn)
	})
}

type chatListCommand struct {
	Chat      string `arg:"" optional:"" help:"The chat to list the turns of. Without it, the latest turn of each chat in the project."`
	Limit     int    `name:"limit" default:"20" help:"Most turns to print, at most 100."`
	PageToken string `name:"page-token" help:"next_page_token of an earlier list, to continue it."`
}

func (l *chatListCommand) Run(ctx context.Context, chat *chatCommand) error {
	return chat.use(func(client *navichat.Client) (any, error) {
		return client.List(ctx, navichat.ListRequest{Chat: l.Chat, Limit: l.Limit, PageToken: l.PageToken})
	})
}

type chatCancelCommand struct {
	Turn string `arg:"" help:"The turn's ID."`
}

func (c *chatCancelCommand) Run(ctx context.Context, chat *chatCommand) error {
	return chat.use(func(client *navichat.Client) (any, error) {
		return client.Cancel(ctx, "", c.Turn)
	})
}

type chatMCPCommand struct{}

func (*chatMCPCommand) Run(ctx context.Context, chat *chatCommand) (err error) {
	client, err := chat.client()
	if err != nil {
		return err
	}
	defer func() {
		if closeErr := client.Close(); err == nil {
			err = closeErr
		}
	}()
	server := navichat.NewMCPServer(client, buildVersion())
	if err := server.Run(ctx, &mcp.StdioTransport{}); err != nil && ctx.Err() == nil {
		return fmt.Errorf("serve MCP: %w", err)
	}
	return nil
}

// buildVersion is the module version p7 was built from, such as the one
// go install records.
func buildVersion() string {
	if info, ok := debug.ReadBuildInfo(); ok && info.Main.Version != "" {
		return info.Main.Version
	}
	return "devel"
}
