# Driving navi chats from a local agent

`p7 chat` drives navi chats over A2A from a person's own machine. `p7 chat mcp` serves the same operations as MCP tools over standard input and output, so a local agent such as Claude Code, Codex or jcode can start several navi chats, wait on them and answer their questions. The skill in `skills/navi-chats/SKILL.md` tells an agent how to use them.

navi's side is described in `gfx-labs/protocol7-selfdev` `docs/a2a.md`. In short, each navi project is an A2A agent at `<navi>/a2a/<project>/`. A context is a chat and a task is one turn of it.

The client is `pkg/transport/navichat`, over a2a-go v2.9.0 and the official Go MCP SDK.

## Install

```sh
go install github.com/gfx-labs/protocol7/cmd/p7@latest
```

## Settings

The settings are flags of `p7 chat`, and they are usually kept in a YAML file passed with `--config`. Each key is a flag's long name.

| Setting | Meaning |
|---|---|
| `navi-url` | navi's base URL. Required. On the tailnet it is `https://navi.<tailnet>.ts.net` |
| `project` | The project used when a command or tool names none |
| `projects` | Further projects that `chat projects` and `chat_projects` report (a list) |
| `login-header` | A login to send in the `Tailscale-User-Login` header. Only for a port forward. Off by default |

On the tailnet the Tailscale ingress in front of navi sets `Tailscale-User-Login` to the caller, so the client sends no credentials:

```yaml
# ~/.config/p7/chat.yaml
navi-url: https://navi.example.ts.net
project: selfdev
projects:
  - oku
```

Through a port forward nothing sets the header, so the client sets it:

```sh
kubie exec -e talos-metal navi --context-headers=never kubectl port-forward deploy/navi 18788:8787
```

```yaml
navi-url: http://127.0.0.1:18788
project: selfdev
login-header: you@example.com
```

## Shell commands

Each prints JSON. `start` and `send` return the turn at once, and `wait` blocks until the first of its turns ends or needs input.

```sh
p7 chat --config ~/.config/p7/chat.yaml projects
p7 chat --config ~/.config/p7/chat.yaml start "Say hello"
p7 chat --config ~/.config/p7/chat.yaml send <chat> "Now say goodbye"
p7 chat --config ~/.config/p7/chat.yaml wait --timeout 5m <turn> [<turn>...]
p7 chat --config ~/.config/p7/chat.yaml get <turn>
p7 chat --config ~/.config/p7/chat.yaml list [<chat>] --limit 20
p7 chat --config ~/.config/p7/chat.yaml cancel <turn>
```

## MCP tools

| Tool | What it does | Annotations |
|---|---|---|
| `chat_projects` | Lists the configured projects and whether each one's agent card can be read | read-only |
| `chat_start` | Starts a chat with its first message and returns the turn | not idempotent |
| `chat_send` | Sends a message to a chat and returns the new turn | not idempotent |
| `chat_wait` | Waits up to `timeout_seconds` (default 60, at most 600) for the first of several turns to end or need input | read-only |
| `chat_get` | Reads one turn | read-only |
| `chat_list` | Lists a chat's turns, or the latest turn of each chat in the project. At most 100 per call, with `next_page_token` when the server pages | read-only |
| `chat_cancel` | Stops a running turn, which is the chat's stop. The chat stays open | destructive |

Every tool talks to navi, so each is marked open-world. Each returns structured content that matches its output schema, and a short text summary. Errors say what to do next, such as which tool finds a missing turn ID.

`chat_wait` follows each turn's A2A stream and subscribes again if the stream breaks, so it does not poll navi while a turn runs.

## Adding the server to an agent

Claude Code:

```sh
claude mcp add navi -- p7 chat --config ~/.config/p7/chat.yaml mcp
```

Codex, in `~/.codex/config.toml`:

```toml
[mcp_servers.navi]
command = "p7"
args = ["chat", "--config", "/home/you/.config/p7/chat.yaml", "mcp"]
```

jcode, in `~/.jcode/mcp.json`:

```json
{
  "mcpServers": {
    "navi": {
      "command": "p7",
      "args": ["chat", "--config", "/home/you/.config/p7/chat.yaml", "mcp"]
    }
  }
}
```

Copy `skills/navi-chats` into the agent's skills directory, such as `~/.claude/skills/navi-chats` or `~/.agents/skills/navi-chats`, so that it knows when to start a chat and when to continue one.

## What was run and what was not

Run:

- `TestChatTools` in `pkg/transport/navichat`. It serves an a2asrv agent in the test laid out as navi lays out a project (card under `/a2a/<project>/`, JSON-RPC at the prefix, the caller named by `Tailscale-User-Login`), with an executor that maps chats and turns as navi does. Through the MCP server over the SDK's in-memory transport it lists projects (one reachable, one missing), starts two chats, waits on both and gets the finished one, continues that chat with a turn that becomes input-required, lists turns of a chat and one page of all turns, cancels the running turn, and checks that an unknown turn and the cancel of an ended turn return errors that name the next step.

- The live check against navi in project `selfdev`, through a port forward with `login-header` set, using `p7 chat mcp` over standard input and output driven by a JSON-RPC client built on the Go MCP SDK. `tools/list` showed the seven tools with their annotations and output schemas. `chat_projects` read the card of `selfdev` and reported `core` as missing. `chat_start` returned the chat and a submitted turn in 0.1 seconds. `chat_wait` returned that turn completed with the agent's answer after 26 seconds. `chat_send` to the same chat and `chat_wait` returned the second turn completed after 3 seconds. `chat_get` read it, `chat_list` listed the chat's two turns, and `chat_list` with limit 3 returned three of the project's 28 chats. The test chat was then archived.

Not run: the configuration snippets for Claude Code, Codex and jcode were not loaded into those agents. `chat_cancel` and an input-required turn were run only in the test, not against navi.
