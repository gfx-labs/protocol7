---
name: navi-chats
description: Drive navi chats (protocol7 selfdev agents) from a local agent with the p7 chat MCP tools or the p7 chat shell commands. Use when a task should be handed to a navi selfdev agent, when several navi chats run at once, or when a navi turn needs input.
---

# Driving navi chats

navi serves each project's chats over A2A. `p7 chat mcp` exposes them as MCP tools, and `p7 chat` has the same operations as shell commands that print JSON. Both read their settings from the file given with `--config` (see `docs/chat-mcp.md` in protocol7).

## The model

- A **chat** is one conversation with a selfdev agent in a navi project. The agent works on the project's repositories and keeps its context across messages. Its ID is the A2A contextId.
- A **turn** is the agent's answer to one message. Its ID is `<chat>-<message>`. Sending returns the turn at once, without waiting.
- A turn ends as `completed`, `failed` or `canceled`. It is `input-required` when the agent waits on a person (an escalation, or it asks something).
- A chat runs one turn at a time. A message sent during a turn waits in the chat's queue.
- navi keeps the result text of a chat's last turn only.

## New chat or the same chat

- Start a new chat (`chat_start`) for each independent task. Separate chats run in parallel and do not share context.
- Continue a chat (`chat_send`) for follow-ups on the same work: corrections, review comments, "now open the PR", or the answer to its question. The agent keeps its workspace and its context.
- Do not start a second chat to fix the first chat's work. Send to the first one.

## Running several chats

1. `chat_start` each task and keep every returned `turn`.
2. `chat_wait` with all running turns. It returns the first that ended or needs input, and the others' current state.
3. Handle that turn, drop it from the list (or replace it with the new turn after a `chat_send`), and call `chat_wait` again.
4. On `timed_out: true`, call `chat_wait` again. Each call waits at most 10 minutes (default 60 seconds).

## Input required

When a turn has `needs_input: true`, its text is the agent's question.

- If the answer is in the task you were given or in the repository, answer with `chat_send` to the same chat, then wait for the new turn.
- If it needs a decision, a credential or an approval from the user, ask the user. Do not guess at approvals.

## Cancel

`chat_cancel` stops a running turn, like the stop button in navi. The chat stays open and takes more messages. Cancel a turn that is clearly going the wrong way, then send a correction. Cancelling an ended turn is an error that says so.

## Reading

- `chat_get` reads one turn.
- `chat_list` with a chat lists its turns. Without a chat it lists the latest turn of each chat in the project, newest first. It returns at most 100 per call (default 20) and says when more remain.
- `chat_projects` lists the configured projects and whether each answers.

## Shell commands

The same operations, for scripts or an agent without MCP:

```sh
p7 chat --config ~/.config/p7/chat.yaml projects
p7 chat --config ~/.config/p7/chat.yaml start "Fix the flaky test in pkg/web"
p7 chat --config ~/.config/p7/chat.yaml send <chat> "Also update the docs"
p7 chat --config ~/.config/p7/chat.yaml wait --timeout 5m <turn> <turn>
p7 chat --config ~/.config/p7/chat.yaml get <turn>
p7 chat --config ~/.config/p7/chat.yaml list [<chat>] --limit 20
p7 chat --config ~/.config/p7/chat.yaml cancel <turn>
```

Each prints JSON with `project`, `chat`, `turn`, `state`, `done`, `needs_input` and `text`. `--project` picks another project for one command.

## Errors

Error messages name the next step. The common ones:

- "no project": set `project` in the config file or pass one.
- "navi denied access": reach navi through the tailnet, or set `login-header` for a port forward. Starting and sending need modify access to the project.
- "no such turn": use a turn ID from `chat_start`, `chat_send` or `chat_list`.
