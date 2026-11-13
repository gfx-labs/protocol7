# Status

What is built, what was run, and where the code departs from `plan.md`. Last updated 2026-11-13.

`plan.md` describes the design. This file describes the code. Where they disagree, the code is what exists and the plan is what is intended.

## What is built

The trusted-host path that the selfdev chat needs (plan section 12, "the chat comes first"), and nothing else.

| Path | Content |
|---|---|
| `cmd/p7acpshim`, `pkg/execution/acpshim` | One detached process per session, on the acphost library. It starts the harness, serves ACP on a unix socket, journals every frame and outlives the worker. The client is built on go-acp |
| `pkg/execution/journal` | Decodes journal frames into session events, and folds the events of one turn into its result |
| `pkg/execution/session` | Sessions on this machine: a directory per session with its workspace, shim directory and record. Places git clone inputs. Runs a turn that survives its caller |
| `pkg/transport/stream` | Session frames on NATS JetStream, one subject per session, and an embedded NATS server for local use |
| `pkg/orchestration/activity`, `pkg/orchestration/coreworker` | The activities `p7.OpenSession`, `p7.RunTurn` and `p7.CloseSession`, and the worker that serves them on `local-<host>` |
| `pkg/p7` | The library: `NewWorker`, `OpenSession`, `PromptText`, `PromptTextFuture`, `Session.Close`, `GitClone` inputs, and `NewClient` with `WatchSession` |
| `pkg/transport/cli`, `cmd/p7`, `cmd/p7-worker` | `p7 worker`, and `p7 dev`, which runs a Temporal development server, an embedded NATS server and the core worker in one process |

The chat workflow and the web UI are in `gfx-labs/protocol7-selfdev`.

## How a turn survives

- The turn ID travels in the `_meta` of `session/prompt`, which the journal keeps. A repeated `p7.RunTurn` with the same ID reads the journal first. It returns the recorded result, or attaches to the running turn, and never prompts twice.
- A worker that shuts down leaves the turn running in the shim. A cancel request or the activity timeout sends `session/cancel` and still returns the turn's result.
- A shim that stopped during a turn makes that turn end as `interrupted`. The next turn starts a new shim, which loads the conversation.

## Run it

```sh
# in protocol7
go build -o bin/ ./cmd/...
bin/p7 dev          # the harness's credentials must be in this environment

# in protocol7-selfdev, with protocol7 checked out beside it
go run ./cmd/selfdev run
# then open http://127.0.0.1:8787
```

`p7 dev` needs the `temporal` CLI and the harness (`towa` by default) on `PATH`. It finds `p7acpshim` beside its own executable.

## What was run

All on one machine on 2026-11-13, with Temporal CLI 1.9.1 (server 1.32.0) and the real towa harness on one API-key account. No Claude or Codex subscription login was used.

| Case | Result |
|---|---|
| A chat started over HTTP, with clones of both repositories from local bare remotes | The agent committed on `chat/<id>` in its clone. The remotes were unchanged |
| The same from a browser: start, tool calls, a message queued behind a turn, Stop, a further turn, reload, close | Passed. Stop ended a `sleep 120` turn in 1.3 seconds and dropped the queued message |
| A chat with the default GitHub remotes, read only | Both repositories cloned over ssh |
| `p7 dev` stopped with SIGTERM during a turn, then started again | The turn ended while Temporal and the worker were down. After the restart the chat read its result from the journal. Two prompts for two turns |
| `p7-worker` killed with SIGKILL during a turn, against a separate NATS server (2.10.20) and Temporal | The activity timed out on its heartbeat, the retry on a new worker attached to the running turn, and it ended normally. One prompt |
| Continue-As-New, with the server's suggestion lowered to 60 events | The chat continued as new after its fifth turn. Turns six to eight ran, the agent still knew a word from turn one, and the chat list showed one entry |
| Session manager against towa, without Temporal: caller killed mid-turn, cancel, shim stopped between turns, shim stopped mid-turn, close then a turn | Each behaved as "How a turn survives" says. A turn on a closed session is refused |

## Tests

`go test ./...` in both repositories. They need no network and no harness.

| Package | What it runs |
|---|---|
| `pkg/execution/journal` | The decoder on the journal of a real towa session that was cancelled, loaded again and interrupted (`testdata/towa-recovery.ndjson`) |
| `pkg/execution/session` | The manager with the real shim and a scripted ACP agent, both run from the test binary: a repeated turn, a turn picked up and cancelled, a shim stopped mid-turn, and context hidden from viewers |
| `pkg/transport/stream` | Publish, deduplication and an ordered read against the embedded NATS server |
| `protocol7-selfdev/pkg/chat` | The chat workflow in Temporal's test environment: message order, stop, close and cancel |

The recorded journal holds the scratch paths of the machine it was recorded on. It was checked for keys and tokens before it was committed, and holds none.

## Where the code departs from the plan

| Plan | Code |
|---|---|
| Domain types are protobuf | Go structs with JSON tags, in `pkg/domain` |
| Clients read live output through ticketd | `p7.Client` reads NATS itself. ticketd does not exist |
| A new chat is a ticket of a chat kind | A chat is a Temporal workflow that the selfdev web server starts. There are no tickets |
| Host, container and Kubernetes backends | The host backend only |
| Model and git credentials come through PDP | Sessions use the machine's own logins, which section 5 allows for trusted host execution |
| Permission requests are decided by policy | Every permission request is allowed |
| Limits, typed prompts, approvals, notices, steering | None of them |

## Known limits

- A chat's agent runs unsandboxed as the user who runs the worker, with that user's git and ssh credentials. The chat's instructions tell it to push only when asked. Nothing enforces that.
- No chat has pushed a branch or opened a pull request yet. That path is not run.
- The web UI has no authentication. It listens on the loopback address by default.
- The chat list reads at most 200 workflow executions and does not page.
- A closed chat answers its state query only while a selfdev worker runs and Temporal still keeps its history.
- Frames stay in the stream for 30 days and nothing archives them. The journal on the session's machine is the full record.
- A Stop that arrives before the turn's activity started drops that message without sending it.

## Not run

- Worker Versioning. `p7.WorkerConfig` takes a deployment and build ID, and that code was only compiled.
- A session whose shim exits on its idle limit. Stopping the shim by hand between turns was run, and the next turn loaded the conversation.
- A harness that refuses to load a conversation, which starts a new one seeded with a summary of the journal.
- A turn that reaches its activity timeout.
- More than one machine, and any harness other than towa.
