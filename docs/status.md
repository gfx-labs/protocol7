# Status

What is built, what was run, and where the code departs from `plan.md`. Last updated 2026-11-13.

`plan.md` describes the design. This file describes the code. Where they disagree, the code is what exists and the plan is what is intended.

## What is built

The trusted-host path that the selfdev chat needs (plan section 12, "the chat comes first"), and nothing else.

| Path | Content |
|---|---|
| `cmd/p7acpshim`, `pkg/execution/acpshim` | One detached process per session, on the acphost library. It starts the harness, serves ACP on a unix socket, journals every frame and outlives the worker. The client is built on go-acp |
| `pkg/execution/journal` | Decodes journal frames into session events, and folds the events of one turn into its result |
| `pkg/execution/session` | Sessions on this machine: a directory per session with its workspace, shim directory and record. Places git clone inputs. Runs a turn that survives its caller. A harness that exits while its session starts is reported with its exit status and its last five lines of output |
| `pkg/transport/stream` | Session frames on NATS JetStream, one subject per session, and an embedded NATS server for local use |
| `pkg/orchestration/activity`, `pkg/orchestration/coreworker` | The activities `p7.OpenSession`, `p7.RunTurn` and `p7.CloseSession`, and the worker that serves them on `local-<host>` |
| `pkg/p7` | The library: `NewWorker`, `OpenSession`, `PromptText`, `PromptTextFuture`, `Session.Close`, `GitClone` inputs, `SessionSpec.Tools` with `HandleTools`, `SessionSpec.Permissions` with `HandlePermissions`, and `NewClient` with `WatchSession`. Plain constructors, with no dependency on fx. `SessionSpec.OpenRetry` and `TextPrompt.Retry` let the workflow set how often opening and a turn are retried, and a harness that stopped fails with the application error type `HarnessStopped` |
| `pkg/p7/p7fx` | Optional fx options for a consumer's process: `TemporalClient`, `Worker`, `Client`, and `Run`, which runs an application until its context ends |
| `pkg/transport/cli`, `cmd/p7`, `cmd/p7-worker` | `p7 worker`, and `p7 dev`, which runs a Temporal development server, an embedded NATS server and the core worker in one process. Each command is an fx application, and `wire.go` holds its modules |
| `containers/p7`, `scripts/build-image`, `.github/workflows/image.yml` | The image `ghcr.io/gfx-labs/protocol7:<commit>`: the three commands and git. It holds no harness and no toolchain for an agent. The workflow builds it on the self-hosted runners and pushes it with its own token. No pod runs this image: protocol7-selfdev builds the commands from this source at a pinned commit, in its own image |
| `.github/pending/notify-selfdev.yml`, to be moved to `.github/workflows/` by a person | On each commit to master that changes the Go source, tells protocol7-selfdev the commit, so that it builds, pins and deploys without a person in between. It needs the Actions secret `ELEE_AGENT_PRIVATE_KEY`, and the `elee-agent` app installed on protocol7-selfdev with write access to its contents. Neither was set up or run |

The chat workflow, the web UI and the deployment on talos-metal are in `gfx-labs/protocol7-selfdev`. That deployment is written and not applied. In it the core worker holds no model credential and has no route to the internet: PDP holds a Claude subscription login and an OpenRouter key, and a forwarder in the worker's pod presents the pod's SPIFFE identity to PDP. `protocol7-selfdev/kubernetes/README.md` describes it and lists what was run.

### How a process is put together

The commands of both repositories are composed with `go.uber.org/fx` (decided 2026-11-13, user). A long-lived component has a constructor that connects to nothing, and separate start and stop methods: `stream.New` then `Connect`, `stream.NewEmbedded` then `Start`, `coreworker.New` then `Start` and `Stop`, `p7.NewClient` then `Connect`, `p7.NewWorker` then `Start` and `Stop`. A command supplies its flags as configuration values and names its modules. fx starts the components in dependency order and stops them in reverse.

fx resolves the components when the process starts, so a missing one is not a compile error. `wire_test.go` in each repository's `pkg/cli` checks every command's graph with `fx.ValidateApp`.

## How a turn survives

- The turn ID travels in the `_meta` of `session/prompt`, which the journal keeps. A repeated `p7.RunTurn` with the same ID reads the journal first. It returns the recorded result, or attaches to the running turn, and never prompts twice.
- A worker that shuts down leaves the turn running in the shim. A cancel request or the activity timeout sends `session/cancel` and still returns the turn's result.
- A shim that stopped during a turn makes that turn end as `interrupted`. The next turn starts a new shim, which loads the conversation.

## Tools

A workflow offers tools to a session's agent with `SessionSpec.Tools` and answers their calls with `p7.HandleTools`. Each `p7.Tool` has a name, a description, a JSON Schema for its arguments and a `Run` function that runs in the workflow.

- The core worker offers the tools to the harness as one MCP server of type `acp` in `session/new` and `session/load`. The agent sends MCP requests as `mcp/message` on the same ACP connection, so there is no second process or endpoint. towa supports this from 0.4.0.
- `tools/list` is answered from the session's request. `tools/call` becomes the Temporal update `p7.ToolCall` to the workflow that opened the session, sent with no run ID so that it reaches the current run after Continue-As-New.
- The update ID is the session, the shim's process ID and the MCP request ID. A call that the shim sends again to a later connection gets the first call's result and does not run twice.
- The update's validator refuses a session the workflow did not open and a tool it did not offer, before the call enters history. A `Run` error is returned to the agent as a result with `isError`, which is how MCP reports a tool that failed.
- What a call may do is decided in the consumer's workflow. It runs activities for any work outside the workflow.

## Permission requests

A workflow decides the permission requests of its session's agent with `SessionSpec.Permissions`, a function that gets a `p7.PermissionRequest` (session, tool call ID, title, kind, raw input, the agent's options) and returns `p7.Allow()` or `p7.Deny(reason)`. With no function, every request is allowed, as before.

- The workflow calls `p7.HandlePermissions` with the same function at the start of every run, like `HandleTools`. A request becomes the Temporal update `p7.Permission` to the workflow that opened the session, sent with no run ID. The validator refuses a session the workflow did not open.
- The function runs in the workflow, so it may run activities or wait for a signal to ask a person. The agent waits for the answer.
- The shim answers the agent with its own option of the wanted kind: `allow_once` then `allow_always`, or `reject_once` then `reject_always`. When the agent offers none of that kind, the answer is `cancelled`. ACP carries no reason, so the agent is not told one.
- Fail closed: an error from the function, a refused update, and a core worker that cannot reach the owner all deny the request.
- ACP's request ID is not visible to the shim's handler, so the update ID is the session, the shim's process ID and a digest of the request. The same request sent again to a later connection gets the first decision. An agent that repeats an identical request in the same shim run gets the same decision.
- A session with `Permissions` and no tools offers no MCP server, and runs on a harness that does not take MCP over ACP.

## Run it

```sh
# in protocol7
go build -o bin/ ./cmd/...
bin/p7 dev          # the harness's credentials must be in this environment

# in protocol7-selfdev, with protocol7 checked out beside it
go run ./cmd/selfdev run
# then open http://127.0.0.1:8787
```

`p7 dev` needs the `temporal` CLI and the harness (`towa` by default) on `PATH`. Neither is in this repository's image. It finds `p7acpshim` beside its own executable.

## What was run

All on one machine on 2026-11-13, with Temporal CLI 1.9.1 (server 1.32.0) and the real towa harness on one API-key account. No Claude or Codex subscription login was used.

| Case | Result |
|---|---|
| A chat started over HTTP, with clones of both repositories from local bare remotes | The agent committed on `chat/<id>` in its clone. The remotes were unchanged |
| The same from a browser: start, tool calls, a message queued behind a turn, Stop, a further turn, reload, close | Passed. Stop ended a `sleep 120` turn in 1.3 seconds and dropped the queued message |
| A chat with the default GitHub remotes, read only | Both repositories cloned over ssh |
| `p7 dev` stopped with SIGTERM during a turn, then started again | The turn ended while Temporal and the worker were down. After the restart the chat read its result from the journal. Two prompts for two turns |
| `p7-worker` killed with SIGKILL during a turn, against a separate NATS server (2.10.20) and Temporal | The harness kept running and ended the turn 8 seconds later, with no worker. The activity timed out on its heartbeat. A new worker started 5 seconds after that, and the retry returned the result from the journal. One prompt |
| Continue-As-New, with the server's suggestion lowered to 60 events | The chat continued as new after its fifth turn. Turns six to eight ran, the agent still knew a word from turn one, and the chat list showed one entry |
| Session manager against towa, without Temporal: caller killed mid-turn, cancel, shim stopped between turns, shim stopped mid-turn, close then a turn | Each behaved as "How a turn survives" says. A turn on a closed session is refused |

Run again after the commands were moved to fx (protocol7 `e21e2d1`, selfdev `4a0b3ec`):

| Case | Result |
|---|---|
| `p7 dev` started, then stopped with SIGTERM | Exit status 0. No process and no listening port was left |
| The Temporal development server killed under a running `p7 dev` | `p7 dev` logged the exit, stopped its other components and exited with status 1 |
| `p7 dev` with a Temporal CLI path that does not exist, and `p7-worker` with no Temporal to reach | Each exited with status 1 and one line naming the cause |
| The browser chat: a commit, a message queued behind a turn, Stop, a further turn, reload | Passed. The commit is on `chat/<id>` in the clone and the remote was unchanged. Stop took 8.9 seconds in this run |
| `selfdev run` stopped with SIGTERM while a page held an event stream open, then started again | It exited in 0.3 seconds with status 0. The page that stayed open sent a message after the restart, showed the answer and closed the chat |

The two Stop times, 1.3 and 8.9 seconds, both fit the limit the code states: a cancel reaches the activity in the answer to a heartbeat, and the SDK sends one at most every 8 seconds. That cause was not isolated by a separate measurement.

Run with every outbound connection through PDP (protocol7 `2ffecb6`, selfdev `f1a9b41`, towa, PDP `f22f6d3`). `p7 dev` and `selfdev run` had the proxy variables and no model key. PDP ran beside a real SPIRE server and agent on the same machine, with the policy of the deployment:

| Case | Result |
|---|---|
| A chat on the DeepSeek model through OpenRouter, from a browser | The agent cloned, edited and committed in 14 seconds. PDP added the key. `strace` of a further chat showed connections to loopback only |
| A tool call that reaches the internet | `curl` to the Go module proxy answered 200 through PDP |
| A chat on a Claude model with no login in PDP | Failed after 304 seconds and ten attempts with "the shim closed the connection". Run again at protocol7 `7a6fe30` and selfdev `f0d0f65`: failed after 7 seconds and three attempts, and the page shows the harness's last lines, which name `credential_unavailable` |
| `p7-worker` with Temporal's address named in `NO_PROXY`, and not named | It connects directly. Without the name, the gRPC client sends the connection to the proxy and the worker exits |
| The model credential removed while a chat is open (protocol7 `8e18d5a`, selfdev `4c6dd98`, PDP `cfd86c5` on a Unix socket) | The turn fails in about two seconds and names `credential_unavailable`. The chat stays open and answers again once the credential is back |
| A turn that finds its harness stopped and cannot start it again, in the same setup | The turn fails in under a second with the harness's last lines, and the chat stays open. Before `96b257b` it was retried ten times over 304 seconds |
| A sonnet chat and an opus chat through PDP with a Claude subscription login (protocol7 `8e18d5a`, selfdev `ccac7aa`, PDP `3ba915a` under SPIRE) | PDP refreshed the login on the first request. Each chat cloned, edited and committed, and a tool call reached the Go module proxy. towa's log shows every response came from the chat's model on that login, and the worker held no credential |

## Tests

`go test ./...` in both repositories. They need no network and no harness.

| Package | What it runs |
|---|---|
| `pkg/execution/journal` | The decoder on the journal of a real towa session that was cancelled, loaded again and interrupted (`testdata/towa-recovery.ndjson`) |
| `pkg/execution/session` | The manager with the real shim and a scripted ACP agent, both run from the test binary: a repeated turn, a turn picked up and cancelled, a shim stopped mid-turn, a harness that exits while it starts, and context hidden from viewers |
| `pkg/transport/stream` | Publish, deduplication and an ordered read against the embedded NATS server |
| `pkg/p7` | A workflow's tool called by a scripted agent over `mcp/message`, through the real shim, the core worker and a Temporal development server, and a tool it did not offer refused. A workflow's permission function allowing, denying and failing on a scripted agent's request, and a session with no function allowed. Skipped when the `temporal` CLI is not on `PATH` |
| `pkg/transport/cli`, `protocol7-selfdev/pkg/cli` | Every command's fx graph has what its components ask for. Nothing is built or started |
| `protocol7-selfdev/pkg/chat` | The chat workflow in Temporal's test environment: message order, stop, close, cancel, and a chat that cannot open |

The recorded journal holds the scratch paths of the machine it was recorded on. It was checked for keys and tokens before it was committed, and holds none.

## Where the code departs from the plan

| Plan | Code |
|---|---|
| Domain types are protobuf | Go structs with JSON tags, in `pkg/domain` |
| Clients read live output through ticketd | `p7.Client` reads NATS itself. ticketd does not exist |
| A new chat is a ticket of a chat kind | A chat is a Temporal workflow that the selfdev web server starts. There are no tickets |
| Host, container and Kubernetes backends | The host backend only |
| Model and git credentials come through PDP | On a workstation, sessions use the machine's own logins, which section 5 allows for trusted host execution. The talos-metal deployment puts the model credentials in PDP. Its git credential is a GitHub App token that a second container writes to a file, not a PDP binding |
| A run pod's SPIFFE ID carries its profile, repository and run ID | The deployment has one worker pod with one identity, which every chat shares |
| Permission requests are decided by policy | The workflow that opened a session decides them, with `SessionSpec.Permissions`. A session with no function has every request allowed. There is no policy language |
| Limits, typed prompts, approvals, notices, steering | None of them |
| `p7.Tool` registers a user activity, called through ticketd with `p7 call` | A tool is a function in the workflow that opened the session, called over the ACP connection as a Temporal update. A tool registered on another worker is reached by the workflow running an activity on that worker's queue |

## Known limits

- A chat's agent runs unsandboxed as the user who runs the worker, with that user's git and ssh credentials. The chat's instructions tell it to push only when asked. Nothing enforces that.
- No chat has pushed a branch or opened a pull request yet. That path is not run.
- The web UI has no authentication. It listens on the loopback address by default.
- The chat list reads at most 200 workflow executions and does not page.
- A closed chat answers its state query only while a selfdev worker runs and Temporal still keeps its history.
- Frames stay in the stream for 30 days and nothing archives them. The journal on the session's machine is the full record.
- A Stop that arrives before the turn's activity started drops that message without sending it.
- A harness that prints a secret to standard error would show it on the chat's page when it stops while starting. Nothing redacts those lines. towa with proxy-held accounts holds only placeholders.
- A turn is retried ten times over about five minutes by default. The selfdev chat sets its own policy, which stops at once on a harness that cannot start. Another consumer has to do the same.
- Stop can take up to about nine seconds to end a turn, because the cancel waits for the activity's next heartbeat.

## Not run

- Worker Versioning. `p7.WorkerConfig` takes a deployment and build ID, and that code was only compiled.
- A session whose shim exits on its idle limit. Stopping the shim by hand between turns was run, and the next turn loaded the conversation.
- A harness that refuses to load a conversation, which starts a new one seeded with a summary of the journal.
- A turn that reaches its activity timeout.
- An activity retry that finds its turn still running and attaches to it. Both restart cases above ended their turn before the new worker started. The session manager does attach in its test and in the run against towa.
- More than one machine, and any harness other than towa.
