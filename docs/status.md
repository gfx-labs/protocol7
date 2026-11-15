# Status

What is built, what was run, and where the code departs from `plan.md`. Last updated 2026-11-15.

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
| `pkg/p7` | The library: `NewWorker`, `OpenSession`, `PromptText`, `PromptTextFuture`, `Session.Close`, `GitClone` inputs, `SessionSpec.Tools` with `HandleTools`, `SessionSpec.Permissions` with `HandlePermissions`, `PromptTyped` with `WithReports`, and `NewClient` with `WatchSession`. Plain constructors, with no dependency on fx. `SessionSpec.OpenRetry` and `TextPrompt.Retry` let the workflow set how often opening and a turn are retried, and a harness that stopped fails with the application error type `HarnessStopped` |
| `pkg/p7` limits | `SessionSpec.Limit` and `Priority`, `WorkerConfig.Limits` (`p7.LimitConfig`), `p7.QueryLimit`. See "Session limit" below |
| `pkg/execution/snapshot`, `p7.SnapshotStore` | Snapshots of a session's directory in S3-compatible storage. See "Session snapshots" below |
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
- Fail closed: an error from the function, a refused update, and a core worker that cannot reach the owner all deny the request, also on a manager with no tool caller and for a session with no owner.
- ACP's request ID is not visible to the shim's handler, so the update ID is the session, the shim's process ID and a digest of the request. The same request sent again to a later connection gets the first decision. An agent that repeats an identical request in the same shim run gets the same decision.
- A session with `Permissions` and no tools offers no MCP server, and runs on a harness that does not take MCP over ACP.

## Typed reports

`p7.PromptTyped[T](ctx, session, prompt)` runs a turn and returns a `T` that the agent reported by calling the tool `p7_report`.

- A workflow makes its context with `p7.WithReports(ctx)` before `HandleTools` and `OpenSession`, and uses that context for all three and for `PromptTyped`. `OpenSession` then offers `p7_report` as one more tool of the session and `HandleTools` answers it. `Session.Reports` records that, so a session survives Continue-As-New.
- A session's tools are fixed when it opens, so the tool's own schema is only `{"type":"object"}`. Each typed turn carries the JSON Schema of `T` in its prompt context, made with `github.com/google/jsonschema-go` from `T`'s fields and `json` tags. A `T` that is not an object is reported as `{"value": <T>}`. The tool is accepted only while a typed prompt runs on that session.
- A call that does not match the schema is returned to the agent as a tool error, so it may fix the call in the same turn. The last valid call is the report.
- A turn that ended normally with no valid report gets one repair turn, with the same session and the turn ID `<id>-repair`. It says what was wrong and repeats the schema. After that the call fails with `*p7.ReportError`, with `Kind` `missing` or `invalid`. A turn that failed, was cancelled or was interrupted is not repaired and fails with `turn_ended`.
- The report is the tool call. Text in the agent's message is never parsed.
- It was run with a scripted agent, the real shim, the core worker and a Temporal development server (`pkg/p7/report_test.go`). It was not run with a real harness, so whether a model calls the tool reliably from the prompt is not known.

## Session snapshots

A session's directory can be kept in S3-compatible object storage, so that a session whose machine or pod is lost goes on somewhere else. It is in p7, for the host and the kubernetes backend alike. With no `--snapshot-bucket` there are no snapshots and nothing changes.

- Configuration: `p7.SnapshotStore` (`snapshot.Config`) with `Endpoint`, `Region`, `Bucket`, `Prefix`, `PathStyle` and `Exclude`. It is `coreworker.Config.Snapshots`, and the flags `--snapshot-endpoint`, `--snapshot-region`, `--snapshot-bucket`, `--snapshot-prefix`, `--snapshot-path-style` and `--snapshot-exclude` (each also `P7_SNAPSHOT_*`) of `p7 worker`, `p7 session`, `p7 pods` and `p7-worker`. `p7 pods` takes the same flags, for deleting snapshots. `p7 dev` has none. Credentials are not configuration: the standard AWS environment, shared credentials file (with `AWS_PROFILE`, SSO and credential processes) and web identity, and on Amazon's own endpoint the instance or container role, read again every five minutes so that a rewritten credential file is followed. With none found the client sends anonymous requests.
- What is kept: the whole session directory, `<state dir>/sessions/<id>`: the workspace (`work/`), the shim directory with the journal, and `session.json`. Left out: the shim's socket, pid file, lock and exit record, whatever matches `Exclude` (default `node_modules` and `go-build`; a name with no slash matches at any depth, a path is from the session directory, `path.Match` syntax), and anything that is not a regular file, a directory or a symbolic link that stays inside the tree. An excluded directory is skipped with its content.
- The harness's conversation: with snapshots on, the manager gives the harness `--store.root=<session dir>/harness` (`--harness-store-flag`, empty to pass none), so that towa's conversation is in the snapshot. A harness with no such flag keeps its conversation elsewhere. Its snapshot then restores files and journal, `session/load` is refused, and the next turn starts a new conversation seeded with a summary of the journal. A session that started before snapshots were turned on has its conversation in towa's own store and falls back the same way.
- Key and format: `<prefix>/<session id>/workspace.tar.zst`. Go's `archive/tar` in PAX format (modes, symbolic links, nanosecond modification times; no owners), compressed with `klauspost/compress/zstd` at its default level. It is streamed through a pipe into a multipart upload with 16 MiB parts, with no temporary file.
- When it is written: after `OpenSession` and after each turn that returns, and by `coreworker.Worker.Stop` (SIGTERM) for every session on the machine that is not closed. The tree is walked first and fingerprinted (names, modes, sizes, file modification times, link targets). Nothing is uploaded if the fingerprint is the one in the user metadata of the existing object (or the last one this process wrote). The upload goes to `workspace.tar.zst.tmp`, which is copied over the real key and removed, so a crash or failure mid-upload keeps the previous snapshot. An object over 5 GiB is copied with `ComposeObject`. The snapshot after a turn runs inside the `p7.RunTurn` activity, which heartbeats every two seconds while it is written, so a snapshot that takes longer than the activity's 10 second heartbeat timeout does not fail the attempt. A snapshot that fails is logged and does not fail the turn. The next one tries again. A file that changes while it is being written is cut or zero-padded at the size the walk saw, and the next walk sees the change.
- When it is read: `coreworker.Config.Session`, which `p7 session` sets from `--session`, makes `Start` restore that session before the worker polls, and fail if it cannot. Any manager also restores a session when `Open`, `RunTurn` finds no `session.json` for it and a snapshot exists, which covers the host worker on a machine whose disk was lost. A session whose directory is on the machine is not restored. The archive is extracted into `.restore-*` beside the session directory, and the stream is read to its end with the zstd checksum before the directory is renamed into place, so a cut or damaged snapshot leaves what was there. Entries with `..`, absolute names, a symbolic link that may leave the tree (absolute, climbing above the root, or with `..` after another element), an entry below a link, and any type but file, directory and symbolic link are refused with `snapshot.ErrUnsafeArchive`. The `shim_pid` of the record is cleared, so the next turn starts a shim and the harness loads the conversation. The journal continues from its last sequence number.
- Deleting: `Manager.Close` marks the session closed and then deletes the snapshot, and a closed session is not snapshotted. Writing and deleting a session's snapshot share one lock in the manager, so a snapshot that is being written when `Close` runs is deleted once its write is done, and one that starts after the mark sees it. `p7 pods`, with the snapshot flags, deletes the pod, polls until the API says it is gone and then deletes the snapshot, so that a pod's worker stopping cannot write it again. That activity heartbeats while it waits, and the workflow gives it 20 minutes and a 30 second heartbeat timeout (`podDeleteOptions`), more than the 12 minutes that the worker may take to write its last snapshot. The poll of 1 second is not configurable. A pod that is still there after 20 minutes fails the attempt, and the retry waits again, up to ten attempts.
- Library: `github.com/minio/minio-go/v7`. It adds about 4 MB to `p7-worker` (40.1 MB to 44.3 MB). A probe program with the S3 service, config and upload manager of `aws-sdk-go-v2` linked to 16.4 MB, against 1.9 MB for an empty program, which was not tried in p7-worker itself. `johannesboyne/gofakes3` is a test dependency, in `pkg/execution/snapshot/snapshottest`.
- Needs in the deployment, not done here: the bucket; credentials; the flags in the session pod template and for `p7 pods`; a `terminationGracePeriodSeconds` that covers the snapshot at stop (the command's stop timeout is 12 minutes when snapshots are on); and a lifecycle rule for incomplete multipart uploads. Nothing restarts a lost session pod yet.
- Work since the last snapshot, at most the turn in progress, is lost on a hard node failure. The workflow's retry runs the turn again.

## Session limit

A session that names a limit waits for a place in it, and `Session.Close` gives the place back.

- The state of a limit is one Temporal workflow per name, `p7.limit/<name>`, served by every `p7.Worker`. The first request starts it with a signal. It continues as new after 500 messages or when the server suggests it. No database is involved.
- The messages are signals. A workflow asks with `p7.limit.request`, the limit answers with `p7.limit.granted/<lease>` to the asking workflow, and the holder releases with `p7.limit.release`. A cancelled `OpenSession` releases its place, and so does an open that fails.
- A higher `Priority` goes first, and equals go in the order they asked. `LimitConfig.Aging` adds one point per interval waited, so that a waiter is not starved. It is off by default.
- The capacity of each name is in `WorkerConfig.Limits.Capacity`, as an exact name or a pattern with `*` (`repo:*`). A name with no capacity fails `OpenSession` with `ErrorLimitNotConfigured` and does not wait. The asking worker's configuration is sent with each request, so a changed capacity reaches a limit that is already running. Lowering it revokes nothing.
- A limit workflow looks every `LimitConfig.Reconcile` (a minute by default) for holders and waiters whose workflow is no longer running, and frees them.
- Limit workflows run on `LimitConfig.TaskQueue`, or on the queue of the first worker that asks. Consumers on several queues that share a limit should name one queue and serve it.

Departures from the plan: a session names one limit, not a list, so a session cannot yet take several limits together. The deployment-wide limit of 20 is not set: it is a name and a number in a consumer's configuration. Pending waits are not ticketd rows, and the attention list does not exist, so `p7.QueryLimit` is the only view of a queue. Temporal's task queue priority and fairness were not checked as a substitute.

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

Snapshots, on 2026-11-15 with Temporal CLI 1.10.1 (server 1.32.2) and gofakes3 1.2.0 as the store: `go vet ./...` and `go test ./...` with the CLI on `PATH`, and the tests in "Tests" below passed. The Temporal tests of `pkg/p7` took 18 seconds in all. The binary sizes above are from `go build ./cmd/p7-worker` before and after.

## Tests

`go test ./...` in both repositories. They need no network and no harness.

| Package | What it runs |
|---|---|
| `pkg/execution/journal` | The decoder on the journal of a real towa session that was cancelled, loaded again and interrupted (`testdata/towa-recovery.ndjson`) |
| `pkg/execution/session` | The manager with the real shim and a scripted ACP agent, both run from the test binary: a repeated turn, a turn picked up and cancelled, a shim stopped mid-turn, a harness that exits while it starts, and context hidden from viewers. Permission requests, with no Temporal: allowed and denied by an owner, an owner that cannot be reached, a manager with no tool caller or a caller that cannot ask, a session with no owner, and a session that did not ask for decisions |
| `pkg/transport/stream` | Publish, deduplication and an ordered read against the embedded NATS server |
| `pkg/p7` | A workflow's tool called by a scripted agent over `mcp/message`, through the real shim, the core worker and a Temporal development server, and a tool it did not offer refused. A workflow's permission function allowing, denying and failing on a scripted agent's request, an update its workflow refuses or has no handler for, an agent with no reject option, and a session with no function allowed. Typed prompts: a report, a wrapped report, a call refused and fixed in the turn, a repair turn, and a `ReportError` after the repair turn. Skipped when the `temporal` CLI is not on `PATH` |
| `pkg/p7` limits | With the same stack: with a limit of two, four sessions on one name run two at a time, and the higher priority waiter opens before an earlier lower one. A workflow that ends holding a place is freed by reconciliation, a cancelled waiter leaves the queue, and an unconfigured name fails. Also the wildcard rule and priority with aging. Skipped without the `temporal` CLI |
| `pkg/execution/snapshot` | The archive and the store against an in-process S3 (gofakes3): a round trip keeping modes, links, long names and times; excludes, a socket and a link that points out left out; an unchanged tree not uploaded again, also by a new store; a failed copy keeping the previous snapshot; a damaged object leaving the directory alone; entries that leave the workspace refused; a 40 MB snapshot in several parts; deleting |
| `pkg/p7` snapshots | With a Temporal development server and the same fake S3: a session writes files and has a conversation in two turns, its worker is stopped and its disk removed, a worker with a new empty state directory starts, and the next turn sees the files and the conversation (the scripted agent keeps its conversation in `--store.root` and refuses `session/load` without it). Run twice: the new worker is started for the session, and it restores the session on its first activity. Also that the journal has every turn once in order, that an excluded directory is gone, and that closing deletes the snapshot. Skipped without the `temporal` CLI |
| `pkg/p7` slow snapshots | With the fake S3 slowed to over twelve seconds per snapshot, every activity of the open and two turns succeeds at its first attempt under the real 10 second heartbeat timeout. It failed with attempt 2 when the heartbeat was removed. About 40 seconds |
| `pkg/orchestration/kubernetes` snapshots | Deleting a pod, run as an activity, heartbeats until the API reports it gone, then deletes the session's snapshot |
| `pkg/execution/session` snapshots | A snapshot that is being uploaded when the session closes is deleted by the close, and a closed session is not snapshotted. The narrow window between `Snapshot` reading the record and taking the store's lock is closed by the manager's lock and is not exercised by a test: the test passes without it too |
| `pkg/transport/cli`, `protocol7-selfdev/pkg/cli` | Every command's fx graph has what its components ask for. Nothing is built or started |
| `protocol7-selfdev/pkg/chat` | The chat workflow in Temporal's test environment: message order, stop, close, cancel, and a chat that cannot open |

The recorded journal holds the scratch paths of the machine it was recorded on. It was checked for keys and tokens before it was committed, and holds none.

## Where the code departs from the plan

| Plan | Code |
|---|---|
| Domain types are protobuf | Go structs with JSON tags, in `pkg/domain` |
| Clients read live output through ticketd | `p7.Client` reads NATS itself. ticketd does not exist |
| A new chat is a ticket of a chat kind | A chat is a Temporal workflow that the selfdev web server starts. There are no tickets |
| Host, container and Kubernetes backends | Host and Kubernetes. On `kubernetes`, `p7 pods` (queue `p7-kubernetes`) creates one pod per session from a JSON pod template and deletes it on close. `p7 session` in that pod runs the host session manager on the queue `p7-session-<id>`, where the session's turns run. A session pod keeps the image it was created with. Run against a Temporal dev server with a stand-in for the pod API (`pkg/p7/kubernetes_test.go`), not in a cluster. No container backend. The workspace is on the pod's own disk and, with snapshots on, also in object storage (see "Session snapshots"). Pods nobody owns are not cleaned up (gfx-labs/protocol7#5) |
| Model and git credentials come through PDP | On a workstation, sessions use the machine's own logins, which section 5 allows for trusted host execution. The talos-metal deployment puts the model credentials in PDP. Its git credential is a GitHub App token that a second container writes to a file, not a PDP binding |
| A run pod's SPIFFE ID carries its profile, repository and run ID | The talos-metal deployment has one worker pod with one identity, which every chat shares. Session pods carry the label `p7.gfx-labs.io/session`, which a ClusterSPIFFEID can render into a per-session ID, and the deployment does not use them yet |
| Permission requests are decided by policy | The workflow that opened a session decides them, with `SessionSpec.Permissions`. A session with no function has every request allowed. There is no policy language |
| Approvals, notices, steering | None of them |
| Shared limits across several names per session | Only the session limit, one name per session (see "Session limit") |
| `p7.Prompt[T]` validates a protobuf `T`; an agent gets one report-repair turn | `p7.PromptTyped[T]` takes a Go type and validates against its JSON Schema (see "Typed reports"). One repair turn, as the plan says |
| `p7.Tool` registers a user activity, called through ticketd with `p7 call` | A tool is a function in the workflow that opened the session, called over the ACP connection as a Temporal update. A tool registered on another worker is reached by the workflow running an activity on that worker's queue |

## Known limits

- Snapshots: a session is snapshotted only when a turn or an open returns, and when the worker stops. A turn that runs for hours is lost with its node. A pod that stays up past the delete's 20 minutes and then writes a snapshot leaves one behind: the delete fails, and its retry deletes it only if the pod is gone by then. With snapshots on, a store that cannot be reached, or credentials that are refused or unreadable, make `Open` and the first turn of a session that has no local record fail with the store's error, and the workflow's retry policy decides how often they are tried again. This is chosen: a manager that cannot tell whether a snapshot exists must not create the session anew, because its first snapshot would replace the one it could not read. Once a session is on the machine, a store that fails only makes the snapshots after its turns fail with a warning, and the turns go on. The 5 GiB multipart copy, a real S3 or SeaweedFS, and the real towa's conversation in `--store.root` were not run.

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

- Snapshots against SeaweedFS, MinIO or Amazon S3, and with a PDP-backed credential file. The tests use gofakes3 only.
- Snapshots with the real towa. The flag `--store.root` was checked to be accepted (`towa config`, v0.4.0), but no turn was run, because there is no model credential here, so whether towa loads a conversation from the restored directory is not known.
- A copy of a snapshot over 5 GiB (`ComposeObject`).
- A kill with SIGKILL during an upload, and a snapshot of a tree that changes while it is written, against a real store.

- Worker Versioning. `p7.WorkerConfig` takes a deployment and build ID, and that code was only compiled.
- A session whose shim exits on its idle limit. Stopping the shim by hand between turns was run, and the next turn loaded the conversation.
- A harness that refuses to load a conversation, which starts a new one seeded with a summary of the journal.
- A turn that reaches its activity timeout.
- An activity retry that finds its turn still running and attaches to it. Both restart cases above ended their turn before the new worker started. The session manager does attach in its test and in the run against towa.
- More than one machine, and any harness other than towa.
