# SDK split: each p7 app builds and runs its own selfdev

Status: design, revised 2026-11-19. Section 1 and PRs 1 to 6 are merged. Sections 2, 3 and 5 from PR 7 on describe the revised direction: protocol7 gives an extendable base chat, and every other workflow stays in the app that runs it.

## Goal

A tenant (oku-trade/oku-development, oku-trade/oku-review) builds and deploys only its own images, compiled against a pinned `github.com/gfx-labs/protocol7` module. It never runs a `ghcr.io/gfx-labs/protocol7-selfdev` image. The tenant's selfdev, meaning the chat workflow worker, its session `p7-worker` and the files server, lives in the tenant's repo, binary, image and manifests. oku-review, for example, does selfdev on its own Deployment, with prompts and reviewer code from `oku-trade/oku-review/kubernetes`.

Each namespace provides its own chat. The tenant's binary registers the SDK's base chat with its own extensions (tools, instructions, per-turn hooks, activities) on its own worker. protocol7 does not ship issue work, reviews, probes, the operator broker or a replay harness. It ships the learnings log (`pkg/orchestration/learning`, see "Learnings in the SDK" below), because all three apps ran identical copies, and leaves curation and prompts to each app. An app that wants one writes it in its own repository, reusing the code of another app by copying it, not by depending on it.

protocol7-selfdev stays the platform: navi web UI, PDP, grants, network, identity, the webhooks receiver and the navi project entry for its own repo. Toward a tenant it only sends messages: Temporal signals, queries and updates, NATS reads, and forwarded webhook events. A project whose namespace has no chat worker has no chat: navi says so and does not start one (section 5, PR 7b).

## What was read

For the first version of this design, at these revisions:

- protocol7 `origin/master` (pkg groups `domain`, `execution`, `orchestration`, `p7`, `support`, `transport`, and `pkg/support/config`).
- protocol7-selfdev `origin/master` at `e0e2bb9`, `pkg/workflow/chat/{chat,escalation,handler,integrationtools}.go`, the names `pkg/web` uses from the workflow packages, `pkg/cli/{cli,wire,files}.go`, and `containers/selfdev/Dockerfile`.
- oku-review `origin/main`: `kubernetes/{selfdev,p7-worker}.yaml`, `Dockerfile`. oku-development `origin/main`: file list of `kubernetes/` and `images/`.

For this revision, at protocol7 `3b13cac` and protocol7-selfdev `6382237`:

- Every file of protocol7-selfdev `pkg/workflow/chat` that is not a test: `chat.go`, `workflow.go`, `children.go`, `escalate.go`, `escalation.go`, `handler.go`, `handleractivities.go`, `handlertools.go`, `pool.go`, `recovery.go`, `scope.go`, and the tool files (`issuetools.go`, `pulltools.go`, `learningtools.go`, `integrationtools.go`, `design.go`, `artifacts.go`, `operator.go`, `probes.go`). The chat tests `workflow_test.go` and `escalation_test.go`, and `replay_test.go`.
- `pkg/cli/wire.go` (what the worker registers), `pkg/web/health.go` (the poller count navi already reads), the Worker Versioning section of `kubernetes/README.md` and `workerVersioning` in `kubernetes/cdk8s/src/shared.ts`.
- protocol7 `pkg/domain/navi`, `pkg/p7/{session,tool,worker}.go`, `pkg/orchestration/upgrade`, `pkg/support/prompt`.

Not checked:

- The frontend (`pkg/web/ts`) beyond a grep.
- Whether the oku Temporal namespaces run versioned workers. The core namespace does not: `workerVersioning` is `false` in `shared.ts`, so a deploy replaces the worker and running core chats replay on the new code.
- No build, test or Temporal command was run for this document. The PRs in section 5 list the tests that prove each step.

## 1. The navi to app contract

navi reads and drives the tenant's workflows by name. Today those names live in the workflow packages themselves, so navi imports the whole chat workflow to learn a string. The contract becomes one package that both sides import, with no workflow code in it.

### Location and versioning

`github.com/gfx-labs/protocol7/pkg/domain/navi`. It holds constants, search attribute keys, memo keys and the JSON types that cross the wire. It imports only `go.temporal.io/sdk/temporal` and the standard library.

It carries `const ContractVersion = 2`. Version 2 (#48) made the chat, escalation and handler payloads navi.v1 protobuf messages. A tenant's worker sets it as a memo field (`MemoContractVersion = "contract"`) on every workflow it starts. navi reads that field and refuses actions it cannot express for a version it does not know, rather than sending signals the workflow would drop. A change that adds a signal or a field keeps the version. A change that renames or removes one raises it, and navi supports the old and new versions for one release. The wire strings below do not change in the split: running workflows are named `selfdev.Chat`, and renaming would orphan them.

### Workflow types and task queue

| Constant | Value | Started by |
| --- | --- | --- |
| `ChatWorkflow` | `selfdev.Chat` | navi, issue workflow, parent chat |
| `EscalationWorkflow` | `selfdev.Escalation` | chat |
| `HandlerWorkflow` | `chat.HandlerWorkflowName` (value not copied here, read at move time) | escalation |
| `IssueWorkflow` | `selfdev.Issue` | scanner, navi |
| `IssueIntakeWorkflow` | `selfdev.IssueIntake` | navi |
| `IssueScanWorkflow` | `selfdev.IssueScan` | scanner |
| `IssueScannerWorkflow` | `selfdev.IssueScanner` | navi |
| `ReviewWorkflow` | `selfdev.Review` | navi, chat |
| `LearningWorkflow` | `selfdev.Learning` | chat |
| `TaskQueue` | `selfdev` | all of the above, per Temporal namespace |

The artifact queue `selfdev-artifacts-<host>` (`artifact.Queue(host)`) is tenant internal: navi does not start work on it. It stays in the SDK artifact package, not in the contract.

### Signals, queries and updates

Chat:

- Signals: `message` (Message), `stop` (Action), `dequeue` (Dequeue), `close`, `chat-report` (Report, sent to the owner workflow).
- Query: `state` (State).
- Update: `archive` (Archive).

Escalation:

- Signals: `escalation`, `escalation-handling`, `escalation-forward`, `escalation-intake` (on the handler).
- Query: `escalation` (EscalationState).
- Updates: `escalation-resolve`, `escalation-decide`, `escalation-review`.

Issue and scanner:

- Issue: query `state`, signal `github-event` (the webhook receiver's wake).
- Scanner: signals `wake`, `pause`, `resume`, `update-template`.

Review: signal `rereview`, query `state`. Learning: query `learning`.

### Search attributes and memo

Keyword attributes `SelfdevKind`, `SelfdevStartedBy`, `SelfdevOwner`, `SelfdevIssue`, `SelfdevProject`, `SelfdevParent`, `SelfdevEscalationState` and the bool `SelfdevArchived`. Memo fields `title`, `reason`, `outcome`, and the new `contract`. `EnsureSearchAttributes` moves with them, and each tenant's worker calls it on start for its own namespace (today the selfdev worker does).

### Wire types

The JSON types navi decodes or sends: `State` and the parts of it the page renders (`Request`, `Person`, `Repository`, `Kind`, `Status*`), `Message`, `Action`, `Dequeue`, `Archive`, `Report`, `EscalationState`, `EscalationRequest`, `EscalationStatus`, `Outcome`, issue `Ref`, `Template`, `IntakeInput`, `ScannerState`, review `Request`, `State`, `Status*`, learning `Entry`, `Scope`, `Accepted`. They move as plain structs. Unexported workflow fields such as `State.integrations` stay in the workflow package, which embeds or converts the contract type.

### NATS subjects

navi reads session output on `p7.session.<id>.out` (`stream.Subject` in protocol7 `pkg/transport/stream`). That is already in the SDK. The contract re-exports it and states that navi reads it from the tenant's NATS (`nats_url` in navi's projects file), never writes.

### Webhook events

The receiver in protocol7-selfdev signals `github-event` on the issue workflow and starts or signals reviews. It does not change, but it stops importing `issue` and `review` and uses the contract names instead.


## 2. What protocol7 gives and what stays in the apps

Placement in protocol7 follows its groups: `domain` for types, `orchestration` for Temporal workflows, `execution` for what runs beside a session, `transport` for external APIs and CLIs.

### Already in the SDK (PRs 1 to 6, merged)

| Package | From protocol7-selfdev |
| --- | --- |
| `pkg/domain/navi`, `pkg/domain/scope` | the wire names and types, the scope part of `acl` |
| `pkg/orchestration/upgrade`, `pkg/orchestration/rollout` | `pkg/workflow/upgrade`, `pkg/workflow/rollout` |
| `pkg/transport/linear`, `githubapi`, `githubapp` | `pkg/integration/*` |
| `pkg/execution/artifact`, `pkg/execution/design` | `pkg/workspace/*` |
| `pkg/execution/kubeop` | the `Writer` and its request types |
| `pkg/transport/cli` `p7-worker files` | the files command |

These stay. They are libraries an app calls, not workflows it must run.

### The base chat (PR 7)

`pkg/orchestration/chat` is drawn from protocol7-selfdev `pkg/workflow/chat`. The base is what navi's contract (`pkg/domain/navi`) needs, plus the turn loop that serves it:

| Base part | From (protocol7-selfdev `pkg/workflow/chat`) |
| --- | --- |
| `State`, the query `state` (navi `ChatState`), the contract memo and search attributes at start | `chat.go` |
| messages queued in order, one turn each on a p7 session, the hidden context of the first turn, a person's guidance on a session | `workflow.go`, `chat.go` |
| stop (keeps the queue unless `Discard`), dequeue, close, cancel, the update `archive` | `workflow.go` |
| reports to an owner workflow (`chat-report`) | `workflow.go` |
| resume of a turn whose session processes stopped | `recovery.go` |
| continue-as-new when Temporal suggests it or the target build changed, the hourly wake of an idle chat | `workflow.go` with `pkg/orchestration/upgrade` |
| the escalate tool, the escalation workflow, delivery of a person's answer as a message | `escalate.go`, `escalation.go` |
| child chats (`chat_spawn`, `chat_await`, `chat_message`, `chat_attention`) | `children.go` |
| session pools named from permissions, placement, effective scope | `pool.go`, `workflow.go`, `scope.go` |

The base keeps every `workflow.GetVersion` change ID it takes over (`upgrade-wake`, `stop-keeps-queue`, `turn-resume`, `chat-model-limit`, `escalation-handler`), with the same branches, so a history written by protocol7-selfdev's chat replays on the base when the app passes the same extensions.

Everything else is an extension the app passes in, and stays in the app's repository:

| Extension | Today in protocol7-selfdev | Who keeps it |
| --- | --- | --- |
| issue tools (`issue_*`) and their activities | `issuetools.go`, `issueactivities.go` | each app that wants them |
| pull request tools (`pr_*`) | `pulltools.go`, `pullactivities.go` | each app that wants them |
| learning tools and the first-turn brief | `learningtools.go`, `pkg/workflow/learning` | each app that wants them, on the SDK's log |
| the escalation handler (an agent that decides before a person) | `handler.go`, `handleractivities.go`, `handlertools.go` | each app that wants one. Without it an escalation goes to a person at once |
| operator credentials | `operator.go`, `pkg/cluster/kubeop` `Broker` | protocol7-selfdev only |
| probes | `probes.go`, `pkg/cluster/probe` | protocol7-selfdev only |
| design board and picks | `design.go` | each app that wants it, on `pkg/execution/design` |
| artifacts | `artifacts.go` | each app that wants it, on `pkg/execution/artifact` |
| Linear and web tools | `integrationtools.go`, `linearactivities.go`, `webactivities.go` | each app that wants them, on `pkg/transport/linear` |
| the instructions of a chat (repositories, review rules, issue rules) | `chat.go` `instructions` | each app. The base has a short default |
| issue, review and learning workflows | `pkg/workflow/{issue,review,learning}` | each app that wants them |

protocol7 ships no replay harness. Each app keeps replay tests of its own histories with its own extensions, as protocol7-selfdev does in `pkg/workflow/chat/replay_test.go`.

### The extension API

```go
package chat

// Registry is where Register puts the workflows and activities. *p7.Worker
// is one.
type Registry interface {
	RegisterWorkflow(name string, function any)
	RegisterActivity(name string, function any)
}

// Options are what an app adds to the base chat. The zero value is a chat
// with the base tools, the default instructions and escalations that go to
// a person at once.
type Options struct {
	// Tools returns the app's tools for one run of a chat. It is called at
	// the start of each run, after Continue-As-New too, and must return the
	// same tools for the same State, because a session keeps the tools it
	// was opened with. The base adds escalate and the child chat tools to
	// every chat that is not an escalation handler's.
	Tools func(chat *Chat) []p7.Tool
	// Instructions is the hidden context of a chat's first turn. Nil uses
	// DefaultInstructions. An error fails the turn and leaves the chat open.
	Instructions func(request Request) (string, error)
	// Brief is added to the first turn's context after the instructions.
	// It may run activities. An empty result adds nothing.
	Brief func(ctx workflow.Context, chat *Chat) string
	// TurnContext is added to the context of every turn, after the brief.
	TurnContext func(ctx workflow.Context, chat *Chat, message Message) string
	// EscalationHandoff names the activity that gives a new escalation to
	// the app's handler, which then has handlerDeadline to decide before a
	// person gets it. Empty sends every escalation to a person at once.
	EscalationHandoff string
	// ValidateDecision, when set, refuses a decision of the app's handler
	// that the base accepts, such as a resolution that names no record of
	// the app's.
	ValidateDecision func(decision EscalationDecision) error
	// Activities are the app's activities, by name, registered with the
	// workflows.
	Activities map[string]any
}

// Chat is one run of a chat workflow, as the app's tools and hooks see it.
type Chat struct {
	// State is the run's state. Tools may change the fields the contract
	// gives extensions (Board, Probes, Operator, Learnings, LastLearning).
	State *State
}

// Escalate raises an escalation for the chat and returns its ID, as the
// escalate tool does.
func (c *Chat) Escalate(ctx workflow.Context, request EscalationRequest) (string, error)

// Spawn starts a child chat with a task and returns its ID, as chat_spawn
// does.
func (c *Chat) Spawn(ctx workflow.Context, task, title string, escalation *Escalation) (string, error)

// Register registers the chat workflow (navi.ChatWorkflow) and the
// escalation workflow (navi.EscalationWorkflow) with the options, and the
// options' activities.
func Register(registry Registry, options Options) error

// NewWorkflow is the chat workflow with the options, for a replayer.
func NewWorkflow(options Options) func(workflow.Context, State) (Report, error)

// NewEscalationWorkflow is the escalation workflow with the options.
func NewEscalationWorkflow(options Options) func(workflow.Context, EscalationState) (EscalationState, error)
```

`State` has the exported fields of `navi.ChatState`, so the query result navi decodes is unchanged. The app's handler workflow, issue workflow and review workflow start chats by the contract names and the base `Request`, `State` and `Report` types.

A tool of the app runs in the chat's workflow, as a Temporal update. Its activities run on the app's worker, on `navi.TaskQueue`. An app that drops a tool, a hook or a `GetVersion` its running chats used must let those chats drain on the old build first (section 4), because a replayed tool call or activity that the new code does not make fails the replay.

## 3. How an app assembles its worker

The app writes a `main` package. Settings come from `--config` (protocol7 `pkg/support/config`: each YAML key is a flag name). There is no plan file or rule language. The app's tools, instructions and workflows are its own Go code. The sample is oku-review's worker, with a review tool of its own and the Linear tools built on `pkg/transport/linear`.

```go
package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/alecthomas/kong"

	"github.com/gfx-labs/protocol7/pkg/domain/navi"
	"github.com/gfx-labs/protocol7/pkg/orchestration/chat"
	"github.com/gfx-labs/protocol7/pkg/orchestration/rollout"
	"github.com/gfx-labs/protocol7/pkg/p7"
	"github.com/gfx-labs/protocol7/pkg/support/config"
	"github.com/gfx-labs/protocol7/pkg/support/prompt"

	"github.com/oku-trade/oku-review/pkg/reviewer"
)

// SelfdevWorkerSettings are the settings of `oku-review selfdev-worker`. Each
// one is a key of the --config YAML file.
type SelfdevWorkerSettings struct {
	config.Flag `embed:""`

	TemporalAddress      string   `name:"temporal-address" required:""`
	TemporalNamespace    string   `name:"temporal-namespace" required:""`
	TemporalServerSPIFFE string   `name:"temporal-server-spiffe-id"`
	TemporalSPIFFESocket string   `name:"temporal-spiffe-socket"`
	GitHubTokenFile      string   `name:"github-token-file" required:""`
	ReviewRepository     string   `name:"review-repo" required:""`
	LinearTeams          []string `name:"linear-team"`
	WorkerDeployment     string   `name:"worker-deployment"`
	WorkerBuildID        string   `name:"worker-build-id"`
	WorkerPromote        bool     `name:"worker-promote"`
}

func main() {
	var settings SelfdevWorkerSettings
	kong.Parse(&settings, config.Option())
	logger := slog.New(slog.NewJSONHandler(os.Stderr, nil))
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := RunSelfdevWorker(ctx, logger, settings); err != nil && !errors.Is(err, context.Canceled) {
		logger.Error("selfdev worker stopped", "error", err)
		os.Exit(1)
	}
}

// ReviewInstructions are the hidden context of an oku-review chat's first
// turn.
func ReviewInstructions(request chat.Request) (string, error) {
	text := prompt.New(prompt.Vars{"Branch": chat.Branch(request.ID), "Repositories": request.Repositories}).
		P("This is an oku-review selfdev chat. A person talks to you through navi.").
		Bullets(`
			Each repository is cloned on the branch {{.Branch}}.
			Run the repository's checks before you say a change works.
			File a review of a pull request you opened with review_file.
		`)
	if err := text.Err(); err != nil {
		return "", fmt.Errorf("build the instructions: %w", err)
	}
	return text.String(), nil
}

// RunSelfdevWorker serves the chat of the oku-review project until ctx ends.
func RunSelfdevWorker(ctx context.Context, logger *slog.Logger, settings SelfdevWorkerSettings) error {
	worker, err := p7.NewWorker(p7.WorkerConfig{
		TaskQueue:         navi.TaskQueue,
		TemporalAddress:   settings.TemporalAddress,
		TemporalNamespace: settings.TemporalNamespace,
		TemporalSPIFFE: p7.TemporalSPIFFE{
			ServerID: settings.TemporalServerSPIFFE,
			Socket:   settings.TemporalSPIFFESocket,
		},
		Deployment: settings.WorkerDeployment,
		BuildID:    settings.WorkerBuildID,
		Logger:     logger,
	})
	if err != nil {
		return fmt.Errorf("create worker: %w", err)
	}
	if err := navi.EnsureSearchAttributes(ctx, worker.Temporal(), settings.TemporalNamespace); err != nil {
		return fmt.Errorf("register search attributes: %w", err)
	}
	reviews, err := reviewer.NewActivities(reviewer.Config{
		TokenFile:  settings.GitHubTokenFile,
		Repository: settings.ReviewRepository,
	})
	if err != nil {
		return fmt.Errorf("configure the reviewer: %w", err)
	}
	err = chat.Register(worker, chat.Options{
		Tools: func(current *chat.Chat) []p7.Tool {
			return []p7.Tool{reviewer.FileTool(current.State.Request)}
		},
		Instructions: ReviewInstructions,
		Activities: map[string]any{
			reviewer.ActivityFile: reviews.File,
		},
	})
	if err != nil {
		return fmt.Errorf("register the chat: %w", err)
	}
	if err := worker.Start(); err != nil {
		return fmt.Errorf("start worker: %w", err)
	}
	defer worker.Stop()
	if settings.WorkerPromote {
		promoter := rollout.FromClient(worker.Temporal(), logger)
		if err := promoter.Promote(ctx, settings.WorkerDeployment, settings.WorkerBuildID); err != nil {
			return fmt.Errorf("promote build %s: %w", settings.WorkerBuildID, err)
		}
	}
	<-ctx.Done()
	return ctx.Err()
}
```

`reviewer.FileTool` returns a `p7.Tool` whose `Run` is a workflow function that runs `reviewer.ActivityFile`. The `reviewer` names are the app's to choose. The requirement is the shape: settings in a config struct, tools and instructions as Go functions passed to `chat.Register`, errors returned, no global state.

The session worker and files server are not app code: the app's image builds `github.com/gfx-labs/protocol7/cmd/p7-worker` at the pinned version, and runs `p7-worker files` beside it.

## 4. Image and deploy

### What each oku repo builds

- **oku-review** builds one image from its `Dockerfile`, with three binaries: `oku-review` (reviewer-a today, plus a new `selfdev-worker` subcommand from section 3), and `p7-worker` and `p7acpshim` built from the pinned protocol7 module. The session tools that `containers/selfdev/Dockerfile` adds today (node, towa, gh, kubectl, chromium for screenshots) move to a stage of this Dockerfile, or to a second image `oku-review-session` if the reviewer image should stay small. Prompts are embedded, or mounted from a ConfigMap generated from `kubernetes/`.
- **oku-development** does the same in `images/Dockerfile` with an `oku-development selfdev-worker` subcommand and its own tools for the trade, retail and backend workers.

### What the manifests become

- `kubernetes/selfdev.yaml`: image is the tenant's own, args are `[selfdev-worker, --config, /etc/selfdev/config.yaml]`. A ConfigMap `selfdev-config` holds `temporal-address`, `temporal-namespace`, `files-url`, `github-token-file`, `review-repo`, `merge-method`, `worker-deployment` and the like. Only `worker-build-id` and `worker-promote` stay per pod (set from the image tag in kustomize), since they change each image.
- `kubernetes/p7-worker.yaml`: both containers use the tenant image. `p7-worker --config /etc/p7-worker/config.yaml` and `p7-worker files --config /etc/p7-worker/files.yaml`. The env block of PDP placeholders and git config stays, since it is credential plumbing, not settings.
- The PDP sidecar keeps `ghcr.io/gfx-labs/pdp`. It is the platform's egress gateway client, not selfdev, and is outside this goal's "never run protocol7-selfdev images" line.

### Cutover order for a tenant (per namespace, oku-review first, then oku)

Running chats are long-lived workflows on queue `selfdev` in namespaces `oku` and `oku-review`. They must keep replaying.

1. **Turn on Worker Versioning with the current image.** Set `worker-deployment: selfdev-oku-review` and a build ID on the existing protocol7-selfdev image, and promote it. Running unversioned workflows keep running. From then on, new workflows are pinned to that build. The chat already continues as new onto the target build when it changes (`pkg/orchestration/upgrade`).
2. **Build the tenant image.** Its chat is the SDK base with the tenant's own extensions, which are not the protocol7-selfdev tools the running chats were opened with. So it does not replay those histories, and must not be given them: deploy it as a second Deployment (`selfdev-next`) with a new build ID in the same Worker Deployment, not promoted. The tenant's own replay test covers histories its new build writes, not the old ones.
3. **Promote the tenant build.** New workflows start on it. Pinned workflows of the old build stay there until their next continue-as-new, which starts on the tenant build with the tenant's tools. Watch the old build's pinned count with `temporal worker deployment describe-version` until it is zero, and the old Deployment idles. The issue and review workflows of the old build drain the same way, or the tenant keeps the old Deployment until they end, if it does not write its own.
4. **Session worker.** The `p7-worker` StatefulSet's queue is per host (`--host oku-review`). Swap its image to the tenant's in a rolling update after step 3. A session that is mid-turn is resumed by the chat's recovery path on restart, as it is on any pod restart today.
5. **Remove the old Deployment** and the protocol7-selfdev image references. A CI check in each tenant repo greps `kubernetes/` for `ghcr.io/gfx-labs/protocol7-selfdev` and fails.

Rollback at step 3 is promoting the old build ID again. Workflows that already moved to the new build stay there.

### Cutover of the core project

protocol7-selfdev's core chat runs unversioned (`workerVersioning` is `false`), so a deploy replaces the worker and every running core chat replays on the new code. Its switch to the base therefore keeps its histories replaying: it passes today's tools, instructions, learning brief, picks and guidance as extensions, keeps every `GetVersion` ID, and its replay test runs histories recorded from the core namespace before the switch (PR 8).

## 4a. A project with no chat worker

A project whose Temporal namespace has no poller on `navi.TaskQueue` (`DescribeTaskQueue`, as `pkg/web/health.go` reads it) has no chat. navi says the chat is not set up for that project, and refuses to start or message a chat there, in the web API, on the project page and over A2A, instead of starting a workflow that never runs. Temporal lists a poller for some minutes after its last poll, so a worker that has just stopped still counts for a while.

## 5. Sequenced PRs

Each PR merges alone and leaves every deployment working.

| # | Repo | Change | Test that proves it | State |
| --- | --- | --- | --- | --- |
| 1 | protocol7 | Add `pkg/domain/navi` and `pkg/domain/scope`. | `go test ./pkg/domain/navi` decodes a recorded `State`. | merged (#34) |
| 2 | protocol7-selfdev | Workflow constants become aliases of `navi`. Set the `contract` memo. | Existing replay tests pass unchanged. | merged |
| 3 | protocol7-selfdev | `pkg/web` and `cmd/webhooks` import `navi`, not workflow packages. | `go list -deps ./pkg/web` shows no `pkg/workflow/`. | merged |
| 4 | protocol7 | Move `upgrade`, `rollout`, `linear`, `githubapi`, `githubapp`, `artifact`. | Moved tests pass in CI. | merged (#35) |
| 5 | protocol7-selfdev | Import PR 4's packages, delete the copies. | `go test ./...` and a deploy. | merged |
| 6 | protocol7 | Move `design`, the kubeop `Writer` and `p7-worker files`. | `go test ./pkg/execution/...`. | merged (#41) |
| 7 | protocol7 | Add `pkg/orchestration/chat`: the base chat of section 2 with `Options`, `Register` and `NewWorkflow`. No issue, review, learning, probe or operator code, no replay harness. | A test on Temporal's test environment: a turn, a tool an extension adds, stop, an escalation answered by a person, continue-as-new onto a changed target build. | merged (#47, `1399d50`) |
| 7b | protocol7-selfdev | navi's "chat not set up" state (section 4a) in the web API, the project page and A2A. | A web test with a project whose namespace has no poller: start and message are refused with that message. | merged (#138) |
| 8 | protocol7-selfdev | The core chat registers the SDK base with today's tools, instructions and hooks as extensions. `pkg/workflow/chat` keeps the extensions, the handler and the replay tests. Issue, review, learning and probe stay in this repo. | `go test ./...`. The replay test runs the oku and oku-review histories already in `testdata` and a core history recorded before the switch. Deploy, and a running core chat answers a message on the new build. | merged (#139, `3b757f9`), deployed, a chat started before the switch answered on the new build |
| 9 | oku-review | Turn on Worker Versioning on the current image (tenant cutover step 1). Settings in a ConfigMap read by `--config`. | `temporal worker deployment describe` lists the build. A new chat starts pinned. | |
| 10 | oku-review | A `selfdev-worker` subcommand that registers the base chat with oku-review's extensions, p7-worker and session tools in the Dockerfile, and the `selfdev-next` Deployment, not promoted (step 2). | Image builds. The pod polls. A replay test in oku-review of a history its new build wrote. | |
| 11 | oku-review | Promote the tenant build, swap `p7-worker` and `files` images, delete the old Deployment, add the image grep check (steps 3 to 5). | Old build's pinned count reaches zero. A chat opened in navi on oku-review runs a turn. The grep check passes. | |
| 12 | oku-development | PRs 9 to 11 for namespace `oku`. | Same checks on namespace `oku`. | |

PRs 1 to 6 changed no behavior. PR 7 adds code no one runs yet. PR 8 makes protocol7-selfdev the first consumer of the base chat, which tests it before a tenant depends on it. PRs 9 to 12 cut each tenant over one namespace at a time, each with its own extensions written in its own repository.

The first version of this plan moved `learning`, `review`, `issue`, `probe` and the whole chat into protocol7 (protocol7 PR #43). That PR was closed: those workflows are app code, and each app reimplements the ones it needs.

## Learnings in the SDK

Decided 2026-11-22 (user), and narrowed the same day (eddy). The learnings log was copied unchanged into protocol7-selfdev, oku-development and oku-review, so the log lives in `pkg/orchestration/learning` now, with `learningtest` beside it. The SDK provides the log and its storage and holds no curation and no prompt text. Each app builds its own curation on the log.

- The SDK holds the `Entry`, `Scope`, `Source` and `State` types and their validation (with the secret check), the learning workflow (one per entry, the memo, the search attribute, the decide update), the `Store` (`Record`, `List`, `Get`, `Decide`), and the `Record` and `Decide` activities, which only write the log. `learning.Register` registers the workflow and those two activities. `learning.RegisterWorkflow` registers the workflow alone, for an app that registers its own activities under the same names. An entry may carry an opaque `Vector` and `VectorModel`, which the SDK stores and does not compute. `pkg/support/embedding` is a plain client that an app uses to compute them.
- Each app writes the rest: search and ranking, the brief for a first prompt, scope scoring, semantic ranking, duplicate holding, area suggestion, the thresholds and areas settings, embedding entries when they are first needed, `Format`, and the chat tools (`learning_record`, `learning_search`, `learning_decide`) with their instructions and every text a model reads. protocol7-selfdev's `pkg/workflow/curation` is the first.
- The workflow is registered as `selfdev.Learning`, the activities as `LearningRecord`, `LearningSearch`, `LearningBrief` and `LearningDecide`, and the memo and search attributes keep their formats. An app's curation activities keep those names, `LearningSearch` and `LearningBrief` and its own `LearningRecord` that replaces the SDK's and calls it to write, so that running entries and chats started on an app's own copy replay.
