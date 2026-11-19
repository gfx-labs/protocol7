# SDK split: each p7 app builds and runs its own selfdev

Status: design. No code moves in this PR.

## Goal

A tenant (oku-trade/oku-selfdev, oku-trade/oku-review) builds and deploys only its own images, compiled against a pinned `github.com/gfx-labs/protocol7` module. It never runs a `ghcr.io/gfx-labs/protocol7-selfdev` image. The tenant's selfdev, meaning the chat workflow worker, its session `p7-worker` and the files server, lives in the tenant's repo, binary, image and manifests. oku-review, for example, does selfdev on its own Deployment, with prompts and reviewer code from `oku-trade/oku-review/kubernetes`.

protocol7-selfdev stays the platform: navi web UI, PDP, grants, network, identity, the webhooks receiver and the navi project entry for its own repo. Toward a tenant it only sends messages: Temporal signals, queries and updates, NATS reads, and forwarded webhook events.

## What was read

Checked, at these revisions:

- protocol7 `origin/master` (pkg groups `domain`, `execution`, `orchestration`, `p7`, `support`, `transport`, and `pkg/support/config`).
- protocol7-selfdev `origin/master` at `e0e2bb9` ("selfdev, navi: read settings from a YAML ConfigMap, not args"). It pins protocol7 `v0.0.0-20261119011845-fa6fd39f51ce`.
- `pkg/workflow/chat/{chat,escalation,handler,integrationtools}.go`, the names `pkg/web` uses from `chat`, `issue`, `review`, `learning`, `artifact` and `design` (counted by grep), `pkg/cli/{cli,wire,files}.go`, the package docs of `kubeop`, `acl`, `githubapp`, `upgrade`, `rollout`, `artifact` and `design`, and `containers/selfdev/Dockerfile`.
- oku-review `origin/main`: `kubernetes/{selfdev,p7-worker}.yaml`, `Dockerfile`. oku-selfdev `origin/main`: file list of `kubernetes/` and `images/`, the image and namespace lines of `kubernetes/selfdev.yaml`.

Not checked:

- `pkg/integration/a2a` is not on protocol7-selfdev `origin/master` yet (worker a2a-chats-2 is adding it). Its plan below follows the pattern of `linear` and must be confirmed when it lands.
- The frontend (`pkg/web/ts`) beyond a grep that shows it names `kubeop`. Every Temporal call in `pkg/web` (16 Signal, Update, Execute and Query call sites) was counted, not read one by one.
- The full oku-selfdev manifests and `images/Dockerfile`. The cutover assumes they match oku-review's shape, which the image line and namespace (`oku`) agree with.
- Whether the oku Temporal namespaces already run versioned workers. The flags exist (`--worker-deployment`, `--worker-build-id`, `--worker-promote`), but the oku-review `selfdev` args read here do not pass them.
- No build, test or Temporal command was run for this design.

## 1. The navi to app contract

navi reads and drives the tenant's workflows by name. Today those names live in the workflow packages themselves, so navi imports the whole chat workflow to learn a string. The contract becomes one package that both sides import, with no workflow code in it.

### Location and versioning

`github.com/gfx-labs/protocol7/pkg/domain/navi`. It holds constants, search attribute keys, memo keys and the JSON types that cross the wire. It imports only `go.temporal.io/sdk/temporal` and the standard library.

It carries `const ContractVersion = 1`. A tenant's worker sets it as a memo field (`MemoContractVersion = "contract"`) on every workflow it starts. navi reads that field and refuses actions it cannot express for a version it does not know, rather than sending signals the workflow would drop. A change that adds a signal or a field keeps the version. A change that renames or removes one raises it, and navi supports the old and new versions for one release. The wire strings below do not change in the split: running workflows are named `selfdev.Chat`, and renaming would orphan them.

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

## 2. Package-by-package plan

Placement in protocol7 follows its groups: `domain` for types, `orchestration` for Temporal workflows, `execution` for what runs beside a session, `transport` for external APIs and CLIs.

| Package (protocol7-selfdev) | Decision | Destination |
| --- | --- | --- |
| `pkg/workflow/chat` | split | `pkg/orchestration/chat` (workflow, activities, tools). Wire names and types to `pkg/domain/navi`. |
| `pkg/workflow/issue` | move | `pkg/orchestration/issue` |
| `pkg/workflow/review` | move | `pkg/orchestration/review` |
| `pkg/workflow/learning` | move | `pkg/orchestration/learning` (with `learningtest`) |
| `pkg/workflow/upgrade` | move | `pkg/orchestration/upgrade` |
| `pkg/workflow/rollout` | move | `pkg/orchestration/rollout` |
| `pkg/workspace/artifact` | move | `pkg/execution/artifact` |
| `pkg/workspace/design` | move | `pkg/execution/design` |
| `pkg/integration/linear` | move | `pkg/transport/linear` |
| `pkg/integration/githubapi` | move | `pkg/transport/githubapi` |
| `pkg/integration/githubapp` | move | `pkg/transport/githubapp` |
| `pkg/integration/a2a` | move once merged (not checked) | `pkg/transport/a2a` |
| `pkg/cluster/kubeop` | split | `Writer` and the request types to `pkg/execution/kubeop`. `Broker` stays. |
| `pkg/access/acl` | split | `scope.go` (Scope, Level, Intersect) to `pkg/domain/scope`. Policy, roles and Store stay. |
| `pkg/cli` files command | move | `pkg/transport/cli` as `p7-worker files`, built on `pkg/execution/{design,artifact,kubeop}` |
| `pkg/cluster/probe` | move with chat | `pkg/orchestration/probe` |
| `pkg/integration/webhooks`, `pkg/web`, `cmd/webhooks` | stay | platform |

Why each:

- **chat** imports `acl`, `kubeop`, `probe`, `githubapi`, `linear`, `learning`, `upgrade`, `artifact` and `design`. Each of those moves or splits first, so chat moves last among the workflows. `probe` is used by chat's probe tools. It is small (391 lines) and has no selfdev imports, so it moves to `pkg/orchestration/probe` with chat. navi does not import it.
- **issue** and **review** import `chat`, `githubapi`, `acl` and `upgrade`. They move after chat's contract types do. A tenant that does not want issue scanning simply does not register those workflows.
- **upgrade** and **rollout** are generic Worker Versioning helpers with no selfdev imports. They move first.
- **artifact** has no selfdev imports. navi reads artifacts through `artifact.Store` (`Server.Artifacts`). After the move navi imports `pkg/execution/artifact` from the SDK, which is a read of shared storage, not tenant code.
- **design**: navi uses `design.FilesClient` to proxy board files. That client moves with design. The file server (`design.Files`) is tenant side.
- **kubeop**: `NewBroker returns nil outside the core namespace`. The broker holds the selfdev pod's projected token to issue the operator credential. That is platform only and stays in protocol7-selfdev. The `Writer`, which places the credential in a session workspace beside the core worker, moves, because a tenant's files server would run it. A tenant gets operator credentials only if the platform issues them to it, which is out of scope here.
- **acl**: navi decides access (platform). The chat only needs the scope arithmetic ("A scope never grows: Intersect is how a child chat, a handler or an ... narrows it"). That part moves so the workflow can compute effective scope without the policy store.
- **githubapp** mints installation tokens from a key. Tenants today get GitHub through PDP (`GH_TOKEN: pdp-proxied-github-oku-review`), so most will not use it, but the code is generic.

### Core-specific behavior becomes options

Where the chat now branches on "the core project" (empty `Project`, `NewBroker` returning nil, `--review-repo`, Linear teams), the SDK takes a config struct from the app instead:

- `chat.Options.Tools []p7.Tool`: tools the app adds, served by `p7.HandleTools`.
- `chat.Options.Integrations` (today's `Integrations{Linear, LinearTeams, Web}`), set from config, not detected.
- `chat.Options.OperatorCredentials bool`: off for tenants. The core worker in protocol7-selfdev turns it on and registers the broker activity itself.
- `chat.Options.Project string`: the navi project name the worker serves. Empty keeps the core meaning for protocol7-selfdev.
- `chat.Options.Prompts fs.FS`: system and tool prompts read from the app's files, so oku-review keeps its prompts under its own repo.
- `review.Options.Repository` and `MergeMethod`, replacing `--review-repo` and `--merge-method`.

## 3. How an app assembles its worker

The app writes a `main` package. Settings come from `--config` (protocol7 `pkg/support/config`: each YAML key is a flag name, command line wins over the file, the file wins over environment). There is no plan file or rule language. The registration helpers (`chat.Register`, `review.Register`) are new SDK functions that wrap the `RegisterWorkflow` and `RegisterActivities` calls `pkg/cli/wire.go` makes today.

```go
package main

import (
	"context"
	"embed"
	"errors"
	"fmt"
	"io/fs"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/alecthomas/kong"

	"github.com/gfx-labs/protocol7/pkg/domain/navi"
	"github.com/gfx-labs/protocol7/pkg/orchestration/chat"
	"github.com/gfx-labs/protocol7/pkg/orchestration/review"
	"github.com/gfx-labs/protocol7/pkg/orchestration/rollout"
	"github.com/gfx-labs/protocol7/pkg/p7"
	"github.com/gfx-labs/protocol7/pkg/support/config"

	"github.com/oku-trade/oku-review/pkg/reviewer"
)

//go:embed prompts
var promptFiles embed.FS

// SelfdevWorkerSettings are the flags of `oku-review selfdev-worker`. Each one
// can be set as a key of the --config YAML file.
type SelfdevWorkerSettings struct {
	Config config.Flag `embed:""`

	TemporalAddress      string `name:"temporal-address" required:""`
	TemporalNamespace    string `name:"temporal-namespace" required:""`
	TemporalServerSPIFFE string `name:"temporal-server-spiffe-id"`
	TemporalSPIFFESocket string `name:"temporal-spiffe-socket"`
	FilesURL             string `name:"files-url"`
	GitHubTokenFile      string `name:"github-token-file" required:""`
	ReviewRepository     string `name:"review-repo"`
	MergeMethod          string `name:"merge-method" default:"squash"`
	LinearTeams          []string `name:"linear-team"`
	WorkerDeployment     string `name:"worker-deployment"`
	WorkerBuildID        string `name:"worker-build-id"`
	WorkerPromote        bool   `name:"worker-promote"`
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

// RunSelfdevWorker serves the chat and review workflows of the oku-review
// project until ctx ends.
func RunSelfdevWorker(ctx context.Context, logger *slog.Logger, settings SelfdevWorkerSettings) error {
	prompts, err := fs.Sub(promptFiles, "prompts")
	if err != nil {
		return fmt.Errorf("open embedded prompts: %w", err)
	}
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
	reviewTools, err := reviewer.Tools(reviewer.Config{TokenFile: settings.GitHubTokenFile})
	if err != nil {
		return fmt.Errorf("build reviewer tools: %w", err)
	}
	err = chat.Register(worker, chat.Options{
		Project:  "oku-review",
		Prompts:  prompts,
		Tools:    reviewTools,
		FilesURL: settings.FilesURL,
		Integrations: chat.Integrations{
			Linear:      len(settings.LinearTeams) > 0,
			LinearTeams: settings.LinearTeams,
		},
		GitHubTokenFile: settings.GitHubTokenFile,
	})
	if err != nil {
		return fmt.Errorf("register chat workflow: %w", err)
	}
	if settings.ReviewRepository != "" {
		err = review.Register(worker, review.Options{
			Repository:      settings.ReviewRepository,
			MergeMethod:     settings.MergeMethod,
			GitHubTokenFile: settings.GitHubTokenFile,
		})
		if err != nil {
			return fmt.Errorf("register review workflow: %w", err)
		}
	}
	if err := worker.Start(); err != nil {
		return fmt.Errorf("start worker: %w", err)
	}
	defer worker.Stop()
	if settings.WorkerPromote {
		if err := rollout.Promote(ctx, worker.Temporal(), settings.WorkerDeployment, settings.WorkerBuildID); err != nil {
			return fmt.Errorf("promote build %s: %w", settings.WorkerBuildID, err)
		}
	}
	<-ctx.Done()
	return ctx.Err()
}
```

The exact `rollout.Promote` and `TemporalSPIFFE` field names are read from the current code at move time. The shape (config struct in, error out, no global state) is the requirement.

The session worker and files server are not app code: the app's image builds `github.com/gfx-labs/protocol7/cmd/p7-worker` at the pinned version, and runs `p7-worker files` (moved in section 2) beside it.

## 4. Image and deploy

### What each oku repo builds

- **oku-review** builds one image from its `Dockerfile`, with three binaries: `oku-review` (reviewer-a today, plus a new `selfdev-worker` subcommand from section 3), and `p7-worker` and `p7acpshim` built from the pinned protocol7 module. The session tools that `containers/selfdev/Dockerfile` adds today (node, towa, gh, kubectl, chromium for screenshots) move to a stage of this Dockerfile, or to a second image `oku-review-session` if the reviewer image should stay small. Prompts are embedded, or mounted from a ConfigMap generated from `kubernetes/`.
- **oku-selfdev** does the same in `images/Dockerfile` with an `oku-selfdev selfdev-worker` subcommand and its own tools for the trade, retail and backend workers.

### What the manifests become

- `kubernetes/selfdev.yaml`: image is the tenant's own, args are `[selfdev-worker, --config, /etc/selfdev/config.yaml]`. A ConfigMap `selfdev-config` holds `temporal-address`, `temporal-namespace`, `files-url`, `github-token-file`, `review-repo`, `merge-method`, `worker-deployment` and the like. Only `worker-build-id` and `worker-promote` stay per pod (set from the image tag in kustomize), since they change each image.
- `kubernetes/p7-worker.yaml`: both containers use the tenant image. `p7-worker --config /etc/p7-worker/config.yaml` and `p7-worker files --config /etc/p7-worker/files.yaml`. The env block of PDP placeholders and git config stays, since it is credential plumbing, not settings.
- The PDP sidecar keeps `ghcr.io/gfx-labs/pdp`. It is the platform's egress gateway client, not selfdev, and is outside this goal's "never run protocol7-selfdev images" line.

### Cutover order (per namespace, oku-review first, then oku)

Running chats are long-lived workflows on queue `selfdev` in namespaces `oku` and `oku-review`. They must keep replaying.

1. **Turn on Worker Versioning with the current image.** Set `worker-deployment: selfdev-oku-review` and a build ID on the existing protocol7-selfdev image, and promote it. Running unversioned workflows keep running. From then on, new workflows are pinned to that build. The `upgrade` package already makes chat, issue and review continue as new onto the target build when it changes (`GetTargetWorkerDeploymentVersionChanged`).
2. **Build the tenant image at the same protocol7 code.** The first tenant build registers the same workflow names on the same queue with workflow code from the SDK version that equals the running build, so history replays. Deploy it as a second Deployment (`selfdev-next`) with a new build ID in the same Worker Deployment, not promoted.
3. **Promote the tenant build.** New workflows start on it. Pinned workflows of the old build stay there until their next continue-as-new, which `upgrade` triggers. Watch the old build's pinned count with `temporal worker deployment describe-version` until it is zero, and the old Deployment idles.
4. **Session worker.** The `p7-worker` StatefulSet's queue is per host (`--host oku-review`). Sessions are child work started by the chat. Swap its image to the tenant's in a rolling update after step 3, when no chat is pinned to the old build. A session that is mid-turn is resumed by the chat's recovery path on restart, as it is on any pod restart today.
5. **Remove the old Deployment** and the protocol7-selfdev image references. A CI check in each tenant repo greps `kubernetes/` for `ghcr.io/gfx-labs/protocol7-selfdev` and fails.

Rollback at step 3 is promoting the old build ID again. Workflows that already moved to the new build stay there, so the new build must not change history-affecting code until step 5 is done.

## 5. Sequenced PRs

Each PR merges alone and leaves every deployment working.

| # | Repo | Change | Test that proves it |
| --- | --- | --- | --- |
| 1 | protocol7 | Add `pkg/domain/navi` (constants, attribute keys, memo keys, wire types, `ContractVersion`). | `go test ./pkg/domain/navi`: a test decodes a recorded `State` JSON from a live chat query into the new type. |
| 2 | protocol7-selfdev | Bump protocol7. Make `chat`, `issue`, `review`, `learning` constants aliases of `navi` ones (`const WorkflowName = navi.ChatWorkflow`). Set the `contract` memo. | `go test ./...`. Existing workflow replay tests pass unchanged, which shows the wire strings did not move. |
| 3 | protocol7-selfdev | Switch `pkg/web` and `cmd/webhooks` to import `navi` only, not workflow packages (coordinate with a2a-chats-2 on `pkg/web`). | `go list -deps ./pkg/web` shows no `pkg/workflow/` package. Web tests pass. |
| 4 | protocol7 | Move `upgrade`, `rollout`, `linear`, `githubapi`, `githubapp`, `scope` (from `acl`), and `artifact`. These have no selfdev imports besides each other. | Their moved tests pass in protocol7 CI. |
| 5 | protocol7-selfdev | Import the packages from PR 4 and delete the local copies. | `go test ./...` and a deploy of the selfdev image in protocol7-selfdev, with one chat run end to end on the dev namespace. |
| 6 | protocol7 | Move `design`, the kubeop `Writer`, and the files command (`p7-worker files`). | `go test ./pkg/execution/...`. A test starts the files server on a temp state dir and fetches a board. |
| 7 | protocol7 | Move `learning`, `review`, `issue`, `probe`, `chat` to `pkg/orchestration`, with `Options` and `Register` from section 2 and 3. Copy the workflow replay tests and add replay of a recorded history from each oku namespace. | Replay tests in protocol7 CI pass against histories exported from `oku` and `oku-review` (`temporal workflow show --output json`). |
| 8 | protocol7-selfdev | Import the SDK workflows and delete the local packages. Keep `Broker`, policy, web, webhooks. `selfdev worker` becomes a thin `main` like section 3 with `OperatorCredentials` on. | Same replay histories pass. Deploy to protocol7-selfdev, and an existing chat continues as new onto the new build and answers a message. |
| 9 | oku-review | Turn on Worker Versioning on the current image (cutover step 1). Settings move into a ConfigMap read by `--config`. | `temporal worker deployment describe` lists the build. A new chat starts pinned. |
| 10 | oku-review | Add `selfdev-worker` subcommand, p7-worker and session tools to the Dockerfile, and the `selfdev-next` Deployment, not promoted (cutover step 2). | Image builds. The pod polls (`describe-version` shows it polling). Replay test in oku-review CI against an exported oku-review history. |
| 11 | oku-review | Promote the tenant build, then swap `p7-worker` and `files` images, then delete the old Deployment and add the image grep check (steps 3 to 5). | Old build's pinned workflow count reaches zero. A chat opened in navi on oku-review runs a turn and a review. The grep check passes. |
| 12 | oku-selfdev | PRs 9 to 11 for namespace `oku`. | Same checks on namespace `oku`, plus one chat on each of the trade, retail and backend workers. |

PRs 1 to 3 change no behavior and can merge while tenants still run protocol7-selfdev images. PRs 4 to 8 make protocol7-selfdev itself the first consumer of the SDK, which tests the SDK before any tenant depends on it. PRs 9 to 12 cut each tenant over one namespace at a time.
