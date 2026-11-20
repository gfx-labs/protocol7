# Subchats and navi

Status: decided, 2026-11-20 at 11:23Z. The user approved implementation and rollout in SDK, core navi, oku-review and oku-selfdev order.

## Summary

Keep subchats in the SDK base chat. They are already native Temporal child workflows, with model tools and protobuf relation fields. Complete the lifecycle behavior and make navi show and control the same tree. Do not add a separate agent scheduler, child registry, result store or retry engine.

Decided behavior:

- `chat_spawn` starts a bounded task in the same Temporal namespace and navi project, then returns its chat ID after Temporal acknowledges the start.
- The parent starts a turn automatically whenever a child needs attention, including a result, escalation, failure or stop waiting for guidance. `chat_await` remains an optional explicit wait, never a prerequisite for wake-up.
- Keep the existing limits of four live children and sixteen lifetime direct children per chat, with a maximum depth of two below the root. Keep the existing 24 hour child execution timeout for the first release.
- Preserve `PARENT_CLOSE_POLICY_REQUEST_CANCEL`. Closing or cancelling a chat requests closure of its descendants. Stopping a turn does not close the tree.
- Keep the parent run alive while it has live children. Do not attempt to transfer native child futures through continue-as-new.
- Keep existing workflow kinds and wire names. navi distinguishes a subchat from an issue or handler session by its verified parent relation, not by `owner` alone.

Section 8 records the user's decisions. Manual spawning is available through the model tool, `Chat.Spawn`, navi UI and A2A.

## 1. Investigation

### Checked

Fetched the repositories and checked their open PRs before reading source on 2026-11-20. All source claims below refer to the fetched revisions, not to the older local default branches.

| Repository | Read revision | Relevant source |
| --- | --- | --- |
| `gfx-labs/protocol7`, `master` | `a503227b` | `docs/sdk-split.md`, `pkg/orchestration/chat/{chat,options,workflow,children,escalate,escalation,pool,recovery}.go`, `pkg/orchestration/upgrade/upgrade.go`, `pkg/domain/navi/{navi,chat,client,attributes}.go`, `proto/navi/v1/{chat,common,escalation}.proto`, `pkg/p7/tool.go` |
| `gfx-labs/protocol7-selfdev`, `master` | `7647e4c` | `pkg/cli/wire.go`, `pkg/workflow/chat/{base,workflow,instructions}.go`, `pkg/web/{server,chats,chatservice,chatworker,scope,access,statejson,page,a2a}.go`, `pkg/integration/a2a/a2a.go`, `pkg/web/ui/src/pages/{ChatsPage,ChatView}.tsx`, UI API types |
| `oku-trade/oku-review`, `main` | `2936313` | `pkg/chat/{register,workflow,instructions}.go`, `go.mod` |
| `oku-trade/oku-selfdev`, `main` | `421bb67` | `pkg/chat/{register,workflow,instructions}.go`, `go.mod` |

The SDK and oku-review had no open PRs. The platform had [protocol7-selfdev#68](https://github.com/gfx-labs/protocol7-selfdev/pull/68), for navi GitHub access. oku-selfdev had [oku-selfdev#30](https://github.com/oku-trade/oku-selfdev/pull/30), for production auditing. Neither implements subchats. Recheck before each implementation PR because these repositories are active.

Also read Temporal's official child workflow, Go child workflow, continue-as-new and Parent Close Policy documentation. Checked cancellation options in the pinned Temporal Go SDK `v1.51.0`, and task metadata/card extension types in the pinned a2a-go `v2.9.0` dependency. See section 9.

### Already implemented

`pkg/orchestration/chat/children.go` supplies `chat_spawn`, `chat_await`, `chat_message` and `chat_attention`. `Chat.Spawn` exposes the same behavior to app extensions. `Options.Tools` adds app tools alongside these base tools. Handler chats do not receive the coordination tools or `escalate`.

A spawn deep-clones the parent's request. It preserves project, model list, effective scope, restrictions, placement and repository configuration. It clears the issue association, sets its own ID and branch, resets the pool name for permission-based recomputation, sets `Parent` to the parent chat ID and `Owner` to the parent workflow ID, and increments `Depth`. The first pending message contains the task. It starts `selfdev.Chat` on `selfdev` with memo, search attributes, `REQUEST_CANCEL` and a 24 hour execution timeout.

The parent's workflow coroutine observes the native child future and stores the final `Report` in `State.Children`. `chat-report` signals currently convey attention to the parent. The parent does not automatically queue a new turn for a completed child. The parent already refuses continue-as-new while any child is live.

The protobuf contract already has the relations:

| Existing field | Tag | Meaning |
| --- | --- | --- |
| `ChatRequest.parent` | 15 | Immediate parent chat ID, not a workflow Run ID |
| `ChatRequest.depth` | 16 | Root is zero |
| `ChatRequest.owner` | 10 | Workflow ID receiving reports, which is not necessarily a chat |
| `ChatState.children` | 3 | Direct children, task message ID, done/attention flags and result |
| `ChatSummary.parent` | 7 | Parent ID in list responses |

`SelfdevParent` already indexes the immediate parent. `SelfdevProject`, `SelfdevOwner`, `SelfdevKind` and the `contract` memo already exist. `ContractVersion` is 2. `SelfdevParent` is also used by escalations, so a relation query must restrict workflow type.

The core project wraps `base.NewWorkflow(Options(...))` and registers it in `pkg/cli/wire.go`. Both oku apps call `base.Register(worker, options)` in `pkg/chat/register.go`. Their custom instructions already describe child tasks. Their fetched SDK pins are `77cb3552` for oku-review and `07ab666d` for oku-selfdev. The platform pin is `a503227b`. No app needs another app's worker image to obtain these tools.

### Gaps found by source inspection

- `navi.KindOf` classifies any ordinary request with an owner as `session`. Subchats therefore appear in Sessions, not in the Chats list. `ChatsPage.tsx` renders flat lists, even though summaries carry `parent`.
- `ChatView.tsx` already has a parent link and a children panel. Children link to `/s/`, show only working/attention/finished, and inherit the owner-controlled session UI. The API's `refuseSession` and the separate A2A stop implementation refuse to stop them.
- The UI is React under `pkg/web/ui`, not server-rendered chat page templates. `pkg/web/page.go` serves its built assets and SPA entry point.
- A child currently auto-closes after a turn without attention, pending messages or open escalations. That condition does not check its own live children. A child that spawns a grandchild can close and cancel it before consuming its result.
- A stopped child can satisfy the same auto-close condition. Merely exposing the existing Stop API would therefore not give it the root chat's stop behavior.
- A child record is appended before its start is acknowledged. Failed starts must settle that record and release the live-child count, not leave a phantom child. This needs a focused check of the start/future ordering.
- The list reads one visibility page of 200 executions and deduplicates runs. A tree cannot assume this is a complete family. Queries use the project's Temporal client but do not explicitly filter `SelfdevProject`.
- `chat-model-limit` version 2 no longer takes shared model leases. Provider rate limits are not a concurrency or spending budget. The current `chat_await` description still claims it releases a shared model slot.

These are source findings, not reproduced production failures.

### Not checked

No cluster, deployed worker/versioning configuration, live Temporal history, namespace retention, browser or A2A endpoint was inspected. No workflow test or application build was run for this design. Existing SDK child tests and tenant replay-test locations were inspected, not executed. The lifecycle changes and acceptance scenarios below remain unverified.

No deployment files are changed by this proposal. Platform migration work is separate. In particular, this design does not change protocol7-selfdev `kubernetes/` or `cdk8s`.

## 2. Model and ownership

### SDK base, not an extension

Keep the existing tools and `Chat.Spawn` in `pkg/orchestration/chat`. Parentage, task completion, stop/close behavior, authority inheritance and navi's contract must mean the same thing for every app. Apps still own their instructions, domain tools, activities and escalation handlers through `Options`.

A subchat is a bounded task conversation, not a copy of the parent's transcript or workspace. It gets its own p7 session and `chat/<id>` branch. Its input is the explicit task and the inherited request configuration. Results name commits, artifacts and remaining questions. The parent decides whether to integrate that work. No automatic merge or shared working directory is introduced.

A spawn cannot choose another namespace, project, model, credential, backend or broader scope. Keep model selection inherited in this release. Keep the existing scope-based pool selection, which may share worker capacity but does not imply a shared conversation.

### Relation and bounds

Temporal's native parent execution relationship owns lifecycle. The stable application relation is `ChatRequest.parent`, mirrored in `SelfdevParent` for visibility. `owner` remains a report destination and is not substituted for `parent`. Memo continues to hold title and contract version. Do not introduce another parent memo or duplicate the whole children list into memo.

Keep existing wire kinds for compatibility. An ordinary subchat has a nonempty parent, no legacy `Escalation`, and is not a handler chat. navi verifies its workflow type and native parent identity, and checks both chats' project membership when both are retained. An expired parent does not remove the child's own project authorization. Add one shared domain helper for the request classification so API and UI mappings do not invent different rules. This classification changes presentation and permitted chat controls, not Temporal ownership.

Keep these bounds initially:

| Bound | Policy |
| --- | --- |
| Depth | Root 0, child 1, grandchild 2. A depth 2 chat cannot spawn |
| Live direct children | 4, including opening and attention/escalation waits |
| Lifetime direct children | 16, including failed starts, retained across continue-as-new |
| Task and guidance | Nonempty, at most 64 KiB. Title at most 120 bytes |
| Child lifetime | 24 hours across its execution chain, not reset by continue-as-new |

These limits apply per parent, not per project. A fully occupied depth-two tree can have 21 live chats including its root. Keep the lifetime count in carried state rather than resetting it each run. Do not add a configurable limit subsystem for this UI change. A future setting, if needed, belongs in an app configuration struct and its YAML config, never in a plan DSL or a list of container flags.

## 3. Execution, results and lifecycle

### Spawn and result flow

1. Validate the task, parent state and limits before creating commands. Generate the stable child ID with the existing `workflow.SideEffect` helper. Keep replayed IDs unchanged.
2. Build the child request from a deep clone. Start it with `workflow.ExecuteChildWorkflow` in the current namespace and queue. Wait for `GetChildWorkflowExecution` before returning success from `chat_spawn`.
3. Retain the native result future in the owning parent run. Await it in a workflow coroutine, not a goroutine or activity. Set the child record terminal on completion, failure, timeout or confirmed cancellation. A failed start is also terminal and carries its error.
4. Keep final result authority with that future. Signals report attention and escalation progress, not a second completion protocol. Match reports to a recorded child and, where present, its task/guidance message ID. An open-escalation report also makes `chat_await` return attention with the escalation reference. Ignore stale attention after completion.
5. `chat_spawn` returns without waiting for the result. The model can continue independent work or call `chat_await` for selected children. A timeout returns pending records rather than cancelling them. Only one await is active per parent. `chat_message` can guide only a live recorded direct child.

Queue a bounded parent message and automatically run a parent turn on every new result, escalation, failure or stop waiting for guidance. Serialize these messages through the existing pending-turn queue, including when a parent turn is already running. Native child futures remain authoritative for terminal results. Attention signals must match a live recorded child and must not duplicate terminal notifications. `chat_await` may consume available records or wait explicitly, but wake-up never depends on its use.

Keep `chat_attention` for a child asking its parent for guidance. `escalate` still means a request to the app's handler or a person. The two must not be conflated.

### Completion, stop and close

| Event | Recommended behavior |
| --- | --- |
| Child completes its task turn | Auto-close only when it has no pending messages, attention, open escalations or live children |
| Child has live grandchildren after its turn | Keep its workflow open without consuming model tokens while waiting. A grandchild result or attention notice queues the next parent turn automatically |
| Person stops a child's running turn | Stop only that turn and preserve the queue. If no queued turn remains, keep the child open for guidance, mark attention and report that it was stopped. Do not turn a stop into successful task completion |
| Person stops the parent's turn | Do not stop its independent children. A cancelled `chat_await` stops waiting, not the children |
| Person closes a chat | Discard its pending messages, cancel its current turn, request close of live direct children, withdraw its open escalations and close its p7 session |
| Workflow cancellation | Run the same cleanup through a disconnected workflow context, then return the cancellation error |
| Parent fails, times out or is terminated | Temporal applies `REQUEST_CANCEL` to its children even when parent cleanup did not run |

Keep the existing `PARENT_CLOSE_POLICY_REQUEST_CANCEL`, not `ABANDON` or `TERMINATE`. It permits child cleanup without deliberately orphaning work. A close request propagates recursively through the same child workflow code. Set `WaitForCancellation` on new child starts so a cancellation request is not treated as observed child completion. Use a bounded graceful drain, initially 30 seconds, for child completion and tool/update handlers before the parent returns. Handler completion alone does not prove child completion. Observe terminal child events through cancellation-independent cleanup context and verify the cancellation ordering with the pinned SDK in the acceptance scenarios.

Cancellation is cooperative. The 30 second drain is not a guarantee that every descendant is already closed. If a child worker is unavailable, navi shows cancelling/closing where known, or unknown, until Temporal reports a terminal execution. The 24 hour execution timeout is the final existing bound. Do not add force-terminate buttons or promise synchronous tree teardown in phase one.

If a child workflow completes with `Report.Error`, show task failure even if its Temporal execution status is Completed. Preserve stopped, failed, cancelled and timed-out outcomes rather than labelling every `done` child successful. Retain the full transcript in the child's existing session output. Keep parent result summaries bounded rather than copying transcripts into parent history.

### Continue-as-new and history pressure

Keep the current quiescent-parent approach. Temporal does not transfer live child futures to a parent's new run. With `REQUEST_CANCEL`, closing the parent run to continue as new is not a way to keep those children attached. Do not change to `ABANDON` and rebuild attachment with a custom signal/result tracker.

A parent can continue as new only after every native child has reached a terminal result, its turn has ended and all tool/update handlers have finished. Carry request parent/depth, completed child records, results, pending messages, escalation references and session state as today. Drain received signals before the transition. Always address parent guidance and escalation notices by Workflow ID with an empty Run ID.

A child can itself continue as new when it has no live children. Temporal keeps that execution chain behind the parent's original future. Parent identity and task state must survive the child's transition. A root or intermediate parent with live children delays worker-build auto-upgrade until they settle.

When `upgrade.Due` is true with live children, reject new spawns so the tree can drain. Continue accepting messages and guidance, because a parent may need another turn to unblock an existing child's attention request. Once children settle, carry queued messages into the new run. This does not bound history growth from continued conversations or a signal flood. Monitor history growth during the later acceptance run and verify that the remaining history budget can accommodate child completion and close. If that cannot be demonstrated, reduce the task lifetime or require closing the outstanding tree before rollout. Do not claim the 24 hour child timeout alone bounds history size, and do not freeze guidance in a way that prevents children from finishing.

### Escalation and cost

An escalation raised inside a subchat remains attached to that subchat. It inherits project and scope, appears in navi's existing Escalations view, and its answer resumes the subchat, not the root. The parent's child row shows the child as needing attention or escalated. A child with an open escalation does not auto-complete. Handler chats still cannot spawn or escalate to themselves.

Keep the escalation workflow's existing `ABANDON` policy, needed for the chat's own continue-as-new, and explicit withdrawal during chat cleanup. Its existing maximum expiry is 14 days. The child's 24 hour task lifetime wins if a person does not answer. Abrupt timeout or termination can skip withdrawal and leave an escalation until its expiry. This is an existing lifecycle limitation, not a guarantee repaired by subchat cancellation. Do not add a custom escalation sweeper as part of this feature.

Concurrent child turns cost model requests, tokens, cloned workspaces and session capacity. Current version 2 relies on provider throttling and session pools, not a shared model concurrency lease. `chat_await` adds no new model request while its tool call waits, but the harness/session remains open and any provider-side cost depends on that provider. Correct its stale shared-slot wording. Do not restore removed account leases implicitly. The proposed four-child limit is not a project-wide spending cap.

## 4. Replay compatibility

Contract compatibility and workflow replay are separate concerns. Keep `ContractVersion = 2` for additive API fields. Do not rename `selfdev.Chat`, queue `selfdev`, signals, tools, updates or existing protobuf fields. Keep `ChatRequest.parent` and `ChatSummary.parent`, rather than adding redundant `parent_id` fields. Preserve existing field numbers and regenerate Go and TypeScript bindings through the repository's buf workflow.

Gate changes that affect workflow commands or control flow with new `workflow.GetVersion` change IDs, including child completion/stop behavior, close/drain commands and history-pressure admission. Preserve the historical branch for histories without each marker. Add markers at the affected deterministic decision point, not only at workflow entry. Preserve the existing `upgrade-wake`, `stop-keeps-queue`, `turn-resume`, `chat-model-limit` and `escalation-handler` markers and branches.

Do not change the tool set on a session already opened with it. A child continues to run with the app's registered extensions. A plain redeploy is not proof of compatibility, and a contract memo does not certify a compatible worker build. Each app must replay its own recorded histories with its exact `Options`, then use its existing Worker Deployment rollout where configured. Do not assume deployed versioning is on without checking it during the later rollout phase.

## 5. navi API and UI

### Contract and API

Reuse existing parent and durable children fields. Add one API-only projection:

- `ChatView.children`, a repeated `ChatSummary` at unused tag 4, holds direct children with current status and navigation metadata. `ChatState.children` remains the durable task/result record. Do not write enriched API summaries back into workflow input.
- Populate it from the bounded recorded child list, using the same project's client and bounded concurrent reads. Use live query status when available, native execution status for failed/cancelled/timed-out executions, and explicit unknown/unavailable when a worker or history cannot answer. Fall back to the durable title/result without inventing success. No unbounded recursive query is needed.
- Expose the same projection through REST JSON, Connect `GetChat`/`WatchChat` and SSE. Update `View`, `statejson.go`, Connect view mapping and UI API types together. Keep streams updating child rows even when the parent is idle. A closed parent's stream must not freeze a child that is still cancelling.

Keep list responses flat with `ChatSummary.parent` so existing clients can ignore the tree. Extend `ListChats` and its REST equivalent to include ordinary child chats stored as sessions. Remove those children from `ListSessions` so they do not appear twice. Keep owner-controlled issue/review/handler sessions there. Do not change `KindOf` or rewrite historical search attributes just to move rows between pages.

List queries restrict both `WorkflowType = selfdev.Chat` and project, including the existing absent-project representation for the core project. Combine root chats and ordinary subchats, then deduplicate by Workflow ID to the latest execution. Consume visibility pagination rather than assuming the first 200 runs is complete. Fetch missing ancestors by ID when a listed child refers to one. Handle Temporal visibility lag with the parent's recorded child list on its detail page, not a new database index.

Root archive controls remain root controls. Do not expose child Archive/Unarchive in this release. The tree groups descendants beneath the root's archived section without mutating child workflow attributes. Filters can show a matching descendant with its ancestors as context. A retained child whose ancestor has expired remains visible as a standalone row with an unavailable-parent label. Never silently drop it.

Reuse `GetChat`, `SendMessage`, `Stop`, `StopDiscard` and `Close` for a child's ID. A subchat is not exempt from permissions, but it is exempt from the rule that only an issue or handler owner may stop its session. Keep that restriction for non-subchat sessions. Share action eligibility between REST, Connect and A2A instead of retaining three interpretations of `owner`.

Do not add a `parent` parameter to `CreateChat` that makes navi start a second top-level workflow. Model tools, app `Chat.Spawn`, navi UI and A2A spawn all use the parent's native child-start path. Manual spawn calls a parent workflow update that executes `ExecuteChildWorkflow`, never `client.ExecuteWorkflow` from navi. Require project modify access and `mayJoin`, preserve caller identity and restrictions, and return only after native child-start acknowledgement.

### Chat list and chat page

The Chats page renders roots newest first with nested descendants. Siblings have stable start-time/ID ordering. Use ordinary nested lists and disclosure buttons with `aria-expanded`, not an incomplete ARIA tree widget. Preserve keyboard row navigation, deep links, narrow layouts and filtering. Expand ancestors of the selected or matching chat. Show each row's task title, status and attention marker.

The chat page keeps its existing family area, with these changes:

- Parent link targets the actual parent's chat/session route. Show unavailable parent if expired or inaccessible, without leaking its title.
- Each child row shows current status, result or error summary, and an Open action. Ordinary children use `/c/<id>`. Existing `/s/<id>` links remain usable for historical subchats.
- Opening a child gives it the existing transcript and message composer, subject to project modify access and `mayJoin`. A family-row Message action can open and focus that composer. No second messaging implementation is needed.
- Stop turn is available on a running child, with a separate Stop and discard queued action. Close child warns that its own descendants are also asked to close. Parent Stop is labelled as stopping only the parent's turn.
- Viewers can open and read the whole authorized family. They do not see mutation actions. Unknown status disables actions whose eligibility cannot be established and offers refresh rather than pretending the task is complete.

The existing Escalations list stays separate. Link a child's escalation back to the child, with parent navigation available there. Do not render escalation workflows as chat-tree children merely because they share `SelfdevParent`.

## 6. A2A and access control

Keep the existing A2A mapping: one agent per project, `contextId` is a chat ID and a task is a message turn. A subchat is its own context. It is not a second root task pretending to be part of the parent's transcript.

- Include ordinary subchats in the same project-scoped chat listing used by `ListTasks`. Existing get, stream, resubscribe and send operations work with the child's context/task ID. Preserve task metadata when emitting initial and status events so streaming clients can discover the family too.
- Add a documented navi metadata object on returned tasks containing `parentChatId` and `childChatIds`. Resolve it from the same workflow state. Advertise its stable extension URI through the agent card's `Capabilities.Extensions`, with `Required` false, without claiming A2A defines a native task tree. The pinned a2a-go version has both task metadata and card extensions.
- `CancelTask` on a child stops that turn through the shared chat action check. It does not close the parent or silently cancel descendants. A2A task cancellation is still not chat closure.
- A message with no context ID continues to start a root. Expose direct spawn through a documented A2A extension targeting a parent context and invoking its native spawn update. Require project modify access and the same scope checks as UI spawning. Include the spawn capability in the agent card extension.

All routes resolve the caller from the existing trusted tailnet identity and project policy. View, modify and admin rules stay unchanged. No new global permission, per-child ACL or privileged credential is introduced. Admin-only backend selection stays admin-only. The effective scope and `Restricted` value can only stay equal or narrow on spawn.

Verify the requested chat belongs to the selected project before reading or mutating it, even where projects currently have distinct namespaces. Apply this to direct IDs, parent links, child projections, visibility results and A2A lookups. The core project's empty request project is a deliberate legacy representation, not a wildcard. A wrong-project or invisible relation returns not found. `owner` and a guessed child ID do not confer access.

Keep `mayJoin` for messages so a project member cannot borrow a more privileged chat's scope. Guidance from a person keeps their identity and restriction flag. Parent model guidance remains limited to its recorded children and inherited authority. Audit stop, close and message with project, child ID and caller through the existing audit path, without message contents.

Preserve the not-set-up check. `requireChat` uses cached task-queue pollers for root starts and user messages, including messages to children. Keep its existing `chat_not_set_up` HTTP/Connect/A2A behavior and fail-open behavior on a describe error. Reading a family must still work without an active chat worker where Temporal data permits. Closing or stopping must not require a poller before submitting the command. A model spawn runs on the project's worker and native queue, not through navi or a fallback core worker.

## 7. PR sequence and validation gates

This document is PR 0. User decisions were confirmed on 2026-11-20 at 11:23Z. Merge this design after CI, then implement SDK, core navi and the tenants in order. Merge each implementation after CI and verify normal pin commits and rollouts before live acceptance.

| PR | Repository | Scope and acceptance |
| --- | --- | --- |
| 1 | protocol7 | Additive `ChatView.children` contract and shared subchat classification. Generate Go/TS bindings. buf lint, generation check and breaking check against master. No workflow behavior change |
| 2 | protocol7 | Versioned lifecycle fixes, child-start failure accounting, stop/attention semantics, descendant-aware completion, close drain and continue-as-new admission. Keep native child futures, existing tool names and bounds. Correct model-slot wording. Focused SDK regression coverage and Temporal server scenarios below |
| 3 | protocol7-selfdev | Bump the merged SDK revision for navi and core chat. Implement project-checked family projection, combined list, action checks and A2A metadata/cancellation. Update React tree and child page controls, REST/Connect serialization and app instructions where needed. Core worker replay plus API and browser acceptance. No `kubernetes/` or `cdk8s` edits |
| 4 | oku-review | Bump the merged SDK revision. Keep `pkg/chat/Register` and its own extensions and image. Replay its histories and run a parent/child/grandchild scenario on its worker |
| 5 | oku-selfdev | Same dependency, replay and lifecycle acceptance for its own worker. Recheck the active audit PR before editing shared dependency files |
| Release verification | Platform and tenant release owners | Follow normal pin commits and rollouts in core, oku-review, oku-selfdev order. Confirm chat pods roll and recorded chats still replay. No manifest migration is part of the feature |

Before publication of each PR, fetch its default branch and inspect open PRs again. Coordinate dependency changes rather than overwriting another owner's pins. A server supporting the additive response may precede the UI. A UI talking to an older server falls back to `state.children` and reports unavailable live status. Existing subchats require no data migration or historical kind rewrite.

Required implementation checks, not yet run:

1. A real Temporal parent starts two children and a grandchild. Assert native parent execution identity, same namespace/project/scope, separate sessions/branches, inherited extensions, limit rejection and failed-start accounting.
2. The parent continues work after spawn and automatically wakes on results, attention, escalations and failure, without requiring `chat_await`. It can also explicitly await, guide a child, and observe timeout/failure/stopped results without duplicate child execution. Worker restart replays the in-flight tree.
3. A stopped child remains available for guidance when no queued turn follows. Root Stop leaves its children running. Child Close and root Close request recursive closure. Test cancellation, parent failure/termination and an unavailable child worker. Observe actual terminal events, not only signal acceptance.
4. Parent continue-as-new is deferred while children are live. A child continues as new and completes through the original parent future. A parent then continues as new with completed results and pending messages intact. Exercise a build-target change and history-pressure admission.
5. A subchat escalates, receives the answer in that subchat, and completes only afterward. Closing it withdraws the escalation on the graceful path. Document the abrupt-termination/expiry limitation.
6. Replay recorded root, child, handler and escalation histories from each app against the exact app options. Cover histories before and after each new version marker. Worker Versioning is a deployment check, not a substitute for replay checks.
7. REST, Connect/SSE and A2A expose the same child relation and outcome. Browser acceptance covers the nested list, parent link, message, stop, close, archive grouping, visibility lag, pagination, expired ancestors and a closed parent with a cancelling child.
8. Viewer, modifier and admin checks cover direct child IDs, same-namespace wrong-project IDs, restricted scopes, session-stop restrictions and the no-worker response across all transports. No subchat path falls back to the core namespace.

Keep tests focused on this repository's behavior and recorded real histories. Use the real Temporal test server for child lifecycle boundaries. Do not add tests that only restate prompt text, protobuf field names or Temporal's own guarantees. Builds and live release verification belong to the later implementation/release phase.

Rollback of navi can remove the tree UI while retaining existing child workflows and wire fields. Workflow rollback is more constrained: do not send post-change histories to an older worker that lacks their versioned branches. Retain compatible worker builds until their pinned workflows drain. Do not delete version markers or registered workflow types as a rollback shortcut.

## 8. Decided

Confirmed by the user on 2026-11-20 at 11:23Z:

| Decision | Behavior |
| --- | --- |
| Parent wake-up | Automatically start a parent turn on a child result, escalation, failure or stop waiting for guidance. Keep `chat_await` as an optional explicit wait, not a dependency |
| Bounds and cost | Keep 4 live and 16 lifetime direct children, depth 2 and 24 hour child lifetime |
| User control of a subchat | Project modifiers may message, stop and close children. A stopped child stays open for guidance |
| Spawn entry points | Model tool, app `Chat.Spawn`, navi UI and A2A. Manual paths invoke a parent workflow update using the same native child-start implementation |

Keep cooperative `REQUEST_CANCEL` rather than immediate termination and the existing abandoned-escalation expiry limitation. Verify lifecycle behavior before completing the release.

## 9. Temporal references

These explain platform behavior, not verification of the deployed system:

- [Child workflows](https://docs.temporal.io/child-workflows): same-namespace execution, parent and child continue-as-new behavior.
- [Go child workflow guide](https://docs.temporal.io/develop/go/workflows/child-workflows): native start acknowledgement, result futures and child options.
- [Parent Close Policy](https://docs.temporal.io/parent-close-policy): request-cancel, terminate and abandon semantics.
- [Go continue-as-new](https://docs.temporal.io/develop/go/workflows/continue-as-new): new run in the same Workflow ID chain and fresh history.
