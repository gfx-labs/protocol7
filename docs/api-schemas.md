# API schemas with Connect

Status: design. Nothing here is built.

## Goal

navi's HTTP API gets one schema that the server, the web UI and other clients are generated from. Today `pkg/web/server.go` registers 47 routes by hand, and `pkg/web/ts/types.d.ts` restates their JSON shapes by hand ("as far as the page reads them"). Nothing checks that the two agree. Tenants' own tools and the `p7` CLI would also get typed clients from the same schema.

## Decision

- Protobuf schemas in protocol7, served and called with connect-go v2 (`connectrpc.com/connect/v2`, v2.0.2) and Connect-ES in the browser.
- The Connect protocol is plain HTTP. Unary calls are `POST /<package>.<Service>/<Method>` with a JSON or binary body, so curl, the tailnet ingress and navi's `Tailscale-User-Login` identity keep working unchanged.
- v2 specifics that matter here: handlers and clients take plain messages (no `connect.Request` wrapper), services register on a `connect.Server` mounted with `connecthttp.Mount`, interceptors are `connect.ServerInterceptor` functions that apply to every service on a server, and `connectinprocess` runs a client against a server with no listener, which is how the handler tests run.

## What it does not cover

- **The Temporal contract** in `pkg/domain/navi` (workflow, signal, query and update payloads between navi and tenant workers). Changing those payloads to protobuf changes their encoding in workflow history, so running chat, issue and release workflows would fail replay. They stay Go structs with JSON encoding and replay tests.
- **A2A and MCP.** Fixed external protocols, served with a2a-go and the MCP SDK.
- **The GitHub and Linear webhooks.** Their payloads are defined upstream.
- **Byte streams and pages**: artifacts, blobs, the board preview (`/chats/{id}/preview/...`) and static files stay plain HTTP handlers on the same mux.

## Layout

```
protocol7/
  proto/navi/v1/*.proto        schemas, one file per service
  buf.yaml, buf.gen.yaml       lint, breaking-change check, generation
  pkg/transport/naviapi/       generated Go: messages and naviv1connect
  web/naviapi/                 generated TypeScript (protoc-gen-es), published as an npm package or vendored
```

Generated code is committed, so building protocol7 or an app needs only Go. CI runs `buf lint`, `buf breaking` against master, and `buf generate` with a clean-diff check.

Package `navi.v1`. Breaking changes go into `navi.v2` side by side.

## Services

Grouped by what the page does, from the current routes:

| Service | Methods (current route) |
|---|---|
| `IdentityService` | `GetMe` (`GET /api/me`), `ListModels` |
| `PolicyService` | `GetPolicy`, `PutPolicy` (admin only) |
| `ChatService` | `ListChats`, `ListArchivedChats`, `ListSessions`, `GetChat`, `CreateChat`, `SendMessage`, `Stop`, `StopDiscard`, `Dequeue`, `Close`, `Archive`, `Unarchive`, `WatchChat` (server stream, replaces the SSE `events` route) |
| `EscalationService` | `ListEscalations`, `GetEscalation`, `Answer`, `Review` |
| `LearningService` | `ListLearnings`, `Decide` |
| `WorkflowService` | `ListWorkflows`, `GetWorkflow`, `Retry`, `Cancel` |
| `ScannerService` | `GetScanner`, `Pause`, `Resume` |
| `GitHubService` | `ListGitHub`, `GetPull`, `ListReviews`, `FileReview` |
| `ProjectService` | `Health`, `ListErrors`, `ListArtifacts` |
| `BoardService` | `GetBoard`, `WatchBoard` (server stream) |

Every project-scoped request carries `string project = 1`. Each method is marked view, modify or admin with a custom method option (`navi.v1.access`). One server interceptor reads the option, resolves the caller from the identity header and checks the access policy. This replaces the `inProject(needView, ...)` wrappers, and a method with no option fails a test, so a new method cannot ship without an access level.

Server streams use the Connect protocol's streaming over HTTP/1.1 and HTTP/2, which works through the tailnet ingress. The ingress timeout for long-lived streams needs checking in the pilot.

## Web UI

The UI is plain scripts today (`tsconfig` `module: None`, one global scope, `<script>` tags in `index.html`). Connect-ES and the generated code are ES modules, so the UI moves to modules: `module: ES2022`, `<script type="module">`, and an import map for `@connectrpc/connect`, `@connectrpc/connect-web` and `@bufbuild/protobuf`, vendored into `static/` so there is no bundler and no CDN. The move is mechanical but touches every file, so it is its own PR before the first service switches.

## Pilot

`ChatService` first, because it is what A2A, the MCP client and the UI all use.

1. protocol7: buf setup, `proto/navi/v1/chat.proto` and `common.proto`, the access option, generated Go and TS, CI checks.
2. protocol7-selfdev: implement `ChatService` in `pkg/web` on a `connect.Server` mounted at the same listener, with the access interceptor. The old `/api/projects/{project}/chats...` routes stay and call the same code.
3. protocol7-selfdev: the UI's module switch, then the chat pages call `ChatService` through the generated client. `types.d.ts` loses the chat types.
4. Tests: one `connectinprocess` test of `ChatService` against the chat workflow on Temporal's test environment, covering access (viewer cannot send, non-member gets nothing), create, send, watch and stop.
5. Measure: binary size, navi memory, and UI load before and after. The project cares about low memory and CPU, so a regression here stops the rollout.

After the pilot, one PR per service, then remove the old routes and `types.d.ts`.

## Clients outside navi

- The `p7 chat` CLI and MCP server keep using A2A for driving chats. Reading lists and state (`p7 chat list`) can use `ChatService` once it exists.
- Tenants and apps import `pkg/transport/naviapi` for a typed Go client.

## Not checked yet

- That the tailnet ingress keeps Connect server streams open as long as SSE today.
- The cost of vendoring the Connect-ES runtime into `static/` (size of the three packages as ES modules).
- Whether `buf` runs in protocol7's CI image without a network fetch of plugins. Pinned local plugins (`protoc-gen-go`, `protoc-gen-connect-go` v2, `protoc-gen-es`) avoid the Buf Schema Registry.
