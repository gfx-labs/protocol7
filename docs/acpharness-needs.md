# What acpharness needs to work well with protocol7

Proposed, 2026-11-11. Nothing here is decided.

Sources: acpharness `docs/design.md` and `docs/notes/2026-11-11-config-telemetry-server.md` at `d166475`, and `plan.md` in this repo. Only step 1 of acpharness is built (ACP on stdio with a stub turn and no model), so everything below is compared against its design, not against running code.

## How it would run here

- The runner starts `acphost serve`, which spawns `acpharness` on stdio. The worker dials acphost over TCP with mutual TLS (D2). acpharness needs no listener of its own in a pod.
- acphost already replays notifications and redelivers unanswered requests when the worker reconnects, so acpharness needs nothing for that.
- The pod holds no real credential. `p7-proxy` adds them (section 9b).
- The agent reports through the `p7` CLI or MCP tools on `run.sock` (D1, D3).

## New asks, not in the acpharness design

| # | Need | Why |
|---|---|---|
| 1 | An account kind whose token the harness never holds or refreshes. It sends a placeholder that names the account, and builds the rest of the request as usual | The proxy owns the token and the refresh. The harness still has to send the request shape each login needs, such as the Claude OAuth billing block and beta headers, because the proxy only swaps the authorization |
| 2 | Every HTTP client uses `http.ProxyFromEnvironment` and the system certificate pool: provider clients, the OpenAI WebSocket transport, remote MCP, webfetch, embeddings, Jev and OTLP. One CI test runs a turn through a test proxy with its own CA | A Go transport built without `Proxy` set ignores `HTTPS_PROXY`. One client that skips the proxy either fails in the pod or needs the DNS fallback |
| 3 | A failed or stopped turn says why in a fixed field: rate limit (with the reset time), auth, provider overload, context full, limit hit, cancelled | protocol7 records a failure class per attempt and treats a rate limit as a deferral, not a failure. Without the field the worker would have to parse error text |
| 4 | Session state that can move to another pod: flushed at the end of each turn, kept under one directory, and loadable with `session/load` in a new process with the same working directory | This closes the "idle pod while captures or CI run" gap. The runner uploads the directory, the pod is deleted, and a new pod continues the conversation |
| 5 | The ACP package (`src/acp`) importable by the protocol7 worker as the client side | The harness pins schema 1.31.0. Its notes show the older SDK misreads the new update types without an error. One package for both sides removes that |
| 6 | Tools from an MCP server named in `session/new` can be put in the prompt directly, not only behind the `mcp` meta-tool | `submit_result` and `ask_human` are used every turn. Hidden behind search, a model may not find them. Until this exists, the agent uses the `p7` CLI from its shell tool |
| 7 | A headless macOS build. Dropped (round 7): iOS captures reach a remote Namespace devbox from a Linux step | iOS evidence captures run on a Mac joined worker. macOS is a v1 non-goal in the design |
| 8 | A command that lists the hosts the resolved config will contact | Builds the run's egress allowlist without guessing |

## Already in the design, and needed here

| Need | In the design as | Built |
|---|---|---|
| Per-session model, effort and limits from `session/new` `_meta` | config layer 7 | no |
| `systemPrompt` append, extra context files and skills directories | system prompt sections, context loading | no, rejected today |
| `mcpServers` on `session/new` | MCP, step 8 | no, rejected today |
| Cancel stops the stream and kills tool processes by process group | limits and interruption | cancel yes, tools no |
| Token usage and cost per turn | `usage_update` and the prompt response | no |
| Trace parent from the request `_meta` | telemetry | yes |
| No permission prompts by default | `auto` mode | no |
| Clean exit on SIGTERM, and on stdin close | shutdown | SIGTERM yes (checked by hand, per its note). Stdin close is not stated |

## One conflict to settle

acpharness has model chains that fall back across accounts (`claude:work`, then `claude:personal`). protocol7 has the worker pick the account for a run, with provider slots and a usage-headroom picker (section 7).

Both cannot own this. Proposed: in a pod the harness runs a chain with one account and reports a rate limit (ask 3). The workflow decides whether to wait or move accounts. Fallback to a different model on the same account stays in the harness.

## Not needed from acpharness

- Its own TCP or unix listener with mutual TLS. acphost does this.
- Hooks as a permission check. D12 puts enforcement in the proxy.
- Many sessions in one process. A pod runs one session.
- The `store` scope of conversation search.

## Order

protocol7 phase 6 needs acpharness step 2 (one API-key provider and the agent loop) plus asks 1 to 3. That is enough for one turn in a pod with no real key inside it. Ask 4 comes with acpharness step 6. Asks 5 to 8 can follow.

## To check

- Whether the Codex OAuth backend needs a header derived from the token, such as an account id. If so the proxy must add it, or the account config must carry it.
- Whether a session resumed on a different account of the same provider keeps its provider-native items (thinking signatures, encrypted reasoning).
