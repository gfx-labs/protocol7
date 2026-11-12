# PDP integration

Written 2026-11-12. This is a design. Each section says what was run and what was only read.

Sources:

- `gfx-labs/pdp` at `origin/main` `e6edc6b`. That commit is the one PDP change this design needed: `oauth_refresh` for a login with no client secret. It was merged and pushed on 2026-11-12. `aeaca30` is the commit before it.
- `gfx-labs/towa` at `origin/master` `7bff6dc`. This is the agent harness.

towa was named acpharness until 2026-11-12 (`7ff30fa`). The local checks here were first run on the acpharness build at `53c4a2a` and run again on towa at `7bff6dc`. The rename changed three names this design uses:

| Name | Was | Where this design uses it |
| --- | --- | --- |
| Binary `towa` | `acph` | The login and logout commands in section 2 |
| Placeholder `towa-proxied-<account id>` | `acpharness-proxied-<account id>` | Each binding's `placeholder` in `pdp.yaml` |
| State directory `towa` under `XDG_STATE_HOME` | `acpharness` | The path of `auth.json` in section 2 |

towa has no code that reads the old names. A PDP binding that still has an `acpharness-proxied-` placeholder matches nothing towa sends. Run locally, PDP found no binding and denied every request.

The files in `docs/pdp-integration/` are the proposed PDP configuration, policy, grants and refresh adapters, with eighteen `pdp explain` inputs. The trust domain `p7.example` in `policy.rego` and the inputs is a stand-in. To rerun the checks with a `pdp` built from `main`:

```sh
cd docs/pdp-integration
pdp validate --config pdp.yaml
for input in explain/*.yaml; do pdp explain --config pdp.yaml --input "$input"; done
```

## Answers

| Question | Answer |
| --- | --- |
| Who refreshes the Claude and Codex logins | PDP, by itself. The harness holds a placeholder and never sees or refreshes a token. This needed one PDP change, which is merged. |
| How a login gets into PDP | Log in once with `towa login`, write the refresh token and scopes to the binding's state file, then `towa logout`. No PDP login command is needed. |
| How a run's scope reaches policy | The run pod's SPIFFE ID carries the profile, the repository and the run ID. Policy data holds what each profile may do. Nothing changes in PDP when a run starts. |
| How a run pod presents its SVID | The harness cannot present a client certificate to a proxy. The runner holds the SVID and runs a loopback forwarder that the pod's proxy settings point at. |

## What was run

| Check | Result |
| --- | --- |
| `pdp validate` on `docs/pdp-integration/pdp.yaml` | Valid. It is also valid on a build of `aeaca30`, because `validate` compiles an adapter and does not run it. |
| `pdp explain` on the eighteen inputs | 6 allowed, 12 denied, as intended. See the table in section 3. |
| `towa accounts` and `towa hosts` on a configuration with three proxy-held accounts | All three report "held by a proxy". Hosts are `api.anthropic.com:443` and `chatgpt.com:443`. No token endpoint is listed. `towa login` refuses a proxy-held account. |
| One towa turn through a local PDP. towa had `HTTPS_PROXY` and PDP's CA in `SSL_CERT_FILE`. PDP held a deliberately invalid token, so no real credential was used. The model's context size and output limit were set in towa's configuration, because the model list cannot be read with an invalid token | PDP intercepted the connection, picked the binding from `towa-proxied-claude-1`, allowed it and forwarded the request to `api.anthropic.com`, which answered 401 as expected. towa then fell back to `claude-2`, which the test policy did not grant, and PDP answered 403. The acpharness build gave the same result. |
| The same turn without those two figures in towa's configuration | towa exited at start. It could not read the model list and reported that nothing is known about the model. See the findings. |
| The same turn with PDP's bindings still on `acpharness-proxied-` placeholders | PDP selected no binding and answered 403 to every request. |
| PDP unit test `TestRefreshPublicClient` | Fails on `aeaca30`, passes on `e6edc6b`. `make check` passes on `e6edc6b`. |
| A `pdp` built from `e6edc6b` serving `claude.star` and `codex.star` against a local stand-in for both token endpoints. The stand-in accepts only the request shape each provider's CLI sends and rejects a reused refresh token | Both logins refreshed, rotated and persisted twice. The stand-in received the real access token and no placeholder. For Codex it received the binding's `chatgpt-account-id`, not the wrong value the caller sent. State files stayed `0600` with no recovery marker. A build of `aeaca30` answered 502 without sending a token request. |

Not run: any refresh against the real Claude or Codex token endpoint, a real turn or model list with a real token, SPIRE, the mTLS listener, a cluster, and git through PDP.

## 1. Refresh

PDP owns the refresh. It keeps each login's refresh token in the binding's `state_file`, exchanges it when it has no unexpired access token cached, writes the rotated pair back before using it, and serializes this with a file lock.

PDP before `e6edc6b` could not refresh these two logins. Its `oauth_refresh` required a client secret and sent a form body. Both logins are public clients with no secret. The requests the provider CLIs send, read from `src/auth/oauth.go` in towa:

| | Claude | Codex |
| --- | --- | --- |
| Token endpoint | `https://platform.claude.com/v1/oauth/token` | `https://auth.openai.com/oauth/token` |
| Client ID | `9d1c250a-e61b-44d9-88ed-5944d1962f5e` | `app_EMoamEEZ73f0CkXaXp7hrann` |
| Body | JSON | form |
| Fields | `grant_type`, `refresh_token`, `client_id`, `scope` | `grant_type`, `refresh_token`, `client_id` |

The PDP change (`e6edc6b`) extends the one builtin and adds no provider-specific code:

```
oauth_refresh(endpoint, client_id, client_secret=None, authentication="body", encoding="form", send_scope=False)
```

Without `client_secret` the grant sends only the client ID. `encoding="json"` sends the grant as a JSON object. `send_scope=True` adds the binding's scopes. The other OAuth builtins are unchanged and still require a secret. The adapters are then one line each:

```python
# claude.star
def acquire(request):
    return oauth_refresh("token", parameter("client_id"), encoding="json", send_scope=True)

# codex.star
def acquire(request):
    return oauth_refresh("token", parameter("client_id"))
```

Things to know before the first real refresh:

- **PDP is strict about the token response, and a refusal is sticky.** It requires `token_type` to be `Bearer` and, when the response has a `scope`, requires it to equal the binding's scopes exactly. If either check fails, PDP keeps the new refresh token but marks the state file `recovery_required`, and the binding stops working until an administrator repairs the file. Whether the two real endpoints return `token_type` has not been observed, because towa does not read that field. The Claude scopes are likely right: the five scopes stored for the existing Claude logins on this machine equal the five the refresh asks for. The Codex response is unknown. Do the first real refresh for each provider with a login whose loss is acceptable, and read the state file afterwards.
- **The upstream entry needs the same scopes as its bindings.** PDP asks the adapter for the upstream entry's `scopes`, and a refresh requires them to equal the binding's. `pdp validate` does not catch a mismatch. The first local run failed with `credential_unavailable` for this reason.
- **PDP refreshes after every restart or reload.** The access token cache is in memory and a reload starts a new one. PDP does not reuse the access token in the state file.
- **One PDP replica.** The lock is a file lock on a local filesystem and the cache is per process. PDP needs a writable volume for `/var/lib/pdp`, with one `0700` directory per account. A Secret volume is read-only and cannot hold the state.
- **PDP reaches the token endpoints directly.** Its credential client ignores proxy settings. PDP's own egress must allow both token hosts and both API hosts.

## 2. Moving a login into PDP

Yes, `towa login` is enough. The steps for one account, on an administrator machine:

```sh
# A separate state directory keeps this login apart from personal ones.
export XDG_STATE_HOME="$(mktemp -d)"
towa login claude --account claude-1 --manual      # codex: towa login codex --account codex-1 --device

umask 077
jq --arg id claude-1 '.accounts[$id] | {refresh_token, scopes}' \
  "$XDG_STATE_HOME/towa/auth/auth.json" > state.json

# Codex only. Not a secret. It goes in the binding's headers in pdp.yaml.
jq -r --arg id codex-1 '.accounts[$id].chatgpt_account_id' "$XDG_STATE_HOME/towa/auth/auth.json"

towa logout --account claude-1                      # deletes the local copy, does not revoke the login
```

Then put `state.json` at the binding's `state_file` path, in a `0700` directory, mode `0600`, owned by PDP's user, and delete the temporary directory.

- The `jq` step was run on a made-up `auth.json` of the same shape, and PDP accepted and refreshed from the resulting file. PDP rejects a state file with any other field, so the access token and expiry must not be copied.
- After the handover nothing else may use that login. Two holders of one rotating refresh token revoke each other.
- The Codex account ID belongs in PDP, not in the harness. PDP sets the binding's `chatgpt-account-id` and drops the caller's value. `towa accounts` accepts a proxy-held Codex account with no ID in its configuration. A Codex turn in that form has not been run.
- Getting the file onto the volume in the cluster is a write to the cluster. Proposed: a one-off pod that mounts PDP's volume, with the file copied in by `kubectl cp`, then a PDP restart. Not tried.

## 3. Run scope in policy

Proposed SPIFFE ID for a run pod:

```
spiffe://<trust domain>/p7/profile/<profile>/repo/<owner>/<repo>/run/<run id>
```

- `profile` names a fixed set of permissions, such as `review`. `docs/pdp-integration/grants.yaml` says what each profile may do: which model accounts it may use and whether it may fetch from git.
- `repo` is the one repository this run works on. A run with no repository uses `repo/_/_`, which cannot name a GitHub repository.
- `run` makes the ID unique and puts the run in PDP's audit log as the principal.

`docs/pdp-integration/policy.rego` parses the ID and decides from it and the grants. A run starts with no change to PDP: no data file write, no reload.

The alternative was a lookup of run assignments in ticketd. PDP's Rego has `http.send` removed and loads data from one file at reload, and a reload also empties the credential cache. A lookup would need a new PDP feature. The SPIFFE ID needs none.

What the eighteen `explain` inputs show:

| Request by a `review` run on `gfx-labs/protocol7` | Decision |
| --- | --- |
| Model call with an account its profile grants (Claude, Codex) | allow |
| Model call with another account's placeholder, or with none | deny |
| Model list with an account its profile grants | allow |
| Model list with another account's placeholder | deny |
| Another path on the model host | deny |
| `git fetch` of its own repository, with or without `.git` | allow |
| `git fetch` of another repository | deny |
| `git push` to its own repository, either request | deny |
| Any host with no rule | deny |

A `triage` run with no git grant is denied the fetch. An ID from another trust domain, an unknown profile and a short ID are all denied.

On Kubernetes the ID is rendered by spire-controller-manager from pod metadata that the executor sets. Proposed, not applied to any cluster:

```yaml
apiVersion: spire.spiffe.io/v1alpha1
kind: ClusterSPIFFEID
metadata:
  name: p7-run
spec:
  spiffeIDTemplate: >-
    spiffe://{{ .TrustDomain }}/p7/profile/{{ index .PodMeta.Annotations "p7.gfx.dev/profile" }}/repo/{{ index .PodMeta.Annotations "p7.gfx.dev/repo-owner" }}/{{ index .PodMeta.Annotations "p7.gfx.dev/repo-name" }}/run/{{ index .PodMeta.Labels "p7.gfx.dev/run" }}
  namespaceSelector:
    matchLabels:
      kubernetes.io/metadata.name: p7-runs
  podSelector:
    matchExpressions:
      - {key: p7.gfx.dev/run, operator: Exists}
```

- The repository goes in annotations because a label value is limited to 63 characters and a GitHub repository name can be 100.
- **The ID is only as trustworthy as the pod metadata.** Whoever can create or patch a pod in the run namespace can claim any profile and repository. Only the core worker's service account may do that, and run pods get no service account token and no route to the Kubernetes API.
- The policy compares the repository exactly. The executor must write the owner and name in GitHub's own spelling. A request in another case is denied.
- PDP refuses a caller whose identity is longer than 256 bytes. GitHub allows 39 characters for an owner and 100 for a repository name, which leaves about 80 for the trust domain, profile and run ID together. Keep run IDs short.
- GitHub owner and repository names use only letters, digits, `-`, `_` and `.`, which are the characters a SPIFFE ID path segment allows.
- A profile that needs a fixed set of extra repositories, such as the maintenance chat, would list them in `grants.yaml`. The proposed policy does not have that rule yet.
- Web search is off by default in towa. When it is on, towa uses the provider's own search inside the model call for Claude and Codex, which needs no rule here. Its own search tool contacts a separate host and would need one.
- Not checked: that a pod with a missing annotation gets no ID at all, and SVID issuance on Talos.

For the container executor, SPIRE's Docker workload attestor gives selectors from container labels. Proposed: the executor labels the container with the run ID and registers one entry with the same ID shape, so one policy serves both executors. That needs a SPIRE server and agent on the development host and is not tried.

## 4. Presenting the SVID

PDP's network listener is TLS 1.3 with a required client certificate, and it verifies the certificate again on every request. Its other listeners identify a caller by Unix UID or by source address, and neither identifies a run.

towa has no setting for a client certificate toward a proxy. Neither do git and most other programs a run starts. So the pod needs one process that holds the SVID:

```
harness, git, tools --HTTPS_PROXY=http://127.0.0.1:<port>--> runner forwarder --mTLS with the pod's SVID--> PDP :8443
```

- Proposed: the protocol7 runner does this. It reads the SVID from the SPIFFE Workload API with `go-spiffe`, listens on loopback, and copies each accepted connection to a new TLS connection to PDP, checking PDP's SPIFFE ID. The proxy conversation, including `CONNECT`, passes through unchanged.
- ghostunnel in client mode does the same job as a separate container, and can load its certificate from the Workload API. It is the fallback if the runner should not hold the SVID.
- Every process in the pod that can reach the loopback port acts as the run. That is the intended granularity.
- The pod also needs PDP's interception CA in its trust store. The harness reads `SSL_CERT_FILE`.

The shape was run locally with a plain forwarder in place of the mTLS one: `HTTPS_PROXY` pointed at a loopback port that copied bytes to PDP's Unix socket. The harness and curl both worked through it, including `CONNECT` and interception. The mTLS leg, the runner code and SVID rotation on a long-lived connection are untested.

## Findings from the local runs

- **towa reads the model list when it starts, and exits if it learns nothing.** For each configured model that its built-in table does not have, towa asks the account's provider for its model list: `GET /v1/models` on `api.anthropic.com`, `GET /backend-api/codex/models` on `chatgpt.com`. No Claude model is in the table, so every Claude account on a run's list is asked. The table has the current Codex models, so the Codex list is not read for them. If the list cannot be read and towa's configuration does not give `model_context_window` and `model_max_output_tokens`, the process exits before it serves a session. The acpharness build at `53c4a2a` did not do this, and the policy first committed with this document had no rule for it, so no Claude session would have started. `policy.rego` now allows the model list for the accounts a profile grants. Setting the two figures in configuration also lets a session start, but they apply to every model the table does not have, so the list is the better source.
- **A policy denial looks like a rejected credential to the harness.** PDP answers a denial with 403 and `{"error":"forbidden"}`. The harness classifies any 403 on a model call as stop reason `auth` for that account. A run whose policy does not grant an account on its model list reports an auth failure, not a denial. Grant every account on the list, as the plan already says, and treat `auth` with the message `forbidden` as a possible policy error.
- **One request took 15 seconds, once.** PDP logged 15,181 ms for the first forwarded request and 156 ms for the same request a minute later. Four curl requests took 33 to 161 ms. It was not reproduced. 15 seconds is PDP's default dial timeout, and PDP tries a host's resolved addresses one after another with the full timeout each, so one unreachable address would cost exactly that. This is a guess from the code, not a diagnosis.
- **PDP's strict YAML loader rejects anchors and aliases.** Scope lists have to be written out for each binding and upstream.

## Order of the first real tests

1. One real refresh per provider with a login that can be lost, then read the state file for `recovery_required`.
2. One real towa start and turn per provider through PDP on a Unix socket: the model list, streaming, a turn that outlives the access token, and the fallback from one Claude account to the next.
3. git fetch through PDP with the GitHub App binding. The `github-read` binding in `pdp.yaml` is a static token standing in for it.
4. SPIRE, the `ClusterSPIFFEID` and the runner forwarder on `talos-metal`.
