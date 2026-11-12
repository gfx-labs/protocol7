# Plan review against the first flows

Written 2026-11-12 against `docs/plan.md` and updated the same day with the decisions below. Nothing here is built.

Sources: `gfx-labs/pdp` at `aeaca30`, `gfx-labs/acpharness` at `4e3e959`, acphost tags, a read-only look at the `talos-metal` cluster, `gfx-labs/baremetal` at `948cd40`, and `elee1766/jobcity` at `5c76f7a`.

Correction: the first version of this review read PDP's `origin/main` and described the older two-route gateway. PDP's local `main` was then 25 commits ahead of it and is a generic forward proxy. Those commits are now on `origin/main`. The claims that PDP had no streaming, proxying or model-provider path were wrong and are removed.

## Decisions

| Topic | Decision |
|---|---|
| Order | Selfdev first. Selfdev then builds the consumer flows |
| First iteration | Not complete until the container and Kubernetes executors both work |
| First consumer flow | Requested pull request review. Linear triage, the implementation flow and opencodereview are deferred |
| Review eligibility | A pull request in a configured repository, not a draft, whose author has write access, with our review requested by a ping from someone with write access or by a label |
| Re-review | A push after a review starts a review of the new head once pushes go quiet. No new ping is needed |
| Verdict | Real approvals and change requests that count toward branch protection, from the start |
| Credentials | Wait for PDP. No interim mode with keys mounted in pods. protocol7 adds the Rego and policies it needs, and PDP gains features as they are needed |
| Workload identity | Install SPIRE on `talos-metal` |
| Human decisions | A basic generic web UI in protocol7 with two kinds of chat: one attached to a ticket, and a new chat for maintenance |
| UI access | No authentication of its own. Reached with `kubectl port-forward` for now. Tailscale or Google sign-in in front is later |
| Actor identity | A name the user gives, recorded as unverified, until an authenticating proxy supplies a verified one |
| Postgres | One CNPG cluster each for Temporal and protocol7, with a primary and one replica for failover. No backups, because this deployment is for testing |
| Chart | Published as an OCI artifact and installed by a Flux `HelmRelease` declared in `gfx-labs/baremetal`'s cdk8s code |
| `p7.OnEvent` | Removed |
| Session limit | 20 open agent sessions across the deployment |
| Agent harness | `gfx-labs/acpharness` |
| Model logins | One Codex login and four or five Claude accounts, used by protocol7 only |
| Maintenance chat | Changes workflows and cluster configuration by pull request. No direct write access to the cluster |
| GitHub identities | Separate author and reviewer identities |
| Rollout | The reviewer runs as an additional reviewer beside the existing jobcity reviewers, on its own configured subset of repositories. The existing ones are shut down once it is proven |
| Webhooks | protocol7 ships its own receiver, `webhookd`, which publishes to NATS JetStream. Consumers pull from the stream |

## The flows

| Flow | Status | Shape in protocol7 |
|---|---|---|
| Requested GitHub review | First consumer flow, milestone 7 | One ticket per pull request. Review the exact head, publish one review, follow replies, pushes and new requests |
| Linear issue to merged PR | Deferred | One long-lived ticket workflow per issue |
| Stale Linear issues | Deferred | A scheduled sweep with one short child per issue or batch |
| opencodereview on a PR | Deferred | A `p7.Step` on the head, then a publish activity |

## Waking on several conditions

The plan has this in section 7 ("Waiting on several conditions", "Long-lived flow workflows"). The loop below is for the deferred implementation flow, which has the most branches. The review flow uses the same loop with fewer of them. The p7 calls are proposed and this has not been compiled against an SDK.

```go
// FlowState is everything a pull request flow carries across Continue-As-New.
type FlowState struct {
	Issue       p7.RecordRef
	PullRequest p7.RecordRef
	HeadCommit  string
	// Handled is the last revision of each watched record this flow has acted on.
	Handled map[string]int64
}

// FollowPullRequest runs after the pull request is open and ends when it is
// merged or abandoned.
func (w *Workflows) FollowPullRequest(ctx workflow.Context, state FlowState) error {
	for {
		info := workflow.GetInfo(ctx)
		if info.GetContinueAsNewSuggested() || info.GetTargetWorkerDeploymentVersionChanged() {
			options := workflow.ContinueAsNewErrorOptions{
				InitialVersioningBehavior: workflow.ContinueAsNewVersioningBehaviorAutoUpgrade,
			}
			return workflow.NewContinueAsNewErrorWithOptions(ctx, options, w.FollowPullRequest, state)
		}

		waitCtx, cancelWait := workflow.WithCancel(ctx)
		changed := p7.Watch(waitCtx, p7.WatchRequest{
			Records: []p7.RecordRef{state.PullRequest, state.Issue},
			Handled: state.Handled,
			Events:  w.config.WakeEvents,
		})
		recheck := workflow.NewTimer(waitCtx, w.config.RecheckInterval)

		var changes p7.Changes
		var waitErr error
		selector := workflow.NewSelector(ctx)
		selector.AddFuture(changed, func(future workflow.Future) {
			waitErr = future.Get(ctx, &changes)
		})
		selector.AddFuture(recheck, func(future workflow.Future) {
			waitErr = future.Get(ctx, nil)
		})
		selector.Select(ctx)
		cancelWait()
		if waitErr != nil {
			return fmt.Errorf("wait for changes to %s: %w", state.PullRequest.ID, waitErr)
		}

		// Decide from what is true now, not from which event woke the flow.
		observed, err := w.readLiveState(ctx, state)
		if err != nil {
			return fmt.Errorf("read live state of %s: %w", state.PullRequest.ID, err)
		}

		switch {
		case observed.Merged:
			return w.finishIssue(ctx, state, observed)
		case observed.Closed, observed.IssueCancelled:
			return w.abandon(ctx, state, observed)
		case observed.HasConflicts, observed.BehindBase:
			state, err = w.updateFromBase(ctx, state, observed)
		case len(observed.UnansweredFeedback) > 0:
			state, err = w.addressFeedback(ctx, state, observed)
		case observed.ChecksFailed:
			state, err = w.repairChecks(ctx, state, observed)
		case observed.ReadyToMerge:
			state, err = w.merge(ctx, state, observed)
		}
		if err != nil {
			return fmt.Errorf("handle changes to %s: %w", state.PullRequest.ID, err)
		}
		if state.Handled == nil {
			state.Handled = make(map[string]int64, len(changes.Revisions))
		}
		for record, revision := range changes.Revisions {
			state.Handled[record] = revision
		}
	}
}
```

`readLiveState`, `finishIssue`, `abandon`, `updateFromBase`, `addressFeedback`, `repairChecks` and `merge` are consumer methods on `Workflows`. Each runs activities or agent turns and returns the updated state.

Three properties do the work that jobcity's waits do badly today:

- The flow acts on current state. A wake only says "look again", so a lost, duplicated or reordered event cannot put the flow in a wrong branch.
- A change that lands during a long agent turn is still there on the next pass, because the wait compares revisions and does not listen for edges.
- The timer makes a missed webhook cost a delay, not a stuck flow.

## Still open

### 1. PDP

PDP's design now covers what the flows need. What is left, from its own docs:

- **Nothing has run against a real provider.** Its acceptance notes say no real GitHub or Linear call, no subscription client, no SPIRE and no cluster has been tried. Milestone 1 is the first real run.
- **Presenting the SVID.** Many HTTP clients cannot present a client certificate to a proxy. PDP lists this as open. The plan proposes that the runner holds the SVID and runs a local forwarder in the pod.
- **Run scope in policy.** PDP loads policy data from one file at reload. A grant for one run, such as "this pod may read this repository", needs either a SPIFFE ID that carries the scope or a lookup of run assignments in ticketd.
- **git and model streaming.** PDP speaks HTTP/1.1 only, sends `Accept-Encoding: identity` and refuses a compressed response when it injected a credential. git clone and push and a model stream should be the first things tried through it.
- **Refresh for subscription logins.** PDP's `oauth_refresh` sends a form body with a client secret. The Claude login refreshes with a JSON body and a scope, and neither the Claude nor the Codex login has a client secret. PDP needs a refresh adapter for each.
- **Getting a login into PDP.** PDP has no login command and expects a state file that already holds a refresh token. The plan proposes logging in with `acpharness login` and moving the refresh token across.

### 2. SPIRE on `talos-metal`

Mostly a deployment: SPIRE server, node agents, the SPIFFE CSI driver and spire-controller-manager, added through `gfx-labs/baremetal`. A `ClusterSPIFFEID` can render a separate ID for each run pod from its pod metadata. Not yet checked on Talos: how the agent's workload attestor verifies the kubelet, and the privileged pod-security label its namespace needs. The two PDP items above (presenting the SVID, run scope in policy) are design work, not deployment.

### 3. Model accounts

Decided: protocol7 gets one Codex login and four or five Claude logins of its own. acpharness takes an ordered model list and moves to the next entry on a rate limit, so spreading sessions over the Claude accounts needs no protocol7 code. The single Codex login has no second account to fall back to, so every Codex session shares one rate limit. Not measured: how many concurrent sessions those accounts carry before every entry is rate limited. The limit of 20 is a starting number.

### 4. Kubernetes reads for the maintenance chat

Decided: the chat changes workflows and cluster configuration by pull request and has no direct write access. Its read access is open. Reading the Kubernetes API through PDP with a read-only credential has not been tried.

### 5. Agent harness

Decided: acpharness. `docs/acpharness-needs.md` in this repo is out of date. It says only a stub turn exists. At `4e3e959` acpharness has Claude and Codex logins, proxy-held accounts, proxy and CA support in every HTTP client, model fallback, typed stop reasons, a `hosts` command and a session directory that a new process can load. Those cover most of what that file asks for.

Not established: a proxy-held account against a real provider, and moving a session directory between pods. Whether it reports tool-load events for the journal was not checked. It has no release tag, so protocol7 pins a commit. A flow that lasts days should still not depend on restoring a conversation: the branch, the pull request and the ticket hold enough to start a fresh session on the next wake.

### 6. Webhook delivery

GitHub marks a delivery failed after 10 seconds without a 2xx and does not redeliver on its own. `webhookd` therefore needs more than one replica, and the flows need their recheck timer and a scheduled poll. JetStream stream replicas need a three-node NATS cluster.

### 7. Shared cluster exposure

`talos-metal` also runs GitHub Actions runners. Cilium is in default policy mode, so nothing is denied until a policy selects a pod. The API and web UI have no authentication of their own, so the NetworkPolicies in section 11 are a precondition for the first deployment. Anyone who can port-forward into the control namespace can act as any name.

### 8. Postgres has no backups

The cluster did back up to Wasabi, and no longer does. `gitlab-postgres` wrote to the bucket `talos-cnpg-backups` at `s3.us-central-1.wasabisys.com` through the Barman Cloud plugin, with 30 days of retention. GitLab was removed on 2026-09-18 (`gfx-labs/baremetal` `340093d`). A backup of the SeaweedFS database to the same bucket was added and reverted that day (`4ffc6cb`, `ab350ca`). Today no `ObjectStore` or `ScheduledBackup` exists and the three CNPG clusters have no backup.

Decided: the first deployment is for testing and has no backups. Each of the two new clusters runs a primary and one replica, so a lost node or volume fails over. Losing both instances, a bad migration or a mistaken delete loses the data, including every running workflow. Unless synchronous replication is configured, a failover can also lose the most recent commits.

Add backups before the deployment holds work that must not be lost. The plugin is still installed. The `wasabi-s3-creds` secret lived only in the `gitlab` namespace and was deleted with it, so that step needs new keys.

## Smaller inconsistencies in the plan

Fixed: `p7.OnEvent` is removed from the SDK table, and the acphost pin now says to take the current release (`v0.25.0` is the latest tag).

Remaining:

- `docs/acpharness-needs.md` refers to `p7-proxy`, decision numbers and plan sections that no longer exist.
- `p7.Count` followed by `p7.File` is not atomic. It works as a cap only while one intake runs per repository at a time.

## Questions

None open. All earlier questions are answered in the decisions table.
