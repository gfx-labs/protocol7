# Plan review against the first flows

Written 2026-11-12 against `docs/plan.md` and updated the same day with the decisions below. Nothing here is built.

Sources: `gfx-labs/pdp` local `main` at `1c3e9f8`, acphost tags, a read-only look at the `talos-metal` cluster, `gfx-labs/baremetal` at `948cd40`, and `elee1766/jobcity` at `5c76f7a`.

Correction: the first version of this review read PDP's `origin/main` and described the older two-route gateway. PDP's local `main` is 25 commits ahead of it and is a generic forward proxy. The claims that PDP had no streaming, proxying or model-provider path were wrong and are removed.

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
| Postgres | For development, one CNPG cluster each for Temporal and protocol7, one instance, with backups |
| Chart | Published as an OCI artifact and installed by a Flux `HelmRelease` declared in `gfx-labs/baremetal`'s cdk8s code |
| `p7.OnEvent` | Removed |
| Session limit | 20 open agent sessions across the deployment |
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

- **Nothing has run against a real provider.** Its acceptance notes say no real GitHub or Linear call, no subscription client such as Jcode, no SPIRE and no cluster has been tried. Milestone 1 is the first real run.
- **Presenting the SVID.** Many HTTP clients cannot present a client certificate to a proxy. PDP lists this as open. The plan proposes that the runner holds the SVID and runs a local forwarder in the pod.
- **Run scope in policy.** PDP loads policy data from one file at reload. A grant for one run, such as "this pod may read this repository", needs either a SPIFFE ID that carries the scope or a lookup of run assignments in ticketd.
- **git and model streaming.** PDP speaks HTTP/1.1 only, sends `Accept-Encoding: identity` and refuses a compressed response when it injected a credential. git clone and push and a model stream should be the first things tried through it.
- **Unpushed work.** The 25 commits that make PDP a forward proxy exist only in the local checkout.

### 2. SPIRE on `talos-metal`

Mostly a deployment: SPIRE server, node agents, the SPIFFE CSI driver and spire-controller-manager, added through `gfx-labs/baremetal`. A `ClusterSPIFFEID` can render a separate ID for each run pod from its pod metadata. Not yet checked on Talos: how the agent's workload attestor verifies the kubelet, and the privileged pod-security label its namespace needs. The two PDP items above (presenting the SVID, run scope in policy) are design work, not deployment.

### 3. Model accounts

The limit of 20 sessions is in section 8. Accounts are not settled. A model account is one login or API key at a provider, and every session on it shares its rate limit. jobcity's notes say a login must not be copied between credential stores, because both copies rotate the same refresh token and one of them gets revoked. PDP therefore needs logins of its own for protocol7, or ones that nothing else uses. How many there are decides how far 20 sessions can go before rate limits.

### 4. Maintenance chat reach

You asked for a chat that can change workflows and configure Kubernetes. The plan proposes that it changes both by pull request: workflows through the development loop, and cluster configuration through `gfx-labs/baremetal`, which Flux applies. It reads cluster state through PDP with a read-only credential. It does not get direct write access to the cluster. Reading the Kubernetes API through PDP has not been tried.

### 5. Agent harness

The plan assumes Jcode. `docs/acpharness-needs.md` says acpharness has only a stub turn and no model. Session restore in a new pod, tool-load events for the journal and placeholder credentials all depend on the harness. A flow that lasts days should not depend on restoring a conversation: the branch, the pull request and the ticket hold enough to start a fresh session on the next wake.

### 6. Webhook delivery

GitHub marks a delivery failed after 10 seconds without a 2xx and does not redeliver on its own. `webhookd` therefore needs more than one replica, and the flows need their recheck timer and a scheduled poll. JetStream stream replicas need a three-node NATS cluster.

### 7. Shared cluster exposure

`talos-metal` also runs GitHub Actions runners. Cilium is in default policy mode, so nothing is denied until a policy selects a pod. The API and web UI have no authentication of their own, so the NetworkPolicies in section 11 are a precondition for the first deployment. Anyone who can port-forward into the control namespace can act as any name.

### 8. Postgres backups

No CNPG cluster on `talos-metal` has a backup today, although the Barman Cloud plugin is installed. The two new clusters need a bucket and credentials. SeaweedFS on the same cluster is a poor target because its own metadata database there has no backup. With one instance each, a lost volume means restoring from backup, and Temporal is down until then.

## Smaller inconsistencies in the plan

Fixed: `p7.OnEvent` is removed from the SDK table, and the acphost pin now says to take the current release (`v0.25.0` is the latest tag).

Remaining:

- `docs/acpharness-needs.md` refers to `p7-proxy`, decision numbers and plan sections that no longer exist.
- `p7.Count` followed by `p7.File` is not atomic. It works as a cap only while one intake runs per repository at a time.

## Questions

1. **Model accounts.** Which provider logins will protocol7 have, and how many? They need to be separate from the ones jobcity and you use.
2. **Backup bucket.** Where should the two Postgres backups go? An off-cluster bucket is safer than SeaweedFS on the same cluster.
3. **Maintenance chat.** Is changing Kubernetes by pull request to `gfx-labs/baremetal` enough, or do you want the chat to apply changes to the cluster directly?
