# Plan review against the first flows

Written 2026-11-12 against `docs/plan.md` and updated the same day with the decisions below. Nothing here is built.

Sources: `gfx-labs/pdp` local `main` at `1c3e9f8`, the acphost README at `v0.24.1`, a read-only look at the `talos-metal` cluster, and `elee1766/jobcity` `docs/ops/ingest.md` at `81da411`.

Correction: the first version of this review read PDP's `origin/main` and described the older two-route gateway. PDP's local `main` is 25 commits ahead of it and is a generic forward proxy. The claims that PDP had no streaming, proxying or model-provider path were wrong and are removed.

## Decisions

| Topic | Decision |
|---|---|
| Order | Selfdev first. Selfdev then builds the consumer flows |
| First iteration | Not complete until the container and Kubernetes executors both work |
| First consumer flow | Requested pull request review. Linear triage, the implementation flow and opencodereview are deferred |
| Review eligibility | A pull request from a collaborator in a configured repository, not a draft, with our review requested by a ping or a label |
| Credentials | Wait for PDP. No interim mode with keys mounted in pods. protocol7 adds the Rego and policies it needs, and PDP gains features as they are needed |
| Workload identity | Install SPIRE on `talos-metal` |
| Human decisions | A basic generic web UI with chat, in protocol7. No authentication of its own during development. People reach it through PDP |
| GitHub identities | Separate author and reviewer identities |
| Rollout | The reviewer runs as an additional reviewer beside the existing jobcity reviewers, on its own configured subset of repositories. The existing ones are shut down once it is proven |
| Webhooks | protocol7 ships its own receiver, `webhookd`, which publishes to NATS JetStream. Consumers pull from the stream |

## The flows

| Flow | Status | Shape in protocol7 |
|---|---|---|
| Requested GitHub review | First consumer flow, milestone 7 | One ticket per pull request head and request. Review the exact head, publish one review, follow replies and new requests |
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

### 3. Provider capacity

Section 8 says model capacity is held per turn and milestone 3 lists "provider capacity". The generic resource layer is gone, so nothing says how many turns run at once per provider account, who picks the account, or what happens on a rate limit. `docs/acpharness-needs.md` still describes the removed design.

### 4. Agent harness

The plan assumes Jcode. `docs/acpharness-needs.md` says acpharness has only a stub turn and no model. Session restore in a new pod, tool-load events for the journal and placeholder credentials all depend on the harness. A flow that lasts days should not depend on restoring a conversation: the branch, the pull request and the ticket hold enough to start a fresh session on the next wake.

### 5. Webhook delivery

GitHub marks a delivery failed after 10 seconds without a 2xx and does not redeliver on its own. `webhookd` therefore needs more than one replica, and the flows need their recheck timer and a scheduled poll. JetStream stream replicas need a three-node NATS cluster.

### 6. Shared cluster exposure

`talos-metal` also runs GitHub Actions runners. Cilium is in default policy mode, so nothing is denied until a policy selects a pod. With the API and web UI holding no authentication of their own, the NetworkPolicies in section 11 are a precondition for the first deployment.

### 7. Postgres durability

Existing CNPG clusters on `talos-metal` run one instance. Temporal's database holds every running flow, and protocol7's holds records the plan keeps forever.

## Smaller inconsistencies in the plan

- `p7.OnEvent` is still in the SDK table. With `webhookd`, consumers pull from a stream, and nothing calls an `OnEvent` registration.
- `docs/acpharness-needs.md` refers to `p7-proxy`, decision numbers and plan sections that no longer exist.
- `docs/example-pr-review.md` is marked historical and still shows removed APIs.
- Section 8 pins acphost `v0.24.0`. `v0.24.1` is tagged.
- `p7.Count` followed by `p7.File` is not atomic. It works as a cap only while one intake runs per repository at a time.

## Questions

1. **Chat.** I took chat to mean the conversation of the session attached to a ticket: you read it live and type into it, and when no session is running your message becomes the answer the workflow passes to the next turn. Or do you want a general operator chat, as Desk has, that can act on any ticket?
2. **Collaborator.** GitHub labels an author as owner, member or collaborator. I would accept all three, meaning anyone with write access. Is that the rule?
3. **Ping.** I took a ping to be a comment that mentions the reviewer. Must the person who pings also be a collaborator? Does a push after a review trigger a new review, or only a new ping or label?
4. **Verdict.** While it runs beside the existing reviewers, does it post approvals and change requests that count toward branch protection, or comments only until it is proven?
5. **UI through PDP.** PDP is a forward proxy for workloads. How does a person's browser reach the UI through it: a proxy setting on your machine, or something in front of the UI? And should PDP pass the person's identity so decisions record who made them?
6. **Postgres.** One instance with backups, or replicas, for Temporal's database and for protocol7's?
7. **Chart.** Render the Helm chart through cdk8s in `gfx-labs/baremetal`, or use a Flux HelmRelease?
8. **`p7.OnEvent`.** Remove it, or redefine it as a handler on the consumer's pull loop?
9. **Capacity.** How many agent sessions should run at once, across how many model accounts?
