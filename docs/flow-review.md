# Plan review against the first four flows

Written 2026-11-12 against `docs/plan.md`. Nothing here is built. Facts about PDP come from `gfx-labs/pdp` `origin/main` at `30cd4b6`, about acphost from its README at `v0.24.1`, about the cluster from a read-only look at `talos-metal`, and about the current webhook path from `elee1766/jobcity` `docs/ops/ingest.md` at `81da411`.

## The flows

| Flow | Trigger | Shape in protocol7 | Outside writes |
|---|---|---|---|
| 1. Stale Linear issues | Temporal Schedule | A sweep workflow lists old issues through an activity, then one short child per issue (or per batch) asks an agent for a typed verdict: keep, update, retriage or close | Linear comment, state, label and assignee changes |
| 2. Linear issue to merged PR | Delegation in Linear, or a deliberate start | One long-lived ticket workflow per issue. Check criteria, claim, implement in an environment, open the PR, run an internal review, then loop on changes until merged or abandoned | Linear state and comments, git push, PR create, review requests, thread replies and resolution, branch update from base, merge |
| 3. Requested GitHub review | Review request on a PR | One ticket per PR and review cycle. Review the exact head, publish one review, follow replies, re-review a new head | PR review, comments, thread replies |
| 4. opencodereview on a PR | PR opened or updated | A `p7.Step` that runs the tool on the head and returns a typed result, then a publish activity | PR review comments, approval |

Flow 2 is the one that tests the design. Flows 3 and 4 are its review half. Flow 1 is mostly independent and needs the least.

## Waking on several conditions

The plan now has this in section 7 ("Waiting on several conditions", "Long-lived flow workflows"). The loop for flow 2 would look like this. The p7 calls are proposed and this has not been compiled against an SDK.

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

## Issues likely to block

### 1. PDP does not yet carry the traffic these flows need

PDP today proxies GitHub REST and Linear GraphQL only, with at most those two routes. It rejects streaming, WebSockets, upgrades and non-JSON responses. It has no forward-proxy or TLS-interception mode, no git transport and no model-provider route. Its real GitHub and Linear acceptance runs are still pending, and its documented initial GitHub scope is read and comment in one repository.

The flows need all of the following through PDP, or a stated exception:

| Need | Used by | PDP today |
|---|---|---|
| Model provider calls with streaming | Every agent session, and opencodereview | Not supported |
| git clone and push over HTTPS | Flows 2, 3, 4 | Not supported |
| GitHub GraphQL | Resolving review threads in flow 2. REST has no call for it | Not supported |
| GitHub REST writes: create PR, request review, submit review, merge, update branch | Flows 2, 3, 4 | Route exists. Grants and schema review needed |
| Linear writes beyond comments on allowlisted issues | Flows 1, 2 | Route exists. PDP cannot yet verify that an issue belongs to a team, so it uses an issue allowlist |

Milestone 1 already assumes this work ("prove no upstream token reaches the step"). The plan should say that milestone 1 depends on PDP gaining a proxy or interception mode with streaming, and git. That is the longest dependency in the plan.

### 2. Workload identity on `talos-metal`

PDP authenticates callers by Unix UID or by SPIFFE certificates from a SPIRE Workload API. Its Kubernetes example needs SPIRE server, agents and the SPIFFE CSI driver. None are installed on `talos-metal`. Either SPIRE is installed there (cluster-level, through `gfx-labs/baremetal`), or PDP gains a second authenticator such as a projected service-account token checked with TokenReview. Per-run identity, not just per-namespace identity, is still open in both.

### 3. Milestone order does not match the first deployment

- Milestone 1 builds the container executor on the Docker Engine API. Talos nodes run containerd with no Docker daemon, so that executor cannot run on `talos-metal`. The Kubernetes executor is milestone 7.
- Linear integration is milestone 8. Flows 1 and 2 start from Linear.
- The plan's first delivery target is selfdev. These four flows are a different first target.

### 4. No place for a human to see and answer questions

Flows 1 and 2 need human decisions: a batch of triage proposals, an unclear requirement, a risky merge. The plan has approvals, an attention query and a CLI, and no UI. Jobcity uses Desk for this and its policy says escalations go to Desk only.

### 5. One GitHub identity cannot approve its own pull request

Flow 2 opens PRs. Flows 3 and 4 approve PRs. GitHub does not accept an approval from the PR author. If one App identity does both, flow 4 can never approve a flow 2 PR, and the internal review in flow 2 cannot count as a GitHub approval. Jobcity handles this today by asking a second agent identity for review.

### 6. Running beside jobcity

Jobcity already implements, reviews and merges on the same repositories, from the same Linear delegation and GitHub events. If protocol7 flows run on the same repositories at the same time, both will act. The raw webhook queue also has exactly one pull consumer today, `jobcity-relay`.

### 7. Webhook volume and Temporal retention

Jobcity's ingest doc reports 56,144 relay pulls in one 24-hour window, and you put current volume near 100k a day. The one sampled batch in that doc was 48 GitHub deliveries of `check_run`, `check_suite` and `issue_comment`, so check events are likely a large share, which I have not measured. One workflow per delivery at that rate is fine for throughput and poor for storage: retention is set per Temporal namespace, so with one namespace at 30 days every ingest execution is kept as long as a ticket workflow. Options are to coalesce check events at the receiver before starting anything, or to put ingestion in a second Temporal namespace with a retention of a few days.

### 8. Provider capacity has no owner since resources were removed

Section 8 says model capacity is held per turn and milestone 3 lists "provider capacity". The generic resource layer is gone, so nothing now says how many turns run at once per provider account, who picks the account, or what happens on a rate limit. `docs/acpharness-needs.md` still describes the removed design (provider slots and a headroom picker).

### 9. Agent harness

The plan assumes Jcode. `docs/acpharness-needs.md` says acpharness has only a stub turn and no model. Session restore in a new pod, tool-load events for the journal and placeholder credentials all depend on the harness. A multi-day flow should not depend on restoring a conversation: the branch, the PR and the ticket hold enough to start a fresh session on the next wake.

### 10. Shared cluster exposure

`talos-metal` also runs GitHub Actions runners (`arc-runners`, privileged pod security). Cilium is in default policy mode, so nothing is denied until a policy selects a pod. With human authentication off, an unprotected ticketd admin API or Temporal frontend is reachable from CI jobs. Section 11 now lists the required policies. They are a precondition for the first deployment, not hardening for later.

### 11. Postgres durability

Existing CNPG clusters on `talos-metal` run one instance. Temporal's database holds every running flow. Losing it loses all in-flight work, and protocol7's own database holds records the plan keeps forever.

## Smaller inconsistencies in the plan

- `p7.OnEvent` is still in the SDK table, but section 10 says protocol7 ships no webhook helpers or provider registry and that the consumer's ingestion workflow starts consumer work. With no dispatcher, nothing would call an `OnEvent` registration. Either remove it or define it as a routing table that the consumer's ingestion workflow calls.
- `docs/acpharness-needs.md` refers to `p7-proxy`, decision numbers and plan sections that no longer exist.
- `docs/example-pr-review.md` is marked historical and still shows removed APIs.
- Section 8 pins acphost `v0.24.0`. `v0.24.1` is tagged.
- `p7.Count` followed by `p7.File` is not atomic. It works as a cap only while one intake workflow per repository runs at a time, which the plan should state.

## Questions

1. **First target.** Are these four flows now the first thing to deliver, ahead of selfdev? If yes I would reorder: Kubernetes executor first (drop Docker from the first deployment), then durable work and waits, sessions and journal, flow 3, flow 4, flow 2, flow 1.
2. **Credentials for the first deployment.** Wait for PDP to carry model traffic and git, or allow a stated interim mode on `talos-metal` where a model key and a repository-scoped git token are mounted into pods?
3. **Workload identity.** Install SPIRE on `talos-metal`, or add a service-account-token authenticator to PDP?
4. **Human decisions.** Where do approvals and questions surface at first: `p7` CLI only, jobcity's Desk, or comments on the Linear issue or PR?
5. **GitHub identities.** Will the author and the reviewer be two App identities? Which one runs flow 4's approval?
6. **Coexistence.** Do protocol7 flows start on repositories jobcity does not serve, or replace jobcity flows one at a time on the same repositories? Which flow moves first?
7. **Webhook path.** Reuse the Cloudflare Worker with a second queue that a consumer in the cluster pulls, or expose a receiver through Traefik? Should jobcity's injection screen and approved-author list be reused or rebuilt in the consumer?
8. **Flow 1 authority.** May the triage flow close and retriage issues directly, or does it propose and wait for a human to accept a batch? Roughly how many old issues are in scope?
9. **Flow 2 criteria and claim.** What makes an issue eligible, rules or an agent's judgement, and what does claiming mean in Linear (assignee, state, label)? Does "merge upstream as needed" mean updating the PR branch from its base when it is behind or conflicting?
10. **Flow 2 merge gate.** Which reviews are required: branch protection as configured per repository, or a protocol7 rule on top? May it merge its own PRs once those are met, on every repository?
11. **Flow 4 details.** I am assuming opencodereview is a CLI that runs on a checkout with a rules directory and a model key and prints findings. Is that right, does it have machine-readable output, and is it a separate posted review or one input to the flow 3 review? Does it run on every push or once per review request?
12. **Concurrency.** How many agent sessions should run at once on this cluster, and how many model accounts are there to spread them across?
13. **Postgres.** One instance with backups, or replicas, for Temporal's database and for protocol7's?
14. **Chart consumption.** Render the Helm chart through cdk8s in `gfx-labs/baremetal`, or use a Flux HelmRelease?
15. **`p7.OnEvent`.** Remove it, or keep it as a routing table for the consumer's ingestion workflow?
