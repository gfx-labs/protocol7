# oku-development rename inventory

Checked on 2026-11-21 against protocol7 2079c92, protocol7-selfdev fe5ff3c, oku-selfdev 22affc0, oku-review dfd930b and baremetal 4cb960d. Platform PR #68 was already open and was left unchanged. The other repositories had no open pull requests at inventory time.

## Rename in place

- GitHub repository oku-trade/oku-selfdev becomes oku-trade/oku-development. Numeric repository ID 1461001424, installation membership, deploy key and webhook subscriptions remain attached to the repository.
- Application module, internal imports, cmd/oku-selfdev command directory, kong command name, runtime binary, log prefix and OKU_SELFDEV_ENABLED environment name become oku-development or OKU_DEVELOPMENT_ENABLED.
- Application Dockerfile, images/Dockerfile and containers/worker/Dockerfile source labels, .github/workflows/ci.yml app and worker publication/cache references use the new GitHub and GHCR names.
- App images in kubernetes/chat.yaml, kubernetes/p7-worker.yaml and the renamed kubernetes/oku-development.yaml move to zot path oku-trade/oku-development after that package contains valid images. Stack image packages oku-worker-trade, oku-worker-retail and oku-worker-backend do not change.
- Platform kubernetes/cdk8s/src/oku.ts repository list, deployRepository URL, images.packages and builds.repository/images use the new path. Inputs remain cmd, pkg, go.mod, go.sum, Dockerfile, .dockerignore and containers.
- Platform okuReview.ts excludes oku-development, not oku-selfdev, from the reviewer repository list. Numeric GitHub repository IDs do not change.
- Flux p7-oku/deploy GitRepository URL changes in place. Its Kustomization, decrypt key, local service account and path stay unchanged.
- Baremetal kubernetes/cdk8s/src/charts/platform-shared.ts changes the zot package ACL. The generated zot unit and zot workload roll in place. Allow both names during the migration, then remove the old package ACL.
- The builder ConfigMap and Deployment roll with the new repository and image package names.
- The webhook bus updates the existing hooks-oku-build durable filter from hooks.github.oku-trade.oku-selfdev.push to hooks.github.oku-trade.oku-development.push. Builder identity is unchanged.
- Navi keeps project ID oku. Add an optional display_name and show oku-development. Update the configured repository URL and clone directory name.
- Protocol7 AGENTS.md, docs/sdk-split.md and docs/subchats.md update application and repository references.
- Platform kubernetes/README.md and docs/image-builder.md update application and repository references. Generated selfdev, tenant-oku and webhooks manifests change through synthesis.
- Oku application README.md, docs/roles.md and docs/audit-readonly.md update command, repository and workload names. There is no application AGENTS.md at the checked head.
- Oku-review AGENTS.md, README.md, docs/review-parity.md and cross-app explanatory Go comments update application names.

## Recreate under new resource names

- p7-oku/oku-selfdev Deployment becomes oku-development, including immutable app selector and matching pod labels.
- p7-oku/oku-selfdev ServiceAccount becomes oku-development. Its SPIFFE identity becomes spiffe://metal.gfxlabs.cloud/ns/p7-oku/sa/oku-development.
- p7-oku/oku-selfdev-config ConfigMap becomes oku-development-config. Config mounts use /etc/oku-development.
- The existing deploy Kustomization has prune:false. After the replacement Deployment is Ready, explicitly stop and remove the old Deployment, then remove its unused ServiceAccount and ConfigMap. Do not run two active role-scanner workers unnecessarily.

## Retain stable identifiers and data

- Kubernetes namespace p7-oku and builder namespace p7-oku-build remain. Namespace rename requires new objects, secret redistribution, network and identity changes, and PVC migration. Existing PVCs nats-data and state-{p7-worker,oku-worker-trade,oku-worker-retail,oku-worker-backend,oku-worker-audit}-0 remain untouched.
- Temporal namespace oku and task queues remain. Renaming would require new namespace provisioning and migration of running chat and role workflows. It is not a repository rename.
- Navi project ID oku remains because ACL maps, chat requests, A2A paths and storage use it as a key.
- PDP bindings github-oku and linear-oku stay. The GitHub binding uses stable numeric repository IDs. Namespace-prefix workload grants already authorize the replacement ServiceAccount. The dedicated readonly audit binding remains unchanged.
- ClusterSPIFFEIDs p7-oku-workload-main and p7-oku-workload-pdp-forward already derive the replacement ServiceAccount identity. No new broad grant is needed.
- ClusterSPIFFEIDs p7-oku-selfdev and p7-oku-selfdev-pdp-forward, plus NetworkPolicy p7-oku-selfdev-egress, name the generic selfdev chat component. They also exist with the same component suffix in oku-review. They are not references to the former application name and remain unchanged.
- Baremetal VAPs, network policy and admission matrix use stable namespaces and generic workload labels. Their source contains no old application name requiring a rename. Run the matrix after the rollout.
- SOPS recipient keys, encrypted secret data, tenant deploy key secret oku-deploy-key, pull-secret ghcr-pull and namespace decryption key sops-age remain. Their ownership is tied to the retained namespace and repository identity, not the repository display name.

## Rollout notes

- GitHub was renamed in place on 2026-11-21. The numeric repository ID remains 1461001424.
- Application PR #43, platform PR #178, reviewer PR #50 and SDK PR #67 changed the source references. Baremetal PR #58 temporarily admitted both registry packages.
- The pin-only gate intentionally refuses a package change, including when the proposed head already has new-package pins but the base does not. The first live PR-maker attempt confirmed that refusal before opening a PR.
- The builder identity copied the currently deployed app and worker manifests into the new registry package and verified their digests unchanged. Application PR #44 explicitly migrated only the package paths. This established a valid base for subsequent trusted pin-only refreshes without weakening the gate.
- Baremetal PR #60 removed the old package permission after new-path node pulls succeeded. No registry bytes or stateful data were deleted.
- Platform PR #180 carries the optional display label through the CLI project-file loader. The stable project and Temporal namespace IDs stay oku.

## Acceptance

- Application PR #45 was created and auto-merged by the tenant PR maker on 2026-11-21 at 10:22:43 UTC. Its source change was one empty commit. The builder added only four immutable digest pins in the three app manifests. Exact-head CI passed for 9bb5eaf54453289ae5b4955ccb3f647060d492d9 before merge 92fda42acc21b2c3629677601847e5502e2fa166. No human merge command was used for this PR.
- The webhook durable hooks-oku-build filters hooks.github.oku-trade.oku-development.push. Read-only SPIFFE-authenticated inspection showed delivery and acknowledgement of stream sequence 219704 at 10:20:53 UTC, with no pending or unacknowledged events.
- Flux p7-oku/deploy fetched merge 92fda42acc21b2c3629677601847e5502e2fa166 from the renamed GitHub URL. The new Deployment, ServiceAccount and ConfigMap exist and the three old application resources were removed after replacement readiness.
- The node pulled app digest sha256:37562a64474dbcc45d9797d18238ba4d67d83f64fbfb332750baffcefbc1d1ac and worker digest sha256:ed6a3cd6600e47d01802d5ab513a58b8625c26246a66120ad02f63e337301b1c from oku-trade/oku-development. The chat, implementer and session worker rolled Ready.
- A state query of pre-existing workflow chat-01m7xxyyt5fqrt62n9kfhgzr8k succeeded on the replacement chat worker at 10:24 UTC. Its retained project and Temporal namespace are oku.
- At 10:28 UTC all 33 Flux Kustomizations and every desired implementer, reviewer and navi workload were Ready.
- The full admission matrix was attempted but stopped before executing cases at scripts/check-protocol7-admission.cjs:263. Concurrent reviewer capture work removed emulator-pod-template from the live reviewer-b-config, while the matrix still expects that old template. The capture owner must migrate the matrix fixture and matching policies before this check can pass. No admission expectation was skipped or weakened for the rename.

- Navi /api/me reports stable project ID oku with display_name oku-development and the renamed repository. Its /api/projects/oku/health endpoint reports Temporal and worker health OK after the replacement image rolled Ready.
