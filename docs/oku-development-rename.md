# oku-development rename inventory

Checked on 2026-11-21 against protocol7 2079c92, protocol7-selfdev fe5ff3c, oku-selfdev 22affc0, oku-review dfd930b and baremetal 4cb960d. These repositories had no open pull requests at inventory time.

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

## Acceptance

Check all Flux Kustomizations and tenant workloads Ready, new zot image pulls, a real pin-only PR built and auto-merged with the new repository and package names, pre-existing oku chat history replay, navi project display and health, webhook bus delivery for the renamed repository, and the baremetal admission matrix.
