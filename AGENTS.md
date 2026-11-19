# Agent notes

- Read docs/plan.md first, in particular "Repository layout".
- **Settings go in config, not flags.** A command's settings come from a YAML file read by `--config` (`pkg/support/config`): each key is a flag's long name and a repeatable flag takes a list. A deployment mounts that file from a ConfigMap and keeps container args to the command and `--config`. A new setting is a field with a flag name in the command's kong struct, which makes it settable from the file. Secrets stay in files or PDP, never in the settings.
- This repository is the SDK that each app builds its own worker from. Workflows, activities, tools and support code that more than one app needs belong here, so that an app (protocol7-selfdev, oku-selfdev, oku-review) upgrades by bumping its protocol7 version and does not run another app's images.
- Packages are grouped under `pkg/` by area (`domain`, `execution`, `orchestration`, `p7`, `support`, `transport`). Put a new package in the group it belongs to.
