# PR-owned image pins

The PR maker pushes its branch, requests `build.Build` for that head, waits for
the result, and commits the marked manifest pins in its own branch. The builder
only reads GitHub and pushes images to zot. It never commits pins.

`Image.Reference(registry)` takes the node-reachable registry from the PR maker's
configuration. This differs from the builder's internal push address,
`zot.registry.svc:5000`. `PinFiles(files, registry, images)` returns changed
repository-relative files under `kubernetes/`. It updates only digest references
whose `# p7-image: <name>` marker and package match a returned image. It accepts
`image:` and named settings such as `emulator-image:`. Missing image markers,
wrong packages and tag-only references fail rather than silently skipping pins.
Unmarked images, including infrastructure images on ghcr, are unchanged.

## Current cluster endpoint

The configured pull address is `10.10.20.21:30500`. This is oku-wk-01's node IP,
not a VIP. On 2026-11-20, zot's Service was NodePort 30500 with
`externalTrafficPolicy: Cluster`, so the NodePort routes through any live worker
to zot, currently on oku-wk-03. Worker addresses .21 through .24 have registry
TLS configured. There is no zot LoadBalancer VIP. The ingress VIP
`10.10.20.100` is assigned to Traefik and is not a registry endpoint.

The chosen reference therefore depends on oku-wk-01 being reachable, even if zot
itself is healthy elsewhere. Choosing another worker in configuration removes
that particular dependency only after the PR maker re-pins references. A stable
registry VIP or hostname needs platform routing, a matching server certificate
and node RegistryTLSConfig before it can replace this endpoint. No Talos or
registry routing change is part of this SDK change.

Existing important images continue to be built on ghcr through GitHub Actions.
There is no ghcr-to-zot mirror and no fallback registry rewrite.
