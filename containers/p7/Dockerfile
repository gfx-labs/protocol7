# syntax=docker/dockerfile:1
#
# The protocol7 commands: p7, p7-worker and p7acpshim, with git, which the
# session manager runs to place clones.
#
# It holds no harness and no toolchain for an agent. A consumer whose agent
# needs them builds its own image, with the harness it chooses. protocol7-selfdev
# does, and builds these commands from this repository's source at a pinned
# commit, so it does not wait for this image.
#
#   scripts/build-image

FROM golang:1.27.3-trixie AS build
WORKDIR /src
COPY go.mod go.sum ./
COPY cmd cmd
COPY pkg pkg
RUN --mount=type=cache,target=/root/.cache/go-build --mount=type=cache,target=/go/pkg/mod \
    CGO_ENABLED=0 go build -trimpath -o /out/ ./cmd/...

FROM debian:trixie-slim
LABEL org.opencontainers.image.source=https://github.com/gfx-labs/protocol7
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates git tini && rm -rf /var/lib/apt/lists/*
# Sessions start detached shims that outlive the call that started them. tini
# is the container's first process so that it collects them when they exit.
RUN useradd --uid 1000 --user-group --create-home --shell /bin/bash p7
COPY --from=build /out/p7 /out/p7-worker /out/p7acpshim /usr/local/bin/
USER 1000:1000
WORKDIR /home/p7
ENTRYPOINT ["/usr/bin/tini", "--"]
CMD ["p7-worker", "--help"]
