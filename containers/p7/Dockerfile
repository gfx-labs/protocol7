# syntax=docker/dockerfile:1
#
# The protocol7 commands: p7, p7-worker and p7acpshim, with the default
# harness (towa) and git, which the session manager runs to place clones.
#
# A session's agent runs inside the core worker's container. This image holds
# no toolchain for it. A consumer whose agent needs one builds its own image,
# as protocol7-selfdev does.
#
#   scripts/build-image

FROM golang:1.27.3-trixie AS build
WORKDIR /src
COPY go.mod go.sum ./
COPY cmd cmd
COPY pkg pkg
RUN --mount=type=cache,target=/root/.cache/go-build --mount=type=cache,target=/go/pkg/mod \
    CGO_ENABLED=0 go build -trimpath -o /out/ ./cmd/...

FROM golang:1.27.3-trixie AS tools
COPY containers/p7/install-towa /install-towa
RUN sh /install-towa /out

FROM debian:trixie-slim
LABEL org.opencontainers.image.source=https://github.com/gfx-labs/protocol7
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates git tini && rm -rf /var/lib/apt/lists/*
# Sessions start detached shims that outlive the call that started them. tini
# is the container's first process so that it collects them when they exit.
RUN useradd --uid 1000 --user-group --create-home --shell /bin/bash p7
COPY --from=tools /out/towa /usr/local/bin/towa
COPY --from=build /out/p7 /out/p7-worker /out/p7acpshim /usr/local/bin/
USER 1000:1000
WORKDIR /home/p7
ENTRYPOINT ["/usr/bin/tini", "--"]
CMD ["p7-worker", "--help"]
