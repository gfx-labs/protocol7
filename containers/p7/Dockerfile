# syntax=docker/dockerfile:1
#
# The protocol7 commands: p7, p7-worker and p7acpshim, with the default
# harness (towa) and git, which the session manager runs to place clones.
#
# A session's agent runs inside the core worker's container. This image holds
# no toolchain for it. A consumer whose agent needs one builds its own image
# and copies the commands in /usr/local/bin from this one at a pinned commit,
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
ARG TOWA_VERSION=0.1.2
ARG TOWA_SHA256=1c2a239c12b43ac9190094a1d42f18b699bbcbfe7483100f0b2149245a058940
RUN set -eu; mkdir /out; cd /tmp; \
    curl -fsSL -o towa.tgz "https://github.com/gfx-labs/towa/releases/download/v${TOWA_VERSION}/towa_${TOWA_VERSION}_linux_amd64.tar.gz"; \
    echo "${TOWA_SHA256}  towa.tgz" | sha256sum -c -; \
    tar -xzf towa.tgz; install -m 0755 "towa_${TOWA_VERSION}_linux_amd64/towa" /out/towa

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
