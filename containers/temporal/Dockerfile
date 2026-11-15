# syntax=docker/dockerfile:1
FROM golang:1.27.3-trixie AS build
WORKDIR /src
COPY temporal/go.mod temporal/go.sum ./
COPY temporal/cmd cmd
COPY temporal/pkg pkg
RUN --mount=type=cache,target=/root/.cache/go-build --mount=type=cache,target=/go/pkg/mod \
    CGO_ENABLED=0 go build -trimpath -o /out/p7-temporal ./cmd/p7-temporal

FROM temporalio/server:1.32.2
LABEL org.opencontainers.image.source=https://github.com/gfx-labs/protocol7
COPY --from=build /out/p7-temporal /usr/local/bin/temporal-server
COPY --from=build /out/p7-temporal /usr/local/bin/p7-temporal
