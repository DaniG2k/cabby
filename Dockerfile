# syntax=docker/dockerfile:1
# Cabby's release image, built from the prebuilt release binaries (glibc 2.28
# baseline) by compat/scripts/release.sh; nothing is compiled here. The build
# context holds:
#   dist/linux-amd64/{cabby,cabby-healthcheck}, dist/linux-arm64/{cabby,cabby-healthcheck},
#   LICENSE, THIRD_PARTY_LICENSES.md
# distroless/cc has glibc, the CA certificates outgoing TLS needs, and a nonroot
# user, but no shell or curl: the health check is its own binary. The digest is
# the multi-arch index of gcr.io/distroless/cc-debian12:nonroot (uploaded
# 2026-08-21). To look it up again:
#   curl -sI -H 'Accept: application/vnd.oci.image.index.v1+json' \
#     https://gcr.io/v2/distroless/cc-debian12/manifests/nonroot | grep -i docker-content-digest
# The data directory for the SQLite broker (--sqlite_path=/var/lib/cabby/...),
# owned by nonroot (65532) with mode 0700: distroless has no shell to create
# it, and a new named volume mounted there inherits this ownership. Built on
# the build platform: the result doesn't depend on the target architecture.
FROM --platform=$BUILDPLATFORM busybox:1.37@sha256:a7dbc319b1438115a207e11930dc4bb3e643dcb3ec0bdd694bb36cd8fad289ec AS data
RUN mkdir -p /out/var/lib/cabby && chown 65532:65532 /out/var/lib/cabby && chmod 0700 /out/var/lib/cabby

FROM gcr.io/distroless/cc-debian12:nonroot@sha256:9dac0a79194e45a7da0158a9c6da57b217585af0786db3845d1f0ec1a0dd182f
ARG TARGETARCH
# The workspace version; the release script passes it.
ARG VERSION=dev
LABEL org.opencontainers.image.title="Cabby" \
      org.opencontainers.image.description="A real-time WebSocket server in Rust, compatible with AnyCable clients" \
      org.opencontainers.image.licenses="MIT" \
      org.opencontainers.image.source="https://github.com/DaniG2k/cabby-releases" \
      org.opencontainers.image.documentation="https://docs.cabbyhq.com" \
      org.opencontainers.image.version="${VERSION}"
COPY --chmod=0755 dist/linux-${TARGETARCH}/cabby dist/linux-${TARGETARCH}/cabby-healthcheck /usr/local/bin/
COPY LICENSE THIRD_PARTY_LICENSES.md /usr/share/doc/cabby/
COPY --from=data --chown=65532:65532 /out/var/lib/cabby /var/lib/cabby
# localhost (the default) is unreachable from outside the container. The
# defaults use the ANYCABLE_* names, as in 0.1.0, so -e CABBY_HOST=... (read
# first) and -e ANYCABLE_HOST=... both override them.
ENV ANYCABLE_HOST=0.0.0.0
# The default broadcast adapters are http,redis; http alone lets the container
# run without Redis. Override with -e CABBY_BROADCAST_ADAPTER=...,
# -e ANYCABLE_BROADCAST_ADAPTER=... or the flag; with a Redis URL and HTTP-only
# broadcasts, Cabby logs a warning at startup.
ENV ANYCABLE_BROADCAST_ADAPTER=http
# 8080: WebSocket, SSE and /health. 8090: HTTP broadcasts when no secret is set;
# with a secret, set CABBY_HTTP_BROADCAST_PORT=8090 to keep broadcasts off the
# public port.
EXPOSE 8080 8090
USER nonroot
# cabby-healthcheck reads the port and the health path from the same variables
# as the server (CABBY_PORT, ANYCABLE_PORT, PORT; CABBY_HEALTH_PATH). It can't
# see flags: change the port or the health path with those variables, not with
# --port or --health-path.
HEALTHCHECK --interval=10s --timeout=3s --start-period=5s --retries=3 \
  CMD ["/usr/local/bin/cabby-healthcheck"]
ENTRYPOINT ["/usr/local/bin/cabby"]
