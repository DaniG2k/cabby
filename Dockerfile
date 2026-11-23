# syntax=docker/dockerfile:1
# Cabby's release image, built from the prebuilt release binaries (glibc 2.28
# baseline); nothing is compiled here. The build context holds:
#   dist/linux-amd64/cabby, dist/linux-arm64/cabby, LICENSE, THIRD_PARTY_LICENSES.md
# distroless/cc has glibc, the CA certificates outgoing TLS needs, and a nonroot
# user. The digest is the multi-arch index of gcr.io/distroless/cc-debian12:nonroot
# (uploaded 2026-08-21). To look it up again:
#   curl -sI -H 'Accept: application/vnd.oci.image.index.v1+json' \
#     https://gcr.io/v2/distroless/cc-debian12/manifests/nonroot | grep -i docker-content-digest
FROM gcr.io/distroless/cc-debian12:nonroot@sha256:9dac0a79194e45a7da0158a9c6da57b217585af0786db3845d1f0ec1a0dd182f
ARG TARGETARCH
LABEL org.opencontainers.image.title="Cabby" \
      org.opencontainers.image.description="A real-time WebSocket server in Rust, compatible with AnyCable clients" \
      org.opencontainers.image.licenses="MIT" \
      org.opencontainers.image.source="https://github.com/DaniG2k/cabby-releases" \
      org.opencontainers.image.documentation="https://docs.cabbyhq.com" \
      org.opencontainers.image.version="0.1.0"
COPY --chmod=0755 dist/linux-${TARGETARCH}/cabby /usr/local/bin/cabby
COPY LICENSE THIRD_PARTY_LICENSES.md /usr/share/doc/cabby/
# localhost (the default) is unreachable from outside the container.
ENV ANYCABLE_HOST=0.0.0.0
# The default broadcast adapters are http,redis; http alone lets the container
# run without Redis. Override with -e ANYCABLE_BROADCAST_ADAPTER=... or the flag.
ENV ANYCABLE_BROADCAST_ADAPTER=http
# 8080: WebSocket, SSE and /health. 8090: HTTP broadcasts when no secret is set.
EXPOSE 8080 8090
USER nonroot
ENTRYPOINT ["/usr/local/bin/cabby"]
