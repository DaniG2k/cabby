#!/usr/bin/env bash
# Runs the upstream Makefile conformance and feature targets against the pinned
# Go reference inside the cabby-reference image, on an isolated Docker network
# with its own Redis and NATS. Archives logs to compat/baseline/conformance/.
set -uo pipefail
here="$(cd "$(dirname "$0")/.." && pwd)"
out="$here/baseline/conformance"; mkdir -p "$out"
net=cabby-ref-net
docker network inspect $net >/dev/null 2>&1 || docker network create $net >/dev/null
docker rm -f cabby-ref-redis cabby-ref-nats >/dev/null 2>&1
docker run -d --rm --name cabby-ref-redis --network $net redis:7.4-alpine >/dev/null
docker run -d --rm --name cabby-ref-nats --network $net nats:alpine >/dev/null
cp "$here/reference-adapters/ruby/Gemfile.lock" "$here/reference/Gemfile.lock"

targets=("$@")
[ ${#targets[@]} -eq 0 ] && targets=(test-conformance test-conformance-ssl test-conformance-http \
  test-conformance-nats test-conformance-nats-embedded test-conformance-broker-http \
  test-conformance-broker-redis test-conformance-broker-nats test-conformance-embedded)

for t in "${targets[@]}"; do
  # Upstream CI sets REDIS_URL job-wide and runs NATS as a service.
  extra=(-e REDIS_URL=redis://cabby-ref-redis:6379/)
  [ "$t" = test-conformance-nats ] && extra+=(-e ANYCABLE_NATS_SERVERS=nats://cabby-ref-nats:4222)
  echo "== $t"
  timeout 900 docker run --rm --network $net -v "$here/reference":/src \
    -v cabby-bundle:/bundle -v cabby-gocache:/root/.cache -v cabby-gocache:/root/go \
    "${extra[@]}" cabby-reference:local \
    bash -c "make $t; rc=\$?; chown -R $(id -u):$(id -g) /src/tmp /src/dist 2>/dev/null; exit \$rc" \
    > "$out/$t.log" 2>&1
  rc=$?; echo "$t exit=$rc" | tee -a "$out/summary.txt"
  tail -4 "$out/$t.log"
done
docker rm -f cabby-ref-redis cabby-ref-nats >/dev/null 2>&1
