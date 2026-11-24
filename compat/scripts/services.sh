#!/usr/bin/env bash
# Starts the external services translated tests require (pinned images from
# compat/upstream.lock) on loopback ports and prints the env to export:
#   eval "$(compat/scripts/services.sh start)"   /   compat/scripts/services.sh stop
# Tests never skip when a service is missing; they fail with a clear message.
# PostgreSQL is for Cabby's own Postgres adapters (D-070, D-071); upstream has none.
set -euo pipefail
redis=redis@sha256:7f7d30b7fdd09467bac58198a1f166dd69789d348d4fe859cccb20e0cf287fbe
nats=nats@sha256:558394a23dd3c36f2d3a64b51865fd09908010e80a542cea5e75c2af543ed62a
postgres=postgres:18.6-alpine@sha256:9ba53c7d41ea093baf5c1ba635ba5434c4f979880527d49bb7d9237d3193085b
case "${1:-start}" in
  start)
    # Idempotent: reuse running containers so concurrent callers share them.
    running() { [ "$(docker inspect -f '{{.State.Running}}' "$1" 2>/dev/null)" = true ]; }
    running cabby-svc-redis || { docker rm -f cabby-svc-redis >/dev/null 2>&1 || true
      docker run -d --rm --name cabby-svc-redis -p 127.0.0.1:0:6379 "$redis" >/dev/null; }
    running cabby-svc-nats || { docker rm -f cabby-svc-nats >/dev/null 2>&1 || true
      docker run -d --rm --name cabby-svc-nats -p 127.0.0.1:0:4222 -p 127.0.0.1:0:8222 "$nats" -js -m 8222 >/dev/null; }
    running cabby-svc-postgres || { docker rm -f cabby-svc-postgres >/dev/null 2>&1 || true
      docker run -d --rm --name cabby-svc-postgres -p 127.0.0.1:0:5432 -e POSTGRES_USER=cabby \
        -e POSTGRES_PASSWORD=cabby -e POSTGRES_DB=cabby "$postgres" -c max_connections=1000 >/dev/null; }
    rp=$(docker port cabby-svc-redis 6379/tcp | head -1 | cut -d: -f2)
    np=$(docker port cabby-svc-nats 4222/tcp | head -1 | cut -d: -f2)
    mp=$(docker port cabby-svc-nats 8222/tcp | head -1 | cut -d: -f2)
    pp=$(docker port cabby-svc-postgres 5432/tcp | head -1 | cut -d: -f2)
    for _ in $(seq 1 100); do docker exec cabby-svc-redis redis-cli ping 2>/dev/null | grep -q PONG && break; sleep 0.1; done
    for _ in $(seq 1 100); do curl -sf "http://127.0.0.1:$mp/healthz" >/dev/null && break; sleep 0.1; done
    # Over TCP: the image's init phase runs a socket-only server first.
    for _ in $(seq 1 300); do docker exec cabby-svc-postgres pg_isready -q -h 127.0.0.1 -U cabby 2>/dev/null && break; sleep 0.1; done
    echo "export CABBY_TEST_REDIS_URL=redis://127.0.0.1:$rp/ CABBY_TEST_NATS_URL=nats://127.0.0.1:$np CABBY_TEST_NATS_MONITOR_URL=http://127.0.0.1:$mp CABBY_TEST_POSTGRES_URL=postgres://cabby:cabby@127.0.0.1:$pp/cabby"
    ;;
  restart) "$0" stop; exec "$0" start ;;
  stop) docker rm -f cabby-svc-redis cabby-svc-nats cabby-svc-postgres >/dev/null 2>&1 || true ;;
  *) echo "usage: $0 start|stop|restart" >&2; exit 2 ;;
esac
