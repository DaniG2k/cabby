#!/usr/bin/env bash
# Go vs Cabby benchmark comparison with the external load harness (crates/cabby-bench).
#
# Both servers are release builds launched by the harness with identical AnyCable flags.
# The server under test is pinned to SERVER_CPUS and the generator to BENCH_CPUS (separate
# physical cores), every configuration runs RUNS times, and each run's key=value summary is
# kept under OUT. Runs that report generator_saturated=true are kept but excluded from the
# summary by compat/scripts/bench-summary.py.
#
# Usage: compat/scripts/bench-compare.sh [OUT]   (default: docs/benchmarks/results)
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
OUT="${1:-${ROOT}/docs/benchmarks/results}"
RUNS="${RUNS:-3}"
SERVER_CPUS="${SERVER_CPUS:-0-3}"
BENCH_CPUS="${BENCH_CPUS:-4-11}"
export CABBY_BENCH_THREADS="${CABBY_BENCH_THREADS:-4}"

BENCH="${ROOT}/target/release/cabby-bench"
declare -A BIN=(
  [cabby]="${ROOT}/target/release/cabby"
  [anycable-go]="${ROOT}/target/reference-bin/anycable-go"
)
for b in "${BENCH}" "${BIN[cabby]}" "${BIN[anycable-go]}"; do
  [[ -x "${b}" ]] || { echo "missing ${b}: build release binaries first" >&2; exit 1; }
done

# exec-wrappers keep the server's PID (the harness samples /proc/<pid>) while pinning it.
WRAP="$(mktemp -d)"
trap 'rm -rf "${WRAP}"' EXIT
for name in cabby anycable-go; do
  mkdir -p "${WRAP}/${name}"
  printf '#!/bin/sh\nexec taskset -c %s %s "$@"\n' "${SERVER_CPUS}" "${BIN[${name}]}" >"${WRAP}/${name}/${name}"
  chmod +x "${WRAP}/${name}/${name}"
done

mkdir -p "${OUT}"
{
  echo "date=$(date -u +%Y-%m-%dT%H:%M:%SZ)"
  echo "cpu=$(lscpu | sed -n 's/^Model name: *//p')"
  echo "kernel=$(uname -r)"
  echo "server_cpus=${SERVER_CPUS} bench_cpus=${BENCH_CPUS} bench_threads=${CABBY_BENCH_THREADS} runs=${RUNS}"
  echo "cabby=$(git -C "${ROOT}" rev-parse --short HEAD)"
  echo "anycable-go=$(sed -n 's/^commit *= *//p' "${ROOT}/compat/upstream.lock" | head -1)"
} >"${OUT}/environment.txt"

run() { # run <workload-name> <scenario> <args...>
  local name="$1" scenario="$2"; shift 2
  for sut in anycable-go cabby; do
    for i in $(seq 1 "${RUNS}"); do
      local f="${OUT}/${name}.${sut}.${i}.txt"
      echo ">> ${name} ${sut} run ${i}" >&2
      taskset -c "${BENCH_CPUS}" "${BENCH}" "${scenario}" --sut "${WRAP}/${sut}/${sut}" \
        --non-interactive "$@" >"${f}" 2>"${f%.txt}.stderr" || echo "exit=$?" >>"${f}"
      sleep 2
    done
  done
}

# Idle connections: memory per connection from the difference between 1k and 10k clients.
run idle-1k   stress_publications -c 1000  -S 100 -s 1 -r 1 -d 10s
run idle-10k  stress_publications -c 10000 -S 100 -s 1 -r 1 -d 10s
# Fan-out: 1000 clients x 5 of 100 streams (about 50 subscribers per stream).
run fanout-50k  stress_publications -c 1000 -S 100 -s 5 -r 1000 -d 20s
run fanout-100k stress_publications -c 1000 -S 100 -s 5 -r 2000 -d 20s
run fanout-150k stress_publications -c 1000 -S 100 -s 5 -r 3000 -d 20s
# Large room: 5000 clients on one stream.
run room-5k stress_publications -c 5000 -S 1 -s 1 -r 20 -d 20s
# Churn: reconnect floods against a 5 ms RPC (connect + subscribe + disconnect over RPC).
run churn reconnect_floods -c 2000 -s 2 --waves 500,1000 --wave-interval 10s --rpc-latency 5ms
echo "results in ${OUT}" >&2
