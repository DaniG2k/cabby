#!/usr/bin/env bash
# Soak comparison (memory stability, AnyCable issue 265, compat/deviations.md D-005): AnyCable
# (Go) and Cabby run the `cabby-bench soak` scenario at the same time, so one run covers both under
# the same machine conditions. Each server is pinned to its own physical core (both hyperthreads),
# each harness (with its fake RPC app) to two other cores:
#
#   AnyCable on ANYCABLE_CPUS (0-1), its harness on ANYCABLE_BENCH_CPUS (4-7)
#   Cabby    on CABBY_CPUS    (2-3), its harness on CABBY_BENCH_CPUS    (8-11)
#
# Both harnesses get the same flags and seed, so their clients follow the same random schedule.
# Results go to OUT (default docs/benchmarks/soak): <server>.txt (key=value summary), <server>.csv
# (samples), environment.txt; then compat/scripts/soak-chart.py draws docs/benchmarks/soak-memory.svg.
#
# Usage: [DURATION=2h] [WARMUP=5m] [SAMPLE_INTERVAL=30s] [CLIENTS=200] [RATE=20] [STREAMS=100]
#        [SEED=1] [SOAK_ARGS="--reject-auth=0.05 ..."] [CHART=docs/benchmarks/soak-memory.svg] \
#        compat/scripts/soak-compare.sh [OUT]
# Build first: cargo build --release -p cabby-server -p cabby-bench (target/reference-bin/anycable-go
# comes from cargo xtask reference-build).
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
OUT="${1:-${ROOT}/docs/benchmarks/soak}"
CHART="${CHART:-${ROOT}/docs/benchmarks/soak-memory.svg}"
DURATION="${DURATION:-2h}"
WARMUP="${WARMUP:-5m}"
SAMPLE_INTERVAL="${SAMPLE_INTERVAL:-30s}"
CLIENTS="${CLIENTS:-200}"
RATE="${RATE:-20}"
STREAMS="${STREAMS:-100}"
SEED="${SEED:-1}"
read -r -a EXTRA <<<"${SOAK_ARGS:-}"
ANYCABLE_CPUS="${ANYCABLE_CPUS:-0-1}"
CABBY_CPUS="${CABBY_CPUS:-2-3}"
ANYCABLE_BENCH_CPUS="${ANYCABLE_BENCH_CPUS:-4-7}"
CABBY_BENCH_CPUS="${CABBY_BENCH_CPUS:-8-11}"
export CABBY_BENCH_THREADS="${CABBY_BENCH_THREADS:-4}"

BENCH="${ROOT}/target/release/cabby-bench"
declare -A BIN=([anycable-go]="${ROOT}/target/reference-bin/anycable-go" [cabby]="${ROOT}/target/release/cabby")
declare -A SERVER_CPUS=([anycable-go]="${ANYCABLE_CPUS}" [cabby]="${CABBY_CPUS}")
declare -A HARNESS_CPUS=([anycable-go]="${ANYCABLE_BENCH_CPUS}" [cabby]="${CABBY_BENCH_CPUS}")
for f in "${BENCH}" "${BIN[anycable-go]}" "${BIN[cabby]}"; do
  [[ -x "${f}" ]] || { echo "missing ${f}: build release binaries first" >&2; exit 1; }
done

# exec-wrappers keep the server's PID (the harness samples /proc/<pid>) while pinning it.
WRAP="$(mktemp -d)"
PIDS=()
cleanup() {
  for p in "${PIDS[@]}"; do kill -TERM "${p}" 2>/dev/null || true; done
  wait 2>/dev/null || true
  rm -rf "${WRAP}"
}
trap cleanup EXIT
trap 'exit 130' INT TERM
for name in anycable-go cabby; do
  mkdir -p "${WRAP}/${name}"
  printf '#!/bin/sh\nexec taskset -c %s %s "$@"\n' "${SERVER_CPUS[${name}]}" "${BIN[${name}]}" >"${WRAP}/${name}/${name}"
  chmod +x "${WRAP}/${name}/${name}"
done

mkdir -p "${OUT}"
FLAGS=(--duration="${DURATION}" --warmup="${WARMUP}" --sample-interval="${SAMPLE_INTERVAL}" -c "${CLIENTS}"
  -r "${RATE}" -S "${STREAMS}" --seed="${SEED}" --non-interactive "${EXTRA[@]}")
{
  echo "date=$(date -u +%Y-%m-%dT%H:%M:%SZ)"
  echo "cpu=$(lscpu | sed -n 's/^Model name: *//p')"
  echo "kernel=$(uname -r)"
  echo "memory=$(free -g | awk '/^Mem:/ {print $2 " GiB"}')"
  echo "anycable_cpus=${ANYCABLE_CPUS} anycable_bench_cpus=${ANYCABLE_BENCH_CPUS} cabby_cpus=${CABBY_CPUS} cabby_bench_cpus=${CABBY_BENCH_CPUS} bench_threads=${CABBY_BENCH_THREADS}"
  echo "flags=${FLAGS[*]}"
  echo "cabby=$(git -C "${ROOT}" rev-parse --short HEAD)$(git -C "${ROOT}" diff --quiet HEAD -- crates || echo -dirty) $("${BIN[cabby]}" --version 2>/dev/null | head -1)"
  echo "anycable-go=$(sed -n 's/^commit *= *//p' "${ROOT}/compat/upstream.lock" | head -1) $(ANYCABLE_DISABLE_TELEMETRY=true "${BIN[anycable-go]}" --version 2>/dev/null | head -1)"
} >"${OUT}/environment.txt"

echo "soak: anycable-go and cabby in parallel for ${DURATION} (results in ${OUT})" >&2
for name in anycable-go cabby; do
  taskset -c "${HARNESS_CPUS[${name}]}" "${BENCH}" soak --sut "${WRAP}/${name}/${name}" \
    --samples-out="${OUT}/${name}.csv" "${FLAGS[@]}" >"${OUT}/${name}.txt" 2>"${OUT}/${name}.stderr" &
  PIDS+=("$!")
done
STATUS=0
for i in 0 1; do
  name=$([[ ${i} == 0 ]] && echo anycable-go || echo cabby)
  if wait "${PIDS[${i}]}"; then code=0; else code=$?; fi
  echo "${name}: exit ${code}" >&2
  ((code == 0)) || STATUS=1
  [[ -s "${OUT}/${name}.stderr" ]] || rm -f "${OUT}/${name}.stderr"
done
PIDS=()

for name in anycable-go cabby; do
  echo "--- ${name}" >&2
  grep -E '^(rss_|residual_rss|residual_threads|residual_sockets|messages_missing|messages_duplicate|messages_misrouted|drained|generator_saturated)' \
    "${OUT}/${name}.txt" >&2 || true
done
python3 "${ROOT}/compat/scripts/soak-chart.py" "${OUT}" "${CHART}" >&2
exit "${STATUS}"
