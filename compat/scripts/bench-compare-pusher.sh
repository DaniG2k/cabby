#!/usr/bin/env bash
# Pusher-protocol benchmark comparison: Soketi vs Laravel Reverb vs AnyCable (Go) vs Cabby, all in
# Pusher mode, with the external load harness (crates/cabby-bench, `--protocol=pusher`).
#
# Same method as compat/scripts/bench-compare.sh: the server under test is pinned to SERVER_CPUS and
# the generator to BENCH_CPUS, each workload gets one discarded warm-up run per server and then RUNS
# rounds in a freshly shuffled server order (seeded by SEED). Clients speak pusher-js's protocol 7 on
# public channels; broadcasts are signed POSTs to the Pusher HTTP API, one event per request for every
# server (AnyCable's Pusher API has no batch_events). Servers and their settings: bench/pusher/README.md.
# AnyCable and Cabby take the Pusher flags natively; Soketi and Reverb run through bench/soketi/sut and
# bench/reverb/sut, and are skipped with a warning when they are not installed (their setup.sh).
#
# Usage: [SUTS="soketi reverb anycable-go cabby"] compat/scripts/bench-compare-pusher.sh [OUT]
#        (default OUT: docs/benchmarks/pusher-results)
#        SMOKE=1 runs every workload at a small scale (200 clients, a few seconds) to check the setup.
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
OUT="${1:-${ROOT}/docs/benchmarks/pusher-results}"
SMOKE="${SMOKE:-0}"
RUNS="${RUNS:-$([[ "${SMOKE}" == 1 ]] && echo 1 || echo 5)}"
SEED="${SEED:-42}"
RANDOM="${SEED}"
SERVER_CPUS="${SERVER_CPUS:-0-3}"
BENCH_CPUS="${BENCH_CPUS:-4-11}"
export CABBY_BENCH_THREADS="${CABBY_BENCH_THREADS:-4}"

# The Action Cable comparison lives in docs/benchmarks/results; never mix the two.
mkdir -p "${OUT}"
if [[ "$(cd "${OUT}" && pwd)" == "$(cd "${ROOT}/docs/benchmarks" && pwd)/results" ]]; then
  echo "refusing to write Pusher results into docs/benchmarks/results (use docs/benchmarks/pusher-results)" >&2
  exit 1
fi

BENCH="${ROOT}/target/release/cabby-bench"
declare -A BIN=(
  [soketi]="${ROOT}/bench/soketi/sut"
  [reverb]="${ROOT}/bench/reverb/sut"
  [anycable-go]="${ROOT}/target/reference-bin/anycable-go"
  [cabby]="${ROOT}/target/release/cabby"
)
declare -A VERSION=()
[[ -x "${BENCH}" ]] || { echo "missing ${BENCH}: build release binaries first" >&2; exit 1; }
read -r -a REQUESTED <<<"${SUTS:-soketi reverb anycable-go cabby}"
SUT_LIST=()
for sut in "${REQUESTED[@]}"; do
  [[ -n "${BIN[${sut}]:-}" ]] || { echo "unknown server ${sut} (soketi, reverb, anycable-go, cabby)" >&2; exit 1; }
  if [[ ! -x "${BIN[${sut}]}" ]]; then
    echo "missing ${BIN[${sut}]}: build release binaries first (anycable-go: cargo xtask reference-build)" >&2
    exit 1
  fi
  if ! VERSION[${sut}]="$("${BIN[${sut}]}" --version 2>/dev/null | head -1)"; then
    if [[ "${sut}" == soketi || "${sut}" == reverb ]]; then
      echo "skipping ${sut}: not installed (run bench/${sut}/setup.sh, see bench/pusher/README.md)" >&2
      continue
    fi
    echo "${BIN[${sut}]} --version failed" >&2
    exit 1
  fi
  SUT_LIST+=("${sut}")
done
((${#SUT_LIST[@]})) || { echo "no server to benchmark" >&2; exit 1; }

# exec-wrappers keep the server's PID (the harness samples /proc/<pid>) while pinning it.
WRAP="$(mktemp -d)"
trap 'rm -rf "${WRAP}"' EXIT
for name in "${SUT_LIST[@]}"; do
  mkdir -p "${WRAP}/${name}"
  printf '#!/bin/sh\nexec taskset -c %s %s "$@"\n' "${SERVER_CPUS}" "${BIN[${name}]}" >"${WRAP}/${name}/${name}"
  chmod +x "${WRAP}/${name}/${name}"
done

mkdir -p "${OUT}/warmup"
{
  echo "date=$(date -u +%Y-%m-%dT%H:%M:%SZ)"
  echo "cpu=$(lscpu | sed -n 's/^Model name: *//p')"
  echo "kernel=$(uname -r)"
  echo "protocol=pusher publish_batch=1"
  echo "server_cpus=${SERVER_CPUS} bench_cpus=${BENCH_CPUS} bench_threads=${CABBY_BENCH_THREADS} runs=${RUNS} warmup=1 order=shuffled seed=${SEED} smoke=${SMOKE}"
  echo "suts=${SUT_LIST[*]}"
  echo "cabby=$(git -C "${ROOT}" rev-parse --short HEAD)"
  echo "anycable-go=$(sed -n 's/^commit *= *//p' "${ROOT}/compat/upstream.lock" | head -1)"
  for sut in "${SUT_LIST[@]}"; do
    [[ "${sut}" == soketi || "${sut}" == reverb ]] && echo "${sut}=${VERSION[${sut}]}"
  done
} >"${OUT}/environment.txt"

shuffled_suts() { # prints SUT_LIST in a random order (Fisher-Yates on $RANDOM)
  local a=("${SUT_LIST[@]}") i j t
  for ((i = ${#a[@]} - 1; i > 0; i--)); do
    j=$((RANDOM % (i + 1))); t="${a[i]}"; a[i]="${a[j]}"; a[j]="${t}"
  done
  echo "${a[@]}"
}

one() { # one <out-file> <sut> <scenario> <args...>
  local f="$1" sut="$2" scenario="$3"; shift 3
  taskset -c "${BENCH_CPUS}" "${BENCH}" "${scenario}" --protocol=pusher --sut "${WRAP}/${sut}/${sut}" \
    --non-interactive "$@" >"${f}" || echo "exit=$?" >>"${f}"
  sleep 2
}

run() { # run <workload-name> <scenario> <args...>
  local name="$1" scenario="$2"; shift 2
  local sut i
  for sut in "${SUT_LIST[@]}"; do
    echo ">> ${name} ${sut} warm-up" >&2
    one "${OUT}/warmup/${name}.${sut}.txt" "${sut}" "${scenario}" "$@"
  done
  for i in $(seq 1 "${RUNS}"); do
    for sut in $(shuffled_suts); do
      echo ">> ${name} ${sut} run ${i}" >&2
      one "${OUT}/${name}.${sut}.${i}.txt" "${sut}" "${scenario}" "$@"
    done
  done
}

if [[ "${SMOKE}" == 1 ]]; then
  # Every workload's shape at a small scale: shows the setup works, measures nothing.
  run idle-1k   stress_publications -c 200 -S 10 -s 1 -r 1 -d 3s
  run fanout-50k stress_publications -c 200 -S 10 -s 2 -r 50 -d 3s
  run room-5k   stress_publications -c 200 -S 1 -s 1 -r 20 -d 3s
  run churn reconnect_floods -c 200 -s 2 --waves 50,100 --wave-interval 5s --settle 1s
  echo "results in ${OUT}" >&2
  exit 0
fi

# Idle connections: memory per connection from the difference between 1k and 10k clients.
run idle-1k   stress_publications -c 1000  -S 100 -s 1 -r 1 -d 10s
run idle-10k  stress_publications -c 10000 -S 100 -s 1 -r 1 -d 10s
# Fan-out: 1000 clients x 5 of 100 channels (about 50 subscribers per channel).
run fanout-50k  stress_publications -c 1000 -S 100 -s 5 -r 1000 -d 20s
run fanout-100k stress_publications -c 1000 -S 100 -s 5 -r 2000 -d 20s
run fanout-150k stress_publications -c 1000 -S 100 -s 5 -r 3000 -d 20s
# Large room: 5000 clients on one channel.
CABBY_BENCH_THREADS=8 run room-5k stress_publications -c 5000 -S 1 -s 1 -r 20 -d 20s
# Churn: the Action Cable comparison's reconnect waves, without RPC (Pusher public channels need no
# application: no connect/subscribe/disconnect latency, no disconnect queue).
run churn reconnect_floods -c 2000 -s 2 --waves 500,1000 --wave-interval 10s
echo "results in ${OUT}" >&2
