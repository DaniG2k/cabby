#!/usr/bin/env bash
# Action Cable vs AnyCable (Go) vs Cabby benchmark comparison with the external load harness
# (crates/cabby-bench).
#
# The Go and Rust servers are release builds launched by the harness with identical AnyCable
# flags; Action Cable is the Rails app in bench/actioncable, whose `sut` wrapper takes the same
# flags and execs Puma (see bench/actioncable/README.md). SUTS picks the servers (default all
# three); Action Cable is skipped with a warning when its Ruby or gems are not installed.
# The server under test is pinned to SERVER_CPUS and the generator to BENCH_CPUS (separate
# physical cores). Each workload first gets one discarded warm-up run per server, then RUNS
# rounds; within a round the servers run in a freshly shuffled order (seeded by SEED), so slow
# drift of the machine does not favor whichever server runs first. Each run's key=value summary
# is kept under OUT. Runs that report generator_saturated=true are kept but excluded from the
# summary by compat/scripts/bench-summary.py.
#
# Usage: [SUTS="actioncable anycable-go cabby"] compat/scripts/bench-compare.sh [OUT]
#        SUTS="cabby-memory cabby-sqlite" compares the memory and SQLite brokers (bench/sqlite).
#        WORKLOADS=ceiling runs only the rate ladder (Go and Cabby, SERVER_CPUS=0-1: one physical
#        core, so that the generator is not the limit). The server's limit is the highest rate
#        with no message lost and a p99 latency of at most 50 ms.
#        (default OUT: docs/benchmarks/results)
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
OUT="${1:-${ROOT}/docs/benchmarks/results}"
RUNS="${RUNS:-5}"
SEED="${SEED:-42}"
RANDOM="${SEED}"
SERVER_CPUS="${SERVER_CPUS:-0-3}"
BENCH_CPUS="${BENCH_CPUS:-4-11}"
export CABBY_BENCH_THREADS="${CABBY_BENCH_THREADS:-4}"

BENCH="${ROOT}/target/release/cabby-bench"
declare -A BIN=(
  [actioncable]="${ROOT}/bench/actioncable/sut"
  [anycable-go]="${ROOT}/target/reference-bin/anycable-go"
  [cabby]="${ROOT}/target/release/cabby"
  [cabby-memory]="${ROOT}/bench/sqlite/cabby-memory"
  [cabby-sqlite]="${ROOT}/bench/sqlite/cabby-sqlite"
)
[[ -x "${BENCH}" ]] || { echo "missing ${BENCH}: build release binaries first" >&2; exit 1; }
read -r -a REQUESTED <<<"${SUTS:-actioncable anycable-go cabby}"
SUT_LIST=()
for sut in "${REQUESTED[@]}"; do
  [[ -n "${BIN[${sut}]:-}" ]] || { echo "unknown server ${sut} (actioncable, anycable-go, cabby, cabby-memory, cabby-sqlite)" >&2; exit 1; }
  if [[ "${sut}" == actioncable ]]; then
    if ! AC_VERSION="$("${BIN[actioncable]}" --version 2>/dev/null)"; then
      echo "skipping actioncable: Ruby or its gems are not installed (see bench/actioncable/README.md)" >&2
      continue
    fi
  elif [[ ! -x "${BIN[${sut}]}" ]]; then
    echo "missing ${BIN[${sut}]}: build release binaries first" >&2
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
  echo "server_cpus=${SERVER_CPUS} bench_cpus=${BENCH_CPUS} bench_threads=${CABBY_BENCH_THREADS} runs=${RUNS} warmup=1 order=shuffled seed=${SEED}"
  echo "suts=${SUT_LIST[*]}"
  echo "cabby=$(git -C "${ROOT}" rev-parse --short HEAD)"
  echo "anycable-go=$(sed -n 's/^commit *= *//p' "${ROOT}/compat/upstream.lock" | head -1)"
  [[ -z "${AC_VERSION:-}" ]] || echo "actioncable=${AC_VERSION}"
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
  taskset -c "${BENCH_CPUS}" "${BENCH}" "${scenario}" --sut "${WRAP}/${sut}/${sut}" \
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

if [[ "${WORKLOADS:-standard}" == ceiling ]]; then
  export CABBY_BENCH_THREADS=8
  for r in 1000 1500 2000 2500 3000 3500 4000; do
    run "core1-$((r * 50 / 1000))k" stress_publications -c 1000 -S 100 -s 5 -r "${r}" -d 20s
  done
  echo "results in ${OUT}" >&2
  exit
fi

# Idle connections: memory per connection from the difference between 1k and 10k clients.
run idle-1k   stress_publications -c 1000  -S 100 -s 1 -r 1 -d 10s
run idle-10k  stress_publications -c 10000 -S 100 -s 1 -r 1 -d 10s
# Fan-out: 1000 clients x 5 of 100 streams (about 50 subscribers per stream).
run fanout-50k  stress_publications -c 1000 -S 100 -s 5 -r 1000 -d 20s
run fanout-100k stress_publications -c 1000 -S 100 -s 5 -r 2000 -d 20s
run fanout-150k stress_publications -c 1000 -S 100 -s 5 -r 3000 -d 20s
# Large room: 5000 clients on one stream.
CABBY_BENCH_THREADS=8 run room-5k stress_publications -c 5000 -S 1 -s 1 -r 20 -d 20s
# Realistic payloads: 1 and 8 KiB fan-out, and 32 KiB to the large room (memory per connection).
run payload-1k stress_publications -c 1000 -S 100 -s 5 -r 800 -d 20s --payload-bytes 1024
run payload-8k stress_publications -c 1000 -S 100 -s 5 -r 200 -d 20s --payload-bytes 8192
CABBY_BENCH_THREADS=8 run room-5k-32k stress_publications -c 5000 -S 1 -s 1 -r 1 -d 20s --payload-bytes 32768
# Churn: reconnect floods against a 5 ms RPC (connect + subscribe + disconnect over RPC). Action
# Cable makes no RPC calls: it sleeps the same jittered 5 ms in its connect, subscribed and
# disconnect callbacks (the harness exports the latency to the server process).
run churn reconnect_floods -c 2000 -s 2 --waves 500,1000 --wave-interval 10s --rpc-latency 5ms
echo "results in ${OUT}" >&2
