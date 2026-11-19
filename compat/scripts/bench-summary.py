#!/usr/bin/env python3
"""Summarize compat/scripts/bench-compare.sh results: the median of each metric per workload and
server, excluding runs where the generator was saturated or the scenario failed.

Usage: compat/scripts/bench-summary.py [RESULTS_DIR] [--keys]   (--keys lists every metric seen)
"""
import statistics
import sys
from collections import defaultdict
from pathlib import Path

root = Path(__file__).resolve().parents[2]
args = [a for a in sys.argv[1:] if not a.startswith("--")]
results = Path(args[0]) if args else root / "docs/benchmarks/results"

runs = defaultdict(list)  # (workload, sut) -> [dict]
excluded = defaultdict(int)
for f in sorted(results.glob("*.*.*.txt")):
    workload, sut, _ = f.name[: -len(".txt")].rsplit(".", 2)
    kv = {}
    for line in f.read_text().splitlines():
        if "=" in line:
            k, v = line.split("=", 1)
            kv[k.strip()] = v.strip()
    if kv.get("generator_saturated") == "true" or "exit" in kv:
        excluded[(workload, sut)] += 1
        continue
    runs[(workload, sut)].append(kv)

if "--keys" in sys.argv:
    keys = sorted({k for rs in runs.values() for r in rs for k in r})
    print("\n".join(keys))
    sys.exit()


def med(workload, sut, key):
    vals = []
    for r in runs.get((workload, sut), []):
        try:
            vals.append(float(r[key]))
        except (KeyError, ValueError):
            pass
    return statistics.median(vals) if vals else None


def total(workload, sut, keys):
    n = 0.0
    for r in runs.get((workload, sut), []):
        for k in keys:
            try:
                n += float(r.get(k, 0))
            except ValueError:
                pass
    return n


def fmt(v, digits=1):
    return "–" if v is None else f"{v:,.{digits}f}"


SUTS = ["anycable-go", "cabby"]
workloads = sorted({w for w, _ in runs} | {w for w, _ in excluded})
print("| Workload | Metric | AnyCable (Go) | Cabby (Rust) |")
print("|---|---|---:|---:|")

if all((f"idle-{n}", s) in runs for n in ("1k", "10k") for s in SUTS):
    vals = {}
    for s in SUTS:
        a, b = med("idle-1k", s, "peak_server_rss_mb"), med("idle-10k", s, "peak_server_rss_mb")
        vals[s] = (a, b, None if a is None or b is None else (b - a) * 1024 / 9000)
    print(f"| Idle connections | RSS with 1,000 clients (MB) | {fmt(vals['anycable-go'][0])} | {fmt(vals['cabby'][0])} |")
    print(f"| | RSS with 10,000 clients (MB) | {fmt(vals['anycable-go'][1])} | {fmt(vals['cabby'][1])} |")
    print(f"| | Memory per extra connection (KB) | {fmt(vals['anycable-go'][2])} | {fmt(vals['cabby'][2])} |")
    t = [med("idle-10k", s, "peak_server_threads") for s in SUTS]
    print(f"| | OS threads with 10,000 clients | {fmt(t[0], 0)} | {fmt(t[1], 0)} |")

for w in workloads:
    if w.startswith("idle"):
        continue
    keyset = {k for s in SUTS for r in runs.get((w, s), []) for k in r}
    rows = []
    if "throughput_msgs_per_sec" in keyset:
        rows += [
            ("Delivered msg/s (expected)", lambda s: f"{fmt(med(w, s, 'throughput_msgs_per_sec'), 0)} ({fmt(med(w, s, 'expected_msgs_per_sec'), 0)})"),
            ("Latency p50 / p99 (ms)", lambda s: f"{fmt(med(w, s, 'latency_p50_ms'), 2)} / {fmt(med(w, s, 'latency_p99_ms'), 2)}"),
            ("Server CPU (cores)", lambda s: fmt(med(w, s, "server_cpu_cores"), 2)),
            ("Server peak RSS (MB)", lambda s: fmt(med(w, s, "peak_server_rss_mb"))),
            ("Missing / duplicate / out-of-order (all runs)", lambda s: " / ".join(
                fmt(total(w, s, [k]), 0) for k in ("messages_missing_ids", "duplicates", "ordering_violations"))),
        ]
    else:
        for k in sorted(keyset):
            if any(x in k for x in ("seconds", "_ms", "peak_server", "cpu_cores", "failed", "lost", "healthy")):
                rows.append((k, lambda s, k=k: fmt(med(w, s, k), 2)))
    first = True
    for label, f in rows:
        print(f"| {w if first else ''} | {label} | {f('anycable-go')} | {f('cabby')} |")
        first = False

for (w, s), n in sorted(excluded.items()):
    print(f"\nExcluded {n} run(s) of {w} on {s} (generator saturated or scenario failed).", file=sys.stderr)
