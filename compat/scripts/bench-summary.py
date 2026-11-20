#!/usr/bin/env python3
"""Summarize compat/scripts/bench-compare.sh results as a Markdown table: the median of each metric
per workload and server, one column per server found in the results (Action Cable, AnyCable, Cabby).

Runs where the generator was saturated are excluded, and runs that printed no summary (the server
or the client pool failed to start) are listed as failures below the table. Runs that completed but
did not deliver the load (missing messages, dropped publications, failed POSTs, delivery well below
the expected rate, clients not recovered) are kept: the table says so for that server (delivered share, missing messages and the
saturation evidence), and its latency and CPU cells are marked, since they describe a server that
fell behind.

Usage: compat/scripts/bench-summary.py [RESULTS_DIR] [--keys]   (--keys lists every metric seen)
"""
import statistics
import sys
from collections import defaultdict
from pathlib import Path

root = Path(__file__).resolve().parents[2]
args = [a for a in sys.argv[1:] if not a.startswith("--")]
results = Path(args[0]) if args else root / "docs/benchmarks/results"

ORDER = ["actioncable", "anycable-go", "cabby"]
LABELS = {"actioncable": "Action Cable (Ruby)", "anycable-go": "AnyCable (Go)", "cabby": "Cabby (Rust)"}
MARK = "†"

runs = defaultdict(list)  # (workload, sut) -> [dict]
excluded = defaultdict(int)  # generator saturated
failed = defaultdict(list)  # (workload, sut) -> [reason]
seen_suts = set()
for f in sorted(results.glob("*.*.*.txt")):
    workload, sut, _ = f.name[: -len(".txt")].rsplit(".", 2)
    seen_suts.add(sut)
    kv, other = {}, []
    for line in f.read_text().splitlines():
        if "=" in line and " " not in line.split("=", 1)[0]:
            k, v = line.split("=", 1)
            kv[k.strip()] = v.strip()
        elif line.strip():
            other.append(line.strip())
    if kv.get("generator_saturated") == "true":
        excluded[(workload, sut)] += 1
    elif "generator_saturated" not in kv:
        failed[(workload, sut)].append(other[0] if other else f"no summary (exit {kv.get('exit', '?')})")
    else:
        runs[(workload, sut)].append(kv)

if "--keys" in sys.argv:
    keys = sorted({k for rs in runs.values() for r in rs for k in r})
    print("\n".join(keys))
    sys.exit()

SUTS = [s for s in ORDER if s in seen_suts] + sorted(seen_suts - set(ORDER))


def num(r, key):
    try:
        v = float(r[key])
    except (KeyError, ValueError):
        return None
    # Negative durations mean "not measured" (e.g. a drain without a queue gauge).
    return None if v < 0 and key.endswith("_seconds") else v


def med(workload, sut, key):
    vals = [v for r in runs.get((workload, sut), []) if (v := num(r, key)) is not None]
    return statistics.median(vals) if vals else None


def total(workload, sut, key):
    return sum(num(r, key) or 0.0 for r in runs.get((workload, sut), []))


def fmt(v, digits=1):
    return "–" if v is None else f"{v:,.{digits}f}"


# A run whose delivered rate (publish window plus drain) fell below this share of the expected
# rate fell behind, even if every message arrived in the end (Go and Cabby stay above 99.7%).
MIN_RATE = 0.95


def rate_share(r):
    exp = num(r, "expected_msgs_per_sec")
    return (num(r, "throughput_msgs_per_sec") or 0) / exp if exp else 1.0


def sustained(r):
    """Whether a stress_publications run delivered everything it published, at the expected rate.
    Ordering violations and duplicates are correctness, not load: they have their own row."""
    return all((num(r, k) or 0) == 0 for k in
               ("messages_missing_ids", "publications_dropped", "publications_failed_posts", "clients_short")) \
        and rate_share(r) >= MIN_RATE


def delivery(workload, sut):
    """'yes', or the delivered share, missing messages and saturation evidence over all runs."""
    rs = runs.get((workload, sut), [])
    if not rs:
        return "–"
    if all(sustained(r) for r in rs):
        return "yes"
    got = sum(max((num(r, "latency_count") or 0) - (num(r, "duplicates") or 0), 0) for r in rs)
    missing = total(workload, sut, "messages_missing_ids")
    share = 100 * got / (got + missing) if got + missing else 0
    parts = [f"no: {share:.1f}% delivered", f"{missing:,.0f} missing",
             f"at {100 * statistics.median(rate_share(r) for r in rs):.0f}% of the expected rate"]
    for key, what in (("publications_dropped", "publications dropped (queue full)"),
                      ("publications_failed_posts", "failed POSTs")):
        if (n := total(workload, sut, key)) > 0:
            parts.append(f"{n:,.0f} {what}")
    if (late := med(workload, sut, "scheduler_late_publishes")) and late > 0:
        parts.append(f"{late:,.0f} late publishes")
    parts.append(f"POST p99 {fmt(med(workload, sut, 'post_round_trip_p99_ms'), 1)} ms")
    return ", ".join(parts) + f" ({sum(not sustained(r) for r in rs)} of {len(rs)} runs)"


def marked(workload, sut, text):
    rs = runs.get((workload, sut), [])
    return f"{text} {MARK}" if rs and not all(sustained(r) for r in rs) and text != "–" else text


workloads = sorted({w for w, _ in runs} | {w for w, _ in excluded} | {w for w, _ in failed})
print("| Workload | Metric | " + " | ".join(LABELS.get(s, s) for s in SUTS) + " |")
print("|---|---|" + "---:|" * len(SUTS))


def row(workload, label, cell):
    print(f"| {workload} | ".replace("|  |", "| |") + f"{label} | " + " | ".join(cell(s) for s in SUTS) + " |")


if any((f"idle-{n}", s) in runs for n in ("1k", "10k") for s in SUTS):
    def per_conn(s):
        a, b = med("idle-1k", s, "peak_server_rss_mb"), med("idle-10k", s, "peak_server_rss_mb")
        return None if a is None or b is None else (b - a) * 1024 / 9000

    row("Idle connections", "RSS with 1,000 clients (MB)", lambda s: fmt(med("idle-1k", s, "peak_server_rss_mb")))
    row("", "RSS with 10,000 clients (MB)", lambda s: fmt(med("idle-10k", s, "peak_server_rss_mb")))
    row("", "Memory per extra connection (KB)", lambda s: fmt(per_conn(s)))
    row("", "OS threads with 10,000 clients", lambda s: fmt(med("idle-10k", s, "peak_server_threads"), 0))

any_marked = False
for w in workloads:
    if w.startswith("idle"):
        continue
    keyset = {k for s in SUTS for r in runs.get((w, s), []) for k in r}
    rows = []
    if "throughput_msgs_per_sec" in keyset:
        rows += [
            ("Delivered msg/s (expected)", lambda s: f"{fmt(med(w, s, 'throughput_msgs_per_sec'), 0)} ({fmt(med(w, s, 'expected_msgs_per_sec'), 0)})"
             if runs.get((w, s)) else "–"),
            ("Sustained the load", lambda s: delivery(w, s)),
            ("Latency p50 / p99 (ms)", lambda s: marked(w, s, f"{fmt(med(w, s, 'latency_p50_ms'), 2)} / {fmt(med(w, s, 'latency_p99_ms'), 2)}")),
            ("Server CPU (cores)", lambda s: marked(w, s, fmt(med(w, s, "server_cpu_cores"), 2))),
            ("Server peak RSS (MB)", lambda s: fmt(med(w, s, "peak_server_rss_mb"))),
            ("Missing / duplicate / out-of-order (all runs)", lambda s: " / ".join(
                fmt(total(w, s, k), 0) for k in ("messages_missing_ids", "duplicates", "ordering_violations"))
             if runs.get((w, s)) else "–"),
        ]
        any_marked |= any(delivery(w, s).startswith("no") for s in SUTS)
    elif "recovered" in keyset:
        rows.append(("Recovered after every wave (runs)", lambda s: (
            f"{sum(r.get('recovered') == 'true' for r in runs[(w, s)])} of {len(runs[(w, s)])}"
            if runs.get((w, s)) else "–")))
        for k in sorted(keyset):
            if any(x in k for x in ("seconds", "_ms", "peak_server", "cpu_cores", "failed", "lost", "healthy")):
                if k.endswith("_ms") and k.split("_")[0] + "_count" in keyset:
                    # A histogram with no samples (e.g. probes the server does not answer) is unknown.
                    def cell(s, k=k):
                        c = med(w, s, k.split("_")[0] + "_count")
                        return "–" if not c else fmt(med(w, s, k), 2)
                    rows.append((k, cell))
                else:
                    rows.append((k, lambda s, k=k: fmt(med(w, s, k), 2)))
    first = True
    for label, f in rows:
        row(w if first else "", label, f)
        first = False

notes = []
if any_marked:
    notes.append(f"{MARK} The server did not sustain this load (see “Sustained the load”): "
                 "latency covers only the messages it delivered, and CPU is not at the expected rate.")
for (w, s), reasons in sorted(failed.items()):
    notes.append(f"{LABELS.get(s, s)} failed {len(reasons)} run(s) of {w}: {reasons[0]}")
for (w, s), n in sorted(excluded.items()):
    notes.append(f"Excluded {n} run(s) of {w} on {LABELS.get(s, s)}: the load generator was saturated.")
if notes:
    print()
    for n in notes:
        print(f"- {n}")
