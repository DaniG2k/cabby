#!/usr/bin/env python3
"""Render the README benchmark charts (SVG, light and dark) from bench-compare.sh results.

Usage: compat/scripts/bench-charts.py [RESULTS_DIR] [OUT_DIR] [--protocol=pusher]
       (defaults: docs/benchmarks/results, docs/benchmarks)
With --protocol=pusher (results of bench-compare-pusher.sh, default docs/benchmarks/pusher-results)
the charts are named pusher-*.svg and their titles say so.
Medians of the runs that were not generator-saturated, as in bench-summary.py. One series per server
found in the results (Action Cable, Soketi, Reverb, AnyCable, Cabby). A server that did not sustain a workload (missing
messages, dropped publications, failed POSTs) gets no bar there but a "not sustained" label: its latency
and CPU would describe a server that fell behind, not the same load.
"""
import statistics
import sys
from pathlib import Path

root = Path(__file__).resolve().parents[2]
pusher = "--protocol=pusher" in sys.argv
args = [a for a in sys.argv[1:] if not a.startswith("--")]
results = Path(args[0]) if args else root / ("docs/benchmarks/pusher-results" if pusher else "docs/benchmarks/results")
out_dir = Path(args[1]) if len(args) > 1 else root / "docs/benchmarks"
PREFIX = "pusher-" if pusher else ""
TITLE_SUFFIX = ", Pusher protocol" if pusher else ""


NOT_SUSTAINED = "not sustained"
FAILED = "failed"


def sustained(kv):
    """As in bench-summary.py: everything delivered, at 95% of the expected rate or more."""
    exp = float(kv.get("expected_msgs_per_sec", 0) or 0)
    return all(float(kv.get(k, 0) or 0) == 0 for k in (
        "messages_missing_ids", "publications_dropped", "publications_failed_posts", "clients_short")) \
        and (not exp or float(kv.get("throughput_msgs_per_sec", 0)) >= 0.95 * exp)


def med(workload, sut, key):
    """The median of `key` over the usable runs; NOT_SUSTAINED when any of them fell behind, FAILED
    when runs printed no summary and none succeeded, None without data."""
    vals, behind, failed = [], False, False
    for f in results.glob(f"{workload}.{sut}.*.txt"):
        kv = dict(line.split("=", 1) for line in f.read_text().splitlines() if "=" in line)
        failed |= "generator_saturated" not in kv
        if kv.get("generator_saturated") != "false":
            continue  # generator saturated, or no summary (failed run)
        behind |= not sustained(kv)
        if key in kv:
            vals.append(float(kv[key]))
    if behind:
        return NOT_SUSTAINED
    return statistics.median(vals) if vals else FAILED if failed else None


def med_raw(workload, sut, key):
    """The median of `key` over every run with a summary and an unsaturated generator, whether the
    server kept up or not (for throughput, where falling behind is the point)."""
    vals = []
    for f in results.glob(f"{workload}.{sut}.*.txt"):
        kv = dict(line.split("=", 1) for line in f.read_text().splitlines() if "=" in line)
        if kv.get("generator_saturated") == "false" and key in kv:
            vals.append(float(kv[key]))
    return statistics.median(vals) if vals else None


# The order keeps amber (Soketi) away from rose (Cabby).
ALL_SERIES = [("actioncable", "Action Cable (Ruby)"), ("soketi", "Soketi (Node.js)"), ("reverb", "Reverb (PHP)"),
              ("anycable-go", "AnyCable (Go)"), ("cabby", "Cabby (Rust)")]
SERIES = [(k, n) for k, n in ALL_SERIES if any(results.glob(f"*.{k}.*.txt"))] or \
    [s for s in ALL_SERIES if s[0] in ("anycable-go", "cabby")]
# Tailwind CSS colors; each server keeps its color whichever servers are shown (emerald, amber, violet,
# sky, rose), on white and slate-900, with slate text. Neighbouring bars stay distinguishable with
# color-blind vision, and every bar carries its value.
STYLE = """
  .surface { fill: #ffffff; } .actioncable { fill: #059669; } .anycable-go { fill: #0284c7; } .cabby { fill: #f43f5e; }
  .soketi { fill: #d97706; } .reverb { fill: #7c3aed; }
  .t1 { fill: #0f172a; } .t2 { fill: #475569; } .grid { stroke: #e2e8f0; } .axis { stroke: #94a3b8; }
  @media (prefers-color-scheme: dark) {
    .surface { fill: #0f172a; } .actioncable { fill: #059669; } .anycable-go { fill: #0ea5e9; } .cabby { fill: #f43f5e; }
    .soketi { fill: #d97706; } .reverb { fill: #8b5cf6; }
    .t1 { fill: #f1f5f9; } .t2 { fill: #cbd5e1; } .grid { stroke: #334155; } .axis { stroke: #64748b; }
  }
  text { font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Helvetica, Arial, sans-serif; }
"""


def nice_scale(v):
    """(tick interval, number of intervals): the lowest axis that covers v with headroom for its label,
    in four or five steps of 1, 2, 3 or 5 x 10^n."""
    return min(((step * 10**exp, n) for exp in range(-2, 7) for step in (1, 2, 3, 5) for n in (4, 5)
                if step * 10**exp * n >= v * 1.08), key=lambda t: (t[0] * t[1], t[1]))


def chart(path, title, subtitle, groups, values, unit, digits=0, series=None, labels=None):
    """groups: category labels; values[series_index][group_index]: a number, NOT_SUSTAINED, FAILED or None.
    series: the (key, name) pairs to draw (default: all); labels[series_index]: text to show on that bar."""
    global SERIES
    saved, SERIES = SERIES, series or SERIES
    w, h = 720, 340
    left, right, top, bottom = 64, 24, 92, 48
    pw, ph = w - left - right, h - top - bottom
    nums = [v for vs in values for v in vs if isinstance(v, (int, float))]
    interval, steps = nice_scale(max(nums, default=1) or 1)
    vmax = interval * steps
    ticks = [interval * i for i in range(steps + 1)]
    gw = pw / len(groups)
    n = len(SERIES)
    bw = min(56 if n <= 2 else 48, gw * 0.6 / n)
    gap = 2
    label_size = 11 if n <= 3 else 10
    y = lambda v: top + ph - ph * v / vmax  # noqa: E731
    fmt = lambda v: "no data" if v is None else v if isinstance(v, str) else f"{v:,.{digits}f}"  # noqa: E731
    unit_of = lambda v: f" {unit}" if isinstance(v, (int, float)) else ""  # noqa: E731

    s = [f'<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 {w} {h}" width="{w}" height="{h}" role="img" aria-labelledby="t d">']
    s.append(f"<title id=\"t\">{title}</title><desc id=\"d\">{subtitle}. " + "; ".join(
        f"{g}: " + ", ".join(f"{name} {(labels or {}).get(i, fmt(values[i][j]))}{unit_of(values[i][j])}" for i, (_, name) in enumerate(SERIES))
        for j, g in enumerate(groups)) + "</desc>")
    s.append(f"<style>{STYLE}</style>")
    s.append(f'<rect class="surface" width="{w}" height="{h}" rx="8"/>')
    s.append(f'<text class="t1" x="{left}" y="30" font-size="16" font-weight="600">{title}</text>')
    s.append(f'<text class="t2" x="{left}" y="50" font-size="12">{subtitle}</text>')
    # legend
    lx = left
    for key, name in SERIES:
        s.append(f'<rect class="{key}" x="{lx}" y="64" width="12" height="12" rx="3"/>')
        s.append(f'<text class="t2" x="{lx + 18}" y="74" font-size="12">{name}</text>')
        lx += 18 + 8 * len(name) + 24
    for t in ticks:
        ty = y(t)
        s.append(f'<line class="{"axis" if t == 0 else "grid"}" x1="{left}" x2="{w - right}" y1="{ty:.1f}" y2="{ty:.1f}" stroke-width="1"/>')
        s.append(f'<text class="t2" x="{left - 8}" y="{ty + 4:.1f}" font-size="11" text-anchor="end">{t:,.{0 if interval >= 1 else 1}f}</text>')
    s.append(f'<text class="t2" x="14" y="{top + ph / 2:.0f}" font-size="11" transform="rotate(-90 14 {top + ph / 2:.0f})" text-anchor="middle">{unit}</text>')
    for j, g in enumerate(groups):
        cx = left + gw * j + gw / 2
        for i, (key, _) in enumerate(SERIES):
            v = values[i][j]
            x = cx - (n * bw + (n - 1) * gap) / 2 + i * (bw + gap)
            if not isinstance(v, (int, float)):
                # No bar: say why in the bar's place, above the baseline.
                words = {NOT_SUSTAINED: ["not", "sustained"], FAILED: ["run", "failed"]}.get(v, ["no", "data"])
                if n > 3:
                    # Narrow bars: one vertical line, so neighbours' notes do not collide.
                    tx, ty = x + bw / 2 + 3, y(0) - 6
                    s.append(f'<text class="t2" x="{tx:.1f}" y="{ty:.1f}" font-size="10" '
                             f'transform="rotate(-90 {tx:.1f} {ty:.1f})">{" ".join(words)}</text>')
                    continue
                for k, word in enumerate(words):
                    s.append(f'<text class="t2" x="{x + bw / 2:.1f}" y="{y(0) - 22 + 12 * k:.1f}" font-size="10" '
                             f'text-anchor="middle">{word}</text>')
                continue
            top_y, base = y(v), y(0)
            hgt = max(base - top_y, 1)
            r = min(4, hgt)
            # rounded data end, square at the baseline
            s.append(f'<path class="{key}" d="M{x:.1f},{base:.1f} V{top_y + r:.1f} Q{x:.1f},{top_y:.1f} {x + r:.1f},{top_y:.1f} '
                     f'H{x + bw - r:.1f} Q{x + bw:.1f},{top_y:.1f} {x + bw:.1f},{top_y + r:.1f} V{base:.1f} Z"/>')
            s.append(f'<text class="t1" x="{x + bw / 2:.1f}" y="{top_y - 6:.1f}" font-size="{label_size}" text-anchor="middle">{(labels or {}).get(i, fmt(v))}</text>')
        for k, line in enumerate(g.split(", ")):
            s.append(f'<text class="t2" x="{cx:.1f}" y="{top + ph + 19 + 15 * k}" font-size="12" text-anchor="middle">{line}</text>')
    s.append("</svg>\n")
    path.write_text("\n".join(s))
    SERIES = saved


out_dir.mkdir(parents=True, exist_ok=True)
suts = [k for k, _ in SERIES]
memory_groups = [("10,000 idle clients", "idle-10k"), ("5,000-client room, 100k msg/s", "room-5k"),
                 ("1,000 idle clients", "idle-1k") if pusher else ("5,000-client room, 32 KiB messages", "room-5k-32k")]
if pusher:
    memory_groups.insert(0, memory_groups.pop())
chart(out_dir / f"{PREFIX}memory.svg", f"Server memory (RSS){TITLE_SUFFIX}", "Peak resident memory; lower is better",
      [g for g, _ in memory_groups], [[med(w, s, "peak_server_rss_mb") for _, w in memory_groups] for s in suts], "MB")
ceiling_dir = root / "docs/benchmarks/ceiling-results"
if not pusher and ceiling_dir.is_dir():
    # The highest rate one core holds (bench-summary.py's rule): p99 at most 50 ms, nothing lost.
    ladder = sorted({int(f.name.split(".")[0][len("core1-"):-1]) for f in ceiling_dir.glob("core1-*.txt")})
    def held(sut):
        best = 0
        for rate in ladder:
            rs = [dict(l.split("=", 1) for l in f.read_text().splitlines() if "=" in l)
                  for f in ceiling_dir.glob(f"core1-{rate}k.{sut}.*.txt")]
            ok = len(rs) >= 3 and all(r.get("generator_saturated") == "false" and sustained(r) for r in rs)
            if not ok or statistics.median(float(r["latency_p99_ms"]) for r in rs) > 50:
                return best, False
            best = rate
        return best, True
    two = [(k, n) for k, n in SERIES if k in ("anycable-go", "cabby")]
    pairs = [held(k) for k, _ in two]
    chart(out_dir / "ceiling.svg", "Highest load one core can hold", "p99 at most 50 ms and no message lost, 1,000 clients; higher is better",
          ["one core, untuned settings"], [[v] for v, _ in pairs], "thousand msg/s", series=two,
          labels={i: (f"at least {v:,}" if top else f"{v:,}") for i, (v, top) in enumerate(pairs)})
fan = [("fanout-50k", "50k msg/s"), ("fanout-100k", "100k msg/s"), ("fanout-150k", "150k msg/s")]
chart(out_dir / f"{PREFIX}latency.svg", f"Broadcast delivery latency, p99{TITLE_SUFFIX}", "Publish to client delivery, 1,000 clients; lower is better",
      [g for _, g in fan], [[med(w, s, "latency_p99_ms") for w, _ in fan] for s in suts], "ms", digits=2)
chart(out_dir / f"{PREFIX}cpu.svg", f"Server CPU at the same delivered rate{TITLE_SUFFIX}", "Average cores used while delivering the load; lower is better",
      [g for _, g in fan], [[med(w, s, "server_cpu_cores") for w, _ in fan] for s in suts], "cores", digits=2)
tput = fan + [("room-5k", "5,000-client room, 100k msg/s")]
chart(out_dir / f"{PREFIX}throughput.svg", f"Messages delivered per second{TITLE_SUFFIX}",
      "What each server delivered of the offered load (50k, 100k, 150k, 100k msg/s); higher is better",
      [g for _, g in tput],
      [[(lambda v: None if v is None else v / 1000)(med_raw(w, s, "throughput_msgs_per_sec")) for w, _ in tput] for s in suts],
      "thousand msg/s", digits=0 if len(SERIES) >= 4 else 1)
print("charts written to", out_dir)
