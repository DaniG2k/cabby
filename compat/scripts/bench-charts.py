#!/usr/bin/env python3
"""Render the README benchmark charts (SVG, light and dark) from bench-compare.sh results.

Usage: compat/scripts/bench-charts.py [RESULTS_DIR] [OUT_DIR]
       (defaults: docs/benchmarks/results, docs/benchmarks)
Medians of the runs that were not generator-saturated, as in bench-summary.py. One series per server
found in the results (Action Cable, AnyCable, Cabby). A server that did not sustain a workload (missing
messages, dropped publications, failed POSTs) gets no bar there but a "not sustained" label: its latency
and CPU would describe a server that fell behind, not the same load.
"""
import statistics
import sys
from pathlib import Path

root = Path(__file__).resolve().parents[2]
results = Path(sys.argv[1]) if len(sys.argv) > 1 else root / "docs/benchmarks/results"
out_dir = Path(sys.argv[2]) if len(sys.argv) > 2 else root / "docs/benchmarks"


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


ALL_SERIES = [("actioncable", "Action Cable (Ruby)"), ("anycable-go", "AnyCable (Go)"), ("cabby", "Cabby (Rust)")]
SERIES = [(k, n) for k, n in ALL_SERIES if any(results.glob(f"*.{k}.*.txt"))] or ALL_SERIES[1:]
# Categorical slots aqua, blue, orange; each server keeps its colour whichever servers are shown.
STYLE = """
  .surface { fill: #fcfcfb; } .actioncable { fill: #1baf7a; } .anycable-go { fill: #2a78d6; } .cabby { fill: #eb6834; }
  .t1 { fill: #0b0b0b; } .t2 { fill: #52514e; } .grid { stroke: #e4e3df; } .axis { stroke: #b9b8b2; }
  @media (prefers-color-scheme: dark) {
    .surface { fill: #1a1a19; } .actioncable { fill: #199e70; } .anycable-go { fill: #3987e5; } .cabby { fill: #d95926; }
    .t1 { fill: #f0efec; } .t2 { fill: #c3c2b7; } .grid { stroke: #383835; } .axis { stroke: #5e5d58; }
  }
  text { font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Helvetica, Arial, sans-serif; }
"""


def nice_interval(v):
    """A round tick interval (1, 2 or 5 x 10^n) so that four of them cover v with headroom."""
    for exp in range(-2, 7):
        for step in (1, 2, 5):
            if step * 10**exp * 4 >= v * 1.08:
                return step * 10**exp
    return v / 4


def chart(path, title, subtitle, groups, values, unit, digits=0):
    """groups: category labels; values[series_index][group_index]: a number, NOT_SUSTAINED, FAILED or None."""
    w, h = 720, 340
    left, right, top, bottom = 64, 24, 92, 48
    pw, ph = w - left - right, h - top - bottom
    nums = [v for vs in values for v in vs if isinstance(v, (int, float))]
    interval = nice_interval(max(nums, default=1) or 1)
    vmax = interval * 4
    ticks = [interval * i for i in range(5)]
    gw = pw / len(groups)
    n = len(SERIES)
    bw = min(56 if n <= 2 else 48, gw * 0.6 / n)
    gap = 2
    y = lambda v: top + ph - ph * v / vmax  # noqa: E731
    fmt = lambda v: "no data" if v is None else v if isinstance(v, str) else f"{v:,.{digits}f}"  # noqa: E731
    unit_of = lambda v: f" {unit}" if isinstance(v, (int, float)) else ""  # noqa: E731

    s = [f'<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 {w} {h}" width="{w}" height="{h}" role="img" aria-labelledby="t d">']
    s.append(f"<title id=\"t\">{title}</title><desc id=\"d\">{subtitle}. " + "; ".join(
        f"{g}: " + ", ".join(f"{name} {fmt(values[i][j])}{unit_of(values[i][j])}" for i, (_, name) in enumerate(SERIES))
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
            s.append(f'<text class="t1" x="{x + bw / 2:.1f}" y="{top_y - 6:.1f}" font-size="11" text-anchor="middle">{fmt(v)}</text>')
        s.append(f'<text class="t2" x="{cx:.1f}" y="{top + ph + 20}" font-size="12" text-anchor="middle">{g}</text>')
    s.append("</svg>\n")
    path.write_text("\n".join(s))


out_dir.mkdir(parents=True, exist_ok=True)
suts = [k for k, _ in SERIES]
chart(out_dir / "memory.svg", "Server memory (RSS)", "Peak resident memory; lower is better",
      ["1,000 idle clients", "10,000 idle clients", "5,000-client room, 100k msg/s"],
      [[med("idle-1k", s, "peak_server_rss_mb"), med("idle-10k", s, "peak_server_rss_mb"), med("room-5k", s, "peak_server_rss_mb")] for s in suts],
      "MB")
fan = [("fanout-50k", "50k msg/s"), ("fanout-100k", "100k msg/s"), ("fanout-150k", "150k msg/s")]
chart(out_dir / "latency.svg", "Broadcast delivery latency, p99", "Publish to client delivery, 1,000 clients; lower is better",
      [g for _, g in fan], [[med(w, s, "latency_p99_ms") for w, _ in fan] for s in suts], "ms", digits=2)
chart(out_dir / "cpu.svg", "Server CPU at the same delivered rate", "Average cores used while delivering the load; lower is better",
      [g for _, g in fan], [[med(w, s, "server_cpu_cores") for w, _ in fan] for s in suts], "cores", digits=2)
tput = fan + [("room-5k", "5,000-client room, 100k msg/s")]
chart(out_dir / "throughput.svg", "Messages delivered per second",
      "What each server delivered of the offered load (50k, 100k, 150k, 100k msg/s); higher is better",
      [g for _, g in tput],
      [[(lambda v: None if v is None else v / 1000)(med_raw(w, s, "throughput_msgs_per_sec")) for w, _ in tput] for s in suts],
      "thousand msg/s", digits=1)
print("charts written to", out_dir)
