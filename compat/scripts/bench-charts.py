#!/usr/bin/env python3
"""Render the README benchmark charts (SVG, light and dark) from bench-compare.sh results.

Usage: compat/scripts/bench-charts.py [RESULTS_DIR] [OUT_DIR]
       (defaults: docs/benchmarks/results, docs/benchmarks)
Medians of the runs that were not generator-saturated, as in bench-summary.py.
"""
import statistics
import sys
from pathlib import Path

root = Path(__file__).resolve().parents[2]
results = Path(sys.argv[1]) if len(sys.argv) > 1 else root / "docs/benchmarks/results"
out_dir = Path(sys.argv[2]) if len(sys.argv) > 2 else root / "docs/benchmarks"


def med(workload, sut, key):
    vals = []
    for f in results.glob(f"{workload}.{sut}.*.txt"):
        kv = dict(line.split("=", 1) for line in f.read_text().splitlines() if "=" in line)
        if kv.get("generator_saturated") == "true" or "exit" in kv:
            continue
        if key in kv:
            vals.append(float(kv[key]))
    return statistics.median(vals) if vals else 0.0


SERIES = [("anycable-go", "AnyCable (Go)"), ("cabby", "Cabby (Rust)")]
STYLE = """
  .surface { fill: #fcfcfb; } .s0 { fill: #2a78d6; } .s1 { fill: #eb6834; }
  .t1 { fill: #0b0b0b; } .t2 { fill: #52514e; } .grid { stroke: #e4e3df; } .axis { stroke: #b9b8b2; }
  @media (prefers-color-scheme: dark) {
    .surface { fill: #1a1a19; } .s0 { fill: #3987e5; } .s1 { fill: #d95926; }
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
    """groups: category labels; values[series_index][group_index]."""
    w, h = 720, 340
    left, right, top, bottom = 64, 24, 92, 48
    pw, ph = w - left - right, h - top - bottom
    interval = nice_interval(max(max(v) for v in values))
    vmax = interval * 4
    ticks = [interval * i for i in range(5)]
    gw = pw / len(groups)
    bw = min(56, gw * 0.3)
    gap = 2
    y = lambda v: top + ph - ph * v / vmax  # noqa: E731
    fmt = lambda v: f"{v:,.{digits}f}"  # noqa: E731

    s = [f'<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 {w} {h}" width="{w}" height="{h}" role="img" aria-labelledby="t d">']
    s.append(f"<title id=\"t\">{title}</title><desc id=\"d\">{subtitle}. " + "; ".join(
        f"{g}: " + ", ".join(f"{name} {fmt(values[i][j])} {unit}" for i, (_, name) in enumerate(SERIES))
        for j, g in enumerate(groups)) + "</desc>")
    s.append(f"<style>{STYLE}</style>")
    s.append(f'<rect class="surface" width="{w}" height="{h}" rx="8"/>')
    s.append(f'<text class="t1" x="{left}" y="30" font-size="16" font-weight="600">{title}</text>')
    s.append(f'<text class="t2" x="{left}" y="50" font-size="12">{subtitle}</text>')
    # legend
    lx = left
    for i, (_, name) in enumerate(SERIES):
        s.append(f'<rect class="s{i}" x="{lx}" y="64" width="12" height="12" rx="3"/>')
        s.append(f'<text class="t2" x="{lx + 18}" y="74" font-size="12">{name}</text>')
        lx += 18 + 8 * len(name) + 24
    for t in ticks:
        ty = y(t)
        s.append(f'<line class="{"axis" if t == 0 else "grid"}" x1="{left}" x2="{w - right}" y1="{ty:.1f}" y2="{ty:.1f}" stroke-width="1"/>')
        s.append(f'<text class="t2" x="{left - 8}" y="{ty + 4:.1f}" font-size="11" text-anchor="end">{t:,.{0 if interval >= 1 else 1}f}</text>')
    s.append(f'<text class="t2" x="14" y="{top + ph / 2:.0f}" font-size="11" transform="rotate(-90 14 {top + ph / 2:.0f})" text-anchor="middle">{unit}</text>')
    for j, g in enumerate(groups):
        cx = left + gw * j + gw / 2
        for i in range(len(SERIES)):
            v = values[i][j]
            x = cx - bw - gap / 2 + i * (bw + gap)
            top_y, base = y(v), y(0)
            hgt = max(base - top_y, 1)
            r = min(4, hgt)
            # rounded data end, square at the baseline
            s.append(f'<path class="s{i}" d="M{x:.1f},{base:.1f} V{top_y + r:.1f} Q{x:.1f},{top_y:.1f} {x + r:.1f},{top_y:.1f} '
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
print("charts written to", out_dir)
