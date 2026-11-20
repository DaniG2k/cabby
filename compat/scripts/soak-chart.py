#!/usr/bin/env python3
"""Render the soak memory chart (one SVG, light and dark) from soak-compare.sh results.

Usage: compat/scripts/soak-chart.py [SOAK_DIR] [OUT_SVG]
       (defaults: docs/benchmarks/soak, docs/benchmarks/soak-memory.svg)
Reads <server>.csv (cabby-bench soak --samples-out) and, when present, <server>.txt (its summary)
for AnyCable (Go) and Cabby (Rust). One 2px line per server: server RSS over the run, from the
baseline sample before the first client to the moment the clients stop. The end labels give the
final RSS, the least-squares slope after the warm-up (rss_slope_mb_per_hour) and the residual RSS
once the clients have left. Colours and type follow compat/scripts/bench-charts.py.
"""
import csv
import sys
from pathlib import Path

root = Path(__file__).resolve().parents[2]
args = [a for a in sys.argv[1:] if not a.startswith("--")]
soak = Path(args[0]) if args else root / "docs/benchmarks/soak"
out = Path(args[1]) if len(args) > 1 else root / "docs/benchmarks/soak-memory.svg"

SERIES = [("anycable-go", "AnyCable (Go)"), ("cabby", "Cabby (Rust)")]
STYLE = """
  .surface { fill: #fcfcfb; } .anycable-go { stroke: #2a78d6; fill: #2a78d6; } .cabby { stroke: #eb6834; fill: #eb6834; }
  .t1 { fill: #0b0b0b; } .t2 { fill: #52514e; } .grid { stroke: #e4e3df; } .axis { stroke: #b9b8b2; }
  .mark { stroke: #b9b8b2; }
  @media (prefers-color-scheme: dark) {
    .surface { fill: #1a1a19; } .anycable-go { stroke: #3987e5; fill: #3987e5; } .cabby { stroke: #d95926; fill: #d95926; }
    .t1 { fill: #f0efec; } .t2 { fill: #c3c2b7; } .grid { stroke: #383835; } .axis { stroke: #5e5d58; }
    .mark { stroke: #5e5d58; }
  }
  text { font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Helvetica, Arial, sans-serif; }
  .line { fill: none; stroke-width: 2; stroke-linejoin: round; stroke-linecap: round; }
"""


def nice_interval(v, n=4):
    """A round tick interval (1, 2 or 5 x 10^n) so that n of them cover v with headroom."""
    for exp in range(-2, 7):
        for step in (1, 2, 5):
            if step * 10**exp * n >= v * 1.08:
                return step * 10**exp
    return v / n


def load(key):
    path = soak / f"{key}.csv"
    if not path.exists():
        return None
    with path.open() as f:
        rows = list(csv.DictReader(f))
    line = [(float(r["elapsed_s"]), float(r["rss_mb"])) for r in rows if r["phase"] != "residual"]
    summary = {}
    txt = soak / f"{key}.txt"
    if txt.exists():
        summary = dict(l.split("=", 1) for l in txt.read_text().splitlines() if "=" in l)
    residual = next((float(r["rss_mb"]) for r in rows if r["phase"] == "residual"), None)
    return {"line": line, "summary": summary, "residual": residual}


data = [(k, n, d) for k, n in SERIES if (d := load(k)) and d["line"]]
if not data:
    sys.exit(f"no soak samples in {soak}")

w, h = 720, 360
left, right, top, bottom = 64, 190, 92, 52
pw, ph = w - left - right, h - top - bottom
t_max = max(p[0] for _, _, d in data for p in d["line"]) or 1
v_max = max(p[1] for _, _, d in data for p in d["line"]) or 1
hours = t_max > 3 * 3600
unit_s, unit = (3600, "h") if hours else (60, "min")
x_interval = nice_interval(t_max / unit_s, 6)
x_ticks = [x_interval * i for i in range(int(t_max / unit_s / x_interval) + 1)]
y_interval = nice_interval(v_max)
y_ticks = [y_interval * i for i in range(5)]
x = lambda t: left + pw * t / t_max  # noqa: E731
y = lambda v: top + ph - ph * v / (y_interval * 4)  # noqa: E731
first = data[0][2]["summary"]
warmup = float(first.get("warmup_seconds", 0) or 0)

title = "Server memory (RSS) during a soak run"
sub = (f"{first.get('clients', '?')} clients connecting, subscribing and leaving over RPC, "
       f"{first.get('publish_rate', '?')} broadcasts/s; lower and flatter is better")


def fmt_slope(s):
    try:
        v = float(s)
    except (TypeError, ValueError):
        return None
    return f"{v:+.1f} MB/h"


desc = []
for key, name, d in data:
    s = d["summary"]
    parts = [f"{name}: {d['line'][-1][1]:.1f} MB at the end"]
    if fmt_slope(s.get("rss_slope_mb_per_hour")):
        parts.append(f"slope after warm-up {fmt_slope(s['rss_slope_mb_per_hour'])}")
    if d["residual"] is not None:
        parts.append(f"{d['residual']:.1f} MB after the clients left")
    desc.append(", ".join(parts))

s = [f'<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 {w} {h}" width="{w}" height="{h}" role="img" aria-labelledby="t d">']
s.append(f'<title id="t">{title}</title><desc id="d">{sub}. ' + "; ".join(desc) + ".</desc>")
s.append(f"<style>{STYLE}</style>")
s.append(f'<rect class="surface" width="{w}" height="{h}" rx="8"/>')
s.append(f'<text class="t1" x="{left}" y="30" font-size="16" font-weight="600">{title}</text>')
s.append(f'<text class="t2" x="{left}" y="50" font-size="12">{sub}</text>')
lx = left
for key, name, _ in data:
    s.append(f'<line class="{key} line" x1="{lx}" x2="{lx + 16}" y1="70" y2="70"/>')
    s.append(f'<text class="t2" x="{lx + 22}" y="74" font-size="12">{name}</text>')
    lx += 22 + 8 * len(name) + 24
for t in y_ticks:
    ty = y(t)
    s.append(f'<line class="{"axis" if t == 0 else "grid"}" x1="{left}" x2="{left + pw}" y1="{ty:.1f}" y2="{ty:.1f}" stroke-width="1"/>')
    s.append(f'<text class="t2" x="{left - 8}" y="{ty + 4:.1f}" font-size="11" text-anchor="end">{t:,.{0 if y_interval >= 1 else 1}f}</text>')
s.append(f'<text class="t2" x="14" y="{top + ph / 2:.0f}" font-size="11" transform="rotate(-90 14 {top + ph / 2:.0f})" text-anchor="middle">RSS (MB)</text>')
for t in x_ticks:
    tx = x(t * unit_s)
    s.append(f'<text class="t2" x="{tx:.1f}" y="{top + ph + 18}" font-size="11" text-anchor="middle">{t:g}</text>')
s.append(f'<text class="t2" x="{left + pw / 2:.0f}" y="{top + ph + 38}" font-size="11" text-anchor="middle">elapsed ({unit})</text>')
if 0 < warmup < t_max:
    wx = x(warmup)
    s.append(f'<line class="mark" x1="{wx:.1f}" x2="{wx:.1f}" y1="{top}" y2="{top + ph}" stroke-width="1" stroke-dasharray="3 3"/>')
    s.append(f'<text class="t2" x="{wx + 4:.1f}" y="{top + 10}" font-size="10">end of warm-up</text>')

# Lines, then end labels nudged apart so they never overlap.
labels = []
for key, name, d in data:
    pts = " ".join(f"{x(t):.1f},{y(v):.1f}" for t, v in d["line"])
    s.append(f'<polyline class="{key} line" points="{pts}"/>')
    et, ev = d["line"][-1]
    s.append(f'<circle class="{key}" cx="{x(et):.1f}" cy="{y(ev):.1f}" r="4"/>')
    lines = [f"{name.split(' ')[0]} {ev:.1f} MB"]
    slope = fmt_slope(d["summary"].get("rss_slope_mb_per_hour"))
    extra = [p for p in (slope, d["residual"] is not None and f"residual {d['residual']:.1f} MB") if p]
    labels.append([y(ev), lines + [", ".join(extra)] if extra else lines])
labels.sort(key=lambda l: l[0])
for i in range(1, len(labels)):
    min_gap = 16 * len(labels[i - 1][1]) + 4
    labels[i][0] = max(labels[i][0], labels[i - 1][0] + min_gap)
for ly, lines in labels:
    for k, text in enumerate(lines):
        cls, size, weight = ("t1", 12, ' font-weight="600"') if k == 0 else ("t2", 11, "")
        s.append(f'<text class="{cls}" x="{left + pw + 10}" y="{ly + 4 + 15 * k:.1f}" font-size="{size}"{weight}>{text}</text>')
s.append("</svg>\n")
out.parent.mkdir(parents=True, exist_ok=True)
out.write_text("\n".join(s))
print("chart written to", out)
