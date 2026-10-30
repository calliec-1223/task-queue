"""Draw benchmark charts from the newest results file.

Usage:
    python -m bench.plot      # newest results for each experiment in bench/results/

Writes PNGs to docs/ for the README.
"""

import json
from pathlib import Path

import matplotlib

matplotlib.use("Agg")  # draw to files, no window
import matplotlib.pyplot as plt  # noqa: E402

ROOT = Path(__file__).resolve().parent.parent
DOCS = ROOT / "docs"

# Colors and styling follow one validated palette: categorical slots in fixed
# order, text in neutral ink (never the series color), recessive grid.
SURFACE = "#fcfcfb"
TEXT = "#0b0b0b"
TEXT_2 = "#52514e"
MUTED = "#7a7974"
GRID = "#ebe9e3"
SERIES_1 = "#2a78d6"  # blue
SERIES_2 = "#eb6834"  # orange
SERIES_3 = "#1baf7a"  # aqua


def style(ax, title: str, xlabel: str, ylabel: str) -> None:
    ax.set_facecolor(SURFACE)
    ax.set_title(title, loc="left", fontsize=13, color=TEXT, pad=14, fontweight="semibold")
    ax.set_xlabel(xlabel, color=TEXT_2, fontsize=10)
    ax.set_ylabel(ylabel, color=TEXT_2, fontsize=10)
    ax.grid(axis="y", color=GRID, linewidth=1)
    ax.set_axisbelow(True)
    for side in ("top", "right", "left"):
        ax.spines[side].set_visible(False)
    ax.spines["bottom"].set_color(GRID)
    ax.tick_params(colors=MUTED, labelsize=9, length=0)


def label_end(ax, x, y, text, dy=0):
    """Direct label at a line's last point, in text color (not series color)."""
    ax.annotate(text, (x, y), xytext=(8, dy), textcoords="offset points",
                va="center", fontsize=9, color=TEXT_2)


def throughput_chart(data: dict) -> Path:
    rows = data["throughput"]
    w = [r["workers"] for r in rows]
    fig, ax = plt.subplots(figsize=(7.5, 4), facecolor=SURFACE)
    ax.plot(w, [r["ideal_jobs_per_second"] for r in rows], color=MUTED, linewidth=1.5,
            linestyle=(0, (4, 3)), label="Ideal (workers ÷ task time)")
    ax.plot(w, [r["jobs_per_second"] for r in rows], color=SERIES_1, linewidth=2,
            marker="o", markersize=7, markeredgecolor=SURFACE, markeredgewidth=2,
            label="Measured")
    for r in rows:  # selective labels: the measured value at each worker count
        ax.annotate(f"{r['jobs_per_second']:.0f}", (r["workers"], r["jobs_per_second"]),
                    xytext=(0, 10), textcoords="offset points", ha="center",
                    fontsize=9, color=TEXT_2)
    ax.set_xscale("log", base=2)
    ax.set_xticks(w, [str(x) for x in w])
    ax.set_ylim(bottom=0)
    style(ax, f"Throughput: {rows[0]['jobs']:,} jobs of {data['task_seconds'] * 1000:.0f} ms each",
          "Workers", "Jobs completed per second")
    ax.legend(frameon=False, fontsize=9, labelcolor=TEXT_2, loc="upper left")
    fig.tight_layout()
    out = DOCS / "bench-throughput.png"
    fig.savefig(out, dpi=160)
    plt.close(fig)
    return out


def latency_chart(data: dict) -> Path:
    rows = data["latency"]
    names = {"fixed-500ms": "Poll every 500 ms", "fixed-50ms": "Poll every 50 ms",
             "adaptive": "Adaptive (10 → 500 ms)"}
    modes = [m for m in names if any(r["poll_mode"] == m for r in rows)]
    fig, ax = plt.subplots(figsize=(7.5, 4), facecolor=SURFACE)
    ends = []
    for mode, color in zip(modes, (SERIES_1, SERIES_2, SERIES_3)):
        pts = sorted((r for r in rows if r["poll_mode"] == mode), key=lambda r: r["workers"])
        w = [r["workers"] for r in pts]
        p99 = [r["p99_ms"] for r in pts]
        name = names[mode]
        ax.plot(w, p99, color=color, linewidth=2, marker="o", markersize=7,
                markeredgecolor=SURFACE, markeredgewidth=2, label=name)
        short = {"fixed-500ms": "500 ms poll", "fixed-50ms": "50 ms poll", "adaptive": "Adaptive"}[mode]
        ends.append((p99[-1], w[-1], f"{short}: {p99[-1]:.0f} ms"))
    # End labels, spread apart vertically if values are close (log scale).
    ends.sort()
    last_y = None
    for v, x, text in ends:
        y = v if last_y is None else max(v, last_y * 1.7)
        ax.annotate(text, (x, v), xytext=(x * 1.12, y), textcoords="data",
                    va="center", fontsize=9, color=TEXT_2)
        last_y = y
    ax.set_xscale("log", base=2)
    ax.set_yscale("log")
    ws = sorted({r["workers"] for r in rows})
    ax.set_xticks(ws, [str(x) for x in ws])
    ax.yaxis.set_major_formatter(matplotlib.ticker.FuncFormatter(lambda v, _: f"{v:,.0f} ms"))
    rate = rows[0]["rate"]
    style(ax, f"p99 latency at a steady {rate:.0f} jobs/sec (enqueue → done)",
          "Workers", "p99 latency (log scale)")
    ax.legend(frameon=False, fontsize=9, labelcolor=TEXT_2, loc="upper right")
    ax.set_xlim(right=max(ws) * 3.2)  # room for the end labels
    ax.yaxis.set_minor_locator(matplotlib.ticker.NullLocator())  # less clutter
    # Say why the left side is so high: the arrival rate was more than those
    # workers can handle (measured in the throughput test), so a backlog builds.
    measured = {r["workers"]: r["jobs_per_second"] for r in data["throughput"]}
    over = [w for w in ws if measured.get(w, float("inf")) < rate]
    if over:
        ax.axvspan(min(ws) / 1.3, max(over) * 1.2, color=GRID, alpha=0.6, linewidth=0, zorder=0)
        span = f"{min(over)}–{max(over)}" if len(over) > 1 else str(over[0])
        ax.text(0, 1.02, f"Shaded: {span} workers handle fewer than {rate:.0f} jobs/sec, "
                "so the backlog keeps growing during the run.",
                transform=ax.transAxes, fontsize=9, color=TEXT_2, va="bottom")
        ax.set_title(ax.get_title(loc="left"), loc="left", fontsize=13, color=TEXT, pad=24,
                     fontweight="semibold")
    fig.tight_layout()
    out = DOCS / "bench-latency.png"
    fig.savefig(out, dpi=160)
    plt.close(fig)
    return out


def main() -> None:
    # Use the newest run that has each experiment (they may come from separate runs).
    runs = [json.loads(p.read_text()) for p in sorted((ROOT / "bench" / "results").glob("2*.json"))]
    data = dict(runs[-1])
    data["throughput"] = next(r["throughput"] for r in reversed(runs) if r["throughput"])
    data["latency"] = next(r["latency"] for r in reversed(runs) if r["latency"])
    DOCS.mkdir(exist_ok=True)
    for out in (throughput_chart(data), latency_chart(data)):
        print(f"wrote {out.relative_to(ROOT)}")


if __name__ == "__main__":
    main()
