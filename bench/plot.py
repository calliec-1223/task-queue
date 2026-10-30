"""Draw benchmark charts from the newest results file.

Usage:
    python -m bench.plot                      # newest bench/results/*.json
    python -m bench.plot bench/results/X.json

Writes PNGs to docs/ for the README.
"""

import json
import sys
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
    polls = sorted({r["poll_interval"] for r in rows}, reverse=True)
    fig, ax = plt.subplots(figsize=(7.5, 4), facecolor=SURFACE)
    for poll, color in zip(polls, (SERIES_1, SERIES_2)):
        pts = sorted((r for r in rows if r["poll_interval"] == poll), key=lambda r: r["workers"])
        w = [r["workers"] for r in pts]
        p99 = [r["p99_ms"] for r in pts]
        name = f"Poll every {poll * 1000:.0f} ms"
        ax.plot(w, p99, color=color, linewidth=2, marker="o", markersize=7,
                markeredgecolor=SURFACE, markeredgewidth=2, label=name)
        label_end(ax, w[-1], p99[-1], f"{name}: {p99[-1]:.0f} ms")
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
    if len(sys.argv) > 1:
        path = Path(sys.argv[1])
    else:
        path = max((ROOT / "bench" / "results").glob("2*.json"))  # newest timestamped run
    data = json.loads(path.read_text())
    DOCS.mkdir(exist_ok=True)
    for out in (throughput_chart(data), latency_chart(data)):
        print(f"wrote {out.relative_to(ROOT)}")


if __name__ == "__main__":
    main()
