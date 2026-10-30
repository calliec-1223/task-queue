"""Chart for the Phase 4 Whisk before/after load test.

Usage:  python -m bench.plot_whisk   (newest bench/results/whisk-load-*.json)
"""

import json
from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402

from bench.plot import GRID, MUTED, SERIES_1, SERIES_2, SURFACE, TEXT, TEXT_2, style  # noqa: E402

ROOT = Path(__file__).resolve().parent.parent


def main() -> None:
    path = max((ROOT / "bench" / "results").glob("whisk-load-*.json"))
    data = json.loads(path.read_text())
    runs = data["runs"]
    users = sorted({r["chat_users"] for r in runs})
    by = {(r["mode"], r["chat_users"]): r for r in runs}
    names = {"sync": "Before: Claude call inside the request", "queue": "After: task queue"}
    colors = {"sync": SERIES_1, "queue": SERIES_2}

    # Two separate panels (no shared y-axis): page loads and chat answers.
    fig, (left, right) = plt.subplots(1, 2, figsize=(11, 4.2), facecolor=SURFACE)
    for mode in ("sync", "queue"):
        pages = [by[(mode, u)]["page_p50_ms"] for u in users]
        answers = [by[(mode, u)]["answer_p50_s"] for u in users]
        kw = dict(color=colors[mode], linewidth=2, marker="o", markersize=7,
                  markeredgecolor=SURFACE, markeredgewidth=2, label=names[mode])
        left.plot(users, pages, **kw)
        right.plot(users, answers, **kw)
        left.annotate(f"{pages[-1]:,.0f} ms", (users[-1], pages[-1]), xytext=(8, 0),
                      textcoords="offset points", va="center", fontsize=9, color=TEXT_2)
        right.annotate(f"{answers[-1]:.1f} s", (users[-1], answers[-1]), xytext=(8, 0),
                       textcoords="offset points", va="center", fontsize=9, color=TEXT_2)

    left.set_yscale("log")
    left.yaxis.set_major_formatter(matplotlib.ticker.FuncFormatter(lambda v, _: f"{v:,.0f} ms"))
    left.yaxis.set_minor_locator(matplotlib.ticker.NullLocator())
    style(left, "Recipe page load (median)", "People chatting at once", "")
    right.set_ylim(bottom=0)
    right.yaxis.set_major_formatter(matplotlib.ticker.FuncFormatter(lambda v, _: f"{v:.0f} s"))
    style(right, "Time until the chat answer appears (median)", "People chatting at once", "")
    for ax in (left, right):
        ax.set_xticks(users, [str(u) for u in users])
        ax.set_xlim(right=max(users) * 1.25)
    left.legend(frameon=False, fontsize=9, labelcolor=TEXT_2, loc="upper left")
    fig.text(0.01, 0.01, f"Whisk on gunicorn with {data['server_processes']} server processes; "
             f"queue mode adds {data['queue_workers']} workers. Claude stand-in replays measured "
             f"response times (median {data['claude_stand_in_median_s']} s).",
             fontsize=8.5, color=MUTED)
    fig.tight_layout(rect=(0, 0.04, 1, 1))
    out = ROOT / "docs" / "whisk-before-after.png"
    fig.savefig(out, dpi=160)
    print(f"wrote {out.relative_to(ROOT)}")


if __name__ == "__main__":
    main()
