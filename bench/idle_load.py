"""Measure Redis requests/sec from idle workers under each polling mode.

Usage:  python -m bench.idle_load
Appends results to the newest bench/results file as "idle_load".
"""

import json
import time
from pathlib import Path

import redis

from bench.benchmark import POLL_MODES, set_workers
from src import config

WORKERS, SECONDS = 16, 10


def main() -> None:
    r = redis.Redis.from_url(config.REDIS_URL)
    results = []
    try:
        for mode in POLL_MODES:
            set_workers(WORKERS, mode)
            time.sleep(3)  # let adaptive workers back off to their idle wait
            before = r.info("stats")["total_commands_processed"]
            time.sleep(SECONDS)
            after = r.info("stats")["total_commands_processed"]
            per_sec = (after - before) / SECONDS
            results.append({"poll_mode": mode, "idle_workers": WORKERS,
                            "redis_requests_per_second": per_sec})
            print(f"{mode:<12} {WORKERS} idle workers -> {per_sec:7.1f} Redis requests/sec")
    finally:
        set_workers(3)
    newest = max(Path("bench/results").glob("2*.json"))
    data = json.loads(newest.read_text())
    data["idle_load"] = results
    newest.write_text(json.dumps(data, indent=2))
    print(f"added to {newest}")


if __name__ == "__main__":
    main()
