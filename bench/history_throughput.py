"""How fast can one history writer copy events into PostgreSQL?

Usage:  python -m bench.history_throughput   (Docker Compose stack running)

Fills a separate benchmark queue with finished jobs' events while no writer
is reading it, then times a single writer draining them at a few batch sizes.
The benchmark's jobs are removed from history afterwards.
"""

import json
import time
from datetime import datetime
from pathlib import Path

import redis

from taskqueue import config, db
from taskqueue.core.queue import Queue
from taskqueue.history import HistoryWriter

JOBS = 20_000
QUEUE = "bench-history"


def fill(r) -> int:
    q = Queue(r, name=QUEUE, max_waiting=0)
    for _ in range(JOBS):
        job_id = q.enqueue({"task": "echo"})
        _, token = q.lease()
        q.ack(job_id, token, "ok")
    return r.xlen(q.events_key)


def main() -> None:
    config.load_dotenv()
    r = redis.Redis.from_url(config.redis_url())
    results = []
    for batch in (100, 500, 1000):
        r.delete(f"tq:{QUEUE}:events")
        events = fill(r)
        pg = db.connect()
        writer = HistoryWriter(r, pg, queue_name=QUEUE, batch_size=batch)
        start = time.perf_counter()
        while writer.process_once():
            pass
        seconds = time.perf_counter() - start
        results.append({"batch_size": batch, "events": events, "seconds": seconds,
                        "events_per_second": events / seconds,
                        "jobs_per_second": JOBS / seconds})
        print(f"batch {batch:>5}: {events:,} events in {seconds:5.1f} s -> "
              f"{events / seconds:,.0f} events/s ({JOBS / seconds:,.0f} jobs/s)", flush=True)
        with pg.transaction():  # keep the real history clean
            pg.execute("DELETE FROM jobs WHERE queue = %s", (QUEUE,))
        pg.close()
        # Job records first: their ids are in the finished set, which goes next.
        for key in r.zrange(f"tq:{QUEUE}:finished", 0, -1):
            r.delete(f"tq:job:{key.decode()}")
        for key in r.scan_iter(f"tq:{QUEUE}:*"):
            r.delete(key)
    out = Path("bench/results") / f"history-{datetime.now():%Y%m%d-%H%M%S}.json"
    out.write_text(json.dumps({"jobs_per_run": JOBS, "runs": results}, indent=2))
    print(f"saved {out}")


if __name__ == "__main__":
    main()
