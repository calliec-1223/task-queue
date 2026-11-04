"""One-time backfill: let cleanup() see done jobs from before it existed.

cleanup() only removes done jobs listed in tq:<queue>:finished, and only ack()
adds them there. Jobs that finished before that change are never listed, so
their Redis copies stay forever. This script lists them, using the finish time
recorded in PostgreSQL history, so the normal cleanup removes them on schedule.

    python -m scripts.backfill_finished          # dry run: only counts
    python -m scripts.backfill_finished --apply  # write to Redis

Safe to run more than once: ZADD NX never changes a job that's already listed.
Jobs PostgreSQL doesn't know about get "now" as their finish time, so they're
removed one cleanup period from now.
"""

import argparse
import time

import redis

from taskqueue import config, db

BATCH = 1000


def main() -> None:
    config.load_dotenv()
    parser = argparse.ArgumentParser()
    parser.add_argument("--apply", action="store_true", help="write to Redis (default: dry run)")
    args = parser.parse_args()

    r = redis.Redis.from_url(config.redis_url())
    conn = db.connect()
    now = time.time()
    counts = {"scanned": 0, "done": 0, "already_listed": 0, "listed": 0, "no_history": 0}

    def flush(ids: list[str]) -> None:
        if not ids:
            return
        rows = conn.execute(
            "SELECT id::text, queue, extract(epoch FROM finished_at) FROM jobs WHERE id = ANY(%s::uuid[])",
            (ids,)).fetchall()
        known = {job_id: (queue, float(fin) if fin else now) for job_id, queue, fin in rows}
        pipe = r.pipeline(transaction=False)
        for job_id in ids:
            queue, finished = known.get(job_id, (config.queue_name(), now))
            if job_id not in known:
                counts["no_history"] += 1
            if args.apply:
                pipe.zadd(f"tq:{queue}:finished", {job_id: finished}, nx=True)
            counts["listed"] += 1
        pipe.execute()

    # Every queue's finished set, to skip jobs that are already listed.
    finished_sets = [k.decode() for k in r.scan_iter("tq:*:finished", count=1000)]
    pending: list[str] = []
    for key in r.scan_iter("tq:job:*", count=1000):
        counts["scanned"] += 1
        if r.hget(key, "status") != b"done":
            continue
        counts["done"] += 1
        job_id = key.decode().removeprefix("tq:job:")
        if any(r.zscore(s, job_id) is not None for s in finished_sets):
            counts["already_listed"] += 1
            continue
        pending.append(job_id)
        if len(pending) >= BATCH:
            flush(pending)
            pending = []
    flush(pending)

    verb = "added" if args.apply else "would add"
    print(f"{counts['scanned']} job records scanned, {counts['done']} done, "
          f"{counts['already_listed']} already listed, {verb} {counts['listed']} "
          f"({counts['no_history']} not in PostgreSQL, given finish time = now)")


if __name__ == "__main__":
    main()
