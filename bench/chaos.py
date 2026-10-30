"""Failure injection: kill workers over and over during a run, then count.

Usage:
    docker compose up -d --build
    python -m bench.chaos              # about 2-3 minutes
    python -m bench.chaos --quick

While jobs are being submitted, a "killer" repeatedly SIGKILLs a random worker
container (no cleanup possible, like pulling the power cord) and the history
writer, then starts them again. At the end we compare:

    jobs submitted  vs  jobs completed (from PostgreSQL history and Redis)

Zero job loss means every submitted job finished, and none is stuck in Redis.
"""

import argparse
import json
import os
import random
import subprocess
import threading
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path

import redis

from taskqueue import config, db
from taskqueue.core.queue import Queue

ROOT = Path(__file__).resolve().parent.parent
RESULTS = ROOT / "bench" / "results"
WORKERS = 4
LEASE_SECONDS = 5  # short, so killed jobs are recovered within seconds


def compose(*args: str, lease: float = LEASE_SECONDS) -> str:
    env = {**os.environ, "LEASE_SECONDS": str(lease)}
    out = subprocess.run(["docker", "compose", *args], cwd=ROOT, check=True,
                         capture_output=True, text=True, env=env)
    return out.stdout


def running_workers() -> list[str]:
    names = compose("ps", "--format", "{{.Name}}", "--status", "running", "worker").split()
    return [n for n in names if n]


def killer(stop: threading.Event, log: list) -> None:
    """Every 2-4 seconds, SIGKILL a random worker; every 4th time, the history writer too."""
    n = 0
    while not stop.wait(random.uniform(2, 4)):
        n += 1
        workers = running_workers()
        if workers:
            victim = random.choice(workers)
            subprocess.run(["docker", "kill", "-s", "KILL", victim], capture_output=True)
            log.append({"t": time.time(), "killed": victim})
        if n % 4 == 0:
            subprocess.run(["docker", "kill", "-s", "KILL", "task-queue-history-1"],
                           capture_output=True)
            log.append({"t": time.time(), "killed": "task-queue-history-1"})
        time.sleep(1)
        # Bring the killed containers back, like a supervisor restarting crashed processes.
        compose("up", "-d", "--scale", f"worker={WORKERS}", "worker", "history")


def main() -> None:
    config.load_dotenv()
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--quick", action="store_true")
    opts = parser.parse_args()
    # Keep the arrival rate well under capacity (4 workers / ~0.28 s average
    # job = ~14 jobs/s), so the test measures crash recovery, not overload.
    jobs, rate = (150, 8) if opts.quick else (1000, 8)

    r = redis.Redis.from_url(config.redis_url())
    q = Queue(r, name=config.queue_name())
    if r.zcard(q.pending_key) or r.zcard(q.inflight_key):
        raise SystemExit("queue isn't empty; wait for it to drain first")
    pg = db.connect()
    pg.autocommit = True

    # Recreate workers with the short lease.
    compose("up", "-d", "--wait", "--force-recreate", "--scale", f"worker={WORKERS}",
            "worker", "history")

    run_id = f"chaos-{uuid.uuid4().hex[:8]}"
    started_at = datetime.now(timezone.utc).isoformat()
    stop, kills = threading.Event(), []
    thread = threading.Thread(target=killer, args=(stop, kills), daemon=True)
    thread.start()

    print(f"submitting {jobs} jobs at {rate}/s while killing workers...")
    start = time.perf_counter()
    for i in range(jobs):
        wait = start + i / rate - time.perf_counter()
        if wait > 0:
            time.sleep(wait)
        seconds = round(random.uniform(0.05, 0.5), 2)  # mixed lengths, many killed mid-run
        # max_attempts is high on purpose: crashes count as attempts (DECISIONS.md #4),
        # and this test is about lost jobs, not about the dead-letter queue.
        q.enqueue({"task": "sleep", "args": {"seconds": seconds}, "run": run_id}, max_attempts=50)

    stop.set()
    thread.join()
    compose("up", "-d", "--wait", "--scale", f"worker={WORKERS}", "worker", "history")
    print(f"done submitting; {len(kills)} kills. waiting for every job to finish...")

    # Wait until Redis has nothing left to do and history has caught up.
    deadline = time.time() + 600
    while time.time() < deadline:
        busy = r.zcard(q.pending_key) + r.zcard(q.inflight_key) + r.xlen(q.events_key)
        if busy == 0:
            break
        time.sleep(1)
    else:
        print("WARNING: queue still not empty after 10 minutes; counts below are a snapshot")

    stats = pg.execute(
        """SELECT
             count(*)                                            AS recorded,
             count(*) FILTER (WHERE status = 'done')             AS done,
             count(*) FILTER (WHERE status = 'dead')             AS dead,
             count(*) FILTER (WHERE status NOT IN ('done','dead')) AS unfinished
           FROM jobs WHERE payload->>'run' = %s""", (run_id,),
    ).fetchone()
    attempts = pg.execute(
        """SELECT
             count(*) FILTER (WHERE a.outcome = 'lease_expired') AS interrupted,
             count(DISTINCT a.job_id) FILTER (WHERE a.outcome = 'lease_expired') AS jobs_interrupted,
             (SELECT count(*) FROM (
                SELECT a2.job_id FROM job_attempts a2 JOIN jobs j2 ON j2.id = a2.job_id
                WHERE j2.payload->>'run' = %s AND a2.outcome = 'done'
                GROUP BY a2.job_id HAVING count(*) > 1) d) AS done_twice
           FROM job_attempts a JOIN jobs j ON j.id = a.job_id
           WHERE j.payload->>'run' = %s""", (run_id, run_id),
    ).fetchone()

    result = {
        "started_at": started_at,
        "finished_at": datetime.now(timezone.utc).isoformat(),
        "run_id": run_id,
        "submitted": jobs,
        "recorded_in_history": stats[0],
        "completed": stats[1],
        "dead_lettered": stats[2],
        "unfinished": stats[3],
        # Lost = submitted but neither finished in history nor still waiting in Redis.
        "still_in_redis": r.zcard(q.pending_key) + r.zcard(q.inflight_key),
        "lost": jobs - stats[1] - stats[2] - (r.zcard(q.pending_key) + r.zcard(q.inflight_key)),
        "worker_kills": sum(1 for k in kills if "worker" in k["killed"]),
        "history_writer_kills": sum(1 for k in kills if "history" in k["killed"]),
        "attempts_interrupted_by_kill": attempts[0],
        "jobs_interrupted_at_least_once": attempts[1],
        "jobs_recorded_done_twice": attempts[2],
        "left_in_redis": {"pending": r.zcard(q.pending_key), "inflight": r.zcard(q.inflight_key)},
    }
    for k, v in result.items():
        print(f"{k:32} {v}")

    # Back to the normal setup (3 workers, default 30-second lease).
    compose("up", "-d", "--force-recreate", "--scale", "worker=3", "worker", "history", lease=30)
    RESULTS.mkdir(parents=True, exist_ok=True)
    name = "chaos-quick" if opts.quick else "chaos-" + datetime.now().strftime("%Y%m%d-%H%M%S")
    (RESULTS / f"{name}.json").write_text(json.dumps(result, indent=2))
    print(f"saved bench/results/{name}.json")


if __name__ == "__main__":
    main()
