"""Throughput and latency benchmark, run against the Docker Compose stack.

Usage:
    docker compose up -d --build
    python -m bench.benchmark            # full run, a few minutes
    python -m bench.benchmark --quick    # small sanity run

Workers run in Docker (scaled with `docker compose --scale`); this script
submits jobs from the host and reads results from PostgreSQL history.

Two experiments:
  throughput: submit N jobs at once, measure how fast W workers finish them.
  latency:    submit a steady R jobs/sec, measure enqueue -> done time per job
              (p50 and p99), for several worker counts and poll intervals.

Latency uses jobs.created_at and jobs.finished_at, both stamped by the Redis
server clock, so no clocks from different machines are mixed.
Results are saved as JSON in bench/results/. Charts: python -m bench.plot
"""

import argparse
import json
import os
import platform
import subprocess
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path

import redis

from taskqueue import config, db
from taskqueue.core.queue import Queue

ROOT = Path(__file__).resolve().parent.parent
RESULTS = ROOT / "bench" / "results"


def compose(*args: str, env: dict | None = None) -> None:
    subprocess.run(
        ["docker", "compose", *args], cwd=ROOT, check=True,
        capture_output=True, env={**os.environ, **(env or {})},
    )


# How idle workers wait: (longest idle wait, first idle wait, wake mode).
POLL_MODES = {
    "fixed-500ms": (0.5, 0.5, "poll"),
    "fixed-50ms": (0.05, 0.05, "poll"),
    "adaptive": (0.5, 0.01, "poll"),        # 10 ms, doubling to 500 ms
    "doorbell": (0.5, 0.01, "doorbell"),    # block until a job is ready (the default)
}


def set_workers(count: int, mode: str = "doorbell") -> None:
    """Run exactly `count` worker containers with the given polling mode."""
    longest, first, wake = POLL_MODES[mode]
    compose("up", "-d", "--wait", "--scale", f"worker={count}", "worker",
            env={"POLL_INTERVAL": str(longest), "POLL_MIN_INTERVAL": str(first),
                 "WAKE_MODE": wake, "DEMO_CONTROLS": "0", "LEASE_SECONDS": "30"})
    time.sleep(2)  # let every worker finish starting and begin polling


def wait_until_done(pg, run_id: str, expected: int, timeout: float = 300) -> None:
    """Wait until PostgreSQL history shows every job of this run finished."""
    deadline = time.time() + timeout
    while time.time() < deadline:
        finished = pg.execute(
            "SELECT count(*) FROM jobs WHERE payload->>'run' = %s AND status IN ('done', 'dead')",
            (run_id,),
        ).fetchone()[0]
        if finished == expected:
            return
        time.sleep(0.5)
    raise TimeoutError(f"run {run_id}: only {finished}/{expected} jobs finished")


def check_all_succeeded(pg, run_id: str) -> None:
    bad = pg.execute(
        "SELECT count(*) FROM jobs WHERE payload->>'run' = %s AND status <> 'done'", (run_id,)
    ).fetchone()[0]
    if bad:
        raise RuntimeError(f"run {run_id}: {bad} jobs did not succeed; results would be misleading")


def throughput_run(q: Queue, pg, workers: int, jobs: int, task_seconds: float) -> dict:
    run_id = f"tp-{uuid.uuid4().hex[:8]}"
    payload = {"task": "sleep", "args": {"seconds": task_seconds}, "run": run_id}
    for _ in range(jobs):
        q.enqueue(payload)
    wait_until_done(pg, run_id, jobs)
    check_all_succeeded(pg, run_id)
    span = pg.execute(
        """SELECT extract(epoch FROM max(finished_at) - min(created_at))
           FROM jobs WHERE payload->>'run' = %s""", (run_id,),
    ).fetchone()[0]
    return {
        "workers": workers, "jobs": jobs, "seconds": float(span),
        "jobs_per_second": jobs / float(span),
        # Upper bound if workers did nothing but run tasks: W / task time.
        "ideal_jobs_per_second": workers / task_seconds,
    }


def latency_run(q: Queue, pg, workers: int, mode: str, rate: float,
                duration: float, task_seconds: float) -> dict:
    run_id = f"lat-{uuid.uuid4().hex[:8]}"
    payload = {"task": "sleep", "args": {"seconds": task_seconds}, "run": run_id}
    total = int(rate * duration)
    start = time.perf_counter()
    for i in range(total):
        # Open loop: submit on a fixed schedule, whether or not workers keep up.
        wait = start + i / rate - time.perf_counter()
        if wait > 0:
            time.sleep(wait)
        q.enqueue(payload)
    wait_until_done(pg, run_id, total)
    check_all_succeeded(pg, run_id)
    p50, p99, mx = pg.execute(
        """SELECT percentile_cont(0.5)  WITHIN GROUP (ORDER BY ms),
                  percentile_cont(0.99) WITHIN GROUP (ORDER BY ms),
                  max(ms)
           FROM (SELECT extract(epoch FROM finished_at - created_at) * 1000 AS ms
                 FROM jobs WHERE payload->>'run' = %s) t""", (run_id,),
    ).fetchone()
    return {
        "workers": workers, "poll_mode": mode, "rate": rate, "jobs": total,
        "p50_ms": float(p50), "p99_ms": float(p99), "max_ms": float(mx),
    }


def main() -> None:
    config.load_dotenv()
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--quick", action="store_true", help="small, fast sanity run")
    parser.add_argument("--skip-throughput", action="store_true",
                        help="only run the latency experiment")
    opts = parser.parse_args()

    task_seconds = 0.02
    if opts.quick:
        worker_counts, tp_jobs, rate, duration = [1, 4], 100, 50, 4
    else:
        worker_counts, tp_jobs, rate, duration = [1, 2, 4, 8, 16], 1000, 100, 20

    r = redis.Redis.from_url(config.redis_url())
    q = Queue(r, name=config.queue_name())
    if q.count_waiting() or r.zcard(q.inflight_key):
        raise SystemExit("queue isn't empty; wait for it to drain so old jobs don't skew results")
    pg = db.connect()
    pg.autocommit = True
    compose("up", "-d", "--wait", "history")

    results = {
        "started_at": datetime.now(timezone.utc).isoformat(),
        "machine": {"platform": platform.platform(), "cpus": os.cpu_count()},
        "task_seconds": task_seconds,
        "throughput": [], "latency": [],
    }
    try:
        for w in ([] if opts.skip_throughput else worker_counts):
            set_workers(w)
            res = throughput_run(q, pg, w, tp_jobs, task_seconds)
            results["throughput"].append(res)
            print(f"throughput  workers={w:<3} {res['jobs_per_second']:7.1f} jobs/s "
                  f"(ideal {res['ideal_jobs_per_second']:.0f})")

        for mode in POLL_MODES:
            for w in worker_counts:
                set_workers(w, mode)
                res = latency_run(q, pg, w, mode, rate, duration, task_seconds)
                results["latency"].append(res)
                print(f"latency     workers={w:<3} {mode:<12} p50={res['p50_ms']:8.1f} ms "
                      f"p99={res['p99_ms']:8.1f} ms")
    finally:
        set_workers(3)  # back to the normal setup

    RESULTS.mkdir(parents=True, exist_ok=True)
    name = "quick" if opts.quick else datetime.now().strftime("%Y%m%d-%H%M%S")
    out = RESULTS / f"{name}.json"
    out.write_text(json.dumps(results, indent=2))
    print(f"saved {out.relative_to(ROOT)}")


if __name__ == "__main__":
    main()
