"""Metrics endpoint and live dashboard.

Run it with:
    python -m taskqueue.metrics
Then open http://localhost:8000 (dashboard) or http://localhost:8000/metrics (JSON).

Uses Python's built-in HTTP server, so no web framework dependency is needed.
Live queue numbers and workers come from Redis; job history comes from PostgreSQL.

Endpoints:
    GET  /               dashboard
    GET  /metrics        everything the dashboard shows, as JSON
    GET  /jobs/<id>      one job's attempt timeline from history
    POST /demo/submit    } only when DEMO_CONTROLS=1: submit sample jobs,
    POST /demo/kill      } make a worker crash, replay dead jobs. Requires the
    POST /demo/replay    } X-Demo header, so other websites can't trigger them.
"""

import json
import logging
import os
import random
import threading
import uuid
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

import psycopg
import redis

from taskqueue import config, db
from taskqueue.core.queue import Queue

log = logging.getLogger("metrics")

DASHBOARD = Path(__file__).resolve().parent / "dashboard.html"
WINDOW_SECONDS = 60


def demo_enabled() -> bool:
    return os.environ.get("DEMO_CONTROLS") == "1"


def kill_flag_key(queue: str, worker_id: str) -> str:
    return f"tq:{queue}:demo:kill:{worker_id}"


def collect(r: redis.Redis, conn: psycopg.Connection | None, queue: str = "default") -> dict:
    """Gather every number the dashboard shows."""
    sec, usec = r.time()  # Redis server clock, same one the queue uses
    now = sec + usec / 1_000_000
    delayed_key = f"tq:{queue}:delayed"
    # Ready = in the ready set, plus delayed jobs whose time has come but that
    # haven't been moved over yet (that happens on the next lease).
    due = r.zcount(delayed_key, "-inf", now)
    ready = r.zcard(f"tq:{queue}:ready") + due

    metrics = {
        "now": now,
        "queue": {
            "ready": ready,
            "delayed": r.zcard(delayed_key) - due,  # waiting for a delay or retry backoff
            "inflight": r.zcard(f"tq:{queue}:inflight"),
            "dead": r.zcard(f"tq:{queue}:dead"),
            "history_backlog": r.xlen(f"tq:{queue}:events"),  # events not yet in PostgreSQL
        },
        "history": None,
        "workers": sorted(Queue(r, name=queue).list_workers(), key=lambda w: w["id"]),
        "demo": demo_enabled(),
    }
    if conn is not None:
        metrics["history"] = _history(conn)
    return metrics


def job_timeline(conn: psycopg.Connection, job_id: str) -> dict | None:
    """One job and every attempt it made, oldest first."""
    try:
        uuid.UUID(job_id)
    except ValueError:
        return None
    job = conn.execute(
        """SELECT payload, status, priority, max_attempts, last_error, result,
                  extract(epoch FROM created_at), extract(epoch FROM finished_at)
           FROM jobs WHERE id = %s""", (job_id,)).fetchone()
    if job is None:
        return None
    attempts = conn.execute(
        """SELECT attempt_number, extract(epoch FROM started_at), extract(epoch FROM finished_at),
                  outcome, error
           FROM job_attempts WHERE job_id = %s ORDER BY started_at""", (job_id,)).fetchall()
    payload, status, priority, max_attempts, last_error, result, created, finished = job
    return {
        "id": job_id, "task": payload.get("task"), "args": payload.get("args", {}),
        "status": status, "priority": priority, "max_attempts": max_attempts,
        "last_error": last_error, "result": result,
        "created_at": float(created), "finished_at": float(finished) if finished else None,
        "attempts": [
            {"number": n, "started_at": float(s), "finished_at": float(f) if f else None,
             "outcome": outcome, "error": error}
            for n, s, f, outcome, error in attempts
        ],
    }


def _history(conn: psycopg.Connection) -> dict:
    q = conn.execute
    by_status = dict(q("SELECT status, count(*) FROM jobs GROUP BY status").fetchall())

    # Per-second counts for the last minute: finished jobs and failed attempts.
    done = q(
        """SELECT floor(extract(epoch FROM finished_at))::bigint, count(*)
           FROM jobs WHERE status = 'done' AND finished_at > now() - make_interval(secs => %s)
           GROUP BY 1""",
        (WINDOW_SECONDS,),
    ).fetchall()
    failed = q(
        """SELECT floor(extract(epoch FROM finished_at))::bigint, count(*)
           FROM job_attempts WHERE outcome IN ('error', 'lease_expired')
             AND finished_at > now() - make_interval(secs => %s)
           GROUP BY 1""",
        (WINDOW_SECONDS,),
    ).fetchall()

    recent = q(
        """SELECT j.id, j.payload->>'task', j.status, j.last_error,
                  extract(epoch FROM j.updated_at),
                  (SELECT count(*) FROM job_attempts a WHERE a.job_id = j.id)
           FROM jobs j ORDER BY j.updated_at DESC LIMIT 12"""
    ).fetchall()

    return {
        "jobs_by_status": by_status,
        "dead_events": q("SELECT count(*) FROM dead_events").fetchone()[0],
        "done_per_second": {int(t): n for t, n in done},
        "failed_per_second": {int(t): n for t, n in failed},
        "recent": [
            {"id": str(i), "task": task, "status": status, "last_error": err,
             "updated_at": float(ts), "attempts": attempts}
            for i, task, status, err, ts, attempts in recent
        ],
    }


class Handler(BaseHTTPRequestHandler):
    # Set once in main(); shared by all request threads.
    redis_client: redis.Redis
    pg_conn: psycopg.Connection | None = None
    pg_lock = threading.Lock()  # one psycopg connection can't be used by two threads at once

    def do_GET(self):
        if self.path == "/":
            self._send(200, "text/html; charset=utf-8", DASHBOARD.read_bytes())
        elif self.path == "/metrics":
            self._json(200, self._metrics())
        elif self.path.startswith("/jobs/"):
            timeline = self._with_pg(lambda conn: job_timeline(conn, self.path[len("/jobs/"):]))
            self._json(200, timeline) if timeline else self._json(404, {"error": "not found"})
        else:
            self._send(404, "text/plain", b"not found")

    def do_POST(self):
        # Demo controls change data, so they're off unless explicitly enabled, and
        # require a custom header: a page on another site can't send one without
        # the browser asking this server first, which it won't approve.
        if not demo_enabled() or self.headers.get("X-Demo") != "1":
            return self._json(403, {"error": "demo controls are off"})
        length = min(int(self.headers.get("Content-Length") or 0), 10_000)
        try:
            body = json.loads(self.rfile.read(length) or b"{}")
        except ValueError:
            return self._json(400, {"error": "bad JSON"})
        q = Queue(self.redis_client, name=config.queue_name())
        if self.path == "/demo/submit":
            self._json(200, self._demo_submit(q, body))
        elif self.path == "/demo/kill":
            alive = [w["id"] for w in q.list_workers() if w["alive"]]
            busy = [w["id"] for w in q.list_workers() if w["alive"] and w["status"] == "busy"]
            if not alive:
                return self._json(409, {"error": "no live workers"})
            victim = random.choice(busy or alive)  # prefer one mid-job: more interesting
            self.redis_client.set(kill_flag_key(q.name, victim), 1, ex=30)
            self._json(200, {"killed": victim})
        elif self.path == "/demo/replay":
            self._json(200, {"replayed": q.replay_dead()})
        else:
            self._json(404, {"error": "not found"})

    def _demo_submit(self, q: Queue, body: dict) -> dict:
        kind = body.get("kind", "mixed")
        count = max(1, min(int(body.get("count", 20)), 500))
        for _ in range(count):
            if kind == "fail":
                q.enqueue({"task": "fail"}, max_attempts=3)
            elif kind == "slow":
                q.enqueue({"task": "sleep", "args": {"seconds": 6}})
            elif kind == "urgent":
                q.enqueue({"task": "sleep", "args": {"seconds": 0.5}}, priority=5)
            else:
                q.enqueue({"task": "sleep", "args": {"seconds": round(random.uniform(0.3, 2.5), 2)}})
        return {"submitted": count, "kind": kind}

    def _with_pg(self, fn):
        with Handler.pg_lock:
            try:
                if Handler.pg_conn is None or Handler.pg_conn.closed:
                    Handler.pg_conn = db.connect()
                    Handler.pg_conn.autocommit = True
                return fn(Handler.pg_conn)
            except psycopg.OperationalError:
                Handler.pg_conn = None
                return None

    def _json(self, status: int, data) -> None:
        self._send(status, "application/json", json.dumps(data).encode())

    def _metrics(self) -> dict:
        with Handler.pg_lock:
            try:
                if Handler.pg_conn is None or Handler.pg_conn.closed:
                    Handler.pg_conn = db.connect()
                    Handler.pg_conn.autocommit = True
                return collect(self.redis_client, Handler.pg_conn, config.queue_name())
            except psycopg.OperationalError:
                # PostgreSQL down: still show live queue numbers from Redis.
                Handler.pg_conn = None
                return collect(self.redis_client, None, config.queue_name())

    def _send(self, status: int, content_type: str, body: bytes) -> None:
        self.send_response(status)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, *args):  # silence per-request logging (one request per second)
        pass


def main() -> None:
    config.load_dotenv()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(message)s")
    Handler.redis_client = redis.Redis.from_url(config.redis_url())
    port = int(os.environ.get("METRICS_PORT", 8000))
    # 127.0.0.1 = only reachable from this computer, unless overridden (Docker sets 0.0.0.0).
    host = os.environ.get("METRICS_HOST", "127.0.0.1")
    log.info("dashboard at http://localhost:%d", port)
    ThreadingHTTPServer((host, port), Handler).serve_forever()


if __name__ == "__main__":
    main()
