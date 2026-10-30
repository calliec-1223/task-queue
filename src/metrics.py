"""Metrics endpoint and live dashboard.

Run it with:
    python -m src.metrics
Then open http://localhost:8000 (dashboard) or http://localhost:8000/metrics (JSON).

Uses Python's built-in HTTP server, so no web framework dependency is needed.
Live queue numbers come from Redis; completed-job history comes from PostgreSQL.
"""

import json
import logging
import os
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

import psycopg
import redis

from src import config, db

log = logging.getLogger("metrics")

DASHBOARD = Path(__file__).resolve().parent / "dashboard.html"
WINDOW_SECONDS = 60


def collect(r: redis.Redis, conn: psycopg.Connection | None, queue: str = "default") -> dict:
    """Gather every number the dashboard shows."""
    sec, usec = r.time()  # Redis server clock, same one the queue uses
    now = sec + usec / 1_000_000
    pending_key = f"tq:{queue}:pending"
    pending_total = r.zcard(pending_key)
    ready = r.zcount(pending_key, "-inf", now)  # score <= now: can run right now

    metrics = {
        "now": now,
        "queue": {
            "ready": ready,
            "delayed": pending_total - ready,  # waiting for a delay or retry backoff
            "inflight": r.zcard(f"tq:{queue}:inflight"),
            "dead": r.zcard(f"tq:{queue}:dead"),
            "history_backlog": r.xlen(f"tq:{queue}:events"),  # events not yet in PostgreSQL
        },
        "history": None,
    }
    if conn is not None:
        metrics["history"] = _history(conn)
    return metrics


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
            self._send(200, "application/json", json.dumps(self._metrics()).encode())
        else:
            self._send(404, "text/plain", b"not found")

    def _metrics(self) -> dict:
        with Handler.pg_lock:
            try:
                if Handler.pg_conn is None or Handler.pg_conn.closed:
                    Handler.pg_conn = db.connect()
                    Handler.pg_conn.autocommit = True
                return collect(self.redis_client, Handler.pg_conn, config.QUEUE_NAME)
            except psycopg.OperationalError:
                # PostgreSQL down: still show live queue numbers from Redis.
                Handler.pg_conn = None
                return collect(self.redis_client, None, config.QUEUE_NAME)

    def _send(self, status: int, content_type: str, body: bytes) -> None:
        self.send_response(status)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, *args):  # silence per-request logging (one request per second)
        pass


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(message)s")
    Handler.redis_client = redis.Redis.from_url(config.REDIS_URL)
    port = int(os.environ.get("METRICS_PORT", 8000))
    # 127.0.0.1 = only reachable from this computer, unless overridden (Docker sets 0.0.0.0).
    host = os.environ.get("METRICS_HOST", "127.0.0.1")
    log.info("dashboard at http://localhost:%d", port)
    ThreadingHTTPServer((host, port), Handler).serve_forever()


if __name__ == "__main__":
    main()
