"""History writer: copies job events from the Redis stream into PostgreSQL.

Run it with:
    python -m src.history

How it stays safe:
- Events are read through a Redis "consumer group", which remembers which
  events were delivered but not yet confirmed (XACK). If the writer crashes,
  the unconfirmed events are delivered again on restart.
- Events are confirmed only AFTER the PostgreSQL transaction commits. So a
  crash can cause an event to be applied twice, never zero times.
- Applying an event twice gives the same result as applying it once
  (idempotent), so being delivered twice is harmless.
- Run exactly one writer per queue: events for the same job must be applied
  in order (e.g. "leased" before "acked").
"""

import json
import logging
import signal
import time

import psycopg
import redis
from psycopg.types.json import Jsonb

from src import config, db

log = logging.getLogger("history")

GROUP = "history"


class HistoryWriter:
    def __init__(
        self,
        client: redis.Redis,
        conn: psycopg.Connection,
        queue_name: str = "default",
        consumer: str = "writer-1",
        batch_size: int = 100,
    ):
        self.r = client
        self.conn = conn
        self.stream = f"tq:{queue_name}:events"
        self.consumer = consumer
        self.batch_size = batch_size
        self._stopping = False
        try:
            # Start the group at the beginning of the stream ("0") so no
            # event written before the writer's first run is skipped.
            self.r.xgroup_create(self.stream, GROUP, id="0", mkstream=True)
        except redis.ResponseError as e:
            if "BUSYGROUP" not in str(e):  # BUSYGROUP = group already exists: fine
                raise

    def process_once(self, block_ms: int = 0) -> int:
        """Apply one batch of events. Returns how many were applied."""
        # First re-read events this consumer received but never confirmed
        # (it crashed last time). "0" = my unconfirmed events; ">" = new ones.
        entries = self._read("0") or self._read(">", block_ms)
        if not entries:
            return 0

        with self.conn.transaction():  # all events in the batch commit together
            for _, fields in entries:
                self._apply({k.decode(): v.decode() for k, v in fields.items()})

        # Only now, after the commit, tell Redis they're handled.
        ids = [entry_id for entry_id, _ in entries]
        self.r.xack(self.stream, GROUP, *ids)
        self.r.xdel(self.stream, *ids)  # free the memory; PostgreSQL has them now
        return len(entries)

    def run(self) -> None:
        signal.signal(signal.SIGINT, self._request_stop)
        signal.signal(signal.SIGTERM, self._request_stop)
        log.info("history writer started")
        while not self._stopping:
            try:
                applied = self.process_once(block_ms=1000)
                if applied:
                    log.info("applied %d event(s)", applied)
            except psycopg.OperationalError as e:
                # PostgreSQL is down. Events wait safely in Redis; retry soon.
                log.warning("PostgreSQL unavailable, retrying: %s", e)
                time.sleep(2)
                self.conn = db.connect()
        log.info("history writer stopped")

    def _read(self, start_id: str, block_ms: int = 0) -> list:
        kwargs = {"block": block_ms} if start_id == ">" and block_ms else {}
        result = self.r.xreadgroup(
            GROUP, self.consumer, {self.stream: start_id}, count=self.batch_size, **kwargs
        )
        return result[0][1] if result else []

    def _apply(self, e: dict) -> None:
        """Write one event to PostgreSQL. Safe to run more than once."""
        kind, job_id, ts = e["type"], e["job_id"], float(e["ts"])
        sql = self.conn.execute

        if kind == "enqueued":
            sql(
                """INSERT INTO jobs (id, queue, payload, status, max_attempts, created_at, updated_at)
                   VALUES (%s, %s, %s, 'pending', %s, to_timestamp(%s), to_timestamp(%s))
                   ON CONFLICT (id) DO NOTHING""",  # already recorded: do nothing
                (job_id, e["queue"], Jsonb(json.loads(e["payload"])), int(e["max_attempts"]), ts, ts),
            )

        elif kind == "leased":
            # WHERE EXISTS: skip jobs created before history existed, instead of
            # failing on the foreign key and blocking every later event.
            sql(
                """INSERT INTO job_attempts (lease_token, job_id, attempt_number, started_at)
                   SELECT %s, %s, %s, to_timestamp(%s)
                   WHERE EXISTS (SELECT 1 FROM jobs WHERE id = %s)
                   ON CONFLICT (lease_token) DO NOTHING""",
                (e["lease_token"], job_id, int(e["attempt"]), ts, job_id),
            )
            self._set_job(job_id, ts, status="leased")

        elif kind == "acked":
            self._finish_attempt(e["lease_token"], ts, "done", None)
            self._set_job(job_id, ts, status="done", result=Jsonb(json.loads(e["result"])),
                          finished=True)

        elif kind in ("failed", "lease_expired"):
            outcome = "error" if kind == "failed" else "lease_expired"
            self._finish_attempt(e["lease_token"], ts, outcome, e["error"])
            dead = e["outcome"] == "dead"
            self._set_job(job_id, ts, status="dead" if dead else "pending",
                          last_error=e["error"], finished=dead)

        elif kind == "replayed":
            self._set_job(job_id, ts, status="pending", clear_finished=True)

        else:
            log.warning("unknown event type %r, skipping", kind)

    def _finish_attempt(self, token: str, ts: float, outcome: str, error: str | None) -> None:
        self.conn.execute(
            """UPDATE job_attempts SET finished_at = to_timestamp(%s), outcome = %s, error = %s
               WHERE lease_token = %s""",
            (ts, outcome, error, token or None),
        )

    def _set_job(self, job_id, ts, status, result=None, last_error=None,
                 finished=False, clear_finished=False) -> None:
        self.conn.execute(
            """UPDATE jobs SET
                   status = %s,
                   updated_at = to_timestamp(%s),
                   result = COALESCE(%s, result),
                   last_error = COALESCE(%s, last_error),
                   finished_at = CASE WHEN %s THEN to_timestamp(%s)
                                      WHEN %s THEN NULL
                                      ELSE finished_at END
               WHERE id = %s""",
            (status, ts, result, last_error, finished, ts, clear_finished, job_id),
        )

    def _request_stop(self, signum, frame) -> None:
        self._stopping = True


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(message)s")
    client = redis.Redis.from_url(config.REDIS_URL)
    conn = db.connect()
    db.init_schema(conn)
    HistoryWriter(client, conn, queue_name=config.QUEUE_NAME).run()


if __name__ == "__main__":
    main()
