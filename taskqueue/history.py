"""History writer: copies job events from the Redis stream into PostgreSQL.

Run it with:
    python -m taskqueue.history

How it stays safe:
- Events are read through a Redis "consumer group", which remembers which
  events were delivered but not yet confirmed (XACK). If the writer crashes,
  the unconfirmed events are delivered again on restart.
- Events are confirmed only AFTER the PostgreSQL transaction commits. So a
  crash can cause an event to be applied twice, never zero times.
- Applying an event twice gives the same result as applying it once
  (idempotent), so being delivered twice is harmless.
- Exactly one writer per queue may write at a time, because events for a job
  must be applied in order ("leased" before "acked"). run() enforces this:
  writers compete for a lock in Redis (with an expiry, renewed while held);
  others wait on standby and take over if the holder dies. Each new holder gets
  a higher fencing token, and every batch transaction checks in PostgreSQL that
  no newer token exists. So a writer that froze, lost the lock, and woke up
  later can't sneak in a stale batch.
- Poison events (bad data that fails every time) can't block history forever.
  A failing event aborts its batch so it's retried; after max_event_failures
  tries it's saved to the dead_events table and the batch moves on.
"""

import json
import logging
import signal
import threading
import time
import uuid

import psycopg
import redis
from psycopg.types.json import Jsonb

from taskqueue import config, db

log = logging.getLogger("history")

GROUP = "history"


class EventFailed(Exception):
    """One event failed to apply; its batch was rolled back and will be retried."""


class FencedOut(Exception):
    """A newer writer has taken over; this writer's batch was rejected."""


# KEYS: lock, fence_counter  ARGV: writer_id, lock_ms
_ACQUIRE = """
-- Claim the writer lock only if nobody holds it, with an expiry. Each new
-- holder gets the next fencing token (a number that only goes up).
if redis.call('SET', KEYS[1], ARGV[1], 'NX', 'PX', ARGV[2]) then
    return redis.call('INCR', KEYS[2])
end
return 0
"""

# KEYS: lock  ARGV: writer_id, lock_ms
_RENEW = """
-- Extend the lock only if this writer still holds it (check + extend in one step).
if redis.call('GET', KEYS[1]) == ARGV[1] then
    return redis.call('PEXPIRE', KEYS[1], ARGV[2])
end
return 0
"""

# KEYS: lock  ARGV: writer_id
_RELEASE = """
if redis.call('GET', KEYS[1]) == ARGV[1] then return redis.call('DEL', KEYS[1]) end
return 0
"""


class HistoryWriter:
    def __init__(
        self,
        client: redis.Redis,
        conn: psycopg.Connection,
        queue_name: str = "default",
        consumer: str = "writer-1",
        batch_size: int = 100,
        max_event_failures: int = 3,
        lock_ms: int = 10_000,
    ):
        self.r = client
        self.conn = conn
        self.stream = f"tq:{queue_name}:events"
        # How many times each event has failed because of its own data.
        # (Not Redis's delivery count: that also rises during PostgreSQL outages.)
        self.failures_key = f"tq:{queue_name}:event_failures"
        self.max_event_failures = max_event_failures
        # Every writer uses the same consumer name on purpose: only the lock
        # holder reads, so a new holder picks up its predecessor's unconfirmed events.
        self.consumer = consumer
        self.queue_name = queue_name
        self.lock_key = f"tq:{queue_name}:history:lock"
        self.fence_key = f"tq:{queue_name}:history:fence"
        self.lock_ms = lock_ms
        self.writer_id = str(uuid.uuid4())
        self.token = None  # fencing token while holding the lock; None = not fenced
        self._acquire = client.register_script(_ACQUIRE)
        self._renew = client.register_script(_RENEW)
        self._release = client.register_script(_RELEASE)
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
            if self.token is not None:
                self._check_fence()
            if not self._try_fast_path(entries):
                # Slow, careful path: one event at a time, so a bad event can be
                # found, counted, and eventually set aside.
                for entry_id, fields in entries:
                    self._apply_or_dead_letter(entry_id, fields)

        # Only now, after the commit, tell Redis they're handled.
        ids = [entry_id for entry_id, _ in entries]
        self.r.xack(self.stream, GROUP, *ids)
        self.r.xdel(self.stream, *ids)  # free the memory; PostgreSQL has them now
        self.r.hdel(self.failures_key, *ids)  # forget any failure counts
        return len(entries)

    def _try_fast_path(self, entries: list) -> bool:
        """Apply the whole batch with a few bulk statements. False = it failed and
        nothing was written, so the caller should use the per-event path."""
        try:
            with self.conn.transaction():  # SAVEPOINT: a failure undoes only this attempt
                events = [{k.decode(): v.decode() for k, v in fields.items()}
                          for _, fields in entries]
                self._apply_batch(events)
            return True
        except psycopg.OperationalError:
            raise  # PostgreSQL is down: the slow path wouldn't do any better
        except Exception as e:
            # Some event in the batch is bad. We don't know which one, so let the
            # per-event path find it (and count its failure).
            log.info("fast path failed (%s: %s); applying batch event by event",
                     type(e).__name__, e)
            return False

    def _apply_batch(self, events: list[dict]) -> None:
        """Write a batch of events with a few bulk statements instead of several per event.

        Gives the same result as calling _apply() on each event in order:
        1. Walk the events in stream order and work out each job's and each
           attempt's FINAL state in this batch (later events overwrite earlier ones,
           the same way the per-event UPDATEs would).
        2. Write those final states: new jobs, then new attempts (they need their
           job to exist), then finished attempts, then job updates.
        Events for different jobs don't affect each other, so only the order
        within one job matters, and step 1 keeps it.
        """
        new_jobs = {}      # job_id -> row for INSERT (first 'enqueued' wins, like DO NOTHING)
        new_attempts = {}  # lease_token -> row for INSERT
        finished = {}      # lease_token -> (ts, outcome, error); last one wins
        jobs = {}          # job_id -> final state from this batch's events

        for e in events:
            kind, job_id, ts = e["type"], e["job_id"], float(e["ts"])
            if kind == "enqueued":
                json.loads(e["payload"])  # reject bad JSON here, before any SQL runs
                new_jobs.setdefault(job_id, (job_id, e["queue"], e["payload"],
                                             int(e["max_attempts"]), int(e.get("priority", 0)), ts))
                continue
            if kind == "leased":
                new_attempts.setdefault(e["lease_token"],
                                        (e["lease_token"], job_id, int(e["attempt"]), ts))
            elif kind == "acked":
                finished[e["lease_token"]] = (ts, "done", None)
            elif kind in ("failed", "lease_expired"):
                outcome = "error" if kind == "failed" else "lease_expired"
                finished[e["lease_token"]] = (ts, outcome, e["error"])
            elif kind == "released":
                finished[e["lease_token"]] = (ts, "released", None)
            elif kind not in ("replayed", "discarded"):
                log.warning("unknown event type %r, skipping", kind)
                continue

            # The job's state after this event. Fields mirror _set_job():
            # result/last_error keep the latest value set; finish = keep/set/clear.
            job = jobs.setdefault(job_id, {"result": None, "last_error": None,
                                           "finish": "keep", "finished_ts": None,
                                           "discarded_ts": None})
            job["ts"] = ts
            if kind == "leased":
                job["status"] = "leased"
            elif kind == "acked":
                job.update(status="done", result=e["result"], finish="set", finished_ts=ts)
                json.loads(e["result"])  # reject bad JSON here, before any SQL runs
            elif kind in ("failed", "lease_expired"):
                dead = e["outcome"] == "dead"
                job.update(status="dead" if dead else "pending", last_error=e["error"])
                if dead:
                    job.update(finish="set", finished_ts=ts)
            elif kind == "released":
                job["status"] = "pending"
            elif kind == "replayed":
                job.update(status="pending", finish="clear", finished_ts=None)
            elif kind == "discarded":
                job.update(status="discarded", discarded_ts=ts)

        sql = self.conn.execute
        # unnest() turns parallel arrays into rows, so one statement covers many jobs.
        if new_jobs:
            ids, queues, payloads, max_attempts, priorities, tss = zip(*new_jobs.values())
            sql("""INSERT INTO jobs (id, queue, payload, status, max_attempts, priority,
                                     created_at, updated_at)
                   SELECT id, queue, payload::jsonb, 'pending', max_attempts, priority,
                          to_timestamp(ts), to_timestamp(ts)
                   FROM unnest(%s::uuid[], %s::text[], %s::text[], %s::int[], %s::int[],
                               %s::float8[]) AS t(id, queue, payload, max_attempts, priority, ts)
                   ON CONFLICT (id) DO NOTHING""",
                (list(ids), list(queues), list(payloads), list(max_attempts),
                 list(priorities), list(tss)))
        if new_attempts:
            tokens, job_ids, numbers, tss = zip(*new_attempts.values())
            # The EXISTS check skips jobs created before history existed, as _apply() does.
            sql("""INSERT INTO job_attempts (lease_token, job_id, attempt_number, started_at)
                   SELECT t.token, t.job_id, t.number, to_timestamp(t.ts)
                   FROM unnest(%s::uuid[], %s::uuid[], %s::int[], %s::float8[])
                        AS t(token, job_id, number, ts)
                   WHERE EXISTS (SELECT 1 FROM jobs WHERE id = t.job_id)
                   ON CONFLICT (lease_token) DO NOTHING""",
                (list(tokens), list(job_ids), list(numbers), list(tss)))
        finished = {t: v for t, v in finished.items() if t}  # empty token matches nothing
        if finished:
            tss, outcomes, errors = zip(*finished.values())
            sql("""UPDATE job_attempts a SET finished_at = to_timestamp(t.ts),
                          outcome = t.outcome, error = t.error
                   FROM unnest(%s::uuid[], %s::float8[], %s::text[], %s::text[])
                        AS t(token, ts, outcome, error)
                   WHERE a.lease_token = t.token""",
                (list(finished), list(tss), list(outcomes), list(errors)))
        if jobs:
            rows = [(job_id, j["status"], j["ts"], j["result"], j["last_error"], j["finish"],
                     j["finished_ts"], j["discarded_ts"]) for job_id, j in jobs.items()]
            (ids, statuses, tss, results, last_errors, finishes,
             finished_tss, discarded_tss) = zip(*rows)
            sql("""UPDATE jobs j SET
                       status = t.status,
                       updated_at = to_timestamp(t.ts),
                       result = COALESCE(t.result::jsonb, j.result),
                       last_error = COALESCE(t.last_error, j.last_error),
                       finished_at = CASE t.finish WHEN 'set' THEN to_timestamp(t.finished_ts)
                                                   WHEN 'clear' THEN NULL
                                                   ELSE j.finished_at END,
                       discarded_at = COALESCE(to_timestamp(t.discarded_ts), j.discarded_at)
                   FROM unnest(%s::uuid[], %s::text[], %s::float8[], %s::text[], %s::text[],
                               %s::text[], %s::float8[], %s::float8[])
                        AS t(id, status, ts, result, last_error, finish, finished_ts, discarded_ts)
                   WHERE j.id = t.id""",
                (list(ids), list(statuses), list(tss), list(results), list(last_errors),
                 list(finishes), list(finished_tss), list(discarded_tss)))

    def _apply_or_dead_letter(self, entry_id: bytes, fields: dict) -> None:
        try:
            # Nested transaction = SAVEPOINT: if this event fails, only its own
            # writes are undone, not the rest of the batch.
            with self.conn.transaction():
                event = {k.decode(): v.decode() for k, v in fields.items()}
                self._apply(event)
        except psycopg.OperationalError:
            raise  # PostgreSQL itself is down: not this event's fault, don't count it
        except Exception as e:
            failures = self.r.hincrby(self.failures_key, entry_id, 1)
            if failures < self.max_event_failures:
                # Abort the whole batch; it's redelivered and this event retried.
                raise EventFailed(f"event {entry_id.decode()} failed ({failures}x): {e}") from e
            # Failed too many times: set it aside so the events behind it can flow.
            log.error("dead-lettering event %s after %d failures: %s",
                      entry_id.decode(), failures, e)
            raw = {k.decode(errors="replace"): v.decode(errors="replace") for k, v in fields.items()}
            self.conn.execute(
                """INSERT INTO dead_events (event_id, stream, fields, error, failures)
                   VALUES (%s, %s, %s, %s, %s)
                   ON CONFLICT (event_id) DO NOTHING""",
                (entry_id.decode(), self.stream, Jsonb(raw), f"{type(e).__name__}: {e}", failures),
            )

    # ---- Single-writer lock + fencing ------------------------------------------

    def try_acquire(self) -> bool:
        """Become the active writer if the lock is free. Sets self.token."""
        token = self._acquire(keys=[self.lock_key, self.fence_key],
                              args=[self.writer_id, self.lock_ms])
        self.token = token or None
        return self.token is not None

    def renew(self) -> bool:
        """Keep the lock. False means it expired and maybe someone else has it."""
        if self._renew(keys=[self.lock_key], args=[self.writer_id, self.lock_ms]):
            return True
        self.token = None
        return False

    def release(self) -> None:
        self._release(keys=[self.lock_key], args=[self.writer_id])
        self.token = None

    def _check_fence(self) -> None:
        """Inside the batch transaction: record our token unless a newer one exists.
        The row stays locked until commit, so two writers' batches can't interleave."""
        row = self.conn.execute(
            """INSERT INTO history_fence (queue, token) VALUES (%s, %s)
               ON CONFLICT (queue) DO UPDATE SET token = EXCLUDED.token
               WHERE history_fence.token <= EXCLUDED.token
               RETURNING token""", (self.queue_name, self.token)).fetchone()
        if row is None:
            raise FencedOut(f"writer token {self.token} is stale; a newer writer took over")

    def run(self) -> None:
        if threading.current_thread() is threading.main_thread():  # only allowed there
            signal.signal(signal.SIGINT, self._request_stop)
            signal.signal(signal.SIGTERM, self._request_stop)
        log.info("history writer %s started (standby until it holds the lock)", self.writer_id[:8])
        renew_every = self.lock_ms / 1000 / 3
        last_renew = 0.0
        while not self._stopping:
            try:
                if self.token is None:
                    if not self.try_acquire():
                        time.sleep(1)  # another writer is active: wait on standby
                        continue
                    log.info("now the active writer (fencing token %d)", self.token)
                    last_renew = time.monotonic()
                elif time.monotonic() - last_renew >= renew_every:
                    if not self.renew():
                        log.warning("lost the writer lock; back to standby")
                        continue
                    last_renew = time.monotonic()
                applied = self.process_once(block_ms=1000)
                if applied:
                    log.info("applied %d event(s)", applied)
            except FencedOut as e:
                log.warning("%s; back to standby", e)
                self.token = None
            except EventFailed as e:
                log.warning("%s; retrying batch", e)
                time.sleep(1)
            except psycopg.OperationalError as e:
                # PostgreSQL is down. Events wait safely in Redis; retry soon.
                log.warning("PostgreSQL unavailable, retrying: %s", e)
                time.sleep(2)
                try:
                    self.conn = db.connect()
                except psycopg.OperationalError:
                    pass  # still down; the next loop tries again
        if self.token is not None:
            self.release()  # clean shutdown: let a standby take over right away
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
                """INSERT INTO jobs (id, queue, payload, status, max_attempts, priority,
                                    created_at, updated_at)
                   VALUES (%s, %s, %s, 'pending', %s, %s, to_timestamp(%s), to_timestamp(%s))
                   ON CONFLICT (id) DO NOTHING""",  # already recorded: do nothing
                (job_id, e["queue"], Jsonb(json.loads(e["payload"])), int(e["max_attempts"]),
                 int(e.get("priority", 0)), ts, ts),
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

        elif kind == "released":
            # Handed back on shutdown: not a failure, and not counted.
            self._finish_attempt(e["lease_token"], ts, "released", None)
            self._set_job(job_id, ts, status="pending")

        elif kind == "replayed":
            self._set_job(job_id, ts, status="pending", clear_finished=True)

        elif kind == "discarded":
            # Someone gave up on this dead job on purpose. History keeps it.
            sql("""UPDATE jobs SET status = 'discarded', discarded_at = to_timestamp(%s),
                       updated_at = to_timestamp(%s) WHERE id = %s""", (ts, ts, job_id))

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
    config.load_dotenv()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(message)s")
    client = redis.Redis.from_url(config.redis_url())
    conn = db.connect()
    db.init_schema(conn)
    HistoryWriter(client, conn, queue_name=config.queue_name()).run()


if __name__ == "__main__":
    main()
