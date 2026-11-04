"""The history writer's fast path (bulk statements per batch) must give exactly
the same PostgreSQL rows as applying events one at a time."""

import random
import time

import pytest

from taskqueue.core.queue import Queue
from taskqueue.history import HistoryWriter


def make_events(r) -> list[dict]:
    """Run a messy workload with no writer, then return its events in stream order.
    Covers every event type: enqueued, leased, acked, failed (retry, dead, no-retry),
    lease_expired, released, replayed, discarded."""
    rng = random.Random(7)
    q = Queue(r, retry_base_seconds=0, jitter=lambda: 0)  # retries are ready at once
    for i in range(40):
        q.enqueue({"task": "t", "n": i}, max_attempts=rng.choice([1, 2, 3]),
                  priority=rng.choice([0, 0, 5]))

    def work_until_empty():
        while True:
            q.reap_expired()
            leased = q.lease(lease_seconds=30)
            if leased is None:
                if r.zcard(q.inflight_key) == 0:
                    return  # nothing waiting, nothing running
                time.sleep(0.02)  # let the short leases below expire, then reap them
                continue
            job_id, token = leased
            action = rng.random()
            if action < 0.35:
                q.ack(job_id, token, rng.choice([None, "ok", {"x": 1}, [1, 2]]))
            elif action < 0.6:
                q.fail(job_id, token, f"boom {rng.randint(1, 9)}")
            elif action < 0.7:
                q.fail(job_id, token, "permanent", retry=False)
            elif action < 0.8:
                q.release(job_id, token)
            else:
                q.extend_lease(job_id, token, lease_seconds=0.01)  # will expire

    work_until_empty()
    dead = [m.decode() for m in r.zrange(q.dead_key, 0, -1)]
    assert len(dead) > 4  # the workload really produced dead jobs
    for job_id in dead[: len(dead) // 2]:
        q.replay(job_id)
    work_until_empty()
    dead = [m.decode() for m in r.zrange(q.dead_key, 0, -1)]
    assert len(dead) > 2
    for job_id in dead[:2]:
        q.replay(job_id)  # left waiting: their finished_at must be cleared
    q.discard_dead()

    entries = r.xrange(q.events_key)
    return [{k.decode(): v.decode() for k, v in fields.items()} for _, fields in entries]


def snapshot(pg) -> tuple:
    jobs = pg.execute(
        """SELECT id, queue, payload, status, max_attempts, priority, result, last_error,
                  created_at, updated_at, finished_at, discarded_at
           FROM jobs ORDER BY id""").fetchall()
    attempts = pg.execute(
        """SELECT lease_token, job_id, attempt_number, started_at, finished_at, outcome, error
           FROM job_attempts ORDER BY lease_token""").fetchall()
    return jobs, attempts


def clear(pg) -> None:
    pg.execute("DELETE FROM jobs")  # attempts go too (ON DELETE CASCADE)


@pytest.fixture
def events(r):
    evs = make_events(r)
    kinds = {e["type"] for e in evs}
    assert kinds == {"enqueued", "leased", "acked", "failed", "lease_expired",
                     "released", "replayed", "discarded"}, kinds
    return evs


def test_fast_path_matches_event_by_event(r, pg, events):
    writer = HistoryWriter(r, pg)
    for e in events:  # reference: the original per-event path
        writer._apply(e)
    expected = snapshot(pg)
    assert len(expected[0]) == 40

    # Same events in batches of different sizes, so a job's events are sometimes
    # split across batches and sometimes all in one.
    for size in (len(events), 100, 7, 1):
        clear(pg)
        for i in range(0, len(events), size):
            writer._apply_batch(events[i:i + size])
        assert snapshot(pg) == expected, f"batch size {size}"


def test_fast_path_twice_gives_same_rows(r, pg, events):
    """Redelivery after a crash: applying the same batch again changes nothing."""
    writer = HistoryWriter(r, pg)
    writer._apply_batch(events)
    once = snapshot(pg)
    writer._apply_batch(events)
    assert snapshot(pg) == once


def test_bad_sql_value_falls_back_and_dead_letters_only_that_event(r, pg):
    """An event that passes the Python checks but fails in PostgreSQL (a lease
    token that isn't a UUID) makes the fast path fail; the per-event path then
    finds that one event, and the good events still get written."""
    from taskqueue.history import EventFailed
    q = Queue(r)
    good = q.enqueue({"task": "a"})
    r.xadd(q.events_key, {"type": "leased", "job_id": good, "ts": "1",
                          "lease_token": "not-a-uuid", "attempt": "1"})
    writer = HistoryWriter(r, pg, max_event_failures=2)
    with pytest.raises(EventFailed):
        writer.process_once()
    assert writer.process_once() == 2  # second failure: set aside, rest goes through
    assert pg.execute("SELECT status FROM jobs WHERE id = %s", (good,)).fetchone()[0] == "pending"
    assert pg.execute("SELECT count(*) FROM dead_events").fetchone()[0] == 1
