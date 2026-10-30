import pytest
import redis

from taskqueue.core.queue import Queue
from taskqueue.history import HistoryWriter


def drain(writer):
    """Apply every waiting event."""
    while writer.process_once():
        pass


def job_row(pg, job_id):
    return pg.execute(
        "SELECT status, result, last_error, finished_at FROM jobs WHERE id = %s", (job_id,)
    ).fetchone()


def attempts(pg, job_id):
    return pg.execute(
        "SELECT attempt_number, outcome, error FROM job_attempts WHERE job_id = %s ORDER BY started_at",
        (job_id,),
    ).fetchall()


def test_successful_job_is_recorded(r, pg):
    q = Queue(r)
    job_id = q.enqueue({"task": "echo"})
    _, token = q.lease()
    q.ack(job_id, token, result={"answer": 42})

    drain(HistoryWriter(r, pg))

    status, result, _, finished_at = job_row(pg, job_id)
    assert status == "done"
    assert result == {"answer": 42}
    assert finished_at is not None
    assert attempts(pg, job_id) == [(1, "done", None)]


def test_retry_then_success_records_both_attempts(r, pg):
    q = Queue(r, retry_base_seconds=0)
    job_id = q.enqueue({})
    _, t1 = q.lease()
    q.fail(job_id, t1, "ValueError: boom")
    _, t2 = q.lease()
    q.ack(job_id, t2)

    drain(HistoryWriter(r, pg))

    assert job_row(pg, job_id)[0] == "done"
    assert attempts(pg, job_id) == [(1, "error", "ValueError: boom"), (2, "done", None)]


def test_crashed_worker_attempts_recorded_as_lease_expired(r, pg):
    import time
    q = Queue(r, retry_base_seconds=0)
    job_id = q.enqueue({}, max_attempts=1)
    q.lease(lease_seconds=0.1)
    time.sleep(0.15)
    q.reap_expired()

    drain(HistoryWriter(r, pg))

    assert job_row(pg, job_id)[0] == "dead"
    assert attempts(pg, job_id) == [(1, "lease_expired", "lease expired")]


def test_replay_keeps_earlier_attempts(r, pg):
    """Decision #8's promise: Redis resets attempts on replay, PostgreSQL keeps them."""
    q = Queue(r, retry_base_seconds=0)
    job_id = q.enqueue({}, max_attempts=1)
    _, t1 = q.lease()
    q.fail(job_id, t1, "db down")
    q.replay(job_id)
    _, t2 = q.lease()
    q.ack(job_id, t2)

    drain(HistoryWriter(r, pg))

    assert job_row(pg, job_id)[0] == "done"
    # Two "attempt 1"s: one before the replay, one after. Both kept.
    assert attempts(pg, job_id) == [(1, "error", "db down"), (1, "done", None)]


def test_rejected_stale_ack_writes_no_history(r, pg):
    """The event is written in the same atomic script as the change, so no change = no event."""
    import time
    q = Queue(r, retry_base_seconds=0)
    job_id = q.enqueue({})
    _, token_a = q.lease(lease_seconds=0.1)
    time.sleep(0.15)
    q.reap_expired()
    _, token_b = q.lease()
    assert q.ack(job_id, token_a) is False  # rejected

    drain(HistoryWriter(r, pg))

    assert job_row(pg, job_id)[0] == "leased"  # B still running; A's ack left no trace


def test_writer_crash_after_commit_does_not_duplicate_history(r, pg, monkeypatch):
    """Failure test: writer commits to PostgreSQL, then crashes before confirming to Redis."""
    q = Queue(r)
    job_id = q.enqueue({})
    _, token = q.lease()
    q.ack(job_id, token)

    writer = HistoryWriter(r, pg)

    def crash(*args):
        raise ConnectionError("writer died before XACK")
    monkeypatch.setattr(writer.r, "xack", crash)
    with pytest.raises(ConnectionError):
        writer.process_once()
    monkeypatch.undo()

    # Restarted writer (same consumer name) gets the unconfirmed events again.
    drain(HistoryWriter(r, pg))

    assert job_row(pg, job_id)[0] == "done"
    assert len(attempts(pg, job_id)) == 1  # applied twice, recorded once
    assert r.xlen(q.events_key) == 0  # and now confirmed


def test_postgres_outage_loses_no_history(r, pg):
    """Failure test: workers keep going while the writer is down; events wait in Redis."""
    q = Queue(r)
    ids = []
    for _ in range(20):  # writer not running at all
        job_id = q.enqueue({})
        _, token = q.lease()
        q.ack(job_id, token)
        ids.append(job_id)
    assert r.xlen(q.events_key) == 60  # 3 events per job, all waiting

    drain(HistoryWriter(r, pg))  # writer comes back

    done = pg.execute("SELECT count(*) FROM jobs WHERE status = 'done'").fetchone()[0]
    assert done == 20


def add_bad_event(r, q):
    """An 'enqueued' event whose payload isn't valid JSON: fails every time."""
    return r.xadd(q.events_key, {"type": "enqueued", "job_id": "11111111-1111-1111-1111-111111111111",
                                 "ts": "1", "queue": "default", "payload": "{not json",
                                 "max_attempts": "1"})


def test_poison_event_is_dead_lettered_and_unblocks_history(r, pg):
    """Failure test: a bad event must not block the events behind it forever."""
    from taskqueue.history import EventFailed
    q = Queue(r)
    before = q.enqueue({"task": "a"})
    add_bad_event(r, q)
    after = q.enqueue({"task": "b"})  # stuck behind the bad one

    writer = HistoryWriter(r, pg, max_event_failures=3)
    for _ in range(2):  # failures 1 and 2: batch rolled back and retried
        with pytest.raises(EventFailed):
            writer.process_once()
        assert pg.execute("SELECT count(*) FROM jobs").fetchone()[0] == 0  # nothing half-saved

    assert writer.process_once() == 3  # failure 3: set aside, batch goes through

    for job_id in (before, after):
        assert job_row(pg, job_id)[0] == "pending"
    dead = pg.execute("SELECT fields->>'payload', failures, error FROM dead_events").fetchall()
    assert len(dead) == 1
    assert dead[0][0] == "{not json" and dead[0][1] == 3 and "JSONDecodeError" in dead[0][2]
    assert r.xlen(q.events_key) == 0
    assert r.hlen(writer.failures_key) == 0


def test_event_that_fails_once_is_retried_not_dead_lettered(r, pg, monkeypatch):
    """A one-off glitch gets another chance (why we picked retry-N-times)."""
    from taskqueue.history import EventFailed
    q = Queue(r)
    job_id = q.enqueue({})
    writer = HistoryWriter(r, pg)

    real_apply = writer._apply
    calls = {"n": 0}

    def flaky(event):
        calls["n"] += 1
        if calls["n"] == 1:
            raise RuntimeError("one-off glitch")
        real_apply(event)
    monkeypatch.setattr(writer, "_apply", flaky)

    with pytest.raises(EventFailed):
        writer.process_once()
    writer.process_once()

    assert job_row(pg, job_id)[0] == "pending"
    assert pg.execute("SELECT count(*) FROM dead_events").fetchone()[0] == 0


def test_postgres_outage_does_not_count_as_event_failure(r, pg, monkeypatch):
    """Failure test: an outage must never push good events into dead_events."""
    import psycopg
    q = Queue(r)
    q.enqueue({})
    writer = HistoryWriter(r, pg, max_event_failures=1)  # even with the strictest setting

    def down(event):
        raise psycopg.OperationalError("connection refused")
    monkeypatch.setattr(writer, "_apply", down)
    for _ in range(3):
        with pytest.raises(psycopg.OperationalError):
            writer.process_once()
    monkeypatch.undo()

    assert r.hlen(writer.failures_key) == 0
    pg.rollback()
    drain(HistoryWriter(r, pg))
    assert pg.execute("SELECT count(*) FROM dead_events").fetchone()[0] == 0
    assert pg.execute("SELECT count(*) FROM jobs").fetchone()[0] == 1
