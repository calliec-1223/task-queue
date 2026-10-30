import pytest
import redis

from src.core.queue import Queue
from src.history import HistoryWriter


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
