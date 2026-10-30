from taskqueue.core.queue import JOB_PREFIX, Queue
from taskqueue.history import HistoryWriter


def make_dead(q, error):
    job_id = q.enqueue({}, max_attempts=1)
    _, token = q.lease()
    q.fail(job_id, token, error)
    return job_id


def drain(r, pg):
    writer = HistoryWriter(r, pg)
    while writer.process_once():
        pass


def test_discard_removes_dead_job_from_redis(r):
    q = Queue(r)
    job_id = make_dead(q, "boom")
    assert q.discard(job_id) is True
    assert r.zscore(q.dead_key, job_id) is None
    assert r.exists(JOB_PREFIX + job_id) == 0
    assert q.replay(job_id) is False  # nothing left to replay


def test_discard_refuses_live_jobs(r):
    """Failure test: a mistyped id must not delete a job that's still running."""
    q = Queue(r)
    job_id = q.enqueue({})
    q.lease()
    assert q.discard(job_id) is False
    assert q.get_job(job_id)["status"] == "leased"


def test_history_records_discarded(r, pg):
    q = Queue(r)
    job_id = make_dead(q, "boom")
    q.discard(job_id)
    drain(r, pg)
    status, discarded_at, last_error = pg.execute(
        "SELECT status, discarded_at, last_error FROM jobs WHERE id = %s", (job_id,)).fetchone()
    assert status == "discarded" and discarded_at is not None
    assert last_error == "boom"  # history keeps why it died


def test_discard_dead_filters_by_error_across_batches(r):
    q = Queue(r)
    for i in range(150):
        make_dead(q, "always fails" if i % 2 else "connection refused")
    assert q.discard_dead("always fails", batch_size=10) == 75
    assert r.zcard(q.dead_key) == 75  # the outage ones are still there to replay


def test_discard_dead_respects_limit(r):
    q = Queue(r)
    for _ in range(12):
        make_dead(q, "x")
    assert q.discard_dead(limit=5, batch_size=2) == 5
    assert r.zcard(q.dead_key) == 7
