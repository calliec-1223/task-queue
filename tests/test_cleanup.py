import time

from taskqueue.core.queue import JOB_PREFIX, Queue
from taskqueue.history import HistoryWriter


def finish(q, payload=None):
    job_id = q.enqueue(payload or {})
    _, token = q.lease()
    q.ack(job_id, token, "ok")
    return job_id


def kill(q):
    job_id = q.enqueue({}, max_attempts=1)
    _, token = q.lease()
    q.fail(job_id, token, "boom")
    return job_id


def test_old_done_jobs_are_removed_recent_ones_kept(r):
    q = Queue(r)
    old = finish(q)
    time.sleep(0.2)
    recent = finish(q)

    assert q.cleanup(done_ttl=0.1) == 1
    assert q.get_job(old) is None
    assert q.get_job(recent)["status"] == "done"
    assert r.zscore(q.finished_key, old) is None


def test_old_dead_jobs_leave_the_dead_letter_queue(r):
    q = Queue(r, retry_base_seconds=0)
    old = kill(q)
    time.sleep(0.2)
    recent = kill(q)

    assert q.cleanup(dead_ttl=0.1) == 1
    assert q.get_job(old) is None
    assert r.zscore(q.dead_key, old) is None
    assert r.zscore(q.dead_key, recent) is not None
    assert q.replay(old) is False  # gone for good; history keeps the record


def test_live_jobs_are_never_touched(r):
    q = Queue(r)
    running = q.enqueue({})
    delayed = q.enqueue({}, delay_seconds=60)
    ready = q.enqueue({})
    assert q.lease()[0] == running  # oldest ready job gets leased
    time.sleep(0.1)

    q.cleanup(done_ttl=0, dead_ttl=0)
    for job_id in (running, delayed, ready):
        assert q.get_job(job_id) is not None


def test_cleanup_works_in_batches(r):
    q = Queue(r)
    for _ in range(250):
        finish(q)
    time.sleep(0.1)
    assert q.cleanup(done_ttl=0.05, batch_size=100, max_batches=2) == 200  # capped
    assert q.cleanup(done_ttl=0.05, batch_size=100) == 50                  # the rest


def test_late_messages_cannot_resurrect_a_cleaned_job(r):
    """Failure test: a slow worker reports in after the job was finished
    elsewhere and cleaned up. Nothing should be recreated in Redis."""
    q = Queue(r, retry_base_seconds=0)
    job_id = q.enqueue({})
    _, stale = q.lease(lease_seconds=0.1)
    time.sleep(0.15)
    q.reap_expired()
    _, token = q.lease()
    q.ack(job_id, token, "ok")
    time.sleep(0.1)
    q.cleanup(done_ttl=0.05)

    assert q.ack(job_id, stale, "late") is False
    assert q.extend_lease(job_id, stale) is False
    q.save_checkpoint(job_id, "late")
    assert r.exists(JOB_PREFIX + job_id) == 0


def test_history_survives_cleanup(r, pg):
    q = Queue(r)
    job_id = finish(q)
    writer = HistoryWriter(r, pg)
    while writer.process_once():
        pass
    time.sleep(0.1)
    q.cleanup(done_ttl=0.05)

    assert q.get_job(job_id) is None
    row = pg.execute("SELECT status, result FROM jobs WHERE id = %s", (job_id,)).fetchone()
    assert row == ("done", "ok")
