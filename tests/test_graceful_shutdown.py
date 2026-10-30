import threading
import time

from taskqueue.core.queue import Queue
from taskqueue.history import HistoryWriter
from taskqueue.worker import Worker


def test_release_returns_job_without_using_an_attempt(r):
    q = Queue(r)
    job_id = q.enqueue({}, priority=3, max_attempts=1)
    _, token = q.lease()
    assert q.release(job_id, token) is True

    job = q.get_job(job_id)
    assert job["status"] == "pending" and job["attempts"] == 0
    assert r.zscore(q.inflight_key, job_id) is None
    assert q.lease()[0] == job_id  # ready again right away (and max_attempts=1 still allows it)


def test_released_job_keeps_its_priority(r):
    q = Queue(r)
    high = q.enqueue({}, priority=5)
    _, token = q.lease()
    normal = q.enqueue({})
    q.release(high, token)
    assert q.lease()[0] == high and q.lease()[0] == normal


def test_stale_worker_cannot_release(r):
    q = Queue(r, retry_base_seconds=0)
    job_id = q.enqueue({})
    _, stale = q.lease(lease_seconds=0.1)
    time.sleep(0.15)
    q.reap_expired()
    q.lease()
    assert q.release(job_id, stale) is False
    assert q.get_job(job_id)["status"] == "leased"


def test_history_shows_released_attempt_not_counted_as_failure(r, pg):
    q = Queue(r)
    job_id = q.enqueue({})
    _, t1 = q.lease()
    q.release(job_id, t1)
    _, t2 = q.lease()
    q.ack(job_id, t2, "ok")
    w = HistoryWriter(r, pg)
    while w.process_once():
        pass
    rows = pg.execute("SELECT attempt_number, outcome FROM job_attempts WHERE job_id = %s "
                      "ORDER BY started_at", (job_id,)).fetchall()
    assert rows == [(1, "released"), (1, "done")]  # second run is attempt 1 again


def run_and_stop(q, job_seconds, grace):
    """Start a job, ask the worker to stop mid-job, and report what happened."""
    exits = []
    w = Worker(q, shutdown_grace_seconds=grace, on_shutdown_exit=lambda: exits.append(True))
    job_id = q.enqueue({"task": "sleep", "args": {"seconds": job_seconds}})
    t = threading.Thread(target=w.run_once)
    t.start()
    time.sleep(0.2)
    w.request_stop()
    t.join()
    return job_id, exits


def test_job_that_finishes_within_grace_completes_normally(r):
    q = Queue(r)
    job_id, exits = run_and_stop(q, job_seconds=0.5, grace=2)
    assert q.get_job(job_id)["status"] == "done"
    assert exits == []  # no hand-back needed


def test_long_job_is_handed_back_at_the_shutdown_deadline(r):
    """Failure test: a deploy stops the worker mid-job. The job must go back to
    the queue with its attempt undone, not be killed and counted as a failure."""
    q = Queue(r)
    job_id, exits = run_and_stop(q, job_seconds=1.5, grace=0.4)
    assert exits == [True]
    job = q.get_job(job_id)
    # The sleep finished after the hand-back; its late ack was rejected.
    assert job["status"] == "pending" and job["attempts"] == 0
    assert q.lease()[0] == job_id
