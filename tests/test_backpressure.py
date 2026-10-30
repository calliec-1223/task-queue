import threading

import pytest

from taskqueue import PayloadTooLarge, QueueFull
from taskqueue.core.queue import Queue


def test_full_queue_refuses_new_jobs(r):
    q = Queue(r, max_waiting=3)
    for _ in range(3):
        q.enqueue({})
    with pytest.raises(QueueFull):
        q.enqueue({})
    assert q.count_waiting() == 3
    assert r.xlen(q.events_key) == 3  # the refused job left no trace


def test_delayed_jobs_count_toward_the_limit(r):
    q = Queue(r, max_waiting=2)
    q.enqueue({}, delay_seconds=60)
    q.enqueue({})
    with pytest.raises(QueueFull):
        q.enqueue({})


def test_leasing_frees_space(r):
    q = Queue(r, max_waiting=1)
    q.enqueue({})
    q.lease()  # running jobs aren't "waiting"
    q.enqueue({})


def test_repeated_idempotency_key_still_works_when_full(r):
    """No new job is created, so the producer gets its original job back."""
    q = Queue(r, max_waiting=1)
    job_id = q.enqueue({}, idempotency_key="k")
    assert q.enqueue({}, idempotency_key="k") == job_id


def test_refused_job_does_not_claim_its_idempotency_key(r):
    q = Queue(r, max_waiting=1)
    q.enqueue({})
    with pytest.raises(QueueFull):
        q.enqueue({}, idempotency_key="k")
    q.lease()  # space frees up; the retry with the same key must create the job
    job_id = q.enqueue({}, idempotency_key="k")
    assert q.get_job(job_id) is not None


def test_simultaneous_producers_cannot_overshoot(r):
    """Failure test: 30 producers race to fill a queue with room for 10."""
    q = Queue(r, max_waiting=10)
    results, start = [], threading.Barrier(30)

    def produce():
        start.wait()
        try:
            q.enqueue({})
            results.append("ok")
        except QueueFull:
            results.append("full")

    threads = [threading.Thread(target=produce) for _ in range(30)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    assert results.count("ok") == 10 and q.count_waiting() == 10


def test_payload_too_large(r):
    q = Queue(r, max_payload_bytes=100)
    with pytest.raises(PayloadTooLarge):
        q.enqueue({"data": "x" * 200})
    assert q.count_waiting() == 0


def test_zero_means_no_limit(r):
    q = Queue(r, max_waiting=0)
    for _ in range(50):
        q.enqueue({})
    assert q.count_waiting() == 50
