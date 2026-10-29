import time

from src.core.queue import Queue


def test_enqueue_then_lease_returns_job(r):
    q = Queue(r)
    job_id = q.enqueue({"task": "hello"})

    job_id_leased, token = q.lease()

    assert job_id_leased == job_id
    job = q.get_job(job_id)
    assert job["status"] == "leased"
    assert job["attempts"] == 1
    assert job["payload"] == {"task": "hello"}


def test_lease_empty_queue_returns_none(r):
    assert Queue(r).lease() is None


def test_delayed_job_not_leased_early(r):
    q = Queue(r)
    q.enqueue({"task": "later"}, delay_seconds=60)
    assert q.lease() is None


def test_delayed_job_leased_once_ready(r):
    q = Queue(r)
    job_id = q.enqueue({"task": "soon"}, delay_seconds=0.2)
    time.sleep(0.3)
    assert q.lease()[0] == job_id


def test_ack_marks_done_and_job_is_not_redelivered(r):
    q = Queue(r)
    job_id = q.enqueue({})
    _, token = q.lease(lease_seconds=0.2)

    assert q.ack(job_id, token) is True
    time.sleep(0.3)
    assert q.reap_expired() == 0  # done jobs are not in inflight anymore
    assert q.get_job(job_id)["status"] == "done"
    assert q.lease() is None


def test_crashed_worker_job_returns_to_queue(r):
    """Failure test: worker leases a job, then dies without acking."""
    q = Queue(r)
    job_id = q.enqueue({"task": "x"})

    q.lease(lease_seconds=0.2)  # worker A "crashes": never acks
    assert q.lease() is None    # job is hidden while the lease is active

    time.sleep(0.3)
    assert q.reap_expired() == 1

    job_id_again, _ = q.lease()  # worker B picks it up
    assert job_id_again == job_id
    assert q.get_job(job_id)["attempts"] == 2


def test_stale_worker_cannot_ack_after_lease_expired(r):
    """Failure test: slow worker A finishes after its lease expired and B took over."""
    q = Queue(r)
    job_id = q.enqueue({})

    _, token_a = q.lease(lease_seconds=0.2)
    time.sleep(0.3)
    q.reap_expired()
    _, token_b = q.lease(lease_seconds=30)

    assert q.ack(job_id, token_a) is False  # A's ack is rejected
    assert q.get_job(job_id)["status"] == "leased"  # B still owns it
    assert q.ack(job_id, token_b) is True


def test_heartbeat_keeps_slow_job_from_being_reaped(r):
    """A job that runs longer than one lease stays leased while heartbeats continue."""
    q = Queue(r)
    job_id = q.enqueue({})
    _, token = q.lease(lease_seconds=0.3)

    for _ in range(3):  # 0.6s total: twice the original lease
        time.sleep(0.2)
        assert q.extend_lease(job_id, token, lease_seconds=0.3) is True
        assert q.reap_expired() == 0

    assert q.ack(job_id, token) is True


def test_heartbeats_stop_when_worker_crashes(r):
    """Failure test: worker heartbeats, then dies. Lease expires and job returns."""
    q = Queue(r)
    job_id = q.enqueue({})
    _, token = q.lease(lease_seconds=0.3)
    q.extend_lease(job_id, token, lease_seconds=0.3)

    time.sleep(0.4)  # worker crashed: no more heartbeats
    assert q.reap_expired() == 1
    assert q.lease()[0] == job_id


def test_stale_worker_cannot_extend_lease(r):
    """Failure test: after its lease expired, a worker's heartbeat is rejected."""
    q = Queue(r)
    job_id = q.enqueue({})
    _, token_a = q.lease(lease_seconds=0.2)
    time.sleep(0.3)
    q.reap_expired()

    assert q.extend_lease(job_id, token_a) is False  # A learns it lost the job
    assert r.zscore(q.inflight_key, job_id) is None  # and didn't sneak it back into inflight
