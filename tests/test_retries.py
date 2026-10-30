import time

from src.core.queue import Queue


def server_now(r):
    sec, usec = r.time()
    return sec + usec / 1_000_000


def test_fail_retries_after_backoff(r):
    q = Queue(r, retry_base_seconds=0.2, jitter=lambda: 1.0)  # jitter=1 -> full backoff
    job_id = q.enqueue({}, max_attempts=3)
    _, token = q.lease()

    assert q.fail(job_id, token, "boom") == "retry"
    assert q.lease() is None  # still backing off
    time.sleep(0.25)
    assert q.lease()[0] == job_id
    assert q.get_job(job_id)["last_error"] == "boom"


def test_backoff_doubles_each_attempt(r):
    q = Queue(r, retry_base_seconds=0.1, jitter=lambda: 1.0)
    job_id = q.enqueue({}, max_attempts=5)

    delays = []
    for _ in range(3):
        while (leased := q.lease()) is None:
            time.sleep(0.02)
        q.fail(job_id, leased[1], "boom")
        delays.append(r.zscore(q.pending_key, job_id) - server_now(r))

    # Expect about 0.1, 0.2, 0.4 seconds
    for actual, expected in zip(delays, [0.1, 0.2, 0.4]):
        assert abs(actual - expected) < 0.03


def test_backoff_is_capped(r):
    q = Queue(r, retry_base_seconds=1, retry_cap_seconds=5, jitter=lambda: 1.0)
    job_id = q.enqueue({}, max_attempts=100)
    r.hset(f"tq:job:{job_id}", "attempts", 20)  # pretend it already failed 20 times
    _, token = q.lease()
    q.fail(job_id, token, "boom")

    delay = r.zscore(q.pending_key, job_id) - server_now(r)
    assert delay <= 5  # not 2^20 seconds


def test_jitter_spreads_out_retries(r):
    """Many jobs failing at once should not all retry at the same moment."""
    q = Queue(r, retry_base_seconds=10)  # real random jitter
    start = server_now(r)  # measure from before the failures, so tiny delays can't look negative
    for _ in range(50):
        job_id = q.enqueue({})
        _, token = q.lease()
        q.fail(job_id, token, "db down")

    elapsed = server_now(r) - start
    delays = [score - start for _, score in r.zrange(q.pending_key, 0, -1, withscores=True)]
    assert all(0 <= d <= 10 + elapsed for d in delays)
    assert max(delays) - min(delays) > 3  # spread out, not bunched together


def test_fail_moves_job_to_dead_letter_after_max_attempts(r):
    q = Queue(r, retry_base_seconds=0)
    job_id = q.enqueue({}, max_attempts=2)

    _, token = q.lease()
    assert q.fail(job_id, token, "boom 1") == "retry"
    _, token = q.lease()
    assert q.fail(job_id, token, "boom 2") == "dead"

    assert r.zscore(q.dead_key, job_id) is not None
    assert q.get_job(job_id)["status"] == "dead"
    assert q.get_job(job_id)["last_error"] == "boom 2"
    assert q.lease() is None  # dead jobs are never handed out


def test_poison_job_that_crashes_workers_is_dead_lettered(r):
    """Failure test: every worker that runs this job dies. It must not loop forever."""
    q = Queue(r, retry_base_seconds=0)
    job_id = q.enqueue({"task": "poison"}, max_attempts=3)

    for _ in range(3):
        assert q.lease(lease_seconds=0.1)[0] == job_id  # worker leases it...
        time.sleep(0.15)                                # ...and crashes
        q.reap_expired()

    assert q.get_job(job_id)["status"] == "dead"
    assert q.get_job(job_id)["last_error"] == "lease expired"
    assert q.lease() is None


def test_stale_worker_cannot_fail_job(r):
    """Failure test: a worker whose lease expired can't fail the job another worker now owns."""
    q = Queue(r, retry_base_seconds=0)
    job_id = q.enqueue({})
    _, token_a = q.lease(lease_seconds=0.1)
    time.sleep(0.15)
    q.reap_expired()
    _, token_b = q.lease()

    assert q.fail(job_id, token_a, "late error") is None
    assert q.get_job(job_id)["status"] == "leased"  # B still owns it
    assert q.ack(job_id, token_b) is True
