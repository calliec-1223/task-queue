import threading
import time

from taskqueue.core.queue import JOB_PREFIX, Queue
from taskqueue.worker import Worker


def server_now(r):
    sec, usec = r.time()
    return sec + usec / 1_000_000


def test_lease_never_extends_past_the_time_limit(r):
    q = Queue(r)
    job_id = q.enqueue({}, timeout_seconds=0.3)
    _, token = q.lease(lease_seconds=30)
    # Lease expiry is capped at start + 0.3 s, not now + 30 s.
    assert r.zscore(q.inflight_key, job_id) - server_now(r) < 0.35
    assert q.heartbeat(job_id, token, lease_seconds=30) == "ok"
    assert r.zscore(q.inflight_key, job_id) - server_now(r) < 0.35  # still capped


def test_heartbeat_reports_timeout_then_reaper_retries(r):
    """Failure test: a hung task keeps heartbeating, but past its limit the
    heartbeat is refused and the job is retried like any failure."""
    q = Queue(r, retry_base_seconds=0)
    job_id = q.enqueue({}, timeout_seconds=0.2, max_attempts=2)
    _, token = q.lease()
    time.sleep(0.25)

    assert q.heartbeat(job_id, token) == "timed_out"
    assert q.reap_expired() == 1
    job = q.get_job(job_id)
    assert job["status"] == "pending"
    assert job["last_error"] == "timed out after 0.2 s"
    assert q.lease()[0] == job_id  # second attempt gets a fresh time limit


def test_job_that_always_hangs_is_dead_lettered(r):
    q = Queue(r, retry_base_seconds=0)
    job_id = q.enqueue({}, timeout_seconds=0.1, max_attempts=2)
    for _ in range(2):
        q.lease()
        time.sleep(0.15)
        q.reap_expired()
    assert q.get_job(job_id)["status"] == "dead"
    assert q.get_job(job_id)["last_error"].startswith("timed out")


def test_crash_is_still_labelled_lease_expired(r):
    q = Queue(r, retry_base_seconds=0)
    job_id = q.enqueue({}, timeout_seconds=60)
    q.lease(lease_seconds=0.1)
    time.sleep(0.15)
    q.reap_expired()
    assert q.get_job(job_id)["last_error"] == "lease expired"


def test_heartbeat_reports_lost_lease(r):
    q = Queue(r, retry_base_seconds=0)
    job_id = q.enqueue({})
    _, stale = q.lease(lease_seconds=0.1)
    time.sleep(0.15)
    q.reap_expired()
    q.lease()
    assert q.heartbeat(job_id, stale) == "lost"


def test_worker_abandons_a_hung_task(r):
    """The worker notices the refused heartbeat and gives up the job (in
    production by exiting; here we record the call instead)."""
    q = Queue(r)
    q.enqueue({"task": "sleep", "args": {"seconds": 1.5}}, timeout_seconds=0.4)
    abandoned = []
    worker = Worker(q, lease_seconds=0.3,
                    on_job_abandoned=lambda job_id, status: abandoned.append(status))
    t = threading.Thread(target=worker.run_once)
    t.start()
    time.sleep(0.8)
    assert abandoned == ["timed_out"]  # noticed while the task was still stuck
    t.join()


def test_job_leased_before_upgrade_has_no_limit(r):
    """Jobs leased by older code have no started_at; heartbeats must keep working."""
    q = Queue(r)
    job_id = q.enqueue({})
    _, token = q.lease()
    r.hdel(JOB_PREFIX + job_id, "started_at", "timeout")
    assert q.heartbeat(job_id, token) == "ok"


def test_worker_gives_up_right_at_the_limit_not_the_next_heartbeat(r):
    """With a 30 s lease, heartbeats are 10 s apart; a 0.4 s limit must still
    be noticed at about 0.4 s."""
    q = Queue(r)
    q.enqueue({"task": "sleep", "args": {"seconds": 1.5}}, timeout_seconds=0.4)
    abandoned = []
    worker = Worker(q, lease_seconds=30,
                    on_job_abandoned=lambda job_id, status: abandoned.append(time.monotonic()))
    start = time.monotonic()
    t = threading.Thread(target=worker.run_once)
    t.start()
    t.join()
    assert len(abandoned) == 1 and abandoned[0] - start < 0.8
