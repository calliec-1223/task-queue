import threading
import time

from taskqueue.core.queue import Queue


def timed_wait(q, results, max_wait=3.0):
    start = time.monotonic()
    woke = q.wait_for_work(max_wait=max_wait)
    results.append((woke, time.monotonic() - start))


def test_ready_job_rings_the_doorbell_delayed_job_does_not(r):
    q = Queue(r)
    q.enqueue({}, delay_seconds=60)
    assert r.llen(q.wake_key) == 0
    q.enqueue({})
    assert r.llen(q.wake_key) == 1


def test_doorbell_is_capped(r):
    q = Queue(r, max_waiting=0)
    for _ in range(1100):
        q.enqueue({})
    assert r.llen(q.wake_key) == 1000


def test_enqueue_wakes_a_waiting_worker_immediately(r):
    q = Queue(r)
    results = []
    t = threading.Thread(target=timed_wait, args=(q, results))
    t.start()
    time.sleep(0.2)
    q.enqueue({})
    t.join()
    woke, waited = results[0]
    assert woke and waited < 0.5  # not the full 3 s


def test_wait_ends_when_the_next_delayed_job_is_due(r):
    q = Queue(r)
    q.enqueue({}, delay_seconds=0.3)
    results = []
    timed_wait(q, results)
    _, waited = results[0]
    assert 0.2 < waited < 0.8
    assert q.lease() is not None  # it really is ready now


def test_one_job_wakes_one_worker_not_all(r):
    """No thundering herd: two idle workers, one new job, one wake-up."""
    q = Queue(r)
    results = []
    threads = [threading.Thread(target=timed_wait, args=(q, results, 1.0)) for _ in range(2)]
    for t in threads:
        t.start()
    time.sleep(0.2)
    q.enqueue({})
    for t in threads:
        t.join()
    assert sorted(woke for woke, _ in results) == [False, True]


def test_retry_and_replay_ring_when_ready(r):
    q = Queue(r, retry_base_seconds=0)
    job_id = q.enqueue({}, max_attempts=1)
    r.delete(q.wake_key)
    _, token = q.lease()
    q.fail(job_id, token, "boom")  # dead: no ring
    assert r.llen(q.wake_key) == 0
    q.replay(job_id)
    assert r.llen(q.wake_key) == 1


def test_direct_leases_use_up_tokens_so_they_cannot_go_stale(r):
    """Regression test: busy workers leasing directly must remove tokens, or
    the list fills with stale tokens and idle workers spin instead of sleeping."""
    q = Queue(r)
    for _ in range(5):
        q.enqueue({})
    for _ in range(5):
        q.lease()  # busy workers grabbing work directly
    assert r.llen(q.wake_key) == 0
    assert q.wait_for_work(max_wait=0.3) is False  # so an idle worker really sleeps


def test_woken_worker_does_not_use_up_a_second_token(r):
    q = Queue(r)
    q.enqueue({})
    q.enqueue({})
    assert q.wait_for_work(max_wait=1) is True       # took job 1's token
    q.lease(consume_token=False)                      # ...so the lease doesn't take job 2's
    assert r.llen(q.wake_key) == 1                    # job 2 can still wake someone
