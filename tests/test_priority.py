import time

from taskqueue.core.queue import Queue
from taskqueue.history import HistoryWriter


def lease_order(q, n):
    return [q.lease()[0] for _ in range(n)]


def test_higher_priority_runs_first(r):
    q = Queue(r)
    low = q.enqueue({}, priority=-1)
    normal = q.enqueue({})
    high = q.enqueue({}, priority=5)
    assert lease_order(q, 3) == [high, normal, low]


def test_same_priority_is_first_come_first_served(r):
    q = Queue(r)
    ids = [q.enqueue({}, priority=2) for _ in range(5)]
    time.sleep(0.01)
    assert lease_order(q, 5) == ids


def test_aging_lets_a_long_waiting_job_beat_a_new_high_priority_job(r):
    """No starvation: one priority level is worth 0.2 s of waiting here, so a
    normal job that has waited 0.3 s outranks a brand-new priority-1 job."""
    q = Queue(r, priority_step_seconds=0.2)
    old_normal = q.enqueue({})
    time.sleep(0.3)
    new_high = q.enqueue({}, priority=1)
    assert lease_order(q, 2) == [old_normal, new_high]


def test_but_a_short_wait_does_not(r):
    q = Queue(r, priority_step_seconds=0.2)
    normal = q.enqueue({})
    time.sleep(0.05)
    high = q.enqueue({}, priority=1)
    assert lease_order(q, 2) == [high, normal]


def test_priority_never_lets_a_delayed_job_run_early(r):
    """Failure test for the rejected design (priority folded into one score):
    a high-priority job delayed by 60 s must still wait 60 s."""
    q = Queue(r)
    q.enqueue({}, priority=1000, delay_seconds=60)
    assert q.lease() is None


def test_delayed_job_keeps_its_priority_once_ready(r):
    q = Queue(r)
    normal = q.enqueue({})
    high = q.enqueue({}, priority=5, delay_seconds=0.1)
    time.sleep(0.15)
    assert lease_order(q, 2) == [high, normal]


def test_retried_job_keeps_its_priority(r):
    q = Queue(r, retry_base_seconds=0.05, jitter=lambda: 1.0)
    high = q.enqueue({}, priority=5)
    _, token = q.lease()
    q.fail(high, token, "boom")  # back to "delayed" for 50 ms of backoff
    normal = q.enqueue({})
    time.sleep(0.1)
    assert lease_order(q, 2) == [high, normal]


def test_replayed_job_keeps_its_priority(r):
    q = Queue(r)
    high = q.enqueue({}, priority=5, max_attempts=1)
    _, token = q.lease()
    q.fail(high, token, "boom")  # dead
    normal = q.enqueue({})
    q.replay(high)
    assert lease_order(q, 2) == [high, normal]


def test_aging_step_is_shared_by_every_queue_object(r):
    """The step must be the same for all producers and workers, so the first
    one stored in Redis wins; a later Queue can't silently change it."""
    Queue(r, priority_step_seconds=0.2)
    Queue(r, priority_step_seconds=999)
    assert float(r.hget("tq:default:config", "priority_step")) == 0.2


def test_priority_is_recorded_in_history(r, pg):
    q = Queue(r)
    job_id = q.enqueue({}, priority=3)
    writer = HistoryWriter(r, pg)
    while writer.process_once():
        pass
    assert pg.execute("SELECT priority FROM jobs WHERE id = %s", (job_id,)).fetchone()[0] == 3
