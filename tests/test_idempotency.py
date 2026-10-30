import threading
import time

from taskqueue.core.queue import Queue
from taskqueue.worker import Worker


# ---- Idempotency keys: duplicate submissions ---------------------------------

def test_same_key_returns_same_job(r):
    q = Queue(r)
    first = q.enqueue({"task": "echo"}, idempotency_key="user7:msg-1")
    second = q.enqueue({"task": "echo"}, idempotency_key="user7:msg-1")

    assert first == second
    assert q.count_waiting() == 1
    assert r.xlen(q.events_key) == 1  # only one "enqueued" history event


def test_different_keys_and_no_key_create_separate_jobs(r):
    q = Queue(r)
    a = q.enqueue({}, idempotency_key="a")
    b = q.enqueue({}, idempotency_key="b")
    c, d = q.enqueue({}), q.enqueue({})
    assert len({a, b, c, d}) == 4


def test_key_expires(r):
    q = Queue(r)
    first = q.enqueue({}, idempotency_key="k", idempotency_ttl=1)
    time.sleep(1.1)
    assert q.enqueue({}, idempotency_key="k") != first


def test_repeated_key_works_even_after_job_finished(r):
    """A retry that arrives after the job is done still gets the original job."""
    q = Queue(r)
    job_id = q.enqueue({}, idempotency_key="k")
    _, token = q.lease()
    q.ack(job_id, token, result="answer")
    assert q.enqueue({}, idempotency_key="k") == job_id


def test_keys_are_per_queue(r):
    assert Queue(r, name="a").enqueue({}, idempotency_key="k") != \
        Queue(r, name="b").enqueue({}, idempotency_key="k")


def test_simultaneous_duplicate_submissions_create_one_job(r):
    """Failure test: 20 clients submit the same key at the same moment."""
    q = Queue(r)
    ids, start = [], threading.Barrier(20)

    def submit():
        start.wait()  # all threads fire together
        ids.append(q.enqueue({}, idempotency_key="same"))

    threads = [threading.Thread(target=submit) for _ in range(20)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    assert len(set(ids)) == 1
    assert q.count_waiting() == 1


# ---- Checkpoints: duplicate execution -----------------------------------------

def counting_task(calls):
    def task():
        calls.append(1)
        return "expensive answer"
    return {"work": task}


def test_rerun_after_crash_before_ack_reuses_result(r):
    """Failure test: worker A finishes the task, then dies before acking.
    The job is redelivered; worker B must not redo the work."""
    q = Queue(r, retry_base_seconds=0)
    job_id = q.enqueue({"task": "work"})
    calls = []

    _, token_a = q.lease(lease_seconds=0.1)
    q.save_checkpoint(job_id, "expensive answer")  # A finished the task...
    calls.append(1)                                 # ...(that's one real call)
    time.sleep(0.15)                                # ...then crashed: no ack
    q.reap_expired()

    Worker(q, tasks=counting_task(calls)).run_once()  # worker B

    assert len(calls) == 1  # B reused A's result instead of calling again
    job = q.get_job(job_id)
    assert job["status"] == "done" and job["attempts"] == 2


def test_overlapping_runs_report_one_result(r):
    """Documented limit: if A's lease expires while A is still running, B also
    runs the task (both do the work). Checkpoints keep the outcome consistent:
    the first finisher's result is the one recorded."""
    q = Queue(r, retry_base_seconds=0)
    job_id = q.enqueue({"task": "work"})
    _, token_a = q.lease(lease_seconds=0.1)
    time.sleep(0.15)
    q.reap_expired()
    _, token_b = q.lease()

    assert q.save_checkpoint(job_id, "A's answer") == "A's answer"  # A finishes first
    assert q.ack(job_id, token_a, "A's answer") is False             # but lost its lease
    stored = q.save_checkpoint(job_id, "B's answer")                 # B finishes
    assert stored == "A's answer"                                     # gets A's result back
    assert q.ack(job_id, token_b, stored) is True


def test_checkpoint_only_after_success(r):
    """A failed attempt leaves no checkpoint, so the retry really runs the task."""
    q = Queue(r, retry_base_seconds=0)
    job_id = q.enqueue({"task": "fail"})
    Worker(q).run_once()
    assert q.get_checkpoint(job_id) == (False, None)


def test_none_result_still_counts_as_done(r):
    q = Queue(r)
    job_id = q.enqueue({})
    q.save_checkpoint(job_id, None)
    assert q.get_checkpoint(job_id) == (True, None)
