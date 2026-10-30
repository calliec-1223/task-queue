from taskqueue import PermanentError, Task
from taskqueue.core.queue import Queue
from taskqueue.worker import Worker


class BadRequest(Exception):
    pass


class BadRequestSubtype(BadRequest):
    pass


class Overloaded(Exception):
    pass


def run(q, tasks, payload, max_attempts=5):
    job_id = q.enqueue(payload, max_attempts=max_attempts)
    Worker(q, tasks=tasks).run_once()
    return q.get_job(job_id)


def raiser(exc):
    def task():
        raise exc
    return task


def test_permanent_error_goes_straight_to_dead(r):
    q = Queue(r, retry_base_seconds=0)
    job = run(q, {"t": raiser(PermanentError("invalid recipe id"))}, {"task": "t"})
    assert job["status"] == "dead"
    assert job["attempts"] == 1  # didn't use up the other 4
    assert job["last_error"] == "PermanentError: invalid recipe id"


def test_listed_error_type_is_not_retried(r):
    q = Queue(r, retry_base_seconds=0)
    tasks = {"t": Task(raiser(BadRequest("400")), no_retry=(BadRequest,))}
    assert run(q, tasks, {"task": "t"})["status"] == "dead"


def test_subtypes_of_listed_errors_count_too(r):
    q = Queue(r, retry_base_seconds=0)
    tasks = {"t": Task(raiser(BadRequestSubtype("401")), no_retry=(BadRequest,))}
    assert run(q, tasks, {"task": "t"})["status"] == "dead"


def test_unlisted_errors_are_still_retried(r):
    q = Queue(r, retry_base_seconds=0)
    tasks = {"t": Task(raiser(Overloaded("529")), no_retry=(BadRequest,))}
    job = run(q, tasks, {"task": "t"})
    assert job["status"] == "pending"  # will be retried with backoff


def test_plain_functions_still_work(r):
    q = Queue(r)
    job = run(q, {"t": lambda: "ok"}, {"task": "t"})
    assert job["status"] == "done"


def test_unknown_task_is_permanent(r):
    q = Queue(r, retry_base_seconds=0)
    job = run(q, {}, {"task": "nope"})
    assert job["status"] == "dead" and job["attempts"] == 1
    assert "unknown task" in job["last_error"]


def test_stale_worker_still_cannot_fail_permanently(r):
    """retry=False doesn't bypass the lease-token check."""
    import time
    q = Queue(r, retry_base_seconds=0)
    job_id = q.enqueue({})
    _, stale = q.lease(lease_seconds=0.1)
    time.sleep(0.15)
    q.reap_expired()
    q.lease()
    assert q.fail(job_id, stale, "late", retry=False) is None
    assert q.get_job(job_id)["status"] == "leased"
