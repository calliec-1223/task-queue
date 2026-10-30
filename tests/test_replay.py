from taskqueue.core.queue import Queue


def make_dead_job(q, error):
    """Enqueue a job and fail it once with max_attempts=1, so it goes straight to dead."""
    job_id = q.enqueue({}, max_attempts=1)
    _, token = q.lease()
    assert q.fail(job_id, token, error) == "dead"
    return job_id


def test_replay_moves_dead_job_back_with_fresh_attempts(r):
    q = Queue(r)
    job_id = make_dead_job(q, "connection refused")

    assert q.replay(job_id) is True

    assert r.zscore(q.dead_key, job_id) is None
    job = q.get_job(job_id)
    assert job["status"] == "pending"
    assert job["attempts"] == 0
    assert q.lease()[0] == job_id


def test_replayed_job_gets_full_retries_again(r):
    q = Queue(r, retry_base_seconds=0)
    job_id = q.enqueue({}, max_attempts=2)
    for _ in range(2):
        _, token = q.lease()
        q.fail(job_id, token, "db down")
    assert q.get_job(job_id)["status"] == "dead"

    q.replay(job_id)
    _, token = q.lease()
    assert q.fail(job_id, token, "db down") == "retry"  # not dead on the first new failure


def test_replay_refuses_jobs_that_are_not_dead(r):
    """Failure test: replaying a live job must not create a duplicate copy of it."""
    q = Queue(r)
    job_id = q.enqueue({})
    _, token = q.lease()

    assert q.replay(job_id) is False
    assert q.get_job(job_id)["status"] == "leased"
    assert q.count_waiting() == 0  # not put back in the queue


def test_replay_dead_only_replays_matching_errors(r):
    q = Queue(r)
    outage = [make_dead_job(q, "ConnectionError: connection refused") for _ in range(3)]
    bad_input = [make_dead_job(q, "ValueError: bad input") for _ in range(2)]

    assert q.replay_dead(error_contains="connection refused") == 3

    for job_id in outage:
        assert q.get_job(job_id)["status"] == "pending"
    for job_id in bad_input:
        assert q.get_job(job_id)["status"] == "dead"


def test_replay_dead_works_across_many_batches(r):
    """Matching and non-matching jobs are mixed, and there are more than one batch of each."""
    q = Queue(r)
    for i in range(150):
        make_dead_job(q, "connection refused" if i % 2 == 0 else "bad input")

    assert q.replay_dead(error_contains="connection refused", batch_size=10) == 75
    assert r.zcard(q.dead_key) == 75  # all the "bad input" ones are still dead


def test_replay_dead_respects_limit(r):
    q = Queue(r)
    for _ in range(30):
        make_dead_job(q, "connection refused")

    assert q.replay_dead(limit=12, batch_size=5) == 12
    assert r.zcard(q.dead_key) == 18
