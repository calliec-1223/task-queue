from taskqueue.core.queue import Queue
from taskqueue.history import HistoryWriter
from taskqueue.metrics import collect


def test_queue_counts_come_from_redis(r):
    q = Queue(r)
    q.enqueue({})                       # ready
    q.enqueue({}, delay_seconds=60)     # waiting
    q.enqueue({})
    q.lease()                           # running

    m = collect(r, None)

    assert m["queue"]["ready"] == 1
    assert m["queue"]["delayed"] == 1
    assert m["queue"]["inflight"] == 1
    assert m["queue"]["history_backlog"] == 4  # 3 enqueued + 1 leased, writer not running
    assert m["history"] is None  # PostgreSQL not given: Redis numbers still work


def test_history_counts_come_from_postgres(r, pg):
    q = Queue(r, retry_base_seconds=0)
    for _ in range(3):
        job_id = q.enqueue({"task": "echo"})
        _, token = q.lease()
        q.ack(job_id, token)
    bad = q.enqueue({"task": "fail"}, max_attempts=1)
    _, token = q.lease()
    q.fail(bad, token, "boom")
    while HistoryWriter(r, pg).process_once():
        pass
    pg.commit()

    h = collect(r, pg)["history"]

    assert h["jobs_by_status"] == {"done": 3, "dead": 1}
    assert sum(h["done_per_second"].values()) == 3
    assert sum(h["failed_per_second"].values()) == 1
    assert {j["task"] for j in h["recent"]} == {"echo", "fail"}


def test_recent_jobs_show_attempts_per_round_and_replays(r, pg):
    """A job that died twice (max 2), was replayed, then failed once more:
    the dashboard should say "1 of 2, replayed 1x", not "3 attempts"."""
    q = Queue(r, retry_base_seconds=0)
    job_id = q.enqueue({"task": "fail"}, max_attempts=2)
    for _ in range(2):
        _, token = q.lease()
        q.fail(job_id, token, "boom")
    q.replay(job_id)
    _, token = q.lease()
    q.fail(job_id, token, "boom")
    while HistoryWriter(r, pg).process_once():
        pass
    pg.commit()

    job = next(j for j in collect(r, pg)["history"]["recent"] if j["id"] == job_id)
    assert (job["attempt"], job["max_attempts"], job["replays"]) == (1, 2, 1)
