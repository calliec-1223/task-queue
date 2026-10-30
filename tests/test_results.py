from taskqueue.core.queue import Queue
from taskqueue.history import HistoryWriter
from taskqueue.results import get_job


def drain(r, pg, queue_name="default"):
    writer = HistoryWriter(r, pg, queue_name=queue_name)
    while writer.process_once():
        pass


def test_done_job_has_its_result(r, pg):
    q = Queue(r)
    job_id = q.enqueue({"task": "echo", "user_id": 7})
    _, token = q.lease()
    q.ack(job_id, token, result="hello")
    drain(r, pg)

    job = get_job(pg, job_id)
    assert job["status"] == "done"
    assert job["result"] == "hello"
    assert job["payload"]["user_id"] == 7  # callers use this to check ownership


def test_not_yet_recorded_returns_none(r, pg):
    job_id = Queue(r).enqueue({})  # history writer hasn't run
    assert get_job(pg, job_id) is None


def test_malformed_id_returns_none(pg):
    assert get_job(pg, "not-a-uuid") is None
    assert get_job(pg, "'; DROP TABLE jobs; --") is None


def test_separate_queues_have_separate_history_streams(r, pg):
    whisk = Queue(r, name="whisk")
    job_id = whisk.enqueue({"task": "whisk_chat"})
    drain(r, pg, queue_name="default")  # wrong queue's writer: sees nothing
    assert get_job(pg, job_id) is None
    drain(r, pg, queue_name="whisk")
    assert get_job(pg, job_id)["status"] == "pending"
