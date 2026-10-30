import threading
import time

from taskqueue.core.queue import Queue
from taskqueue.worker import Worker


def by_id(q, worker_id, **kw):
    return next((w for w in q.list_workers(**kw) if w["id"] == worker_id), None)


def test_checked_in_worker_is_alive(r):
    q = Queue(r)
    q.worker_beat("w1", host="h", status="idle")
    w = by_id(q, "w1")
    assert w["alive"] and w["status"] == "idle" and w["host"] == "h"


def test_silent_worker_shows_dead_then_is_removed(r):
    """The full cycle the dashboard shows: working -> dead -> removed."""
    q = Queue(r)
    q.worker_beat("w1", status="busy")
    time.sleep(0.3)
    assert by_id(q, "w1", dead_after=0.2)["alive"] is False  # dead, still listed
    assert q.prune_workers(remove_after=0.2) == 1
    assert by_id(q, "w1") is None                              # removed
    assert r.exists(q.worker_prefix + "w1") == 0


def test_prune_keeps_live_workers(r):
    q = Queue(r)
    q.worker_beat("w1")
    assert q.prune_workers(remove_after=60) == 0
    assert by_id(q, "w1") is not None


def test_clean_shutdown_removes_worker_immediately(r):
    q = Queue(r)
    w = Worker(q)
    w.start_presence(interval=0.05)
    assert by_id(q, w.worker_id)["alive"]
    w.stop_presence()
    assert by_id(q, w.worker_id) is None  # never shown as dead


def test_busy_worker_reports_its_job_and_stays_alive(r):
    """A long task blocks the main thread; the presence thread keeps it alive."""
    q = Queue(r)
    job_id = q.enqueue({"task": "sleep", "args": {"seconds": 0.6}})
    w = Worker(q)
    w.start_presence(interval=0.05)
    t = threading.Thread(target=w.run_once)
    t.start()
    time.sleep(0.3)
    mid = by_id(q, w.worker_id, dead_after=0.2)
    assert mid["alive"] and mid["status"] == "busy"
    assert mid["job"] == job_id and mid["task"] == "sleep"
    t.join()
    time.sleep(0.1)
    after = by_id(q, w.worker_id)
    assert after["status"] == "idle" and after["jobs_done"] == "1"
    w.stop_presence()


def test_failed_jobs_are_counted(r):
    q = Queue(r, retry_base_seconds=0)
    q.enqueue({"task": "fail"})
    w = Worker(q)
    w.run_once()
    w._beat()
    assert by_id(q, w.worker_id)["jobs_failed"] == "1"
