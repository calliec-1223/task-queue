import os
import signal
import subprocess
import sys
import time
from pathlib import Path

from src.core.queue import Queue
from src.worker import Worker

from tests.conftest import TEST_REDIS_URL

REPO_ROOT = Path(__file__).resolve().parent.parent


def test_worker_runs_and_acks_job(r):
    q = Queue(r)
    job_id = q.enqueue({"task": "echo", "args": {"msg": "hi"}})

    assert Worker(q).run_once() is True
    assert q.get_job(job_id)["status"] == "done"


def test_worker_returns_false_when_queue_empty(r):
    assert Worker(Queue(r)).run_once() is False


def test_worker_reports_task_error_as_failure(r):
    q = Queue(r, retry_base_seconds=0)
    job_id = q.enqueue({"task": "fail", "args": {"message": "boom"}}, max_attempts=1)

    Worker(q).run_once()

    job = q.get_job(job_id)
    assert job["status"] == "dead"
    assert job["last_error"] == "RuntimeError: boom"


def test_unknown_task_is_a_failure(r):
    q = Queue(r, retry_base_seconds=0)
    job_id = q.enqueue({"task": "nope"}, max_attempts=1)
    Worker(q).run_once()
    assert "unknown task" in q.get_job(job_id)["last_error"]


def test_heartbeats_keep_long_job_alive(r):
    """A job 3x longer than the lease finishes on its first attempt."""
    q = Queue(r)
    job_id = q.enqueue({"task": "sleep", "args": {"seconds": 0.9}})

    Worker(q, lease_seconds=0.3).run_once()

    job = q.get_job(job_id)
    assert job["status"] == "done"
    assert job["attempts"] == 1  # never reaped and re-run


def test_killed_worker_process_loses_no_job(r):
    """Failure test: a real worker process is killed with SIGKILL mid-job.

    SIGKILL can't be caught, so the worker gets no chance to clean up,
    which is like pulling the power cord. Another worker must finish the job.
    """
    q = Queue(r, retry_base_seconds=0)
    job_id = q.enqueue({"task": "sleep", "args": {"seconds": 1}})

    env = {**os.environ, "REDIS_URL": TEST_REDIS_URL, "LEASE_SECONDS": "1"}
    proc = subprocess.Popen(
        [sys.executable, "-m", "src.worker"], cwd=REPO_ROOT, env=env,
        stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
    )
    try:
        deadline = time.time() + 10
        while q.get_job(job_id)["status"] != "leased":  # wait until it's mid-job
            assert time.time() < deadline, "worker never leased the job"
            time.sleep(0.05)
        proc.send_signal(signal.SIGKILL)
        proc.wait()
    finally:
        proc.kill()

    # A second worker picks up the abandoned job once the lease expires.
    worker_b = Worker(q, lease_seconds=1, reap_interval=0.1)
    deadline = time.time() + 10
    while q.get_job(job_id)["status"] != "done":
        assert time.time() < deadline, "job was never completed"
        if not worker_b.run_once():
            time.sleep(0.05)

    assert q.get_job(job_id)["attempts"] == 2


def test_adaptive_poll_backs_off_while_idle_and_resets_after_a_job(r):
    w = Worker(Queue(r), poll_interval=0.5, min_poll_interval=0.01)
    wait, waits = w.min_poll_interval, []
    for _ in range(8):  # empty queue: 10, 20, 40, ... capped at 500 ms
        waits.append(wait)
        wait = w.next_wait(wait, found_job=False)
    assert waits == [0.01, 0.02, 0.04, 0.08, 0.16, 0.32, 0.5, 0.5]
    assert w.next_wait(0.5, found_job=True) == 0.01  # work arrived: check quickly again


def test_equal_intervals_mean_fixed_polling(r):
    w = Worker(Queue(r), poll_interval=0.5, min_poll_interval=0.5)
    assert w.next_wait(0.5, found_job=False) == 0.5
    assert w.next_wait(0.5, found_job=True) == 0.5
