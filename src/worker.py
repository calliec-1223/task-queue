"""Worker process: leases jobs, runs them, and reports the result.

Run it with:
    python -m src.worker

Settings come from environment variables:
    REDIS_URL       default redis://localhost:6379/0
    QUEUE_NAME      default "default"
    LEASE_SECONDS   default 30
"""

import logging
import os
import signal
import threading
import time

import redis

from src import config
from src.core.queue import Queue
from src.tasks import TASKS

log = logging.getLogger("worker")


class Worker:
    def __init__(
        self,
        queue: Queue,
        tasks: dict = TASKS,
        lease_seconds: float = 30,
        poll_interval: float = 0.5,
        reap_interval: float = 1.0,
    ):
        self.queue = queue
        self.tasks = tasks
        self.lease_seconds = lease_seconds
        # Heartbeat 3x per lease, so two heartbeats can be late or lost
        # before the lease expires (the "slack" from DECISIONS.md #3).
        self.heartbeat_interval = lease_seconds / 3
        self.poll_interval = poll_interval
        self.reap_interval = reap_interval
        self._last_reap = 0.0
        self._stopping = False

    def run(self) -> None:
        """Loop until asked to stop (Ctrl-C or SIGTERM)."""
        # Finish the current job before exiting instead of dying mid-job.
        signal.signal(signal.SIGINT, self._request_stop)
        signal.signal(signal.SIGTERM, self._request_stop)
        log.info("worker started")
        while not self._stopping:
            if not self.run_once():
                time.sleep(self.poll_interval)  # queue empty: poll again shortly
        log.info("worker stopped")

    def run_once(self) -> bool:
        """Lease and run at most one job. Returns False if no job was ready."""
        self._maybe_reap()

        leased = self.queue.lease(self.lease_seconds)
        if leased is None:
            return False
        job_id, token = leased
        job = self.queue.get_job(job_id)
        log.info("job %s leased (attempt %d/%d)", job_id, job["attempts"], job["max_attempts"])

        # Heartbeat in a background thread while the task runs.
        stop_heartbeat = threading.Event()
        heartbeat = threading.Thread(
            target=self._heartbeat, args=(job_id, token, stop_heartbeat), daemon=True
        )
        heartbeat.start()

        error = None
        result = None
        try:
            result = self._run_task(job["payload"])
        except Exception as e:  # any error in the task counts as a failed attempt
            error = f"{type(e).__name__}: {e}"
        finally:
            # Stop heartbeats before ack/fail, so a heartbeat can't race the ack.
            stop_heartbeat.set()
            heartbeat.join()

        if error is None:
            if self.queue.ack(job_id, token, result):
                log.info("job %s done", job_id)
            else:
                log.warning("job %s: lease was lost before ack (another worker owns it)", job_id)
        else:
            outcome = self.queue.fail(job_id, token, error)
            log.warning("job %s failed (%s): %s", job_id, outcome or "lease lost", error)
        return True

    def _run_task(self, payload: dict):
        name = payload.get("task")
        if name not in self.tasks:
            raise ValueError(f"unknown task: {name!r}")
        return self.tasks[name](**payload.get("args", {}))

    def _heartbeat(self, job_id: str, token: str, stop: threading.Event) -> None:
        # stop.wait() returns True as soon as stop is set, so this exits promptly.
        while not stop.wait(self.heartbeat_interval):
            try:
                if not self.queue.extend_lease(job_id, token, self.lease_seconds):
                    log.warning("job %s: heartbeat rejected, lease lost", job_id)
                    return
            except redis.RedisError as e:
                # Keep trying: the lease survives a few missed heartbeats.
                log.warning("job %s: heartbeat failed: %s", job_id, e)

    def _maybe_reap(self) -> None:
        # Every worker also acts as a reaper. That's safe because reap_expired
        # is one atomic script: two workers can't reap the same job twice.
        now = time.monotonic()
        if now - self._last_reap >= self.reap_interval:
            self._last_reap = now
            reaped = self.queue.reap_expired()
            if reaped:
                log.info("reaped %d expired lease(s)", reaped)

    def _request_stop(self, signum, frame) -> None:
        log.info("stop requested, finishing current job")
        self._stopping = True


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(message)s")
    client = redis.Redis.from_url(config.REDIS_URL)
    queue = Queue(client, name=config.QUEUE_NAME)
    Worker(queue, lease_seconds=float(os.environ.get("LEASE_SECONDS", 30))).run()


if __name__ == "__main__":
    main()
