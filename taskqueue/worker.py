"""Worker process: leases jobs, runs them, and reports the result.

Run it with:
    python -m taskqueue.worker

Settings come from environment variables:
    REDIS_URL       default redis://localhost:6379/0
    QUEUE_NAME      default "default"
    LEASE_SECONDS   default 30
    POLL_INTERVAL      default 0.5  (longest an idle worker waits between checks)
    POLL_MIN_INTERVAL  default 0.01 (first wait after the queue goes empty; doubles up to POLL_INTERVAL)
"""

import logging
import os
import signal
import threading
import time

import redis

from taskqueue import config
from dataclasses import dataclass

from taskqueue.core.queue import Queue
from taskqueue.errors import PermanentError
from taskqueue.tasks import TASKS

log = logging.getLogger("worker")


@dataclass
class Task:
    """A task function plus the exception types that should never be retried.

    tasks={"send": send}                                     # every error retried
    tasks={"send": Task(send, no_retry=(BadRequestError,))}  # these go straight to dead
    A task can also raise PermanentError itself for cases only it can recognize.
    """
    fn: callable
    no_retry: tuple = ()


class Worker:
    def __init__(
        self,
        queue: Queue,
        tasks: dict = TASKS,
        lease_seconds: float = 30,
        poll_interval: float = 0.5,
        min_poll_interval: float = 0.01,
        reap_interval: float = 1.0,
        cleanup_interval: float = 60.0,
        on_job_abandoned=None,
    ):
        self.queue = queue
        # Plain functions are allowed too; wrap them so every entry is a Task.
        self.tasks = {name: t if isinstance(t, Task) else Task(t) for name, t in tasks.items()}
        self.lease_seconds = lease_seconds
        # Heartbeat 3x per lease, so two heartbeats can be late or lost
        # before the lease expires (the "slack" from DECISIONS.md #3).
        self.heartbeat_interval = lease_seconds / 3
        # Adaptive polling: when the queue is empty, wait min_poll_interval,
        # then double the wait after each empty check, up to poll_interval.
        # Finding a job resets it. Fast pickup while work flows, cheap when idle.
        # (Set both equal for a fixed interval.)
        self.poll_interval = poll_interval
        self.min_poll_interval = min(min_poll_interval, poll_interval)
        self.reap_interval = reap_interval
        self.cleanup_interval = cleanup_interval
        # Called when a running job hits its time limit or loses its lease. A
        # Python thread can't be killed, so by default the whole worker process
        # exits and its supervisor (Docker's restart policy) starts a fresh one.
        # Tests pass a function instead, so pytest isn't killed.
        self.on_job_abandoned = on_job_abandoned or _exit_process
        self._last_cleanup = 0.0
        self._last_reap = 0.0
        self._stopping = False

    def run(self) -> None:
        """Loop until asked to stop (Ctrl-C or SIGTERM)."""
        # Finish the current job before exiting instead of dying mid-job.
        signal.signal(signal.SIGINT, self._request_stop)
        signal.signal(signal.SIGTERM, self._request_stop)
        log.info("worker started")
        wait = self.min_poll_interval
        while not self._stopping:
            found = self.run_once()
            if not found:
                time.sleep(wait)  # queue empty: poll again after a short wait
            wait = self.next_wait(wait, found)
        log.info("worker stopped")

    def next_wait(self, wait: float, found_job: bool) -> float:
        """Idle wait before the next check: reset after a job, else double (capped)."""
        if found_job:
            return self.min_poll_interval
        return min(wait * 2, self.poll_interval)

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
        # The job's time limit, measured from now (the lease just started).
        deadline = time.monotonic() + float(job.get("timeout", 300))
        heartbeat = threading.Thread(
            target=self._heartbeat, args=(job_id, token, stop_heartbeat, deadline), daemon=True
        )
        heartbeat.start()

        error = None
        permanent = False
        result = None
        try:
            done_before, saved = self.queue.get_checkpoint(job_id)
            if done_before:
                # An earlier attempt finished the task but never got its ack in
                # (crashed or lost its lease). Reuse its result; don't redo the work.
                log.info("job %s: reusing result from an earlier attempt", job_id)
                result = saved
            else:
                result = self.queue.save_checkpoint(job_id, self._run_task(job["payload"]))
        except Exception as e:  # any error in the task counts as a failed attempt
            error = f"{type(e).__name__}: {e}"
            # Never retry if the task said so (PermanentError), or if this error
            # type is on the task's no_retry list.
            task = self.tasks.get(job["payload"].get("task"))
            permanent = isinstance(e, PermanentError) or (
                task is not None and isinstance(e, task.no_retry))
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
            outcome = self.queue.fail(job_id, token, error, retry=not permanent)
            log.warning("job %s failed (%s%s): %s", job_id, outcome or "lease lost",
                        ", permanent" if permanent else "", error)
        return True

    def _run_task(self, payload: dict):
        name = payload.get("task")
        if name not in self.tasks:
            # Retrying can't make an unknown task exist.
            raise PermanentError(f"unknown task: {name!r}")
        return self.tasks[name].fn(**payload.get("args", {}))

    def _heartbeat(self, job_id: str, token: str, stop: threading.Event, deadline: float) -> None:
        # Wake at the next regular heartbeat, or right at the time limit if that
        # comes first, so a hung job is abandoned on time. stop.wait() returns
        # True as soon as stop is set, so this exits promptly when the job ends.
        while not stop.wait(max(0.0, min(self.heartbeat_interval, deadline - time.monotonic()) + 0.01)):
            try:
                status = self.queue.heartbeat(job_id, token, self.lease_seconds)
                if status != "ok":
                    reason = "hit its time limit" if status == "timed_out" else "lost its lease"
                    log.error("job %s %s; abandoning it", job_id, reason)
                    self.on_job_abandoned(job_id, status)
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
        # Less often: remove old finished and dead jobs from Redis memory.
        if now - self._last_cleanup >= self.cleanup_interval:
            self._last_cleanup = now
            removed = self.queue.cleanup()
            if removed:
                log.info("cleaned up %d old job(s) from Redis", removed)

    def _request_stop(self, signum, frame) -> None:
        log.info("stop requested, finishing current job")
        self._stopping = True


def _exit_process(job_id: str, status: str) -> None:
    logging.shutdown()
    # os._exit: stop immediately, even though the main thread is stuck in the task.
    os._exit(3)


def main() -> None:
    config.load_dotenv()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(message)s")
    client = redis.Redis.from_url(config.redis_url())
    queue = Queue(client, name=config.queue_name())
    Worker(
        queue,
        lease_seconds=float(os.environ.get("LEASE_SECONDS", 30)),
        # How long an idle worker waits before checking for jobs again.
        poll_interval=float(os.environ.get("POLL_INTERVAL", 0.5)),
        min_poll_interval=float(os.environ.get("POLL_MIN_INTERVAL", 0.01)),
    ).run()


if __name__ == "__main__":
    main()
