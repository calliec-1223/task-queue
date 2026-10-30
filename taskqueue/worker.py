"""Worker process: leases jobs, runs them, and reports the result.

Run it with:
    python -m taskqueue.worker

Settings come from environment variables:
    REDIS_URL       default redis://localhost:6379/0
    QUEUE_NAME      default "default"
    LEASE_SECONDS   default 30
    POLL_INTERVAL      default 0.5  (longest an idle worker waits between checks)
    POLL_MIN_INTERVAL  default 0.01 (first wait after the queue goes empty; doubles up to POLL_INTERVAL)
    WAKE_MODE          "doorbell" (default: idle workers block until a job is ready) or "poll"
"""

import logging
import os
import signal
import socket
import uuid
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
        wake_mode: str = "doorbell",  # "doorbell" (default) or "poll" (for comparison)
        min_poll_interval: float = 0.01,
        reap_interval: float = 1.0,
        cleanup_interval: float = 60.0,
        on_job_abandoned=None,
        shutdown_grace_seconds: float = 25.0,
        on_shutdown_exit=None,
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
        self.wake_mode = wake_mode
        self._took_token = False
        self.min_poll_interval = min(min_poll_interval, poll_interval)
        self.reap_interval = reap_interval
        self.cleanup_interval = cleanup_interval
        # Called when a running job hits its time limit or loses its lease. A
        # Python thread can't be killed, so by default the whole worker process
        # exits and its supervisor (Docker's restart policy) starts a fresh one.
        # Tests pass a function instead, so pytest isn't killed.
        self.on_job_abandoned = on_job_abandoned or _exit_process
        self._last_cleanup = 0.0
        # Registry identity. The random suffix keeps a restarted worker (same
        # container, maybe same process id) from looking like the dead one.
        self.worker_id = f"{socket.gethostname()}-{os.getpid()}-{uuid.uuid4().hex[:4]}"
        self._status = {"status": "idle", "job": "", "task": "", "job_started": ""}
        self._jobs_done = 0
        self._jobs_failed = 0
        self._stop_presence = threading.Event()
        self._last_reap = 0.0
        self._stopping = False
        # On a stop signal, the current job gets this long to finish. After that
        # it's handed back (release) and the process exits, before Docker's own
        # 30 s stop window runs out and kills it mid-job.
        self.shutdown_grace_seconds = shutdown_grace_seconds
        self._shutdown_deadline = None
        self.on_shutdown_exit = on_shutdown_exit or (lambda: os._exit(0))

    def run(self) -> None:
        """Loop until asked to stop (Ctrl-C or SIGTERM)."""
        # Finish the current job before exiting instead of dying mid-job.
        signal.signal(signal.SIGINT, self._request_stop)
        signal.signal(signal.SIGTERM, self._request_stop)
        log.info("worker %s started", self.worker_id)
        self.start_presence()
        wait = self.min_poll_interval
        try:
            while not self._stopping:
                found = self.run_once()
                if not found:
                    if self.wake_mode == "doorbell":
                        # Sleep until a job becomes ready (instant wake-up) or the
                        # next delayed job is due. Capped at 1 s so the worker
                        # still reaps, cleans up and notices a stop request.
                        self._took_token = self.queue.wait_for_work(max_wait=1.0)
                    else:
                        time.sleep(wait)  # polling: try again after a short wait
                wait = self.next_wait(wait, found)
        finally:
            self.stop_presence()
        log.info("worker stopped")

    def start_presence(self, interval: float = 1.0) -> None:
        """Check in with the worker registry every `interval` seconds, from its own
        thread, so a worker busy with a long task still shows as alive. If the
        process dies, the check-ins stop: that's how the dashboard sees it die."""
        def loop():
            while not self._stop_presence.wait(interval):
                self._beat()
                self._check_demo_kill()
        self._beat()
        threading.Thread(target=loop, daemon=True).start()

    def stop_presence(self) -> None:
        self._stop_presence.set()
        try:
            self.queue.remove_worker(self.worker_id)  # clean exit: not "dead"
        except redis.RedisError:
            pass

    def _check_demo_kill(self) -> None:
        """Demo controls only: the dashboard asked this worker to crash. Exit at
        once with no cleanup, exactly like a SIGKILL, so the dashboard shows the
        real recovery (dead worker, lease expiry, job retried elsewhere)."""
        if os.environ.get("DEMO_CONTROLS") != "1":
            return
        key = f"tq:{self.queue.name}:demo:kill:{self.worker_id}"
        try:
            if self.queue.r.delete(key):
                log.warning("demo: crashing on purpose")
                logging.shutdown()
                os._exit(9)
        except redis.RedisError:
            pass

    def _beat(self) -> None:
        try:
            self.queue.worker_beat(
                self.worker_id, host=socket.gethostname(), pid=os.getpid(),
                jobs_done=self._jobs_done, jobs_failed=self._jobs_failed, **self._status)
        except redis.RedisError as e:
            log.warning("registry check-in failed: %s", e)

    def next_wait(self, wait: float, found_job: bool) -> float:
        """Idle wait before the next check: reset after a job, else double (capped)."""
        if found_job:
            return self.min_poll_interval
        return min(wait * 2, self.poll_interval)

    def run_once(self) -> bool:
        """Lease and run at most one job. Returns False if no job was ready."""
        self._maybe_reap()

        # If the doorbell woke us, we already took this job's token.
        leased = self.queue.lease(self.lease_seconds, consume_token=not self._took_token)
        self._took_token = False
        if leased is None:
            return False
        job_id, token = leased
        job = self.queue.get_job(job_id)
        log.info("job %s leased (attempt %d/%d)", job_id, job["attempts"], job["max_attempts"])
        self._status = {"status": "busy", "job": job_id, "task": job["payload"].get("task", ""),
                        "job_started": job.get("started_at", "")}

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
                self._jobs_done += 1
            else:
                log.warning("job %s: lease was lost before ack (another worker owns it)", job_id)
        else:
            outcome = self.queue.fail(job_id, token, error, retry=not permanent)
            self._jobs_failed += 1
            log.warning("job %s failed (%s%s): %s", job_id, outcome or "lease lost",
                        ", permanent" if permanent else "", error)
        self._status = {"status": "idle", "job": "", "task": "", "job_started": ""}
        return True

    def _run_task(self, payload: dict):
        name = payload.get("task")
        if name not in self.tasks:
            # Retrying can't make an unknown task exist.
            raise PermanentError(f"unknown task: {name!r}")
        return self.tasks[name].fn(**payload.get("args", {}))

    def _heartbeat(self, job_id: str, token: str, stop: threading.Event, deadline: float) -> None:
        # Checks a few times a second (cheap, all local) so it reacts on time to
        # the job's time limit and to the shutdown deadline; it only talks to
        # Redis at each regular heartbeat or when the time limit is reached.
        # stop.wait() returns True as soon as the job ends, so this exits promptly.
        next_beat = time.monotonic() + self.heartbeat_interval
        while not stop.wait(0.2):
            now = time.monotonic()
            if self._shutdown_deadline is not None and now >= self._shutdown_deadline:
                self._hand_back(job_id, token)
                return
            if now < next_beat and now < deadline:
                continue
            next_beat = now + self.heartbeat_interval
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
            self.queue.prune_workers()  # forget workers silent for a minute
        # Less often: remove old finished and dead jobs from Redis memory.
        if now - self._last_cleanup >= self.cleanup_interval:
            self._last_cleanup = now
            removed = self.queue.cleanup()
            if removed:
                log.info("cleaned up %d old job(s) from Redis", removed)

    def _hand_back(self, job_id: str, token: str) -> None:
        """Shutdown deadline reached with the job still running: return it to
        the queue without using up an attempt, then exit."""
        try:
            if self.queue.release(job_id, token):
                log.warning("job %s still running at shutdown; handed back to the queue", job_id)
        except redis.RedisError as e:
            log.warning("job %s: release failed (%s); its lease will expire instead", job_id, e)
        self.stop_presence()
        logging.shutdown()
        self.on_shutdown_exit()

    def request_stop(self) -> None:
        """Finish the current job if it ends within shutdown_grace_seconds;
        otherwise hand it back. Then stop."""
        if not self._stopping:
            log.info("stop requested: finishing current job (up to %.0f s)", self.shutdown_grace_seconds)
            self._stopping = True
            self._shutdown_deadline = time.monotonic() + self.shutdown_grace_seconds

    def _request_stop(self, signum, frame) -> None:
        self.request_stop()


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
        shutdown_grace_seconds=float(os.environ.get("SHUTDOWN_GRACE_SECONDS", 25)),
        wake_mode=os.environ.get("WAKE_MODE", "doorbell"),
    ).run()


if __name__ == "__main__":
    main()
