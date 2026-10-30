"""Example task functions workers can run.

A job's payload names a task and its arguments:
    {"task": "echo", "args": {"msg": "hello"}}
"""

import logging
import time

log = logging.getLogger(__name__)


def echo(msg: str = "") -> str:
    log.info("echo: %s", msg)
    return msg


def sleep(seconds: float = 1) -> None:
    """Simulates a slow job (useful for testing leases and heartbeats)."""
    time.sleep(seconds)


def fail(message: str = "this task always fails") -> None:
    """Simulates a broken job (useful for testing retries and the dead-letter queue)."""
    raise RuntimeError(message)


TASKS = {"echo": echo, "sleep": sleep, "fail": fail}
