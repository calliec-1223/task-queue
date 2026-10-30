"""Exceptions tasks can raise to control retries."""


class QueueFull(Exception):
    """enqueue() refused the job: too many jobs are already waiting. Callers
    should back off and try later (e.g. tell the user "busy, try again")."""


class PayloadTooLarge(ValueError):
    """enqueue() refused the job: its payload is over the size limit. Jobs
    should carry references (ids, URLs), not large data."""


class PermanentError(Exception):
    """Raise from a task when the error can never succeed on retry (bad input,
    invalid credentials...). The job goes straight to the dead-letter queue
    instead of using up its retries."""
