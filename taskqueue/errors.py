"""Exceptions tasks can raise to control retries."""


class PermanentError(Exception):
    """Raise from a task when the error can never succeed on retry (bad input,
    invalid credentials...). The job goes straight to the dead-letter queue
    instead of using up its retries."""
