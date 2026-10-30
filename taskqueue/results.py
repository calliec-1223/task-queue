"""Read a job's outcome from PostgreSQL history (for apps that submit jobs).

History is written by the history writer, so it can lag Redis slightly
(milliseconds normally). A job that was just enqueued may not be here yet:
callers should treat "not found" as "still pending".
"""

import uuid

import psycopg


def get_job(conn: psycopg.Connection, job_id: str) -> dict | None:
    """Return {status, payload, result, last_error}, or None if not recorded (yet)."""
    try:
        uuid.UUID(job_id)  # reject malformed ids before they reach SQL
    except (ValueError, TypeError):
        return None
    row = conn.execute(
        "SELECT status, payload, result, last_error FROM jobs WHERE id = %s", (job_id,)
    ).fetchone()
    if row is None:
        return None
    status, payload, result, last_error = row
    return {"status": status, "payload": payload, "result": result, "last_error": last_error}
