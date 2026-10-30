import uuid

import psycopg
import pytest
from psycopg.types.json import Jsonb


def insert_job(pg, status="pending"):
    job_id = uuid.uuid4()
    pg.execute(
        "INSERT INTO jobs (id, queue, payload, status, max_attempts) VALUES (%s, %s, %s, %s, %s)",
        (job_id, "default", Jsonb({"task": "echo"}), status, 5),
    )
    return job_id


def insert_attempt(pg, job_id, attempt_number, token=None):
    pg.execute(
        "INSERT INTO job_attempts (lease_token, job_id, attempt_number, started_at)"
        " VALUES (%s, %s, %s, now())",
        (token or uuid.uuid4(), job_id, attempt_number),
    )


def test_schema_init_is_safe_to_run_twice(pg):
    from taskqueue import db
    db.init_schema(pg)  # must not fail on existing tables


def test_rejects_unknown_status(pg):
    with pytest.raises(psycopg.errors.CheckViolation):
        insert_job(pg, status="exploded")


def test_attempt_must_belong_to_existing_job(pg):
    with pytest.raises(psycopg.errors.ForeignKeyViolation):
        insert_attempt(pg, uuid.uuid4(), 1)


def test_attempts_survive_replay_numbering(pg):
    """After a replay Redis restarts attempt numbers at 1. Both attempt 1s must be kept."""
    job_id = insert_job(pg)
    insert_attempt(pg, job_id, 1)
    insert_attempt(pg, job_id, 2)
    insert_attempt(pg, job_id, 1)  # after replay

    count = pg.execute("SELECT count(*) FROM job_attempts WHERE job_id = %s", (job_id,)).fetchone()[0]
    assert count == 3


def test_same_lease_token_cannot_be_recorded_twice(pg):
    """Duplicate delivery of the same history write must not create two attempts."""
    job_id = insert_job(pg)
    token = uuid.uuid4()
    insert_attempt(pg, job_id, 1, token)
    with pytest.raises(psycopg.errors.UniqueViolation):
        insert_attempt(pg, job_id, 1, token)
