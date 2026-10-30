import os

import pytest
import redis

# Tests use Redis database 15 and wipe it, so they never touch real data in db 0.
TEST_REDIS_URL = os.environ.get("TEST_REDIS_URL", "redis://localhost:6379/15")


@pytest.fixture
def r():
    client = redis.Redis.from_url(TEST_REDIS_URL)
    try:
        client.ping()
    except redis.ConnectionError:
        pytest.skip("Redis not running (start it with: docker compose up -d)")
    client.flushdb()
    yield client
    client.flushdb()


TEST_PG_DB = "taskqueue_test"


@pytest.fixture
def pg():
    """A connection to a separate test database, with fresh empty tables."""
    import psycopg
    from psycopg import sql

    from src import db

    try:
        admin = db.connect()
    except (psycopg.OperationalError, RuntimeError):
        pytest.skip("PostgreSQL not running (start it with: docker compose up -d)")
    with admin:
        admin.autocommit = True  # CREATE DATABASE can't run inside a transaction
        exists = admin.execute(
            "SELECT 1 FROM pg_database WHERE datname = %s", (TEST_PG_DB,)
        ).fetchone()
        if not exists:
            admin.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(TEST_PG_DB)))

    conn = db.connect(TEST_PG_DB)
    conn.execute("DROP TABLE IF EXISTS job_attempts, jobs")
    db.init_schema(conn)
    conn.commit()
    yield conn
    conn.close()
