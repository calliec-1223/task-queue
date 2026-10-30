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
