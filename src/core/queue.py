"""Core queue: enqueue, lease, heartbeat, ack, fail, and returning expired leases.

Redis layout (for a queue named "default"):
    tq:job:<id>               hash   -> job data: payload, status, attempts,
                                        max_attempts, lease_token, last_error
    tq:default:pending        zset   -> job ids, score = time the job becomes ready
    tq:default:inflight       zset   -> job ids, score = time the lease expires
    tq:default:dead           zset   -> job ids, score = time the job was dead-lettered

Every multi-step change runs inside a Lua script so Redis executes it as one
atomic step: no other client can see or change the data halfway through.
All times come from the Redis server clock (TIME), not the worker's clock, so
workers with slightly wrong clocks can't break lease expiry.

Failure rule (used by both fail() and reap_expired()):
    if attempts < max_attempts: retry after exponential backoff with jitter
    else:                       move to the dead-letter set
"""

import json
import random
import uuid
from typing import Callable

import redis

JOB_PREFIX = "tq:job:"

# Shared Lua helper: current Redis server time as float seconds.
_NOW = """
local t = redis.call('TIME')
local now = tonumber(t[1]) + tonumber(t[2]) / 1000000
"""

# Shared Lua helper: the failure rule. Callers must already have removed the
# job from inflight. `rand` is a number in [0, 1) chosen in Python (jitter).
# Expects locals: now, pending_key, dead_key, base, cap.
_RETRY_OR_DEAD = """
local function retry_or_dead(job_key, id, rand)
    redis.call('HDEL', job_key, 'lease_token')  -- this lease is over
    local attempts = tonumber(redis.call('HGET', job_key, 'attempts'))
    local max_attempts = tonumber(redis.call('HGET', job_key, 'max_attempts'))
    if attempts >= max_attempts then
        redis.call('ZADD', dead_key, now, id)
        redis.call('HSET', job_key, 'status', 'dead')
        return 'dead'
    end
    -- Exponential backoff: base * 2^(attempts-1), capped. attempts is >= 1 here
    -- because lease() already counted this attempt.
    local backoff = math.min(cap, base * 2 ^ (attempts - 1))
    -- Full jitter: wait a random fraction of the backoff, so jobs that failed
    -- together don't all retry at the same moment.
    redis.call('ZADD', pending_key, now + rand * backoff, id)
    redis.call('HSET', job_key, 'status', 'pending')
    return 'retry'
end
"""

# KEYS[1]=pending  ARGV: job_key, job_id, payload_json, delay_seconds, max_attempts
_ENQUEUE = _NOW + """
redis.call('HSET', ARGV[1], 'id', ARGV[2], 'payload', ARGV[3], 'status', 'pending',
           'attempts', 0, 'max_attempts', ARGV[5], 'created_at', now)
redis.call('ZADD', KEYS[1], now + tonumber(ARGV[4]), ARGV[2])
return 1
"""

# KEYS[1]=pending KEYS[2]=inflight  ARGV: job_prefix, lease_seconds, lease_token
_LEASE = _NOW + """
-- Oldest job whose ready-time has passed (delayed jobs are skipped until then).
local ids = redis.call('ZRANGEBYSCORE', KEYS[1], '-inf', now, 'LIMIT', 0, 1)
if #ids == 0 then return false end
local id = ids[1]
local job_key = ARGV[1] .. id
-- Move pending -> inflight. Score is the lease expiry time.
redis.call('ZREM', KEYS[1], id)
redis.call('ZADD', KEYS[2], now + tonumber(ARGV[2]), id)
redis.call('HINCRBY', job_key, 'attempts', 1)
-- The token proves which lease is current (see ack).
redis.call('HSET', job_key, 'status', 'leased', 'lease_token', ARGV[3])
return id
"""

# KEYS[1]=inflight  ARGV: job_key, job_id, lease_token
_ACK = """
-- Only the worker holding the current lease may ack. A worker whose lease
-- already expired (and the job went to someone else) gets rejected.
if redis.call('HGET', ARGV[1], 'lease_token') ~= ARGV[3] then return 0 end
redis.call('ZREM', KEYS[1], ARGV[2])
redis.call('HSET', ARGV[1], 'status', 'done')
redis.call('HDEL', ARGV[1], 'lease_token')
return 1
"""

# KEYS[1]=inflight  ARGV: job_key, job_id, lease_token, lease_seconds
_EXTEND = _NOW + """
-- Heartbeat: only the current lease holder may extend.
if redis.call('HGET', ARGV[1], 'lease_token') ~= ARGV[3] then return 0 end
-- Reset expiry to now + lease_seconds (not "add to old expiry"), so it can't run away.
-- XX = only update if the job is already in inflight; never re-add it.
redis.call('ZADD', KEYS[1], 'XX', now + tonumber(ARGV[4]), ARGV[2])
return 1
"""

# KEYS[1]=pending KEYS[2]=inflight KEYS[3]=dead
# ARGV: job_key, job_id, lease_token, error, base, cap, rand
_FAIL = _NOW + """
local pending_key, dead_key = KEYS[1], KEYS[3]
local base, cap = tonumber(ARGV[5]), tonumber(ARGV[6])
""" + _RETRY_OR_DEAD + """
-- Same rule as ack: a stale worker can't fail a job it no longer owns.
if redis.call('HGET', ARGV[1], 'lease_token') ~= ARGV[3] then return false end
redis.call('ZREM', KEYS[2], ARGV[2])
redis.call('HSET', ARGV[1], 'last_error', ARGV[4])
return retry_or_dead(ARGV[1], ARGV[2], tonumber(ARGV[7]))
"""

# KEYS[1]=pending KEYS[2]=inflight KEYS[3]=dead
# ARGV: job_prefix, batch_size, base, cap, rand_1, rand_2, ... (one per job)
_REAP = _NOW + """
local pending_key, dead_key = KEYS[1], KEYS[3]
local base, cap = tonumber(ARGV[3]), tonumber(ARGV[4])
""" + _RETRY_OR_DEAD + """
-- Leases whose expiry time has passed: the worker is presumed dead.
-- That counts as a failed attempt, so poison jobs eventually dead-letter.
local ids = redis.call('ZRANGEBYSCORE', KEYS[2], '-inf', now, 'LIMIT', 0, tonumber(ARGV[2]))
for i, id in ipairs(ids) do
    local job_key = ARGV[1] .. id
    redis.call('ZREM', KEYS[2], id)
    redis.call('HSET', job_key, 'last_error', 'lease expired')
    retry_or_dead(job_key, id, tonumber(ARGV[4 + i]))
end
return #ids
"""


class Queue:
    def __init__(
        self,
        client: redis.Redis,
        name: str = "default",
        retry_base_seconds: float = 1,
        retry_cap_seconds: float = 300,
        jitter: Callable[[], float] = random.random,  # tests can pass a fixed value
    ):
        self.r = client
        self.pending_key = f"tq:{name}:pending"
        self.inflight_key = f"tq:{name}:inflight"
        self.dead_key = f"tq:{name}:dead"
        self.retry_base = retry_base_seconds
        self.retry_cap = retry_cap_seconds
        self.jitter = jitter
        # register_script sends the script once, then calls it by its hash.
        self._enqueue = client.register_script(_ENQUEUE)
        self._lease = client.register_script(_LEASE)
        self._ack = client.register_script(_ACK)
        self._extend = client.register_script(_EXTEND)
        self._fail = client.register_script(_FAIL)
        self._reap = client.register_script(_REAP)

    def enqueue(self, payload: dict, delay_seconds: float = 0, max_attempts: int = 5) -> str:
        """Add a job. It can't be leased until delay_seconds have passed.

        max_attempts counts every run, including the first.
        """
        job_id = str(uuid.uuid4())
        self._enqueue(
            keys=[self.pending_key],
            args=[JOB_PREFIX + job_id, job_id, json.dumps(payload), delay_seconds, max_attempts],
        )
        return job_id

    def lease(self, lease_seconds: float = 30) -> tuple[str, str] | None:
        """Claim the next ready job for lease_seconds.

        Returns (job_id, lease_token), or None if nothing is ready.
        Keep the token: ack, fail and extend_lease need it.
        """
        token = str(uuid.uuid4())
        job_id = self._lease(
            keys=[self.pending_key, self.inflight_key],
            args=[JOB_PREFIX, lease_seconds, token],
        )
        if job_id is None:
            return None
        return job_id.decode(), token

    def ack(self, job_id: str, lease_token: str) -> bool:
        """Mark a job done. Returns False if this lease is no longer valid."""
        ok = self._ack(
            keys=[self.inflight_key],
            args=[JOB_PREFIX + job_id, job_id, lease_token],
        )
        return ok == 1

    def extend_lease(self, job_id: str, lease_token: str, lease_seconds: float = 30) -> bool:
        """Heartbeat: push the lease expiry to now + lease_seconds.

        Returns False if this lease is no longer valid (the worker should stop the job).
        """
        ok = self._extend(
            keys=[self.inflight_key],
            args=[JOB_PREFIX + job_id, job_id, lease_token, lease_seconds],
        )
        return ok == 1

    def fail(self, job_id: str, lease_token: str, error: str) -> str | None:
        """Report that the job raised an error.

        Returns "retry" (rescheduled with backoff), "dead" (moved to the
        dead-letter set), or None if this lease is no longer valid.
        """
        result = self._fail(
            keys=[self.pending_key, self.inflight_key, self.dead_key],
            args=[JOB_PREFIX + job_id, job_id, lease_token, error,
                  self.retry_base, self.retry_cap, self.jitter()],
        )
        return result.decode() if result else None

    def reap_expired(self, batch_size: int = 100) -> int:
        """Treat expired leases as failed attempts (retry or dead-letter).

        Returns how many jobs were reaped.
        """
        # One random jitter value per job the script might process.
        rands = [self.jitter() for _ in range(batch_size)]
        return self._reap(
            keys=[self.pending_key, self.inflight_key, self.dead_key],
            args=[JOB_PREFIX, batch_size, self.retry_base, self.retry_cap, *rands],
        )

    def get_job(self, job_id: str) -> dict | None:
        data = self.r.hgetall(JOB_PREFIX + job_id)
        if not data:
            return None
        job = {k.decode(): v.decode() for k, v in data.items()}
        job["payload"] = json.loads(job["payload"])
        job["attempts"] = int(job["attempts"])
        job["max_attempts"] = int(job["max_attempts"])
        return job
