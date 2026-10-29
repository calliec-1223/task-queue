"""Core queue: enqueue, lease, ack, and returning expired leases.

Redis layout (for a queue named "default"):
    tq:job:<id>               hash   -> job data: payload, status, attempts, lease_token
    tq:default:pending        zset   -> job ids, score = time the job becomes ready
    tq:default:inflight       zset   -> job ids, score = time the lease expires

Every multi-step change runs inside a Lua script so Redis executes it as one
atomic step: no other client can see or change the data halfway through.
All times come from the Redis server clock (TIME), not the worker's clock, so
workers with slightly wrong clocks can't break lease expiry.
"""

import json
import uuid

import redis

JOB_PREFIX = "tq:job:"

# Shared Lua helper: current Redis server time as float seconds.
_NOW = """
local t = redis.call('TIME')
local now = tonumber(t[1]) + tonumber(t[2]) / 1000000
"""

# KEYS[1]=pending  ARGV: job_key, job_id, payload_json, delay_seconds
_ENQUEUE = _NOW + """
redis.call('HSET', ARGV[1], 'id', ARGV[2], 'payload', ARGV[3],
           'status', 'pending', 'attempts', 0, 'created_at', now)
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

# KEYS[1]=pending KEYS[2]=inflight  ARGV: job_prefix, batch_size
_REAP = _NOW + """
-- Leases whose expiry time has passed: the worker is presumed dead.
local ids = redis.call('ZRANGEBYSCORE', KEYS[2], '-inf', now, 'LIMIT', 0, tonumber(ARGV[2]))
for _, id in ipairs(ids) do
    redis.call('ZREM', KEYS[2], id)
    redis.call('ZADD', KEYS[1], now, id)  -- ready immediately
    redis.call('HSET', ARGV[1] .. id, 'status', 'pending')
    redis.call('HDEL', ARGV[1] .. id, 'lease_token')  -- old lease is now invalid
end
return #ids
"""


class Queue:
    def __init__(self, client: redis.Redis, name: str = "default"):
        self.r = client
        self.pending_key = f"tq:{name}:pending"
        self.inflight_key = f"tq:{name}:inflight"
        # register_script sends the script once, then calls it by its hash.
        self._enqueue = client.register_script(_ENQUEUE)
        self._lease = client.register_script(_LEASE)
        self._ack = client.register_script(_ACK)
        self._extend = client.register_script(_EXTEND)
        self._reap = client.register_script(_REAP)

    def enqueue(self, payload: dict, delay_seconds: float = 0) -> str:
        """Add a job. It can't be leased until delay_seconds have passed."""
        job_id = str(uuid.uuid4())
        self._enqueue(
            keys=[self.pending_key],
            args=[JOB_PREFIX + job_id, job_id, json.dumps(payload), delay_seconds],
        )
        return job_id

    def lease(self, lease_seconds: float = 30) -> tuple[str, str] | None:
        """Claim the next ready job for lease_seconds.

        Returns (job_id, lease_token), or None if nothing is ready.
        Keep the token: ack needs it.
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

    def reap_expired(self, batch_size: int = 100) -> int:
        """Return jobs with expired leases to pending. Returns how many moved."""
        return self._reap(
            keys=[self.pending_key, self.inflight_key],
            args=[JOB_PREFIX, batch_size],
        )

    def get_job(self, job_id: str) -> dict | None:
        data = self.r.hgetall(JOB_PREFIX + job_id)
        if not data:
            return None
        job = {k.decode(): v.decode() for k, v in data.items()}
        job["payload"] = json.loads(job["payload"])
        job["attempts"] = int(job["attempts"])
        return job
