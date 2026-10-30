"""Core queue: enqueue, lease, heartbeat, ack, fail, reaping, and dead-letter replay.

Redis layout (for a queue named "default"):
    tq:job:<id>               hash   -> job data: payload, status, attempts,
                                        max_attempts, lease_token, last_error,
                                        checkpoint (result saved by a finished attempt)
    tq:default:delayed        zset   -> jobs waiting (delay or retry backoff), score = time they become ready
    tq:default:ready          zset   -> jobs that can run now, score = time they became ready
                                        minus priority x priority_step (lowest score runs first)
    tq:default:config         hash   -> priority_step (seconds of waiting one priority level is worth)
    tq:default:inflight       zset   -> job ids, score = time the lease expires
    tq:default:dead           zset   -> job ids, score = time the job was dead-lettered
    tq:default:finished       zset   -> done job ids, score = time finished (cleanup() removes old ones)
    tq:default:events         stream -> history events for PostgreSQL (the "outbox")
    tq:default:idem:<key>     string -> job id created with this idempotency key (expires)

Every multi-step change runs inside a Lua script so Redis executes it as one
atomic step: no other client can see or change the data halfway through.
All times come from the Redis server clock (TIME), not the worker's clock, so
workers with slightly wrong clocks can't break lease expiry.

Each script that changes a job also appends a history event to the events
stream in the same atomic step. So a change and its event either both happen
or neither does: history can't silently miss a change. A separate history
writer (taskqueue/history.py) copies events into PostgreSQL.

Priority with aging: a job with priority p is ordered as if it had become
ready p x priority_step seconds earlier. So higher priority goes first, but a
lower-priority job that has waited longer than that catches up, and nothing
starves. Jobs that aren't ready yet wait in "delayed" (score = ready time), so
priority never lets a delayed job run early.

Failure rule (used by both fail() and reap_expired()):
    if attempts < max_attempts: retry after exponential backoff with jitter
    else:                       move to the dead-letter set
"""

import json
import random
import uuid
from typing import Any, Callable

import redis

JOB_PREFIX = "tq:job:"

# Shared Lua helper: current Redis server time as float seconds.
_NOW = """
local t = redis.call('TIME')
local now = tonumber(t[1]) + tonumber(t[2]) / 1000000
"""

# Shared Lua helper: append a history event. `fields` is a flat list of
# extra name/value pairs. Expects locals: now, events_key.
_EMIT = """
local function emit(event_type, job_id, fields)
    local args = {'XADD', events_key, '*', 'type', event_type, 'job_id', job_id, 'ts', tostring(now)}
    for _, v in ipairs(fields) do table.insert(args, v) end
    redis.call(unpack(args))
end
"""

# Shared Lua helper: put a job where it belongs, given when it becomes ready.
# Expects locals: now, delayed_key, ready_key, config_key.
_SCHEDULE = """
local step = tonumber(redis.call('HGET', config_key, 'priority_step') or '60')

local function schedule(job_key, id, ready_at)
    if ready_at > now then
        -- Not ready yet: wait in "delayed", scored by when it becomes ready.
        redis.call('ZADD', delayed_key, ready_at, id)
    else
        -- Ready: aging score. Each priority level counts as `step` seconds of
        -- waiting, so higher priority sorts earlier but can't starve the rest.
        local priority = tonumber(redis.call('HGET', job_key, 'priority') or '0')
        redis.call('ZADD', ready_key, ready_at - priority * step, id)
    end
end

-- Move jobs whose ready time has passed from "delayed" into "ready",
-- keeping the time they actually became ready (not the time we noticed).
local function promote_due(job_prefix, limit)
    local due = redis.call('ZRANGEBYSCORE', delayed_key, '-inf', now, 'WITHSCORES', 'LIMIT', 0, limit)
    for i = 1, #due, 2 do
        redis.call('ZREM', delayed_key, due[i])
        schedule(job_prefix .. due[i], due[i], tonumber(due[i + 1]))
    end
end
"""

# Shared Lua helper: the failure rule. Callers must already have removed the
# job from inflight. `rand` is a number in [0, 1) chosen in Python (jitter).
# Expects locals: now, dead_key, base, cap, plus emit() and schedule().
_RETRY_OR_DEAD = """
local function retry_or_dead(job_key, id, rand, event_type, error)
    local token = redis.call('HGET', job_key, 'lease_token') or ''
    redis.call('HDEL', job_key, 'lease_token')  -- this lease is over
    local attempts = tonumber(redis.call('HGET', job_key, 'attempts'))
    local max_attempts = tonumber(redis.call('HGET', job_key, 'max_attempts'))
    local outcome
    if attempts >= max_attempts then
        redis.call('ZADD', dead_key, now, id)
        redis.call('HSET', job_key, 'status', 'dead')
        outcome = 'dead'
    else
        -- Exponential backoff: base * 2^(attempts-1), capped. attempts is >= 1
        -- here because lease() already counted this attempt.
        local backoff = math.min(cap, base * 2 ^ (attempts - 1))
        -- Full jitter: wait a random fraction of the backoff, so jobs that
        -- failed together don't all retry at the same moment.
        redis.call('HSET', job_key, 'status', 'pending')
        schedule(job_key, id, now + rand * backoff)
        outcome = 'retry'
    end
    emit(event_type, id, {'lease_token', token, 'outcome', outcome, 'error', error})
    return outcome
end
"""

# KEYS: delayed, ready, config, events, idempotency_key ("" if none)
# ARGV: job_key, job_id, payload_json, delay_seconds, max_attempts, queue_name,
#       idempotency_ttl, priority
_ENQUEUE = _NOW + """
local delayed_key, ready_key, config_key, events_key = KEYS[1], KEYS[2], KEYS[3], KEYS[4]
""" + _EMIT + _SCHEDULE + """
-- Idempotency: if this key was used before (and hasn't expired), return the
-- job it created instead of creating a duplicate. Checking and creating happen
-- in one script, so two simultaneous submissions can't both get through.
if KEYS[5] ~= '' then
    local existing = redis.call('GET', KEYS[5])
    if existing then return existing end
    redis.call('SET', KEYS[5], ARGV[2], 'EX', tonumber(ARGV[7]))
end
redis.call('HSET', ARGV[1], 'id', ARGV[2], 'payload', ARGV[3], 'status', 'pending',
           'attempts', 0, 'max_attempts', ARGV[5], 'priority', ARGV[8], 'created_at', now)
schedule(ARGV[1], ARGV[2], now + tonumber(ARGV[4]))
emit('enqueued', ARGV[2], {'queue', ARGV[6], 'payload', ARGV[3], 'max_attempts', ARGV[5],
                           'priority', ARGV[8]})
return ARGV[2]
"""

# KEYS: finished, dead
# ARGV: job_prefix, finished_before, dead_before, batch_size
_CLEANUP = """
-- Delete the Redis copy of jobs that finished, or died, long enough ago.
-- Their permanent record is in PostgreSQL history. Limited to batch_size jobs
-- per call, so this script never blocks Redis for long.
local removed = 0
local function purge(set_key, before)
    local ids = redis.call('ZRANGEBYSCORE', set_key, '-inf', before,
                           'LIMIT', 0, tonumber(ARGV[4]) - removed)
    for _, id in ipairs(ids) do
        redis.call('ZREM', set_key, id)
        redis.call('DEL', ARGV[1] .. id)
        removed = removed + 1
    end
end
purge(KEYS[1], tonumber(ARGV[2]))
if removed < tonumber(ARGV[4]) then purge(KEYS[2], tonumber(ARGV[3])) end
return removed
"""

# KEYS: (none)  ARGV: job_key, result_json
_CHECKPOINT = """
-- Save a finished task's result, unless an earlier attempt already saved one.
-- First finisher wins; everyone gets back the same stored result.
-- No lease-token check on purpose: a worker whose lease expired still did the
-- work, and its result is valid even though its ack will be rejected.
if redis.call('EXISTS', ARGV[1]) == 0 then return false end
redis.call('HSETNX', ARGV[1], 'checkpoint', ARGV[2])
return redis.call('HGET', ARGV[1], 'checkpoint')
"""

# KEYS: delayed, ready, config, inflight, events  ARGV: job_prefix, lease_seconds, lease_token
_LEASE = _NOW + """
local delayed_key, ready_key, config_key, events_key = KEYS[1], KEYS[2], KEYS[3], KEYS[5]
""" + _EMIT + _SCHEDULE + """
-- First, delayed jobs whose time has come join the ready set.
promote_due(ARGV[1], 1000)
-- Take the lowest aging score: highest priority, adjusted for waiting time.
local popped = redis.call('ZPOPMIN', ready_key)
if #popped == 0 then return false end
local id = popped[1]
local job_key = ARGV[1] .. id
-- Move ready -> inflight. Score is the lease expiry time.
redis.call('ZADD', KEYS[4], now + tonumber(ARGV[2]), id)
local attempt = redis.call('HINCRBY', job_key, 'attempts', 1)
-- The token proves which lease is current (see ack).
redis.call('HSET', job_key, 'status', 'leased', 'lease_token', ARGV[3])
emit('leased', id, {'lease_token', ARGV[3], 'attempt', tostring(attempt)})
return id
"""

# KEYS: inflight, events, finished  ARGV: job_key, job_id, lease_token, result_json
_ACK = _NOW + "local events_key = KEYS[2]\n" + _EMIT + """
-- Only the worker holding the current lease may ack. A worker whose lease
-- already expired (and the job went to someone else) gets rejected.
if redis.call('HGET', ARGV[1], 'lease_token') ~= ARGV[3] then return 0 end
redis.call('ZREM', KEYS[1], ARGV[2])
redis.call('HSET', ARGV[1], 'status', 'done')
redis.call('HDEL', ARGV[1], 'lease_token')
-- Remember when it finished, so cleanup() can remove it from Redis later.
redis.call('ZADD', KEYS[3], now, ARGV[2])
emit('acked', ARGV[2], {'lease_token', ARGV[3], 'result', ARGV[4]})
return 1
"""

# KEYS: inflight  ARGV: job_key, job_id, lease_token, lease_seconds
# (Heartbeats don't change history, so no event.)
_EXTEND = _NOW + """
-- Heartbeat: only the current lease holder may extend.
if redis.call('HGET', ARGV[1], 'lease_token') ~= ARGV[3] then return 0 end
-- Reset expiry to now + lease_seconds (not "add to old expiry"), so it can't run away.
-- XX = only update if the job is already in inflight; never re-add it.
redis.call('ZADD', KEYS[1], 'XX', now + tonumber(ARGV[4]), ARGV[2])
return 1
"""

# KEYS: delayed, ready, config, inflight, dead, events
# ARGV: job_key, job_id, lease_token, error, base, cap, rand
_FAIL = _NOW + """
local delayed_key, ready_key, config_key = KEYS[1], KEYS[2], KEYS[3]
local inflight_key, dead_key, events_key = KEYS[4], KEYS[5], KEYS[6]
local base, cap = tonumber(ARGV[5]), tonumber(ARGV[6])
""" + _EMIT + _SCHEDULE + _RETRY_OR_DEAD + """
-- Same rule as ack: a stale worker can't fail a job it no longer owns.
if redis.call('HGET', ARGV[1], 'lease_token') ~= ARGV[3] then return false end
redis.call('ZREM', inflight_key, ARGV[2])
redis.call('HSET', ARGV[1], 'last_error', ARGV[4])
return retry_or_dead(ARGV[1], ARGV[2], tonumber(ARGV[7]), 'failed', ARGV[4])
"""

# KEYS: delayed, ready, config, inflight, dead, events
# ARGV: job_prefix, batch_size, base, cap, rand_1, rand_2, ... (one per job)
_REAP = _NOW + """
local delayed_key, ready_key, config_key = KEYS[1], KEYS[2], KEYS[3]
local inflight_key, dead_key, events_key = KEYS[4], KEYS[5], KEYS[6]
local base, cap = tonumber(ARGV[3]), tonumber(ARGV[4])
""" + _EMIT + _SCHEDULE + _RETRY_OR_DEAD + """
-- Leases whose expiry time has passed: the worker is presumed dead.
-- That counts as a failed attempt, so poison jobs eventually dead-letter.
local ids = redis.call('ZRANGEBYSCORE', inflight_key, '-inf', now, 'LIMIT', 0, tonumber(ARGV[2]))
for i, id in ipairs(ids) do
    local job_key = ARGV[1] .. id
    redis.call('ZREM', inflight_key, id)
    redis.call('HSET', job_key, 'last_error', 'lease expired')
    retry_or_dead(job_key, id, tonumber(ARGV[4 + i]), 'lease_expired', 'lease expired')
end
return #ids
"""

# KEYS: delayed, ready, config, dead, events  ARGV: job_key, job_id
_REPLAY_ONE = _NOW + """
local delayed_key, ready_key, config_key, dead_key, events_key = KEYS[1], KEYS[2], KEYS[3], KEYS[4], KEYS[5]
""" + _EMIT + _SCHEDULE + """
-- ZREM returns 0 if the job isn't dead, so live jobs can't be "replayed".
if redis.call('ZREM', dead_key, ARGV[2]) == 0 then return 0 end
-- Fresh start: full retries again. Past failures are kept in PostgreSQL.
redis.call('HSET', ARGV[1], 'attempts', 0, 'status', 'pending')
schedule(ARGV[1], ARGV[2], now)
emit('replayed', ARGV[2], {})
return 1
"""

# KEYS: delayed, ready, config, dead, events
# ARGV: job_prefix, error_contains ("" = all), start_index, examine_count, max_replay
_REPLAY_BATCH = _NOW + """
local delayed_key, ready_key, config_key, dead_key, events_key = KEYS[1], KEYS[2], KEYS[3], KEYS[4], KEYS[5]
""" + _EMIT + _SCHEDULE + """
-- Look at a limited slice of the dead set, so this script never runs long.
-- (Redis runs one script at a time; a long one would stall every worker.)
local start = tonumber(ARGV[3])
local ids = redis.call('ZRANGE', dead_key, start, start + tonumber(ARGV[4]) - 1)
local replayed, skipped = 0, 0
for _, id in ipairs(ids) do
    local job_key = ARGV[1] .. id
    local err = redis.call('HGET', job_key, 'last_error') or ''
    -- string.find(..., 1, true) = plain substring search, not a pattern.
    if replayed < tonumber(ARGV[5]) and string.find(err, ARGV[2], 1, true) then
        redis.call('ZREM', dead_key, id)
        redis.call('HSET', job_key, 'attempts', 0, 'status', 'pending')
        schedule(job_key, id, now)
        emit('replayed', id, {})
        replayed = replayed + 1
    else
        skipped = skipped + 1
    end
end
-- Replayed jobs left the dead set, so only skipped ones are still ahead of
-- the next slice. That's where the next call should start.
local done = (#ids < tonumber(ARGV[4])) and 1 or 0
return {replayed, start + skipped, done}
"""


class Queue:
    def __init__(
        self,
        client: redis.Redis,
        name: str = "default",
        retry_base_seconds: float = 1,
        retry_cap_seconds: float = 300,
        jitter: Callable[[], float] = random.random,  # tests can pass a fixed value
        priority_step_seconds: float = 60,
    ):
        self.r = client
        self.name = name
        self.delayed_key = f"tq:{name}:delayed"
        self.ready_key = f"tq:{name}:ready"
        self.config_key = f"tq:{name}:config"
        self.inflight_key = f"tq:{name}:inflight"
        self.dead_key = f"tq:{name}:dead"
        self.finished_key = f"tq:{name}:finished"
        self.events_key = f"tq:{name}:events"
        self.retry_base = retry_base_seconds
        self.retry_cap = retry_cap_seconds
        self.jitter = jitter
        # The aging step must be the same for every producer and worker, so it's
        # stored once in Redis. HSETNX: the first Queue to start sets it.
        client.hsetnx(self.config_key, "priority_step", priority_step_seconds)
        # register_script sends the script once, then calls it by its hash.
        self._enqueue = client.register_script(_ENQUEUE)
        self._lease = client.register_script(_LEASE)
        self._ack = client.register_script(_ACK)
        self._extend = client.register_script(_EXTEND)
        self._fail = client.register_script(_FAIL)
        self._reap = client.register_script(_REAP)
        self._replay_one = client.register_script(_REPLAY_ONE)
        self._replay_batch = client.register_script(_REPLAY_BATCH)
        self._checkpoint = client.register_script(_CHECKPOINT)
        self._cleanup = client.register_script(_CLEANUP)

    def enqueue(
        self,
        payload: dict,
        delay_seconds: float = 0,
        max_attempts: int = 5,
        idempotency_key: str | None = None,
        idempotency_ttl: int = 86400,
        priority: int = 0,
    ) -> str:
        """Add a job. It can't be leased until delay_seconds have passed.

        max_attempts counts every run, including the first.
        idempotency_key: submitting the same key again (within idempotency_ttl
        seconds) returns the original job's id instead of creating a duplicate.
        priority: higher runs sooner (any integer; default 0). Each level is
        worth priority_step_seconds of waiting, so lower priorities still run.
        """
        job_id = str(uuid.uuid4())
        idem = f"tq:{self.name}:idem:{idempotency_key}" if idempotency_key else ""
        result = self._enqueue(
            keys=[self.delayed_key, self.ready_key, self.config_key, self.events_key, idem],
            args=[JOB_PREFIX + job_id, job_id, json.dumps(payload), delay_seconds,
                  max_attempts, self.name, idempotency_ttl, int(priority)],
        )
        return result.decode()  # the new job, or the original one for a repeated key

    def get_checkpoint(self, job_id: str) -> tuple[bool, Any]:
        """(True, result) if an earlier attempt already finished this job's task."""
        raw = self.r.hget(JOB_PREFIX + job_id, "checkpoint")
        return (False, None) if raw is None else (True, json.loads(raw))

    def save_checkpoint(self, job_id: str, result: Any) -> Any:
        """Record a finished task's result. Returns the stored result, which is an
        earlier attempt's if one finished first, so every attempt reports the same answer."""
        stored = self._checkpoint(
            keys=[], args=[JOB_PREFIX + job_id, json.dumps(result, default=str)]
        )
        return result if stored is None else json.loads(stored)

    def lease(self, lease_seconds: float = 30) -> tuple[str, str] | None:
        """Claim the next ready job for lease_seconds.

        Returns (job_id, lease_token), or None if nothing is ready.
        Keep the token: ack, fail and extend_lease need it.
        """
        token = str(uuid.uuid4())
        job_id = self._lease(
            keys=[self.delayed_key, self.ready_key, self.config_key, self.inflight_key,
                  self.events_key],
            args=[JOB_PREFIX, lease_seconds, token],
        )
        if job_id is None:
            return None
        return job_id.decode(), token

    def ack(self, job_id: str, lease_token: str, result: Any = None) -> bool:
        """Mark a job done. Returns False if this lease is no longer valid."""
        ok = self._ack(
            keys=[self.inflight_key, self.events_key, self.finished_key],
            # default=str: results that aren't plain JSON are stored as text.
            args=[JOB_PREFIX + job_id, job_id, lease_token, json.dumps(result, default=str)],
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
            keys=[self.delayed_key, self.ready_key, self.config_key, self.inflight_key,
                  self.dead_key, self.events_key],
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
            keys=[self.delayed_key, self.ready_key, self.config_key, self.inflight_key,
                  self.dead_key, self.events_key],
            args=[JOB_PREFIX, batch_size, self.retry_base, self.retry_cap, *rands],
        )

    def replay(self, job_id: str) -> bool:
        """Move one dead job back to pending with attempts reset to 0.

        Returns False if the job isn't in the dead-letter set.
        """
        ok = self._replay_one(
            keys=[self.delayed_key, self.ready_key, self.config_key, self.dead_key,
                  self.events_key],
            args=[JOB_PREFIX + job_id, job_id],
        )
        return ok == 1

    def replay_dead(
        self, error_contains: str = "", batch_size: int = 100, limit: int | None = None
    ) -> int:
        """Replay dead jobs whose last error contains `error_contains` ("" = all).

        Works in batches of `batch_size` so Redis is never blocked for long:
        other clients get a turn between batches. Returns how many were replayed.
        """
        total = 0
        start = 0
        while limit is None or total < limit:
            remaining = limit - total if limit is not None else batch_size
            replayed, start, done = self._replay_batch(
                keys=[self.delayed_key, self.ready_key, self.config_key, self.dead_key,
                      self.events_key],
                args=[JOB_PREFIX, error_contains, start, batch_size, remaining],
            )
            total += replayed
            if done:
                break
        return total

    def count_waiting(self) -> int:
        """Jobs not yet leased: ready now, or waiting on a delay or backoff."""
        return self.r.zcard(self.ready_key) + self.r.zcard(self.delayed_key)

    def cleanup(self, done_ttl: float = 86400, dead_ttl: float = 30 * 86400,
                batch_size: int = 100, max_batches: int = 10) -> int:
        """Remove the Redis copy of jobs done more than done_ttl seconds ago and
        dead-lettered more than dead_ttl seconds ago (which takes them out of the
        dead-letter queue for good). History in PostgreSQL is kept.

        Works in batches so Redis is never blocked for long. Returns how many
        jobs were removed.
        """
        sec, usec = self.r.time()  # Redis clock, like everything else
        now = sec + usec / 1_000_000
        total = 0
        for _ in range(max_batches):
            removed = self._cleanup(
                keys=[self.finished_key, self.dead_key],
                args=[JOB_PREFIX, now - done_ttl, now - dead_ttl, batch_size],
            )
            total += removed
            if removed < batch_size:
                break
        return total

    def list_dead(self, count: int = 20) -> list[dict]:
        """The oldest dead jobs, with their last error."""
        ids = self.r.zrange(self.dead_key, 0, count - 1)
        return [self.get_job(i.decode()) for i in ids]

    def get_job(self, job_id: str) -> dict | None:
        data = self.r.hgetall(JOB_PREFIX + job_id)
        if not data:
            return None
        job = {k.decode(): v.decode() for k, v in data.items()}
        job["payload"] = json.loads(job["payload"])
        job["attempts"] = int(job["attempts"])
        job["max_attempts"] = int(job["max_attempts"])
        job["priority"] = int(job.get("priority", 0))
        return job
