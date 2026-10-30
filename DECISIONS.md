# Design Decisions

Each entry records a choice, the alternatives considered, and why.

## 1. Run Redis in Docker from Phase 1

- **Decision**: Use Docker Desktop to run Redis locally starting in Phase 1.
- **Alternatives**: Install Redis directly with Homebrew and add Docker in Phase 2.
- **Why**: Every developer (and CI) gets the same Redis version with one command, and the Phase 2 Docker Compose setup builds on it instead of replacing a local install.

## 2. Store the queue in Redis sorted sets, with Lua scripts for atomic moves

- **Decision**: Two sorted sets. In `pending`, a job's score is the time it becomes ready to be picked up. In `inflight` (a worker is working on it), the score is the time the worker's lease expires.
- **Alternatives**:
  - *Two Lists*: Lists don't track time, so handling expired leases, delays and retry backoff would need extra structures.
  - *Redis Streams*: Redis implements the lease and recovery logic for me, so there's less of my own design to explain, and it still doesn't support delayed jobs.
- **Why**: Every score answers "at what time should something happen to this job?", so delays, retry backoff and lease expiry all use the same mechanism. Moves between the sets run as Lua scripts because they're atomic: all steps run or none do, and Redis runs one script at a time. That stops two workers from grabbing the same job, and stops a crash from leaving a job in neither set, where it would be lost.
- **Tradeoffs accepted**: I had to learn Lua, and workers have to poll for jobs instead of waiting for one to arrive.

## 3. Heartbeats reset the lease expiry to "now + N"

- **Decision**: While a worker runs a job, it calls `extend_lease`, which sets the job's inflight score to now + N seconds. Only the holder of the current lease token can extend.
- **Alternatives**:
  - *Separate heartbeat key with a TTL*: Redis deletes it automatically, but job state ends up split across two places and the inflight score stops meaning anything.
  - *Add N to the current expiry*: even a healthy worker's expiry keeps drifting further out, so crashes are detected more slowly the longer a job runs, and it needs an extra cap to stay bounded.
- **Why**: It reuses the "score = time" mechanism with no new structures, and a crash is always detected within N seconds of the last heartbeat.
- **Tradeoffs accepted**: Each heartbeat is a round trip to Redis, which adds load with many workers.

## 4. Errors and lease expiries both count as failed attempts

- **Decision**: One rule for every failure. When a worker reports an error (`fail`) or its lease expires (`reap_expired`): if `attempts` < `max_attempts`, retry with backoff; otherwise move the job to the dead-letter set.
- **Alternatives**:
  - *Only reported errors count*: simpler, but a poison job that crashes every worker would be requeued forever.
  - *Separate limits for errors and crashes*: more precise, but two counters and two settings for little gain right now.
- **Why**: It handles every kind of failure, including poison jobs, with one rule.
- **Tradeoffs accepted**: A healthy job can be dead-lettered if its workers keep getting killed for unrelated reasons. In Phase 3's kill-workers test, `max_attempts` must be set high enough for that not to happen.

## 5. Exponential backoff with full jitter

- **Decision**: Retry delay = random(0, 1) × min(cap, base × 2^(attempts−1)). Defaults: base 1s, cap 300s.
- **Alternatives**: *Plain exponential backoff*: predictable, but jobs that fail together retry together.
- **Why**: Plain backoff doesn't work at production scale. If an outage fails 1,000 jobs at once, they all retry at the same instant and overload the recovering service again (the "thundering herd"). Jitter spreads the retries out.
- **Tradeoffs accepted**: Retry times are random, so tests either pin the jitter to a fixed value or check a range.
- **Note**: The random number is chosen in Python and passed into the Lua script, because Redis's Lua `math.random` may not be truly random across calls.

## 6. Dead-letter jobs go in a sorted set scored by time of death

- **Decision**: `tq:<queue>:dead`, with the last error saved on the job.
- **Why**: Keeps the "score = time" idea, lists the oldest failures first, and makes a later "replay this job" function easy.
