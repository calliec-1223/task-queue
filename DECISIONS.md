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
