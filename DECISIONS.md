# Design Decisions

Each entry records a choice, the alternatives considered, and why.

## 1. Run Redis in Docker from Phase 1

- **Decision**: Use Docker Desktop to run Redis locally starting in Phase 1.
- **Alternatives**: Install Redis directly with Homebrew and add Docker in Phase 2.
- **Why**: Every developer (and CI) gets the same Redis version with one command, and the Phase 2 Docker Compose setup builds on it instead of replacing a local install.

## 2. Store the queue in Redis sorted sets, with Lua scripts for atomic moves

- **Decision**: Pending jobs live in a sorted set scored by "ready time." Leased jobs live in an `inflight` sorted set scored by "lease expiry time." Each move between them runs in a Lua script.
- **Alternatives**:
  - *Two Lists (`BLMOVE` reliable queue)*: simpler, and workers can block instead of polling. But Lists don't store time, so lease expiry, delayed jobs and retry backoff would each need a separate structure.
  - *Redis Streams with consumer groups*: lease recovery is built in. But Redis would do the core reliability work for me, and delayed or priority jobs still need another structure.
- **Why**: A score that means "time" handles three features with one idea. A delayed job is a future score, a retry with backoff is a re-add with a future score, and an expired lease is an inflight score that is now in the past. Lua makes each move atomic, so a job is never in both sets or in neither.
- **Tradeoffs accepted**: Workers poll instead of blocking, which adds some latency and Redis load. I have to learn Lua. Jobs that become ready at the exact same microsecond have no guaranteed order.
