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

## 7. Dead-letter replay: single job, plus batched bulk replay by error

- **Decision**: `replay(job_id)` for one job, and `replay_dead(error_contains=...)` to replay every dead job whose last error matches, in batches.
- **Alternatives**: *Single job only*: simpler, but after an outage someone would have to replay hundreds of jobs by hand.
- **Why**: After an outage is fixed, one command replays every job that failed for that reason, which saves operator time.
- **Tradeoffs accepted**: Redis runs one command or script at a time, so a script that looped over thousands of jobs would stall every worker, including their heartbeats, which could expire healthy leases. Each script call therefore examines at most `batch_size` jobs, and Python loops between calls so other clients get a turn. It's more code than single-job replay.

## 8. Replay resets `attempts` to 0

- **Decision**: A replayed job starts over with a full set of retries.
- **Alternatives**: *Keep `attempts` and raise `max_attempts`*: keeps an honest total count, but the meaning of `max_attempts` drifts and there are two numbers to explain.
- **Why**: Redis only needs the current attempt count to decide retry vs. dead-letter. The full failure history will live in PostgreSQL (Phase 2) for debugging.

## 9. Redis persistence: append-only file (AOF), synced every second

- **Decision**: Run Redis with `--appendonly yes`. Redis logs every write to disk and replays the log on restart. The default sync is once per second.
- **Alternatives**:
  - *No persistence*: fastest, but a Redis restart loses every queued job.
  - *Snapshots only (RDB)*: periodic full copies, so a crash can lose minutes of jobs.
  - *AOF synced on every write*: loses nothing, but much slower.
- **Why**: A queue that loses jobs on restart breaks the "zero job loss" goal. Syncing every second keeps writes fast.
- **Tradeoffs accepted**: If the machine itself crashes, up to about 1 second of writes can be lost.

## 10. PostgreSQL schema: `jobs` (current state) + `job_attempts` (one row per attempt)

- **Decision**: `jobs` has one row per job, updated as it changes. `job_attempts` has one row per attempt that is never overwritten, keyed by the attempt's lease token.
- **Alternatives**:
  - *One `jobs` table only*: simplest, but each update overwrites the last, so the attempt history is lost. That would break decision #8 (replay resets `attempts` because PostgreSQL keeps the history).
  - *Append-only `job_events` log*: a complete audit trail, but the table grows fastest, and a job's current status has to be worked out from its latest event instead of read from a column.
- **Why**: Current status is a simple lookup, and every attempt is kept, even across replays.
- **Detail**: The lease token is the attempt's primary key. After a replay, Redis restarts attempt numbers at 1, so "job X, attempt 1" can happen twice; tokens never repeat. It also means the same attempt can't be recorded twice.
- **Tradeoffs accepted**: Two writes per attempt, and a join to see a job together with its history.

## 11. History reaches PostgreSQL through an event stream in Redis (outbox pattern)

- **Decision**: Every Lua script that changes a job also appends an event to a Redis Stream (`tq:<queue>:events`) in the same atomic step. A separate history writer reads the stream through a consumer group, writes each batch to PostgreSQL in one transaction, and only then confirms the events (`XACK`) and deletes them.
- **Alternatives**:
  - *Workers write to PostgreSQL right after each Redis step*: simple and instant, but a crash between the two writes loses that history forever, and a slow PostgreSQL slows every job.
  - *Periodic sync of Redis state into PostgreSQL*: workers don't change, but anything that happens between syncs (like a failed attempt) is never recorded.
- **Why**: The others can lose history. Here a change and its event happen together or not at all, so history can't miss a change Redis has saved.
- **How duplicates are handled**: Events are confirmed after the commit, so a writer crash can cause redelivery (at-least-once). Every write is idempotent (`ON CONFLICT DO NOTHING`, or updates that set the same values), so applying an event twice is harmless.
- **Tradeoffs accepted**: One more process to run. History lags slightly. If the writer is down, events pile up in Redis memory. Exactly one writer per queue, so events for a job are applied in order. Redis's ~1 second disk-sync window (#9) still applies, but state and events are lost together, so they never disagree.
- **Why Streams here but not for the queue (#2)**: Here Redis only needs to be a reliable ordered log with redelivery, which is exactly what Streams provide. For the queue I wanted to design lease and retry logic myself, and Streams don't support delayed jobs.

## 12. Poison history events: retry N times, then move to `dead_events`

- **Decision**: If one event fails to apply because of its own data, its batch is rolled back and retried. After 3 failures, the event is saved to a `dead_events` table and the batch continues without it.
- **Alternatives**:
  - *Dead-letter on the first failure*: simplest, but a one-off glitch would send a good event to `dead_events` and need manual work.
  - *Log and skip*: almost no code, but the event would be lost from history, breaking #11's promise.
- **Why**: Brief glitches get another chance, history never stays blocked behind a bad event, and nothing is thrown away. Only events that fail every time need a person.
- **Details**:
  - Each event is applied inside a savepoint, so a failing event's writes are undone without losing the rest of the batch.
  - Failures are counted in a separate Redis hash, not with Redis's built-in delivery count, because that also rises during PostgreSQL outages and would dead-letter good events after a long outage.
  - Connection errors (PostgreSQL down) never count as event failures.
- **Tradeoffs accepted**: A poison event holds up history for N retry rounds (about N seconds) before it's set aside. Each retry re-applies the good events in its batch, which is safe because the writes are idempotent.
