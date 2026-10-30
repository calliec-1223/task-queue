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

## 13. Adaptive polling for idle workers

- **Decision**: When the queue is empty, a worker waits 10 ms, then doubles the wait after each empty check, up to 500 ms. Finding a job resets the wait to 10 ms.
- **Alternatives**:
  - *Fixed 500 ms*: least Redis load, but a job arriving at an idle worker can wait up to 500 ms. Measured p99: 111–253 ms with 4–16 workers.
  - *Fixed 50 ms*: p99 of 31–39 ms, but 952 Redis commands/sec from 16 idle workers, 6.6× more.
  - *Fixed 100 ms*: a middle ground that's still a fixed tradeoff.
- **Why**: Measured, it gets most of 50 ms polling's latency (p99 33–62 ms) at 500 ms polling's idle cost (146 vs 144 commands/sec). It's the same exponential backoff idea as job retries (#5).
- **Revisited after benchmarking**: Fixed 50 ms had the best p99 by up to about 25 ms, and at 16 workers its idle cost (about 950 commands/sec) is under 1% of Redis's capacity. I kept adaptive anyway: the idle cost of fixed polling grows with every worker (about 60,000 commands/sec at 1,000 idle workers), and the real jobs in Phase 4 (Claude API calls) take seconds, so 25 ms is about 1% of a job.
- **Re-measured after priority (#18)**: p99 at 4–16 workers was 155, 64 and 55 ms (fixed 50 ms: 38, 33, 43 ms), so adaptive's p99 is less consistent than the first run suggested. Idle load stayed equal to 500 ms polling (228 vs 225 commands/sec; fixed 50 ms: 1566). Every empty check now costs more Redis commands because of the priority step, but the ratio between modes held.
- **Tradeoffs accepted**: Slightly worse p99 than fixed 50 ms right after an idle stretch, because a backed-off worker can be mid-wait (up to 500 ms) when work arrives. Still no true push notification, which is the cost of choosing sorted sets in #2.

## 14. Phase 4: Whisk uses task-queue as a library

- **Decision**: Whisk installs `taskqueue` as a Python package, enqueues chat jobs from its web server, and runs its own worker process with a `whisk_chat` task defined in Whisk's code. Whisk uses its own queue (`whisk`) so the demo workers never pick up chat jobs.
- **Alternatives**:
  - *Put the chat task inside task-queue*: nothing new to run, but the queue would contain baking-app code, and its workers would need Whisk's API key and database.
  - *Separate service with an HTTP API*: the most service-like, but the most work: a new API, auth between services, and the chat code still has to run somewhere.
- **Why**: The queue stays generic infrastructure, and Whisk's secrets and data stay in Whisk. It's how real task libraries are used.
- **Consequences**: The package was renamed from `src` to `taskqueue`, and importing it no longer loads task-queue's `.env` (only the command-line tools do), so it can't change the host app's settings.

## 15. Phase 4: the browser polls for the chat answer

- **Decision**: `POST /chat` returns `202 Accepted` with a job ID immediately; the browser checks `GET /chat/<id>` every second until the answer is ready.
- **Alternatives**:
  - *Long polling*: answer arrives instantly, but each waiting request holds a server process, which is the problem being fixed.
  - *Server-Sent Events*: real push and a path to word-by-word streaming, but Whisk's server handles one request per process, so it would need an async server; and with results in PostgreSQL the server would still be polling underneath.
- **Why**: Keeps the web server free; each check takes milliseconds. Up to 1 second of extra delay on a multi-second answer isn't noticeable.
- **Tradeoffs accepted**: Extra small requests while waiting, and no word-by-word streaming.

## 16. Phase 4: apps read results from PostgreSQL history

- **Decision**: Whisk reads a finished job's result from the history database (`taskqueue.results.get_job`). "Not found yet" means still pending.
- **Alternatives**: *Store results in Redis on the job*: instant and fast, but a core change plus an expiry rule to keep Redis from filling up, and results would be temporary.
- **Why**: No core queue changes, and results are kept permanently.
- **Tradeoffs accepted**: History lags Redis slightly (milliseconds when the writer is keeping up), so answers appear a little later. Whisk needs read access to the queue's database, which couples the two systems.

## 17. Idempotency: keys at enqueue, plus result checkpoints

- **Decision**: Both. (1) `enqueue(..., idempotency_key=...)`: a repeated key within 24 hours returns the original job instead of creating a duplicate. The check and the create happen in the same Lua script. (2) When a task succeeds, the worker saves its result on the job only if no result is saved yet (first finisher wins). Before running a task, a worker checks for a saved result and reuses it instead of redoing the work.
- **Alternatives**:
  - *Keys only*: stops duplicate submissions (double-clicks, client retries) but not a job running twice after a lease expiry.
  - *Checkpoints only*: stops most re-runs but not two separate jobs created by a double submission.
- **Why**: The queue is at-least-once, so duplicates come from two places, submission and execution. Each needs its own fix.
- **Limits accepted**: If a worker's lease expires while it's still running, another worker can start the same job at the same time, and both do the work. Checkpoints keep the recorded result consistent (the first finisher's), but only the external service supporting idempotency keys could stop the second call. Exactly-once execution isn't achievable inside the queue. Checkpoints are kept on the job hash in Redis.

## 18. Priority: separate "delayed" and "ready" sets, with aging

- **Decision**: The single `pending` set became two. `delayed` holds jobs waiting on a delay or retry backoff, scored by when they become ready. `ready` holds jobs that can run now, scored by *the time they became ready − priority × 60 s*. Each lease first moves due jobs from `delayed` to `ready`, then takes the lowest score. Any integer priority works; default 0.
- **Alternatives**:
  - *One pending set per priority level*: the smallest change, but a fixed number of levels, up to one lookup per level on every lease, and every other part (reaper, replay, metrics) needs to know all the sets.
  - *Fold priority into the existing score*: one set and almost no code, but it breaks the design: a high-priority job delayed 30 minutes would look ready immediately.
- **Why**: Any number of levels, first-come-first-served within a level, one lookup per lease, and "score = time" still holds for delayed jobs, so delays and backoff are untouched.
- **Starvation**: *Aging*, not strict priority. Each priority level is worth 60 seconds of waiting (default, set per queue), so a lower-priority job that has waited longer than that catches up with new higher-priority jobs. Strict priority was simpler but lets a steady stream of urgent jobs block everything else forever. The 60 s step is my default; it's stored once in Redis so every producer and worker uses the same value.
- **Tradeoffs accepted**: The biggest core change so far (enqueue, lease, retries, reaper, replay and metrics all changed). Each lease does a small "move what's due" step, capped at 1,000 jobs per lease; if more than that become due at the same instant, priority ordering among them is approximate until the backlog clears.

## 19. Cleanup of finished jobs: a "finished" set plus a batched sweep

- **Decision**: On ack, a job joins a `finished` sorted set scored by finish time. `cleanup()` deletes the Redis copy of jobs done more than 24 hours ago and dead-lettered more than 30 days ago (which removes them from the dead-letter queue for good), in batches of 100. Workers run it once a minute. PostgreSQL history keeps every job.
- **Alternatives**:
  - *Auto-expire on ack (Redis EXPIRE)*: one line and no sweep, but dead jobs would still pile up forever if nobody looks at them.
  - *Delete on ack*: least memory, but a finished job would briefly exist in neither Redis nor history (history lags slightly), so tools couldn't find it. (Its history isn't lost; events wait in the stream.)
- **Why**: The only option that also bounds an ignored dead-letter queue, and you can see how many finished jobs are waiting to be cleaned.
- **Tradeoffs accepted**: Another sorted set and Lua script; the sweep is batched like replay so it never blocks Redis. A dead job older than 30 days can no longer be replayed. The 24-hour and 30-day defaults are mine, adjustable per call.

## 20. Time limits for hung tasks: enforced in Redis, stuck worker restarts itself

- **Decision**: Each job has `timeout_seconds` (default 300, my choice). `lease()` records the start time, and neither the lease nor any heartbeat can push the lease expiry past *start + timeout*. The reaper then treats it as a failed attempt labelled "timed out after N s". The worker's heartbeat thread wakes right at the limit, sees the refusal, and exits the process; Docker's restart policy starts a fresh worker.
- **Alternatives**:
  - *Run each task in a child process the worker can kill*: truly stops the hung work and isolates crashes, but starting a process per job adds overhead that would cut throughput on short jobs, and arguments and results must be passable between processes.
  - *Give tasks a deadline and trust them*: no machinery, but only protects tasks written correctly, not the unexpected hang this is meant to catch.
- **Why**: It reuses the lease and reaper machinery and is enforced centrally, so buggy task code can't dodge it.
- **Tradeoffs accepted**: Python can't kill a thread, so the stuck task stops only because its whole worker process exits, which needs a supervisor (Docker here). A worker that loses its lease for any reason now also exits, to stop doing duplicate work. Jobs leased before this change have no limit.

## 21. Errors that shouldn't be retried: a per-task list plus `PermanentError`

- **Decision**: Both. A task can be registered as `Task(fn, no_retry=(SomeError, ...))` to list error types that never succeed on retry, and any task can raise `PermanentError` for cases only it can recognize. Either way the worker calls `fail(..., retry=False)`, which sends the job straight to the dead-letter queue in the same atomic script. Unknown task names are now permanent too.
- **Alternatives**:
  - *Only the exception*: explicit, but every task must catch and convert errors itself, and it's easy to miss one.
  - *Only a per-task list*: keeps task code clean, but matches by type only, so it can't handle cases where the same exception type is sometimes temporary and sometimes not.
  - *Classify automatically by exception type*: no work for task authors, but it guesses wrong and is hard to defend.
- **Why**: The list covers whole error types in one visible place; the exception covers the edge cases, because the task knows its own errors. Unlisted errors are still retried, so the safe default stays "retry."
- **Tradeoffs accepted**: Two mechanisms to explain. A wrongly-listed error type dead-letters jobs that might have succeeded; replay recovers them.

## 22. Worker registry: a sorted set scored by "last seen"

- **Decision**: Each worker checks in about once a second from its own presence thread: `tq:<queue>:workers` (score = last check-in) plus a small status record (host, current job, jobs done and failed). A worker silent for 5 s shows as dead; after 60 s the reaper removes it. A clean shutdown removes the worker immediately, so only crashes, kills and timeouts show as dead.
- **Alternatives**:
  - *One self-expiring key per worker*: no cleanup code, but a dead worker just vanishes (you can't show it dying), and listing workers means scanning all of Redis.
  - *No registry; show running jobs*: no core change, but idle workers are invisible and there's no worker identity.
- **Why**: It shows the whole cycle, working → dead → removed, and reuses the "score = time" pattern. Listing workers is one sorted-set read.
- **Details**: Check-ins come from a separate thread because a busy worker's main thread is stuck inside the task (job heartbeats are only every 10 s). The worker ID has a random suffix so a restarted container doesn't look like the worker that died.
- **Tradeoffs accepted**: One more structure and a removal rule. The registry is for visibility only; nothing in job processing depends on it.
