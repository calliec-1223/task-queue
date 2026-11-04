# Design Decisions

Every real choice I made while building this, with what I considered and why I picked what I did. Each entry starts with a one-sentence version; the details follow.

**Later decisions change earlier ones in three places:** #18 split the single `pending` set from #2 into `delayed` + `ready`; #25 enforces the "one history writer" rule from #11; #27 replaced polling (#2, #13) with a doorbell.

## 1. Run Redis in Docker from day one

**In short:** Docker gives everyone, including CI, the same Redis with one command.

- **Decision**: Run Redis in Docker from Phase 1.
- **Alternatives**: Install Redis with Homebrew now, switch to Docker in Phase 2.
- **Why**: The Phase 2 Docker Compose setup builds on it instead of replacing it, and nobody's laptop setup can differ from CI's.

## 2. The queue is Redis sorted sets, changed only by Lua scripts

**In short:** Every score is "the time something should happen to this job," and Lua scripts make each move all-or-nothing.

- **Decision**: Jobs live in sorted sets. Waiting jobs are scored by when they become ready; running jobs (`inflight`) are scored by when their lease expires. Every move between sets is one Lua script.
- **Alternatives**:
  - *Two lists*: lists don't know about time, so delays, retry backoff and lease expiry would each need something extra.
  - *Redis Streams*: Redis would handle leases and recovery for me, which leaves less of my own design to explain, and Streams still can't delay a job.
- **Why**: One idea (score = time) covers delays, backoff and lease expiry. Redis runs one script at a time and a script either fully runs or doesn't, so two workers can never grab the same job, and a crash can't leave a job in neither set.
- **Tradeoffs**: I had to learn Lua. Originally, idle workers had to poll for work; #27 fixed that with a doorbell.

## 3. A heartbeat resets the lease to "now + N seconds"

**In short:** A crashed worker is always noticed within N seconds of its last heartbeat.

- **Decision**: While a job runs, the worker's heartbeat sets the job's lease expiry to now + N. Only the worker holding the current lease token can do this.
- **Alternatives**:
  - *A separate heartbeat key that expires on its own*: Redis cleans it up for me, but the job's state is split across two places and the inflight score stops meaning anything.
  - *Add N to the current expiry*: a healthy job's expiry drifts further and further out, so crashes are noticed later the longer a job runs.
- **Why**: Same "score = time" idea, no new structures, and a fixed worst-case detection time.
- **Tradeoffs**: Each heartbeat is a Redis round trip, so more workers means more load.

## 4. A crash and an error both count as a failed attempt

**In short:** One retry rule for every kind of failure, so a job that crashes every worker can't loop forever.

- **Decision**: Whether the worker reports an error or its lease just expires, the job uses up an attempt. Attempts left: retry with backoff. None left: dead-letter queue.
- **Alternatives**:
  - *Only reported errors count*: a "poison" job that crashes every worker would be retried forever.
  - *Separate limits for errors and crashes*: more precise, but two counters and two settings for little gain.
- **Why**: One rule handles everything, including poison jobs.
- **Tradeoffs**: A healthy job can be dead-lettered if its workers keep getting killed for unrelated reasons. The chaos test sets `max_attempts` high enough to avoid that.

## 5. Exponential backoff with full jitter

**In short:** Retries wait longer each time, with a random spread so jobs that failed together don't all retry together.

- **Decision**: Delay = random(0..1) × min(300 s, 1 s × 2^(attempt − 1)).
- **Alternatives**: *Plain exponential backoff*: predictable, but if an outage fails 1,000 jobs at once, they all retry at the same moment and knock the recovering service over again (the "thundering herd").
- **Why**: Jitter spreads retries out, which matters most exactly when something is already struggling.
- **Tradeoffs**: Retry times are random, so tests pin the random value or check a range.
- **Detail**: Python picks the random number and passes it into the Lua script, because Lua's `math.random` inside Redis may not really be random between calls.

## 6. Dead jobs go in a sorted set scored by time of death

**In short:** Same "score = time" idea: oldest failures first, and replay is easy.

- **Decision**: `tq:<queue>:dead`, with the last error saved on the job.

## 7. Replay one dead job, or all of them that failed for the same reason

**In short:** After an outage is fixed, one command replays every job it killed, in small batches so Redis never freezes.

- **Decision**: `replay(job_id)` for one job; `replay_dead(error_contains=...)` for every dead job whose last error matches.
- **Alternatives**: *One job at a time only*: simpler, but someone would replay hundreds of jobs by hand after an outage.
- **Why**: It saves the person on call a lot of time.
- **Tradeoffs**: Redis runs one script at a time. A script that looped over thousands of jobs would block every worker, including their heartbeats, so healthy leases could expire. Each call handles at most `batch_size` jobs, and Python loops between calls so other clients get a turn.

## 8. A replayed job starts with a full set of retries

**In short:** Redis only needs the current count; PostgreSQL keeps the full history.

- **Decision**: Replay resets `attempts` to 0.
- **Alternatives**: *Keep the count and raise `max_attempts`*: keeps an honest total, but now there are two numbers whose meaning drifts.
- **Why**: Redis only uses the count to decide "retry or dead-letter." Every attempt, including ones before the replay, is kept in PostgreSQL (#10).

## 9. Redis saves every write to an append-only file, synced once a second

**In short:** A Redis restart keeps the queue; the worst case is losing about one second of writes if the whole machine crashes.

- **Decision**: `--appendonly yes`. Redis logs each write and replays the log on restart.
- **Alternatives**:
  - *No persistence*: fastest, but a restart loses every job.
  - *Periodic snapshots only*: a crash can lose minutes of jobs.
  - *Sync to disk on every write*: loses nothing, but much slower.
- **Why**: Losing jobs on restart would break the "zero job loss" goal, and syncing once a second keeps writes fast.
- **Tradeoffs**: Up to about 1 second of writes can be lost if the machine dies. Jobs and their history events are lost together, so they never disagree.

## 10. PostgreSQL: a `jobs` table plus a `job_attempts` table

**In short:** One row per job for its current state, and one row per attempt that's never overwritten.

- **Decision**: `jobs` holds each job's current state. `job_attempts` gets a new row per attempt, keyed by that attempt's lease token.
- **Alternatives**:
  - *Only a `jobs` table*: simplest, but each update overwrites the last, so attempt history is lost, which #8 depends on.
  - *An append-only log of every event*: a complete audit trail, but it grows fastest, and "what's this job's status?" means finding its latest event.
- **Why**: Current status is a simple lookup, and every attempt survives, even across replays.
- **Detail**: Lease tokens are the attempt's ID because attempt numbers restart at 1 after a replay ("job X, attempt 1" can happen twice), while tokens never repeat. That also stops the same attempt from being recorded twice.
- **Tradeoffs**: Two writes per attempt, and a join to see a job with its history.

## 11. History goes through an event stream in Redis (the outbox pattern)

**In short:** A job change and its history event are written in the same atomic step, so history can never miss a change.

- **Decision**: Every Lua script that changes a job also appends an event to a Redis Stream. A separate history writer copies events into PostgreSQL, and only confirms (`XACK`) and deletes them after the PostgreSQL commit.
- **Alternatives**:
  - *Workers write to PostgreSQL right after Redis*: simple, but a crash between the two writes loses that history forever, and a slow PostgreSQL slows every job.
  - *Copy Redis state into PostgreSQL every so often*: workers don't change, but anything between copies (like a failed attempt) is never recorded.
- **Why**: Both alternatives can lose history; this can't.
- **Duplicates**: A writer that crashes after committing but before confirming gets the same events again. Every write is idempotent (applying it twice gives the same result as once), so that's harmless.
- **Tradeoffs**: One more process. History lags slightly. If the writer is down, events pile up in Redis memory. Only one writer may write at a time, so a job's events stay in order (#25 enforces this).
- **Why Streams here but not for the queue (#2)**: Here I only need a reliable, ordered log with redelivery, which is exactly what Streams are. For the queue I wanted to design leases and retries myself, and Streams can't delay jobs.

## 12. A bad history event is retried 3 times, then set aside

**In short:** A one-off glitch gets another chance, a truly broken event can't block history, and nothing is thrown away.

- **Decision**: If one event fails because of its own data, the batch is rolled back and retried. After 3 failures, the event is saved to a `dead_events` table and the batch goes on without it.
- **Alternatives**:
  - *Set it aside on the first failure*: a brief glitch would push a good event aside and need manual work.
  - *Log it and skip it*: the event is lost, breaking #11's promise.
- **Why**: Only events that fail every time need a person.
- **Details**:
  - Each event runs inside a savepoint, so a failing event is undone without undoing the rest of the batch.
  - I count failures myself instead of using Redis's delivery count, because that also rises during a PostgreSQL outage and would push good events aside.
  - Connection errors (PostgreSQL down) never count as an event's fault.
- **Tradeoffs**: A bad event holds history up for about 3 seconds before it's set aside.

## 13. Adaptive polling for idle workers (replaced by #27)

**In short:** Check often right after work, back off when idle: most of fast polling's speed at slow polling's cost.

- **Decision**: An idle worker waits 10 ms, doubling after each empty check up to 500 ms; finding a job resets it to 10 ms.
- **Alternatives**:
  - *Fixed 500 ms*: least load, but a new job can wait up to 500 ms (p99 111–253 ms).
  - *Fixed 50 ms*: p99 31–39 ms, but 16 idle workers sent 952 commands/sec, 6.6× more.
- **Why**: First measurement: p99 33–62 ms at the same idle cost as 500 ms polling. Same backoff idea as retries (#5).
- **What re-measuring showed**: Fixed 50 ms was more consistent (p99 33–43 ms vs adaptive's 55–155 ms in a later run). I kept adaptive because fixed polling's cost grows with every worker, and real jobs take seconds. Later the doorbell (#27) beat both.

## 14. Whisk uses the queue as a library

**In short:** The queue stays generic; Whisk's code, API key and data stay in Whisk.

- **Decision**: Whisk installs `taskqueue` as a package, enqueues chat jobs from its web server, and runs its own worker with a `whisk_chat` task. It uses its own queue name (`whisk`), so demo workers never pick up chat jobs.
- **Alternatives**:
  - *Put the chat task inside task-queue*: nothing new to run, but the queue would contain baking-app code and need Whisk's secrets.
  - *A separate service with an HTTP API*: most "real-service," but the most work: a new API, authentication between services, and the chat code still has to run somewhere.
- **Why**: It's how real task libraries are used.
- **Consequences**: The package was renamed from `src` to `taskqueue`, and importing it no longer loads task-queue's `.env`, so it can't change Whisk's settings.

## 15. The browser checks for the chat answer once a second

**In short:** The web server answers instantly and stays free; up to a second of extra delay on a multi-second answer isn't noticeable.

- **Decision**: `POST /chat` returns `202 Accepted` with a job ID right away; the browser asks `GET /chat/<id>` every second until it's ready.
- **Alternatives**:
  - *Long polling*: instant answer, but each waiting request ties up a server process, which is the problem I was fixing.
  - *Server-Sent Events (push)*: real push and a path to word-by-word streaming, but Whisk's server handles one request per process, so it would need an async server, and the server would still poll PostgreSQL underneath.
- **Why**: Each check takes milliseconds.
- **Tradeoffs**: Extra small requests while waiting, and no word-by-word streaming.

## 16. Apps read results from PostgreSQL history

**In short:** No core queue change, and results are kept forever.

- **Decision**: Whisk reads a finished job's result from the history database. "Not found yet" means still pending.
- **Alternatives**: *Store results in Redis*: faster, but a core change plus an expiry rule so Redis doesn't fill up, and results would be temporary.
- **Why**: Reuses what already exists.
- **Tradeoffs**: History lags Redis by milliseconds when the writer keeps up, so answers appear slightly later. Whisk needs read access to the queue's database, which ties the two together.

## 17. Idempotency: keys when submitting, checkpoints when running

**In short:** Duplicates come from two places, submitting twice and running twice, so each gets its own fix.

- **Decision**:
  1. `enqueue(..., idempotency_key=...)`: the same key within 24 hours returns the original job instead of creating a new one. The check and the create are one Lua script, so two requests can't both create one.
  2. When a task succeeds, its result is saved on the job only if none is saved yet (first finisher wins). Before running a task, a worker reuses a saved result instead of redoing the work.
- **Alternatives**:
  - *Keys only*: stops double-clicks and client retries, but not a job running twice after a lease expires.
  - *Checkpoints only*: stops most re-runs, but not two separate jobs from a double submit.
- **Why**: The queue is at-least-once (every job runs, sometimes more than once), so both sources need covering.
- **Limits**: If a frozen worker loses its lease but is still running, another worker can run the same job at the same time. Checkpoints keep the recorded result consistent, but only the outside service (with its own idempotency keys) can stop the second call. Exactly-once isn't possible inside the queue alone.

## 18. Priority: separate "delayed" and "ready" sets, with aging

**In short:** Priority only orders jobs that can run now, and a long-waiting job eventually catches up with urgent ones, so nothing starves.

- **Decision**: The single `pending` set became two. `delayed` holds jobs waiting on a delay or backoff, scored by when they're ready. `ready` holds jobs that can run now, scored by *time it became ready − priority × 60 s*. Each lease moves due jobs from `delayed` to `ready`, then takes the lowest score.
- **Alternatives**:
  - *One set per priority level*: smallest change, but a fixed number of levels, a lookup per level on every lease, and every other part has to know all the sets.
  - *Fold priority into the existing score*: almost no code, but a high-priority job delayed 30 minutes would look ready immediately.
- **Why**: Any number of levels, first-come-first-served within a level, one lookup per lease, and delays and backoff don't change.
- **Starvation**: I chose aging over strict priority. Each level is worth 60 seconds of waiting, so an old low-priority job overtakes new urgent ones eventually. Strict priority lets a steady stream of urgent jobs block everything else forever. The 60 s is stored once in Redis so every producer and worker agrees.
- **Tradeoffs**: The biggest core change (enqueue, lease, retry, reaper, replay and metrics all changed). The "move what's due" step is capped at 1,000 jobs per lease, so if more than that become due at once, their order is approximate until it catches up.

## 19. Cleaning up finished jobs: a "finished" set plus a batched sweep

**In short:** Redis memory stays bounded, including dead jobs nobody ever looks at, while PostgreSQL keeps everything.

- **Decision**: When a job finishes, it joins a `finished` set scored by finish time. Once a minute, workers delete Redis copies of jobs done over 24 hours ago and dead over 30 days ago, 100 at a time.
- **Alternatives**:
  - *Expire each job automatically on finish*: one line, but ignored dead jobs would pile up forever.
  - *Delete on finish*: least memory, but for a moment the job would be in neither Redis nor history (history lags slightly).
- **Why**: The only option that also limits an ignored dead-letter queue.
- **Tradeoffs**: Another set and script. A dead job older than 30 days can't be replayed. Jobs that finished before this feature existed weren't in the set, so a one-time script (`scripts/backfill_finished.py`) added about 106,000 of them.

## 20. Hung tasks: a time limit enforced in Redis, and the stuck worker restarts itself

**In short:** No heartbeat can extend a lease past the job's time limit, so a hung task is always caught, even if its code is buggy.

- **Decision**: Each job has `timeout_seconds` (default 300). Neither the lease nor any heartbeat can push the expiry past *start + timeout*. The reaper then counts it as a failed attempt ("timed out after N s"). The stuck worker notices, exits, and Docker restarts it.
- **Alternatives**:
  - *Run each task in a child process the worker can kill*: really stops the work, but starting a process per job slows short jobs a lot.
  - *Give tasks a deadline and trust them*: only protects tasks written correctly, not the surprise hang this is for.
- **Why**: It reuses leases and the reaper, enforced in one place.
- **Tradeoffs**: Python can't kill a thread, so the hung task only stops because its whole process exits, which needs a supervisor like Docker. A worker that loses its lease for any reason also exits, so it stops doing duplicate work.

## 21. Errors that shouldn't be retried

**In short:** A per-task list of hopeless error types, plus `PermanentError` for cases only the task can recognize; everything else is still retried.

- **Decision**: `Task(fn, no_retry=(SomeError, ...))` lists error types that will never succeed, and any task can raise `PermanentError`. Either way the job goes straight to the dead-letter queue. Unknown task names do too.
- **Alternatives**:
  - *Only the exception*: every task must catch and convert errors itself, and it's easy to miss one.
  - *Only the list*: matches by type only, but sometimes the same type is temporary one time and permanent the next.
  - *Guess from the exception type automatically*: it guesses wrong and is hard to defend.
- **Why**: In Whisk, a bad API key (401) fails immediately instead of costing 2 more Claude calls, while "overloaded" (529) is still retried.
- **Tradeoffs**: Two mechanisms to explain. Listing the wrong error dead-letters jobs that might have worked, but replay recovers them.

## 22. Worker registry: a sorted set scored by "last seen"

**In short:** The dashboard can show each worker working, dying (red), and being removed.

- **Decision**: Each worker checks in about once a second from its own thread, with a small status record (current job, done and failed counts). Silent for 5 s means dead; after 60 s it's removed. A clean shutdown removes it right away, so only crashes show as dead.
- **Alternatives**:
  - *One self-expiring key per worker*: no cleanup code, but dead workers just vanish, and listing them means scanning all of Redis.
  - *No registry; show running jobs*: idle workers would be invisible.
- **Why**: Same "score = time" pattern; listing workers is one read.
- **Details**: Check-ins use a separate thread because a busy worker's main thread is stuck inside the task. Worker IDs have a random part so a restarted container doesn't look like the one that died.
- **Tradeoffs**: One more structure. It's for visibility only; nothing in job processing depends on it.

## 23. Discarding dead jobs, recorded in history

**In short:** Free the dead-letter queue now, but history still shows what was given up on and when.

- **Decision**: `discard(job_id)` and `discard_dead(error_contains=...)` delete dead jobs from Redis and write a `discarded` event in the same script. History marks them `discarded` with a time.
- **Alternatives**:
  - *Delete from Redis only*: history would still say `dead`, so you couldn't tell "waiting for a person" from "a person gave up."
  - *Archive instead of delete*: allows undo, but memory isn't freed, and replay already gives a second chance.
- **Why**: An audit trail for free, since every change is already an event.
- **Tradeoffs**: A schema change and one more event type.

## 24. Backpressure: say no when the queue is full

**In short:** When overloaded, refuse new jobs with a clear error instead of letting Redis run out of memory.

- **Decision**: Enqueue raises `QueueFull` once 100,000 jobs are waiting; counting and adding are one atomic step, so producers can't overshoot. Payloads over 64 KB raise `PayloadTooLarge`. Redis itself is capped at 512 MB and refuses writes rather than silently deleting data.
- **Alternatives**:
  - *Make producers wait for space*: in Whisk, web requests would hang, which is what the queue was built to fix.
  - *Drop low-priority jobs to make room*: keeps urgent work flowing, but throwing away accepted jobs breaks the promise that an accepted job is never lost.
- **Why**: The caller can respond sensibly (Whisk says "busy, try again in a minute"), and Redis memory, which also holds the history events, stays safe.
- **Tradeoffs**: Every producer must handle `QueueFull`.

## 25. One active history writer: a Redis lock plus a fencing token

**In short:** The lock decides who *should* write; the fencing token makes sure a writer that froze and lost the lock *can't*.

- **Decision**: Writers compete for a Redis lock that expires in 10 s and is renewed every ~3 s. Each new holder gets the next number from a counter (the fencing token). Every batch first records its token in PostgreSQL, but only if no higher token exists; otherwise the batch is rejected. Other writers wait on standby and take over when the lock expires.
- **Alternatives**:
  - *Redis lock only*: a writer that freezes past the expiry can wake up and write one more batch after someone else took over.
  - *PostgreSQL advisory lock*: little code, but a writer that's frozen yet still connected keeps the lock, and history stalls.
- **Why**: Correct even when a writer freezes, with failover in about 10 s. It's the same idea as lease tokens on jobs, applied to the writer.
- **Tradeoffs**: The most moving parts of the three.
- **Verified**: Killing the active writer with SIGKILL: the standby took over with the next token and caught up.

## 26. Graceful shutdown: finish within 25 s, otherwise hand the job back

**In short:** Short jobs finish during a deploy, long ones go back to the queue without using up an attempt, and deploys stay fast.

- **Decision**: On a stop signal, the worker keeps running its current job for up to 25 s (inside Docker's 30 s window). If the job doesn't finish, the worker calls `release()`: the job goes back to the ready set with its attempt undone and its priority kept. History records it as `released`, not a failure.
- **Alternatives**:
  - *Make the stop window as long as the longest job*: no code, but a deploy could take 5+ minutes.
  - *Hand back immediately*: fast, but throws away work that was a second from finishing.
- **Why**: The common case (short jobs) loses no work; deploy time stays bounded.
- **Tradeoffs**: Long jobs still restart from scratch. If a job finishes at the same moment it's released, the lease token lets only one win. Whisk sets this to 50 s because its Claude calls can take 45 s.
- **Verified**: `docker stop` during a 40 s job: it waited 25 s, handed the job back, exited cleanly, and another worker ran it as attempt 1.

## 27. A doorbell instead of polling

**In short:** Each new job drops one token in a Redis list; one idle worker wakes for it, and idle workers send nothing while they wait.

- **Decision**: When a job becomes ready, the script also pushes a token onto `tq:<queue>:wake`. Idle workers block on `BLPOP` until a token arrives (at most 1 s, so they still do housekeeping). Each token is used exactly once: by the worker it woke, or by the lease script if a busy worker took the job first.
- **Alternatives**:
  - *Publish/subscribe*: every idle worker wakes for every job (thundering herd), and messages sent while a worker isn't listening are lost.
  - *Redis keyspace notifications*: the same problems, plus a server setting that's off by default.
- **Why**: One job wakes one worker. The sorted sets are still the source of truth, so a missed or extra token is harmless.
- **A bug I found by measuring**: in my first version, only idle workers removed tokens. Under load, busy workers took jobs directly, tokens piled up to the 1,000 cap, and workers never actually waited. Tests passed; I caught it by watching the list stuck at 1,000 during a benchmark. Now the lease script uses up a token too, and a regression test covers it.
- **Result**: p99 about 23 ms at 4–16 workers, the best and most consistent of every mode, with the lowest idle load (293 Redis requests/sec vs 1,667 for 50 ms polling, 16 workers).
- **Tradeoffs**: Each waiting worker holds a Redis connection. A worker that crashes holding a token loses it, but the 1 s cap limits the delay.

## 28. The history writer writes each batch in bulk, with the old way as a fallback

**In short:** Work out every job's final state for the batch, write it in 4 bulk statements, and fall back to one event at a time only if something's wrong.

- **Decision**: For each batch, the writer goes through the events in order, works out where each job and attempt ends up, and writes that with 4 statements: insert new jobs, insert new attempts, finish attempts, update jobs. If that fails for any reason except PostgreSQL being down, it's undone and the batch runs one event at a time as before, which finds the bad event (#12).
- **Alternatives**:
  - *Pipeline mode*: send the same per-event statements without waiting for each reply. Smallest change, but still several statements per event.
  - *Several streams, one writer each*: scales by adding writers, but multiplies the lock and token machinery from #25.
- **Why**: Measuring showed the cost was per event (bigger batches didn't help), so I cut the statements per event.
- **Tradeoffs**: The bulk version has to follow exactly the same rules as the per-event version. A test applies a messy workload both ways, in batch sizes from 1 to all events, and checks the rows are identical. I broke the bulk version on purpose to make sure the test catches it.
- **Result**: 950 → 14,405 events/s at the default batch size (317 → 4,802 jobs/s), 15× faster, now well above what the workers produce.
