# task-queue

A distributed task queue built from scratch in Python, using Redis for the queue and PostgreSQL for job history. It focuses on reliability: jobs survive worker crashes, failed jobs retry with backoff, and repeatedly failing jobs go to a dead-letter queue. No Celery or RQ; building the queue is the point.

> Status: all six reliability features are built (leases, retries with backoff, dead-letter queue, idempotency, delayed and priority jobs, benchmarks and failure tests), plus PostgreSQL history and a real integration with my recipe app.

## Architecture

```mermaid
flowchart LR
    P[Producers] -->|enqueue| R[(Redis queue)]
    R -->|lease job| W[Workers]
    W -->|ack / fail| R
    R -->|event stream| H[History writer]
    H -->|batched, idempotent writes| DB[(PostgreSQL)]
```

- **Producers** submit jobs.
- **Redis** holds pending jobs.
- **Workers** lease jobs, run them, and report results.
- **PostgreSQL** stores job history: a `jobs` table with each job's current state, and a `job_attempts` table with one permanent row per attempt.
- **History writer** copies events from Redis into PostgreSQL. Each job change and its history event are written in the same atomic Redis step (the outbox pattern), so history can't miss a change, and a PostgreSQL outage never slows the workers.

## Features

- **Leases**: a worker that takes a job holds it for a fixed time. If the worker doesn't ack (confirm it finished) before the lease expires, a reaper puts the job back in the queue.
- **Heartbeats**: a worker running a long job extends its lease every few seconds. If the worker dies, the heartbeats stop and the lease expires.
- **Lease tokens**: each lease gets a unique token, so a slow worker whose lease already expired can't mark a job done that another worker now owns.
- **Delayed jobs**: a job can be scheduled to become ready later.
- **Atomic moves**: every state change runs as a Redis Lua script, so jobs can't be lost between steps.

- **Retries with backoff and jitter**: a failed job waits a random time between 0 and `base × 2^(attempts−1)` (capped) before retrying, so jobs that fail together don't retry together.
- **Dead-letter queue**: after `max_attempts` failures, including worker crashes, a job moves to a dead-letter set so poison jobs can't loop forever.
- **Dead-letter replay**: after fixing the cause of an outage, replay one dead job or every job whose error matches. Bulk replay runs in small batches so Redis never stalls the workers.
- **Durable job history**: every attempt is recorded in PostgreSQL, including attempts before a replay. Writes are idempotent, so a crashed history writer can safely re-apply events. A malformed event is retried 3 times, then set aside in a `dead_events` table so it can't block history.
- **Idempotency keys**: submitting a job with the same key twice returns the original job instead of a duplicate. The check and the create are one atomic script, so even simultaneous submissions create one job.
- **Result checkpoints**: a finished task's result is saved before the ack. If the job is redelivered (the worker crashed or lost its lease before acking), the next worker reuses the result instead of redoing the work.
- **Priority with aging**: higher-priority jobs run first, but each priority level is worth 60 seconds of waiting, so a lower-priority job that has waited long enough catches up and nothing starves. Delayed and retrying jobs wait in a separate set, so priority never lets a job run before its time.

## Getting started

Requires [Docker Desktop](https://www.docker.com/products/docker-desktop/).

```bash
git clone https://github.com/calliec-1223/task-queue.git && cd task-queue
cp .env.example .env              # then set POSTGRES_PASSWORD to any strong value
docker compose up -d --build      # Redis, PostgreSQL, 3 workers, history writer, dashboard
```

Open the dashboard at http://localhost:8000.

- `docker compose up -d --scale worker=10` runs 10 workers instead of 3.
- `docker compose logs -f worker` follows the worker logs.
- `docker compose down` stops everything. Data is kept in Docker volumes, so jobs and history survive restarts.

To submit jobs or run tests from your own machine, also set up Python:

```bash
python3 -m venv .venv && source .venv/bin/activate
pip install -r requirements-dev.txt
```

## Usage

With Docker Compose running, workers and the history writer are already up. To run them directly on your machine instead (e.g. while developing):

```bash
python -m taskqueue.worker
python -m taskqueue.history
```

Submit jobs:

```bash
python -m taskqueue.enqueue echo '{"msg": "hello"}'          # runs right away
python -m taskqueue.enqueue sleep '{"seconds": 5}' --delay 10  # ready in 10 seconds
python -m taskqueue.enqueue fail --max-attempts 3             # always fails: retries, then dead-letters
python -m taskqueue.enqueue echo '{"msg": "urgent"}' --priority 5  # runs ahead of priority-0 jobs
```

Watch it live at http://localhost:8000. It shows where jobs are right now (ready, waiting, running, done, dead), jobs completed and attempts failed per second over the last minute, and recent jobs with their status and errors. Raw numbers are at http://localhost:8000/metrics as JSON.

Started by Docker Compose, or directly with `python -m taskqueue.metrics`.

Inspect and replay the dead-letter queue:

```bash
python -m taskqueue.dlq list
python -m taskqueue.dlq replay <job_id>
python -m taskqueue.dlq replay-all --error "connection refused"
```

Each worker leases a job, sends heartbeats from a background thread while the job runs, then acks or fails it. Every worker also reaps expired leases, which is safe because reaping is one atomic Lua script. Ctrl-C finishes the current job before exiting.

Settings (environment variables): `REDIS_URL`, `QUEUE_NAME`, `LEASE_SECONDS`.

## Benchmarks

All numbers below come from real runs ([raw results](bench/results/)) of the current code on an Apple M2 MacBook (8 cores), with every service running in Docker Compose. Workers run 20 ms jobs (`sleep`), standing in for I/O-bound work like an API call.

### Throughput scales almost linearly with workers

![Throughput chart](docs/bench-throughput.png)

| Workers | Jobs/sec | Ideal | Efficiency |
|---|---|---|---|
| 1 | 38 | 50 | 76% |
| 2 | 81 | 100 | 81% |
| 4 | 176 | 200 | 88% |
| 8 | 331 | 400 | 83% |
| 16 | 667 | 800 | 83% |

"Ideal" is workers ÷ 20 ms, assuming zero queue overhead. Efficiency stays around 80% from 1 to 16 workers, so the queue itself isn't the bottleneck at this scale. The gap is most likely fixed per-job overhead in the worker (several Redis round trips per job: lease, read the job, checkpoint, ack; plus starting a heartbeat thread), not yet profiled.

### Latency: adaptive polling vs. fixed intervals

![Latency chart](docs/bench-latency.png)

Jobs arrive at a steady 100/sec for 20 seconds. Latency is enqueue → done, timed by the Redis clock on both ends. Cells show p50 / p99.

| Workers | Poll every 500 ms | Poll every 50 ms | Adaptive (default) |
|---|---|---|---|
| 1 | 12,815 / 28,178 ms | 12,347 / 27,738 ms | 12,820 / 28,277 ms |
| 2 | 1,313 / 2,540 ms | 984 / 2,050 ms | 1,272 / 2,358 ms |
| 4 | 46 / 161 ms | 26 / 38 ms | 26 / 155 ms |
| 8 | 45 / 212 ms | 25 / 33 ms | 26 / 64 ms |
| 16 | 36 / 186 ms | 24 / 43 ms | 25 / 55 ms |

Workers can't be notified of new jobs (sorted sets have no blocking pop, [DECISIONS.md](DECISIONS.md) #2), so idle workers poll. A job that arrives while a worker is waiting sits ready until the worker checks again.

Faster polling costs Redis work while workers are idle. Measured with 16 idle workers:

| Polling mode | Redis commands/sec (idle) |
|---|---|
| Every 500 ms | 225 |
| Every 50 ms | 1,566 |
| **Adaptive** | **228** |

**Adaptive polling** (the default) waits 10 ms after the queue goes empty and doubles the wait after each empty check, up to 500 ms; finding a job resets it. It costs the same idle load as 500 ms polling, about 7× less than 50 ms polling. Its p50 matches 50 ms polling, but its p99 is less consistent: 155 ms at 4 workers in this run (50 ms in an earlier run), because at moderate load a worker can be mid-backoff when work arrives. Counts include the commands each Lua script runs inside Redis.

- 1–2 workers can't keep up with 100 jobs/sec, so the backlog grows and latency is measured in seconds in every mode.
- p99 varies between runs, so compare modes within the same run.

### Zero job loss while workers are killed

`python -m bench.chaos` submits 1,000 jobs while repeatedly `SIGKILL`ing random worker containers and the history writer (no chance to clean up), restarting them a second later.

| Jobs submitted | Completed | Lost | Worker kills | History writer kills | Jobs interrupted mid-run | Recorded done twice |
|---|---|---|---|---|---|---|
| 1,000 | 1,000 | **0** | 20 | 5 | 13 | 0 |

Interrupted jobs were recovered by lease expiry and finished on a later attempt. Some kills hit idle workers, which is why there are fewer interrupted jobs than kills. Every job appears exactly once in PostgreSQL history, despite the history writer being killed 5 times.

Reproduce: `docker compose up -d --build`, then `python -m bench.benchmark`, `python -m bench.idle_load`, `python -m bench.chaos`, and `python -m bench.plot`.

## Real-world use: Whisk's AI baking assistant

[Whisk](https://github.com/calliec-1223/whisk) is my recipe app. Its chat assistant called the Claude API inside the web request, so a server process sat idle for the whole answer (2-3 seconds) and couldn't serve anyone else. Whisk now uses this queue as a library: `POST /chat` enqueues the question and returns `202 Accepted` with a job ID in milliseconds; a Whisk worker asks Claude; the browser checks `GET /chat/<id>` once a second. Only the user who asked can read the answer. ([DECISIONS.md](DECISIONS.md) #14-16)

![Whisk before and after](docs/whisk-before-after.png)

Load test: Whisk on gunicorn with 2 server processes. Some users chat back to back while another loads their recipe list 5 times a second. Claude is replaced by a stand-in that replays 20 measured real response times (median 2.416 s), so the test is free and repeatable. Medians:

| People chatting | Page load, before | Page load, after | `POST /chat`, before | `POST /chat`, after | Answer, before | Answer, after |
|---|---|---|---|---|---|---|
| 1 | 7 ms | 6 ms | 2,437 ms | 5 ms | 2.4 s | 3.1 s |
| 4 | 3,375 ms | 7 ms | 4,935 ms | 17 ms | 4.9 s | 3.1 s |
| 8 | 9,255 ms | 8 ms | 9,959 ms | 21 ms | 10.0 s | 5.2 s |

- **Page loads stay fast.** Before, every chat held one of the 2 server processes for the whole Claude call, so recipe pages waited behind chats. After, the server only enqueues and reads results.
- **Answers are faster under load, slower when idle.** With 1 user, the queue adds about 0.7 s (the browser checks once a second, plus the history writer's lag). With 4-8 users, answers arrive sooner, partly because queue mode runs 4 workers in addition to the 2 server processes, so it isn't an equal-resources comparison for answer time. The page-load result doesn't depend on that: it comes from the web server no longer waiting on Claude.
- **Retries for free.** A failed Claude call is retried with backoff, then dead-lettered, instead of the user getting an error.

[Raw results](bench/results/). Reproduce from Whisk's `backend/`: `python -m bench.measure_claude` (real calls, costs a few cents) and `python -m bench.load_test`.

## Design decisions

See [DECISIONS.md](DECISIONS.md).

## Testing

```bash
pytest
```

Tests need Redis and PostgreSQL running (`docker compose up -d redis postgres`). They use Redis database 15 and wipe it, so they never touch real data. The failure tests include a real worker process killed with `SIGKILL` mid-job, a poison job that crashes every worker, and stale workers trying to ack, fail or heartbeat after losing their lease.
