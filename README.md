# task-queue

A distributed task queue built from scratch in Python, using Redis for the queue and PostgreSQL for job history. It focuses on reliability: jobs survive worker crashes, failed jobs retry with backoff, and repeatedly failing jobs go to a dead-letter queue. No Celery or RQ; building the queue is the point.

> Status: Phase 3 complete: benchmarks and failure-injection results below. Next: integrating with Whisk.

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

Planned: idempotency, priority.

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

All numbers below come from real runs ([raw results](bench/results/)) on an Apple M2 MacBook (8 cores), with every service running in Docker Compose. Workers run 20 ms jobs (`sleep`), standing in for I/O-bound work like an API call.

### Throughput scales almost linearly with workers

![Throughput chart](docs/bench-throughput.png)

| Workers | Jobs/sec | Ideal | Efficiency |
|---|---|---|---|
| 1 | 39 | 50 | 78% |
| 2 | 84 | 100 | 84% |
| 4 | 172 | 200 | 86% |
| 8 | 327 | 400 | 82% |
| 16 | 642 | 800 | 80% |

"Ideal" is workers ÷ 20 ms, assuming zero queue overhead. Efficiency stays around 80% from 1 to 16 workers, so the queue itself isn't the bottleneck at this scale. The gap is most likely fixed per-job overhead in the worker (several Redis round trips per job: lease, read the job, ack; plus starting a heartbeat thread), not yet profiled.

### Latency: adaptive polling gets fast pickup without the idle cost

![Latency chart](docs/bench-latency.png)

Jobs arrive at a steady 100/sec for 20 seconds. Latency is enqueue → done, timed by the Redis clock on both ends. Cells show p50 / p99.

| Workers | Poll every 500 ms | Poll every 50 ms | Adaptive (default) |
|---|---|---|---|
| 1 | 12,996 / 28,060 ms | 11,922 / 26,830 ms | 12,109 / 27,006 ms |
| 2 | 1,188 / 2,166 ms | 900 / 1,919 ms | 810 / 1,616 ms |
| 4 | 41 / 253 ms | 26 / 38 ms | 26 / 50 ms |
| 8 | 36 / 112 ms | 24 / 39 ms | 25 / 62 ms |
| 16 | 41 / 203 ms | 24 / 31 ms | 24 / 33 ms |

Workers can't be notified of new jobs (sorted sets have no blocking pop, [DECISIONS.md](DECISIONS.md) #2), so idle workers poll. A job that arrives while a worker is waiting sits ready until the worker checks again, which is why a fixed 500 ms interval shows up in p99.

Faster polling costs Redis work while workers are idle. Measured with 16 idle workers:

| Polling mode | Redis commands/sec (idle) |
|---|---|
| Every 500 ms | 144 |
| Every 50 ms | 952 |
| **Adaptive** | **146** |

**Adaptive polling** (the default) waits 10 ms after the queue goes empty and doubles the wait after each empty check, up to 500 ms. Finding a job resets it. That gives roughly the latency of 50 ms polling at the idle cost of 500 ms polling. (Counts include the commands each Lua script runs inside Redis.)

- 1–2 workers can't keep up with 100 jobs/sec, so the backlog grows and latency is measured in seconds in every mode.
- p99 varies between runs: in an earlier run, 500 ms polling at 16 workers measured 71 ms instead of 203 ms. Compare modes within the same run.

### Zero job loss while workers are killed

`python -m bench.chaos` submits 1,000 jobs while repeatedly `SIGKILL`ing random worker containers and the history writer (no chance to clean up), restarting them a second later.

| Jobs submitted | Completed | Lost | Worker kills | History writer kills | Jobs interrupted mid-run | Recorded done twice |
|---|---|---|---|---|---|---|
| 1,000 | 1,000 | **0** | 20 | 5 | 11 | 0 |

Interrupted jobs were recovered by lease expiry and finished on a later attempt. Some kills hit idle workers, which is why there are fewer interrupted jobs than kills. Every job also appears exactly once in PostgreSQL history, despite the history writer being killed 5 times.

Reproduce: `docker compose up -d --build`, then `python -m bench.benchmark`, `python -m bench.chaos`, and `python -m bench.plot`.

## Design decisions

See [DECISIONS.md](DECISIONS.md).

## Testing

```bash
pytest
```

Tests need Redis and PostgreSQL running (`docker compose up -d redis postgres`). They use Redis database 15 and wipe it, so they never touch real data. The failure tests include a real worker process killed with `SIGKILL` mid-job, a poison job that crashes every worker, and stale workers trying to ack, fail or heartbeat after losing their lease.
