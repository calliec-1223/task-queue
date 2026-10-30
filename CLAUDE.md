# CLAUDE.md

## About this project

This is a distributed task queue built in Python with Redis, PostgreSQL, and Docker.

It is a **resume and interview project**. I am a second-year CS student targeting FAANG software engineering internships. Claude may write code, but I must be able to explain what every part does, why it was designed that way, and what happens when things fail. Interviewers will ask "why" and "what if," not just "what."

## What the system does

- **Producers** (apps such as my baking app, Whisk) submit jobs to the queue.
- **Redis** stores the queue of pending jobs.
- **Workers** are separate processes that pick up jobs, run them, and report results.
- **PostgreSQL** stores job history: status, attempts, timestamps, results, and errors.

Required reliability features:

1. **Leases / visibility timeouts**: if a worker crashes mid-job, the job returns to the queue after a timeout.
2. **Retries with exponential backoff**: failed jobs are retried after increasing delays.
3. **Dead-letter queue**: jobs that keep failing are moved aside so they stop blocking others.
4. **Idempotency**: jobs carry IDs so duplicate deliveries do not cause duplicate work.
5. **Delayed and priority jobs**: jobs can be scheduled for later and given priority levels.
6. **Benchmarks and failure tests**: measured throughput and p99 latency as workers scale, and proof of zero job loss when workers are killed mid-run.

## How Claude writes code

Claude may write any code in this project, following these rules.

### Core logic (`taskqueue/core/`): design first, then code

Core logic includes enqueue, dequeue, leases, acks, retries, backoff, dead-letter handling, idempotency, Redis data structure choices, and the PostgreSQL schema.

Before writing any core component:

1. Lay out 2 or 3 design options with the tradeoffs of each.
2. Wait for me to choose one. Do not pick for me.
3. Then write the code.

After writing it:

1. Explain how it works in plain language.
2. Walk through at least one failure scenario it handles, step by step.
3. Ask me 2 or 3 interview-style questions about it ("why this and not X?", "what happens if Y crashes here?"). Tell me if my answers have gaps.

### Everything else: write freely

Docker files, CI, load tests, scripts, CLI parsing, config, logging, and test scaffolding can be written without asking first. Give a short summary of what was added.

**Docker exception**: whenever Claude writes or changes a Dockerfile or `docker-compose.yml`, explain what each line does and why, in plain language. I am new to Docker and need to be able to explain why the project uses it.

### General code rules

- Keep code simple and readable over clever. I need to explain it out loud.
- Add short comments on non-obvious lines, especially in core logic.
- Do not use Celery, RQ, or other existing task queue libraries. Building that is the point.
- Keep dependencies minimal and tell me why each new one is needed.

## Security: never leak secrets

This project will use secrets, including database passwords and my Claude API key for the Whisk integration. The repo will be public on GitHub.

- **Never** write real API keys, passwords, tokens, or connection strings into any file that gets committed. This includes code, tests, docs, the README, Docker files, and CI config.
- Load all secrets from environment variables, read from a `.env` file locally.
- `.env` must be listed in `.gitignore` before any secrets are added. Check this at the start of the project and before every commit.
- Keep a committed `.env.example` with variable names and placeholder values only (for example `ANTHROPIC_API_KEY=your-key-here`).
- In CI, use GitHub Actions secrets, never values written into workflow files.
- Never print or log secret values, including in error messages and debug output.
- Never echo a secret back to me in chat, even if I paste one by accident. Tell me to rotate it instead.
- Before any commit, scan the staged changes for anything that looks like a key, password, or token. If something looks suspicious, stop and tell me before committing.
- If a secret is ever committed, tell me immediately. Deleting it in a later commit is not enough because it stays in git history, so the key must be rotated (replaced with a new one).

## README maintenance

Maintain `README.md` as the public face of the project. Recruiters and interviewers will read it. Update it whenever a feature, setup step, or benchmark changes, without waiting for me to ask.

The README should contain:

1. **One-paragraph summary**: what the project is and why it exists.
2. **Architecture**: a short explanation of producers, Redis, workers, and PostgreSQL, plus a simple diagram (Mermaid is fine).
3. **Features**: the reliability features above, each with one sentence on how it works.
4. **Getting started**: exact steps to clone, create `.env` from `.env.example`, and run with Docker Compose.
5. **Usage**: how to submit a job and start workers, with examples.
6. **Benchmarks**: throughput and p99 latency results with charts, and the failure-injection results (jobs submitted vs. jobs completed while workers were killed).
7. **Design decisions**: a short summary with a link to `DECISIONS.md`.
8. **Testing**: how to run the tests.

Rules:

- Never put secrets or real credentials in the README.
- Only report benchmark numbers that came from actual runs. Never estimate or invent results.
- Keep it scannable. Short sections, no filler.

## Decision log

Maintain `DECISIONS.md`. Whenever I choose between design options, add an entry with:

- **Decision**: what I chose
- **Alternatives**: what was considered and rejected
- **Why**: the reasoning, including tradeoffs accepted

Claude drafts the entry. I review it and rewrite it in my own words if needed, since I will be explaining these in interviews.

## Testing

- Every reliability feature needs at least one test that simulates failure (crashes at bad moments, duplicate delivery, Redis restarts).
- When a test fails, explain what the failure proves is broken before fixing it.

## How I learn best

- One component at a time.
- Explain jargon the first time it comes up. Do not assume I know DevOps or infrastructure terms.
- Prefer clear, plain explanations over long ones.

## Build plan

- **Phase 1**: Core queue (enqueue, lease, ack, retry, dead-letter) with Redis, plus crash-simulation tests.
- **Phase 2**: PostgreSQL job history, Docker Compose setup, basic metrics endpoint.
- **Phase 3**: Load testing, failure injection, benchmark charts, README benchmarks section.
- **Phase 4**: Integrate with Whisk by moving slow Claude API calls onto the queue, and measure the before and after latency.

Do not jump ahead of the current phase without asking.

## Tech details

- Language: Python
- Queue store: Redis
- History store: PostgreSQL
- Local environment: Docker Compose
- OS: macOS

## Interview check

At the end of each phase, ask me to explain that phase as if to an interviewer, including one failure scenario it handles and one design tradeoff I made. If my explanation has gaps, tell me what is missing before we move on.
