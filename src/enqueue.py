"""Submit a job from the command line.

Examples:
    python -m src.enqueue echo '{"msg": "hello"}'
    python -m src.enqueue sleep '{"seconds": 5}' --delay 10
    python -m src.enqueue fail --max-attempts 3
"""

import argparse
import json
import os

import redis

from src.core.queue import Queue


def main() -> None:
    parser = argparse.ArgumentParser(description="Submit a job to the queue.")
    parser.add_argument("task", help="task name, e.g. echo, sleep, fail")
    parser.add_argument("args", nargs="?", default="{}", help="task arguments as JSON")
    parser.add_argument("--delay", type=float, default=0, help="seconds before the job is ready")
    parser.add_argument("--max-attempts", type=int, default=5)
    parser.add_argument("--count", type=int, default=1, help="submit this many copies")
    opts = parser.parse_args()

    client = redis.Redis.from_url(os.environ.get("REDIS_URL", "redis://localhost:6379/0"))
    queue = Queue(client, name=os.environ.get("QUEUE_NAME", "default"))
    payload = {"task": opts.task, "args": json.loads(opts.args)}
    for _ in range(opts.count):
        job_id = queue.enqueue(payload, delay_seconds=opts.delay, max_attempts=opts.max_attempts)
        print(job_id)


if __name__ == "__main__":
    main()
