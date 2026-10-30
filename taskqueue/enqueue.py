"""Submit a job from the command line.

Examples:
    python -m taskqueue.enqueue echo '{"msg": "hello"}'
    python -m taskqueue.enqueue sleep '{"seconds": 5}' --delay 10
    python -m taskqueue.enqueue fail --max-attempts 3
    python -m taskqueue.enqueue echo '{"msg": "urgent"}' --priority 5
"""

import argparse
import json

import redis

from taskqueue import config
from taskqueue.core.queue import Queue


def main() -> None:
    config.load_dotenv()
    parser = argparse.ArgumentParser(description="Submit a job to the queue.")
    parser.add_argument("task", help="task name, e.g. echo, sleep, fail")
    parser.add_argument("args", nargs="?", default="{}", help="task arguments as JSON")
    parser.add_argument("--delay", type=float, default=0, help="seconds before the job is ready")
    parser.add_argument("--max-attempts", type=int, default=5)
    parser.add_argument("--priority", type=int, default=0, help="higher runs sooner (default 0)")
    parser.add_argument("--timeout", type=float, default=300,
                        help="longest one attempt may run, in seconds (default 300)")
    parser.add_argument("--count", type=int, default=1, help="submit this many copies")
    opts = parser.parse_args()

    client = redis.Redis.from_url(config.redis_url())
    queue = Queue(client, name=config.queue_name())
    payload = {"task": opts.task, "args": json.loads(opts.args)}
    for _ in range(opts.count):
        job_id = queue.enqueue(payload, delay_seconds=opts.delay, max_attempts=opts.max_attempts,
                               priority=opts.priority, timeout_seconds=opts.timeout)
        print(job_id)


if __name__ == "__main__":
    main()
