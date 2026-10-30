"""Inspect and replay the dead-letter queue from the command line.

Examples:
    python -m src.dlq list
    python -m src.dlq replay <job_id>
    python -m src.dlq replay-all --error "connection refused"
"""

import argparse
import os

import redis

from src.core.queue import Queue


def main() -> None:
    parser = argparse.ArgumentParser(description="Dead-letter queue tools.")
    sub = parser.add_subparsers(dest="command", required=True)
    list_cmd = sub.add_parser("list", help="show the oldest dead jobs")
    list_cmd.add_argument("--count", type=int, default=20)
    one = sub.add_parser("replay", help="replay one dead job")
    one.add_argument("job_id")
    many = sub.add_parser("replay-all", help="replay dead jobs, optionally filtered by error")
    many.add_argument("--error", default="", help="only jobs whose last error contains this text")
    many.add_argument("--limit", type=int, default=None)
    opts = parser.parse_args()

    client = redis.Redis.from_url(os.environ.get("REDIS_URL", "redis://localhost:6379/0"))
    queue = Queue(client, name=os.environ.get("QUEUE_NAME", "default"))

    if opts.command == "list":
        for job in queue.list_dead(opts.count):
            print(f"{job['id']}  attempts={job['attempts']}  error={job.get('last_error', '')}")
    elif opts.command == "replay":
        print("replayed" if queue.replay(opts.job_id) else "not in dead-letter queue")
    else:
        print(f"replayed {queue.replay_dead(opts.error, limit=opts.limit)} job(s)")


if __name__ == "__main__":
    main()
