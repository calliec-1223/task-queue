"""Inspect and replay the dead-letter queue from the command line.

Examples:
    python -m taskqueue.dlq list
    python -m taskqueue.dlq replay <job_id>
    python -m taskqueue.dlq replay-all --error "connection refused"
    python -m taskqueue.dlq discard <job_id>
    python -m taskqueue.dlq discard-all --error "this task always fails"
"""

import argparse

import redis

from taskqueue import config
from taskqueue.core.queue import Queue


def main() -> None:
    config.load_dotenv()
    parser = argparse.ArgumentParser(description="Dead-letter queue tools.")
    sub = parser.add_subparsers(dest="command", required=True)
    list_cmd = sub.add_parser("list", help="show the oldest dead jobs")
    list_cmd.add_argument("--count", type=int, default=20)
    one = sub.add_parser("replay", help="replay one dead job")
    one.add_argument("job_id")
    many = sub.add_parser("replay-all", help="replay dead jobs, optionally filtered by error")
    many.add_argument("--error", default="", help="only jobs whose last error contains this text")
    many.add_argument("--limit", type=int, default=None)
    gone = sub.add_parser("discard", help="give up on one dead job (history keeps it)")
    gone.add_argument("job_id")
    gone_many = sub.add_parser("discard-all", help="give up on dead jobs, optionally filtered by error")
    gone_many.add_argument("--error", default="", help="only jobs whose last error contains this text")
    gone_many.add_argument("--limit", type=int, default=None)
    opts = parser.parse_args()

    client = redis.Redis.from_url(config.redis_url())
    queue = Queue(client, name=config.queue_name())

    if opts.command == "list":
        for job in queue.list_dead(opts.count):
            print(f"{job['id']}  attempts={job['attempts']}  error={job.get('last_error', '')}")
    elif opts.command == "replay":
        print("replayed" if queue.replay(opts.job_id) else "not in dead-letter queue")
    elif opts.command == "replay-all":
        print(f"replayed {queue.replay_dead(opts.error, limit=opts.limit)} job(s)")
    elif opts.command == "discard":
        print("discarded" if queue.discard(opts.job_id) else "not in dead-letter queue")
    else:
        print(f"discarded {queue.discard_dead(opts.error, limit=opts.limit)} job(s)")


if __name__ == "__main__":
    main()
