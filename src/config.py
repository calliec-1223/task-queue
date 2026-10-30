"""Settings from environment variables, with .env loaded for local development.

Secrets (like POSTGRES_PASSWORD) only ever come from the environment or .env,
never from code. Values are never printed or logged.
"""

import os
from pathlib import Path

ENV_FILE = Path(__file__).resolve().parent.parent / ".env"


def load_dotenv(path: Path = ENV_FILE) -> None:
    """Read KEY=VALUE lines from .env into os.environ.

    Variables already set in the real environment win, so CI and Docker
    can override anything without editing files.
    """
    if not path.exists():
        return
    for line in path.read_text().splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        os.environ.setdefault(key.strip(), value.strip())


load_dotenv()

REDIS_URL = os.environ.get("REDIS_URL", "redis://localhost:6379/0")
QUEUE_NAME = os.environ.get("QUEUE_NAME", "default")


def postgres_conninfo(dbname: str | None = None) -> str:
    """Connection settings for psycopg, built from separate variables.

    Kept as key=value pairs (not a URL), so the password needs no escaping.
    """
    password = os.environ.get("POSTGRES_PASSWORD")
    if not password:
        raise RuntimeError("POSTGRES_PASSWORD is not set (copy .env.example to .env)")
    parts = {
        "host": os.environ.get("POSTGRES_HOST", "localhost"),
        "port": os.environ.get("POSTGRES_PORT", "5432"),
        "user": os.environ.get("POSTGRES_USER", "taskqueue"),
        "password": password,
        "dbname": dbname or os.environ.get("POSTGRES_DB", "taskqueue"),
    }
    return " ".join(f"{k}={v}" for k, v in parts.items())
