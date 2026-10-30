"""PostgreSQL connection and schema setup.

Create the tables with:
    python -m src.db init
"""

import sys
from pathlib import Path

import psycopg

from src import config

SCHEMA_FILE = Path(__file__).resolve().parent / "core" / "schema.sql"


def connect(dbname: str | None = None) -> psycopg.Connection:
    return psycopg.connect(config.postgres_conninfo(dbname))


def init_schema(conn: psycopg.Connection) -> None:
    """Create tables and indexes if they don't exist yet."""
    with conn.transaction():
        conn.execute(SCHEMA_FILE.read_text())


def main() -> None:
    if sys.argv[1:] != ["init"]:
        sys.exit("usage: python -m src.db init")
    with connect() as conn:
        init_schema(conn)
    print("schema ready")


if __name__ == "__main__":
    main()
