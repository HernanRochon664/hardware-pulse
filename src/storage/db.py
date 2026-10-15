"""
SQLite connection factory and a minimal forward-only migration runner.

Migrations are the numbered ``*.sql`` files in ``src/storage/migrations``;
each one is applied once, in order, inside a transaction, and recorded in
``schema_migrations``.
"""

from __future__ import annotations

import sqlite3
from pathlib import Path

from src.settings import db_path, to_iso, utcnow

MIGRATIONS_DIR = Path(__file__).parent / "migrations"


def connect(path: Path | str | None = None, *, readonly: bool = False) -> sqlite3.Connection:
    """
    Open the database. Read-write connections are migrated to the latest schema.

    ``path=":memory:"`` gives an isolated, fully migrated in-memory database (tests).
    """
    target = str(path) if path is not None else str(db_path())
    if readonly:
        conn = sqlite3.connect(f"file:{target}?mode=ro", uri=True, check_same_thread=False)
    else:
        if target != ":memory:":
            Path(target).parent.mkdir(parents=True, exist_ok=True)
        conn = sqlite3.connect(target)
        conn.execute("PRAGMA journal_mode=WAL")
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys=ON")
    conn.execute("PRAGMA busy_timeout=10000")
    if not readonly:
        migrate(conn)
    return conn


def migrate(conn: sqlite3.Connection) -> list[str]:
    """Apply pending migrations. Returns the names of the migrations applied."""
    conn.execute(
        "CREATE TABLE IF NOT EXISTS schema_migrations (name TEXT PRIMARY KEY, applied_at TEXT)"
    )
    applied = {row[0] for row in conn.execute("SELECT name FROM schema_migrations")}
    done: list[str] = []
    for script in sorted(MIGRATIONS_DIR.glob("*.sql")):
        if script.name in applied:
            continue
        sql = script.read_text(encoding="utf-8")
        try:
            conn.execute("BEGIN")
            for statement in _split_statements(sql):
                conn.execute(statement)
            conn.execute(
                "INSERT INTO schema_migrations (name, applied_at) VALUES (?, ?)",
                (script.name, to_iso(utcnow())),
            )
            conn.execute("COMMIT")
        except Exception:
            conn.execute("ROLLBACK")
            raise
        done.append(script.name)
    return done


def _split_statements(sql: str) -> list[str]:
    """Split a migration file into statements (no semicolons inside our DDL strings)."""
    lines = [line for line in sql.splitlines() if not line.strip().startswith("--")]
    return [stmt.strip() for stmt in "\n".join(lines).split(";") if stmt.strip()]
