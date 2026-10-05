"""DuckDB connection and schema migrations."""

from __future__ import annotations

from importlib import resources
from pathlib import Path

import duckdb

_MIGRATIONS_PACKAGE = "sds.db.migrations"


def connect(path: Path | str = ":memory:") -> duckdb.DuckDBPyConnection:
    """Open the database and bring the schema up to date."""
    if isinstance(path, Path):
        path.parent.mkdir(parents=True, exist_ok=True)
    conn = duckdb.connect(str(path))
    conn.execute("SET TimeZone = 'UTC'")
    migrate(conn)
    return conn


def _migration_files() -> list[tuple[int, str]]:
    files = []
    for entry in resources.files(_MIGRATIONS_PACKAGE).iterdir():
        name = entry.name
        if name.endswith(".sql"):
            version = int(name.split("_", 1)[0])
            files.append((version, entry.read_text(encoding="utf-8")))
    return sorted(files)


def migrate(conn: duckdb.DuckDBPyConnection) -> list[int]:
    """Apply pending migrations in order, each in its own transaction. Idempotent."""
    conn.execute(
        "CREATE TABLE IF NOT EXISTS schema_migrations ("
        " version INTEGER PRIMARY KEY, applied_at TIMESTAMPTZ NOT NULL DEFAULT now())"
    )
    applied = {row[0] for row in conn.execute("SELECT version FROM schema_migrations").fetchall()}
    newly_applied = []
    for version, sql in _migration_files():
        if version in applied:
            continue
        conn.execute("BEGIN")
        try:
            conn.execute(sql)
            conn.execute("INSERT INTO schema_migrations (version) VALUES (?)", [version])
            conn.execute("COMMIT")
        except Exception:
            conn.execute("ROLLBACK")
            raise
        newly_applied.append(version)
    return newly_applied
