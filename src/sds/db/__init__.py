"""DuckDB connection and schema migrations."""

from __future__ import annotations

from importlib import resources
from pathlib import Path

import duckdb
import polars as pl

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


def insert_frame(
    conn: duckdb.DuckDBPyConnection, table: str, frame: pl.DataFrame, on_conflict: str = ""
) -> int:
    """Insert a whole frame in one statement; returns the row count.

    Row-at-a-time `executemany` runs at about 1k rows/s in DuckDB; a scan of the frame
    is orders of magnitude faster. Columns are matched by name. `on_conflict` is a
    trusted SQL suffix such as "ON CONFLICT (...) DO UPDATE SET ...".
    """
    if frame.is_empty():
        return 0
    cols = ", ".join(frame.columns)
    conn.register("_insert_frame", frame)
    try:
        conn.execute(f"INSERT INTO {table} ({cols}) SELECT {cols} FROM _insert_frame {on_conflict}")
    finally:
        conn.unregister("_insert_frame")
    return frame.height
