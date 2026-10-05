from __future__ import annotations

import duckdb

from sds.db import migrate


def test_migrations_are_idempotent(conn: duckdb.DuckDBPyConnection) -> None:
    assert migrate(conn) == []
    versions = conn.execute("SELECT version FROM schema_migrations").fetchall()
    assert versions == [(1,)]
