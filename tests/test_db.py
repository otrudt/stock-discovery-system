from __future__ import annotations

import duckdb
import polars as pl

from sds.db import insert_frame, migrate


def test_migrations_are_idempotent(conn: duckdb.DuckDBPyConnection) -> None:
    assert migrate(conn) == []
    versions = conn.execute("SELECT version FROM schema_migrations ORDER BY version").fetchall()
    assert versions == [(1,), (2,)]


def test_insider_tables_hold_a_joint_filing(conn: duckdb.DuckDBPyConnection) -> None:
    conn.execute(
        "INSERT INTO insider_filings (accession_no, issuer_cik, form, filing_date,"
        " period_of_report, known_at, raw_hash) VALUES ('a1', '0000000001', '4',"
        " '2024-01-02', '2024-01-01', '2024-01-03 03:00:00+00', 'h')"
    )
    for cik in ("0000000011", "0000000012"):
        conn.execute(
            "INSERT INTO insider_owners VALUES ('a1', ?, NULL, false, false, true, false, NULL)",
            [cik],
        )
    conn.execute(
        "INSERT INTO insider_txns VALUES ('a1', '1', 'P', '2024-01-01', 100, 10.0, 'A', 1100, 'I')"
    )
    assert conn.execute("SELECT count(*) FROM insider_owners").fetchone() == (2,)
    assert conn.execute("SELECT sum(shares) FROM insider_txns").fetchone() == (100.0,)


def test_insert_frame_returns_rows_actually_written(conn: duckdb.DuckDBPyConnection) -> None:
    conn.execute("CREATE TABLE t (k INTEGER PRIMARY KEY, v INTEGER)")
    frame = pl.DataFrame({"k": [1, 2], "v": [1, 1]})
    assert insert_frame(conn, "t", frame, "ON CONFLICT DO NOTHING") == 2
    assert insert_frame(conn, "t", frame, "ON CONFLICT DO NOTHING") == 0
