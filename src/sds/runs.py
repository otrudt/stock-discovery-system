"""Job run log and quarantine. Every job runs inside `run_job`."""

from __future__ import annotations

import uuid
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass

import duckdb

from sds.timeutil import utc_now


class QualityGateError(RuntimeError):
    """Too many rows failed validation; downstream tables are left untouched."""


@dataclass
class RunStats:
    run_id: str
    job: str
    rows_in: int = 0
    rows_written: int = 0
    rows_quarantined: int = 0


@contextmanager
def run_job(conn: duckdb.DuckDBPyConnection, job: str) -> Iterator[RunStats]:
    stats = RunStats(run_id=uuid.uuid4().hex, job=job)
    conn.execute(
        "INSERT INTO runs (run_id, job, started_at, status) VALUES (?, ?, ?, 'running')",
        [stats.run_id, job, utc_now()],
    )
    try:
        yield stats
    except BaseException as exc:
        _finish(conn, stats, "failed", f"{type(exc).__name__}: {exc}"[:2000])
        raise
    _finish(conn, stats, "ok", None)


def _finish(
    conn: duckdb.DuckDBPyConnection, stats: RunStats, status: str, error: str | None
) -> None:
    conn.execute(
        """
        UPDATE runs SET finished_at = ?, status = ?, rows_in = ?, rows_written = ?,
                        rows_quarantined = ?, error = ?
        WHERE run_id = ?
        """,
        [
            utc_now(),
            status,
            stats.rows_in,
            stats.rows_written,
            stats.rows_quarantined,
            error,
            stats.run_id,
        ],
    )


def quarantine(
    conn: duckdb.DuckDBPyConnection,
    stats: RunStats,
    *,
    source: str,
    reason: str,
    payload: str | None = None,
    raw_hash: str | None = None,
) -> None:
    conn.execute(
        "INSERT INTO quarantine (run_id, source, raw_hash, reason, payload) VALUES (?, ?, ?, ?, ?)",
        [stats.run_id, source, raw_hash, reason, payload],
    )
    stats.rows_quarantined += 1


def enforce_quality_gate(stats: RunStats, max_rate: float = 0.01) -> None:
    if stats.rows_in and stats.rows_quarantined / stats.rows_in > max_rate:
        raise QualityGateError(
            f"{stats.job}: {stats.rows_quarantined}/{stats.rows_in} rows quarantined "
            f"(limit {max_rate:.0%}); see the quarantine table for run {stats.run_id}"
        )
