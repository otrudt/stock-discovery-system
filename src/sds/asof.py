"""Point-in-time reads.

Every function here answers "what could the system have known at `as_of`?". Signal
builders and backtests must read through this module, never from raw tables, so the
look-ahead rule lives in exactly one place.

`as_of` is always a timezone-aware timestamp (knowledge time). Passing a bare date is
refused: "as of Oct 5" is ambiguous between the open and the close, and that ambiguity
is precisely how look-ahead bugs get in.
"""

from __future__ import annotations

from datetime import date, datetime

import duckdb
import polars as pl

from sds.timeutil import require_aware


def fundamentals_as_of(
    conn: duckdb.DuckDBPyConnection,
    as_of: datetime,
    ciks: list[str] | None = None,
    concepts: list[str] | None = None,
) -> pl.DataFrame:
    """Latest filed version of each fact that was public at `as_of`.

    A later restatement of the same period is invisible until its own filing time.
    """
    as_of = require_aware(as_of)
    sql = """
        SELECT cik, concept, unit, period_start, period_end, value, accession_no, form, known_at
        FROM fundamentals
        WHERE known_at <= $as_of
          AND ($ciks IS NULL OR list_contains($ciks, cik))
          AND ($concepts IS NULL OR list_contains($concepts, concept))
        QUALIFY row_number() OVER (
            PARTITION BY cik, concept, unit, period_start, period_end
            ORDER BY known_at DESC, accession_no DESC
        ) = 1
        ORDER BY cik, concept, period_end
    """
    return conn.execute(sql, {"as_of": as_of, "ciks": ciks, "concepts": concepts}).pl()


def prices_as_of(
    conn: duckdb.DuckDBPyConnection,
    as_of: datetime,
    security_ids: list[int] | None = None,
    start: date | None = None,
    source: str | None = None,
) -> pl.DataFrame:
    """Daily bars whose close had printed by `as_of`."""
    as_of = require_aware(as_of)
    sql = """
        SELECT security_id, date, open, high, low, close, adj_close, volume, source
        FROM prices_daily
        WHERE known_at <= $as_of
          AND ($ids IS NULL OR list_contains($ids, security_id))
          AND ($start IS NULL OR date >= $start)
          AND ($source IS NULL OR source = $source)
        ORDER BY security_id, date
    """
    params = {"as_of": as_of, "ids": security_ids, "start": start, "source": source}
    return conn.execute(sql, params).pl()


def insider_txns_as_of(
    conn: duckdb.DuckDBPyConnection,
    as_of: datetime,
    issuer_ciks: list[str] | None = None,
) -> pl.DataFrame:
    """Form 4 lines public at `as_of`, with an amendment replacing its original only
    once the amendment itself was public."""
    as_of = require_aware(as_of)
    sql = """
        SELECT t.*
        FROM insider_txns t
        LEFT JOIN insider_txns amend
          ON amend.accession_no = t.superseded_by AND amend.known_at <= $as_of
        WHERE t.known_at <= $as_of
          AND amend.accession_no IS NULL
          AND ($ciks IS NULL OR list_contains($ciks, t.issuer_cik))
        ORDER BY t.issuer_cik, t.event_date
    """
    return conn.execute(sql, {"as_of": as_of, "ciks": issuer_ciks}).pl()


def universe_as_of(
    conn: duckdb.DuckDBPyConnection,
    on: date,
    as_of: datetime,
    *,
    allow_hindsight: bool = False,
) -> pl.DataFrame:
    """Members of the universe on business date `on`, as recorded by `as_of`.

    Strict by default: a membership decision recorded after `as_of` is excluded, so a
    curated list written today cannot define a backtest's past universe (survivorship
    bias). `allow_hindsight=True` relaxes that for exploration only; results computed
    that way must never be reported as a backtest.
    """
    as_of = require_aware(as_of)
    sql = """
        SELECT m.security_id, s.cik, s.name, m.family, m.layer, m.basis,
               m.valid_from, m.valid_to, m.recorded_at
        FROM universe_membership m
        JOIN securities s USING (security_id)
        WHERE m.valid_from <= $on
          AND (m.valid_to IS NULL OR m.valid_to > $on)
          AND ($hindsight OR m.recorded_at <= $as_of)
        ORDER BY m.family, m.layer, s.name
    """
    params = {"on": on, "as_of": as_of, "hindsight": allow_hindsight}
    return conn.execute(sql, params).pl()


def security_for_ticker(conn: duckdb.DuckDBPyConnection, ticker: str, on: date) -> int | None:
    """Resolve a ticker as it was used on `on`. Tickers get reused, so the date matters."""
    row = conn.execute(
        """
        SELECT security_id FROM ticker_history
        WHERE upper(ticker) = upper($t)
          AND valid_from <= $on AND (valid_to IS NULL OR valid_to > $on)
        """,
        {"t": ticker, "on": on},
    ).fetchone()
    return None if row is None else int(row[0])
