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


# Which filings are public at $as_of. Both insider reads use this one rule:
# - a filing appears once filed;
# - an amendment appears only if the link pass tied it to exactly one original
#   (decided 2026-10-05, before any data: no double counting);
# - a filing disappears once the next amendment in its chain is itself public.
_VISIBLE_INSIDER_FILINGS = """
    SELECT f.*
    FROM insider_filings f
    LEFT JOIN insider_filings nxt
      ON nxt.accession_no = f.superseded_by AND nxt.known_at <= $as_of
    WHERE f.known_at <= $as_of
      AND (f.form <> '4/A' OR f.amendment_match = 'one')
      AND nxt.accession_no IS NULL
      AND ($ciks IS NULL OR list_contains($ciks, f.issuer_cik))
"""


def insider_txns_as_of(
    conn: duckdb.DuckDBPyConnection,
    as_of: datetime,
    issuer_ciks: list[str] | None = None,
) -> pl.DataFrame:
    """Form 4 lines public at `as_of`, one row per line, with its filing's fields.

    An amendment replaces its original only once the amendment itself was public.
    """
    as_of = require_aware(as_of)
    sql = f"""
        WITH visible AS ({_VISIBLE_INSIDER_FILINGS})
        SELECT t.accession_no, t.line_id, f.issuer_cik, f.form, f.filing_date,
               f.is_10b5_1, f.known_at, t.txn_code, t.txn_date, t.shares, t.price,
               t.acquired_disposed, t.shares_owned_after, t.direct_indirect
        FROM insider_txns t JOIN visible f USING (accession_no)
        ORDER BY f.issuer_cik, t.txn_date, t.accession_no, t.line_id
    """
    return conn.execute(sql, {"as_of": as_of, "ciks": issuer_ciks}).pl()


def insider_owners_as_of(
    conn: duckdb.DuckDBPyConnection,
    as_of: datetime,
    issuer_ciks: list[str] | None = None,
) -> pl.DataFrame:
    """Reporting owners of the filings public at `as_of`, one row per (filing, owner).

    Roles are as reported in each filing. Join to `insider_txns_as_of` on accession_no
    only after aggregating lines per filing, or a joint filing's shares count once per
    owner.
    """
    as_of = require_aware(as_of)
    sql = f"""
        WITH visible AS ({_VISIBLE_INSIDER_FILINGS})
        SELECT o.accession_no, f.issuer_cik, f.known_at, o.owner_cik, o.owner_name,
               o.is_director, o.is_officer, o.is_ten_pct_owner, o.is_other, o.officer_title
        FROM insider_owners o JOIN visible f USING (accession_no)
        ORDER BY f.issuer_cik, o.accession_no, o.owner_cik
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
