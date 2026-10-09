"""The look-ahead guard. If these fail, every backtest built on the store is suspect."""

from __future__ import annotations

from datetime import UTC, date, datetime, timedelta

import duckdb
import polars as pl
import pytest
from polars.testing import assert_frame_equal

from sds import asof
from sds.ingest.prices import Bar, upsert_bars
from sds.timeutil import market_close_utc

CIK = "0001045810"


def ts(s: str) -> datetime:
    return datetime.fromisoformat(s).replace(tzinfo=UTC)


def add_fact(
    conn: duckdb.DuckDBPyConnection, period_end: str, value: float, accession: str, known: str
) -> None:
    conn.execute(
        "INSERT INTO fundamentals (cik, concept, unit, period_start, period_end, value,"
        " accession_no, form, known_at) VALUES (?, 'us-gaap:Revenues', 'USD', NULL, ?, ?, ?,"
        " '10-Q', ?)",
        [CIK, period_end, value, accession, ts(known)],
    )


def add_security(conn: duckdb.DuckDBPyConnection, ticker: str = "NVDA") -> int:
    row = conn.execute(
        "INSERT INTO securities (cik, name) VALUES (?, ?) RETURNING security_id", [CIK, ticker]
    ).fetchone()
    assert row is not None
    sid = int(row[0])
    conn.execute("INSERT INTO ticker_history VALUES (?, ?, '2000-01-01', NULL)", [sid, ticker])
    return sid


def test_restatement_is_invisible_until_filed(conn: duckdb.DuckDBPyConnection) -> None:
    add_fact(conn, "2024-03-31", 100.0, "acc-original", "2024-05-01 20:00")
    add_fact(conn, "2024-03-31", 90.0, "acc-restated", "2024-08-01 20:00")

    before = asof.fundamentals_as_of(conn, ts("2024-06-01 00:00"))
    after = asof.fundamentals_as_of(conn, ts("2024-09-01 00:00"))

    assert before["value"].to_list() == [100.0]
    assert after["value"].to_list() == [90.0]


def test_fact_not_visible_before_filing(conn: duckdb.DuckDBPyConnection) -> None:
    add_fact(conn, "2024-03-31", 100.0, "acc-1", "2024-05-01 20:00")
    # Quarter ended Mar 31, but nobody outside knew the number until the May filing.
    assert asof.fundamentals_as_of(conn, ts("2024-04-15 00:00")).is_empty()


def test_naive_timestamp_refused(conn: duckdb.DuckDBPyConnection) -> None:
    with pytest.raises(ValueError, match="timezone-aware"):
        asof.fundamentals_as_of(conn, datetime(2024, 1, 1))  # noqa: DTZ001


def test_bar_known_only_after_close(conn: duckdb.DuckDBPyConnection) -> None:
    sid = add_security(conn)
    day = date(2024, 6, 11)
    upsert_bars(conn, sid, "test", [Bar(day, 1, 2, 1, 1.5, 1.5, 100)])
    close = market_close_utc(day)

    assert asof.prices_as_of(conn, close - timedelta(seconds=1)).is_empty()
    assert asof.prices_as_of(conn, close).height == 1


def add_filing(
    conn: duckdb.DuckDBPyConnection,
    acc: str,
    known: str,
    *,
    form: str = "4",
    superseded_by: str | None = None,
    match: str | None = None,
    owners: tuple[str, ...] = ("0000000001",),
) -> None:
    conn.execute(
        "INSERT INTO insider_filings (accession_no, issuer_cik, form, filing_date,"
        " period_of_report, known_at, superseded_by, amendment_match, raw_hash)"
        " VALUES (?, ?, ?, ?, '2024-03-01', ?, ?, ?, 'h')",
        [acc, CIK, form, ts(known).date(), ts(known), superseded_by, match],
    )
    for owner in owners:
        conn.execute(
            "INSERT INTO insider_owners VALUES (?, ?, NULL, false, true, false, false, 'CFO')",
            [acc, owner],
        )


def add_line(conn: duckdb.DuckDBPyConnection, acc: str, shares: float) -> None:
    conn.execute(
        "INSERT INTO insider_txns (accession_no, line_id, txn_code, txn_date, shares, price)"
        " VALUES (?, '1', 'P', '2024-03-01', ?, 10.0)",
        [acc, shares],
    )


def test_amendment_supersedes_only_once_public(conn: duckdb.DuckDBPyConnection) -> None:
    add_filing(conn, "orig", "2024-03-04 21:00", superseded_by="amend")
    add_filing(conn, "amend", "2024-03-20 21:00", form="4/A", match="one")
    add_line(conn, "orig", 1000)
    add_line(conn, "amend", 10000)

    mid = asof.insider_txns_as_of(conn, ts("2024-03-10 00:00"))
    late = asof.insider_txns_as_of(conn, ts("2024-04-01 00:00"))

    assert mid["shares"].to_list() == [1000]
    assert late["shares"].to_list() == [10000]


def test_amendment_chain_shows_only_the_latest_public_version(
    conn: duckdb.DuckDBPyConnection,
) -> None:
    add_filing(conn, "o", "2024-03-04 21:00", superseded_by="a1")
    add_filing(conn, "a1", "2024-03-10 21:00", form="4/A", match="one", superseded_by="a2")
    add_filing(conn, "a2", "2024-03-20 21:00", form="4/A", match="one")
    for acc, shares in (("o", 1), ("a1", 2), ("a2", 3)):
        add_line(conn, acc, shares)

    seen = [
        asof.insider_txns_as_of(conn, ts(t))["shares"].to_list()
        for t in ("2024-03-05 00:00", "2024-03-11 00:00", "2024-03-21 00:00")
    ]
    assert seen == [[1.0], [2.0], [3.0]]


def test_unlinked_amendments_are_excluded_and_original_kept(
    conn: duckdb.DuckDBPyConnection,
) -> None:
    add_filing(conn, "orig", "2024-03-04 21:00")
    add_filing(conn, "none", "2024-03-20 21:00", form="4/A", match="none")
    add_filing(conn, "many", "2024-03-20 21:00", form="4/A", match="many")
    add_filing(conn, "unlinked", "2024-03-20 21:00", form="4/A")  # link pass not run yet
    for acc in ("orig", "none", "many", "unlinked"):
        add_line(conn, acc, 5)

    visible = asof.insider_txns_as_of(conn, ts("2024-04-01 00:00"))
    assert visible["accession_no"].to_list() == ["orig"]


def test_joint_filing_shares_counted_once(conn: duckdb.DuckDBPyConnection) -> None:
    add_filing(conn, "joint", "2024-03-04 21:00", owners=("0000000011", "0000000012", "0000000013"))
    add_line(conn, "joint", 100)
    as_of = ts("2024-04-01 00:00")

    assert asof.insider_txns_as_of(conn, as_of)["shares"].sum() == 100
    assert asof.insider_owners_as_of(conn, as_of).height == 3


def test_curated_universe_excluded_from_strict_backtest(conn: duckdb.DuckDBPyConnection) -> None:
    sid = add_security(conn)
    conn.execute(
        "INSERT INTO universe_membership VALUES (?, 'ai_compute', 'accelerators',"
        " '2015-01-01', NULL, 'curated', 'seed', ?)",
        [sid, ts("2026-10-05 12:00")],
    )
    on, as_of = date(2020, 1, 2), ts("2020-01-02 21:00")

    assert asof.universe_as_of(conn, on, as_of).is_empty()
    assert asof.universe_as_of(conn, on, as_of, allow_hindsight=True).height == 1


def test_history_reproduces_after_new_data_arrives(conn: duckdb.DuckDBPyConnection) -> None:
    """The core contract: a past view never changes when later data lands."""
    sid = add_security(conn)
    add_fact(conn, "2024-03-31", 100.0, "acc-1", "2024-05-01 20:00")
    upsert_bars(conn, sid, "test", [Bar(date(2024, 6, 10), 1, 2, 1, 1.5, 1.5, 10)])
    as_of = ts("2024-06-15 00:00")

    def snapshot() -> tuple[pl.DataFrame, pl.DataFrame]:
        return (
            asof.fundamentals_as_of(conn, as_of).drop("known_at"),
            asof.prices_as_of(conn, as_of),
        )

    first = snapshot()
    # Later knowledge: a restatement, a new quarter, and new bars.
    add_fact(conn, "2024-03-31", 95.0, "acc-2", "2024-08-01 20:00")
    add_fact(conn, "2024-06-30", 120.0, "acc-3", "2024-08-01 20:00")
    upsert_bars(conn, sid, "test", [Bar(date(2024, 6, 17), 1, 2, 1, 1.6, 1.6, 10)])
    second = snapshot()

    assert_frame_equal(first[0], second[0])
    assert_frame_equal(first[1], second[1])
