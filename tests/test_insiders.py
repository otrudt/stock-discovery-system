from __future__ import annotations

from datetime import UTC, date, datetime

import duckdb
from form345_fixture import Row, build_zip, line, owner, submission
from polars.testing import assert_frame_equal

from sds import asof
from sds.ingest.form345 import parse_zip, write_frames
from sds.insiders import link_amendments


def load(conn: duckdb.DuckDBPyConnection, raw: bytes) -> None:
    frames = parse_zip(raw, forms=["4", "4/A"], txn_codes=["P", "S"], max_date=date(2026, 12, 31))
    write_frames(conn, frames, "h")


def with_owner(subs: list[Row], cik: str = "11") -> bytes:
    return build_zip(subs, [owner(str(s["ACCESSION_NUMBER"]), cik) for s in subs])


def matches(conn: duckdb.DuckDBPyConnection) -> dict[str, str]:
    rows = conn.execute(
        "SELECT accession_no, amendment_match FROM insider_filings WHERE form = '4/A'"
    ).fetchall()
    return dict(rows)


def links(conn: duckdb.DuckDBPyConnection) -> dict[str, str]:
    rows = conn.execute(
        "SELECT accession_no, superseded_by FROM insider_filings WHERE superseded_by IS NOT NULL"
    ).fetchall()
    return dict(rows)


def amendment(acc: str, *, orig: str, filed: str = "20-MAR-2024", **kw: str) -> Row:
    return submission(acc, form="4/A", filed=filed, orig=orig, **kw)


def test_link_outcomes(conn: duckdb.DuckDBPyConnection) -> None:
    load(
        conn,
        with_owner(
            [
                submission("o1", filed="05-MAR-2024"),
                amendment("one", orig="05-MAR-2024"),
                amendment("none", orig="06-MAR-2024"),
                submission("o2", issuer="2", filed="05-MAR-2024"),
                submission("o3", issuer="2", filed="05-MAR-2024"),
                amendment("many", orig="05-MAR-2024", issuer="2"),
            ]
        ),
    )
    assert link_amendments(conn) == {"one": 1, "none": 1, "many": 1}
    assert matches(conn) == {"one": "one", "none": "none", "many": "many"}
    assert links(conn) == {"o1": "one"}


def test_period_breaks_a_tie(conn: duckdb.DuckDBPyConnection) -> None:
    load(
        conn,
        with_owner(
            [
                submission("o1", filed="05-MAR-2024", period="01-MAR-2024"),
                submission("o2", filed="05-MAR-2024", period="02-MAR-2024"),
                amendment("a", orig="05-MAR-2024", period="02-MAR-2024"),
            ]
        ),
    )
    link_amendments(conn)
    assert links(conn) == {"o2": "a"}


def test_owner_set_must_match(conn: duckdb.DuckDBPyConnection) -> None:
    subs = [submission("o1", filed="05-MAR-2024"), amendment("a", orig="05-MAR-2024")]
    load(conn, build_zip(subs, [owner("o1", "11"), owner("o1", "12"), owner("a", "11")]))
    link_amendments(conn)
    assert matches(conn) == {"a": "none"}


def test_repeat_amendments_chain_in_knowledge_order(conn: duckdb.DuckDBPyConnection) -> None:
    load(
        conn,
        with_owner(
            [
                submission("o", filed="05-MAR-2024"),
                amendment("a2", orig="05-MAR-2024", filed="20-MAR-2024"),
                amendment("a1", orig="05-MAR-2024", filed="10-MAR-2024"),
            ]
        ),
    )
    link_amendments(conn)
    assert links(conn) == {"o": "a1", "a1": "a2"}
    link_amendments(conn)  # a full recompute: running it again changes nothing
    assert links(conn) == {"o": "a1", "a1": "a2"}


def test_original_loaded_after_amendment_still_links(conn: duckdb.DuckDBPyConnection) -> None:
    # Backfill order is not knowledge order: a 4/A can arrive before its original.
    load(conn, with_owner([amendment("a", orig="28-MAR-2024", filed="02-APR-2024")]))
    link_amendments(conn)
    assert matches(conn) == {"a": "none"}

    load(conn, with_owner([submission("o", filed="28-MAR-2024")]))
    link_amendments(conn)
    assert links(conn) == {"o": "a"}


def test_past_view_unchanged_by_linking_and_later_quarters(
    conn: duckdb.DuckDBPyConnection,
) -> None:
    """The core contract: a past view never changes when later data or links land."""
    load(
        conn,
        build_zip(
            [submission("o", filed="05-MAR-2024")],
            [owner("o", "11")],
            [line("o", "1", shares="100")],
        ),
    )
    past = datetime(2024, 3, 10, tzinfo=UTC)
    txns_before = asof.insider_txns_as_of(conn, past)
    owners_before = asof.insider_owners_as_of(conn, past)

    load(
        conn,
        build_zip(
            [
                amendment("a", orig="05-MAR-2024", filed="02-APR-2024"),
                submission("n", filed="03-APR-2024"),
            ],
            [owner("a", "11"), owner("n", "11")],
            [line("a", "1", shares="150"), line("n", "1")],
        ),
    )
    link_amendments(conn)

    assert links(conn) == {"o": "a"}  # the link really landed on the old row
    assert_frame_equal(txns_before, asof.insider_txns_as_of(conn, past))
    assert_frame_equal(owners_before, asof.insider_owners_as_of(conn, past))
    later = asof.insider_txns_as_of(conn, datetime(2024, 4, 10, tzinfo=UTC))
    assert later["shares"].sum() == 250  # the amendment (150) replaces o; n adds 100


def test_chain_order_follows_known_at_not_accession(conn: duckdb.DuckDBPyConnection) -> None:
    load(
        conn,
        with_owner(
            [
                submission("o", filed="05-MAR-2024"),
                amendment("a1", orig="05-MAR-2024", filed="20-MAR-2024"),
                amendment("a2", orig="05-MAR-2024", filed="10-MAR-2024"),
            ]
        ),
    )
    link_amendments(conn)
    assert links(conn) == {"o": "a2", "a2": "a1"}


def test_an_amendment_is_never_an_original(conn: duckdb.DuckDBPyConnection) -> None:
    load(
        conn,
        with_owner(
            [
                submission("o", filed="05-MAR-2024"),
                amendment("a", orig="05-MAR-2024"),
                amendment("b", orig="01-MAR-2024", filed="05-MAR-2024"),
            ]
        ),
    )
    link_amendments(conn)
    assert matches(conn) == {"a": "one", "b": "none"}


def test_a_stale_link_is_cleared_when_a_rival_original_arrives(
    conn: duckdb.DuckDBPyConnection,
) -> None:
    load(
        conn,
        with_owner([submission("o1", filed="05-MAR-2024"), amendment("a", orig="05-MAR-2024")]),
    )
    link_amendments(conn)
    assert links(conn) == {"o1": "a"}

    load(conn, with_owner([submission("o2", filed="05-MAR-2024")]))
    link_amendments(conn)
    assert matches(conn) == {"a": "many"}
    assert links(conn) == {}
