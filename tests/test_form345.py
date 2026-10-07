from __future__ import annotations

import dataclasses
from datetime import UTC, date, datetime, timedelta
from pathlib import Path

import duckdb
import httpx
import pytest
from form345_fixture import build_zip, line, owner, submission

from sds.ingest.config import Form345Config
from sds.ingest.form345 import (
    NONDERIV_TRANS,
    InsiderFrames,
    SchemaDriftError,
    discover,
    load_insiders,
    load_quarter,
    parse_zip,
    write_frames,
)
from sds.ingest.http import PoliteClient
from sds.ingest.raw_store import RawStore
from sds.runs import QualityGateError

FORMS = ["4", "4/A"]
CODES = ["P", "S"]
MAX_DATE = date(2026, 12, 31)  # fixed, so tests do not depend on the clock


def parse(raw: bytes) -> InsiderFrames:
    return parse_zip(raw, forms=FORMS, txn_codes=CODES, max_date=MAX_DATE)


def test_parse_types_and_pads_ciks() -> None:
    frames = parse(build_zip([submission("a1", aff="1")], [owner("a1", "11")], [line("a1", "7")]))

    assert frames.filings.row(0, named=True) == {
        "accession_no": "a1",
        "issuer_cik": "0000000001",
        "form": "4",
        "filing_date": date(2024, 1, 2),
        "period_of_report": date(2024, 1, 1),
        "date_of_orig_sub": None,
        "is_10b5_1": True,
        "known_at": datetime(2024, 1, 3, 3, 0, tzinfo=UTC),  # 22:00 EST
    }
    assert frames.owners["owner_cik"].to_list() == ["0000000011"]
    assert frames.txns.row(0, named=True)["shares"] == 100.0
    assert frames.rejected == []
    assert frames.rows_in == 3


def test_known_at_follows_new_york_daylight_time() -> None:
    frames = parse(build_zip([submission("a1", filed="01-JUL-2024")]))
    assert frames.filings["known_at"].to_list() == [datetime(2024, 7, 2, 2, 0, tzinfo=UTC)]


def test_10b5_1_encodings() -> None:
    subs = [submission(f"a{i}", aff=v) for i, v in enumerate(["true", "1", "false", "0", None])]
    frames = parse(build_zip(subs))
    assert frames.filings.sort("accession_no")["is_10b5_1"].to_list() == [
        True,
        True,
        False,
        False,
        None,
    ]


def test_missing_optional_10b5_1_column_gives_null() -> None:
    frames = parse(build_zip([submission("a1", aff="1")], drop=frozenset({"AFF10B5ONE"})))
    assert frames.filings["is_10b5_1"].to_list() == [None]


def test_missing_required_column_fails_by_name() -> None:
    raw = build_zip([submission("a1")], lines=[line("a1", "1")], drop=frozenset({"TRANS_SHARES"}))
    with pytest.raises(SchemaDriftError, match="TRANS_SHARES"):
        parse(raw)


def test_roles_split_and_empty_relationship_has_no_roles() -> None:
    owners = [
        owner("a1", "11", relationship="Director,Officer,TenPercentOwner"),
        owner("a1", "12", relationship=None),
        owner("a1", "13", relationship="Director"),
        owner("a1", "14", relationship="Other"),
    ]
    frames = parse(build_zip([submission("a1")], owners))
    roles = frames.owners.sort("owner_cik").select(
        "is_director", "is_officer", "is_ten_pct_owner", "is_other"
    )
    assert roles.rows() == [
        (True, True, True, False),
        (False, False, False, False),
        (True, False, False, False),
        (False, False, False, True),
    ]


def test_out_of_scope_forms_and_codes_are_skipped_not_rejected() -> None:
    raw = build_zip(
        [submission("f4"), submission("f5", form="5")],
        [owner("f4", "11"), owner("f5", "11")],
        [line("f4", "1"), line("f4", "2", code="A"), line("f5", "3")],
    )
    frames = parse(raw)
    assert frames.filings["accession_no"].to_list() == ["f4"]
    assert frames.txns["line_id"].to_list() == ["1"]
    assert frames.rejected == []
    assert frames.rows_in == 3


def test_bad_rows_are_rejected_with_reasons() -> None:
    raw = build_zip(
        [submission("ok"), submission("bad", filed="31-FOO-2024"), submission("flag", aff="maybe")],
        [owner("ghost", "11")],
        [
            line("ok", "1"),
            line("ok", "2", shares="lots"),
            line("ok", "3", price="n/a"),
            line("bad", "9"),
        ],
    )
    frames = parse(raw)

    assert sorted((r.table, r.accession_no, r.reason) for r in frames.rejected) == [
        ("filings", "bad", "bad filing_date"),
        ("filings", "flag", "bad is_10b5_1"),
        ("owners", "ghost", "no filing"),
        ("txns", "bad", "filing rejected"),
        ("txns", "ok", "bad price"),
        ("txns", "ok", "bad shares"),
    ]
    assert frames.filings["accession_no"].to_list() == ["ok"]
    assert frames.txns["line_id"].to_list() == ["1"]
    assert frames.rows_in == 8
    bad_shares = next(r for r in frames.rejected if r.reason == "bad shares")
    assert '"shares":"lots"' in bad_shares.payload  # quarantine keeps the bad value


@pytest.mark.parametrize(
    ("column", "kwargs"),
    [
        ("filing_date", {"filed": "02-JAN-24"}),  # 2-digit year parses to 0024
        ("filing_date", {"filed": "02-JAN-2999"}),
        ("period_of_report", {"period": "01-JAN-24"}),
        ("date_of_orig_sub", {"form": "4/A", "orig": "01-JAN-24"}),
    ],
)
def test_out_of_bound_filing_dates_are_rejected(column: str, kwargs: dict[str, str]) -> None:
    frames = parse(build_zip([submission("a1", **kwargs)], [owner("a1", "11")]))
    assert [(r.table, r.reason) for r in frames.rejected] == [
        ("filings", f"bad {column}"),
        ("owners", "filing rejected"),
    ]
    assert frames.filings.is_empty()


@pytest.mark.parametrize("day", ["01-JAN-24", "31-DEC-2002", "01-JAN-2999"])
def test_out_of_bound_txn_date_is_rejected(day: str) -> None:
    frames = parse(build_zip([submission("a1")], [owner("a1", "11")], [line("a1", "1", date=day)]))
    assert [(r.table, r.reason) for r in frames.rejected] == [("txns", "bad txn_date")]
    assert frames.txns.is_empty()


def test_date_bounds_are_inclusive() -> None:
    frames = parse(
        build_zip(
            [submission("a1", filed="31-DEC-2026", period="01-JAN-2003")],
            [owner("a1", "11")],
            [line("a1", "1", date="01-JAN-2003")],
        )
    )
    assert frames.rejected == []
    assert frames.filings["filing_date"].to_list() == [date(2026, 12, 31)]


@pytest.mark.parametrize("value", ["NaN", "nan", "inf", "-inf", "1e400"])
@pytest.mark.parametrize("column", ["shares", "price", "shares_owned_after"])
def test_non_finite_numbers_are_rejected(column: str, value: str) -> None:
    # DuckDB evaluates NaN > 0 as TRUE, so a later "shares > 0" filter would not stop it.
    sec_name = {ours: sec for sec, ours in NONDERIV_TRANS.columns.items()}[column]
    row = {**line("a1", "1"), sec_name: value}
    frames = parse(build_zip([submission("a1")], [owner("a1", "11")], [row]))
    assert [(r.table, r.reason) for r in frames.rejected] == [("txns", f"bad {column}")]
    assert frames.txns.is_empty()


@pytest.mark.parametrize("cik", ["ab c", "12-34", "12345678901"])
def test_non_digit_issuer_cik_is_rejected(cik: str) -> None:
    frames = parse(build_zip([submission("a1", issuer=cik)], [owner("a1", "11")]))
    assert [(r.table, r.reason) for r in frames.rejected] == [
        ("filings", "bad issuer_cik"),
        ("owners", "filing rejected"),
    ]


@pytest.mark.parametrize("cik", ["ab c", "12-34", "12345678901"])
def test_non_digit_owner_cik_is_rejected(cik: str) -> None:
    frames = parse(build_zip([submission("a1")], [owner("a1", cik), owner("a1", "0012")]))
    assert [(r.table, r.reason) for r in frames.rejected] == [("owners", "bad owner_cik")]
    assert frames.owners["owner_cik"].to_list() == ["0000000012"]


def test_missing_ciks_keep_their_reasons() -> None:
    raw = build_zip([submission("a1"), submission("a2", issuer="")], [owner("a1", "")])
    reasons = sorted((r.table, r.reason) for r in parse(raw).rejected)
    assert reasons == [("filings", "no issuer_cik"), ("owners", "no owner_cik")]


def test_amendment_naming_an_original_after_its_own_filing_is_rejected() -> None:
    raw = build_zip(
        [
            submission("late", form="4/A", filed="05-MAR-2024", orig="20-MAR-2024"),
            submission("same", form="4/A", filed="05-MAR-2024", orig="05-MAR-2024"),
        ]
    )
    frames = parse(raw)
    assert [(r.accession_no, r.reason) for r in frames.rejected] == [
        ("late", "bad date_of_orig_sub")
    ]
    assert frames.filings["accession_no"].to_list() == ["same"]


def test_duplicate_keys_are_rejected_not_dropped() -> None:
    raw = build_zip(
        [submission("a1")],
        [owner("a1", "11"), owner("a1", "11")],
        [line("a1", "1"), line("a1", "1")],
    )
    frames = parse(raw)
    assert sorted((r.table, r.reason) for r in frames.rejected) == [
        ("owners", "duplicate key"),
        ("owners", "duplicate key"),
        ("txns", "duplicate key"),
        ("txns", "duplicate key"),
    ]
    assert frames.owners.is_empty()
    assert frames.txns.is_empty()


GOOD = build_zip([submission("a1")], [owner("a1", "11")], [line("a1", "1")])


@pytest.fixture
def cfg() -> Form345Config:
    return Form345Config(start_quarter="2012q1", forms=FORMS, txn_codes=CODES)


def test_load_quarter_writes_and_reload_writes_nothing(
    conn: duckdb.DuckDBPyConnection, cfg: Form345Config
) -> None:
    first = load_quarter(conn, "2024q1", cfg, lambda: (GOOD, "h1"))
    second = load_quarter(conn, "2024q1", cfg, lambda: (GOOD, "h1"))

    assert (first.rows_written, second.rows_written) == (3, 0)
    assert conn.execute("SELECT raw_hash FROM insider_filings").fetchall() == [("h1",)]
    assert conn.execute("SELECT job, status FROM runs").fetchall() == [
        ("form345.2024q1", "ok"),
        ("form345.2024q1", "ok"),
    ]


def test_gate_blocks_every_write_for_the_quarter(
    conn: duckdb.DuckDBPyConnection, cfg: Form345Config
) -> None:
    raw = build_zip(
        [submission("a1"), submission("bad", filed="nope")],
        [owner("a1", "11")],
        [line("a1", "1")],
    )
    with pytest.raises(QualityGateError):
        load_quarter(conn, "2024q1", cfg, lambda: (raw, "h1"))

    for table in ("insider_filings", "insider_owners", "insider_txns"):
        assert conn.execute(f"SELECT count(*) FROM {table}").fetchone() == (0,)
    assert conn.execute("SELECT reason, raw_hash FROM quarantine").fetchall() == [
        ("filings: bad filing_date", "h1")
    ]
    assert conn.execute("SELECT status FROM runs").fetchall() == [("failed",)]


def test_load_quarter_rejects_a_filing_dated_past_next_year(
    conn: duckdb.DuckDBPyConnection, cfg: Form345Config
) -> None:
    far = (datetime.now(UTC).date() + timedelta(days=800)).strftime("%d-%b-%Y")
    raw = build_zip([submission(f"a{i}") for i in range(200)] + [submission("x", filed=far)])
    load_quarter(conn, "2024q1", cfg, lambda: (raw, "h1"))
    assert conn.execute("SELECT reason FROM quarantine").fetchall() == [
        ("filings: bad filing_date",)
    ]


def test_schema_drift_fails_the_run_by_name(
    conn: duckdb.DuckDBPyConnection, cfg: Form345Config
) -> None:
    raw = build_zip([submission("a1")], drop=frozenset({"FILING_DATE"}))
    with pytest.raises(SchemaDriftError, match="FILING_DATE"):
        load_quarter(conn, "2024q1", cfg, lambda: (raw, "h1"))
    status, error = conn.execute("SELECT status, error FROM runs").fetchone() or ("", "")
    assert status == "failed"
    assert "FILING_DATE" in error


def test_fetch_failure_is_recorded_as_a_failed_run(
    conn: duckdb.DuckDBPyConnection, cfg: Form345Config
) -> None:
    def broken() -> tuple[bytes, str]:
        raise OSError("connection reset")

    with pytest.raises(OSError):
        load_quarter(conn, "2024q1", cfg, broken)
    assert conn.execute("SELECT status FROM runs").fetchall() == [("failed",)]


def test_failed_write_rolls_back_earlier_tables(conn: duckdb.DuckDBPyConnection) -> None:
    frames = parse(GOOD)
    broken = dataclasses.replace(frames, txns=frames.txns.drop("shares"))  # NOT NULL column gone
    with pytest.raises(duckdb.Error):
        write_frames(conn, broken, "h1")

    assert conn.execute("SELECT count(*) FROM insider_filings").fetchone() == (0,)
    assert write_frames(conn, frames, "h1") == 3  # the connection is usable again


SETS = "/data/insider-transactions-data-sets"
PAGE = (
    f'<a href="/files/structureddata{SETS}/2011q4_form345.zip">x</a>'
    f'<a href="/files/structureddata{SETS}/2012q1_form345.zip">x</a>'
    f'<a href="/files/datastandardsinnovation{SETS}/2012q2_form345.zip">x</a>'
).encode()


def sec(zips: dict[str, bytes], seen: list[str]) -> PoliteClient:
    def handler(request: httpx.Request) -> httpx.Response:
        path = request.url.path
        seen.append(path)
        if path.endswith("insider-transactions-data-sets"):
            return httpx.Response(200, content=PAGE)
        for quarter, raw in zips.items():
            if path.endswith(f"/{quarter}_form345.zip"):
                return httpx.Response(200, content=raw)
        return httpx.Response(404)

    return PoliteClient(
        "t t@example.com", 1000, transport=httpx.MockTransport(handler), sleep=lambda _: None
    )


def zip_paths(seen: list[str]) -> list[str]:
    return [p for p in seen if p.endswith(".zip")]


def test_discover_reads_both_paths_and_dedupes() -> None:
    found = discover(PAGE + PAGE)
    assert sorted(found) == ["2011q4", "2012q1", "2012q2"]
    assert found["2012q2"] == (
        f"https://www.sec.gov/files/datastandardsinnovation{SETS}/2012q2_form345.zip"
    )


def test_discover_first_link_per_quarter_wins() -> None:
    other = PAGE.replace(b"structureddata", b"elsewhere")
    assert discover(PAGE + other)["2012q1"].endswith(f"/structureddata{SETS}/2012q1_form345.zip")


def test_discover_without_links_stops() -> None:
    with pytest.raises(ValueError, match=r"no \*_form345\.zip links"):
        discover(b"<html>moved</html>")


def test_load_respects_start_quarter_and_skips_loaded(
    conn: duckdb.DuckDBPyConnection, tmp_path: Path, cfg: Form345Config
) -> None:
    seen: list[str] = []
    client = sec({"2012q1": GOOD, "2012q2": build_zip([submission("b1")])}, seen)
    store = RawStore(tmp_path, conn)

    first = load_insiders(conn, store, client, cfg)
    assert (first.loaded, first.failed) == (["2012q1", "2012q2"], {})
    assert first.amendments == {}

    seen.clear()
    second = load_insiders(conn, store, client, cfg)
    assert (second.loaded, second.skipped) == ([], ["2012q1", "2012q2"])
    assert zip_paths(seen) == []


def test_failed_quarter_is_reparsed_from_store_without_fetch(
    conn: duckdb.DuckDBPyConnection, tmp_path: Path, cfg: Form345Config
) -> None:
    bad = build_zip([submission("x", filed="nope")])  # every row rejected: the gate fails
    seen: list[str] = []
    client = sec({"2012q1": bad, "2012q2": GOOD}, seen)
    store = RawStore(tmp_path, conn)

    report = load_insiders(conn, store, client, cfg)
    assert (list(report.failed), report.loaded) == (["2012q1"], ["2012q2"])

    seen.clear()
    again = load_insiders(conn, store, client, cfg)
    assert (list(again.failed), again.skipped) == (["2012q1"], ["2012q2"])
    assert zip_paths(seen) == []  # the stored zip was re-parsed, not fetched


def test_refetch_fetches_again(
    conn: duckdb.DuckDBPyConnection, tmp_path: Path, cfg: Form345Config
) -> None:
    seen: list[str] = []
    client = sec({"2012q1": GOOD}, seen)
    store = RawStore(tmp_path, conn)
    load_insiders(conn, store, client, cfg, only="2012q1")

    seen.clear()
    report = load_insiders(conn, store, client, cfg, only="2012q1", refetch=True)
    assert report.loaded == ["2012q1"]
    assert zip_paths(seen) == [f"/files/structureddata{SETS}/2012q1_form345.zip"]


def test_fetch_failure_fails_only_that_quarter(
    conn: duckdb.DuckDBPyConnection, tmp_path: Path, cfg: Form345Config
) -> None:
    report = load_insiders(conn, RawStore(tmp_path, conn), sec({"2012q2": GOOD}, []), cfg)
    assert report.loaded == ["2012q2"]
    assert "404" in report.failed["2012q1"]


def test_unknown_quarter_is_refused(
    conn: duckdb.DuckDBPyConnection, tmp_path: Path, cfg: Form345Config
) -> None:
    with pytest.raises(ValueError, match="2030q1"):
        load_insiders(conn, RawStore(tmp_path, conn), sec({}, []), cfg, only="2030q1")


def test_load_links_amendments_after_loading(
    conn: duckdb.DuckDBPyConnection, tmp_path: Path, cfg: Form345Config
) -> None:
    orphan = build_zip([submission("am1", form="4/A", orig="01-JAN-2024")])
    client = sec({"2012q1": orphan}, [])
    report = load_insiders(conn, RawStore(tmp_path, conn), client, cfg, only="2012q1")
    assert report.amendments == {"none": 1}
