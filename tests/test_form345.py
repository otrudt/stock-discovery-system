from __future__ import annotations

from datetime import UTC, date, datetime

import pytest
from form345_fixture import build_zip, line, owner, submission

from sds.ingest.form345 import InsiderFrames, SchemaDriftError, parse_zip

FORMS = ["4", "4/A"]
CODES = ["P", "S"]


def parse(raw: bytes) -> InsiderFrames:
    return parse_zip(raw, forms=FORMS, txn_codes=CODES)


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
