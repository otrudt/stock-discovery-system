"""Builds small Form 3/4/5 flat-file zips with the real SEC column headers (2026q2)."""

from __future__ import annotations

import io
import zipfile
from collections.abc import Iterable

Row = dict[str, str | None]

# fmt: off
HEADERS = {
    "SUBMISSION.tsv": [
        "ACCESSION_NUMBER", "FILING_DATE", "PERIOD_OF_REPORT", "DATE_OF_ORIG_SUB",
        "NO_SECURITIES_OWNED", "NOT_SUBJECT_SEC16", "FORM3_HOLDINGS_REPORTED",
        "FORM4_TRANS_REPORTED", "DOCUMENT_TYPE", "ISSUERCIK", "ISSUERNAME",
        "ISSUERTRADINGSYMBOL", "REMARKS", "AFF10B5ONE",
    ],
    "REPORTINGOWNER.tsv": [
        "ACCESSION_NUMBER", "RPTOWNERCIK", "RPTOWNERNAME", "RPTOWNER_RELATIONSHIP",
        "RPTOWNER_TITLE", "RPTOWNER_TXT", "RPTOWNER_STREET1", "RPTOWNER_STREET2",
        "RPTOWNER_CITY", "RPTOWNER_STATE", "RPTOWNER_ZIPCODE", "RPTOWNER_STATE_DESC",
        "FILE_NUMBER",
    ],
    "NONDERIV_TRANS.tsv": [
        "ACCESSION_NUMBER", "NONDERIV_TRANS_SK", "SECURITY_TITLE", "SECURITY_TITLE_FN",
        "TRANS_DATE", "TRANS_DATE_FN", "DEEMED_EXECUTION_DATE", "DEEMED_EXECUTION_DATE_FN",
        "TRANS_FORM_TYPE", "TRANS_CODE", "EQUITY_SWAP_INVOLVED", "EQUITY_SWAP_TRANS_CD_FN",
        "TRANS_TIMELINESS", "TRANS_TIMELINESS_FN", "TRANS_SHARES", "TRANS_SHARES_FN",
        "TRANS_PRICEPERSHARE", "TRANS_PRICEPERSHARE_FN", "TRANS_ACQUIRED_DISP_CD",
        "TRANS_ACQUIRED_DISP_CD_FN", "SHRS_OWND_FOLWNG_TRANS", "SHRS_OWND_FOLWNG_TRANS_FN",
        "VALU_OWND_FOLWNG_TRANS", "VALU_OWND_FOLWNG_TRANS_FN", "DIRECT_INDIRECT_OWNERSHIP",
        "DIRECT_INDIRECT_OWNERSHIP_FN", "NATURE_OF_OWNERSHIP", "NATURE_OF_OWNERSHIP_FN",
    ],
}
# fmt: on


def submission(
    acc: str,
    *,
    form: str = "4",
    issuer: str = "1",
    filed: str = "02-JAN-2024",
    period: str = "01-JAN-2024",
    orig: str | None = None,
    aff: str | None = None,
) -> Row:
    return {
        "ACCESSION_NUMBER": acc,
        "DOCUMENT_TYPE": form,
        "ISSUERCIK": issuer,
        "FILING_DATE": filed,
        "PERIOD_OF_REPORT": period,
        "DATE_OF_ORIG_SUB": orig,
        "AFF10B5ONE": aff,
    }


def owner(
    acc: str, cik: str, *, relationship: str | None = "Officer", title: str | None = "CFO"
) -> Row:
    return {
        "ACCESSION_NUMBER": acc,
        "RPTOWNERCIK": cik,
        "RPTOWNERNAME": "DOE JANE",
        "RPTOWNER_RELATIONSHIP": relationship,
        "RPTOWNER_TITLE": title,
    }


def line(
    acc: str,
    sk: str,
    *,
    code: str = "P",
    date: str = "01-JAN-2024",
    shares: str = "100",
    price: str | None = "10.5",
) -> Row:
    return {
        "ACCESSION_NUMBER": acc,
        "NONDERIV_TRANS_SK": sk,
        "TRANS_CODE": code,
        "TRANS_DATE": date,
        "TRANS_SHARES": shares,
        "TRANS_PRICEPERSHARE": price,
        "TRANS_ACQUIRED_DISP_CD": "A" if code == "P" else "D",
        "SHRS_OWND_FOLWNG_TRANS": "1100",
        "DIRECT_INDIRECT_OWNERSHIP": "D",
    }


def build_zip(
    subs: Iterable[Row],
    owners: Iterable[Row] = (),
    lines: Iterable[Row] = (),
    *,
    drop: frozenset[str] = frozenset(),
) -> bytes:
    """`drop` removes columns from every header, to mimic older or drifted files."""
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as archive:
        for name, rows in (
            ("SUBMISSION.tsv", subs),
            ("REPORTINGOWNER.tsv", owners),
            ("NONDERIV_TRANS.tsv", lines),
        ):
            cols = [c for c in HEADERS[name] if c not in drop]
            text = ["\t".join(cols)] + ["\t".join(r.get(c) or "" for c in cols) for r in rows]
            archive.writestr(name, "\n".join(text) + "\n")
    return buf.getvalue()
