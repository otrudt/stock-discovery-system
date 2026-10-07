"""SEC Insider Transactions Data Sets: the quarterly Form 3/4/5 flat files.

Flow per quarter: discover -> raw store -> parse_zip (pure) -> gate -> write.

`known_at` is FILING_DATE at 22:00 New York. The files carry no acceptance time, and
EDGAR's filing cut-off makes this late by up to a day, never early.
"""

from __future__ import annotations

import io
import zipfile
from dataclasses import dataclass

import polars as pl

DATE_FORMAT = "%d-%b-%Y"
KNOWN_AT_HOUR = 22  # EDGAR filing cut-off, New York time
TRUE_VALUES = ["1", "true"]
FALSE_VALUES = ["0", "false"]


@dataclass(frozen=True)
class Table:
    """One SEC table: its file, the columns we keep (SEC name -> ours), and which of
    them may be absent from older files."""

    file: str
    columns: dict[str, str]
    optional: frozenset[str] = frozenset()


SUBMISSION = Table(
    "SUBMISSION.tsv",
    {
        "ACCESSION_NUMBER": "accession_no",
        "ISSUERCIK": "issuer_cik",
        "DOCUMENT_TYPE": "form",
        "FILING_DATE": "filing_date",
        "PERIOD_OF_REPORT": "period_of_report",
        "DATE_OF_ORIG_SUB": "date_of_orig_sub",
        "AFF10B5ONE": "is_10b5_1",
    },
    optional=frozenset({"AFF10B5ONE"}),  # absent before the 2023 form change
)
REPORTINGOWNER = Table(
    "REPORTINGOWNER.tsv",
    {
        "ACCESSION_NUMBER": "accession_no",
        "RPTOWNERCIK": "owner_cik",
        "RPTOWNERNAME": "owner_name",
        "RPTOWNER_RELATIONSHIP": "relationship",
        "RPTOWNER_TITLE": "officer_title",
    },
)
NONDERIV_TRANS = Table(
    "NONDERIV_TRANS.tsv",
    {
        "ACCESSION_NUMBER": "accession_no",
        "NONDERIV_TRANS_SK": "line_id",
        "TRANS_CODE": "txn_code",
        "TRANS_DATE": "txn_date",
        "TRANS_SHARES": "shares",
        "TRANS_PRICEPERSHARE": "price",
        "TRANS_ACQUIRED_DISP_CD": "acquired_disposed",
        "SHRS_OWND_FOLWNG_TRANS": "shares_owned_after",
        "DIRECT_INDIRECT_OWNERSHIP": "direct_indirect",
    },
)


class SchemaDriftError(ValueError):
    """A required column is missing: the SEC changed the file layout."""


@dataclass(frozen=True)
class Rejected:
    table: str
    accession_no: str | None
    reason: str
    payload: str  # the row's raw text fields as JSON, so quarantine shows the bad value


@dataclass(frozen=True)
class InsiderFrames:
    filings: pl.DataFrame
    owners: pl.DataFrame
    txns: pl.DataFrame
    rejected: list[Rejected]
    rows_in: int


def read_table(archive: zipfile.ZipFile, table: Table) -> pl.DataFrame:
    """Read one TSV as text, renamed to our columns. Strict UTF-8: a bad byte fails
    the quarter instead of silently altering text."""
    frame = pl.read_csv(
        io.BytesIO(archive.read(table.file)),
        separator="\t",
        quote_char=None,
        infer_schema=False,
        empty_string_is_null=True,
    )
    missing = [c for c in table.columns if c not in frame.columns and c not in table.optional]
    if missing:
        raise SchemaDriftError(f"{table.file}: missing column(s) {', '.join(missing)}")
    frame = frame.with_columns(
        pl.lit(None, pl.String).alias(c) for c in table.columns if c not in frame.columns
    )
    return frame.select(pl.col(sec).alias(ours) for sec, ours in table.columns.items())


def _payload(frame: pl.DataFrame) -> pl.Expr:
    """The row's raw text columns as JSON; derived `_` columns are left out."""
    raw = [c for c in frame.columns if not c.startswith("_")]
    return pl.struct(raw).struct.json_encode().alias("_payload")


def _reject_all(frame: pl.DataFrame, table: str, reason: str) -> list[Rejected]:
    rows = frame.select("accession_no", _payload(frame)).iter_rows()
    return [Rejected(table, acc, reason, payload) for acc, payload in rows]


def _split_rejects(
    frame: pl.DataFrame, table: str, checks: dict[str, pl.Expr]
) -> tuple[pl.DataFrame, list[Rejected]]:
    """`checks` maps a reason to an expression that is true for a bad row. A row
    failing several checks gets all their reasons."""
    reason = pl.concat_str(
        [pl.when(bad).then(pl.lit(label)) for label, bad in checks.items()],
        separator="; ",
        ignore_nulls=True,
    )
    tagged = frame.with_columns(reason.alias("_reason"))
    bad = tagged.filter(pl.col("_reason") != "")
    rows = bad.select("accession_no", "_reason", _payload(frame)).iter_rows()
    rejected = [Rejected(table, acc, why, payload) for acc, why, payload in rows]
    return tagged.filter(pl.col("_reason") == "").drop("_reason"), rejected


def _date(col: str) -> pl.Expr:
    return pl.col(col).str.strptime(pl.Date, DATE_FORMAT, strict=False)


def _cast_failed(raw: str, typed: pl.Expr) -> pl.Expr:
    """True where the text was present but did not convert."""
    return pl.col(raw).is_not_null() & typed.is_null()


def _filings(subs: pl.DataFrame) -> tuple[pl.DataFrame, list[Rejected]]:
    flag = pl.col("is_10b5_1").str.to_lowercase()
    typed = subs.with_columns(
        _date("filing_date").alias("_filing_date"),
        _date("period_of_report").alias("_period"),
        _date("date_of_orig_sub").alias("_orig"),
        pl.when(flag.is_in(TRUE_VALUES))
        .then(True)
        .when(flag.is_in(FALSE_VALUES))
        .then(False)
        .alias("_10b5"),
    )
    good, rejected = _split_rejects(
        typed,
        "filings",
        {
            "duplicate key": pl.col("accession_no").is_duplicated(),
            "bad filing_date": pl.col("_filing_date").is_null(),
            "bad period_of_report": pl.col("_period").is_null(),
            "bad date_of_orig_sub": _cast_failed("date_of_orig_sub", pl.col("_orig")),
            "bad is_10b5_1": _cast_failed("is_10b5_1", pl.col("_10b5")),
            "no issuer_cik": pl.col("issuer_cik").is_null(),
        },
    )
    known_at = (
        pl.col("_filing_date")
        .cast(pl.Datetime("us"))
        .dt.offset_by(f"{KNOWN_AT_HOUR}h")
        .dt.replace_time_zone("America/New_York")
        .dt.convert_time_zone("UTC")
    )
    return good.select(
        "accession_no",
        pl.col("issuer_cik").str.zfill(10),
        "form",
        pl.col("_filing_date").alias("filing_date"),
        pl.col("_period").alias("period_of_report"),
        pl.col("_orig").alias("date_of_orig_sub"),
        pl.col("_10b5").alias("is_10b5_1"),
        known_at.alias("known_at"),
    ), rejected


def _owners(owners: pl.DataFrame) -> tuple[pl.DataFrame, list[Rejected]]:
    good, rejected = _split_rejects(
        owners,
        "owners",
        {
            "duplicate key": pl.struct("accession_no", "owner_cik").is_duplicated(),
            "no owner_cik": pl.col("owner_cik").is_null(),
        },
    )
    roles = pl.col("relationship").fill_null("").str.split(",")
    return good.select(
        "accession_no",
        pl.col("owner_cik").str.zfill(10),
        "owner_name",
        roles.list.contains("Director").alias("is_director"),
        roles.list.contains("Officer").alias("is_officer"),
        roles.list.contains("TenPercentOwner").alias("is_ten_pct_owner"),
        roles.list.contains("Other").alias("is_other"),
        "officer_title",
    ), rejected


def _txns(lines: pl.DataFrame) -> tuple[pl.DataFrame, list[Rejected]]:
    numbers = ("shares", "price", "shares_owned_after")
    typed = lines.with_columns(
        _date("txn_date").alias("_txn_date"),
        *(pl.col(c).cast(pl.Float64, strict=False).alias(f"_{c}") for c in numbers),
    )
    good, rejected = _split_rejects(
        typed,
        "txns",
        {
            "duplicate key": pl.struct("accession_no", "line_id").is_duplicated(),
            "no line_id": pl.col("line_id").is_null(),
            "bad txn_date": pl.col("_txn_date").is_null(),
            "bad shares": pl.col("_shares").is_null(),
            "bad price": _cast_failed("price", pl.col("_price")),
            "bad shares_owned_after": _cast_failed(
                "shares_owned_after", pl.col("_shares_owned_after")
            ),
        },
    )
    return good.select(
        "accession_no",
        "line_id",
        "txn_code",
        pl.col("_txn_date").alias("txn_date"),
        pl.col("_shares").alias("shares"),
        pl.col("_price").alias("price"),
        "acquired_disposed",
        pl.col("_shares_owned_after").alias("shares_owned_after"),
        "direct_indirect",
    ), rejected


def parse_zip(raw: bytes, *, forms: list[str], txn_codes: list[str]) -> InsiderFrames:
    """Parse one quarterly zip into typed frames. Pure: no database, no network.

    Rows of out-of-scope forms or codes are skipped and not counted. Every other row
    that fails a check is returned in `rejected` with its reason, never dropped.
    """
    with zipfile.ZipFile(io.BytesIO(raw)) as archive:
        subs = read_table(archive, SUBMISSION)
        owners = read_table(archive, REPORTINGOWNER)
        lines = read_table(archive, NONDERIV_TRANS)

    in_scope = subs.filter(pl.col("form").is_in(forms))
    filings, rejected = _filings(in_scope)
    every_filing = subs.select("accession_no")
    scoped = in_scope.select("accession_no")
    kept = filings.select("accession_no")
    rows_in = in_scope.height

    # Owners and lines follow their filing: out-of-scope form -> skipped; filing
    # rejected -> rejected; no filing anywhere in the file -> rejected.
    children: dict[str, pl.DataFrame] = {}
    for table, frame in (
        ("owners", owners),
        ("txns", lines.filter(pl.col("txn_code").is_in(txn_codes))),
    ):
        orphans = frame.join(every_filing, on="accession_no", how="anti")
        frame = frame.join(scoped, on="accession_no", how="semi")
        rows_in += orphans.height + frame.height
        rejected += _reject_all(orphans, table, "no filing")
        rejected += _reject_all(
            frame.join(kept, on="accession_no", how="anti"), table, "filing rejected"
        )
        children[table] = frame.join(kept, on="accession_no", how="semi")

    owner_rows, bad_owners = _owners(children["owners"])
    txn_rows, bad_txns = _txns(children["txns"])
    return InsiderFrames(filings, owner_rows, txn_rows, rejected + bad_owners + bad_txns, rows_in)
