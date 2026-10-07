"""SEC Insider Transactions Data Sets: the quarterly Form 3/4/5 flat files.

Flow per quarter: discover -> raw store -> parse_zip (pure) -> gate -> write.

`known_at` is FILING_DATE at 22:00 New York. The files carry no acceptance time, and
EDGAR's filing cut-off makes this late by up to a day, never early.
"""

from __future__ import annotations

import io
import re
import zipfile
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import date, timedelta
from urllib.parse import urljoin

import duckdb
import polars as pl

from sds.db import insert_frame
from sds.ingest.config import Form345Config, quarter_key
from sds.ingest.http import PoliteClient
from sds.ingest.raw_store import RawStore
from sds.insiders import link_amendments
from sds.runs import RunStats, enforce_quality_gate, quarantine, run_job
from sds.timeutil import utc_now

DATE_FORMAT = "%d-%b-%Y"
# Lower date bounds. A 2-digit year such as "02-JAN-24" parses as the year 0024.
# filing_date drives known_at, so it must not precede electronic Form 4 filing (2003).
# Other dates describe facts a late filing can report from earlier years.
MIN_FILING_DATE = date(2003, 1, 1)
MIN_FACT_DATE = date(1990, 1, 1)
MAX_DATE_AHEAD = timedelta(days=366)  # load_quarter's upper bound: one year past today
KNOWN_AT_HOUR = 22  # EDGAR filing cut-off, New York time
CIK_PATTERN = r"^[0-9]{1,10}$"
# RPTOWNER_RELATIONSHIP token -> our flag. Pre-2023 files join "Other" with no comma
# ("DirectorOther"), so a valid value is any sequence of these tokens, with optional
# commas and whitespace between them. No token is a substring of another.
ROLE_FLAGS = {
    "Director": "is_director",
    "Officer": "is_officer",
    "TenPercentOwner": "is_ten_pct_owner",
    "Other": "is_other",
}
ROLE_PATTERN = rf"^\s*(?:(?:{'|'.join(ROLE_FLAGS)})\s*,?\s*)*$"
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


def _ragged_rows(data: bytes) -> list[bool]:
    """Per data row: does its field count differ from the header's?

    polars fills a short row's missing columns with NULL and raises no error, and a
    newline inside a text field splits one record into two short rows. The lines are
    split as polars splits them: on "\n", one trailing newline ends the last row, and an
    empty line is a row. A "\r" before "\n" holds no tab, so it does not change a count.
    """
    lines = data.split(b"\n")
    if data.endswith(b"\n"):
        lines.pop()
    header, *rows = (line.count(b"\t") for line in lines)
    return [n != header for n in rows]


def read_table(archive: zipfile.ZipFile, table: Table) -> pl.DataFrame:
    """Read one TSV as text, renamed to our columns, plus `_ragged` per row. Strict
    UTF-8: a bad byte fails the quarter instead of silently altering text."""
    data = archive.read(table.file)
    frame = pl.read_csv(
        io.BytesIO(data),
        separator="\t",
        quote_char=None,
        infer_schema=False,
        empty_string_is_null=True,
    )
    missing = [c for c in table.columns if c not in frame.columns and c not in table.optional]
    if missing:
        raise SchemaDriftError(f"{table.file}: missing column(s) {', '.join(missing)}")
    ragged = _ragged_rows(data)
    if len(ragged) != frame.height:
        raise ValueError(
            f"{table.file}: {len(ragged)} lines but {frame.height} rows; ragged rows cannot"
            " be told apart"
        )
    frame = frame.with_columns(
        pl.lit(None, pl.String).alias(c) for c in table.columns if c not in frame.columns
    )
    return frame.select(
        *(pl.col(sec).alias(ours) for sec, ours in table.columns.items()),
        pl.Series("_ragged", ragged, pl.Boolean),
    )


def _payload(frame: pl.DataFrame) -> pl.Expr:
    """The row's raw text columns as JSON; derived `_` columns are left out."""
    raw = [c for c in frame.columns if not c.startswith("_")]
    return pl.struct(raw).struct.json_encode().alias("_payload")


def _reject_all(frame: pl.DataFrame, table: str, reason: str) -> list[Rejected]:
    rows = frame.select("accession_no", _payload(frame)).iter_rows()
    return [Rejected(table, acc, reason, payload) for acc, payload in rows]


def _split_ragged(frame: pl.DataFrame, table: str) -> tuple[pl.DataFrame, list[Rejected]]:
    """Reject ragged rows before any scope filter: their columns are shifted, so a
    fragment's form or code is garbage and must not make it look out of scope."""
    ragged = frame.filter(pl.col("_ragged"))
    return frame.filter(~pl.col("_ragged")).drop("_ragged"), _reject_all(
        ragged, table, "ragged row"
    )


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


def _date(col: str, max_date: date, min_date: date = MIN_FACT_DATE) -> pl.Expr:
    """The parsed date, or null when unparseable or outside [min_date, max_date]."""
    parsed = pl.col(col).str.strptime(pl.Date, DATE_FORMAT, strict=False)
    return pl.when(parsed.is_between(min_date, max_date)).then(parsed)


def _number(col: str) -> pl.Expr:
    """The value as a float, or null when not a finite number. "NaN", "inf" and "1e400"
    all cast; DuckDB evaluates NaN > 0 as TRUE, so a later filter would not stop them."""
    value = pl.col(col).cast(pl.Float64, strict=False)
    return pl.when(value.is_finite()).then(value)


def _bad_cik(col: str) -> pl.Expr:
    """True where a CIK is present but not 1 to 10 digits; a missing CIK is null."""
    return pl.col(col).str.contains(CIK_PATTERN).not_()


def _cast_failed(raw: str, typed: pl.Expr) -> pl.Expr:
    """True where the text was present but did not convert."""
    return pl.col(raw).is_not_null() & typed.is_null()


def _filings(subs: pl.DataFrame, max_date: date) -> tuple[pl.DataFrame, list[Rejected]]:
    flag = pl.col("is_10b5_1").str.to_lowercase()
    typed = subs.with_columns(
        _date("filing_date", max_date, MIN_FILING_DATE).alias("_filing_date"),
        _date("period_of_report", max_date).alias("_period"),
        _date("date_of_orig_sub", max_date).alias("_orig"),
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
            # An original cannot postdate the filing that amends it.
            "bad date_of_orig_sub": _cast_failed("date_of_orig_sub", pl.col("_orig"))
            | (pl.col("_orig") > pl.col("_filing_date")),
            "bad is_10b5_1": _cast_failed("is_10b5_1", pl.col("_10b5")),
            "no issuer_cik": pl.col("issuer_cik").is_null(),
            "bad issuer_cik": _bad_cik("issuer_cik"),
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
            "bad owner_cik": _bad_cik("owner_cik"),
            "bad relationship": pl.col("relationship").str.contains(ROLE_PATTERN).not_(),
        },
    )
    roles = pl.col("relationship").fill_null("")
    return good.select(
        "accession_no",
        pl.col("owner_cik").str.zfill(10),
        "owner_name",
        *(roles.str.contains(token, literal=True).alias(f) for token, f in ROLE_FLAGS.items()),
        "officer_title",
    ), rejected


def _txns(lines: pl.DataFrame, max_date: date) -> tuple[pl.DataFrame, list[Rejected]]:
    numbers = ("shares", "price", "shares_owned_after")
    typed = lines.with_columns(
        _date("txn_date", max_date).alias("_txn_date"),
        *(_number(c).alias(f"_{c}") for c in numbers),
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


def parse_zip(
    raw: bytes, *, forms: list[str], txn_codes: list[str], max_date: date
) -> InsiderFrames:
    """Parse one quarterly zip into typed frames. Pure: no database, no network, no clock.

    A date outside its bounds (MIN_FILING_DATE or MIN_FACT_DATE, to max_date) is
    rejected like an unparseable one.

    Rows of out-of-scope forms or codes are skipped and not counted, except ragged rows:
    their fields are shifted, so they are rejected and counted whatever their form or
    code. Every other row that fails a check is returned in `rejected` with its reason,
    never dropped.
    """
    with zipfile.ZipFile(io.BytesIO(raw)) as archive:
        all_subs = read_table(archive, SUBMISSION)
        all_owners = read_table(archive, REPORTINGOWNER)
        all_lines = read_table(archive, NONDERIV_TRANS)

    subs, rejected = _split_ragged(all_subs, "filings")
    owners, bad = _split_ragged(all_owners, "owners")
    rejected += bad
    lines, bad = _split_ragged(all_lines, "txns")
    rejected += bad
    rows_in = len(rejected)

    in_scope = subs.filter(pl.col("form").is_in(forms))
    filings, bad = _filings(in_scope, max_date)
    rejected += bad
    every_filing = all_subs.select("accession_no")
    # A ragged filing's form is unknown, so its children are rejected, not skipped.
    scoped = pl.concat(
        [in_scope.select("accession_no"), all_subs.filter("_ragged").select("accession_no")]
    )
    kept = filings.select("accession_no")
    rows_in += in_scope.height

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
    txn_rows, bad_txns = _txns(children["txns"], max_date)
    return InsiderFrames(filings, owner_rows, txn_rows, rejected + bad_owners + bad_txns, rows_in)


SOURCE = "sec/form345"
_KEEP_EXISTING = "ON CONFLICT DO NOTHING"  # a filed fact does not change


def _stored_accessions(conn: duckdb.DuckDBPyConnection, filings: pl.DataFrame) -> pl.DataFrame:
    """The accessions of `filings` already in insider_filings."""
    conn.register("_new_filings", filings.select("accession_no"))
    try:
        return conn.execute(
            "SELECT accession_no FROM insider_filings"
            " WHERE accession_no IN (SELECT accession_no FROM _new_filings)"
        ).pl()
    finally:
        conn.unregister("_new_filings")


def write_frames(conn: duckdb.DuckDBPyConnection, frames: InsiderFrames, raw_hash: str) -> int:
    """Write one quarter in one transaction; returns rows inserted.

    A stored filing is complete: it gets no new owners or lines from a later load. DO
    NOTHING on the filing alone would not stop new child keys, such as a renumbered
    NONDERIV_TRANS_SK, and the filing's shares would count twice.
    """
    conn.execute("BEGIN")
    try:
        stored = _stored_accessions(conn, frames.filings)
        owners = frames.owners.join(stored, on="accession_no", how="anti")
        txns = frames.txns.join(stored, on="accession_no", how="anti")
        filings = frames.filings.with_columns(pl.lit(raw_hash).alias("raw_hash"))
        written = insert_frame(conn, "insider_filings", filings, _KEEP_EXISTING)
        written += insert_frame(conn, "insider_owners", owners, _KEEP_EXISTING)
        written += insert_frame(conn, "insider_txns", txns, _KEEP_EXISTING)
        conn.execute("COMMIT")
    except Exception:
        conn.execute("ROLLBACK")
        raise
    return written


def load_quarter(
    conn: duckdb.DuckDBPyConnection,
    quarter: str,
    cfg: Form345Config,
    raw: Callable[[], tuple[bytes, str]],
) -> RunStats:
    """Fetch (or read stored), parse, gate and write one quarter as one run.

    `raw` returns (zip bytes, raw-store hash). It runs inside the run, so a fetch
    failure is a failed run that `health` shows, like a gate failure.
    """
    with run_job(conn, f"form345.{quarter}") as stats:
        data, raw_hash = raw()
        frames = parse_zip(
            data,
            forms=cfg.forms,
            txn_codes=cfg.txn_codes,
            max_date=utc_now().date() + MAX_DATE_AHEAD,
        )
        stats.rows_in = frames.rows_in
        for r in frames.rejected:
            quarantine(
                conn,
                stats,
                source=SOURCE,
                reason=f"{r.table}: {r.reason}",
                payload=r.payload,
                raw_hash=raw_hash,
            )
        enforce_quality_gate(stats)
        stats.rows_written = write_frames(conn, frames, raw_hash)
    return stats


DATA_SETS_URL = "https://www.sec.gov/data-research/sec-markets-data/insider-transactions-data-sets"
_ZIP_LINK = re.compile(r'href="([^"]*/(\d{4}q[1-4])_form345\.zip)"')


def discover(page: bytes) -> dict[str, str]:
    """{quarter: zip URL} from the data-set page. URLs are read, never built: the SEC
    already serves quarters from two different paths. The first link per quarter wins."""
    found: dict[str, str] = {}
    for href, quarter in _ZIP_LINK.findall(page.decode("utf-8", errors="replace")):
        found.setdefault(quarter, urljoin(DATA_SETS_URL, href))
    if not found:
        raise ValueError(f"no *_form345.zip links on {DATA_SETS_URL}; has the page changed?")
    return found


@dataclass
class LoadReport:
    loaded: list[str] = field(default_factory=list)
    skipped: list[str] = field(default_factory=list)
    failed: dict[str, str] = field(default_factory=dict)
    amendments: dict[str, int] = field(default_factory=dict)


def _loaded(conn: duckdb.DuckDBPyConnection, quarter: str) -> bool:
    row = conn.execute(
        "SELECT 1 FROM runs WHERE job = ? AND status = 'ok' LIMIT 1", [f"form345.{quarter}"]
    ).fetchone()
    return row is not None


def _raw_source(
    conn: duckdb.DuckDBPyConnection,
    store: RawStore,
    client: PoliteClient,
    url: str,
    *,
    refetch: bool,
) -> Callable[[], tuple[bytes, str]]:
    """Stored bytes when we have them (a parser fix is a re-parse), else a fetch."""

    def get() -> tuple[bytes, str]:
        if not refetch:
            row = conn.execute(
                "SELECT raw_hash FROM raw_documents WHERE uri = ? ORDER BY fetched_at DESC LIMIT 1",
                [url],
            ).fetchone()
            if row is not None:
                return store.get(str(row[0])), str(row[0])
        data = client.get(url)
        doc = store.put(data, source=SOURCE, uri=url, fetched_at=utc_now())
        return data, doc.raw_hash

    return get


def load_insiders(
    conn: duckdb.DuckDBPyConnection,
    store: RawStore,
    client: PoliteClient,
    cfg: Form345Config,
    *,
    since: str | None = None,
    only: str | None = None,
    refetch: bool = False,
) -> LoadReport:
    """Load every quarter from `since` (default: config), or only one, then link.

    A quarter with an ok run is skipped unless `refetch`. One failed quarter does not
    stop the rest; it is reported and its run is marked failed.
    """
    urls = discover(client.get(DATA_SETS_URL))
    if only is not None:
        if only not in urls:
            raise ValueError(f"{only} is not on the SEC data-set page")
        wanted = [only]
    else:
        first = quarter_key(since or cfg.start_quarter)
        wanted = sorted((q for q in urls if quarter_key(q) >= first), key=quarter_key)

    report = LoadReport()
    for quarter in wanted:
        if not refetch and _loaded(conn, quarter):
            report.skipped.append(quarter)
            continue
        try:
            load_quarter(
                conn, quarter, cfg, _raw_source(conn, store, client, urls[quarter], refetch=refetch)
            )
            report.loaded.append(quarter)
        except Exception as exc:  # one bad quarter should not stop the rest
            report.failed[quarter] = f"{type(exc).__name__}: {exc}"
    report.amendments = link_amendments(conn)
    return report
