# Form 4 Ingestion Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Load SEC quarterly Form 4 flat files (2012q1 onward, codes P and S) into three normalized, point-in-time tables, with amendments linked to their originals, so pre-registration 001 can run.

**Architecture:** Fetch each quarterly zip to the raw store, parse it with a pure polars function into three typed frames plus rejected rows, gate and write each quarter as one run, then recompute amendment links over all filings in one SQL pass. Research code reads only through two as-of functions that share one visibility rule.

**Tech Stack:** Python 3.11+, DuckDB 1.5, polars 1.44, pydantic 2, httpx, typer, pytest. No new dependencies.

**Spec:** `docs/superpowers/specs/2026-10-05-form4-ingestion-design.md`

## Global Constraints

- Run all four checks before every commit: `uv run ruff check . && uv run ruff format --check . && uv run mypy && uv run pytest`.
- Line length 100; mypy strict over `src/sds`; ruff rules E, F, I, B, UP, SIM, DTZ.
- No database foreign keys. Integrity is enforced in code and tests.
- `known_at` = `FILING_DATE` at 22:00 America/New_York, stored as UTC.
- SEC requests send `SDS_USER_AGENT` and use `SEC_RATE_PER_SECOND = 8.0` (under 10/s).
- Fetch to the raw store first, parse second (invariant 6). Write research tables only after the 1% gate passes (invariant 7).
- Never overwrite a stored fact: inserts use `ON CONFLICT DO NOTHING`.
- Files are decoded as strict UTF-8. `AFF10B5ONE` is the only optional column.
- Research code reads insider data only through `sds.asof`.
- Every new test must be shown to fail against a deliberate mutation of the code it guards (one mutation per task, noted in the task).

## Review Focus

1. An amendment whose original sits in a quarter not yet loaded (backfill order is not knowledge order): it must link once the older quarter loads. Pinned in Task 5 (`test_original_loaded_after_amendment_still_links`).
2. CIKs without zero padding: issuer and owner CIKs must come out as 10 digits. Pinned in Task 3 (`test_parse_types_and_pads_ciks`).
3. An owner row with an empty relationship: all four role flags false, not a crash or a NULL. Pinned in Task 3 (`test_roles_split_and_empty_relationship_has_no_roles`).
4. The SEC page lists one quarter twice or under two paths: one URL per quarter. Pinned in Task 6 (`test_discover_reads_both_paths_and_dedupes`).
5. A quarter that failed its gate (or crashed) after the fetch: the next run re-parses the stored zip, with no fetch, instead of skipping it forever. Pinned in Task 6 (`test_failed_quarter_is_reparsed_from_store_without_fetch`).

## File Structure

| File | Responsibility |
|---|---|
| `src/sds/db/migrations/0002_insider_normalized.sql` (create) | The three insider tables; drops the empty 0001 table |
| `src/sds/asof.py` (modify) | `insider_txns_as_of`, `insider_owners_as_of`, one shared visibility rule |
| `src/sds/cli.py` (modify) | `health` reads `insider_filings`; `insiders load` / `insiders link` |
| `src/sds/configmodel.py` (create) | `StrictModel`, the shared base for config models |
| `src/sds/universe.py` (modify) | Uses `StrictModel` instead of its private copy |
| `config/ingest.yaml` (create) | What to load: start quarter, forms, codes |
| `src/sds/ingest/config.py` (create) | `Form345Config`, `IngestConfig`, `load_ingest_config`, `quarter_key` |
| `src/sds/ingest/form345.py` (create) | Column map, `parse_zip`, `write_frames`, `load_quarter`, `discover`, `load_insiders` |
| `src/sds/insiders.py` (create) | `link_amendments` (source-independent) |
| `src/sds/db/__init__.py` (modify) | `insert_frame` returns rows actually written |
| `tests/form345_fixture.py` (create) | Builds small zips with the real SEC headers |
| `tests/test_db.py`, `tests/test_asof.py`, `tests/test_cli.py` (modify) | Schema, as-of and CLI tests |
| `tests/test_ingest_config.py`, `tests/test_form345.py`, `tests/test_insiders.py` (create) | Config, parse/load and link tests |

---

### Task 1: Schema 0002, as-of reads, health

**Files:**
- Create: `src/sds/db/migrations/0002_insider_normalized.sql`
- Modify: `src/sds/asof.py:70-90` (replace `insider_txns_as_of`)
- Modify: `src/sds/cli.py:104` (health query line)
- Test: `tests/test_db.py`, `tests/test_asof.py`, `tests/test_cli.py`

**Interfaces:**
- Consumes: nothing new.
- Produces: tables `insider_filings`, `insider_owners`, `insider_txns` (columns exactly as in the migration below); `asof.insider_txns_as_of(conn, as_of, issuer_ciks: list[str] | None = None) -> pl.DataFrame`; `asof.insider_owners_as_of(conn, as_of, issuer_ciks: list[str] | None = None) -> pl.DataFrame`.

- [ ] **Step 1: Write the failing tests**

In `tests/test_db.py`, replace the body of `test_migrations_are_idempotent` and add a schema test:

```python
def test_migrations_are_idempotent(conn: duckdb.DuckDBPyConnection) -> None:
    assert migrate(conn) == []
    versions = conn.execute("SELECT version FROM schema_migrations ORDER BY version").fetchall()
    assert versions == [(1,), (2,)]


def test_insider_tables_hold_a_joint_filing(conn: duckdb.DuckDBPyConnection) -> None:
    conn.execute(
        "INSERT INTO insider_filings (accession_no, issuer_cik, form, filing_date,"
        " period_of_report, known_at, raw_hash) VALUES ('a1', '0000000001', '4',"
        " '2024-01-02', '2024-01-01', '2024-01-03 03:00:00+00', 'h')"
    )
    for cik in ("0000000011", "0000000012"):
        conn.execute(
            "INSERT INTO insider_owners VALUES ('a1', ?, NULL, false, false, true, false, NULL)",
            [cik],
        )
    conn.execute(
        "INSERT INTO insider_txns VALUES ('a1', '1', 'P', '2024-01-01', 100, 10.0, 'A', 1100, 'I')"
    )
    assert conn.execute("SELECT count(*) FROM insider_owners").fetchone() == (2,)
    assert conn.execute("SELECT sum(shares) FROM insider_txns").fetchone() == (100.0,)
```

In `tests/test_asof.py`, delete `test_amendment_supersedes_only_once_public` and add, below `add_security`:

```python
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
    add_filing(
        conn, "joint", "2024-03-04 21:00", owners=("0000000011", "0000000012", "0000000013")
    )
    add_line(conn, "joint", 100)
    as_of = ts("2024-04-01 00:00")

    assert asof.insider_txns_as_of(conn, as_of)["shares"].sum() == 100
    assert asof.insider_owners_as_of(conn, as_of).height == 3
```

In `tests/test_cli.py`, add:

```python
def test_health_reports_insider_filings(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("SDS_DATA_DIR", str(tmp_path))
    conn = connect(tmp_path / "sds.duckdb")
    conn.execute(
        "INSERT INTO insider_filings (accession_no, issuer_cik, form, filing_date,"
        " period_of_report, known_at, raw_hash) VALUES ('a1', '0000000001', '4',"
        " '2024-01-02', '2024-01-01', '2024-01-03 03:00:00+00', 'h')"
    )
    conn.close()

    result = CliRunner().invoke(app, ["health"])

    assert result.exit_code == 0, result.output
    assert "insider_filings" in result.output
    assert "2024-01-03" in result.output
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `uv run pytest tests/test_db.py tests/test_asof.py tests/test_cli.py -q`
Expected: FAIL — `insider_filings` does not exist; versions are `[(1,)]`.

- [ ] **Step 3: Write the migration**

Create `src/sds/db/migrations/0002_insider_normalized.sql`:

```sql
-- 0002: Form 4 data in three tables that mirror the SEC flat files.
--
-- 0001 held one insider per line, which cannot represent a joint filing (several
-- reporting owners share one set of lines; 11% of purchase lines in 2026q2). The old
-- table never held data, so it is dropped rather than migrated.

DROP TABLE IF EXISTS insider_txns;

-- One row per filing. Knowledge time and amendment links live here, once.
CREATE TABLE IF NOT EXISTS insider_filings (
    accession_no     VARCHAR PRIMARY KEY,
    issuer_cik       VARCHAR NOT NULL,
    form             VARCHAR NOT NULL,          -- '4' or '4/A'
    filing_date      DATE NOT NULL,
    period_of_report DATE NOT NULL,
    date_of_orig_sub DATE,                      -- amendments only
    is_10b5_1        BOOLEAN,                   -- NULL: not reported (field added 2023)
    known_at         TIMESTAMPTZ NOT NULL,      -- filing_date 22:00 New York
    superseded_by    VARCHAR,                   -- next amendment in the chain
    amendment_match  VARCHAR CHECK (amendment_match IN ('one', 'none', 'many')),
    raw_hash         VARCHAR NOT NULL,
    ingested_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- One row per reporting owner of a filing. Roles are as reported in that filing.
CREATE TABLE IF NOT EXISTS insider_owners (
    accession_no     VARCHAR NOT NULL,
    owner_cik        VARCHAR NOT NULL,
    owner_name       VARCHAR,
    is_director      BOOLEAN NOT NULL,
    is_officer       BOOLEAN NOT NULL,
    is_ten_pct_owner BOOLEAN NOT NULL,
    is_other         BOOLEAN NOT NULL,
    officer_title    VARCHAR,
    PRIMARY KEY (accession_no, owner_cik)
);

-- One row per non-derivative line. Shares are stored once, whatever the owner count.
CREATE TABLE IF NOT EXISTS insider_txns (
    accession_no       VARCHAR NOT NULL,
    line_id            VARCHAR NOT NULL,        -- SEC NONDERIV_TRANS_SK
    txn_code           VARCHAR NOT NULL,        -- 'P' purchase, 'S' sale
    txn_date           DATE NOT NULL,
    shares             DOUBLE NOT NULL,
    price              DOUBLE,
    acquired_disposed  VARCHAR,                 -- 'A' or 'D'
    shares_owned_after DOUBLE,
    direct_indirect    VARCHAR,                 -- 'D' or 'I'
    PRIMARY KEY (accession_no, line_id)
);
```

- [ ] **Step 4: Replace the insider read in `src/sds/asof.py`**

Replace the whole `insider_txns_as_of` function with:

```python
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
```

- [ ] **Step 5: Point `health` at the new table**

In `src/sds/cli.py`, in `health`, replace the line

```python
        UNION ALL SELECT 'insider_txns', max(known_at) FROM insider_txns
```

with

```python
        UNION ALL SELECT 'insider_filings', max(known_at) FROM insider_filings
```

- [ ] **Step 6: Run the tests to verify they pass**

Run: `uv run pytest -q`
Expected: all pass.

- [ ] **Step 7: Prove the visibility test can fail**

In `_VISIBLE_INSIDER_FILINGS`, delete ` AND nxt.known_at <= $as_of`. Run `uv run pytest tests/test_asof.py -q`. Expected: `test_amendment_supersedes_only_once_public` and the chain test FAIL. Restore the line and rerun: pass.

- [ ] **Step 8: Run the four checks and commit**

```bash
uv run ruff check . && uv run ruff format --check . && uv run mypy && uv run pytest
git add src/sds/db/migrations/0002_insider_normalized.sql src/sds/asof.py src/sds/cli.py tests/test_db.py tests/test_asof.py tests/test_cli.py
git commit -m "Insider schema 0002: filings, owners, lines; as-of reads share one visibility rule"
```

---

### Task 2: Ingest config and the shared config base

**Files:**
- Create: `src/sds/configmodel.py`, `config/ingest.yaml`, `src/sds/ingest/config.py`
- Modify: `src/sds/universe.py` (replace `_Strict` with `StrictModel`)
- Test: `tests/test_ingest_config.py`

**Interfaces:**
- Consumes: nothing new.
- Produces: `sds.configmodel.StrictModel`; `sds.ingest.config.Form345Config(start_quarter: str, forms: list[str], txn_codes: list[str])`; `IngestConfig(form345: Form345Config)`; `load_ingest_config(path: Path = DEFAULT_INGEST_CONFIG) -> IngestConfig`; `quarter_key(quarter: str) -> tuple[int, int]`; `DEFAULT_INGEST_CONFIG: Path`.

- [ ] **Step 1: Write the failing tests**

Create `tests/test_ingest_config.py`:

```python
from __future__ import annotations

from pathlib import Path

import pytest
from pydantic import ValidationError

from sds.ingest.config import load_ingest_config, quarter_key


def test_repo_ingest_config_loads() -> None:
    cfg = load_ingest_config().form345
    assert cfg.start_quarter == "2012q1"
    assert cfg.forms == ["4", "4/A"]
    assert cfg.txn_codes == ["P", "S"]


def test_bad_quarter_refused(tmp_path: Path) -> None:
    path = tmp_path / "ingest.yaml"
    path.write_text("form345: {start_quarter: 2012q5, forms: ['4'], txn_codes: [P]}\n")
    with pytest.raises(ValidationError, match="not a quarter"):
        load_ingest_config(path)


def test_unknown_key_refused(tmp_path: Path) -> None:
    path = tmp_path / "ingest.yaml"
    path.write_text(
        "form345: {start_quarter: 2012q1, forms: ['4'], txn_codes: [P], typo: 1}\n"
    )
    with pytest.raises(ValidationError, match="typo"):
        load_ingest_config(path)


def test_quarter_key_orders_quarters() -> None:
    assert sorted(["2013q1", "2012q4", "2012q1"], key=quarter_key) == [
        "2012q1",
        "2012q4",
        "2013q1",
    ]
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `uv run pytest tests/test_ingest_config.py -q`
Expected: FAIL — `ModuleNotFoundError: No module named 'sds.ingest.config'`.

- [ ] **Step 3: Create the shared base and use it in `universe.py`**

Create `src/sds/configmodel.py`:

```python
"""Base for config models: unknown keys are errors and values are immutable."""

from __future__ import annotations

from pydantic import BaseModel, ConfigDict


class StrictModel(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)
```

In `src/sds/universe.py`: delete the `class _Strict(BaseModel): ...` block (two lines plus the blank lines), add `from sds.configmodel import StrictModel` to the imports, replace every `_Strict` with `StrictModel`, and change the pydantic import to `from pydantic import Field, field_validator, model_validator` (drop `BaseModel` and `ConfigDict`, now unused).

- [ ] **Step 4: Create the config file and its model**

Create `config/ingest.yaml`:

```yaml
# What to load from each source. sectors.yaml says what to study; this file says
# what to fetch. Widening a load is an edit here, then a re-parse of the raw store.

form345:
  # SEC Insider Transactions Data Sets. 2012q1 gives three years of history before
  # the first 2015 event, for the routine/opportunistic classification.
  start_quarter: 2012q1
  forms: ["4", "4/A"]
  # S is loaded for the routine/opportunistic classification; the signal uses P only.
  txn_codes: [P, S]
```

Create `src/sds/ingest/config.py`:

```python
"""Ingestion config: what each source loads. Validated like config/sectors.yaml."""

from __future__ import annotations

import re
from pathlib import Path
from typing import Any

import yaml
from pydantic import Field, field_validator

from sds.configmodel import StrictModel

DEFAULT_INGEST_CONFIG = Path(__file__).resolve().parents[3] / "config" / "ingest.yaml"

_QUARTER = re.compile(r"^(\d{4})q([1-4])$")


def quarter_key(quarter: str) -> tuple[int, int]:
    """'2012q1' -> (2012, 1), for ordering. Raises ValueError on any other shape."""
    match = _QUARTER.match(quarter)
    if match is None:
        raise ValueError(f"not a quarter like 2012q1: {quarter!r}")
    return int(match.group(1)), int(match.group(2))


class Form345Config(StrictModel):
    start_quarter: str
    forms: list[str] = Field(min_length=1)
    txn_codes: list[str] = Field(min_length=1)

    @field_validator("start_quarter")
    @classmethod
    def _quarter(cls, v: str) -> str:
        quarter_key(v)
        return v


class IngestConfig(StrictModel):
    form345: Form345Config


def load_ingest_config(path: Path = DEFAULT_INGEST_CONFIG) -> IngestConfig:
    raw: Any = yaml.safe_load(path.read_text(encoding="utf-8"))
    return IngestConfig.model_validate(raw)
```

- [ ] **Step 5: Run the tests to verify they pass**

Run: `uv run pytest -q`
Expected: all pass (the universe tests confirm the `StrictModel` swap).

- [ ] **Step 6: Prove the quarter test can fail**

Change the regex to `r"^(\d{4})q(\d)$"`. Run `uv run pytest tests/test_ingest_config.py -q`. Expected: `test_bad_quarter_refused` FAILS. Restore.

- [ ] **Step 7: Run the four checks and commit**

```bash
uv run ruff check . && uv run ruff format --check . && uv run mypy && uv run pytest
git add src/sds/configmodel.py src/sds/universe.py config/ingest.yaml src/sds/ingest/config.py tests/test_ingest_config.py
git commit -m "Add config/ingest.yaml for Form 4 loads; share one strict config base"
```

---

### Task 3: `parse_zip` — pure parse and validation

**Files:**
- Create: `src/sds/ingest/form345.py`, `tests/form345_fixture.py`
- Test: `tests/test_form345.py`

**Interfaces:**
- Consumes: nothing from earlier tasks.
- Produces (in `sds.ingest.form345`): `parse_zip(raw: bytes, *, forms: list[str], txn_codes: list[str]) -> InsiderFrames`; `InsiderFrames(filings, owners, txns: pl.DataFrame, rejected: list[Rejected], rows_in: int)`; `Rejected(table: str, accession_no: str | None, reason: str, payload: str)` (payload = the raw row as JSON); `SchemaDriftError(ValueError)`. Frame columns: `filings` = accession_no, issuer_cik, form, filing_date, period_of_report, date_of_orig_sub, is_10b5_1, known_at; `owners` = accession_no, owner_cik, owner_name, is_director, is_officer, is_ten_pct_owner, is_other, officer_title; `txns` = accession_no, line_id, txn_code, txn_date, shares, price, acquired_disposed, shares_owned_after, direct_indirect.
- Produces (in `tests/form345_fixture.py`): `submission(...)`, `owner(...)`, `line(...)`, `build_zip(subs, owners=(), lines=(), *, drop=frozenset()) -> bytes`.

- [ ] **Step 1: Create the test fixture builder**

Create `tests/form345_fixture.py`:

```python
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
```


- [ ] **Step 2: Write the failing tests**

Create `tests/test_form345.py`:

```python
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
    raw = build_zip(
        [submission("a1")], lines=[line("a1", "1")], drop=frozenset({"TRANS_SHARES"})
    )
    with pytest.raises(SchemaDriftError, match="TRANS_SHARES"):
        parse(raw)


def test_roles_split_and_empty_relationship_has_no_roles() -> None:
    owners = [
        owner("a1", "11", relationship="Director,Officer,TenPercentOwner"),
        owner("a1", "12", relationship=None),
    ]
    frames = parse(build_zip([submission("a1")], owners))
    roles = frames.owners.sort("owner_cik").select(
        "is_director", "is_officer", "is_ten_pct_owner", "is_other"
    )
    assert roles.rows() == [(True, True, True, False), (False, False, False, False)]


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
```

- [ ] **Step 3: Run the tests to verify they fail**

Run: `uv run pytest tests/test_form345.py -q`
Expected: FAIL — `ModuleNotFoundError: No module named 'sds.ingest.form345'`.

- [ ] **Step 4: Write `src/sds/ingest/form345.py` (parse part)**

```python
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
        missing_utf8_is_empty_string=False,
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
```

- [ ] **Step 5: Run the tests to verify they pass**

Run: `uv run pytest tests/test_form345.py -q`
Expected: all pass. Then run `uv run ruff format .` and `uv run ruff check .` and fix any line-length findings by wrapping.

- [ ] **Step 6: Prove the tests can fail**

Remove `.str.zfill(10)` from the `issuer_cik` select. Run `uv run pytest tests/test_form345.py -q`. Expected: `test_parse_types_and_pads_ciks` FAILS. Restore. Then delete the `"duplicate key"` entry in `_txns`; expected: `test_duplicate_keys_are_rejected_not_dropped` FAILS. Restore.

- [ ] **Step 7: Run the four checks and commit**

```bash
uv run ruff check . && uv run ruff format --check . && uv run mypy && uv run pytest
git add src/sds/ingest/form345.py tests/form345_fixture.py tests/test_form345.py
git commit -m "Parse SEC Form 345 quarterly zips into typed frames with reasoned rejects"
```

---

### Task 4: Gate and write one quarter

**Files:**
- Modify: `src/sds/db/__init__.py` (`insert_frame` return value)
- Modify: `src/sds/ingest/form345.py` (add `write_frames`, `load_quarter`)
- Test: `tests/test_db.py`, `tests/test_form345.py`

**Interfaces:**
- Consumes: `parse_zip`, `InsiderFrames`, `SchemaDriftError` (Task 3); `Form345Config` (Task 2); `run_job`, `quarantine`, `enforce_quality_gate`, `RunStats` from `sds.runs`.
- Produces: `insert_frame(...) -> int` now returns rows inserted or updated; `write_frames(conn, frames: InsiderFrames, raw_hash: str) -> int`; `load_quarter(conn, quarter: str, cfg: Form345Config, raw: Callable[[], tuple[bytes, str]]) -> RunStats`; `SOURCE = "sec/form345"`.

- [ ] **Step 1: Write the failing tests**

In `tests/test_db.py`, add (with `import polars as pl` and `from sds.db import insert_frame, migrate`):

```python
def test_insert_frame_returns_rows_actually_written(conn: duckdb.DuckDBPyConnection) -> None:
    conn.execute("CREATE TABLE t (k INTEGER PRIMARY KEY, v INTEGER)")
    frame = pl.DataFrame({"k": [1, 2], "v": [1, 1]})
    assert insert_frame(conn, "t", frame, "ON CONFLICT DO NOTHING") == 2
    assert insert_frame(conn, "t", frame, "ON CONFLICT DO NOTHING") == 0
```

In `tests/test_form345.py`, extend the imports:

```python
import duckdb

from sds.ingest.config import Form345Config
from sds.ingest.form345 import InsiderFrames, SchemaDriftError, load_quarter, parse_zip
from sds.runs import QualityGateError
```

and add:

```python
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
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `uv run pytest tests/test_db.py tests/test_form345.py -q`
Expected: FAIL — `insert_frame` returns 2 the second time; `load_quarter` cannot be imported.

- [ ] **Step 3: Make `insert_frame` return the real count**

In `src/sds/db/__init__.py`, change the body of `insert_frame` and its docstring's first line:

```python
    """Insert a whole frame in one statement; returns rows inserted or updated.

    Row-at-a-time `executemany` runs at about 1k rows/s in DuckDB; a scan of the frame
    is orders of magnitude faster. Columns are matched by name. `on_conflict` is a
    trusted SQL suffix such as "ON CONFLICT (...) DO UPDATE SET ...".
    """
    if frame.is_empty():
        return 0
    cols = ", ".join(frame.columns)
    conn.register("_insert_frame", frame)
    try:
        row = conn.execute(
            f"INSERT INTO {table} ({cols}) SELECT {cols} FROM _insert_frame {on_conflict}"
        ).fetchone()
    finally:
        conn.unregister("_insert_frame")
    return 0 if row is None else int(row[0])
```

- [ ] **Step 4: Add `write_frames` and `load_quarter`**

In `src/sds/ingest/form345.py`, add to the imports:

```python
from collections.abc import Callable

import duckdb

from sds.db import insert_frame
from sds.ingest.config import Form345Config
from sds.runs import RunStats, enforce_quality_gate, quarantine, run_job
```

and append:

```python
SOURCE = "sec/form345"
_KEEP_EXISTING = "ON CONFLICT DO NOTHING"  # a filed fact does not change


def write_frames(conn: duckdb.DuckDBPyConnection, frames: InsiderFrames, raw_hash: str) -> int:
    """Write one quarter in one transaction; returns rows inserted."""
    conn.execute("BEGIN")
    try:
        filings = frames.filings.with_columns(pl.lit(raw_hash).alias("raw_hash"))
        written = insert_frame(conn, "insider_filings", filings, _KEEP_EXISTING)
        written += insert_frame(conn, "insider_owners", frames.owners, _KEEP_EXISTING)
        written += insert_frame(conn, "insider_txns", frames.txns, _KEEP_EXISTING)
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
        frames = parse_zip(data, forms=cfg.forms, txn_codes=cfg.txn_codes)
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
```

- [ ] **Step 5: Run the tests to verify they pass**

Run: `uv run pytest -q`
Expected: all pass (the price tests still see 3 rows written on reload: `DO UPDATE` counts updated rows).

- [ ] **Step 6: Prove the gate test can fail**

Move `enforce_quality_gate(stats)` below the `write_frames` line. Run `uv run pytest tests/test_form345.py -q`. Expected: `test_gate_blocks_every_write_for_the_quarter` FAILS (rows were written). Restore.

- [ ] **Step 7: Run the four checks and commit**

```bash
uv run ruff check . && uv run ruff format --check . && uv run mypy && uv run pytest
git add src/sds/db/__init__.py src/sds/ingest/form345.py tests/test_db.py tests/test_form345.py
git commit -m "Gate and write each Form 345 quarter as one run; insert_frame returns rows written"
```

---

### Task 5: Link amendments

**Files:**
- Create: `src/sds/insiders.py`
- Test: `tests/test_insiders.py`

**Interfaces:**
- Consumes: `parse_zip`, `write_frames` (Tasks 3–4); `asof.insider_txns_as_of`, `asof.insider_owners_as_of` (Task 1); fixture builders (Task 3).
- Produces: `sds.insiders.link_amendments(conn) -> dict[str, int]` (count of 4/A filings per `amendment_match` value).

- [ ] **Step 1: Write the failing tests**

Create `tests/test_insiders.py`:

```python
from __future__ import annotations

from datetime import UTC, datetime

import duckdb
from form345_fixture import Row, build_zip, line, owner, submission
from polars.testing import assert_frame_equal

from sds import asof
from sds.ingest.form345 import parse_zip, write_frames
from sds.insiders import link_amendments


def load(conn: duckdb.DuckDBPyConnection, raw: bytes) -> None:
    write_frames(conn, parse_zip(raw, forms=["4", "4/A"], txn_codes=["P", "S"]), "h")


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
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `uv run pytest tests/test_insiders.py -q`
Expected: FAIL — `ModuleNotFoundError: No module named 'sds.insiders'`.

- [ ] **Step 3: Write `src/sds/insiders.py`**

```python
"""Insider-filing logic that does not depend on the source (flat files now, XML later)."""

from __future__ import annotations

import duckdb

# A 4/A names its original only by filing date. Match on issuer, the exact set of
# reporting owners and that date; the period of report breaks ties.
_LINK_SQL = """
CREATE OR REPLACE TEMP TABLE _owner_sets AS
SELECT accession_no, string_agg(owner_cik, ',' ORDER BY owner_cik) AS owners
FROM insider_owners GROUP BY accession_no;

CREATE OR REPLACE TEMP TABLE _candidates AS
SELECT a.accession_no AS amendment, o.accession_no AS original,
       o.period_of_report = a.period_of_report AS same_period
FROM insider_filings a
JOIN _owner_sets ao ON ao.accession_no = a.accession_no
JOIN insider_filings o
  ON o.form = '4' AND o.issuer_cik = a.issuer_cik AND o.filing_date = a.date_of_orig_sub
JOIN _owner_sets oo ON oo.accession_no = o.accession_no AND oo.owners = ao.owners
WHERE a.form = '4/A';

-- Tie-break: when some candidates share the amendment's period, keep only those.
CREATE OR REPLACE TEMP TABLE _best AS
SELECT amendment, original FROM _candidates c
WHERE same_period OR NOT EXISTS (
    SELECT 1 FROM _candidates c2 WHERE c2.amendment = c.amendment AND c2.same_period
);

UPDATE insider_filings SET superseded_by = NULL, amendment_match = NULL
WHERE superseded_by IS NOT NULL OR amendment_match IS NOT NULL;

UPDATE insider_filings f
SET amendment_match = CASE m.n WHEN 0 THEN 'none' WHEN 1 THEN 'one' ELSE 'many' END
FROM (
    SELECT a.accession_no, count(b.original) AS n
    FROM insider_filings a LEFT JOIN _best b ON b.amendment = a.accession_no
    WHERE a.form = '4/A' GROUP BY a.accession_no
) m
WHERE f.accession_no = m.accession_no;

-- Amendments of one original form a chain in knowledge-time order:
-- original -> first amendment -> second amendment.
CREATE OR REPLACE TEMP TABLE _chain AS
SELECT b.original, b.amendment,
       row_number() OVER (PARTITION BY b.original ORDER BY f.known_at, f.accession_no) AS k
FROM _best b JOIN insider_filings f ON f.accession_no = b.amendment
WHERE f.amendment_match = 'one';

UPDATE insider_filings f SET superseded_by = l.next_accession
FROM (
    SELECT original AS prev_accession, amendment AS next_accession FROM _chain WHERE k = 1
    UNION ALL
    SELECT c1.amendment, c2.amendment
    FROM _chain c1 JOIN _chain c2 ON c2.original = c1.original AND c2.k = c1.k + 1
) l
WHERE f.accession_no = l.prev_accession;
"""


def link_amendments(conn: duckdb.DuckDBPyConnection) -> dict[str, int]:
    """Tie each 4/A to its original and chain repeat amendments.

    Recomputed in full, so it is safe after any load in any quarter order. Setting a
    link updates an old row, but the as-of reads gate it on the amendment's own
    known_at, so no past view changes. Returns the count of 4/A filings per match
    result ('one', 'none', 'many').
    """
    conn.execute("BEGIN")
    try:
        conn.execute(_LINK_SQL)
        conn.execute("COMMIT")
    except Exception:
        conn.execute("ROLLBACK")
        raise
    rows = conn.execute(
        "SELECT amendment_match, count(*) FROM insider_filings WHERE form = '4/A' GROUP BY 1"
    ).fetchall()
    return {str(match): int(n) for match, n in rows}
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `uv run pytest -q`
Expected: all pass. If DuckDB rejects the multi-statement string inside the transaction, split `_LINK_SQL` on `";\n\n"` and execute each statement in turn inside the same `BEGIN`/`COMMIT`; rerun.

- [ ] **Step 5: Prove the tests can fail**

In `_LINK_SQL`, delete `AND oo.owners = ao.owners`. Run `uv run pytest tests/test_insiders.py -q`. Expected: `test_owner_set_must_match` FAILS. Restore. Then in `src/sds/asof.py` delete ` AND nxt.known_at <= $as_of`; expected: `test_past_view_unchanged_by_linking_and_later_quarters` FAILS. Restore.

- [ ] **Step 6: Run the four checks and commit**

```bash
uv run ruff check . && uv run ruff format --check . && uv run mypy && uv run pytest
git add src/sds/insiders.py tests/test_insiders.py
git commit -m "Link 4/A amendments to originals in one recomputed SQL pass"
```

---

### Task 6: Discover, fetch, load all quarters; CLI

**Files:**
- Modify: `src/sds/ingest/form345.py` (add `discover`, `LoadReport`, `load_insiders`)
- Modify: `src/sds/cli.py` (add the `insiders` command group)
- Test: `tests/test_form345.py`, `tests/test_cli.py`

**Interfaces:**
- Consumes: `load_quarter`, `SOURCE` (Task 4); `link_amendments` (Task 5); `quarter_key`, `Form345Config`, `load_ingest_config`, `DEFAULT_INGEST_CONFIG` (Task 2); `PoliteClient`, `RawStore`.
- Produces: `DATA_SETS_URL: str`; `discover(page: bytes) -> dict[str, str]`; `LoadReport(loaded: list[str], skipped: list[str], failed: dict[str, str], amendments: dict[str, int])`; `load_insiders(conn, store, client, cfg, *, since: str | None = None, only: str | None = None, refetch: bool = False) -> LoadReport`; CLI `sds insiders load [--since] [--quarter] [--refetch]` and `sds insiders link`.

- [ ] **Step 1: Write the failing tests**

In `tests/test_form345.py`, extend the imports:

```python
from pathlib import Path

import httpx

from sds.ingest.form345 import discover, load_insiders
from sds.ingest.http import PoliteClient
from sds.ingest.raw_store import RawStore
```

and add:

```python
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
```

In `tests/test_cli.py`, add (with `from sds.ingest.form345 import LoadReport`):

```python
def test_insiders_load_exits_nonzero_when_a_quarter_fails(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("SDS_DATA_DIR", str(tmp_path))
    monkeypatch.setenv("SDS_USER_AGENT", "t t@example.com")
    report = LoadReport(loaded=["2012q2"], failed={"2012q1": "HTTPStatusError: HTTP 404"})
    monkeypatch.setattr("sds.cli.load_insiders", lambda *args, **kwargs: report)

    result = CliRunner().invoke(app, ["insiders", "load"])

    assert result.exit_code == 1
    assert "failed 2012q1" in result.output


def test_insiders_link_reports_counts(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("SDS_DATA_DIR", str(tmp_path))
    result = CliRunner().invoke(app, ["insiders", "link"])
    assert result.exit_code == 0, result.output
    assert "amendments:" in result.output
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `uv run pytest tests/test_form345.py tests/test_cli.py -q`
Expected: FAIL — `discover`, `load_insiders` and `LoadReport` cannot be imported.

- [ ] **Step 3: Add discovery and the multi-quarter load**

In `src/sds/ingest/form345.py`, add to the imports:

```python
import re
from dataclasses import field
from urllib.parse import urljoin

from sds.ingest.config import quarter_key
from sds.ingest.http import PoliteClient
from sds.ingest.raw_store import RawStore
from sds.insiders import link_amendments
from sds.timeutil import utc_now
```

(merge `field` into the existing `from dataclasses import dataclass` line) and append:

```python
DATA_SETS_URL = (
    "https://www.sec.gov/data-research/sec-markets-data/insider-transactions-data-sets"
)
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
                "SELECT raw_hash FROM raw_documents WHERE uri = ?"
                " ORDER BY fetched_at DESC LIMIT 1",
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
```

- [ ] **Step 4: Add the CLI commands**

In `src/sds/cli.py`, add imports:

```python
from sds.ingest.config import DEFAULT_INGEST_CONFIG, load_ingest_config
from sds.ingest.form345 import load_insiders
from sds.insiders import link_amendments
```

register the group next to the others:

```python
insiders_app = typer.Typer(no_args_is_help=True, help="Insider (Form 4) commands.")
app.add_typer(insiders_app, name="insiders")
```

and add the commands below `prices_load`:

```python
def _amendment_line(counts: dict[str, int]) -> str:
    return "amendments: " + (", ".join(f"{k}={v}" for k, v in sorted(counts.items())) or "none")


@insiders_app.command("load")
def insiders_load(
    since: str | None = typer.Option(None, help="First quarter, e.g. 2012q1 (default: config)."),
    quarter: str | None = typer.Option(None, help="Load only this quarter, e.g. 2026q2."),
    refetch: bool = typer.Option(False, help="Fetch zips again even if stored."),
    config: Path = DEFAULT_INGEST_CONFIG,
) -> None:
    """Load SEC quarterly Form 4 flat files, then link amendments."""
    settings = Settings.from_env()
    cfg = load_ingest_config(config).form345
    conn = connect(settings.db_path)
    client = PoliteClient(settings.require_user_agent(), SEC_RATE_PER_SECOND)
    try:
        report = load_insiders(
            conn,
            RawStore(settings.raw_dir, conn),
            client,
            cfg,
            since=since,
            only=quarter,
            refetch=refetch,
        )
    finally:
        client.close()
    typer.echo(
        f"quarters loaded {len(report.loaded)}, skipped {len(report.skipped)}, "
        f"failed {len(report.failed)}"
    )
    typer.echo(_amendment_line(report.amendments))
    for failed_quarter, error in report.failed.items():
        typer.echo(f"  failed {failed_quarter}: {error}", err=True)
    if report.failed:
        raise typer.Exit(1)


@insiders_app.command("link")
def insiders_link() -> None:
    """Recompute amendment links over all loaded filings."""
    typer.echo(_amendment_line(link_amendments(connect(Settings.from_env().db_path))))
```

- [ ] **Step 5: Run the tests to verify they pass**

Run: `uv run pytest -q`
Expected: all pass.

- [ ] **Step 6: Prove the tests can fail**

In `_raw_source.get`, change `if not refetch:` to `if False:`. Run `uv run pytest tests/test_form345.py -q`. Expected: `test_failed_quarter_is_reparsed_from_store_without_fetch` FAILS (the zip was fetched). Restore. Then in `load_insiders` change `quarter_key(q) >= first` to `quarter_key(q) > first`; expected: `test_load_respects_start_quarter_and_skips_loaded` FAILS (2012q1 missing). Restore.

- [ ] **Step 7: Run the four checks and commit**

```bash
uv run ruff check . && uv run ruff format --check . && uv run mypy && uv run pytest
git add src/sds/ingest/form345.py src/sds/cli.py tests/test_form345.py tests/test_cli.py
git commit -m "Add sds insiders load/link: discover SEC quarters, load each as a run, link amendments"
```

---

### Task 7: Live check, backfill, docs

**Files:**
- Modify: `CLAUDE.md` (Status, Commands), `README.md` (Setup)

**Interfaces:**
- Consumes: the CLI from Task 6.
- Produces: a loaded `insider_*` dataset and updated docs.

- [ ] **Step 1: Load one quarter live**

Run: `set -a; . ./.env; set +a; uv run sds insiders load --quarter 2026q2`
Expected: `quarters loaded 1, skipped 0, failed 0` and an `amendments:` line with `one`, `many`, `none` counts (2026q2 alone gave one=416, many=8, none=289 in the design check).

- [ ] **Step 2: Compare against an independent count**

Run this; it reads the stored zip with DuckDB only, not with `parse_zip`:

```bash
uv run python - <<'EOF'
import duckdb, tempfile, zipfile
c = duckdb.connect("data/sds.duckdb", read_only=True)
path = c.execute("select path from raw_documents where uri like '%2026q2_form345.zip'").fetchone()[0]
d = tempfile.mkdtemp(); zipfile.ZipFile(f"data/raw/{path}").extractall(d)
t = lambda n: f"read_csv('{d}/{n}.tsv', delim='\t', header=true, all_varchar=true, quote='')"
m = duckdb.connect()
subs = f"(select ACCESSION_NUMBER from {t('SUBMISSION')} where DOCUMENT_TYPE in ('4','4/A'))"
print("filings", m.sql(f"select count(*) from {subs}").fetchone(),
      c.execute("select count(*) from insider_filings").fetchone())
print("owners", m.sql(f"select count(*) from {t('REPORTINGOWNER')} where ACCESSION_NUMBER in {subs}").fetchone(),
      c.execute("select count(*) from insider_owners").fetchone())
print("txns", m.sql(f"select count(*) from {t('NONDERIV_TRANS')} where TRANS_CODE in ('P','S') and ACCESSION_NUMBER in {subs}").fetchone(),
      c.execute("select count(*) from insider_txns").fetchone())
EOF
```

Expected: each pair is equal (design check: 50,545 / 54,062 / 32,909), and the run shows 0 quarantined. If a pair differs, stop and find out why before Step 3.

- [ ] **Step 3: Backfill from 2012q1**

Run (background; about 58 zips, ~700 MB): `set -a; . ./.env; set +a; uv run sds insiders load`
Expected: `failed 0`. Then `uv run sds health` shows `insider_filings` freshness at 2026-07-01 02:00 UTC or later (the last 2026q2 filing date at 22:00 New York), and one `form345.<quarter>` ok run per quarter.

- [ ] **Step 4: Check the amendment counts after the backfill**

Run: `uv run sds insiders link`
Expected: the `none` share of amendments drops well below the single-quarter 41% (most cross-quarter originals are now loaded). Record the final `one`/`none`/`many` counts for the verdict.

- [ ] **Step 5: Update the docs**

In `CLAUDE.md`, Status: replace Phase 1 step 1 and 2 with a done line giving the quarter range, row counts and amendment counts from Steps 3–4. In Commands, add `insiders load [--since 2012q1] [--quarter 2026q2] [--refetch] | insiders link`. In `README.md`, Setup, add `uv run sds insiders load   # SEC Form 4 flat files, 2012q1 onward`.

- [ ] **Step 6: Run the four checks and commit**

```bash
uv run ruff check . && uv run ruff format --check . && uv run mypy && uv run pytest
git add CLAUDE.md README.md
git commit -m "Form 4 backfill loaded: update status and commands"
```
