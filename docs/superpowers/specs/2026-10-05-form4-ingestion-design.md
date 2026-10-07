# Form 4 ingestion from the SEC quarterly flat files — design

Date: 2026-10-05. Status: approved in conversation; awaiting review of this file.

## Purpose

Load enough Form 4 data to run pre-registration 001
(`docs/preregistration/001-insider-buys.md`) exactly as written: open-market
purchases and sales (codes P and S), owner roles, the 10b5-1 flag and amendments
linked to their originals, from 2012 to the latest published quarter.

Success:

- A re-parse of the raw store reproduces the tables, with no network.
- Each quarter passes the 1% quarantine gate.
- `as_of` reads return what was public at that moment, and a past view never changes
  when later data or amendment links arrive.

## Scope

In: the SEC Insider Transactions Data Sets (quarterly `*_form345.zip`), forms 4 and
4/A, non-derivative lines with codes P and S, 2012q1 onward.

Out (deferred, each a later design): per-filing XML for filings newer than the last
quarterly file; derivative lines, holdings and footnotes; Forms 3 and 5. The raw store
keeps every zip whole, so each of these is a re-parse, not a refetch.

## Facts checked against real data (2026q2_form345.zip, 2026-10-05)

- 82 quarterly zips exist, 2006q1–2026q2. 81 sit under `/files/structureddata/`, the
  newest under `/files/datastandardsinnovation/`. URLs are read from the page, never
  built from a pattern.
- `SUBMISSION.FILING_DATE` is a date (DD-MON-YYYY). No acceptance time exists.
- `SUBMISSION.AFF10B5ONE` exists in 2026q2 (the PDF readme omits it). Values seen:
  `true`, `false`, `1`, `0`, NULL. The flag is per filing. The column is absent from
  2012q1 and 2019q1 (checked 2026-10-06), so it is optional: a file without it
  gives NULL.
- 2012q1, 2019q1 and 2026q2 all decode as strict UTF-8.
- One filing can have several reporting owners: 585 of 5,326 P lines (11%) sit in
  filings with 2+ owners.
- `RPTOWNER_RELATIONSHIP` is a comma list of `Director`, `Officer`,
  `TenPercentOwner`, `Other`.
- A 4/A carries `DATE_OF_ORIG_SUB` but not the original's accession number. Of the
  443 amendments whose original was filed in the same quarter, a match on (issuer,
  owner set, original filing date) found exactly one original for 414, several for
  10 (2 resolved by `PERIOD_OF_REPORT`) and none for 19. The other 270 amendments
  refer to earlier quarters, so linking runs over all loaded filings, not per file.

## Decisions

| Decision | Choice | Reason |
|---|---|---|
| Schema shape | Three normalized tables (approach A) | One owner per fact; joint-filing shares stored once |
| Parse engine | polars, all-text read, vectorized casts | No per-row Python; failed casts become quarantine rows |
| `known_at` | `filing_date` at 22:00 America/New_York | EDGAR cut-off; late by up to a day, never early |
| Unit of work | One `run_job`, gate and transaction per quarter | A bad quarter fails alone |
| Conflict policy | `ON CONFLICT DO NOTHING` | A filed fact does not change. `--refetch` adds filings new to a republished quarter; it never overwrites a stored row. Detecting changed rows is deferred |
| Amendment with no match or several matches | Excluded from events; originals kept; counted | Decided before data; no double counting |
| Transaction codes | P and S (config) | S is needed for the routine/opportunistic classification |

## Schema — migration `0002_insider_normalized.sql`

Drops the empty `insider_txns` from 0001 and creates:

```sql
CREATE TABLE insider_filings (
    accession_no     VARCHAR PRIMARY KEY,
    issuer_cik       VARCHAR NOT NULL,
    form             VARCHAR NOT NULL,          -- '4' or '4/A'
    filing_date      DATE NOT NULL,
    period_of_report DATE NOT NULL,
    date_of_orig_sub DATE,                      -- amendments only
    is_10b5_1        BOOLEAN,                   -- NULL: not reported (pre-2023 form)
    known_at         TIMESTAMPTZ NOT NULL,      -- filing_date 22:00 New York
    superseded_by    VARCHAR,                   -- set by link_amendments
    amendment_match  VARCHAR CHECK (amendment_match IN ('one', 'none', 'many')),
    raw_hash         VARCHAR NOT NULL,
    ingested_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE insider_owners (
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

CREATE TABLE insider_txns (
    accession_no       VARCHAR NOT NULL,
    line_id            VARCHAR NOT NULL,        -- SEC NONDERIV_TRANS_SK
    txn_code           VARCHAR NOT NULL,
    txn_date           DATE NOT NULL,
    shares             DOUBLE NOT NULL,
    price              DOUBLE,
    acquired_disposed  VARCHAR,                 -- 'A' or 'D'
    shares_owned_after DOUBLE,
    direct_indirect    VARCHAR,                 -- 'D' or 'I'
    PRIMARY KEY (accession_no, line_id)
);
```

No foreign keys (project decision); tests enforce integrity. `amendment_match` is NULL
for form 4 and set for every 4/A by the link pass, so the verdict's amendment counts
are a query.

Roles come from each filing, never from a stored current role: a director today may
have been the CFO in 2016.

## Point-in-time reads — `src/sds/asof.py`

`insider_txns_as_of(conn, as_of, issuer_ciks=None)` replaces the current function. It
returns one row per line joined to its filing, where:

- the filing has `known_at <= as_of`;
- a 4/A with `amendment_match` other than `'one'` is excluded;
- a filing is hidden once the filing named in its `superseded_by` is visible at
  `as_of`.

`insider_owners_as_of(conn, as_of, issuer_ciks=None)` returns the owners of the
visible filings, one row per (filing, owner). Shares are never repeated per owner.

The existing amendment test in `tests/test_asof.py` moves to the new tables and keeps
its contract.

## Pipeline — `src/sds/ingest/form345.py`

1. **Discover.** Fetch the data-set page once; read the `*_form345.zip` links; keep
   quarters from `start_quarter`. No links found: stop with an error.
2. **Fetch.** Each zip goes to the raw store (`source = sec/form345`) through
   `PoliteClient` with `SDS_USER_AGENT`, under 10 requests per second. Without
   `--refetch`: a quarter with an `ok` run (`job = form345.<quarter>`) is skipped; a
   quarter whose zip is in the raw store but has no `ok` run is re-parsed from the
   store with no fetch (a failed gate must not strand a quarter). With `--refetch`,
   the zip is fetched again.
3. **Parse** (`parse_zip(raw: bytes) -> InsiderFrames`, no network). Read
   `SUBMISSION`, `REPORTINGOWNER` and `NONDERIV_TRANS` as text (tab separated, no
   quote character). Keep forms and codes from config. Lines and owners of a filtered
   form are out of scope, not errors. Column names map to ours through one table in
   the module (data, not branches).
4. **Validate** (vectorized). Cast dates (`%d-%b-%Y`), numbers and the 10b5-1 flag
   (`true`/`1` → true, `false`/`0` → false, NULL → NULL, anything else → quarantine).
   Split the relationship list into four flags. A failed cast quarantines the row with
   the column and the value. A line or owner whose accession is missing from
   `SUBMISSION` is quarantined. A required column missing from a header fails the
   quarter with the column name; an optional column (only `AFF10B5ONE`) missing
   gives NULL; extra columns are ignored. Files are decoded as strict UTF-8, so a
   bad byte fails the quarter rather than altering text.
5. **Gate and write.** One `run_job` per quarter (`job = form345.<quarter>`). Enforce
   the 1% gate on that quarter's rows, then write the three frames in one transaction
   with `insert_frame(..., "ON CONFLICT DO NOTHING")`.
6. **Link** (`link_amendments(conn)` in `src/sds/insiders.py`, source-independent).
   One SQL pass over all 4/A filings, recomputed in full each run:
   - match key: same issuer, same sorted owner-CIK set, `form = '4'`,
     `filing_date = date_of_orig_sub`; `period_of_report` breaks ties;
   - set `amendment_match` to `one` (exactly one candidate after the tie-break),
     `none` (no candidate) or `many` (still several after the tie-break);
   - for matches of `one`, order the amendments of one original by `known_at`
     (accession as tie-break) and chain them: original → A1 → A2, through
     `superseded_by`.

   Setting `superseded_by` updates an existing row, but the as-of read gates it on the
   amendment's own `known_at`, so no past view changes. A test proves this.

## Config — `config/ingest.yaml`

```yaml
form345:
  start_quarter: 2012q1
  forms: ["4", "4/A"]
  txn_codes: [P, S]
```

Validated by a pydantic model with `extra="forbid"`. `sectors.yaml` stays "what to
study"; this file is "what to load".

## CLI

- `sds insiders load [--since 2012q1] [--quarter 2026q2] [--refetch]`: discover,
  fetch, parse, write per quarter, then link. Lists failed quarters and exits non-zero
  if any failed.
- `sds insiders link`: the link pass alone.
- `sds health` reads freshness from `insider_filings.known_at`.

## Errors

| Case | Behavior |
|---|---|
| Network failure for a quarter | That quarter's run (the fetch is inside it) fails; other quarters continue; non-zero exit |
| Gate exceeded | That quarter writes nothing; run fails |
| Required column missing | That quarter fails with the column name |
| No zip links on the page | The command stops |
| HTTP error text | Already redacted by `PoliteClient`; no secrets in this source anyway |

## Testing

A test helper builds a small zip with the real 2026q2 column headers, holding: a
joint filing with three owners; a 4/A with one match, one with none, one with several,
and a chain of two amendments; a bad date; all five 10b5-1 encodings; a Form 5 line.

1. `parse_zip` returns the expected frames; bad rows land in quarantine with reasons.
2. The gate blocks all writes for a quarter above 1%.
3. Loading the same zip twice writes nothing the second time.
4. Re-parse from the raw store makes no network call.
5. `link_amendments` sets `one`, `none` and `many` correctly and builds the chain.
6. Core contract: a past `as_of` view is identical before and after the link pass and
   after a newer quarter loads.
7. A joint filing's shares are counted once.
8. A missing required column fails the quarter by name.

Each new test is shown to fail against a deliberate mutation of the code it guards.

Live check: load 2026q2 alone and compare row counts with direct DuckDB counts over the
TSVs; then backfill from 2012q1 (58 zips, about 700 MB raw).

## Extendability

1. **Next change:** the per-filing XML source for recent filings. It produces the same
   `InsiderFrames` and reuses validation, writing and `link_amendments`. The
   routine/opportunistic classification is research code over the as-of reads.
2. **Forecloses:** nothing durable. Derivative lines, holdings, footnotes and other
   forms are a re-parse of zips already stored.
3. **Data, not code:** forms, codes and the start quarter are config; the column map
   is a table. A wider load is a config edit.

## Known debt this design does not fix

- The 1% gate counts rows, so a whole quarter that fails to fetch is visible only as
  a failed run, not in any row rate. Per-quarter runs make this visible in `health`.
- Recent filings (after the last quarterly file) are absent until the XML design.
