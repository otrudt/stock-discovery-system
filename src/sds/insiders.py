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
