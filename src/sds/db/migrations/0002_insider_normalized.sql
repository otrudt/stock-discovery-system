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
