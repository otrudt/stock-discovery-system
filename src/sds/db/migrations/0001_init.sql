-- 0001: initial bitemporal schema.
--
-- Two kinds of time appear throughout:
--   * business time  — when something happened or applies (event_date, period_end, valid_from)
--   * knowledge time — when the system could first have known it (known_at, recorded_at)
-- Every read for research or scoring filters on knowledge time. That one rule is the
-- guard against look-ahead bias.

-- No FOREIGN KEY constraints: DuckDB treats an update to a referenced row as a
-- delete plus insert and rejects it, which breaks upserts. Referential integrity is
-- enforced in code and checked by tests instead.

CREATE SEQUENCE IF NOT EXISTS security_id_seq START 1;

-- One row per listed security. CIK identifies the issuer; share_class separates
-- issuers with several listed classes (e.g. A and C shares).
CREATE TABLE IF NOT EXISTS securities (
    security_id               BIGINT PRIMARY KEY DEFAULT nextval('security_id_seq'),
    cik                       VARCHAR NOT NULL,          -- 10-digit, zero-padded
    share_class               VARCHAR NOT NULL DEFAULT '',
    name                      VARCHAR NOT NULL,
    is_foreign_private_issuer BOOLEAN NOT NULL DEFAULT FALSE,  -- no Form 4 filings
    created_at                TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (cik, share_class)
);

-- Tickers change; CIK does not. Resolve a ticker on a date through this table.
CREATE TABLE IF NOT EXISTS ticker_history (
    security_id BIGINT NOT NULL,
    ticker      VARCHAR NOT NULL,
    valid_from  DATE NOT NULL,
    valid_to    DATE,                                    -- exclusive; NULL = current
    PRIMARY KEY (security_id, valid_from)
);

-- Universe membership is bitemporal too.
--   valid_from / valid_to : the period the name belongs to the family (business time)
--   recorded_at           : when that membership decision was made (knowledge time)
--   basis                 : 'curated' (hand-picked) or 'rule' (derived from point-in-time data)
-- A curated list made today about the past is survivorship-biased by construction;
-- strict as-of reads exclude it (see sds.asof.universe_as_of).
CREATE TABLE IF NOT EXISTS universe_membership (
    security_id BIGINT NOT NULL,
    family      VARCHAR NOT NULL,
    layer       VARCHAR NOT NULL,
    valid_from  DATE NOT NULL,
    valid_to    DATE,
    basis       VARCHAR NOT NULL CHECK (basis IN ('curated', 'rule')),
    rationale   VARCHAR NOT NULL,
    recorded_at TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (security_id, family, valid_from)
);

-- Form 4 transaction lines. Amendments (4/A) supersede originals.
CREATE TABLE IF NOT EXISTS insider_txns (
    accession_no             VARCHAR NOT NULL,
    line_no                  INTEGER NOT NULL,
    issuer_cik               VARCHAR NOT NULL,
    insider_cik              VARCHAR NOT NULL,
    insider_name             VARCHAR,
    is_director              BOOLEAN NOT NULL,
    is_officer               BOOLEAN NOT NULL,
    is_ten_pct_owner         BOOLEAN NOT NULL,
    officer_title            VARCHAR,
    txn_code                 VARCHAR NOT NULL,          -- 'P' = open-market purchase
    shares                   DOUBLE NOT NULL,
    price                    DOUBLE,
    shares_owned_after       DOUBLE,
    is_10b5_1                BOOLEAN,
    event_date               DATE NOT NULL,             -- transaction date
    known_at                 TIMESTAMPTZ NOT NULL,      -- filing acceptance time
    superseded_by            VARCHAR,                   -- accession of an amendment
    ingested_at              TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (accession_no, line_no)
);

-- Congressional periodic transaction reports. Amounts are disclosed as ranges.
CREATE TABLE IF NOT EXISTS congress_txns (
    filing_id   VARCHAR NOT NULL,
    line_no     INTEGER NOT NULL,
    member_id   VARCHAR NOT NULL,
    chamber     VARCHAR NOT NULL CHECK (chamber IN ('house', 'senate')),
    owner       VARCHAR,                                -- self, spouse, dependent, joint
    ticker      VARCHAR,
    asset_name  VARCHAR NOT NULL,
    txn_type    VARCHAR NOT NULL,                       -- purchase, sale, exchange
    amount_low  DOUBLE,
    amount_high DOUBLE,
    event_date  DATE NOT NULL,
    known_at    TIMESTAMPTZ NOT NULL,
    ingested_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (filing_id, line_no)
);

CREATE TABLE IF NOT EXISTS committee_assignments (
    member_id  VARCHAR NOT NULL,
    committee  VARCHAR NOT NULL,
    valid_from DATE NOT NULL,
    valid_to   DATE,
    PRIMARY KEY (member_id, committee, valid_from)
);

-- XBRL facts. Every filed version is kept, so a restatement adds a row instead of
-- overwriting history.
CREATE TABLE IF NOT EXISTS fundamentals (
    cik          VARCHAR NOT NULL,
    concept      VARCHAR NOT NULL,                      -- e.g. us-gaap:Revenues
    unit         VARCHAR NOT NULL,
    period_start DATE,                                  -- NULL for point-in-time facts
    period_end   DATE NOT NULL,
    value        DOUBLE NOT NULL,
    accession_no VARCHAR NOT NULL,
    form         VARCHAR NOT NULL,
    known_at     TIMESTAMPTZ NOT NULL,                  -- filing date of this version
    ingested_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (cik, concept, unit, period_end, accession_no)
);

-- Daily bars. `close` is the unadjusted print and never changes. `adj_close` reflects
-- splits and dividends as of the latest fetch; ratios of adjusted closes give correct
-- returns, so this is safe for return math but not for reconstructing quoted prices.
CREATE TABLE IF NOT EXISTS prices_daily (
    security_id BIGINT NOT NULL,
    date        DATE NOT NULL,
    open        DOUBLE,
    high        DOUBLE,
    low         DOUBLE,
    close       DOUBLE NOT NULL,
    adj_close   DOUBLE,
    volume      DOUBLE,
    source      VARCHAR NOT NULL,
    known_at    TIMESTAMPTZ NOT NULL,                   -- that session's close
    ingested_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (security_id, date, source)
);

-- Computed signal values, versioned so old research stays reproducible.
CREATE TABLE IF NOT EXISTS signals (
    security_id    BIGINT NOT NULL,
    signal         VARCHAR NOT NULL,
    signal_version VARCHAR NOT NULL,
    as_of          DATE NOT NULL,
    value          DOUBLE,
    computed_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (security_id, signal, signal_version, as_of)
);

-- Every file fetched from outside, keyed by content hash.
CREATE TABLE IF NOT EXISTS raw_documents (
    raw_hash   VARCHAR PRIMARY KEY,                     -- sha256 of the bytes
    source     VARCHAR NOT NULL,
    uri        VARCHAR NOT NULL,
    path       VARCHAR NOT NULL,                        -- relative to the raw dir
    n_bytes    BIGINT NOT NULL,
    fetched_at TIMESTAMPTZ NOT NULL
);

-- One row per job run; feeds the data-health report.
CREATE TABLE IF NOT EXISTS runs (
    run_id           VARCHAR PRIMARY KEY,
    job              VARCHAR NOT NULL,
    started_at       TIMESTAMPTZ NOT NULL,
    finished_at      TIMESTAMPTZ,
    status           VARCHAR NOT NULL CHECK (status IN ('running', 'ok', 'failed')),
    rows_in          BIGINT NOT NULL DEFAULT 0,
    rows_written     BIGINT NOT NULL DEFAULT 0,
    rows_quarantined BIGINT NOT NULL DEFAULT 0,
    error            VARCHAR
);

-- Rows a parser could not accept. Nothing here reaches research tables.
CREATE TABLE IF NOT EXISTS quarantine (
    run_id     VARCHAR NOT NULL,
    source     VARCHAR NOT NULL,
    raw_hash   VARCHAR,
    reason     VARCHAR NOT NULL,
    payload    VARCHAR,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
