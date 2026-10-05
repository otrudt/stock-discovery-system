# CLAUDE.md

Context for Claude Code sessions in this repo. Read this before changing anything.

## What this is

A personal research tool. It finds stocks worth researching for a 6–18 month hold in
five sector families: AI compute, electronic components, robotics and automation,
pharmaceuticals, and bioscience. A small-cap tier cuts across all five. It runs locally
for one user. It is not a service, and it does not place trades.

The design doc covers the reasoning, signals, validation plan and roadmap:
https://claude.ai/code/artifact/19830f68-f1d2-4037-b20c-2fecebdf57c8

## The rule everything follows

No signal ranks anything until it has passed an event study on data it could not
have seen. If a change could let future information leak into a past view, stop and
say so. Look-ahead bugs make results look great and mean nothing.

## Invariants (do not break)

1. Every fact stores business time (`event_date`, `period_end`, `valid_from`) and
   knowledge time (`known_at`, `recorded_at`). Never overwrite history; add a row.
2. Research and signal code reads only through `src/sds/asof.py`.
3. `as_of` is a timezone-aware UTC timestamp. Bare dates are refused on purpose.
4. A signal's event date is the first tradable moment after `known_at`, never the
   transaction date.
5. Curated seeds in `config/sectors.yaml` carry hindsight. Backtests use rule-based
   membership (SIC codes, point-in-time market cap). Strict `universe_as_of` excludes
   curated rows recorded after `as_of`.
6. Fetch to the raw store first, parse second. A parser fix is a re-parse, not a refetch.
7. Ingestion writes to research tables only after the 1% quarantine gate passes.
8. SEC requests send `SDS_USER_AGENT` and stay under 10 requests per second.
9. Secrets (API tokens) never reach the raw store, logs or git.

## Commands

```bash
uv sync --all-groups
uv run ruff check . && uv run ruff format --check . && uv run mypy && uv run pytest

uv run sds init | universe check | universe seed | prices load | health
```

Run all four checks before every commit. CI runs the same ones.

## Decisions already made (with reasons)

- **Python, not Rust.** The work waits on the network and the SEC rate limit, the data
  is small, and the statistics and LLM tooling are in Python. Revisit only if
  profiling finds a CPU-bound hot path, and then only as a PyO3 module for that path.
- **DuckDB and cron, not Postgres or Airflow.** One user, analytical queries, a few
  daily jobs.
- **No database foreign keys.** DuckDB rejects upserts on rows other tables reference.
  Integrity is enforced in code and tests.
- **Weighted z-scores of validated signals, not ML**, until there are 3+ validated
  signals and enough events to hold out.
- **Benchmark:** the equal-weight point-in-time universe, per family. SMH and similar
  ETFs are secondary checks.
- **Free data first** (SEC EDGAR, Tiingo free tier), with $50/month as the ceiling.
  Pay for point-in-time data only once a validated signal proves it needs it.
- **Small-cap tier defaults:** $300M–$2B market cap, $1M median daily dollar volume
  over 63 trading days. These are config values.
- **Momentum is a feature, not a separate system.** One horizon: 6–18 months.

## Signals and the hypotheses to test

- **Insider buys (Form 4, code P):** opportunistic, not routine (Cohen, Malloy and
  Pomorski 2012). Test separately by role (CEO, CFO, other officers, directors, 10%
  owners) and by size (whether the buy raises the insider's holding by 10% or more).
  Some research finds CFO buys more informative than CEO buys. Ignore sales; most are
  10b5-1 plans.
- **Congressional buys in the member's committee jurisdiction.** Disclosures lag up to
  45 days and give amounts as ranges. A purchase ban passed the House in July 2026 and
  failed in the Senate on Sept 30, 2026. Keep the signal definition configurable.
- **Fundamental acceleration** (XBRL), **12-1 month momentum**, **hyperscaler capex**.
- **Bioscience:** cash runway, dilution flags (S-3, 424B), and catalysts as risk flags.
  Never predict trial outcomes.
- **Sentiment is deferred** until there is a specific, testable hypothesis.
- **Statistics:** cluster standard errors by date, use t > 3, pre-register each
  hypothesis in the design doc, and treat the pre-2023 and 2023+ samples as different
  regimes for AI compute.

## Status

- **Phase 0 done:** schema, as-of reads, sector config, raw store, run log, Tiingo
  price loader, 20 tests.
- **Not yet verified against live sources.** The first real `universe seed` and
  `prices load` on a local machine is the check. Expect unresolved tickers (e.g.
  share classes, recent IPOs) and fix the config, not the code, where possible.
- **Next, Phase 1:**
  1. Form 4 ingestion. Backfill from the SEC Insider Transactions Data Sets
     (quarterly flat files); use per-filing XML for recent filings.
  2. Mark amendments (4/A) through `superseded_by`.
  3. Mark routine vs. opportunistic insiders.
  4. Run the first event study: excess return vs. the universe at 21, 63, 126 and 252
     trading days.
  5. Write down a verdict, pass or fail.

## Open questions

- Which free price source holds up long term (Tiingo vs. alternatives)?
- Where estimate revisions come from within budget, or whether to defer them.
- The committee-to-sector mapping table.
- How to rank names without insider data (foreign private issuers such as TSM, ASML,
  NVO).

## How to work with me

I'm using this project to learn and to prepare for senior and AI-engineering
interviews. Be a skeptical mentor:

- Ask why when I make a claim, and point out flawed assumptions.
- Prefer the right-sized solution and explain the tradeoff.
- Be direct and keep it terse.
- Put tests next to every change.
- Don't commit or push unless I ask.
