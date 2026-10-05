# stock-discovery-system

Finds stocks worth researching in chosen sectors (AI compute, electronic components,
robotics, pharma, bioscience, plus a small-cap tier) for a 6–18 month hold.

The rule that shapes everything: **no signal ranks anything until it has been tested
against data it could not have seen.** The design doc covers the reasoning; this
README covers running it.

## Status

Phase 0 — foundations. Done:

- Bitemporal DuckDB schema: every fact records when it happened and when it became public.
- Point-in-time reads (`sds.asof`): the only way research code touches data.
- Sector config (`config/sectors.yaml`), validated, with curated seeds per family.
- Raw document store (content-addressed), run log, quarantine and a 1% quality gate.
- Daily price loader (Tiingo) that writes only after the quality gate passes.

Next: Form 4 insider ingestion and the first event study (Phase 1).

## Setup

```bash
uv sync --all-groups
cp .env.example .env        # set SDS_USER_AGENT and SDS_TIINGO_TOKEN
set -a; source .env; set +a

uv run sds init             # create data/sds.duckdb
uv run sds universe check   # validate the sector config
uv run sds universe seed    # resolve tickers to SEC CIKs
uv run sds prices load --start 2015-01-01
uv run sds health           # freshness per source, last run per job
```

Every command is safe to re-run.

## Layout

```
config/sectors.yaml          what to study: families, layers, seeds, small-cap tier
src/sds/db/migrations/       schema, applied in order on connect
src/sds/asof.py              point-in-time reads (the look-ahead guard)
src/sds/universe.py          config model and seeding
src/sds/ingest/              polite HTTP client, raw store, price loader
src/sds/runs.py              run log, quarantine, quality gate
tests/                       contract tests; test_asof.py is the one that matters most
```

## Rules for contributors (including future me)

1. Research and signal code reads through `sds.asof`, never raw tables.
2. `as_of` is a timezone-aware timestamp. A bare date is refused on purpose.
3. A signal's event date is the first tradable moment after `known_at`, never the
   transaction date.
4. Curated seeds carry hindsight. Backtests use rule-based membership; strict
   `universe_as_of` excludes curated rows recorded after `as_of`.
5. Parsers read from the raw store, so a parser fix is a re-parse, not a refetch.

## Known limits

- Ticker history starts at the date a ticker was first seen; historical renames need
  a dated source (planned).
- `adj_close` reflects corporate actions as of the latest fetch. Use it for return
  ratios only, never as a historical quote.
- Neither SEC nor Tiingo calls have been exercised against the live services from CI;
  fixtures mimic the documented formats.
