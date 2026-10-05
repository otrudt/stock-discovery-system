# Pre-registration 001: insider open-market purchases

Status: **Registered on 2026-10-05.** The commit that set this line is the
registration. After it, this file does not change. A change to any
rule below is a new pre-registration (002, ...) with its own date, and the result of
001 is still reported.

Written before any Form 4 data is loaded into this system.

## 1. Hypothesis

**H1 (primary).** Open-market purchases (Form 4, transaction code P) by
*opportunistic* insiders are followed by positive excess returns over the equal-weight
point-in-time universe of the issuer's family.

Basis: Cohen, Malloy and Pomorski (2012). Routine insiders trade on a calendar
pattern and carry no information; opportunistic insiders do.

**H0.** Mean excess return at the primary horizon is zero or negative.

## 2. Primary test (one test; it alone decides pass or fail)

- **Events:** opportunistic purchase events (section 4), all families pooled.
- **Horizon:** 126 trading days, the short end of the 6–18 month hold. More events
  have a full window than at 252, so the test has more power.
- **Statistic:** mean excess return, standard errors clustered by event date.
- **Pass:** t > 3 **and** the mean has the same sign in each half of the sample
  (split at the median event date). Anything else is a fail.

## 3. Secondary tests (reported, never decide pass or fail)

Each is reported with its t-statistic and event count, whatever the result.

1. Horizons 21, 63 and 252 trading days.
2. By role: CEO, CFO, other officer, director, 10% owner. An insider with two roles
   is counted once, in the first role in that list.
3. By size: the purchase raises the insider's holding by 10% or more, vs. less.
   Holding before = `shares_owned_after - shares`.
4. Routine vs. opportunistic (the contrast, not only the opportunistic leg).
5. AI compute only, pre-2023 vs. 2023+ (different regimes).
6. Small-cap tier vs. the rest.

With this many cuts, some will cross t > 3 by chance. A secondary result can
motivate a new pre-registration. It cannot rank stocks.

## 4. Definitions

- **Purchase line:** `txn_code = 'P'`, non-derivative, `shares > 0`, `price > 0`.
  Lines from filings flagged 10b5-1 are excluded (planned trades carry no timing
  choice). The flag is per filing: `AFF10B5ONE` in the flat files' SUBMISSION table,
  copied to each line's `is_10b5_1`. The Form 4 checkbox exists only from April 2023;
  earlier filings have no flag and stay in. Report the excluded count, so the rule's effect on the 2023+ sample
  is visible.
- **Amendments:** an amended line replaces its original only from the amendment's
  own `known_at` (`insider_txns_as_of` already does this).
- **Routine insider (CMP 2012):** traded in the same calendar month in each of the
  three preceding calendar years. Every other insider who traded in each of those
  three years is opportunistic. An insider without a trade in each of the three
  years is *unclassified* and excluded from H1 (reported as a count). As in CMP,
  classification uses all open-market trades (codes P and S), so sales are ingested
  even though the signal ignores them. This needs Form 4 data from 3 years before
  the first event: load from 2012.
- **Event:** one per (issuer, filing date). Several opportunistic insiders buying the
  same issuer on one filing date form one event; the number of insiders is recorded.
  A second event for the same issuer within 21 trading days of the first is dropped
  (overlapping windows would double-count one price move).
- **Minimum size:** total event value (shares × price, summed over the event's
  lines) ≥ $10,000, to keep token buys out.

## 5. Timing (the look-ahead rule)

- `known_at` is the filing's EDGAR acceptance time when the per-filing index gives
  it. The quarterly flat files give only a filing **date** (`FILING_DATE`,
  DD-MON-YYYY; checked against 2026q2_form345.zip on 2026-10-05): there, `known_at`
  is that date at 22:00 New York, the EDGAR filing cut-off. This is late by up to a
  day for filings accepted in the session, which errs away from look-ahead.
- **Entry:** the first session open strictly after `known_at`. Never the transaction
  date.
- **Return:** from entry open to the close `h` trading days later, on adjusted prices
  (adjusted open = open × adj_close / close).
- **Benchmark:** the equal-weight return of the issuer's family over the same window,
  from the rule-based universe as known at entry (`universe_as_of`, strict). The
  issuer itself is excluded from its benchmark.
- An issuer delisted inside the window keeps its last return; it is not dropped.

## 6. Sample

- **Universe:** rule-based membership only (SIC codes and point-in-time market cap
  from `config/sectors.yaml`). Curated seeds are never used. Foreign private issuers
  have no Form 4 and are out of scope.
- **Period:** events with entry from 2015-01-02 to the last date that leaves a full
  primary horizon of prices.
- **Price filter:** unadjusted entry open ≥ $2 (sub-$2 prints are dominated by
  spread).

## 7. Blockers known at drafting time

- The rule-based universe and its prices do not exist yet. The 59 curated seeds
  loaded on 2026-10-05 are not a valid sample for this test.
- The price source for a universe of that size is not chosen (Tiingo free tier:
  ~50 requests/hour).

## 8. Verdict

To be written in `docs/verdicts/001-insider-buys.md`: the primary statistic, pass or
fail, every secondary result, and the event counts at each filter step.
