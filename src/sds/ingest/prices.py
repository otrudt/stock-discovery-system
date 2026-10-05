"""Daily price ingestion.

Flow per security: fetch -> raw store -> parse -> validate -> upsert. A failed row is
quarantined, not dropped silently, and the run fails if more than 1% of rows fail.

Each bar's `known_at` is that session's 16:00 New York close: a bar loaded today for
2019 was still knowable in 2019, so backfills do not need fetch-time knowledge stamps.
"""

from __future__ import annotations

import json
from collections.abc import Iterable
from dataclasses import dataclass
from datetime import date, datetime
from typing import Protocol

import duckdb

from sds.ingest.http import PoliteClient
from sds.ingest.raw_store import RawStore
from sds.runs import RunStats, enforce_quality_gate, quarantine, run_job
from sds.timeutil import market_close_utc, utc_now


@dataclass(frozen=True)
class Bar:
    date: date
    open: float | None
    high: float | None
    low: float | None
    close: float
    adj_close: float | None
    volume: float | None


class PriceSource(Protocol):
    name: str

    def fetch(self, ticker: str, start: date) -> tuple[bytes, str]:
        """Return (raw bytes, a URI safe to store: no secrets)."""
        ...

    def parse(self, raw: bytes) -> list[dict[str, object]]:
        """Return loosely typed rows; validation happens in one place, below."""
        ...


class TiingoSource:
    """Tiingo end-of-day prices: unadjusted OHLC plus split- and dividend-adjusted close.

    Needs a token (free tier available; check current limits at signup).
    """

    name = "tiingo"
    base = "https://api.tiingo.com/tiingo/daily"

    def __init__(self, client: PoliteClient, token: str) -> None:
        self.client = client
        self.token = token

    def fetch(self, ticker: str, start: date) -> tuple[bytes, str]:
        url = f"{self.base}/{ticker.lower()}/prices"
        params = {"startDate": start.isoformat(), "format": "json"}
        raw = self.client.get(url, params={**params, "token": self.token})
        safe_uri = f"{url}?startDate={params['startDate']}"
        return raw, safe_uri

    def parse(self, raw: bytes) -> list[dict[str, object]]:
        rows = json.loads(raw)
        if not isinstance(rows, list):
            raise ValueError(f"unexpected Tiingo payload: {str(rows)[:200]}")
        return [
            {
                "date": str(r.get("date", ""))[:10],
                "open": r.get("open"),
                "high": r.get("high"),
                "low": r.get("low"),
                "close": r.get("close"),
                "adj_close": r.get("adjClose"),
                "volume": r.get("volume"),
            }
            for r in rows
        ]


def _num(v: object) -> float | None:
    if v is None or v == "":
        return None
    if isinstance(v, bool) or not isinstance(v, int | float | str):
        raise ValueError(f"not a number: {v!r}")
    return float(v)


def validate_bar(row: dict[str, object], today: date) -> Bar:
    """Raise ValueError with a readable reason if the row is not usable."""
    d = date.fromisoformat(str(row["date"]))
    if d > today:
        raise ValueError(f"bar dated in the future: {d}")
    close = _num(row["close"])
    if close is None or close <= 0:
        raise ValueError(f"non-positive close: {row['close']!r}")
    high, low = _num(row.get("high")), _num(row.get("low"))
    if high is not None and low is not None and high < low:
        raise ValueError(f"high {high} below low {low}")
    volume = _num(row.get("volume"))
    if volume is not None and volume < 0:
        raise ValueError(f"negative volume: {volume}")
    adj = _num(row.get("adj_close"))
    if adj is not None and adj <= 0:
        raise ValueError(f"non-positive adjusted close: {adj}")
    return Bar(d, _num(row.get("open")), high, low, close, adj, volume)


def upsert_bars(
    conn: duckdb.DuckDBPyConnection, security_id: int, source: str, bars: Iterable[Bar]
) -> int:
    """Insert or refresh bars. A refetch may update adj_close after a new split or
    dividend; the unadjusted close is the same print either way."""
    rows = [
        (
            security_id,
            b.date,
            b.open,
            b.high,
            b.low,
            b.close,
            b.adj_close,
            b.volume,
            source,
            market_close_utc(b.date),
        )
        for b in bars
    ]
    if not rows:
        return 0
    conn.executemany(
        """
        INSERT INTO prices_daily
          (security_id, date, open, high, low, close, adj_close, volume, source, known_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT (security_id, date, source) DO UPDATE SET
          open = excluded.open, high = excluded.high, low = excluded.low,
          close = excluded.close, adj_close = excluded.adj_close,
          volume = excluded.volume, known_at = excluded.known_at,
          ingested_at = now()
        """,
        rows,
    )
    return len(rows)


def fetch_and_parse(
    conn: duckdb.DuckDBPyConnection,
    store: RawStore,
    source: PriceSource,
    stats: RunStats,
    *,
    ticker: str,
    start: date,
    today: date,
) -> list[Bar]:
    raw, uri = source.fetch(ticker, start)
    doc = store.put(raw, source=f"prices/{source.name}", uri=uri, fetched_at=utc_now())
    return parse_raw(conn, source, stats, raw=raw, raw_hash=doc.raw_hash, today=today)


def parse_raw(
    conn: duckdb.DuckDBPyConnection,
    source: PriceSource,
    stats: RunStats,
    *,
    raw: bytes,
    raw_hash: str,
    today: date,
) -> list[Bar]:
    """Parse and validate one stored document; bad rows go to quarantine.

    Separate from fetching, so re-parsing the raw store after a parser fix needs no
    network. Writes nothing to research tables: the caller does that after the gate.
    """
    seen: set[date] = set()
    good: list[Bar] = []
    for row in source.parse(raw):
        stats.rows_in += 1
        try:
            bar = validate_bar(row, today)
            if bar.date in seen:
                raise ValueError(f"duplicate date {bar.date}")
            seen.add(bar.date)
            good.append(bar)
        except (ValueError, KeyError, TypeError) as exc:
            quarantine(
                conn,
                stats,
                source=f"prices/{source.name}",
                reason=str(exc),
                payload=json.dumps(row, default=str),
                raw_hash=raw_hash,
            )
    return good


def load_prices(
    conn: duckdb.DuckDBPyConnection,
    store: RawStore,
    source: PriceSource,
    *,
    start: date,
    on: date | None = None,
) -> RunStats:
    """Fetch prices for every security with a current ticker.

    All bars are staged in memory and written in one transaction only after the
    quality gate passes, so a bad run leaves the price table exactly as it was.
    Quarantined rows are kept either way, for debugging.
    """
    today = on or utc_now().date()
    targets = conn.execute(
        "SELECT security_id, ticker FROM ticker_history WHERE valid_to IS NULL ORDER BY ticker"
    ).fetchall()
    with run_job(conn, f"prices.{source.name}") as stats:
        staged: list[tuple[int, list[Bar]]] = []
        failures: list[str] = []
        for security_id, ticker in targets:
            try:
                bars = fetch_and_parse(
                    conn, store, source, stats, ticker=str(ticker), start=start, today=today
                )
                staged.append((int(security_id), bars))
            except Exception as exc:  # one bad ticker should not stop the rest
                failures.append(f"{ticker}: {exc}")
                quarantine(
                    conn,
                    stats,
                    source=f"prices/{source.name}",
                    reason=f"fetch failed: {exc}",
                    payload=str(ticker),
                )
        if targets and len(failures) == len(targets):
            raise RuntimeError("every ticker failed: " + "; ".join(failures[:5]))
        enforce_quality_gate(stats)
        conn.execute("BEGIN")
        try:
            for security_id, bars in staged:
                stats.rows_written += upsert_bars(conn, security_id, source.name, bars)
            conn.execute("COMMIT")
        except Exception:
            conn.execute("ROLLBACK")
            raise
    return stats


def latest_known(conn: duckdb.DuckDBPyConnection) -> list[tuple[str, datetime | None]]:
    """Freshness per price source, for the data-health report."""
    return [
        (str(s), k)
        for s, k in conn.execute(
            "SELECT source, max(known_at) FROM prices_daily GROUP BY source ORDER BY source"
        ).fetchall()
    ]
