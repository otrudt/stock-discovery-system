from __future__ import annotations

import json
from datetime import date
from pathlib import Path

import duckdb
import httpx
import pytest

from sds.ingest.http import PoliteClient, RateLimiter
from sds.ingest.prices import Bar, TiingoSource, load_prices, upsert_bars
from sds.ingest.raw_store import RawStore
from sds.runs import QualityGateError
from sds.timeutil import market_close_utc

TODAY = date(2024, 6, 12)


def add_ticker(conn: duckdb.DuckDBPyConnection, ticker: str, cik: str) -> int:
    row = conn.execute(
        "INSERT INTO securities (cik, name) VALUES (?, ?) RETURNING security_id", [cik, ticker]
    ).fetchone()
    assert row is not None
    conn.execute("INSERT INTO ticker_history VALUES (?, ?, '2020-01-01', NULL)", [row[0], ticker])
    return int(row[0])


def tiingo(payload: bytes, seen: list[httpx.Request] | None = None) -> TiingoSource:
    def handler(request: httpx.Request) -> httpx.Response:
        if seen is not None:
            seen.append(request)
        return httpx.Response(200, content=payload)

    client = PoliteClient(
        "test test@example.com", 1000, transport=httpx.MockTransport(handler), sleep=lambda _: None
    )
    return TiingoSource(client, token="SECRET")


def test_prices_load_and_reload_is_idempotent(
    conn: duckdb.DuckDBPyConnection, tmp_path: Path, fixtures: Path
) -> None:
    add_ticker(conn, "NVDA", "0001045810")
    store = RawStore(tmp_path, conn)
    source = tiingo((fixtures / "tiingo_prices.json").read_bytes())

    first = load_prices(conn, store, source, start=date(2024, 6, 1), on=TODAY)
    second = load_prices(conn, store, source, start=date(2024, 6, 1), on=TODAY)

    assert first.rows_written == second.rows_written == 3
    assert conn.execute("SELECT count(*) FROM prices_daily").fetchone() == (3,)
    assert conn.execute("SELECT count(*) FROM raw_documents").fetchone() == (1,)
    # Unadjusted print kept; the split shows up only in adj_close.
    pre_split = conn.execute(
        "SELECT close, adj_close FROM prices_daily WHERE date = '2024-06-07'"
    ).fetchone()
    assert pre_split == (1208.88, 120.888)


def test_token_never_stored(
    conn: duckdb.DuckDBPyConnection, tmp_path: Path, fixtures: Path
) -> None:
    add_ticker(conn, "NVDA", "0001045810")
    seen: list[httpx.Request] = []
    source = tiingo((fixtures / "tiingo_prices.json").read_bytes(), seen)
    load_prices(conn, RawStore(tmp_path, conn), source, start=date(2024, 6, 1), on=TODAY)

    assert "SECRET" in str(seen[0].url)
    uris = conn.execute("SELECT uri FROM raw_documents").fetchall()
    assert all("SECRET" not in u for (u,) in uris)


def test_upsert_refreshes_adjusted_close(conn: duckdb.DuckDBPyConnection) -> None:
    # A refetch after a split changes adj_close only.
    sid = add_ticker(conn, "NVDA", "0001045810")
    day = date(2024, 6, 7)
    assert upsert_bars(conn, sid, "tiingo", [Bar(day, None, None, None, 10.0, 10.0, None)]) == 1
    assert upsert_bars(conn, sid, "tiingo", [Bar(day, None, None, None, 10.0, 1.0, None)]) == 1

    rows = conn.execute(
        "SELECT open, high, low, close, adj_close, volume, known_at FROM prices_daily"
    ).fetchall()
    assert rows == [(None, None, None, 10.0, 1.0, None, market_close_utc(day))]


def test_upsert_types_columns_beyond_the_first_rows(conn: duckdb.DuckDBPyConnection) -> None:
    # Old history often lacks OHLC. Type inference from the first rows alone would see
    # an all-NULL column and then fail, or truncate, on the first real value.
    sid = add_ticker(conn, "NVDA", "0001045810")
    first = date(2010, 1, 1).toordinal()
    bars = [
        Bar(date.fromordinal(first + i), None, None, None, 10.0, 10.0, None) for i in range(200)
    ]
    bars.append(Bar(date.fromordinal(first + 200), 12.5, 13.0, 12.0, 12.75, 12.75, 1000.0))

    assert upsert_bars(conn, sid, "tiingo", bars) == 201
    last = conn.execute(
        "SELECT open, volume FROM prices_daily ORDER BY date DESC LIMIT 1"
    ).fetchone()
    assert last == (12.5, 1000.0)


def test_only_missing_skips_securities_with_bars(
    conn: duckdb.DuckDBPyConnection, tmp_path: Path, fixtures: Path
) -> None:
    add_ticker(conn, "NVDA", "0001045810")
    add_ticker(conn, "AMD", "0000002488")
    store = RawStore(tmp_path, conn)
    payload = (fixtures / "tiingo_prices.json").read_bytes()
    load_prices(conn, store, tiingo(payload), start=date(2024, 6, 1), on=TODAY)
    add_ticker(conn, "MU", "0000723125")

    seen: list[httpx.Request] = []
    stats = load_prices(
        conn, store, tiingo(payload, seen), start=date(2024, 6, 1), on=TODAY, only_missing=True
    )

    assert [r.url.path for r in seen] == ["/tiingo/daily/mu/prices"]
    assert stats.rows_written == 3


def test_token_never_stored_on_http_error(conn: duckdb.DuckDBPyConnection, tmp_path: Path) -> None:
    # Tiingo answers 404 for a ticker it does not know; httpx puts the full URL,
    # query string included, in the error text that the run log and quarantine keep.
    add_ticker(conn, "ZZZZ", "0000000001")
    client = PoliteClient(
        "t t@example.com",
        1000,
        transport=httpx.MockTransport(lambda _: httpx.Response(404)),
        sleep=lambda _: None,
    )
    source = TiingoSource(client, token="SECRET")
    with pytest.raises(RuntimeError) as exc_info:
        load_prices(conn, RawStore(tmp_path, conn), source, start=date(2024, 6, 1), on=TODAY)

    assert "404" in str(exc_info.value)
    assert "SECRET" not in str(exc_info.value)
    reasons = conn.execute("SELECT reason FROM quarantine").fetchall()
    errors = conn.execute("SELECT error FROM runs").fetchall()
    assert reasons and errors
    assert all("SECRET" not in str(text) for (text,) in reasons + errors)


def test_bad_rows_quarantined_and_gate_blocks_writes(
    conn: duckdb.DuckDBPyConnection, tmp_path: Path
) -> None:
    add_ticker(conn, "NVDA", "0001045810")
    rows = [
        {"date": "2024-06-10", "close": 10.0, "high": 11, "low": 9, "adjClose": 10.0},
        {"date": "2024-06-11", "close": -1.0},  # non-positive
        {"date": "2024-06-11", "close": 10.0, "high": 8, "low": 9},  # high < low
        {"date": "2099-01-01", "close": 10.0},  # future
    ]
    source = tiingo(json.dumps(rows).encode())

    with pytest.raises(QualityGateError):
        load_prices(conn, RawStore(tmp_path, conn), source, start=date(2024, 6, 1), on=TODAY)

    assert conn.execute("SELECT count(*) FROM prices_daily").fetchone() == (0,)
    assert conn.execute("SELECT count(*) FROM quarantine").fetchone() == (3,)
    assert conn.execute("SELECT status FROM runs").fetchone() == ("failed",)


def test_raw_store_dedupes_and_detects_corruption(
    conn: duckdb.DuckDBPyConnection, tmp_path: Path
) -> None:
    from datetime import UTC, datetime

    store = RawStore(tmp_path, conn)
    now = datetime(2024, 1, 1, tzinfo=UTC)
    a = store.put(b"hello", source="s", uri="u1", fetched_at=now)
    b = store.put(b"hello", source="s", uri="u2", fetched_at=now)
    assert a.raw_hash == b.raw_hash
    assert store.get(a.raw_hash) == b"hello"

    a.path.write_bytes(b"tampered")
    with pytest.raises(ValueError, match="corrupted"):
        store.get(a.raw_hash)


def test_rate_limiter_spaces_calls() -> None:
    clock = [0.0]
    slept: list[float] = []

    def sleep(s: float) -> None:
        slept.append(s)
        clock[0] += s

    limiter = RateLimiter(per_second=10, clock=lambda: clock[0], sleep=sleep)
    for _ in range(3):
        limiter.wait()
    assert slept == pytest.approx([0.1, 0.1])


def test_client_retries_on_throttle() -> None:
    responses = iter([httpx.Response(429, headers={"Retry-After": "2"}), httpx.Response(200)])
    slept: list[float] = []
    client = PoliteClient(
        "t t@example.com",
        1000,
        transport=httpx.MockTransport(lambda _: next(responses)),
        sleep=slept.append,
    )
    client.get("https://example.com/x")
    assert 2.0 in slept
