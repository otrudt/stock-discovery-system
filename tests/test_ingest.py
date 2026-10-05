from __future__ import annotations

import json
from datetime import date
from pathlib import Path

import duckdb
import httpx
import pytest

from sds.ingest.http import PoliteClient, RateLimiter
from sds.ingest.prices import TiingoSource, load_prices
from sds.ingest.raw_store import RawStore
from sds.runs import QualityGateError

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
