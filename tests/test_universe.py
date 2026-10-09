from __future__ import annotations

from datetime import UTC, date, datetime
from pathlib import Path

import duckdb
import pytest
from pydantic import ValidationError

from sds.universe import (
    DEFAULT_CONFIG,
    SectorConfig,
    load_config,
    parse_sec_ticker_map,
    seed_universe,
)

NOW = datetime(2026, 10, 5, 16, 0, tzinfo=UTC)


def small_config(**overrides: object) -> SectorConfig:
    raw: dict[str, object] = {
        "version": 1,
        "families": {
            "ai_compute": {
                "label": "AI compute",
                "layers": {
                    "accelerators": {"label": "Accelerators", "seeds": ["nvda", "AMD"]},
                    "foundry": {"label": "Foundry", "seeds": ["TSM", "ZZZZ"]},
                },
            }
        },
        "foreign_private_issuers": ["TSM"],
        "small_cap": {
            "market_cap_min_usd": 3e8,
            "market_cap_max_usd": 2e9,
            "min_median_dollar_volume_usd": 1e6,
            "liquidity_lookback_days": 63,
        },
    }
    raw.update(overrides)
    return SectorConfig.model_validate(raw)


def test_repo_config_is_valid() -> None:
    cfg = load_config(DEFAULT_CONFIG)
    assert {"ai_compute", "components", "robotics", "pharma", "bioscience"} <= set(cfg.families)
    assert len(cfg.seeds()) > 50


def test_rejects_unknown_keys_and_bad_band() -> None:
    with pytest.raises(ValidationError):
        small_config(extra_key=1)
    with pytest.raises(ValidationError, match="min must be below max"):
        small_config(
            small_cap={
                "market_cap_min_usd": 3e9,
                "market_cap_max_usd": 2e9,
                "min_median_dollar_volume_usd": 0,
                "liquidity_lookback_days": 63,
            }
        )


def test_rejects_duplicate_seed_in_family() -> None:
    fams = {
        "x": {
            "label": "X",
            "layers": {
                "a": {"label": "A", "seeds": ["NVDA"]},
                "b": {"label": "B", "seeds": ["NVDA"]},
            },
        }
    }
    with pytest.raises(ValidationError, match="seeded twice"):
        small_config(families=fams)


def test_seed_is_idempotent_and_reports_unresolved(
    conn: duckdb.DuckDBPyConnection, fixtures: Path
) -> None:
    tickers = parse_sec_ticker_map((fixtures / "sec_company_tickers.json").read_bytes())
    cfg = small_config()

    written, unresolved = seed_universe(conn, cfg, tickers, recorded_at=NOW, seen_on=NOW.date())
    again, _ = seed_universe(conn, cfg, tickers, recorded_at=NOW, seen_on=NOW.date())

    assert (written, again) == (3, 0)
    assert unresolved == ["ZZZZ"]
    assert conn.execute("SELECT count(*) FROM securities").fetchone() == (3,)
    fpi = conn.execute(
        "SELECT is_foreign_private_issuer FROM securities WHERE cik = '0001046179'"
    ).fetchone()
    assert fpi == (True,)


def test_ticker_rename_closes_old_history(conn: duckdb.DuckDBPyConnection, fixtures: Path) -> None:
    tickers = parse_sec_ticker_map((fixtures / "sec_company_tickers.json").read_bytes())
    cfg = small_config()
    seed_universe(conn, cfg, tickers, recorded_at=NOW, seen_on=date(2026, 1, 1))

    renamed = dict(tickers)
    renamed["NVDX"] = renamed.pop("NVDA")
    layers = {"accelerators": {"label": "A", "seeds": ["NVDX"]}}
    cfg2 = small_config(families={"ai_compute": {"label": "AI", "layers": layers}})
    seed_universe(conn, cfg2, renamed, recorded_at=NOW, seen_on=date(2026, 6, 1))

    rows = conn.execute(
        "SELECT ticker, valid_from, valid_to FROM ticker_history th"
        " JOIN securities s USING (security_id) WHERE s.cik = '0001045810' ORDER BY valid_from"
    ).fetchall()
    assert rows == [
        ("NVDA", date(2026, 1, 1), date(2026, 6, 1)),
        ("NVDX", date(2026, 6, 1), None),
    ]


def test_unquoted_yaml_boolean_ticker_is_caught(tmp_path: Path) -> None:
    # YAML 1.1 reads a bare ON as true; ON Semiconductor would silently vanish.
    text = DEFAULT_CONFIG.read_text().replace('"ON"', "ON")
    path = tmp_path / "sectors.yaml"
    path.write_text(text)
    with pytest.raises(ValidationError, match="quote it in YAML"):
        load_config(path)
