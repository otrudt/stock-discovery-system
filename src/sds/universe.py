"""Sector configuration and universe seeding."""

from __future__ import annotations

import json
from datetime import date, datetime
from pathlib import Path
from typing import Any

import duckdb
import yaml
from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

DEFAULT_CONFIG = Path(__file__).resolve().parents[2] / "config" / "sectors.yaml"


class _Strict(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)


class Layer(_Strict):
    label: str
    seeds: list[str] = Field(default_factory=list)

    @field_validator("seeds", mode="before")
    @classmethod
    def _tickers(cls, v: object) -> object:
        if isinstance(v, list):
            for t in v:
                if not isinstance(t, str):
                    raise ValueError(
                        f'seed {t!r} is not a string; quote it in YAML (e.g. "ON" parses as true)'
                    )
            return [t.strip().upper() for t in v]
        return v


class Family(_Strict):
    label: str
    sic_codes: list[int] = Field(default_factory=list)
    layers: dict[str, Layer]


class SmallCapTier(_Strict):
    market_cap_min_usd: float = Field(gt=0)
    market_cap_max_usd: float = Field(gt=0)
    min_median_dollar_volume_usd: float = Field(ge=0)
    liquidity_lookback_days: int = Field(gt=0)

    @model_validator(mode="after")
    def _band(self) -> SmallCapTier:
        if self.market_cap_min_usd >= self.market_cap_max_usd:
            raise ValueError("small_cap band: min must be below max")
        return self


class SectorConfig(_Strict):
    version: int
    families: dict[str, Family]
    foreign_private_issuers: list[str] = Field(default_factory=list)
    small_cap: SmallCapTier

    @model_validator(mode="after")
    def _no_duplicate_seeds(self) -> SectorConfig:
        seen: dict[str, str] = {}
        for fam_key, fam in self.families.items():
            for layer_key, layer in fam.layers.items():
                for t in layer.seeds:
                    where = f"{fam_key}.{layer_key}"
                    # A ticker may sit in two families (e.g. a chip in AI compute and
                    # components) but not twice in the same family.
                    key = f"{fam_key}:{t}"
                    if key in seen:
                        raise ValueError(f"{t} seeded twice in {fam_key}: {seen[key]}, {where}")
                    seen[key] = where
        return self

    def seeds(self) -> list[tuple[str, str, str]]:
        """(family, layer, ticker) for every seed."""
        return [
            (fam_key, layer_key, t)
            for fam_key, fam in self.families.items()
            for layer_key, layer in fam.layers.items()
            for t in layer.seeds
        ]


def load_config(path: Path = DEFAULT_CONFIG) -> SectorConfig:
    raw: Any = yaml.safe_load(path.read_text(encoding="utf-8"))
    return SectorConfig.model_validate(raw)


def parse_sec_ticker_map(raw: bytes) -> dict[str, tuple[str, str]]:
    """Parse SEC's company_tickers.json into {TICKER: (cik10, name)}."""
    data = json.loads(raw)
    out: dict[str, tuple[str, str]] = {}
    for row in data.values():
        ticker = str(row["ticker"]).upper()
        out[ticker] = (str(row["cik_str"]).zfill(10), str(row["title"]))
    return out


def _share_class(ticker: str) -> str:
    # BRK-B style tickers carry their class after the separator.
    for sep in ("-", "."):
        if sep in ticker:
            return ticker.split(sep, 1)[1]
    return ""


def seed_universe(
    conn: duckdb.DuckDBPyConnection,
    config: SectorConfig,
    ticker_map: dict[str, tuple[str, str]],
    *,
    recorded_at: datetime,
    seen_on: date,
    valid_from: date = date(2015, 1, 1),
) -> tuple[int, list[str]]:
    """Upsert securities, current tickers and curated memberships. Idempotent.

    Returns (memberships written, tickers that could not be resolved).
    `seen_on` starts the ticker's history: SEC's map only tells us today's ticker, so
    lookups before that date fail loudly instead of guessing.
    """
    fpis = set(config.foreign_private_issuers)
    unresolved: list[str] = []
    written = 0
    for family, layer, ticker in config.seeds():
        if ticker not in ticker_map:
            unresolved.append(ticker)
            continue
        cik, name = ticker_map[ticker]
        share_class = _share_class(ticker)
        row = conn.execute(
            """
            INSERT INTO securities (cik, share_class, name, is_foreign_private_issuer)
            VALUES (?, ?, ?, ?)
            ON CONFLICT (cik, share_class) DO UPDATE
              SET name = excluded.name,
                  is_foreign_private_issuer = excluded.is_foreign_private_issuer
            RETURNING security_id
            """,
            [cik, share_class, name, ticker in fpis],
        ).fetchone()
        assert row is not None
        security_id = int(row[0])

        current = conn.execute(
            "SELECT ticker FROM ticker_history WHERE security_id = ? AND valid_to IS NULL",
            [security_id],
        ).fetchone()
        if current is None:
            conn.execute(
                "INSERT INTO ticker_history VALUES (?, ?, ?, NULL)",
                [security_id, ticker, seen_on],
            )
        elif current[0] != ticker:
            # Rename: close the old ticker, open the new one.
            conn.execute(
                "UPDATE ticker_history SET valid_to = ? WHERE security_id = ? AND valid_to IS NULL",
                [seen_on, security_id],
            )
            conn.execute(
                "INSERT INTO ticker_history VALUES (?, ?, ?, NULL)",
                [security_id, ticker, seen_on],
            )

        exists = conn.execute(
            """
            SELECT 1 FROM universe_membership
            WHERE security_id = ? AND family = ? AND valid_to IS NULL
            """,
            [security_id, family],
        ).fetchone()
        if exists is None:
            conn.execute(
                """
                INSERT INTO universe_membership
                  (security_id, family, layer, valid_from, valid_to, basis, rationale, recorded_at)
                VALUES (?, ?, ?, ?, NULL, 'curated', ?, ?)
                """,
                [security_id, family, layer, valid_from, f"seed: {family}.{layer}", recorded_at],
            )
            written += 1
    return written, unresolved
