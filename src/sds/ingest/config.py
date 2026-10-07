"""Ingestion config: what each source loads. Validated like config/sectors.yaml."""

from __future__ import annotations

import re
from pathlib import Path
from typing import Any

import yaml
from pydantic import Field, field_validator

from sds.configmodel import StrictModel

DEFAULT_INGEST_CONFIG = Path(__file__).resolve().parents[3] / "config" / "ingest.yaml"

_QUARTER = re.compile(r"^(\d{4})q([1-4])$")


def quarter_key(quarter: str) -> tuple[int, int]:
    """'2012q1' -> (2012, 1), for ordering. Raises ValueError on any other shape."""
    match = _QUARTER.match(quarter)
    if match is None:
        raise ValueError(f"not a quarter like 2012q1: {quarter!r}")
    return int(match.group(1)), int(match.group(2))


class Form345Config(StrictModel):
    start_quarter: str
    forms: list[str] = Field(min_length=1)
    txn_codes: list[str] = Field(min_length=1)

    @field_validator("start_quarter")
    @classmethod
    def _quarter(cls, v: str) -> str:
        quarter_key(v)
        return v


class IngestConfig(StrictModel):
    form345: Form345Config


def load_ingest_config(path: Path = DEFAULT_INGEST_CONFIG) -> IngestConfig:
    raw: Any = yaml.safe_load(path.read_text(encoding="utf-8"))
    return IngestConfig.model_validate(raw)
