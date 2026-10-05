from __future__ import annotations

from collections.abc import Iterator
from pathlib import Path

import duckdb
import pytest

from sds.db import connect

FIXTURES = Path(__file__).parent / "fixtures"


@pytest.fixture
def conn() -> Iterator[duckdb.DuckDBPyConnection]:
    c = connect(":memory:")
    yield c
    c.close()


@pytest.fixture
def fixtures() -> Path:
    return FIXTURES
