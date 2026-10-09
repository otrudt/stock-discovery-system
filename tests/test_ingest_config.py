from __future__ import annotations

from pathlib import Path

import pytest
from pydantic import ValidationError

from sds.ingest.config import load_ingest_config, quarter_key


def test_repo_ingest_config_loads() -> None:
    cfg = load_ingest_config().form345
    assert cfg.start_quarter == "2012q1"
    assert cfg.forms == ["4", "4/A"]
    assert cfg.txn_codes == ["P", "S"]


def test_bad_quarter_refused(tmp_path: Path) -> None:
    path = tmp_path / "ingest.yaml"
    path.write_text("form345: {start_quarter: 2012q5, forms: ['4'], txn_codes: [P]}\n")
    with pytest.raises(ValidationError, match="not a quarter"):
        load_ingest_config(path)


def test_unknown_key_refused(tmp_path: Path) -> None:
    path = tmp_path / "ingest.yaml"
    path.write_text("form345: {start_quarter: 2012q1, forms: ['4'], txn_codes: [P], typo: 1}\n")
    with pytest.raises(ValidationError, match="typo"):
        load_ingest_config(path)


def test_quarter_key_orders_quarters() -> None:
    assert sorted(["2013q1", "2012q4", "2012q1"], key=quarter_key) == [
        "2012q1",
        "2012q4",
        "2013q1",
    ]
