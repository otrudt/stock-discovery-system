from __future__ import annotations

from pathlib import Path

import pytest
from typer.testing import CliRunner

from sds.cli import app
from sds.db import connect
from sds.ingest.form345 import LoadReport


def test_health_reports_loaded_prices(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    # TIMESTAMPTZ values cross into Python only once a table has rows; an empty
    # database hides any problem with that conversion.
    monkeypatch.setenv("SDS_DATA_DIR", str(tmp_path))
    conn = connect(tmp_path / "sds.duckdb")
    conn.execute(
        "INSERT INTO prices_daily (security_id, date, close, source, known_at)"
        " VALUES (1, '2024-06-11', 100.0, 'tiingo', '2024-06-11 20:00:00+00')"
    )
    conn.close()

    result = CliRunner().invoke(app, ["health"])

    assert result.exit_code == 0, result.output
    assert "prices/tiingo" in result.output
    assert "2024-06-11" in result.output


def test_health_reports_insider_filings(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("SDS_DATA_DIR", str(tmp_path))
    conn = connect(tmp_path / "sds.duckdb")
    conn.execute(
        "INSERT INTO insider_filings (accession_no, issuer_cik, form, filing_date,"
        " period_of_report, known_at, raw_hash) VALUES ('a1', '0000000001', '4',"
        " '2024-01-02', '2024-01-01', '2024-01-03 03:00:00+00', 'h')"
    )
    conn.close()

    result = CliRunner().invoke(app, ["health"])

    assert result.exit_code == 0, result.output
    assert "insider_filings" in result.output
    assert "2024-01-03" in result.output


def test_insiders_load_exits_nonzero_when_a_quarter_fails(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("SDS_DATA_DIR", str(tmp_path))
    monkeypatch.setenv("SDS_USER_AGENT", "t t@example.com")
    report = LoadReport(loaded=["2012q2"], failed={"2012q1": "HTTPStatusError: HTTP 404"})
    monkeypatch.setattr("sds.cli.load_insiders", lambda *args, **kwargs: report)

    result = CliRunner().invoke(app, ["insiders", "load"])

    assert result.exit_code == 1
    assert "failed 2012q1" in result.output


def test_insiders_link_reports_counts(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("SDS_DATA_DIR", str(tmp_path))
    result = CliRunner().invoke(app, ["insiders", "link"])
    assert result.exit_code == 0, result.output
    assert "amendments:" in result.output
