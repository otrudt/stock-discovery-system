"""Command line: one command per job, each safe to re-run."""

from __future__ import annotations

import os
from datetime import date
from pathlib import Path

import typer

from sds.db import connect
from sds.ingest.config import DEFAULT_INGEST_CONFIG, load_ingest_config
from sds.ingest.form345 import load_insiders
from sds.ingest.http import PoliteClient
from sds.ingest.prices import TiingoSource, load_prices
from sds.ingest.raw_store import RawStore
from sds.insiders import link_amendments
from sds.settings import Settings
from sds.timeutil import utc_now
from sds.universe import DEFAULT_CONFIG, load_config, parse_sec_ticker_map, seed_universe

app = typer.Typer(no_args_is_help=True, help="Stock discovery system.")
universe_app = typer.Typer(no_args_is_help=True, help="Universe commands.")
prices_app = typer.Typer(no_args_is_help=True, help="Price commands.")
insiders_app = typer.Typer(no_args_is_help=True, help="Insider (Form 4) commands.")
app.add_typer(universe_app, name="universe")
app.add_typer(prices_app, name="prices")
app.add_typer(insiders_app, name="insiders")

SEC_TICKERS_URL = "https://www.sec.gov/files/company_tickers.json"
SEC_RATE_PER_SECOND = 8.0  # under SEC's 10/s fair-access ceiling


@app.command()
def init() -> None:
    """Create the database and apply migrations."""
    settings = Settings.from_env()
    connect(settings.db_path)
    typer.echo(f"database ready at {settings.db_path}")


@universe_app.command("check")
def universe_check(config: Path = DEFAULT_CONFIG) -> None:
    """Validate config/sectors.yaml without touching the database."""
    cfg = load_config(config)
    seeds = cfg.seeds()
    typer.echo(f"ok: {len(cfg.families)} families, {len(seeds)} seeds")


@universe_app.command("seed")
def universe_seed(config: Path = DEFAULT_CONFIG) -> None:
    """Resolve seed tickers to SEC CIKs and record curated memberships."""
    settings = Settings.from_env()
    cfg = load_config(config)
    conn = connect(settings.db_path)
    store = RawStore(settings.raw_dir, conn)
    client = PoliteClient(settings.require_user_agent(), SEC_RATE_PER_SECOND)
    try:
        now = utc_now()
        raw = client.get(SEC_TICKERS_URL)
        store.put(raw, source="sec/company_tickers", uri=SEC_TICKERS_URL, fetched_at=now)
        written, unresolved = seed_universe(
            conn, cfg, parse_sec_ticker_map(raw), recorded_at=now, seen_on=now.date()
        )
    finally:
        client.close()
    typer.echo(f"memberships written: {written}")
    if unresolved:
        typer.echo(f"unresolved tickers (not in SEC map): {', '.join(unresolved)}", err=True)


@prices_app.command("load")
def prices_load(
    start: str = "2015-01-01",
    only_missing: bool = typer.Option(False, help="Fetch only securities with no bars yet."),
) -> None:
    """Fetch daily prices for every current ticker from Tiingo."""
    settings = Settings.from_env()
    token = os.environ.get("SDS_TIINGO_TOKEN", "")
    if not token:
        raise typer.BadParameter("set SDS_TIINGO_TOKEN (free account at tiingo.com)")
    conn = connect(settings.db_path)
    client = PoliteClient(settings.user_agent or "stock-discovery-system", per_second=1.0)
    try:
        stats = load_prices(
            conn,
            RawStore(settings.raw_dir, conn),
            TiingoSource(client, token),
            start=date.fromisoformat(start),
            only_missing=only_missing,
        )
    finally:
        client.close()
    typer.echo(
        f"rows in {stats.rows_in}, written {stats.rows_written}, "
        f"quarantined {stats.rows_quarantined} (run {stats.run_id})"
    )


def _amendment_line(counts: dict[str, int]) -> str:
    return "amendments: " + (", ".join(f"{k}={v}" for k, v in sorted(counts.items())) or "none")


@insiders_app.command("load")
def insiders_load(
    since: str | None = typer.Option(None, help="First quarter, e.g. 2012q1 (default: config)."),
    quarter: str | None = typer.Option(None, help="Load only this quarter, e.g. 2026q2."),
    refetch: bool = typer.Option(False, help="Fetch zips again even if stored."),
    config: Path = DEFAULT_INGEST_CONFIG,
) -> None:
    """Load SEC quarterly Form 4 flat files, then link amendments."""
    settings = Settings.from_env()
    cfg = load_ingest_config(config).form345
    conn = connect(settings.db_path)
    client = PoliteClient(settings.require_user_agent(), SEC_RATE_PER_SECOND)
    try:
        report = load_insiders(
            conn,
            RawStore(settings.raw_dir, conn),
            client,
            cfg,
            since=since,
            only=quarter,
            refetch=refetch,
        )
    finally:
        client.close()
    typer.echo(
        f"quarters loaded {len(report.loaded)}, skipped {len(report.skipped)}, "
        f"failed {len(report.failed)}"
    )
    typer.echo(_amendment_line(report.amendments))
    for failed_quarter, error in report.failed.items():
        typer.echo(f"  failed {failed_quarter}: {error}", err=True)
    if report.failed:
        raise typer.Exit(1)


@insiders_app.command("link")
def insiders_link() -> None:
    """Recompute amendment links over all loaded filings."""
    typer.echo(_amendment_line(link_amendments(connect(Settings.from_env().db_path))))


@app.command()
def health() -> None:
    """Data health: freshness per source and the last run of each job."""
    conn = connect(Settings.from_env().db_path)
    typer.echo("freshness (latest known_at):")
    rows = conn.execute(
        """
        SELECT 'prices/' || source, max(known_at) FROM prices_daily GROUP BY source
        UNION ALL SELECT 'fundamentals', max(known_at) FROM fundamentals
        UNION ALL SELECT 'insider_filings', max(known_at) FROM insider_filings
        UNION ALL SELECT 'congress_txns', max(known_at) FROM congress_txns
        """
    ).fetchall()
    for source, latest in rows:
        typer.echo(f"  {source:<16} {latest or 'never loaded'}")
    typer.echo("last runs:")
    runs = conn.execute(
        """
        SELECT job, status, started_at, rows_in, rows_written, rows_quarantined
        FROM runs QUALIFY row_number() OVER (PARTITION BY job ORDER BY started_at DESC) = 1
        ORDER BY job
        """
    ).fetchall()
    for job, status, started, n_in, n_out, n_q in runs or []:
        typer.echo(f"  {job:<16} {status:<7} {started}  in={n_in} out={n_out} quarantined={n_q}")
    if not runs:
        typer.echo("  none yet")
