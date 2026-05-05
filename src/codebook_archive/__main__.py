"""Command-line interface."""

from __future__ import annotations

import logging

import typer
from rich.console import Console
from rich.logging import RichHandler
from rich.table import Table

from .classify import run_classifier
from .config import Keywords, Settings
from .db import connect
from .export import export_candidates
from .sources import REGISTRY

app = typer.Typer(help="Discover and curate political-communication codebooks.")
console = Console()


def _setup_logging(verbose: bool) -> None:
    logging.basicConfig(
        level=logging.DEBUG if verbose else logging.INFO,
        format="%(message)s",
        datefmt="[%X]",
        handlers=[RichHandler(rich_tracebacks=True, show_path=False)],
    )
    # httpx and httpcore log full request URLs (including any query-string
    # secrets) at INFO/DEBUG. Silence them so tokens never reach the console.
    for noisy in ("httpx", "httpcore", "httpcore.http11", "httpcore.connection"):
        logging.getLogger(noisy).setLevel(logging.WARNING)


@app.command()
def discover(
    source: str = typer.Option("all", help="Source name or 'all'."),
    max_pages: int = typer.Option(4, help="Max pages per keyword per source."),
    verbose: bool = typer.Option(False, "--verbose", "-v"),
) -> None:
    """Run discovery against one or all sources and store candidates."""
    _setup_logging(verbose)
    keywords = Keywords.load()
    settings = Settings.from_env()
    sources = REGISTRY.keys() if source == "all" else [source]
    if any(s not in REGISTRY for s in sources):
        raise typer.BadParameter(f"Unknown source. Available: {', '.join(REGISTRY)}")
    with connect() as conn:
        for s in sources:
            console.rule(f"[bold]{s}")
            counts = REGISTRY[s].run(conn, keywords, settings, max_pages_per_term=max_pages)
            console.print(counts)


@app.command()
def export(
    status: str = typer.Option("all", help="Filter by review_status (or 'all')."),
) -> None:
    """Write the current candidates table to out/candidates.csv."""
    _setup_logging(False)
    path = export_candidates(status=status)
    console.print(f"[green]Wrote[/green] {path}")


@app.command()
def classify(
    model: str = typer.Option("claude-haiku-4-5", help="Anthropic model to use."),
    no_file_check: bool = typer.Option(False, "--no-file-check", help="Skip HTTP availability check."),
    dry_run: bool = typer.Option(False, "--dry-run", help="Print candidates without calling the API."),
    verbose: bool = typer.Option(False, "--verbose", "-v"),
) -> None:
    """Run the LLM classifier on all unclassified candidates."""
    _setup_logging(verbose)
    settings = Settings.from_env()
    if not dry_run and not settings.anthropic_api_key:
        raise typer.BadParameter("ANTHROPIC_API_KEY not set in .env")
    with connect() as conn:
        counts = run_classifier(
            conn,
            settings,
            model=model,
            check_files=not no_file_check,
            dry_run=dry_run,
        )
    console.print(counts)


@app.command()
def stats() -> None:
    """Print a summary of the candidates database."""
    _setup_logging(False)
    with connect() as conn:
        total = conn.execute("SELECT COUNT(*) FROM candidates").fetchone()[0]
        by_source = conn.execute(
            "SELECT source, COUNT(*) AS n FROM candidates GROUP BY source ORDER BY n DESC"
        ).fetchall()
        by_status = conn.execute(
            "SELECT review_status, COUNT(*) AS n FROM candidates "
            "GROUP BY review_status ORDER BY n DESC"
        ).fetchall()
    console.print(f"[bold]Total candidates:[/bold] {total}")
    t1 = Table(title="By source")
    t1.add_column("source")
    t1.add_column("count", justify="right")
    for r in by_source:
        t1.add_row(r["source"], str(r["n"]))
    console.print(t1)
    t2 = Table(title="By review status")
    t2.add_column("status")
    t2.add_column("count", justify="right")
    for r in by_status:
        t2.add_row(r["review_status"], str(r["n"]))
    console.print(t2)


if __name__ == "__main__":
    app()
