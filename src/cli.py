"""
Typer-based CLI entrypoint with robust error handling.

Commands:
- version: show app version (placeholder).
- scan: validates roots (from arg or dup.toml), streams candidate files via walker, and lists them.
"""

from __future__ import annotations

from pathlib import Path
from typing import Optional

import typer

from config import AppConfig
from errors import ConfigLoadError, InvalidPathError, InternalError, PdaiError
from logs import init_logging, get_logger
from walker import iter_media_files

app = typer.Typer(
    add_completion=False,
    no_args_is_help=True,
    help="Photo Dedup AI — CLI (Phase 3: walker + DB bootstrap)",
)

# Initialized in main(); module-level fallback is harmless.
log = get_logger("pdai")


@app.callback()
def main(
    verbose: bool = typer.Option(
        False, "--verbose", "-v", help="Enable verbose (DEBUG) logging"
    ),
) -> None:
    """Global CLI options and logging setup."""
    init_logging(level="DEBUG" if verbose else "INFO")
    global log
    log = get_logger("pdai.cli")
    if verbose:
        log.debug("Verbose logging enabled")


@app.command("version")
def version_cmd() -> None:
    """Show version (placeholder until we wire __version__)."""
    typer.echo("photo-dedup-ai v0.0.2")


@app.command("scan")
def scan_cmd(
    folder: Optional[str] = typer.Argument(None, help="Folder to scan (optional)"),
    config_file: Optional[Path] = typer.Option(
        None, "--config", "-c", help="Path to dup.toml"
    ),
    list_only: bool = typer.Option(
        True,
        "--list-only/--no-list-only",
        help="List files only (placeholder; later phases will persist to DB)",
    ),
) -> None:
    """
    Stream candidate media files and list them.

    - Loads config (dup.toml if present).
    - If a FOLDER arg is provided, it takes precedence over config roots.
    - Applies filters: include_ext, ignore_hidden, follow_symlinks.
    """
    try:
        cfg = AppConfig.load(config_file)

        roots = [Path(folder).resolve()] if folder else cfg.scan.roots
        if not roots:
            raise InvalidPathError(
                "No folder provided and no roots configured. "
                "Pass a FOLDER argument or create dup.toml with [scan.roots]."
            )

        log.info(f"[bold]Scan starting[/]: {len(roots)} root(s)")
        total = 0

        for path, _ext in iter_media_files(
            roots,
            include_ext=cfg.scan.include_ext,
            ignore_hidden=cfg.scan.ignore_hidden,
            follow_symlinks=cfg.scan.follow_symlinks,
        ):
            total += 1
            if list_only:
                typer.echo(str(path))

        log.info(f"[green]Scan complete[/] — candidates: {total}")

    except (InvalidPathError, ConfigLoadError) as exc:
        log.error(f"[red]Error:[/] {exc}")
        raise typer.Exit(code=1)

    except PdaiError as exc:
        log.error(f"[red]Internal error:[/] {exc}")
        raise typer.Exit(code=1)

    except Exception:  # pragma: no cover — unexpected edge cases
        log.exception("Unexpected error during scan")
        raise typer.Exit(code=1) from InternalError(
            "Unexpected failure. Re-run with -v for details."
        )
