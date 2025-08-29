"""
Typer-based CLI entrypoint with robust error handling.

Commands:
- version: show app version (placeholder).
- scan: Phase 2 placeholder that validates roots and lists candidate files.

Design notes:
- We validate directories carefully and raise our own InvalidPathError. Typer
  catches these and we convert them into a clean exit with code 1.
- We catch unexpected exceptions, log them, and exit with a helpful message
  without dumping scary stack traces by default. Use -v for DEBUG logs.
"""

from __future__ import annotations

import os
from pathlib import Path
from typing import Optional
import typer

from config import AppConfig
from errors import ConfigLoadError, InvalidPathError, InternalError, PdaiError
from logs import init_logging, get_logger

app = typer.Typer(
    add_completion=False, no_args_is_help=True, help="Photo Dedup AI (Phase 2 CLI)"
)
log = get_logger("pdai")


def _validate_folder(p: str) -> Path:
    """
    Ensure a folder exists and is a directory. Raise InvalidPathError on failure.
    """
    try:
        path = Path(os.path.expanduser(p)).resolve()
    except Exception as exc:
        # Path resolution errors are rare but we handle them to be safe.
        raise InvalidPathError(f"Invalid path: {p}") from exc

    if not path.exists():
        raise InvalidPathError(f"Path not found: {path}")
    if not path.is_dir():
        raise InvalidPathError(f"Not a directory: {path}")
    return path


@app.callback()
def main(
    verbose: bool = typer.Option(
        False, "--verbose", "-v", help="Enable verbose logging"
    ),
) -> None:
    # Initialize logging once per process
    init_logging(level="DEBUG" if verbose else "INFO")
    global log
    log = get_logger("pdai.cli")
    if verbose:
        log.debug("Verbose logging enabled")


@app.command("version")
def version_cmd() -> None:
    """
    Show version (placeholder until we wire a proper __version__).
    """
    typer.echo("photo-dedup-ai v0.0.2")


@app.command("scan")
def scan_cmd(
    folder: Optional[str] = typer.Argument(None, help="Folder to scan (optional)"),
    config_file: Optional[Path] = typer.Option(
        None, "--config", "-c", help="Path to dup.toml"
    ),
    list_only: bool = typer.Option(
        True, "--list-only/--no-list-only", help="List files only (Phase 2 placeholder)"
    ),
) -> None:
    """
    Phase 2 placeholder scan:
      - loads config (dup.toml if present)
      - optionally overrides roots with a single folder argument
      - lists candidate files by extension filters

    On errors:
      - Gracefully exits with a user-friendly message and exit code 1.
    """
    try:
        cfg = AppConfig.load(config_file)

        # Determine roots: argument takes precedence over config.
        roots = [Path(folder).resolve()] if folder else cfg.scan.roots
        if not roots:
            raise InvalidPathError(
                "No folder provided and no roots configured. "
                "Pass a FOLDER argument or create a dup.toml with [scan.roots]."
            )

        log.info(f"[bold]Scan starting[/]: {len(roots)} root(s)")
        total = 0

        for root in roots:
            root = _validate_folder(str(root))
            log.info(f"Root: {root}")

            for dirpath, _, filenames in os.walk(
                root, followlinks=cfg.scan.follow_symlinks
            ):
                pdir = Path(dirpath)

                # Optionally ignore hidden directories; this is cheap and avoids surprises.
                if cfg.scan.ignore_hidden and any(
                    part.startswith(".") for part in pdir.parts
                ):
                    continue

                for name in filenames:
                    ext = Path(name).suffix.lower()
                    if ext in cfg.scan.include_ext:
                        total += 1
                        if list_only:
                            # Print to stdout for easy piping (e.g., | wc -l).
                            typer.echo(str(pdir / name))

        log.info(f"[green]Scan complete[/] — candidates: {total}")

    except (InvalidPathError, ConfigLoadError) as exc:
        # Expected, user-facing errors: print a clean message and exit(1).
        log.error(f"[red]Error:[/] {exc}")
        raise typer.Exit(code=1)  # non-zero indicates failure

    except PdaiError as exc:
        # Known internal errors: advise user; keep logs useful.
        log.error(f"[red]Internal error:[/] {exc}")
        raise typer.Exit(code=1)

    except Exception:  # pragma: no cover - unexpected edge cases
        # Unexpected failure: we don't spam a full traceback by default, but
        # DEBUG mode will show more detail via logging configuration.
        log.exception("Unexpected error during scan")  # includes traceback
        # Mask raw error behind a friendly message:
        raise typer.Exit(code=1) from InternalError(
            "Unexpected failure. Re-run with -v for details."
        )
