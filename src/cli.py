"""
CLI Phase 4:
- scan: persists file rows into SQLite when --no-list-only is used
- report: shows totals (optionally JSON)
"""

from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Optional

import typer

from config import AppConfig
from db import Database
from errors import ConfigLoadError, InvalidPathError, InternalError, PdaiError
from logs import get_logger, init_logging
from media import media_type_from_ext
from walker import iter_media_files

app = typer.Typer(
    add_completion=False,
    no_args_is_help=True,
    help="Photo Dedup AI — CLI (Phase 4: persist files + report)",
)

log = get_logger("pdai")


@app.callback()
def main(
    verbose: bool = typer.Option(
        False, "--verbose", "-v", help="Enable verbose (DEBUG) logging"
    ),
) -> None:
    init_logging(level="DEBUG" if verbose else "INFO")
    global log
    log = get_logger("pdai.cli")
    if verbose:
        log.debug("Verbose logging enabled")


@app.command("version")
def version_cmd() -> None:
    typer.echo("photo-dedup-ai v0.0.3")


@app.command("scan")
def scan_cmd(
    folder: Optional[str] = typer.Argument(None, help="Folder to scan (optional)"),
    config_file: Optional[Path] = typer.Option(
        None, "--config", "-c", help="Path to dup.toml"
    ),
    list_only: bool = typer.Option(
        True,
        "--list-only/--no-list-only",
        help="List files only (default). Use --no-list-only to persist into DB.",
    ),
    batch_size: int = typer.Option(1000, "--batch-size", help="DB upsert batch size"),
) -> None:
    """
    Stream candidate media files.
    - With --list-only (default): print paths (no DB changes).
    - With --no-list-only: persist into SQLite (files table).
    """
    db: Optional[Database] = None
    try:
        cfg = AppConfig.load(config_file)

        # Ensure DB exists and schema applied
        db = Database(cfg.scan.db_path)
        db.connect()
        log.info(f"DB ready at: {cfg.scan.db_path}")

        roots = [Path(folder).resolve()] if folder else cfg.scan.roots
        if not roots:
            raise InvalidPathError(
                "No folder provided and no roots configured. "
                "Pass a FOLDER argument or create dup.toml with [scan.roots]."
            )

        log.info(f"[bold]Scan starting[/]: {len(roots)} root(s)")
        total = 0

        batch: list[tuple[str, int, float, str, str]] = []

        for path, ext in iter_media_files(
            roots,
            include_ext=cfg.scan.include_ext,
            ignore_hidden=cfg.scan.ignore_hidden,
            follow_symlinks=cfg.scan.follow_symlinks,
        ):
            total += 1
            if list_only:
                typer.echo(str(path))
                continue

            # Gather file stats for DB
            try:
                st = os.stat(path, follow_symlinks=True)
            except OSError:
                # Skip unreadable paths
                continue

            mtype = media_type_from_ext(ext)
            row = (str(path), int(st.st_size), float(st.st_mtime), ext, mtype)
            batch.append(row)

            if len(batch) >= batch_size:
                db.upsert_files(batch)
                log.debug(f"Upserted {len(batch)} rows")
                batch.clear()

        if not list_only and batch:
            db.upsert_files(batch)
            log.debug(f"Upserted {len(batch)} rows (final batch)")

        log.info(f"[green]Scan complete[/] — candidates: {total}")

    except (InvalidPathError, ConfigLoadError) as exc:
        log.error(f"[red]Error:[/] {exc}")
        raise typer.Exit(code=1)

    except PdaiError as exc:
        log.error(f"[red]Internal error:[/] {exc}")
        raise typer.Exit(code=1)

    except Exception:
        log.exception("Unexpected error during scan")
        raise typer.Exit(code=1) from InternalError(
            "Unexpected failure. Re-run with -v for details."
        )
    finally:
        try:
            if db:
                db.close()
        except Exception:
            pass


@app.command("report")
def report_cmd(
    config_file: Optional[Path] = typer.Option(
        None, "--config", "-c", help="Path to dup.toml"
    ),
    json_out: bool = typer.Option(False, "--json", help="Emit JSON"),
) -> None:
    """
    Print file counts from the DB (totals + per media type).
    """
    db: Optional[Database] = None
    try:
        cfg = AppConfig.load(config_file)
        db = Database(cfg.scan.db_path)
        db.connect()

        total = db.count_files()
        by_type = db.count_by_media_type()

        if json_out:
            payload = {
                "total": total,
                "by_type": [{"media_type": k, "count": v} for k, v in by_type],
            }
            typer.echo(json.dumps(payload, ensure_ascii=False, indent=2))
        else:
            typer.echo(f"Total files: {total}")
            for k, v in by_type:
                typer.echo(f"  {k:>5}: {v}")

    except (InvalidPathError, ConfigLoadError) as exc:
        log.error(f"[red]Error:[/] {exc}")
        raise typer.Exit(code=1)
    except PdaiError as exc:
        log.error(f"[red]Internal error:[/] {exc}")
        raise typer.Exit(code=1)
    except Exception:
        log.exception("Unexpected error during report")
        raise typer.Exit(code=1) from InternalError(
            "Unexpected failure. Re-run with -v for details."
        )
    finally:
        try:
            if db:
                db.close()
        except Exception:
            pass
