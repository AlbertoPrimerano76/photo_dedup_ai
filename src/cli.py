"""
CLI Phase 5:
- scan: computes BLAKE3 (and optional SHA-256) and persists into DB.
- dupes: lists exact duplicate groups (by BLAKE3), optionally printing paths.
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
from hash_exact import compute_hashes
from logs import get_logger, init_logging
from media import media_type_from_ext
from walker import iter_media_files

app = typer.Typer(
    add_completion=False,
    no_args_is_help=True,
    help="Photo Dedup AI — CLI (Phase 5: exact hashes + dupes report)",
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
    typer.echo("photo-dedup-ai v0.0.5")


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
    batch_size: int = typer.Option(800, "--batch-size", help="DB upsert batch size"),
    sha256: bool = typer.Option(
        False, "--sha256", help="Also compute SHA-256 (slower)"
    ),
) -> None:
    """
    Stream candidate media files.
    - With --list-only (default): print paths (no DB changes).
    - With --no-list-only: persist into SQLite (files + hashes).
    """
    db: Optional[Database] = None
    try:
        cfg = AppConfig.load(config_file)

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

        file_batch: list[tuple[str, int, float, str, str]] = []
        hash_batch: list[tuple[str, str, Optional[str]]] = []

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

            # Stats
            try:
                st = os.stat(path, follow_symlinks=True)
            except OSError:
                # Skip unreadable files
                continue

            # Queue file row
            mtype = media_type_from_ext(ext)
            file_batch.append(
                (str(path), int(st.st_size), float(st.st_mtime), ext, mtype)
            )

            # Compute hashes
            try:
                b3, s256 = compute_hashes(path, with_sha256=sha256)
                hash_batch.append((str(path), b3, s256))
            except OSError:
                # Skip hashing failures; file still exists in DB without hash
                continue

            # Flush in order: files first (so hashes subselect finds file_id), then hashes
            if len(file_batch) >= batch_size:
                db.upsert_files(file_batch)
                db.upsert_hashes(hash_batch)
                log.debug(f"Upserted {len(file_batch)} files, {len(hash_batch)} hashes")
                file_batch.clear()
                hash_batch.clear()

        if not list_only:
            if file_batch:
                db.upsert_files(file_batch)
            if hash_batch:
                db.upsert_hashes(hash_batch)
            if file_batch or hash_batch:
                log.debug(
                    f"Upserted final batch: {len(file_batch)} files, {len(hash_batch)} hashes"
                )

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


@app.command("dupes")
def dupes_cmd(
    config_file: Optional[Path] = typer.Option(
        None, "--config", "-c", help="Path to dup.toml"
    ),
    limit: Optional[int] = typer.Option(
        20, "--limit", help="Max groups to display (None for all)"
    ),
    show_paths: bool = typer.Option(
        False, "--paths", help="Print file paths for each group"
    ),
    json_out: bool = typer.Option(False, "--json", help="Emit JSON"),
) -> None:
    """
    Show groups of exact duplicates (same BLAKE3).
    """
    db: Optional[Database] = None
    try:
        cfg = AppConfig.load(config_file)
        db = Database(cfg.scan.db_path)
        db.connect()

        groups = db.exact_dupe_groups(limit)

        if json_out:
            payload = []
            for b3, count in groups:
                entry = {"blake3": b3, "count": count}
                if show_paths:
                    entry["paths"] = db.paths_for_blake3(b3)
                payload.append(entry)
            typer.echo(json.dumps(payload, ensure_ascii=False, indent=2))
            return

        if not groups:
            typer.echo("No exact duplicate groups found.")
            return

        for i, (b3, count) in enumerate(groups, 1):
            typer.echo(f"{i:3}. {b3}  x{count}")
            if show_paths:
                for p in db.paths_for_blake3(b3):
                    typer.echo(f"     - {p}")

    except (InvalidPathError, ConfigLoadError) as exc:
        log.error(f"[red]Error:[/] {exc}")
        raise typer.Exit(code=1)
    except PdaiError as exc:
        log.error(f"[red]Internal error:[/] {exc}")
        raise typer.Exit(code=1)
    except Exception:
        log.exception("Unexpected error during dupes report")
        raise typer.Exit(code=1) from InternalError(
            "Unexpected failure. Re-run with -v for details."
        )
    finally:
        try:
            if db:
                db.close()
        except Exception:
            pass
