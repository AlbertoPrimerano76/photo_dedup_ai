"""
CLI entrypoint (Phase 7):
- scan: list / persist files + exact hashes
- images-hash: calcola pHash/dHash e salva
- dupes: gruppi di duplicati esatti (BLAKE3)
- near: cluster near-duplicate da pHash/dHash
- confirm-near: conferma coppie con ORB+RANSAC e cache in DB
"""

from __future__ import annotations

import json
import os
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import typer

from config import AppConfig
from db import Database
from errors import ConfigLoadError, InvalidPathError, InternalError, PdaiError
from hash_exact import compute_hashes
from image_match import orb_ransac_confirm
from image_phash import dhash64, hamming64, phash64
from logs import get_logger, init_logging
from media import media_type_from_ext
from walker import iter_media_files

app = typer.Typer(
    add_completion=False,
    no_args_is_help=True,
    help="Photo Dedup AI — CLI (Phase 7: exact+perceptual+ORB)",
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
    # HEIC/HEIF opener se disponibile
    try:
        import pillow_heif  # type: ignore[import-not-found]

        pillow_heif.register_heif_opener()  # type: ignore[no-untyped-call]
        log.debug("HEIF/HEIC opener registered")
    except Exception:
        pass


@app.command("version")
def version_cmd() -> None:
    typer.echo("photo-dedup-ai v0.0.7")


# -------------------------------- scan ----------------------------------------


@app.command("scan")
def scan_cmd(
    folder: Optional[Path] = typer.Argument(None, help="Folder to scan (optional)"),
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
    Stream dei media file.
    --list-only: stampa i path
    --no-list-only: persist a DB (files + hashes)
    """
    db: Optional[Database] = None
    try:
        cfg = AppConfig.load(config_file)
        db = Database(cfg.scan.db_path)
        db.connect()
        log.info(f"DB ready at: {cfg.scan.db_path}")

        roots = [folder.resolve()] if folder else cfg.scan.roots
        if not roots:
            raise InvalidPathError("No folder provided and no roots configured.")

        log.info(f"[bold]Scan starting[/]: {len(roots)} root(s)")
        total = 0
        file_batch: list[Tuple[str, int, float, str, str]] = []
        hash_batch: list[Tuple[str, str, Optional[str]]] = []

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

            try:
                st = os.stat(path, follow_symlinks=True)
            except OSError:
                continue

            mtype = media_type_from_ext(ext)
            file_batch.append(
                (str(path), int(st.st_size), float(st.st_mtime), ext, mtype)
            )

            try:
                b3, s256 = compute_hashes(path, with_sha256=sha256)
                hash_batch.append((str(path), b3, s256))
            except OSError:
                continue

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
        if db:
            db.close()


# ------------------------------- dupes ----------------------------------------


@app.command("dupes")
def dupes_cmd(
    config_file: Optional[Path] = typer.Option(
        None, "--config", "-c", help="Path to dup.toml"
    ),
    limit: Optional[int] = typer.Option(20, "--limit", help="Max groups to display"),
    show_paths: bool = typer.Option(
        False, "--paths", help="Show file paths in each group"
    ),
    json_out: bool = typer.Option(False, "--json", help="Emit JSON"),
) -> None:
    """Gruppi di duplicati esatti (stesso BLAKE3)."""
    db: Optional[Database] = None
    try:
        cfg = AppConfig.load(config_file)
        db = Database(cfg.scan.db_path)
        db.connect()
        groups = db.exact_dupe_groups(limit)

        if json_out:
            payload = [
                {
                    "blake3": b3,
                    "count": count,
                    "paths": db.paths_for_blake3(b3) if show_paths else None,
                }
                for b3, count in groups
            ]
            typer.echo(json.dumps(payload, ensure_ascii=False, indent=2))
            return

        if not groups:
            typer.echo("No exact duplicates found.")
            return

        for i, (b3, count) in enumerate(groups, 1):
            typer.echo(f"{i:3}. {b3}  x{count}")
            if show_paths:
                for p in db.paths_for_blake3(b3):
                    typer.echo(f"   - {p}")

    except (InvalidPathError, ConfigLoadError) as exc:
        log.error(f"Error: {exc}")
        raise typer.Exit(code=1)
    finally:
        if db:
            db.close()


# ---------------------------- images-hash -------------------------------------


@app.command("images-hash")
def images_hash_cmd(
    config_file: Optional[Path] = typer.Option(
        None, "--config", "-c", help="Path to dup.toml"
    ),
    limit: Optional[int] = typer.Option(
        None, "--limit", help="Limit number of images to hash"
    ),
    batch_size: int = typer.Option(512, "--batch-size", help="DB upsert batch size"),
) -> None:
    """Calcola pHash/dHash per immagini mancanti e salva in DB."""
    from PIL import Image

    db: Optional[Database] = None
    try:
        cfg = AppConfig.load(config_file)
        db = Database(cfg.scan.db_path)
        db.connect()

        to_go = limit if limit is not None else float("inf")
        total = 0
        for paths in db.iter_paths_missing_image_hashes(batch=batch_size):
            rows = []
            for spath in paths:
                if to_go <= 0:
                    break
                p = Path(spath)
                try:
                    with Image.open(p) as im:
                        w, h = im.size
                    p64 = phash64(p)
                    d64 = dhash64(p)
                    rows.append((str(p), p64, d64, w, h))
                    total += 1
                    to_go -= 1
                except Exception:
                    log.debug(f"skip (cannot hash): {p}")
                    continue
            if rows:
                db.upsert_image_hashes(rows)
                log.info(f"Hashed {len(rows)} images (total {total})")
            if to_go <= 0:
                break
        if total == 0:
            log.info("No new images needed hashing.")
        else:
            log.info(f"[green]Done[/] — hashed images: {total}")

    except Exception as exc:
        log.error(f"Error: {exc}")
        raise typer.Exit(code=1)
    finally:
        if db:
            db.close()


# -------------------------------- near ----------------------------------------


@dataclass
class _Item:
    path: str
    phash: int
    dhash: int


class _DSU:
    def __init__(self, n: int) -> None:
        self.p = list(range(n))
        self.r = [0] * n

    def find(self, x: int) -> int:
        while self.p[x] != x:
            self.p[x] = self.p[self.p[x]]
            x = self.p[x]
        return x

    def union(self, a: int, b: int) -> None:
        ra, rb = self.find(a), self.find(b)
        if ra == rb:
            return
        if self.r[ra] < self.r[rb]:
            ra, rb = rb, ra
        self.p[rb] = ra
        if self.r[ra] == self.r[rb]:
            self.r[ra] += 1


@app.command("near")
def near_cmd(
    config_file: Optional[Path] = typer.Option(
        None, "--config", "-c", help="Path to dup.toml"
    ),
    phash_threshold: int = typer.Option(
        10, "--phash-threshold", help="Max Hamming for pHash"
    ),
    dhash_threshold: int = typer.Option(
        10, "--dhash-threshold", help="Max Hamming for dHash"
    ),
    json_out: bool = typer.Option(False, "--json", help="Emit JSON clusters"),
    show_paths: bool = typer.Option(
        False, "--paths", help="Print file paths for each cluster"
    ),
) -> None:
    """Cluster near-duplicate via pHash/dHash (senza ORB)."""
    db: Optional[Database] = None
    try:
        cfg = AppConfig.load(config_file)
        db = Database(cfg.scan.db_path)
        db.connect()
        rows = db.load_all_image_hashes()
        items = [_Item(p, ph, dh) for (p, ph, dh) in rows]
        n = len(items)
        if n == 0:
            typer.echo("No image hashes found. Run: pdai images-hash")
            raise typer.Exit()

        dsu = _DSU(n)
        for i in range(n):
            for j in range(i + 1, n):
                if (
                    hamming64(items[i].phash, items[j].phash) <= phash_threshold
                    and hamming64(items[i].dhash, items[j].dhash) <= dhash_threshold
                ):
                    dsu.union(i, j)

        clusters: Dict[int, List[str]] = {}
        for idx, it in enumerate(items):
            root = dsu.find(idx)
            if root != idx:
                clusters.setdefault(root, []).append(it.path)

        result = [[items[root].path] + members for root, members in clusters.items()]
        result = [grp for grp in result if len(grp) >= 2]

        if not result:
            typer.echo("No near-duplicate clusters found.")
            return

        if json_out:
            payload = [{"paths": grp, "size": len(grp)} for grp in result]
            typer.echo(json.dumps(payload, indent=2))
        else:
            for i, grp in enumerate(result, 1):
                typer.echo(f"Cluster {i} (size {len(grp)}):")
                if show_paths:
                    for p in grp:
                        typer.echo(f"  - {p}")

    except Exception as exc:
        log.error(f"Error: {exc}")
        raise typer.Exit(code=1)
    finally:
        if db:
            db.close()


# --------------------------- confirm-near (ORB) --------------------------------


@app.command("confirm-near")
def confirm_near_cmd(
    config_file: Optional[Path] = typer.Option(
        None, "--config", "-c", help="Path to dup.toml"
    ),
    phash_threshold: int = typer.Option(
        10, "--phash-threshold", help="Max Hamming pHash (candidates)"
    ),
    dhash_threshold: int = typer.Option(
        10, "--dhash-threshold", help="Max Hamming dHash (candidates)"
    ),
    use_db_candidates: bool = typer.Option(
        False, "--db-candidates", help="Candidati via DB (self-join)"
    ),
    limit_pairs: Optional[int] = typer.Option(
        200, "--limit", help="Max coppie da processare"
    ),
    max_workers: int = typer.Option(
        4, "--max-workers", help="Parallel ORB verifications"
    ),
    cache: bool = typer.Option(True, "--cache/--no-cache", help="Salva conferme in DB"),
    min_inliers: int = typer.Option(20, "--min-inliers", help="Min inliers RANSAC"),
    min_inlier_ratio: float = typer.Option(
        0.15, "--min-inlier-ratio", help="Min inlier ratio RANSAC"
    ),
    json_out: bool = typer.Option(False, "--json", help="Emit JSON"),
) -> None:
    """
    Conferma coppie near-duplicate via ORB+RANSAC e (opzionalmente) cache.
    """
    db: Optional[Database] = None
    try:
        cfg = AppConfig.load(config_file)
        db = Database(cfg.scan.db_path)
        db.connect()

        # Build candidate pairs
        pairs: list[tuple[str, str]]
        if use_db_candidates:
            pairs = db.phash_dhash_candidates(
                phash_threshold, dhash_threshold, limit_pairs=limit_pairs
            )
        else:
            rows = db.load_all_image_hashes()

            def top_bits(x: int, bits: int = 16) -> int:
                return (x >> (64 - bits)) & ((1 << bits) - 1)

            buckets: Dict[int, list[tuple[str, int, int]]] = {}
            for p, ph, dh in rows:
                buckets.setdefault(top_bits(ph), []).append((p, ph, dh))
            pairs = []  # <-- no type annotation here
            for _, items in buckets.items():
                for i in range(len(items)):
                    pi, phi, dhi = items[i]
                    for j in range(i + 1, len(items)):
                        pj, phj, dhj = items[j]
                        if (
                            bin(phi ^ phj).count("1") <= phash_threshold
                            and bin(dhi ^ dhj).count("1") <= dhash_threshold
                        ):
                            pairs.append((pi, pj))
                            if limit_pairs is not None and len(pairs) >= limit_pairs:
                                break
                if limit_pairs is not None and len(pairs) >= limit_pairs:
                    break

        if not pairs:
            typer.echo("No candidate pairs to confirm.")
            return

        results: list[tuple[str, str, bool, int, float]] = []

        def worker(sp: str, dp: str) -> tuple[str, str, bool, int, float]:
            ok, inl, ratio = orb_ransac_confirm(
                Path(sp),
                Path(dp),
                min_inliers=min_inliers,
                min_inlier_ratio=min_inlier_ratio,
            )
            return (sp, dp, ok, inl, ratio)

        with ThreadPoolExecutor(max_workers=max_workers) as ex:
            futs = [ex.submit(worker, a, b) for (a, b) in pairs]
            for f in as_completed(futs):
                sp, dp, ok, inl, ratio = f.result()
                if ok:
                    results.append((sp, dp, ok, inl, ratio))

        if not results:
            typer.echo("No pairs confirmed by ORB.")
            return

        if cache:
            db.upsert_orb_confirm(
                [(sp, dp, inl, ratio) for (sp, dp, _ok, inl, ratio) in results]
            )

        if json_out:
            payload = [
                {"src": sp, "dst": dp, "inliers": inl, "inlier_ratio": ratio}
                for (sp, dp, _ok, inl, ratio) in results
            ]
            typer.echo(json.dumps(payload, ensure_ascii=False, indent=2))
        else:
            for sp, dp, _ok, inl, ratio in results:
                typer.echo(f"[OK] inliers={inl:3d} ratio={ratio:.2f}  {sp}  <>  {dp}")

    except Exception as exc:
        log.error(f"Error: {exc}")
        raise typer.Exit(code=1)
    finally:
        if db:
            db.close()
