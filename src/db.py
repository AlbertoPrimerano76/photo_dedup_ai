"""
SQLite database — Phase 8:
- Incremental bookkeeping (files.present, files.seen_at ISO timestamp)
- Staleness tracking for hashes (hashes.file_mtime, image_hashes.file_mtime)
- Back-compat wrappers for older call sites/tests (upsert_files, upsert_hashes)
"""

from __future__ import annotations

import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable, Optional, Tuple

from errors import PdaiError


class DatabaseError(PdaiError):
    """Raised when the SQLite database cannot be created or accessed."""


# Phase 8 schema version
DB_SCHEMA_VERSION = "6"

_SCHEMA = """
PRAGMA journal_mode=WAL;
PRAGMA synchronous=NORMAL;
PRAGMA foreign_keys=ON;

CREATE TABLE IF NOT EXISTS meta (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS files (
    id INTEGER PRIMARY KEY,
    path TEXT NOT NULL UNIQUE,   -- absolute path, normalized
    size INTEGER NOT NULL,       -- in bytes
    mtime REAL NOT NULL,         -- POSIX timestamp (float)
    ext TEXT NOT NULL,           -- lowercase extension with dot
    media_type TEXT NOT NULL,    -- image|raw|video|other
    present INTEGER NOT NULL DEFAULT 1,                -- 1=present, 0=missing
    seen_at TEXT NOT NULL DEFAULT ''                   -- ISO8601 last scan touch
);
CREATE INDEX IF NOT EXISTS idx_files_media ON files(media_type);
CREATE INDEX IF NOT EXISTS idx_files_ext ON files(ext);
CREATE INDEX IF NOT EXISTS idx_files_present ON files(present);

-- Exact hashes (Phase 5) + staleness (Phase 8)
CREATE TABLE IF NOT EXISTS hashes (
    file_id INTEGER PRIMARY KEY,
    blake3 TEXT NOT NULL,
    sha256 TEXT,
    file_mtime REAL NOT NULL DEFAULT 0,                -- mtime when hash was computed
    FOREIGN KEY(file_id) REFERENCES files(id) ON DELETE CASCADE
);
CREATE INDEX IF NOT EXISTS idx_hashes_blake3 ON hashes(blake3);

-- Perceptual image hashes (Phase 6) + staleness (Phase 8)
CREATE TABLE IF NOT EXISTS image_hashes (
    file_id INTEGER PRIMARY KEY,
    phash INTEGER NOT NULL,      -- 64-bit int
    dhash INTEGER NOT NULL,      -- 64-bit int
    width INTEGER,
    height INTEGER,
    file_mtime REAL NOT NULL DEFAULT 0,                -- mtime when phash/dhash were computed
    FOREIGN KEY(file_id) REFERENCES files(id) ON DELETE CASCADE
);
CREATE INDEX IF NOT EXISTS idx_image_hashes_phash ON image_hashes(phash);
CREATE INDEX IF NOT EXISTS idx_image_hashes_dhash ON image_hashes(dhash);

-- ORB confirmations cache (Phase 7)
CREATE TABLE IF NOT EXISTS near_confirms (
    src_file_id INTEGER NOT NULL,
    dst_file_id INTEGER NOT NULL,
    method TEXT NOT NULL,           -- 'orb'
    inliers INTEGER NOT NULL,
    inlier_ratio REAL NOT NULL,
    created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')),
    PRIMARY KEY (src_file_id, dst_file_id, method),
    FOREIGN KEY(src_file_id) REFERENCES files(id) ON DELETE CASCADE,
    FOREIGN KEY(dst_file_id) REFERENCES files(id) ON DELETE CASCADE
);
CREATE INDEX IF NOT EXISTS idx_near_confirms_method ON near_confirms(method);
"""


def _now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%fZ")


class Database:
    """Thin wrapper around sqlite3 with safe init & helpers."""

    def __init__(self, path: Path) -> None:
        self.path = path
        self._conn: Optional[sqlite3.Connection] = None

    # --- lifecycle ------------------------------------------------------------

    def connect(self) -> None:
        try:
            self.path.parent.mkdir(parents=True, exist_ok=True)
        except OSError as exc:
            raise DatabaseError(
                f"Cannot create DB directory: {self.path.parent}"
            ) from exc
        try:
            self._conn = sqlite3.connect(
                self.path.as_posix(), isolation_level=None
            )  # autocommit
            self._conn.execute("PRAGMA busy_timeout=5000;")
            self._conn.executescript(_SCHEMA)
            self._set_meta("schema_version", DB_SCHEMA_VERSION)
        except sqlite3.Error as exc:
            raise DatabaseError(
                f"Failed to open or initialize DB at {self.path}"
            ) from exc

    def close(self) -> None:
        if self._conn is not None:
            try:
                self._conn.close()
            except sqlite3.Error:
                pass
            finally:
                self._conn = None

    # --- low-level helpers ----------------------------------------------------

    def _ensure(self) -> sqlite3.Connection:
        if self._conn is None:
            raise DatabaseError("Database not connected. Call connect() first.")
        return self._conn

    def execute(self, sql: str, params: Tuple = ()) -> sqlite3.Cursor:
        try:
            return self._ensure().execute(sql, params)
        except sqlite3.Error as exc:
            raise DatabaseError(f"DB execute failed: {sql}") from exc

    def executemany(self, sql: str, rows: Iterable[Tuple]) -> None:
        try:
            self._ensure().executemany(sql, rows)
        except sqlite3.Error as exc:
            raise DatabaseError(f"DB executemany failed: {sql}") from exc

    # --- meta -----------------------------------------------------------------

    def _set_meta(self, key: str, value: str) -> None:
        self.execute(
            "INSERT INTO meta(key, value) VALUES(?, ?) "
            "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
            (key, value),
        )

    def get_meta(self, key: str) -> Optional[str]:
        cur = self.execute("SELECT value FROM meta WHERE key = ?", (key,))
        row = cur.fetchone()
        return row[0] if row else None

    # --- Phase 8: scan tokens & present/seen ----------------------------------

    def start_scan_token(self) -> str:
        """
        Returns ISO timestamp token to tag seen files in the current scan.
        """
        return _now_iso()

    def finalize_scan(self, scan_token: str) -> int:
        """
        Mark files not seen in this scan as missing (present=0).
        Returns number of rows affected.
        """
        cur = self.execute(
            "UPDATE files SET present=0 "
            "WHERE (seen_at='' OR seen_at < ?) AND present=1",
            (scan_token,),
        )
        return int(cur.rowcount or 0)

    # --- Phase 4 + Phase 8: files upsert with seen_at -------------------------

    def upsert_files_with_seen(
        self, rows: Iterable[Tuple[str, int, float, str, str, str]]
    ) -> None:
        """
        Upsert many files and stamp seen_at/present.
        Row tuple: (path, size, mtime, ext, media_type, seen_at_iso)
        """
        sql = (
            "INSERT INTO files(path, size, mtime, ext, media_type, present, seen_at) "
            "VALUES (?, ?, ?, ?, ?, 1, ?) "
            "ON CONFLICT(path) DO UPDATE SET "
            "  size=excluded.size, "
            "  mtime=excluded.mtime, "
            "  ext=excluded.ext, "
            "  media_type=excluded.media_type, "
            "  present=1, "
            "  seen_at=excluded.seen_at"
        )
        self.executemany(sql, rows)

    # --- Back-compat wrappers (for older tests/callers) -----------------------

    def upsert_files(self, rows: Iterable[Tuple[str, int, float, str, str]]) -> None:
        """
        Backward-compatible wrapper for Phase 4 signature:
        (path, size, mtime, ext, media_type).
        Stamps present=1 and seen_at=now.
        """
        seen = _now_iso()
        rows_seen = ((p, sz, mt, ext, mtp, seen) for (p, sz, mt, ext, mtp) in rows)
        self.upsert_files_with_seen(rows_seen)

    # --- counts & reporting ----------------------------------------------------

    def count_files(self) -> int:
        cur = self.execute("SELECT COUNT(*) FROM files")
        return int(cur.fetchone()[0])

    def count_by_media_type(self) -> list[Tuple[str, int]]:
        cur = self.execute(
            "SELECT media_type, COUNT(*) FROM files WHERE present=1 GROUP BY media_type ORDER BY COUNT(*) DESC"
        )
        return [(str(mt), int(cnt)) for (mt, cnt) in cur.fetchall()]

    # --- Phase 5: exact hashes + staleness ------------------------------------

    def upsert_hashes_with_mtime(
        self, rows: Iterable[Tuple[str, str, Optional[str], float]]
    ) -> None:
        """
        Row: (path, blake3, sha256, file_mtime)
        """
        sql = (
            "INSERT INTO hashes(file_id, blake3, sha256, file_mtime) "
            "SELECT id, ?, ?, ? FROM files WHERE path = ? "
            "ON CONFLICT(file_id) DO UPDATE SET "
            "  blake3=excluded.blake3, "
            "  sha256=excluded.sha256, "
            "  file_mtime=excluded.file_mtime"
        )
        # reorder to (b3, sha, mtime, path)
        reordered = ((b3, sha or None, mtime, p) for (p, b3, sha, mtime) in rows)
        self.executemany(sql, reordered)

    def upsert_hashes(self, rows: Iterable[Tuple[str, str, Optional[str]]]) -> None:
        """
        Backward-compatible wrapper for Phase 5 signature:
        (path, blake3, sha256).
        Uses current files.mtime for file_mtime.
        """
        out: list[Tuple[str, str, Optional[str], float]] = []
        for path, b3, sha in rows:
            cur = self.execute("SELECT mtime FROM files WHERE path = ?", (path,))
            row = cur.fetchone()
            mtime = float(row[0]) if row else 0.0
            out.append((path, b3, sha, mtime))
        self.upsert_hashes_with_mtime(out)

    def needs_exact_hash(self, path: str, file_mtime: float) -> bool:
        """
        Return True if no row in hashes OR stored file_mtime != current file_mtime.
        """
        cur = self.execute(
            "SELECT h.file_mtime FROM hashes h JOIN files f ON f.id = h.file_id WHERE f.path = ?",
            (path,),
        )
        row = cur.fetchone()
        return row is None or float(row[0]) != float(file_mtime)

    def exact_dupe_groups(self, limit: Optional[int] = None) -> list[Tuple[str, int]]:
        sql = (
            "SELECT blake3, COUNT(*) as c "
            "FROM hashes "
            "GROUP BY blake3 "
            "HAVING COUNT(*) > 1 "
            "ORDER BY c DESC"
        )
        if limit is not None:
            sql += f" LIMIT {int(limit)}"
        cur = self.execute(sql)
        return [(str(b3), int(c)) for (b3, c) in cur.fetchall()]

    def paths_for_blake3(
        self, blake3_hex: str, limit: Optional[int] = None
    ) -> list[str]:
        sql = (
            "SELECT f.path FROM hashes h "
            "JOIN files f ON f.id = h.file_id "
            "WHERE h.blake3 = ? "
            "ORDER BY f.path"
        )
        if limit is not None:
            sql += f" LIMIT {int(limit)}"
        cur = self.execute(sql, (blake3_hex,))
        return [str(row[0]) for row in cur.fetchall()]

    # --- Phase 6: image hashes + staleness ------------------------------------

    def upsert_image_hashes_with_mtime(
        self, rows: Iterable[Tuple[str, int, int, int, int, float]]
    ) -> None:
        """
        Row: (path, phash64, dhash64, width, height, file_mtime)
        """
        sql = (
            "INSERT INTO image_hashes(file_id, phash, dhash, width, height, file_mtime) "
            "SELECT id, ?, ?, ?, ?, ? FROM files WHERE path = ? "
            "ON CONFLICT(file_id) DO UPDATE SET "
            "  phash=excluded.phash, "
            "  dhash=excluded.dhash, "
            "  width=excluded.width, "
            "  height=excluded.height, "
            "  file_mtime=excluded.file_mtime"
        )
        # reorder to (phash, dhash, w, h, mtime, path)
        reordered = (
            (p64, d64, w, h, mtime, path) for (path, p64, d64, w, h, mtime) in rows
        )
        self.executemany(sql, reordered)

    def iter_paths_needing_image_hashes(
        self, batch: int = 2000
    ) -> Iterable[list[Tuple[str, float]]]:
        """
        Yield (path, file_mtime) for images that need (re)hashing:
        - missing in image_hashes OR image_hashes.file_mtime != files.mtime
        Only considers present=1.
        """
        offset = 0
        while True:
            cur = self.execute(
                "SELECT f.path, f.mtime FROM files f "
                "LEFT JOIN image_hashes ih ON ih.file_id = f.id "
                "WHERE f.media_type = 'image' AND f.present=1 "
                "AND (ih.file_id IS NULL OR ih.file_mtime != f.mtime) "
                "ORDER BY f.id LIMIT ? OFFSET ?",
                (batch, offset),
            )
            rows = [(str(r[0]), float(r[1])) for r in cur.fetchall()]
            if not rows:
                break
            yield rows
            offset += batch

    def load_all_image_hashes(self) -> list[Tuple[str, int, int]]:
        cur = self.execute(
            "SELECT f.path, ih.phash, ih.dhash "
            "FROM image_hashes ih JOIN files f ON f.id = ih.file_id "
            "WHERE f.present=1 "
            "ORDER BY f.id"
        )
        return [(str(p), int(ph), int(dh)) for (p, ph, dh) in cur.fetchall()]

    # --- Phase 7: ORB confirmations ------------------------------------------

    def upsert_orb_confirm(self, pairs: Iterable[Tuple[str, str, int, float]]) -> None:
        sql = (
            "INSERT INTO near_confirms(src_file_id, dst_file_id, method, inliers, inlier_ratio) "
            "SELECT s.id, d.id, 'orb', ?, ? "
            "FROM files s, files d "
            "WHERE s.path = ? AND d.path = ? "
            "ON CONFLICT(src_file_id, dst_file_id, method) DO UPDATE SET "
            "  inliers=excluded.inliers, inlier_ratio=excluded.inlier_ratio"
        )
        reordered = ((inl, ratio, sp, dp) for (sp, dp, inl, ratio) in pairs)
        self.executemany(sql, reordered)

    def confirmed_pairs(self, limit: int = 50) -> list[Tuple[str, str, int, float]]:
        cur = self.execute(
            "SELECT fs.path, fd.path, c.inliers, c.inlier_ratio "
            "FROM near_confirms c "
            "JOIN files fs ON fs.id = c.src_file_id "
            "JOIN files fd ON fd.id = c.dst_file_id "
            "WHERE c.method = 'orb' "
            "ORDER BY c.created_at DESC, c.inliers DESC "
            "LIMIT ?",
            (int(limit),),
        )
        return [
            (str(a), str(b), int(inl), float(r)) for (a, b, inl, r) in cur.fetchall()
        ]

    # --- Candidate generation (same logic as CLI 'near') ----------------------

    def phash_dhash_candidates(
        self,
        phash_threshold: int,
        dhash_threshold: int,
        limit_pairs: int | None = None,
    ) -> list[Tuple[str, str]]:
        rows = self.load_all_image_hashes()

        def top_bits(x: int, bits: int = 16) -> int:
            return (x >> (64 - bits)) & ((1 << bits) - 1)

        buckets: dict[int, list[tuple[str, int, int]]] = {}
        for p, ph, dh in rows:
            buckets.setdefault(top_bits(ph), []).append((p, ph, dh))
        cand: list[tuple[str, str]] = []
        for _, items in buckets.items():
            for i in range(len(items)):
                pi, phi, dhi = items[i]
                for j in range(i + 1, len(items)):
                    pj, phj, dhj = items[j]
                    if (
                        bin(phi ^ phj).count("1") <= phash_threshold
                        and bin(dhi ^ dhj).count("1") <= dhash_threshold
                    ):
                        cand.append((pi, pj))
                        if limit_pairs is not None and len(cand) >= limit_pairs:
                            return cand
        return cand
