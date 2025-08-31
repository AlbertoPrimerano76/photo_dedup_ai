"""
SQLite database with files, exact hashes, and image perceptual hashes (Phase 6).
"""

from __future__ import annotations

import sqlite3
from pathlib import Path
from typing import Iterable, Optional, Tuple

from errors import PdaiError


class DatabaseError(PdaiError):
    """Raised when the SQLite database cannot be created or accessed."""


# Bump schema version for Phase 6
DB_SCHEMA_VERSION = "4"

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
    media_type TEXT NOT NULL     -- image|raw|video|other
);

CREATE INDEX IF NOT EXISTS idx_files_media ON files(media_type);
CREATE INDEX IF NOT EXISTS idx_files_ext ON files(ext);

-- Phase 5: exact hashes
CREATE TABLE IF NOT EXISTS hashes (
    file_id INTEGER PRIMARY KEY,
    blake3 TEXT NOT NULL,
    sha256 TEXT,
    FOREIGN KEY(file_id) REFERENCES files(id) ON DELETE CASCADE
);
CREATE INDEX IF NOT EXISTS idx_hashes_blake3 ON hashes(blake3);

-- Phase 6: perceptual hashes for images
CREATE TABLE IF NOT EXISTS image_hashes (
    file_id INTEGER PRIMARY KEY,
    phash INTEGER NOT NULL,      -- 64-bit int
    dhash INTEGER NOT NULL,      -- 64-bit int
    width INTEGER,
    height INTEGER,
    FOREIGN KEY(file_id) REFERENCES files(id) ON DELETE CASCADE
);
CREATE INDEX IF NOT EXISTS idx_image_hashes_phash ON image_hashes(phash);
CREATE INDEX IF NOT EXISTS idx_image_hashes_dhash ON image_hashes(dhash);
"""


class Database:
    """Thin wrapper around sqlite3 with safe init & context management."""

    def __init__(self, path: Path) -> None:
        self.path = path
        self._conn: Optional[sqlite3.Connection] = None

    def connect(self) -> None:
        """Create parent dirs, open connection, and apply schema."""
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

    # --- basic helpers --------------------------------------------------------

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

    # --- files persistence (Phase 4) ------------------------------------------

    def upsert_files(self, rows: Iterable[Tuple[str, int, float, str, str]]) -> None:
        sql = (
            "INSERT INTO files(path, size, mtime, ext, media_type) "
            "VALUES (?, ?, ?, ?, ?) "
            "ON CONFLICT(path) DO UPDATE SET "
            "  size=excluded.size, "
            "  mtime=excluded.mtime, "
            "  ext=excluded.ext, "
            "  media_type=excluded.media_type"
        )
        self.executemany(sql, rows)

    def count_files(self) -> int:
        cur = self.execute("SELECT COUNT(*) FROM files")
        return int(cur.fetchone()[0])

    def count_by_media_type(self) -> list[Tuple[str, int]]:
        cur = self.execute(
            "SELECT media_type, COUNT(*) FROM files GROUP BY media_type ORDER BY COUNT(*) DESC"
        )
        return [(str(mt), int(cnt)) for (mt, cnt) in cur.fetchall()]

    # --- exact hashes (Phase 5) -----------------------------------------------

    def upsert_hashes(self, rows: Iterable[Tuple[str, str, Optional[str]]]) -> None:
        sql = (
            "INSERT INTO hashes(file_id, blake3, sha256) "
            "SELECT id, ?, ? FROM files WHERE path = ? "
            "ON CONFLICT(file_id) DO UPDATE SET "
            "  blake3=excluded.blake3, "
            "  sha256=excluded.sha256"
        )
        reordered = ((b3, sha or None, p) for (p, b3, sha) in rows)
        self.executemany(sql, reordered)

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

    # --- image perceptual hashes (Phase 6) ------------------------------------

    def upsert_image_hashes(
        self, rows: Iterable[Tuple[str, int, int, int, int]]
    ) -> None:
        """
        Upsert many image hashes.
        Row tuple: (path, phash64, dhash64, width, height).
        """
        sql = (
            "INSERT INTO image_hashes(file_id, phash, dhash, width, height) "
            "SELECT id, ?, ?, ?, ? FROM files WHERE path = ? "
            "ON CONFLICT(file_id) DO UPDATE SET "
            "  phash=excluded.phash, "
            "  dhash=excluded.dhash, "
            "  width=excluded.width, "
            "  height=excluded.height"
        )
        # Reorder to (phash, dhash, width, height, path)
        reordered = ((p64, d64, w, h, path) for (path, p64, d64, w, h) in rows)
        self.executemany(sql, reordered)

    def iter_paths_missing_image_hashes(self, batch: int = 2000) -> Iterable[list[str]]:
        """
        Yield paths that are images (files.media_type='image') and missing image_hashes.
        Batches results to reduce memory pressure.
        """
        offset = 0
        while True:
            cur = self.execute(
                "SELECT f.path FROM files f "
                "LEFT JOIN image_hashes ih ON ih.file_id = f.id "
                "WHERE f.media_type = 'image' AND ih.file_id IS NULL "
                "ORDER BY f.id LIMIT ? OFFSET ?",
                (batch, offset),
            )
            rows = [str(r[0]) for r in cur.fetchall()]
            if not rows:
                break
            yield rows
            offset += batch

    def load_all_image_hashes(self) -> list[Tuple[str, int, int]]:
        """
        Return list of (path, phash, dhash) for all images that have hashes.
        """
        cur = self.execute(
            "SELECT f.path, ih.phash, ih.dhash "
            "FROM image_hashes ih JOIN files f ON f.id = ih.file_id "
            "ORDER BY f.id"
        )
        return [(str(p), int(ph), int(dh)) for (p, ph, dh) in cur.fetchall()]
