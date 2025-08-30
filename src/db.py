"""
SQLite database bootstrap + files persistence for Photo Dedup AI (Phase 4).

Adds:
- `files` table (path UNIQUE, size, mtime, ext, media_type)
- batch upsert helpers
- simple counters for report
"""

from __future__ import annotations

import sqlite3
from pathlib import Path
from typing import Iterable, Optional, Tuple

from errors import PdaiError


class DatabaseError(PdaiError):
    """Raised when the SQLite database cannot be created or accessed."""


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
            self._set_meta("schema_version", "2")
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

    # --- files persistence ----------------------------------------------------

    def upsert_files(self, rows: Iterable[Tuple[str, int, float, str, str]]) -> None:
        """
        Upsert many files.
        Row tuple shape: (path, size, mtime, ext, media_type)
        """
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
