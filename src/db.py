"""
SQLite database bootstrap for Photo Dedup AI.

Phase 3:
- Ensure the DB exists at the configured path (create parents).
- Create initial schema (idempotent).
- Provide a tiny DAO for connectivity and meta values.

We’ll extend the schema (files, hashes, fingerprints) in the next phase.
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
            self._set_meta("schema_version", "1")
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
