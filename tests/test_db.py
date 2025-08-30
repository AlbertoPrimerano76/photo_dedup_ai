from __future__ import annotations

from pathlib import Path

from db import Database


def test_db_initializes(tmp_path: Path) -> None:
    db_path = tmp_path / "idx.sqlite3"
    db = Database(db_path)
    db.connect()
    try:
        assert db.get_meta("schema_version") == "3"
    finally:
        db.close()
