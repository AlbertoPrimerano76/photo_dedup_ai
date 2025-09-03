from __future__ import annotations

import os
import time
from pathlib import Path

from db import Database


def test_incremental_flags(tmp_path: Path) -> None:
    # create a file and insert into DB
    f = tmp_path / "a.jpg"
    f.write_bytes(b"x")
    st = os.stat(f)
    db = Database(tmp_path / "idx.sqlite3")
    db.connect()
    try:
        token = db.start_scan_token()
        db.upsert_files_with_seen(
            [(str(f), int(st.st_size), float(st.st_mtime), ".jpg", "image", token)]
        )
        # Initially, needs_exact_hash = True
        assert db.needs_exact_hash(str(f), float(st.st_mtime)) is True
        # After inserting a hash with matching mtime, it should be False
        db.upsert_hashes_with_mtime([(str(f), "deadbeef", None, float(st.st_mtime))])
        assert db.needs_exact_hash(str(f), float(st.st_mtime)) is False

        # Touch the file (change mtime) -> becomes True again
        time.sleep(0.01)
        f.write_bytes(b"xx")
        st2 = os.stat(f)
        assert db.needs_exact_hash(str(f), float(st2.st_mtime)) is True
    finally:
        db.close()
