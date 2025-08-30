from __future__ import annotations

import os
from pathlib import Path

from db import Database
from hash_exact import compute_hashes


def test_exact_dupe_groups(tmp_path: Path) -> None:
    # Build two identical files and one different
    a = tmp_path / "a.jpg"
    b = tmp_path / "b.jpg"
    c = tmp_path / "c.jpg"
    a.write_bytes(b"xyz")
    b.write_bytes(b"xyz")
    c.write_bytes(b"zzz")

    # Setup DB
    db = Database(tmp_path / "idx.sqlite3")
    db.connect()
    try:
        # Insert files first
        rows = []
        for p in (a, b, c):
            st = os.stat(p)
            rows.append((str(p), int(st.st_size), float(st.st_mtime), ".jpg", "image"))
        db.upsert_files(rows)

        # Insert hashes
        hrows = []
        for p in (a, b, c):
            b3, s2 = compute_hashes(p, with_sha256=False)
            hrows.append((str(p), b3, None))
        db.upsert_hashes(hrows)

        groups = db.exact_dupe_groups()
        # Expect 1 group (a,b)
        assert len(groups) == 1
        b3, count = groups[0]
        assert count == 2

        paths = db.paths_for_blake3(b3)
        assert set(map(Path, paths)) == {a, b}
    finally:
        db.close()
