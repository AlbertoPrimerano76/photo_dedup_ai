from __future__ import annotations

from pathlib import Path

from hash_exact import compute_hashes


def test_compute_hashes_basic(tmp_path: Path) -> None:
    f1 = tmp_path / "a.bin"
    f2 = tmp_path / "b.bin"

    f1.write_bytes(b"hello world")
    f2.write_bytes(b"hello world")

    b31, s1 = compute_hashes(f1, with_sha256=True)
    b32, s2 = compute_hashes(f2, with_sha256=True)
    assert b31 == b32
    assert s1 == s2
    assert len(b31) == 64  # hex length for 32-byte digest
    assert len(s1 or "") == 64

    f2.write_bytes(b"HELLO WORLD")
    b33, s3 = compute_hashes(f2, with_sha256=True)
    assert b31 != b33
    assert s1 != s3
