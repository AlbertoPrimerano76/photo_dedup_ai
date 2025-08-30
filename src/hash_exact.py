"""
Exact hashing utilities (Phase 5).

- BLAKE3 for fast, collision-resistant content hashing.
- Optional SHA-256 (for verification/export; slower).
- Stream files in chunks to keep memory low.
"""

from __future__ import annotations

from pathlib import Path
from typing import Optional, Tuple

from blake3 import blake3  # type: ignore[import-untyped]
import hashlib


_CHUNK = 1024 * 1024  # 1 MiB


def compute_hashes(
    path: Path, *, with_sha256: bool = False
) -> Tuple[str, Optional[str]]:
    """
    Compute BLAKE3 (and optionally SHA-256) for a file.

    Args:
        path: filesystem path to the file.
        with_sha256: also compute SHA-256 if True.

    Returns:
        (blake3_hex, sha256_hex or None)

    Raises:
        OSError: if the file cannot be read.
    """
    b3 = blake3()
    sha = hashlib.sha256() if with_sha256 else None

    with path.open("rb") as f:
        while True:
            chunk = f.read(_CHUNK)
            if not chunk:
                break
            b3.update(chunk)
            if sha is not None:
                sha.update(chunk)

    return b3.hexdigest(), (sha.hexdigest() if sha is not None else None)
