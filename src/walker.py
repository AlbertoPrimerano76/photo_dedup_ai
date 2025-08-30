"""
Streaming filesystem walker.

- Iterates multiple roots without loading everything in memory.
- Respects: ignore_hidden, follow_symlinks, include_ext.
- Yields (path, ext_lower) for candidate files.
- Best-effort cycle guard when following symlinks (tracks (st_dev, st_ino)).
"""

from __future__ import annotations

import os
from pathlib import Path
from typing import Generator, Iterable, Set, Tuple

from errors import InvalidPathError


def _is_hidden_path(p: Path) -> bool:
    return any(part.startswith(".") for part in p.parts)


def iter_media_files(
    roots: Iterable[Path],
    *,
    include_ext: Iterable[str],
    ignore_hidden: bool = True,
    follow_symlinks: bool = False,
) -> Generator[Tuple[Path, str], None, None]:
    """
    Yield (absolute_path, lower_ext) for files whose extension is in include_ext.

    Raises:
        InvalidPathError: if any root is missing or not a directory.
    """
    roots_norm: list[Path] = []
    for r in roots:
        try:
            rp = r.expanduser().resolve()
        except Exception as exc:
            raise InvalidPathError(f"Invalid root: {r}") from exc
        if not rp.exists():
            raise InvalidPathError(f"Root not found: {rp}")
        if not rp.is_dir():
            raise InvalidPathError(f"Not a directory: {rp}")
        roots_norm.append(rp)

    include = {ext.lower() for ext in include_ext}

    visited_dirs: Set[Tuple[int, int]] = set()  # (st_dev, st_ino) for cycles

    for root in roots_norm:
        for dirpath, dirnames, filenames in os.walk(root, followlinks=follow_symlinks):
            pdir = Path(dirpath)

            if ignore_hidden and _is_hidden_path(pdir):
                dirnames[:] = []  # prune traversal
                continue

            if follow_symlinks:
                try:
                    st = os.stat(pdir, follow_symlinks=True)
                    key = (st.st_dev, st.st_ino)
                    if key in visited_dirs:
                        dirnames[:] = []
                        continue
                    visited_dirs.add(key)
                except OSError:
                    dirnames[:] = []
                    continue

            for name in filenames:
                ext = Path(name).suffix.lower()
                if ext in include:
                    yield (pdir / name, ext)
