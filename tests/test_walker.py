from __future__ import annotations

from pathlib import Path

import pytest

from errors import InvalidPathError
from walker import iter_media_files


def test_iter_media_files_basic(tmp_path: Path) -> None:
    (tmp_path / "a.jpg").write_bytes(b"x")
    (tmp_path / "b.HEIC").write_bytes(b"x")
    (tmp_path / ".hidden").mkdir()
    (tmp_path / ".hidden" / "c.jpg").write_bytes(b"x")

    results = list(
        iter_media_files(
            [tmp_path],
            include_ext=[".jpg", ".heic"],
            ignore_hidden=True,
            follow_symlinks=False,
        )
    )
    paths = {p.name.lower() for p, _ in results}
    assert "a.jpg" in paths
    assert "b.heic" in paths
    assert "c.jpg" not in paths  # hidden dir ignored


def test_iter_media_files_invalid(tmp_path: Path) -> None:
    with pytest.raises(InvalidPathError):
        list(iter_media_files([tmp_path / "missing"], include_ext=[".jpg"]))
