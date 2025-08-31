from __future__ import annotations
from pathlib import Path
from PIL import Image, ImageDraw

from image_phash import phash64, dhash64, hamming64


def _mk_img(p: Path, color: int) -> None:
    Image.new("L", (32, 32), color=color).save(p)


def _mk_split_img(p: Path, left: int, right: int) -> None:
    im = Image.new("L", (32, 32), color=left)
    draw = ImageDraw.Draw(im)
    # right half different shade
    draw.rectangle([16, 0, 31, 31], fill=right)
    im.save(p)


def test_phash_dhash_identical(tmp_path: Path) -> None:
    a = tmp_path / "a.png"
    b = tmp_path / "b.png"
    _mk_img(a, 128)
    _mk_img(b, 128)
    pa, da = phash64(a), dhash64(a)
    pb, db = phash64(b), dhash64(b)
    assert pa == pb
    assert da == db
    assert hamming64(pa, pb) == 0
    assert hamming64(da, db) == 0


def test_phash_dhash_different(tmp_path: Path) -> None:
    a = tmp_path / "a.png"
    b = tmp_path / "b.png"
    # Make adjacent-pixel differences show up for dHash
    _mk_split_img(a, left=0, right=255)
    _mk_split_img(b, left=255, right=0)
    pa, da = phash64(a), dhash64(a)
    pb, db = phash64(b), dhash64(b)
    assert hamming64(pa, pb) > 0
    assert hamming64(da, db) > 0
