from __future__ import annotations

from pathlib import Path

import numpy as np
from PIL import Image

from image_match import orb_ransac_confirm


def _mk_pattern(p: Path, seed: int) -> None:
    rng = np.random.default_rng(seed)
    arr = (rng.random((256, 256)) * 255).astype("uint8")
    # blocco bianco per assicurare keypoints consistenti
    arr[60:120, 60:120] = 255
    Image.fromarray(arr).convert("L").save(p)


def test_orb_confirm_identical(tmp_path: Path) -> None:
    a = tmp_path / "a.png"
    b = tmp_path / "b.png"
    _mk_pattern(a, 123)
    Image.open(a).save(b)  # copia identica
    ok, inl, ratio = orb_ransac_confirm(a, b, min_inliers=10, min_inlier_ratio=0.05)
    assert ok is True
    assert inl >= 10
    assert ratio >= 0.05


def test_orb_confirm_different(tmp_path: Path) -> None:
    a = tmp_path / "a.png"
    b = tmp_path / "b.png"
    _mk_pattern(a, 123)
    _mk_pattern(b, 999)
    ok, inl, ratio = orb_ransac_confirm(a, b, min_inliers=25, min_inlier_ratio=0.2)
    assert ok is False
