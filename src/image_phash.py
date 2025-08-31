"""
Perceptual hashing for images (Phase 6).

- pHash (via DCT) on 32x32 grayscale -> 8x8 low-frequency block -> 64-bit int
- dHash on 9x8 grayscale (adjacent pixel diffs) -> 64-bit int
"""

from __future__ import annotations

from pathlib import Path
from typing import Tuple, cast

import numpy as np
from numpy.typing import NDArray
from PIL import Image
import cv2  # type: ignore[import-untyped]


def _load_grayscale(path: Path, *, size: Tuple[int, int]) -> NDArray[np.float32]:
    """
    Load an image, convert to grayscale, resize to `size`,
    and return a float32 array normalized to [0, 1].
    """
    with Image.open(path) as im:
        im = im.convert("L")
        im = im.resize(size, Image.Resampling.LANCZOS)
        arr = np.asarray(im, dtype=np.float32) / np.float32(255.0)
        return cast(NDArray[np.float32], arr)


def phash64(path: Path) -> int:
    """
    Compute a 64-bit pHash:
      - 32x32 grayscale
      - DCT (cv2.dct)
      - take top-left 8x8 (low freq)
      - threshold by median to produce 64 bits
    """
    arr = _load_grayscale(path, size=(32, 32))

    # cv2.dct has no stubs; cast its result to NDArray[float32]
    dct_out = cv2.dct(arr)  # type: ignore[no-untyped-call]
    dct_arr: NDArray[np.float32] = cast(
        NDArray[np.float32], np.asarray(dct_out, dtype=np.float32)
    )

    low: NDArray[np.float32] = dct_arr[:8, :8]

    # Some NumPy stub versions mis-detect overloads here; we force a float.
    med = float(np.median(low))  # type: ignore[call-overload]

    bits = (low > med).astype(np.uint8).ravel()
    acc = 0
    for b in bits:
        acc = (acc << 1) | int(b)
    return int(acc)


def dhash64(path: Path) -> int:
    """
    Compute a 64-bit dHash:
      - 9x8 grayscale
      - compare each pixel to its right neighbor (8*8 = 64)
    """
    arr = _load_grayscale(path, size=(9, 8))
    diff: NDArray[np.bool_] = arr[:, 1:] > arr[:, :-1]
    bits = diff.astype(np.uint8).ravel()
    acc = 0
    for b in bits:
        acc = (acc << 1) | int(b)
    return int(acc)


def hamming64(a: int, b: int) -> int:
    """Hamming distance of two 64-bit ints."""
    return int((a ^ b).bit_count())
