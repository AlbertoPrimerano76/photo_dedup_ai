"""
Feature-based image match (Phase 7).

- ORB keypoints + BRIEF descriptors (OpenCV)
- BFMatcher (Hamming) + Lowe's ratio test
- RANSAC homography per convalida geometrica

Ritorna: (is_similar, inliers, inlier_ratio)
"""

from __future__ import annotations

from pathlib import Path
from typing import Tuple, cast

import cv2  # type: ignore[import-untyped]
import numpy as np
from numpy.typing import NDArray


def _read_gray(p: Path, max_side: int = 1024) -> NDArray[np.uint8]:
    """
    Legge l'immagine in scala di grigi; ridimensiona se lato max > max_side.
    """
    # imdecode gestisce path con caratteri non-ASCII meglio su alcune piattaforme
    arr = np.fromfile(str(p), dtype=np.uint8)
    img = cv2.imdecode(arr, cv2.IMREAD_GRAYSCALE)  # type: ignore[no-untyped-call]
    if img is None:
        img = cv2.imread(str(p), cv2.IMREAD_GRAYSCALE)  # type: ignore[no-untyped-call]
    if img is None:
        raise OSError(f"Cannot read image: {p}")
    h, w = img.shape[:2]
    scale = min(1.0, float(max_side) / float(max(h, w)))
    if scale < 1.0:
        img = cv2.resize(
            img, (int(w * scale), int(h * scale)), interpolation=cv2.INTER_AREA
        )  # type: ignore[no-untyped-call]
    # Ensure dtype is uint8 (it should already be)
    return cast(NDArray[np.uint8], img)


def orb_ransac_confirm(
    a: Path,
    b: Path,
    *,
    nfeatures: int = 800,
    ratio: float = 0.75,
    ransac_thresh: float = 5.0,
    min_inliers: int = 20,
    min_inlier_ratio: float = 0.15,
    max_side: int = 1024,
) -> Tuple[bool, int, float]:
    """
    Conferma similarità con ORB + RANSAC.

    Args:
        a, b: percorsi immagine
        nfeatures: ORB max features
        ratio: Lowe's ratio test (KNN)
        ransac_thresh: soglia reproiezione (findHomography)
        min_inliers: inlier minimi per accettare
        min_inlier_ratio: inliers/good_matches minimo
        max_side: lato max per il resize (speed)

    Returns:
        (ok, inliers, inlier_ratio)
    """
    imgA = _read_gray(a, max_side=max_side)
    imgB = _read_gray(b, max_side=max_side)

    orb = cv2.ORB_create(nfeatures=nfeatures)  # type: ignore[attr-defined, no-untyped-call]
    kA, dA = orb.detectAndCompute(imgA, None)  # type: ignore[no-untyped-call]
    kB, dB = orb.detectAndCompute(imgB, None)  # type: ignore[no-untyped-call]

    if dA is None or dB is None or len(kA) == 0 or len(kB) == 0:
        return (False, 0, 0.0)

    # BFMatcher with Hamming norm for ORB descriptors
    bf = cv2.BFMatcher(cv2.NORM_HAMMING, crossCheck=False)  # type: ignore[no-untyped-call]
    # Two-NN for ratio test
    matches = bf.knnMatch(dA, dB, k=2)  # type: ignore[no-untyped-call]

    good = []
    for m_n in matches:
        if len(m_n) < 2:
            continue
        m, n = m_n
        if m.distance < ratio * n.distance:
            good.append(m)

    if len(good) < 4:
        return (False, 0, 0.0)

    # Build coordinate arrays with explicit dtype to satisfy NumPy stubs
    ptsA_list = [kA[m.queryIdx].pt for m in good]
    ptsB_list = [kB[m.trainIdx].pt for m in good]

    ptsA: NDArray[np.float32] = np.asarray(ptsA_list, dtype=np.float32).reshape(
        -1, 1, 2
    )
    ptsB: NDArray[np.float32] = np.asarray(ptsB_list, dtype=np.float32).reshape(
        -1, 1, 2
    )

    H, mask = cv2.findHomography(ptsA, ptsB, cv2.RANSAC, ransac_thresh)  # type: ignore[no-untyped-call]
    if H is None or mask is None:
        return (False, 0, 0.0)

    # mask is 0/1 uint8; ensure we count correctly
    mask_arr: NDArray[np.uint8] = cast(NDArray[np.uint8], mask)
    inliers = int(mask_arr.sum())
    inlier_ratio = float(inliers) / float(len(good)) if len(good) else 0.0
    ok = inliers >= int(min_inliers) and inlier_ratio >= float(min_inlier_ratio)
    return (bool(ok), inliers, inlier_ratio)
