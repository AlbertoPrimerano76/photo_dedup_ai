"""
Media helpers: map extensions → coarse media types for storage/reporting.
"""

from __future__ import annotations

from typing import Literal

MediaType = Literal["image", "video", "raw", "other"]

IMAGE_EXTS = {
    ".jpg",
    ".jpeg",
    ".png",
    ".webp",
    ".gif",
    ".heic",
    ".heif",
    ".tif",
    ".tiff",
    ".bmp",
}
RAW_EXTS = {".cr2", ".nef", ".arw", ".dng", ".raf", ".rw2", ".orf", ".srw"}
VIDEO_EXTS = {".mov", ".mp4", ".m4v", ".mkv", ".avi", ".hevc"}


def media_type_from_ext(ext: str) -> MediaType:
    e = ext.lower()
    if e in IMAGE_EXTS:
        return "image"
    if e in RAW_EXTS:
        return "raw"
    if e in VIDEO_EXTS:
        return "video"
    return "other"
