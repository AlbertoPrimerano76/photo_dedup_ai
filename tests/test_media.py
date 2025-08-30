from __future__ import annotations
from media import media_type_from_ext


def test_media_type_from_ext() -> None:
    assert media_type_from_ext(".jpg") == "image"
    assert media_type_from_ext(".HEIC".lower()) == "image"
    assert media_type_from_ext(".cr2") == "raw"
    assert media_type_from_ext(".mov") == "video"
    assert media_type_from_ext(".weird") == "other"
