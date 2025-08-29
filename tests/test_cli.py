"""
CLI smoke tests:

- Ensure help/version work.
- Ensure listing behavior works and hidden directories are skipped by default.
- We do not test error branches deeply here; those will be covered by unit
  tests of components in later phases (walker/db/etc.).
"""

from __future__ import annotations

from pathlib import Path

from typer.testing import CliRunner

from cli import app

runner = CliRunner()


def test_help() -> None:
    result = runner.invoke(app, ["--help"])
    assert result.exit_code == 0
    assert "Photo Dedup AI" in result.stdout


def test_version() -> None:
    result = runner.invoke(app, ["version"])
    assert result.exit_code == 0
    assert "photo-dedup-ai" in result.stdout.lower()


def test_scan_list_only(tmp_path: Path) -> None:
    # Create a couple of files that should pass our ext filter
    (tmp_path / "a.jpg").write_bytes(b"xx")
    (tmp_path / "b.HEIC").write_bytes(b"xx")
    (tmp_path / ".hidden").mkdir()
    (tmp_path / ".hidden" / "c.jpg").write_bytes(b"xx")  # should be ignored by default

    result = runner.invoke(app, ["scan", str(tmp_path)])
    assert result.exit_code == 0
    out = result.stdout.lower()
    assert "a.jpg" in out
    assert "b.heic" in out
    assert ".hidden" not in out
