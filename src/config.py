# src/config.py
"""
Configuration loader with validation and safe error handling.

- Reads optional dup.toml (or a provided path).
- Provides defaults if file is absent.
- Validates and normalizes paths.
- Exposes a typed configuration object used by the CLI and engine.
"""

from __future__ import annotations

import os
from pathlib import Path
from typing import List, Optional

from pydantic import BaseModel, Field, ValidationError, field_validator

from errors import ConfigLoadError


class ScanConfig(BaseModel):
    """Configuration governing filesystem scanning."""

    roots: List[Path] = Field(default_factory=list, description="Folders to scan")
    follow_symlinks: bool = False
    ignore_hidden: bool = True
    include_ext: List[str] = Field(
        default_factory=lambda: [
            ".jpg",
            ".jpeg",
            ".heic",
            ".mov",
            ".cr2",
            ".nef",
            ".arw",
            ".dng",
        ],
        description="File extensions (lowercase) to include during scan.",
    )
    db_path: Path = Path(".pdai/index.sqlite3")

    @field_validator("roots", mode="after")
    @classmethod
    def _expand_roots(cls, v: List[Path]) -> List[Path]:
        """Expand ~ and make absolute paths for reliability."""
        return [Path(os.path.expanduser(str(p))).resolve() for p in v]

    @field_validator("include_ext", mode="after")
    @classmethod
    def _normalize_exts(cls, v: List[str]) -> List[str]:
        """Normalize extensions to lowercase and ensure they begin with a dot."""
        normed: List[str] = []
        for e in v:
            e = e.strip().lower()
            if not e:
                continue
            if not e.startswith("."):
                e = "." + e
            normed.append(e)
        return normed


class AppConfig(BaseModel):
    """Root application configuration object."""

    scan: ScanConfig = ScanConfig()

    @staticmethod
    def load(path: Optional[Path] = None) -> "AppConfig":
        """
        Load config from TOML if present; otherwise return defaults.

        Load order:
          1) Provided path (if any).
          2) ./dup.toml in the current working directory.
        Env overrides:
          - PDAI_DB_PATH: overrides scan.db_path

        Raises:
            ConfigLoadError: if a TOML file exists but cannot be read or validated.
        """
        cfg = AppConfig()
        toml_path = path or (Path.cwd() / "dup.toml")

        if toml_path.exists():
            # Import tomllib here so it's always a real module (no Optional[Module|None]).
            try:
                import tomllib  # Python 3.11+ standard library
            except Exception as exc:
                raise ConfigLoadError(
                    "TOML support is unavailable (tomllib missing). "
                    "Python 3.11+ should include tomllib."
                ) from exc

            try:
                raw_text = toml_path.read_text(encoding="utf-8")
            except OSError as exc:
                raise ConfigLoadError(
                    f"Failed to read config file: {toml_path}"
                ) from exc

            try:
                data = tomllib.loads(raw_text)
            except Exception as exc:
                raise ConfigLoadError(
                    f"Invalid TOML in config file: {toml_path}"
                ) from exc

            try:
                scan_data = data.get("scan", data)
                cfg.scan = ScanConfig(**scan_data)
            except ValidationError as exc:
                raise ConfigLoadError(
                    f"Invalid configuration values in {toml_path}"
                ) from exc

        db_env = os.getenv("PDAI_DB_PATH")
        if db_env:
            try:
                cfg.scan.db_path = Path(db_env).expanduser().resolve()
            except Exception as exc:
                raise ConfigLoadError(f"Invalid PDAI_DB_PATH value: {db_env}") from exc

        return cfg
