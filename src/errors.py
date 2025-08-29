"""
Centralized, typed exceptions for the app.

Having explicit exception types lets us:
- Surface user-friendly messages in the CLI.
- Keep logs clean and actionable.
- Write more precise tests (e.g., expect ConfigLoadError).
"""

from __future__ import annotations


class PdaiError(Exception):
    """Base class for all custom errors in Photo Dedup AI."""


class ConfigLoadError(PdaiError):
    """Raised when a configuration file is missing, unreadable, or invalid."""


class InvalidPathError(PdaiError):
    """Raised when a provided path does not exist or is not a directory."""


class InternalError(PdaiError):
    """Raised for unexpected internal failures to be reported gracefully."""
