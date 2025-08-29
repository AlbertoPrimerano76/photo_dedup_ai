"""
Production-ready logging with:
- Rich console for humans (default).
- Optional rotating file logs.
- Optional JSON logs.
- Multiprocessing-safe QueueHandler/QueueListener.

Usage:
    from logs import init_logging, get_logger

    init_logging(level="INFO", to_file=True, json=False)
    log = get_logger(__name__)
    log.info("hello")

Env vars:
    PDAI_LOG_LEVEL   = DEBUG|INFO|WARNING|ERROR (default INFO)
    PDAI_LOG_JSON    = 0|1  (default 0)
    PDAI_LOG_TO_FILE = 0|1  (default 0)
    PDAI_LOG_FILE    = path to log file (default .pdai/logs/app.log)
"""

from __future__ import annotations

import atexit
import json
import logging
import os
import queue
import sys
from dataclasses import dataclass
from logging.handlers import QueueHandler, QueueListener, RotatingFileHandler
from pathlib import Path
from typing import Optional

from rich.console import Console
from rich.logging import RichHandler


# ------------ Config model ------------


@dataclass
class LogConfig:
    level: str = "INFO"
    json: bool = False
    to_file: bool = False
    file_path: Path = Path(".pdai/logs/app.log")
    max_bytes: int = 5 * 1024 * 1024  # 5 MB per file
    backup_count: int = 3
    app_name: str = "pdai"


# ------------ Globals ------------

_CONSOLE = Console(stderr=True, highlight=False, soft_wrap=True)
_QUEUE: Optional[queue.Queue] = None
_LISTENER: Optional[QueueListener] = None
_INITIALIZED = False


# ------------ Formatters ------------


class JsonFormatter(logging.Formatter):
    """Minimal JSON formatter (keeps keys stable for ingestion)."""

    def format(self, record: logging.LogRecord) -> str:  # type: ignore[override]
        payload = {
            "level": record.levelname,
            "time": self.formatTime(record, datefmt="%Y-%m-%dT%H:%M:%S%z"),
            "logger": record.name,
            "msg": record.getMessage(),
            "pid": record.process,
            "proc": record.processName,
            "file": record.filename,
            "line": record.lineno,
        }
        if record.exc_info:
            payload["exc_info"] = self.formatException(record.exc_info)
        return json.dumps(payload, ensure_ascii=False)


def _ensure_parent(path: Path) -> None:
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
    except Exception:
        # Logging must never crash the app; silently ignore here.
        pass


# ------------ Initialization ------------


def init_logging(
    level: Optional[str] = None,
    *,
    json: Optional[bool] = None,
    to_file: Optional[bool] = None,
    file_path: Optional[Path] = None,
    app_name: str = "pdai",
) -> None:
    """
    Initialize process-wide logging. Safe to call multiple times (idempotent).

    - Installs a QueueHandler on root logger.
    - Starts a QueueListener with configured sinks (console/file).
    - Honors env vars when arguments are not provided.
    """
    global _INITIALIZED, _QUEUE, _LISTENER

    if _INITIALIZED:
        return

    # Resolve config from args or env
    cfg = LogConfig(
        level=(level or os.getenv("PDAI_LOG_LEVEL") or "INFO").upper(),
        json=(json if json is not None else os.getenv("PDAI_LOG_JSON", "0") == "1"),
        to_file=(
            to_file
            if to_file is not None
            else os.getenv("PDAI_LOG_TO_FILE", "0") == "1"
        ),
        file_path=Path(
            os.getenv("PDAI_LOG_FILE") or (file_path or Path(".pdai/logs/app.log"))
        ),
        app_name=app_name,
    )

    # Root logger: minimal setup, all records go to queue.
    root = logging.getLogger()
    root.setLevel(getattr(logging, cfg.level, logging.INFO))

    # Avoid installing handlers twice.
    if not any(isinstance(h, QueueHandler) for h in root.handlers):
        _QUEUE = queue.Queue(-1)
        root.addHandler(QueueHandler(_QUEUE))

    # Build real sinks (on the listener side)
    handlers: list[logging.Handler] = []

    if cfg.json:
        # JSON to console (stderr)
        console_handler = logging.StreamHandler(stream=sys.stderr)
        console_handler.setFormatter(JsonFormatter())
        handlers.append(console_handler)
    else:
        # Pretty console for humans
        handlers.append(
            RichHandler(console=_CONSOLE, show_time=True, show_path=False, markup=True)
        )
        # RichHandler already formats; but we need a basic Formatter to avoid %(message)s warnings
        handlers[-1].setFormatter(logging.Formatter("%(message)s"))

    if cfg.to_file:
        try:
            _ensure_parent(cfg.file_path)
            file_handler = RotatingFileHandler(
                cfg.file_path,
                maxBytes=cfg.max_bytes,
                backupCount=cfg.backup_count,
                encoding="utf-8",
            )
            file_handler.setFormatter(
                JsonFormatter()
                if cfg.json
                else logging.Formatter(
                    "%(asctime)s %(levelname)s %(name)s: %(message)s"
                )
            )
            handlers.append(file_handler)
        except Exception:
            # Never fail app init because file logging is unavailable.
            pass

    # Start listener
    if _QUEUE is not None:
        _LISTENER = QueueListener(_QUEUE, *handlers, respect_handler_level=True)
        _LISTENER.start()
        atexit.register(_stop_listener)

    # Quiet noisy third-party loggers (tune as needed later)
    logging.getLogger("PIL").setLevel(logging.WARNING)
    logging.getLogger("urllib3").setLevel(logging.WARNING)

    _INITIALIZED = True


def _stop_listener() -> None:
    global _LISTENER
    if _LISTENER:
        try:
            _LISTENER.stop()
        except Exception:
            pass
        _LISTENER = None


def get_logger(name: Optional[str] = None) -> logging.Logger:
    """
    Get a namespaced logger. Call `init_logging()` once early in your program
    (CLI entrypoint) to configure sinks/levels. This getter stays cheap.
    """
    return logging.getLogger(name or "pdai")
