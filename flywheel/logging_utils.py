"""Logging configuration helpers with structured output."""

from __future__ import annotations

import json
import logging
from datetime import datetime, timezone
from logging.handlers import TimedRotatingFileHandler
from typing import Any

from .config import AppConfig


class JsonFormatter(logging.Formatter):
    """Minimal JSON formatter for log aggregation systems."""

    def format(self, record: logging.LogRecord) -> str:
        payload: dict[str, Any] = {
            "ts": datetime.fromtimestamp(record.created, tz=timezone.utc).isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
            "event": getattr(record, "event", record.funcName),
            "environment": getattr(record, "environment", "unknown"),
        }
        if record.exc_info:
            payload["exc_info"] = self.formatException(record.exc_info)
        if record.__dict__.get("extra_fields"):
            payload.update(record.__dict__["extra_fields"])
        return json.dumps(payload, default=str)


class ContextFilter(logging.Filter):
    """Injects common context fields into every log record."""

    def __init__(self, environment: str) -> None:
        super().__init__()
        self._environment = environment

    def filter(self, record: logging.LogRecord) -> bool:  # pragma: no cover - logging integration
        record.environment = self._environment
        record.event = getattr(record, "event", record.funcName)
        return True


def configure_logging(config: AppConfig) -> None:
    """Configure structured logging with both console and rotating file outputs."""
    root = logging.getLogger()
    root.setLevel(logging.INFO if config.environment != "development" else logging.DEBUG)

    for handler in list(root.handlers):
        root.removeHandler(handler)

    context_filter = ContextFilter(config.environment)

    console_handler = logging.StreamHandler()
    console_handler.setFormatter(
        logging.Formatter(
            "%(asctime)s | %(levelname)s | %(name)s | %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
        )
    )
    console_handler.addFilter(context_filter)
    root.addHandler(console_handler)

    file_handler = TimedRotatingFileHandler(
        filename=str(config.log_path),
        when="midnight",
        backupCount=14,
        utc=True,
        encoding="utf-8",
    )
    file_handler.setFormatter(JsonFormatter())
    file_handler.addFilter(context_filter)
    root.addHandler(file_handler)
