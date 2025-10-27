"""Utility helpers shared across the viral crawler integration."""

from __future__ import annotations

import asyncio
import logging
import os
import re
from typing import Any, Awaitable, Callable, Optional

ISO_DURATION_RE = re.compile(r"PT(?:(\d+)H)?(?:(\d+)M)?(?:(\d+)S)?")


def setup_logging(level: str = "INFO") -> logging.Logger:
    """Initialise a minimal logger for the crawler module."""
    lvl = getattr(logging, level.upper(), logging.INFO)
    logging.basicConfig(
        level=lvl,
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    )
    return logging.getLogger("viral_cc")


def iso8601_to_seconds(duration: str) -> int:
    """Convert ISO8601 duration strings (PTxxHxxMxxS) into seconds."""
    match = ISO_DURATION_RE.fullmatch(duration)
    if not match:
        return 0
    hours, minutes, seconds = match.groups()
    return (int(hours or 0) * 3600) + (int(minutes or 0) * 60) + int(seconds or 0)


async def with_retry(
    coro_fn: Callable[[], Awaitable[Any]],
    *,
    retries: int = 3,
    base_delay: float = 1.0,
    logger: Optional[logging.Logger] = None,
) -> Any:
    """Execute a coroutine with exponential backoff retries."""
    last_exc: Exception | None = None
    for attempt in range(1, retries + 1):
        try:
            return await coro_fn()
        except Exception as exc:  # pragma: no cover - network variability
            last_exc = exc
            if logger:
                logger.warning("Attempt %s/%s failed: %s", attempt, retries, exc)
            await asyncio.sleep(base_delay * (2 ** (attempt - 1)))
    if last_exc:
        raise last_exc
    raise RuntimeError("with_retry exited unexpectedly")


def env(key: str, default: Optional[str] = None) -> Optional[str]:
    """Safer getenv wrapper used by the crawler."""
    value = os.getenv(key, default)
    if value is None:
        return None
    stripped = value.strip()
    return stripped or None
