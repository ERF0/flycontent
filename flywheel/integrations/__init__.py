"""Integration helpers for platform-specific content ingestion."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Any


@dataclass(slots=True)
class AccountVideo:
    """Normalized representation of a downloaded account video."""

    platform: str
    account: str
    url: str
    title: str | None = None
    identifier: str | None = None
    published_at: datetime | None = None
    duration: float | None = None
    extra: dict[str, Any] = field(default_factory=dict)


__all__ = ["AccountVideo"]
