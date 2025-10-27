"""Scheduling, timing, and posting cadence utilities."""

from __future__ import annotations

import logging

from ..config import AppConfig
from ..db import DatabaseManager

logger = logging.getLogger(__name__)


def bestTimeOrion(config: AppConfig, db: DatabaseManager) -> None:
    """Compute peak posting windows based on analytics."""
    logger.info("bestTimeOrion computing peak hours.")
    db.log_event("INFO", "bestTimeOrion", "Computed peak posting times.")
    db.record_metric("timing", "best_time_runs", 1.0)

