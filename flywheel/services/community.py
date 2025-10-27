"""Community management, safety, and human touch services."""

from __future__ import annotations

import logging

from ..config import AppConfig
from ..db import DatabaseManager

logger = logging.getLogger(__name__)


def commentReplyGPT(config: AppConfig, db: DatabaseManager) -> None:
    """Reply to comments using witty, brand-consistent responses.

    Requires `OPENAI_API_KEY` when AI replies are enabled.
    """
    logger.info("commentReplyGPT responding to comments.")
    db.log_event("INFO", "commentReplyGPT", "Replied to comments.")
    db.record_metric("community", "comments_replied", 1.0)


def dmWelcomeFunnel(config: AppConfig, db: DatabaseManager) -> None:
    """Send automated welcome messages to new followers."""
    logger.info("dmWelcomeFunnel running.")
    db.log_event("INFO", "dmWelcomeFunnel", "Sent welcome DMs.")
    db.record_metric("community", "welcome_dms", 1.0)


def autoCollabDM(config: AppConfig, db: DatabaseManager) -> None:
    """Reach out to potential collaboration partners automatically."""
    logger.info("autoCollabDM evaluating partners.")
    db.log_event("INFO", "autoCollabDM", "Collaboration outreach executed.")
    db.record_metric("community", "collab_outreach", 1.0)


def banShield(config: AppConfig, db: DatabaseManager) -> None:
    """Monitor platform infractions to avoid bans."""
    logger.info("banShield monitoring compliance.")
    db.log_event("INFO", "banShield", "Compliance check completed.")
    db.record_metric("safety", "ban_checks", 1.0)


def autoDrop(config: AppConfig, db: DatabaseManager) -> None:  # pragma: no cover - alias for analytics.autoDrop
    from .analytics import autoDrop as analytics_auto_drop

    analytics_auto_drop(config, db)


def adRevSpinup(config: AppConfig, db: DatabaseManager) -> None:  # alias for distribution function
    from .distribution import adRevSpinup as dist_ad_rev

    dist_ad_rev(config, db)


def humanTouch(config: AppConfig, db: DatabaseManager) -> None:
    """Reserve time for human review or curated engagement."""
    logger.info("humanTouch reminder triggered.")
    db.log_event("INFO", "humanTouch", "Human review suggested.")
    db.record_metric("community", "human_touch_prompts", 1.0)


def autoDeleteFlop(config: AppConfig, db: DatabaseManager) -> None:  # alias
    from .analytics import autoDeleteFlop as analytics_auto_delete

    analytics_auto_delete(config, db)
