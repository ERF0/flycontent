"""Instagram account ingestion utilities."""

from __future__ import annotations

import logging
from typing import List, Sequence

from instagrapi import Client
from instagrapi.exceptions import ClientError

from . import AccountVideo

logger = logging.getLogger(__name__)


def fetch_recent_instagram_clips(
    usernames: Sequence[str],
    *,
    session_id: str | None = None,
    max_results: int = 8,
) -> list[AccountVideo]:
    """Return the latest clips for the given Instagram usernames."""
    if not usernames:
        return []

    client = Client()
    if not session_id:
        logger.warning("Instagram session id missing; skipping account ingestion.")
        return []

    try:
        client.login_by_sessionid(session_id)
        logger.debug("Authenticated to Instagram with session id.")
    except ClientError as exc:
        logger.warning("Instagram session login failed: %s", exc)
        return []

    clips: List[AccountVideo] = []
    for username in usernames:
        try:
            user_id = client.user_id_from_username(username)
            medias = client.user_medias(user_id, amount=max_results)
        except ClientError as exc:
            logger.warning("Failed to load media for @%s: %s", username, exc)
            continue

        for media in medias:
            if not getattr(media, "video_url", None):
                continue
            clips.append(
                AccountVideo(
                    platform="instagram",
                    account=username,
                    url=media.video_url,
                    title=getattr(media, "caption_text", None),
                    identifier=str(media.pk),
                    published_at=getattr(media, "taken_at", None),
                    duration=getattr(media, "video_duration", None),
                    extra={"thumbnail_url": getattr(media, "thumbnail_url", None)},
                )
            )

    logger.info("Fetched %d Instagram clips across %d accounts.", len(clips), len(usernames))
    return clips
