"""YouTube channel ingestion utilities."""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import List, Sequence

from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

from . import AccountVideo

logger = logging.getLogger(__name__)


def _parse_published_at(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        if value.endswith("Z"):
            value = value.replace("Z", "+00:00")
        return datetime.fromisoformat(value).astimezone(timezone.utc)
    except ValueError:
        return None


def _build_client(api_key: str):
    return build("youtube", "v3", developerKey=api_key, cache_discovery=False)


def fetch_recent_youtube_clips(
    channel_ids: Sequence[str],
    *,
    api_key: str | None,
    max_results: int = 5,
) -> list[AccountVideo]:
    """Fetch the latest videos for each YouTube channel."""
    if not channel_ids:
        return []
    if not api_key:
        logger.warning("YouTube API key missing; skipping channel ingestion.")
        return []

    try:
        client = _build_client(api_key)
    except Exception as exc:  # pragma: no cover - network initialisation
        logger.error("Failed to initialise YouTube client: %s", exc)
        return []

    clips: List[AccountVideo] = []
    for channel_id in channel_ids:
        try:
            channel_resp = (
                client.channels()
                .list(part="contentDetails,snippet", id=channel_id)
                .execute()
            )
        except HttpError as exc:
            logger.warning("YouTube channel lookup failed for %s: %s", channel_id, exc)
            continue

        items = channel_resp.get("items", [])
        if not items:
            logger.info("No items returned for YouTube channel %s", channel_id)
            continue
        uploads_playlist = (
            items[0]
            .get("contentDetails", {})
            .get("relatedPlaylists", {})
            .get("uploads")
        )
        if not uploads_playlist:
            logger.debug("Uploads playlist missing for channel %s", channel_id)
            continue
        try:
            playlist_resp = (
                client.playlistItems()
                .list(
                    playlistId=uploads_playlist,
                    part="contentDetails,snippet",
                    maxResults=max_results,
                )
                .execute()
            )
        except HttpError as exc:
            logger.warning("Playlist fetch failed for channel %s: %s", channel_id, exc)
            continue

        for item in playlist_resp.get("items", []):
            details = item.get("contentDetails", {})
            snippet = item.get("snippet", {})
            video_id = details.get("videoId")
            if not video_id:
                continue
            clips.append(
                AccountVideo(
                    platform="youtube",
                    account=channel_id,
                    url=f"https://www.youtube.com/watch?v={video_id}",
                    title=snippet.get("title"),
                    identifier=video_id,
                    published_at=_parse_published_at(snippet.get("publishedAt")),
                    extra={"thumbnails": snippet.get("thumbnails")},
                )
            )

    logger.info("Fetched %d YouTube clips across %d channels.", len(clips), len(channel_ids))
    return clips
