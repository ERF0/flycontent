"""TikTok account ingestion utilities."""

from __future__ import annotations

import logging
from typing import List, Sequence

from yt_dlp import YoutubeDL
from yt_dlp.utils import DownloadError

from . import AccountVideo

logger = logging.getLogger(__name__)


def fetch_recent_tiktok_clips(
    accounts: Sequence[str],
    *,
    session_id: str | None = None,
    max_results: int = 6,
) -> list[AccountVideo]:
    """Fetch the latest TikTok clips for each account."""
    if not accounts:
        return []

    ydl_opts = {
        "quiet": True,
        "skip_download": True,
        "no_warnings": True,
        "extract_flat": True,
    }
    if session_id:
        ydl_opts["http_headers"] = {"Cookie": f"sessionid={session_id}"}

    clips: List[AccountVideo] = []
    with YoutubeDL(ydl_opts) as ydl:
        for account in accounts:
            account_slug = account.lstrip("@")
            url = f"https://www.tiktok.com/@{account_slug}"
            try:
                info = ydl.extract_info(url, download=False)
            except DownloadError as exc:
                logger.warning("TikTok fetch failed for @%s: %s", account_slug, exc)
                continue

            entries = info.get("entries", []) or []
            for entry in entries[:max_results]:
                video_id = entry.get("id") or entry.get("display_id")
                video_url = entry.get("url")
                if video_url and not video_url.startswith("http"):
                    video_url = f"https://www.tiktok.com/@{account_slug}/video/{video_url.strip('/')}"
                if not video_url and video_id:
                    video_url = f"https://www.tiktok.com/@{account_slug}/video/{video_id}"
                if not video_url:
                    continue
                clips.append(
                    AccountVideo(
                        platform="tiktok",
                        account=account_slug,
                        url=video_url,
                        title=entry.get("title"),
                        identifier=video_id,
                        published_at=None,
                        duration=entry.get("duration"),
                        extra={"thumbnail": entry.get("thumbnail")},
                    )
                )

    logger.info("Fetched %d TikTok clips across %d accounts.", len(clips), len(accounts))
    return clips
