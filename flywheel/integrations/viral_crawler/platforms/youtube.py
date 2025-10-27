
from __future__ import annotations

import asyncio
from datetime import datetime, timedelta, timezone
from typing import List, Optional

from googleapiclient.discovery import build  # type: ignore
from googleapiclient.errors import HttpError  # type: ignore

from ..core.models import Video
from ..core.utils import iso8601_to_seconds


class YouTubeCC:
    def __init__(self, api_key: str, logger):
        if not api_key:
            raise ValueError("YT_API_KEY not configured")
        self.client = build("youtube", "v3", developerKey=api_key)
        self.logger = logger
        self.rate_delay = 1.0

    def _to_video(self, item: dict) -> Optional[Video]:
        status = item.get("status", {}) or {}
        if status.get("license") != "creativeCommon":
            return None
        content = item.get("contentDetails") or {}
        duration_iso = content.get("duration")
        if not duration_iso:
            self.logger.debug("Skipping video %s due to missing duration metadata", item.get("id"))
            return None
        duration = iso8601_to_seconds(duration_iso)
        snippet = item.get("snippet") or {}
        stats = item.get("statistics", {})
        thumbnail = snippet.get("thumbnails", {}).get("high", {}).get("url")
        return Video(
            id=item["id"],
            title=snippet["title"],
            url=f"https://youtu.be/{item['id']}",
            platform="youtube",
            license="creativeCommon",
            duration=duration,
            creator=snippet.get("channelTitle"),
            description=snippet.get("description"),
            thumbnail=thumbnail,
            view_count=int(stats.get("viewCount", 0) or 0),
            like_count=int(stats.get("likeCount", 0) or 0),
            published_at=snippet.get("publishedAt"),
        )

    async def search_cc_shorts(
        self,
        query: str,
        limit: int = 20,
        freshness_hours: Optional[int] = None,
    ) -> List[Video]:
        results: List[Video] = []
        token = None
        published_after = None
        if freshness_hours:
            published_after = (
                datetime.now(timezone.utc) - timedelta(hours=freshness_hours)
            ).isoformat().replace("+00:00", "Z")
        while len(results) < limit:
            try:
                params = {
                    "q": query,
                    "part": "id,snippet",
                    "type": "video",
                    "videoLicense": "creativeCommon",
                    "videoDuration": "short",
                    "order": "viewCount",
                    "maxResults": min(50, limit - len(results)),
                    "pageToken": token,
                }
                if published_after:
                    params["publishedAfter"] = published_after
                sreq = self.client.search().list(**params)
                sres = await asyncio.to_thread(sreq.execute)
                ids = [x["id"]["videoId"] for x in sres.get("items", []) if x.get("id", {}).get("videoId")]
                if not ids:
                    break
                dreq = self.client.videos().list(part="contentDetails,statistics,status,snippet", id=",".join(ids))
                dres = await asyncio.to_thread(dreq.execute)

                for item in dres.get("items", []):
                    video = self._to_video(item)
                    if not video or video.duration > 60:
                        continue
                    results.append(video)
                token = sres.get("nextPageToken")
                if not token:
                    break
                await asyncio.sleep(self.rate_delay)
            except HttpError as e:
                self.logger.warning("YouTube rate/HTTP issue: %s", e)
                await asyncio.sleep(5)
            except Exception as e:
                self.logger.error("YouTube search error: %s", e)
                break
        return results[:limit]

    async def search_latest_movie_clips(
        self,
        query: Optional[str] = None,
        limit: int = 20,
        freshness_hours: int = 72,
    ) -> List[Video]:
        """Fetch the most recent Creative Commons movie clips."""
        search_query = query or "movie clips"
        published_after = (
            datetime.now(timezone.utc) - timedelta(hours=freshness_hours)
        ).isoformat().replace("+00:00", "Z")
        results: List[Video] = []
        token = None

        while len(results) < limit:
            try:
                sreq = self.client.search().list(
                    q=search_query,
                    part="id,snippet",
                    type="video",
                    videoLicense="creativeCommon",
                    order="date",
                    videoDuration="medium",
                    videoCategoryId="1",  # Film & Animation keeps us near cinematic uploads
                    publishedAfter=published_after,
                    relevanceLanguage="en",
                    maxResults=min(50, limit - len(results)),
                    pageToken=token,
                )
                sres = await asyncio.to_thread(sreq.execute)
                ids = [x["id"]["videoId"] for x in sres.get("items", []) if x.get("id", {}).get("videoId")]
                if not ids:
                    break
                dreq = self.client.videos().list(part="contentDetails,statistics,status,snippet", id=",".join(ids))
                dres = await asyncio.to_thread(dreq.execute)

                for item in dres.get("items", []):
                    video = self._to_video(item)
                    if not video:
                        continue
                    if video.duration < 45:  # prune micro-clips
                        continue
                    results.append(video)

                token = sres.get("nextPageToken")
                if not token:
                    break
                await asyncio.sleep(self.rate_delay)
            except HttpError as e:
                self.logger.warning("YouTube rate/HTTP issue: %s", e)
                await asyncio.sleep(5)
            except Exception as e:
                self.logger.error("YouTube movie clip search error: %s", e)
                break

        return results[:limit]
