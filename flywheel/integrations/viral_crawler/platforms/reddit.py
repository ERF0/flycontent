
from __future__ import annotations
import re, asyncio
from typing import List
import praw  # type: ignore
from ..core.models import Video
from ..core.utils import iso8601_to_seconds

YID_RE = re.compile(r"(?:v=|/shorts/|youtu\.be/)([A-Za-z0-9_-]{11})")

def _extract_id(url: str) -> str | None:
    m = YID_RE.search(url)
    return m.group(1) if m else None

class RedditYouTubeMiner:
    def __init__(self, client_id: str, client_secret: str, user_agent: str, yt_client, logger):
        if not (client_id and client_secret):
            raise ValueError("Reddit credentials not configured")
        self.reddit = praw.Reddit(client_id=client_id, client_secret=client_secret, user_agent=user_agent)
        self.yt = yt_client
        self.logger = logger

    async def mine_cc_videos(self, subs: list[str], limit_per_sub: int = 25) -> List[Video]:
        videos: List[Video] = []
        for sub in subs:
            try:
                sr = self.reddit.subreddit(sub)
                urls: list[str] = []
                for post in sr.hot(limit=limit_per_sub):
                    if "youtu" in post.url:
                        urls.append(post.url)
                    if hasattr(post, "selftext") and post.selftext:
                        urls += re.findall(r'https?://(?:www\\.)?(?:youtube\\.com/watch\\?v=|youtu\\.be/)([\\w-]+)', post.selftext)
                norm_ids = []
                for u in urls:
                    vid = _extract_id(u) or u
                    if len(vid) == 11:
                        norm_ids.append(vid)
                if not norm_ids:
                    continue
                dreq = self.yt.videos().list(part="contentDetails,statistics,status,snippet", id=",".join(norm_ids))
                dres = await asyncio.to_thread(dreq.execute)
                for item in dres.get("items", []):
                    if item["status"].get("license") != "creativeCommon":
                        continue
                    duration = iso8601_to_seconds(item["contentDetails"]["duration"])
                    if duration > 60:
                        continue
                    videos.append(Video(
                        id=item["id"],
                        title=item["snippet"]["title"],
                        url=f"https://youtu.be/{item['id']}",
                        platform="reddit",
                        license="creativeCommon",
                        duration=duration,
                        creator=item["snippet"]["channelTitle"],
                        description=item["snippet"].get("description"),
                        thumbnail=item["snippet"]["thumbnails"]["high"]["url"],
                        view_count=int(item["statistics"].get("viewCount", 0)),
                        like_count=int(item["statistics"].get("likeCount", 0)),
                        published_at=item["snippet"]["publishedAt"],
                    ))
            except Exception as e:
                self.logger.warning("Reddit mining error on r/%s: %s", sub, e)
        return videos
