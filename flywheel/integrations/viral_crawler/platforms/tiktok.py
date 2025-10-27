
from __future__ import annotations
from datetime import datetime, timedelta
from typing import List
from ..core.models import Video

class TikTokCCClient:
    def __init__(self, access_token: str | None, client_key: str | None, logger):
        self.access_token = access_token
        self.client_key = client_key
        self.logger = logger

    async def search_creative_commons(self, query: str, limit: int = 10) -> List[Video]:
        if not self.access_token:
            self.logger.warning("TikTok access token missing; using mock data")
            return self._mock(query, limit)
        self.logger.warning("TikTok API integration requires business access; returning mock")
        return self._mock(query, limit)

    def _mock(self, query: str, limit: int) -> List[Video]:
        out: List[Video] = []
        for i in range(min(limit, 5)):
            out.append(Video(
                id=f"tiktok_mock_{i}",
                title=f"TikTok about {query} #{i+1}",
                url=f"https://www.tiktok.com/@creator/video/123456{i}",
                platform="tiktok",
                license="creativeCommon",
                duration=30,
                creator=f"mock_creator_{i}",
                view_count=10000 + i*1000,
                like_count=500 + i*100,
                published_at=datetime.utcnow() - timedelta(days=i),
                hashtags=[query, "viral", "fyp"],
            ))
        return out
