
from __future__ import annotations
from datetime import datetime, timedelta
from typing import List
from ..core.models import Video

class InstagramBusinessClient:
    def __init__(self, access_token: str | None, business_id: str | None, logger):
        self.access_token = access_token
        self.business_id = business_id
        self.logger = logger

    async def search_hashtag(self, hashtag: str, limit: int = 10) -> List[Video]:
        if not self.access_token:
            self.logger.warning("Instagram access token missing; using mock data")
            return self._mock(hashtag, limit)
        self.logger.warning("Instagram Graph integration not configured; returning mock")
        return self._mock(hashtag, limit)

    def _mock(self, hashtag: str, limit: int) -> List[Video]:
        out: List[Video] = []
        for i in range(min(limit, 5)):
            out.append(Video(
                id=f"instagram_mock_{i}",
                title=f"Instagram Reel about #{hashtag} #{i+1}",
                url=f"https://www.instagram.com/reel/ABC{i}/",
                platform="instagram",
                license="unknown",
                duration=25,
                creator="instagram_user",
                view_count=15000 + i*2000,
                like_count=800 + i*150,
                published_at=datetime.utcnow() - timedelta(days=i),
                hashtags=[hashtag, "reels", "viral"],
                reuse_warning="Verify permissions before use",
            ))
        return out
