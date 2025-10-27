
from __future__ import annotations
from pydantic import BaseModel, HttpUrl, Field, validator
from datetime import datetime
from typing import List, Optional, Literal

LicenseType = Literal["creativeCommon", "owned", "unknown"]

class Video(BaseModel):
    id: str
    title: str
    url: HttpUrl
    platform: Literal["youtube", "reddit", "tiktok", "instagram", "unknown"]
    license: LicenseType = Field(default="unknown", description="CC license or ownership info")
    duration: int = Field(ge=0, description="Duration in seconds")
    creator: Optional[str] = None
    description: Optional[str] = None
    thumbnail: Optional[HttpUrl] = None
    view_count: Optional[int] = None
    like_count: Optional[int] = None
    published_at: Optional[datetime] = None
    hashtags: Optional[List[str]] = None
    reuse_warning: Optional[str] = None

    @validator("duration")
    def _max_one_minute(cls, v: int) -> int:
        if v < 0:
            raise ValueError("duration must be >= 0")
        return v
