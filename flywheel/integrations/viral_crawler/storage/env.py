
from __future__ import annotations
from dataclasses import dataclass
from pathlib import Path
from typing import Optional
from dotenv import load_dotenv  # type: ignore
import os

@dataclass
class Settings:
    yt_api_key: Optional[str]
    reddit_client_id: Optional[str]
    reddit_client_secret: Optional[str]
    reddit_user_agent: str
    tiktok_access_token: Optional[str]
    tiktok_client_key: Optional[str]
    instagram_access_token: Optional[str]
    instagram_business_id: Optional[str]
    out_dir: Path

def load_settings(out_dir: str = "downloads") -> Settings:
    load_dotenv()
    return Settings(
        yt_api_key=os.getenv("YT_API_KEY"),
        reddit_client_id=os.getenv("REDDIT_CLIENT_ID"),
        reddit_client_secret=os.getenv("REDDIT_CLIENT_SECRET"),
        reddit_user_agent=os.getenv("REDDIT_USER_AGENT", "viral-cc-crawler/1.0"),
        tiktok_access_token=os.getenv("TIKTOK_ACCESS_TOKEN"),
        tiktok_client_key=os.getenv("TIKTOK_CLIENT_KEY"),
        instagram_access_token=os.getenv("INSTAGRAM_ACCESS_TOKEN"),
        instagram_business_id=os.getenv("INSTAGRAM_BUSINESS_ACCOUNT_ID"),
        out_dir=Path(out_dir),
    )
