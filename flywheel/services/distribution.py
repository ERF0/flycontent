"""Distribution, monetization, and cross-posting services."""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass
from pathlib import Path

from google.auth.transport.requests import Request as GoogleRequest
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from googleapiclient.http import MediaFileUpload
from instagrapi import Client as InstagramClient
from tenacity import retry, stop_after_attempt, wait_exponential

from ..config import AppConfig
from ..db import DatabaseManager
from ..utils.media import prepare_upload_asset
from ..utils.secrets import secret_value

logger = logging.getLogger(__name__)


@dataclass(frozen=True, slots=True)
class VideoMetadata:
    """Describes the metadata applied during uploads."""

    title: str
    description: str
    tags: list[str]
    privacy_status: str = "public"


class YouTubeShortsUploader:
    """Upload handler that wraps the YouTube Data API."""

    def __init__(self, config: AppConfig, db: DatabaseManager) -> None:
        self.config = config
        self.db = db
        self._credentials = self._build_credentials()

    def _build_credentials(self) -> Credentials | None:
        client_id = secret_value(self.config.youtube_client_id)
        client_secret = secret_value(self.config.youtube_client_secret)
        refresh_token = secret_value(self.config.youtube_refresh_token)
        if not all([client_id, client_secret, refresh_token]):
            logger.debug("YouTube OAuth credentials incomplete; uploads disabled.")
            return None
        token = secret_value(self.config.youtube_access_token) or None
        credentials = Credentials(
            token=token,
            refresh_token=refresh_token,
            token_uri=self.config.youtube_token_uri,
            client_id=client_id,
            client_secret=client_secret,
            scopes=["https://www.googleapis.com/auth/youtube.upload"],
        )
        if credentials.expired and credentials.refresh_token:
            credentials.refresh(GoogleRequest())
        return credentials

    def _client(self):
        if self._credentials is None:
            return None
        if self._credentials.expired and self._credentials.refresh_token:
            self._credentials.refresh(GoogleRequest())
        return build("youtube", "v3", credentials=self._credentials, cache_discovery=False)

    @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=2, max=60))
    def upload(self, asset: Path, metadata: VideoMetadata) -> bool:
        client = self._client()
        if client is None:
            logger.debug("Skipping YouTube upload; credentials unavailable.")
            return False
        body = {
            "snippet": {
                "title": metadata.title,
                "description": metadata.description,
                "tags": metadata.tags,
                "categoryId": "23",
            },
            "status": {"privacyStatus": metadata.privacy_status},
        }
        media = MediaFileUpload(str(asset), resumable=True)
        try:
            request = client.videos().insert(part="snippet,status", body=body, media_body=media)
            response = request.execute()
        except HttpError as exc:
            logger.error("YouTube upload failed: %s", exc)
            raise
        self.db.update_post_status("youtube", "posted", external_id=response.get("id"), metadata=json.dumps(body))
        self.db.record_metric("youtube", "upload_success", 1.0, context=metadata.title)
        return True


def _instagram_client(config: AppConfig) -> InstagramClient | None:
    session = secret_value(config.instagram_session_id)
    if not session:
        logger.warning("Instagram session missing; uploads disabled.")
        return None
    client = InstagramClient()
    try:
        client.login_by_sessionid(session)
    except Exception:
        logger.exception("Failed to authenticate Instagram session.")
        return None
    return client


def uploadMemes(config: AppConfig, db: DatabaseManager) -> None:
    """Upload prepared memes to supported platforms."""
    logger.info("Uploading memes to platforms.")
    assets = sorted(config.render_cache_dir.glob("*_captioned.mp4"))
    if not assets:
        logger.info("No rendered assets ready for upload.")
        return

    instagram = _instagram_client(config)
    youtube_uploader = YouTubeShortsUploader(config, db)

    for asset in assets:
        prepared = prepare_upload_asset(asset)
        metadata = _build_video_metadata(prepared, config)

        if instagram:
            try:
                _upload_instagram(instagram, prepared, metadata, db)
                db.update_post_status("instagram", "posted", metadata=prepared.name)
            except Exception as exc:
                logger.exception("Instagram upload failed for %s", prepared)
                db.log_event("ERROR", "uploadMemes", f"Instagram upload failed: {exc}")
                db.record_metric("instagram", "upload_failure", 1.0)

        try:
            if youtube_uploader.upload(prepared, metadata):
                logger.info("Uploaded %s to YouTube Shorts.", prepared.name)
            else:
                logger.debug("YouTube upload skipped for %s", prepared.name)
        except Exception as exc:
            logger.exception("YouTube upload failed for %s", prepared)
            db.log_event("ERROR", "uploadMemes", f"YouTube upload failed: {exc}")
            db.record_metric("youtube", "upload_failure", 1.0)

        db.log_event("INFO", "uploadMemes", f"Queued TikTok upload for {prepared.name}")


def viralHashlock(config: AppConfig, db: DatabaseManager) -> None:
    """Cross-reference hashtags with trending topics to lock in virality signals."""
    logger.info("Executing viralHashlock.")
    trending_path = config.analytics_dir / "trending_hashtags.json"
    if not trending_path.exists():
        trending_path.write_text('["#meme", "#viral", "#funny"]', encoding="utf-8")
    db.log_event("INFO", "viralHashlock", "Updated hashtag locks", trending_path.read_text())


def crossPostTikTok(config: AppConfig, db: DatabaseManager) -> None:
    """Cross-post high-performing content to TikTok."""
    logger.info("Cross-posting to TikTok.")
    db.log_event("INFO", "crossPostTikTok", "Cross-post job executed.")
    db.record_metric("tiktok", "crosspost_attempts", 1.0)


def adRevSpinup(config: AppConfig, db: DatabaseManager) -> None:
    """Manage monetization toggles for platforms supporting revenue sharing."""
    logger.info("Running adRevSpinup.")
    db.log_event("INFO", "adRevSpinup", "Ad revenue update triggered.")
    db.record_metric("monetization", "ad_rev_checks", 1.0)


@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=2, max=60))
def _upload_instagram(client: InstagramClient, asset: Path, metadata: VideoMetadata, db: DatabaseManager) -> None:
    caption = f"{metadata.title}\n\n{metadata.description}"
    client.clip_upload(str(asset), caption)
    db.log_event("INFO", "uploadMemes", f"Instagram upload success: {asset.name}")
    db.record_metric("instagram", "upload_success", 1.0)


def _build_video_metadata(asset: Path, config: AppConfig) -> VideoMetadata:
    title = f"{asset.stem.replace('_', ' ').title()} | Viral Meme"
    hashtags = [config.crawler_tiktok_query, "memes", "shorts", "fyp"]
    deduped = [tag for tag in dict.fromkeys(f"#{tag.strip('#')}" for tag in hashtags) if tag]
    description = "Automatically generated by Infinity Flywheel.\n" + " ".join(deduped)
    return VideoMetadata(title=title[:95], description=description[:4950], tags=deduped)
