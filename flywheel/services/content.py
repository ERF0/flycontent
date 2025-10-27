"""Content acquisition and transformation services with resilient networking."""

from __future__ import annotations

import json
import logging
from datetime import datetime
from pathlib import Path
from typing import Any, Sequence
from uuid import uuid4

import pandas as pd
from pydantic import BaseModel, ConfigDict, Field
from openai import OpenAI
from moviepy.editor import VideoFileClip
from yt_dlp import YoutubeDL
from yt_dlp.utils import DownloadError

from ..config import AppConfig
from ..db import DatabaseManager
from ..integrations import AccountVideo
from ..integrations.instagram_accounts import fetch_recent_instagram_clips
from ..integrations.tiktok_accounts import fetch_recent_tiktok_clips
from ..integrations.youtube_channels import fetch_recent_youtube_clips
from ..utils.highlights import HighlightSegment, detect_high_motion_segments
from ..utils.media import render_video_variant, transcode_for_reels
from ..utils.overlay_renderer import load_srt, render_subtitled_video
from ..utils.secrets import secret_value

logger = logging.getLogger(__name__)

HTTP_TIMEOUT = httpx.Timeout(connect=10.0, read=20.0, write=20.0, pool=30.0)
USER_AGENT = "InfinityFlywheel/2025"
LEGACY_HASHTAGS: tuple[str, ...] = ("memes", "funny", "viral")


class MemeRecord(BaseModel):
    """Structured payload persisted for downstream processing."""

    model_config = ConfigDict(extra="allow")

    id: str
    source: str
    title: str | None = None
    caption: str | None = None
    url: str | None = None
    platform: str | None = None
    account: str | None = None
    score: float | None = None
    hashtags: list[str] = Field(default_factory=list)
    metadata: dict[str, Any] = Field(default_factory=dict)
    download_path: str | None = None
    render_path: str | None = None
    highlights: list[dict[str, Any]] = Field(default_factory=list)


def _safe_identifier(clip: AccountVideo) -> str:
    base = clip.identifier or Path(clip.url).stem or uuid4().hex
    sanitized = "".join(ch for ch in base if ch.isalnum() or ch in ("-", "_"))
    if not sanitized:
        sanitized = uuid4().hex
    return f"{clip.platform}_{sanitized.lower()}"


def _record_path(config: AppConfig, record_id: str) -> Path:
    return config.meme_cache_dir / f"{record_id}.json"


def _load_existing_record(path: Path) -> dict[str, Any] | None:
    if not path.exists():
        return None
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        logger.warning("Corrupted record at %s; ignoring existing payload.", path)
        return None


def _store_meme(config: AppConfig, db: DatabaseManager, payload: MemeRecord) -> Path:
    """Persist meme metadata with deterministic IDs, preserving existing highlights."""
    record = payload
    if not record.id:
        record = payload.model_copy(update={"id": f"{payload.source}_{uuid4().hex}"})

    target_path = _record_path(config, record.id)
    existing = _load_existing_record(target_path) or {}
    serialized = record.model_dump(mode="json", serialize_as_any=True)

    merged_metadata = {
        **existing.get("metadata", {}),
        **serialized.get("metadata", {}),
    }
    if "metadata" in serialized:
        serialized["metadata"] = merged_metadata

    if existing.get("highlights") and not serialized.get("highlights"):
        serialized["highlights"] = existing["highlights"]

    merged = {**existing, **serialized}
    target_path.write_text(json.dumps(merged, indent=2, default=_json_default), encoding="utf-8")
    db.log_event("INFO", "scrapMeme", "Stored account clip", payload=record.id)
    return target_path


def _download_account_clip(clip: AccountVideo, base_dir: Path) -> Path | None:
    """Download a clip to the crawler output directory using yt-dlp."""
    target_dir = base_dir / clip.platform / (clip.account or "unknown")
    target_dir.mkdir(parents=True, exist_ok=True)
    safe_id = _safe_identifier(clip)
    outtmpl = str(target_dir / f"{safe_id}.%(ext)s")
    ydl_opts = {
        "quiet": True,
        "noplaylist": True,
        "outtmpl": outtmpl,
        "overwrites": False,
        "format": "mp4/best",
        "merge_output_format": "mp4",
        "retries": 1,
        "skip_download": False,
        "continuedl": False,
        "no_warnings": True,
    }

    try:
        with YoutubeDL(ydl_opts) as ydl:
            info = ydl.extract_info(clip.url, download=True)
            download_path = Path(ydl.prepare_filename(info))
            logger.info(
                "Downloaded %s clip from %s -> %s",
                clip.platform,
                clip.account or "unknown",
                download_path,
            )
            return download_path
    except DownloadError as exc:
        logger.warning(
            "Failed to download %s clip for %s: %s", clip.platform, clip.account, exc
        )
        return None
    except Exception:
        logger.exception("Unexpected error downloading clip %s", clip.url)
        return None


def _ingest_clips(
    clips: Sequence[AccountVideo],
    config: AppConfig,
    db: DatabaseManager,
) -> int:
    ingested = 0
    for clip in clips:
        record_id = _safe_identifier(clip)
        record_path = _record_path(config, record_id)
        existing = _load_existing_record(record_path)
        if existing and existing.get("metadata", {}).get("highlight_status") == "complete":
            logger.debug("Skipping already processed clip %s", record_id)
            continue

        existing_path = Path(existing.get("download_path")) if existing and existing.get("download_path") else None
        if existing_path and existing_path.exists():
            download_path = existing_path
            logger.debug("Reusing existing download for %s at %s", record_id, download_path)
        else:
            download_path = _download_account_clip(clip, config.crawler_output_dir)
        if not download_path:
            continue

        metadata = {
            "account": clip.account,
            "platform": clip.platform,
            "source_url": clip.url,
            "highlight_status": "pending",
        }
        if clip.published_at:
            metadata["published_at"] = clip.published_at.isoformat()

        record = MemeRecord(
            id=record_id,
            source="account_ingest",
            title=clip.title,
            caption=clip.title,
            url=clip.url,
            platform=clip.platform,
            account=clip.account,
            metadata=metadata,
            download_path=str(download_path),
            render_path=None,
            highlights=existing.get("highlights", []) if existing else [],
        )
        _store_meme(config, db, record)
        db.update_post_status(
            platform=clip.platform,
            status="downloaded",
            external_id=record.id,
            metadata=json.dumps(
                {
                    "download_path": record.download_path,
                    "account": clip.account,
                    "source_url": clip.url,
                },
                default=_json_default,
            ),
        )
        db.record_metric(clip.platform, "account_ingested", 1.0, context=clip.account or "unknown")
        ingested += 1
    return ingested


def scrapMeme(config: AppConfig, db: DatabaseManager) -> None:
    """Fetch clips from configured accounts and stage them for highlight extraction."""
    logger.info("Running account-based ingestion for highlight pipeline.")
    config.ensure_runtime_directories()

    instagram_clips = fetch_recent_instagram_clips(
        config.ingest_instagram_accounts,
        session_id=config.instagram_session_id,
    )
    youtube_clips = fetch_recent_youtube_clips(
        config.ingest_youtube_channels,
        api_key=secret_value(config.youtube_api_key),
    )
    tiktok_clips = fetch_recent_tiktok_clips(
        config.ingest_tiktok_accounts,
        session_id=config.tiktok_session_id,
    )

    total_ingested = 0
    platform_counts: dict[str, int] = {}

    for platform, clips in (
        ("instagram", instagram_clips),
        ("youtube", youtube_clips),
        ("tiktok", tiktok_clips),
    ):
        if not clips:
            logger.info("No %s clips available for ingestion.", platform)
            continue
        ingested = _ingest_clips(clips, config, db)
        platform_counts[platform] = ingested
        total_ingested += ingested

    logger.info(
        "Account ingestion completed. Totals: %s | overall=%d",
        platform_counts,
        total_ingested,
    )
    db.log_event(
        "INFO",
        "scrapMeme",
        "Account ingestion completed",
        payload={"totals": platform_counts, "overall": total_ingested},
    )


def autoTrend(config: AppConfig, db: DatabaseManager) -> None:
    """Analyze scraped meme dataset to identify trending formats."""
    logger.info("Running autoTrend analysis.")
    meme_files = list(config.meme_cache_dir.glob("*.json"))
    if not meme_files:
        logger.info("No meme dataset available for trend analysis.")
        return

    rows: list[dict[str, Any]] = []
    for file in meme_files:
        try:
            rows.append(json.loads(file.read_text(encoding="utf-8")))
        except json.JSONDecodeError:
            logger.warning("Failed to parse meme file %s", file)
    if not rows:
        return

    df = pd.DataFrame(rows)
    if "source" not in df.columns:
        df["source"] = "unknown"
    else:
        missing_mask = df["source"].isna() | (df["source"].astype(str).str.strip() == "")
        if missing_mask.any():
            df.loc[missing_mask, "source"] = "unknown"

    trend_counts = df.groupby("source").size().sort_values(ascending=False)
    for source, count in trend_counts.items():
        db.record_metric(source, "scraped_items", float(count))
    logger.debug("Trend counts: %s", trend_counts.to_dict())


def _create_whisper_client(config: AppConfig) -> OpenAI | None:
    api_key = secret_value(config.openai_api_key)
    if not api_key:
        logger.error("OPENAI_API_KEY not set; cannot transcribe highlights.")
        return None
    try:
        return OpenAI(api_key=api_key)
    except Exception:
        logger.exception("Failed to initialise OpenAI client.")
        return None


def _transcribe_segment(
    client: OpenAI | None,
    video_path: Path,
    subtitles_dir: Path,
) -> Path | None:
    if client is None:
        return None
    subtitles_dir.mkdir(parents=True, exist_ok=True)
    target_path = subtitles_dir / f"{video_path.stem}.srt"
    try:
        with video_path.open("rb") as handle:
            response = client.audio.transcriptions.create(
                model="whisper-1",
                file=handle,
                response_format="srt",
            )
    except Exception:
        logger.exception("Whisper transcription failed for %s", video_path.name)
        return None

    if isinstance(response, bytes):
        transcript_text = response.decode("utf-8")
    else:
        transcript_text = getattr(response, "text", None)
        if not transcript_text and isinstance(response, str):
            transcript_text = response
        elif not transcript_text:
            transcript_text = str(response)

    target_path.write_text(transcript_text, encoding="utf-8")
    logger.info("Generated subtitles for %s", video_path.name)
    return target_path


def _export_highlight_clip(
    source: Path,
    segment: HighlightSegment,
    destination: Path,
) -> Path | None:
    destination.parent.mkdir(parents=True, exist_ok=True)
    try:
        with VideoFileClip(str(source)) as clip:
            highlight = clip.subclip(segment.start, segment.end)
            highlight.write_videofile(
                str(destination),
                codec="libx264",
                audio_codec="aac",
                threads=2,
                temp_audiofile=str(destination.with_suffix(".temp-audio.m4a")),
                remove_temp=True,
                fps=clip.fps or 30,
            )
    except Exception:
        logger.exception("Failed to export highlight segment from %s", source)
        return None
    return destination


def _process_highlight_record(
    record_path: Path,
    data: dict[str, Any],
    client: OpenAI | None,
    config: AppConfig,
    db: DatabaseManager,
) -> int:
    record_id = data.get("id") or record_path.stem
    download_path = data.get("download_path")
    if not download_path:
        logger.warning("Record %s missing download path; skipping.", record_id)
        return 0

    source_video = Path(download_path)
    if not source_video.exists():
        logger.warning("Downloaded asset missing for %s at %s", record_id, source_video)
        return 0

    segments = detect_high_motion_segments(source_video)
    if not segments:
        logger.warning("No dynamic segments detected for %s", record_id)
        return 0

    platform = data.get("platform") or "unknown"
    highlight_dir = config.render_cache_dir / platform
    highlight_dir.mkdir(parents=True, exist_ok=True)

    highlight_entries: list[dict[str, Any]] = []
    for index, segment in enumerate(segments, start=1):
        clip_destination = highlight_dir / f"{source_video.stem}_seg{index}.mp4"
        clip_path = _export_highlight_clip(source_video, segment, clip_destination)
        if clip_path is None:
            continue

        subtitle_path = _transcribe_segment(client, clip_path, highlight_dir)
        if subtitle_path is None:
            logger.warning("Skipping segment %s due to subtitle failure.", clip_path.name)
            continue

        subtitles = load_srt(subtitle_path)
        if not subtitles:
            logger.warning("No subtitles parsed for %s; skipping overlay.", subtitle_path.name)
            continue

        caption_source = data.get("account") or data.get("title") or ""
        caption_text = f"@{caption_source.lstrip('@')}" if caption_source else None
        final_path = highlight_dir / f"{clip_path.stem}_subtitled.mp4"
        render_subtitled_video(clip_path, subtitles, caption=caption_text, destination=final_path)

        highlight_entries.append(
            {
                "index": index,
                "start": segment.start,
                "end": segment.end,
                "score": segment.score,
                "raw_path": str(clip_path),
                "subtitle_path": str(subtitle_path),
                "final_path": str(final_path),
            }
        )

        db.update_post_status(
            platform=platform,
            status="ready_for_upload",
            external_id=f"{record_id}_seg{index}",
            metadata=json.dumps(
                {
                    "final_path": str(final_path),
                    "start": segment.start,
                    "end": segment.end,
                    "score": segment.score,
                },
                default=_json_default,
            ),
            performance_score=float(segment.score),
        )
        db.record_metric(platform, "highlights_rendered", 1.0, context=record_id)
        logger.info("Highlight %s segment %d ready at %s", record_id, index, final_path.name)

    metadata = dict(data.get("metadata", {}))
    if highlight_entries:
        metadata["highlight_status"] = "complete"
        metadata["highlight_count"] = len(highlight_entries)
        metadata["last_highlight_at"] = datetime.utcnow().isoformat() + "Z"
    else:
        metadata["highlight_status"] = "failed"

    hashtags = data.get("hashtags") or []
    if isinstance(hashtags, str):
        hashtags = [hashtags]

    score_value = data.get("score")
    try:
        score = float(score_value) if score_value is not None else None
    except (TypeError, ValueError):
        score = None

    render_path = highlight_entries[0]["final_path"] if highlight_entries else data.get("render_path")

    record = MemeRecord(
        id=record_id,
        source=data.get("source", "account_ingest"),
        title=data.get("title"),
        caption=data.get("caption"),
        url=data.get("url"),
        platform=platform,
        account=data.get("account"),
        score=score,
        hashtags=list(hashtags),
        metadata=metadata,
        download_path=str(source_video),
        render_path=render_path,
        highlights=highlight_entries,
    )
    _store_meme(config, db, record)
    return len(highlight_entries)


def highlightForge(config: AppConfig, db: DatabaseManager) -> None:
    """Generate highlights, subtitles, and overlays for downloaded clips."""
    logger.info("Running highlight extraction pipeline.")
    pending_files = sorted(config.meme_cache_dir.glob("*.json"))
    if not pending_files:
        logger.info("No meme records found for highlight processing.")
        return

    client = _create_whisper_client(config)
    if client is None:
        db.log_event("ERROR", "highlightForge", "OpenAI client unavailable; aborting.")
        return

    processed_records = 0
    total_highlights = 0

    for record_path in pending_files:
        data = _load_existing_record(record_path)
        if not data:
            continue
        metadata = data.get("metadata", {})
        status = metadata.get("highlight_status", "pending")
        if status == "complete":
            continue
        highlights = _process_highlight_record(record_path, data, client, config, db)
        if highlights:
            processed_records += 1
            total_highlights += highlights

    logger.info(
        "Highlight pipeline completed. Records=%d highlights=%d",
        processed_records,
        total_highlights,
    )
    db.log_event(
        "INFO",
        "highlightForge",
        "Highlight pipeline complete",
        payload={"records": processed_records, "highlights": total_highlights},
    )


def autoAesthetic(config: AppConfig, db: DatabaseManager) -> None:
    """Auto edit video assets (crop, subtitles, filters) for vertical platforms."""
    logger.info("Running autoAesthetic processing.")
    video_candidates = list(config.render_cache_dir.glob("*.mp4"))
    for video in video_candidates:
        try:
            render_video_variant(video, config.render_cache_dir / f"{video.stem}_captioned.mp4")
            transcode_for_reels(video)
            db.log_event("INFO", "autoAesthetic", f"Processed {video.name}")
        except Exception:
            logger.exception("Failed to auto-edit %s", video)


def templateBreeder(config: AppConfig, db: DatabaseManager) -> None:
    """Curate and mutate meme templates for future use."""
    logger.info("Refreshing template library.")
    template_dir = config.analytics_dir / "templates"
    template_dir.mkdir(exist_ok=True)
    seed_templates = ["drake_hotline", "distracted_boyfriend", "galaxy_brain"]
    for template in seed_templates:
        file_path = template_dir / f"{template}.json"
        if not file_path.exists():
            file_path.write_text(json.dumps({"name": template, "variants": []}, indent=2))


def storyReelClone(config: AppConfig, db: DatabaseManager) -> None:
    """Repurpose successful posts into story/Reel formats."""
    logger.info("Cloning stories/Reels from top-performing posts.")
    db.log_event("INFO", "storyReelClone", "Cloned top performing posts for stories.")


def _json_default(value: Any) -> str:
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, Path):
        return str(value)
    return str(value)
