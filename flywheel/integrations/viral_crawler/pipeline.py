"""Runtime bridge between the viral crawler example and the flywheel scheduler."""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, List, Optional, Sequence

from .core.downloader import Downloader
from .core.models import Video
from .platforms.instagram import InstagramBusinessClient
from .platforms.reddit import RedditYouTubeMiner
from .platforms.tiktok import TikTokCCClient
from .platforms.youtube import YouTubeCC
from .storage.env import Settings
from .storage.manager import ContentManager

try:  # pragma: no cover - optional dependency resolved at runtime
    from googleapiclient.discovery import build  # type: ignore
except Exception:  # pragma: no cover - handled gracefully for offline/dev use
    build = None  # type: ignore


@dataclass(slots=True)
class ViralCrawlerCredentials:
    """Credentials and identifiers required for the crawler."""

    youtube_api_key: Optional[str]
    reddit_client_id: Optional[str]
    reddit_client_secret: Optional[str]
    reddit_user_agent: str
    tiktok_access_token: Optional[str] = None
    tiktok_client_key: Optional[str] = None
    instagram_access_token: Optional[str] = None
    instagram_business_id: Optional[str] = None


@dataclass(slots=True)
class ViralCrawlerRequest:
    """Runtime options for the crawler execution."""

    output_dir: Path
    youtube_query: Optional[str] = None
    movie_mode: bool = False
    reddit_subs: Sequence[str] = ()
    tiktok_query: Optional[str] = None
    instagram_hashtag: Optional[str] = None
    max_results: int = 20
    freshness_hours: Optional[int] = None
    dry_run: bool = False
    min_duration: Optional[int] = None
    max_duration: Optional[int] = None
    min_likes: Optional[int] = None


@dataclass(slots=True)
class ViralCrawlerResult:
    """Outcome container returned to the caller."""

    videos: List[Video]
    downloaded: int
    output_dir: Path
    report: Optional[dict[str, Any]]


async def _gather_sources(
    request: ViralCrawlerRequest,
    creds: ViralCrawlerCredentials,
    logger,
) -> List[Video]:
    """Gather CC-friendly videos from configured sources."""

    all_videos: List[Video] = []

    if request.youtube_query:
        if not creds.youtube_api_key:
            logger.warning("YouTube query requested but YT_API_KEY missing; skipping.")
        else:
            try:
                yt = YouTubeCC(creds.youtube_api_key, logger)
                if request.movie_mode:
                    vids = await yt.search_latest_movie_clips(
                        request.youtube_query,
                        request.max_results,
                        request.freshness_hours or 72,
                    )
                    logger.info("YouTube returned %d latest movie clips.", len(vids))
                else:
                    vids = await yt.search_cc_shorts(
                        request.youtube_query,
                        request.max_results,
                        freshness_hours=request.freshness_hours,
                    )
                    logger.info("YouTube returned %d CC shorts.", len(vids))
                all_videos.extend(vids)
            except Exception as exc:  # pragma: no cover - network/runtime error
                logger.error("YouTube crawler failed: %s", exc)

    if request.reddit_subs:
        if not (creds.reddit_client_id and creds.reddit_client_secret):
            logger.warning("Reddit subs configured but credentials missing; skipping Reddit miner.")
        elif not creds.youtube_api_key:
            logger.warning("Reddit miner requires YouTube API key for hydration; skipping.")
        elif build is None:
            logger.warning("google-api-python-client not installed; skipping Reddit miner.")
        else:
            subs = [s.strip() for s in request.reddit_subs if s.strip()]
            if subs:
                try:
                    yt_client = await asyncio.to_thread(build, "youtube", "v3", developerKey=creds.youtube_api_key)
                    reddit = RedditYouTubeMiner(
                        client_id=creds.reddit_client_id,
                        client_secret=creds.reddit_client_secret,
                        user_agent=creds.reddit_user_agent,
                        yt_client=yt_client,
                        logger=logger,
                    )
                    vids = await reddit.mine_cc_videos(subs, limit_per_sub=max(10, request.max_results // 2))
                    logger.info("Reddit miner returned %d CC videos.", len(vids))
                    all_videos.extend(vids)
                except Exception as exc:  # pragma: no cover - network/runtime error
                    logger.error("Reddit crawler failed: %s", exc)

    if request.tiktok_query:
        tt = TikTokCCClient(creds.tiktok_access_token, creds.tiktok_client_key, logger)
        try:
            vids = await tt.search_creative_commons(request.tiktok_query, request.max_results)
            logger.info("TikTok client produced %d candidates.", len(vids))
            all_videos.extend(vids)
        except Exception as exc:  # pragma: no cover - network/runtime error
            logger.error("TikTok crawler failed: %s", exc)

    if request.instagram_hashtag:
        ig = InstagramBusinessClient(creds.instagram_access_token, creds.instagram_business_id, logger)
        try:
            vids = await ig.search_hashtag(request.instagram_hashtag, request.max_results)
            logger.info("Instagram client produced %d candidates.", len(vids))
            all_videos.extend(vids)
        except Exception as exc:  # pragma: no cover - network/runtime error
            logger.error("Instagram crawler failed: %s", exc)

    return all_videos


async def _run_pipeline_async(
    request: ViralCrawlerRequest,
    creds: ViralCrawlerCredentials,
    logger,
) -> ViralCrawlerResult:
    """Execute the crawler asynchronously and download assets."""

    request.output_dir.mkdir(parents=True, exist_ok=True)

    settings = Settings(
        yt_api_key=creds.youtube_api_key,
        reddit_client_id=creds.reddit_client_id,
        reddit_client_secret=creds.reddit_client_secret,
        reddit_user_agent=creds.reddit_user_agent,
        tiktok_access_token=creds.tiktok_access_token,
        tiktok_client_key=creds.tiktok_client_key,
        instagram_access_token=creds.instagram_access_token,
        instagram_business_id=creds.instagram_business_id,
        out_dir=request.output_dir,
    )

    videos = await _gather_sources(request, creds, logger)

    if request.freshness_hours is not None:
        cutoff = datetime.now(timezone.utc) - timedelta(hours=request.freshness_hours)
        filtered_recent: List[Video] = []
        for video in videos:
            published = getattr(video, "published_at", None)
            dt = None
            if isinstance(published, datetime):
                dt = published if published.tzinfo else published.replace(tzinfo=timezone.utc)
            elif isinstance(published, str):
                try:
                    dt = datetime.fromisoformat(published.replace("Z", "+00:00"))
                except ValueError:
                    dt = None
            if dt is None or dt >= cutoff:
                filtered_recent.append(video)
        removed = len(videos) - len(filtered_recent)
        videos = filtered_recent
        if removed > 0:
            logger.info(
                "Filtered out %d videos published before %d hours ago.",
                removed,
                request.freshness_hours,
            )

    min_duration = request.min_duration
    max_duration = request.max_duration
    if min_duration is not None or max_duration is not None:
        filtered: List[Video] = []
        for video in videos:
            duration = getattr(video, "duration", None)
            try:
                duration_val = int(duration) if duration is not None else 0
            except (ValueError, TypeError):
                duration_val = 0

            if min_duration is not None and duration_val < min_duration:
                continue
            if max_duration is not None and duration_val > max_duration:
                continue
            filtered.append(video)
        removed = len(videos) - len(filtered)
        videos = filtered
        if removed > 0:
            window_desc = f"{min_duration or 0}-{max_duration or 'inf'}s"
            logger.info("Filtered out %d videos outside %s window.", removed, window_desc)

    if request.min_likes is not None:
        filtered_likes: List[Video] = []
        for video in videos:
            like_count = getattr(video, "like_count", None)
            try:
                likes_val = int(like_count) if like_count is not None else 0
            except (ValueError, TypeError):
                likes_val = 0
            if likes_val >= request.min_likes:
                filtered_likes.append(video)
        removed = len(videos) - len(filtered_likes)
        videos = filtered_likes
        if removed > 0:
            logger.info("Filtered out %d videos below %d likes.", removed, request.min_likes)

    if request.dry_run:
        for video in videos:
            logger.info(
                "DRY RUN | %s | %s | license=%s | duration=%ss",
                video.platform,
                video.title[:60],
                video.license,
                video.duration,
            )
        return ViralCrawlerResult(videos=videos, downloaded=0, output_dir=settings.out_dir, report=None)

    downloader = Downloader(settings.out_dir, logger)
    downloaded = await downloader.download_all(videos)

    report = ContentManager(settings.out_dir, logger).report()

    return ViralCrawlerResult(videos=videos, downloaded=downloaded, output_dir=settings.out_dir, report=report)


def run_pipeline(
    request: ViralCrawlerRequest,
    creds: ViralCrawlerCredentials,
    logger,
) -> ViralCrawlerResult:
    """Synchronous entry point used by the flywheel scheduler."""

    try:
        return asyncio.run(_run_pipeline_async(request, creds, logger))
    except RuntimeError as exc:
        if "asyncio.run()" not in str(exc):
            raise
        loop = asyncio.new_event_loop()
        try:
            asyncio.set_event_loop(loop)
            return loop.run_until_complete(_run_pipeline_async(request, creds, logger))
        finally:
            asyncio.set_event_loop(None)
            loop.close()
