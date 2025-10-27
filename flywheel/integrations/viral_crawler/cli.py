
from __future__ import annotations
import argparse, asyncio
from pathlib import Path
from .core.utils import setup_logging
from .core.downloader import Downloader
from .storage.env import load_settings
from .storage.manager import ContentManager
from .platforms.youtube import YouTubeCC
from .platforms.reddit import RedditYouTubeMiner
from .platforms.tiktok import TikTokCCClient
from .platforms.instagram import InstagramBusinessClient
from googleapiclient.discovery import build  # type: ignore

async def main_async(args):
    logger = setup_logging("INFO")
    settings = load_settings(args.output_dir)

    all_videos = []

    if args.youtube_query:
        yt = YouTubeCC(settings.yt_api_key, logger)
        vids = await yt.search_cc_shorts(args.youtube_query, args.max_results)
        logger.info("YouTube: %d videos", len(vids))
        all_videos.extend(vids)

    if args.reddit_subs:
        yt_hydrator = build("youtube", "v3", developerKey=settings.yt_api_key)
        r = RedditYouTubeMiner(
            client_id=settings.reddit_client_id,
            client_secret=settings.reddit_client_secret,
            user_agent=settings.reddit_user_agent,
            yt_client=yt_hydrator,
            logger=logger
        )
        subs = [s.strip() for s in args.reddit_subs.split(",") if s.strip()]
        vids = await r.mine_cc_videos(subs, limit_per_sub=max(10, args.max_results // 2))
        logger.info("Reddit: %d videos", len(vids))
        all_videos.extend(vids)

    if args.tiktok_query:
        tt = TikTokCCClient(settings.tiktok_access_token, settings.tiktok_client_key, logger)
        vids = await tt.search_creative_commons(args.tiktok_query, args.max_results)
        logger.info("TikTok: %d videos", len(vids))
        all_videos.extend(vids)

    if args.instagram_hashtag:
        ig = InstagramBusinessClient(settings.instagram_access_token, settings.instagram_business_id, logger)
        vids = await ig.search_hashtag(args.instagram_hashtag, args.max_results)
        logger.info("Instagram: %d videos", len(vids))
        all_videos.extend(vids)

    if args.dry_run:
        logger.info("DRY RUN: %d items would be downloaded", len(all_videos))
        for v in all_videos:
            lic = "✅" if v.license in ("creativeCommon", "owned") else "❓"
            logger.info("%s [%s] %s", lic, v.platform, v.title[:60])
        return 0

    dl = Downloader(settings.out_dir, logger)
    count = await dl.download_all(all_videos)
    logger.info("Downloaded %d/%d", count, len(all_videos))

    manager = ContentManager(settings.out_dir, logger)
    manager.report()
    return 0

def main():
    p = argparse.ArgumentParser(description="Viral CC crawler (modular, typed, async)")
    p.add_argument("--youtube-query")
    p.add_argument("--reddit-subs", help="Comma-separated subreddit names")
    p.add_argument("--tiktok-query")
    p.add_argument("--instagram-hashtag")
    p.add_argument("--max-results", type=int, default=20)
    p.add_argument("--output-dir", default="downloads")
    p.add_argument("--dry-run", action="store_true")
    args = p.parse_args()
    asyncio.run(main_async(args))

if __name__ == "__main__":
    main()
