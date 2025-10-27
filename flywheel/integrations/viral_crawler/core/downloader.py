
from __future__ import annotations

import asyncio
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable

import yt_dlp  # type: ignore

from .models import Video


class Downloader:
    """Download Creative Commons video assets referenced by crawler results."""

    def __init__(self, out_dir: Path, logger) -> None:
        self.out_dir = Path(out_dir)
        self.logger = logger
        for sub in ("youtube", "reddit", "tiktok", "instagram", "metadata"):
            (self.out_dir / sub).mkdir(parents=True, exist_ok=True)
        (self.out_dir / "ATTRIBUTION.txt").touch(exist_ok=True)

    async def download_all(self, videos: Iterable[Video]) -> int:
        """Download all provided videos sequentially with retry logging."""
        count = 0
        for video in videos:
            try:
                if await self._download_one(video):
                    count += 1
            except Exception as exc:  # pragma: no cover - network variability
                self.logger.error("Download failed for %s: %s", video.url, exc)
        return count

    async def _download_one(self, video: Video) -> bool:
        """Download a single video via yt-dlp, persisting metadata and attribution."""
        platform_dir = self.out_dir / video.platform
        outtmpl = str(platform_dir / "%(id)s.%(ext)s")
        ydl_opts = {
            "format": "best[height<=1080]/best[ext=mp4]/best",
            "outtmpl": outtmpl,
            "writethumbnail": True,
            "writeinfojson": True,
            "ignoreerrors": False,
            "no_warnings": False,
            "merge_output_format": "mp4",
        }

        def _run() -> None:
            with yt_dlp.YoutubeDL(ydl_opts) as ydl:
                ydl.download([str(video.url)])

        await asyncio.to_thread(_run)

        self._write_metadata(video)
        self._append_attribution(video)
        return True

    def _write_metadata(self, video: Video) -> None:
        destination = self.out_dir / "metadata" / f"{video.id}.json"
        try:
            data = video.model_dump(mode="json")  # type: ignore[attr-defined]
        except AttributeError:
            data = json.loads(video.json())
        published_at = data.get("published_at")
        if published_at is not None:
            data["published_at"] = (
                published_at.isoformat()
                if hasattr(published_at, "isoformat")
                else str(published_at)
            )
        destination.write_text(json.dumps(data, ensure_ascii=False, indent=2, default=str), encoding="utf-8")

    def _append_attribution(self, video: Video) -> None:
        line = (
            f"Title: {video.title} | Creator: {video.creator or 'Unknown'} | "
            f"Source: {video.url} | License: {video.license} | "
            f"Downloaded: {datetime.now(timezone.utc).isoformat()}\n"
        )
        (self.out_dir / "ATTRIBUTION.txt").open("a", encoding="utf-8").write(line)
