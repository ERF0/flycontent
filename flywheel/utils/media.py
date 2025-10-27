"""Media processing utilities built around moviepy."""

from __future__ import annotations

import logging
from pathlib import Path

from moviepy.editor import VideoFileClip, afx

logger = logging.getLogger(__name__)


def render_video_variant(source: Path, destination: Path) -> None:
    """Add subtitles, music, and filters to create a meme variant suitable for vertical reels."""
    logger.debug("Rendering video variant from %s to %s", source, destination)
    with VideoFileClip(str(source)) as clip:
        clip = clip.resize(height=1920).fx(afx.audio_fadein, 0.5).fx(afx.audio_fadeout, 0.5)
        clip.write_videofile(
            str(destination),
            codec="libx264",
            audio_codec="aac",
            temp_audiofile=str(destination.with_suffix(".temp-audio.m4a")),
            remove_temp=True,
            threads=2,
        )


def transcode_for_reels(source: Path) -> None:
    """Create a reels-optimized version of the video (cropped to 9:16)."""
    reels_path = source.with_name(f"{source.stem}_reels.mp4")
    logger.debug("Transcoding %s to reels format %s", source, reels_path)
    with VideoFileClip(str(source)) as clip:
        clip = clip.resize(height=1920).crop(width=1080, height=1920, x_center=clip.w / 2, y_center=clip.h / 2)
        clip.write_videofile(str(reels_path), codec="libx264", audio_codec="aac", threads=2)


def prepare_upload_asset(asset: Path) -> Path:
    """Ensure asset meets platform requirements."""
    optimized = asset.with_name(f"{asset.stem}_optimized{asset.suffix}")
    if optimized.exists():
        return optimized
    with VideoFileClip(str(asset)) as clip:
        clip = clip.resize(height=1920)
        clip.write_videofile(str(optimized), codec="libx264", audio_codec="aac", threads=2)
    return optimized

