"""Subtitle overlay rendering helpers built on MoviePy."""

from __future__ import annotations

import logging
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, Sequence

from moviepy.editor import CompositeVideoClip, TextClip, VideoFileClip

logger = logging.getLogger(__name__)

TIME_PATTERN = re.compile(r"(\d+):(\d+):(\d+),(\d+)")


@dataclass(slots=True)
class SubtitleEntry:
    """Represents a single subtitle line with timing."""

    start: float
    end: float
    text: str


def _parse_timestamp(value: str) -> float:
    match = TIME_PATTERN.match(value.strip())
    if not match:
        return 0.0
    hours, minutes, seconds, millis = (int(part) for part in match.groups())
    return hours * 3600 + minutes * 60 + seconds + millis / 1000.0


def parse_srt(text: str) -> list[SubtitleEntry]:
    """Parse SRT-formatted text into subtitle entries."""
    entries: list[SubtitleEntry] = []
    if not text.strip():
        return entries

    blocks = re.split(r"\n\s*\n", text.strip())
    for block in blocks:
        lines = [line.strip() for line in block.splitlines() if line.strip()]
        if not lines:
            continue
        time_line_idx = 0
        if "-->" not in lines[0] and len(lines) > 1:
            time_line_idx = 1
        if time_line_idx >= len(lines):
            continue
        time_line = lines[time_line_idx]
        if "-->" not in time_line:
            continue
        start_str, end_str = [part.strip() for part in time_line.split("-->")]
        caption_lines = [
            line
            for idx, line in enumerate(lines)
            if idx > time_line_idx and not line.isdigit()
        ]
        caption_text = " ".join(caption_lines)
        entries.append(
            SubtitleEntry(
                start=_parse_timestamp(start_str),
                end=_parse_timestamp(end_str),
                text=caption_text,
            )
        )
    return entries


def load_srt(path: Path) -> list[SubtitleEntry]:
    """Load SRT subtitles from disk."""
    if not path.exists():
        logger.warning("Subtitle file %s missing.", path)
        return []
    return parse_srt(path.read_text(encoding="utf-8"))


def _make_text_clip(text: str, *, width: int, fontsize: int, max_width_ratio: float) -> TextClip:
    fonts = ["Arial-Bold", "Arial", "Helvetica", "LiberationSans-Bold"]
    for font in fonts:
        try:
            return TextClip(
                text,
                fontsize=fontsize,
                color="white",
                font=font,
                method="caption",
                align="center",
                size=(int(width * max_width_ratio), None),
                stroke_color="black",
                stroke_width=2,
            )
        except OSError:
            continue

    return TextClip(
        text,
        fontsize=fontsize,
        color="white",
        method="caption",
        align="center",
        size=(int(width * max_width_ratio), None),
        stroke_color="black",
        stroke_width=2,
    )


def render_subtitled_video(
    source: Path,
    subtitles: Sequence[SubtitleEntry],
    *,
    caption: str | None,
    destination: Path,
    fontsize: int = 52,
) -> Path:
    """Render subtitles and caption onto the video clip."""
    destination.parent.mkdir(parents=True, exist_ok=True)
    with VideoFileClip(str(source)) as base_clip:
        overlays: list = []
        if subtitles:
            for entry in subtitles:
                duration = max(entry.end - entry.start, 0.1)
                subtitle_clip = (
                    _make_text_clip(entry.text, width=int(base_clip.w), fontsize=fontsize, max_width_ratio=0.9)
                    .set_position(("center", int(base_clip.h * 0.82)))
                    .set_start(entry.start)
                    .set_duration(duration)
                )
                overlays.append(subtitle_clip)
        else:
            logger.info("No subtitles detected for %s; caption only.", source.name)

        if caption:
            caption_clip = (
                _make_text_clip(caption, width=int(base_clip.w), fontsize=fontsize, max_width_ratio=0.8)
                .set_position(("center", int(base_clip.h * 0.12)))
                .set_start(0)
                .set_duration(base_clip.duration)
            )
            overlays.append(caption_clip)

        if overlays:
            composite = CompositeVideoClip([base_clip] + overlays)
            composite.audio = base_clip.audio
            composite.write_videofile(
                str(destination),
                codec="libx264",
                audio_codec="aac",
                threads=2,
                temp_audiofile=str(destination.with_suffix(".temp-audio.m4a")),
                remove_temp=True,
                fps=base_clip.fps or 30,
            )
            composite.close()
        else:
            logger.debug("No overlays to render; copying source to %s", destination)
            base_clip.write_videofile(
                str(destination),
                codec="libx264",
                audio_codec="aac",
                threads=2,
                temp_audiofile=str(destination.with_suffix(".temp-audio.m4a")),
                remove_temp=True,
                fps=base_clip.fps or 30,
            )

    logger.info("Rendered overlay to %s", destination)
    return destination
