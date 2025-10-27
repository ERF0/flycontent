"""Highlight detection utilities for dynamic clip extraction."""

from __future__ import annotations

import logging
from dataclasses import dataclass
from pathlib import Path
from typing import List

import numpy as np
from moviepy.editor import VideoFileClip

logger = logging.getLogger(__name__)


@dataclass(slots=True)
class HighlightSegment:
    """Represents a high-energy portion of a clip."""

    start: float
    end: float
    score: float


def detect_high_motion_segments(
    video_path: Path,
    *,
    min_duration: float = 3.0,
    max_duration: float = 10.0,
    max_segments: int = 3,
    sample_fps: float | None = None,
) -> list[HighlightSegment]:
    """Identify the most dynamic segments in a clip using frame differencing."""
    segments: List[HighlightSegment] = []
    if not video_path.exists():
        logger.warning("Highlight detection skipped; %s missing.", video_path)
        return segments

    with VideoFileClip(str(video_path)) as clip:
        clip_duration = float(clip.duration or 0.0)
        if clip_duration <= 0:
            return segments

        target_fps = sample_fps or clip.fps or 24.0
        target_fps = min(target_fps, 12.0)
        frame_step = 1.0 / max(target_fps, 1.0)

        if clip_duration <= min_duration:
            end_time = min(clip_duration, max_duration)
            segments.append(HighlightSegment(start=0.0, end=end_time, score=1.0))
            return segments

        prev_gray: np.ndarray | None = None
        diffs: list[float] = []
        for frame in clip.iter_frames(fps=target_fps, dtype="uint8"):
            gray = frame.mean(axis=2)
            if prev_gray is None:
                diffs.append(0.0)
            else:
                diffs.append(float(np.mean(np.abs(gray - prev_gray))))
            prev_gray = gray

        diff_arr = np.array(diffs, dtype=np.float32)
        if diff_arr.size <= 1:
            end_time = min(clip_duration, max_duration)
            segments.append(HighlightSegment(start=0.0, end=end_time, score=1.0))
            return segments

        min_frames = max(int(min_duration * target_fps), 1)
        max_frames = max(int(max_duration * target_fps), min_frames)
        max_frames = min(max_frames, diff_arr.size)
        cumulative = np.concatenate(([0.0], np.cumsum(diff_arr)))

        candidates: list[HighlightSegment] = []
        for window in range(min_frames, max_frames + 1):
            window_sums = cumulative[window:] - cumulative[:-window]
            scores = window_sums / float(window)
            for start_idx, score in enumerate(scores):
                start_time = start_idx * frame_step
                end_time = min(start_time + window * frame_step, clip_duration)
                if end_time - start_time < min_duration * 0.8:
                    continue
                candidates.append(HighlightSegment(start=start_time, end=end_time, score=float(score)))

        if not candidates:
            end_time = min(clip_duration, max_duration)
            segments.append(HighlightSegment(start=0.0, end=end_time, score=1.0))
            return segments

        candidates.sort(key=lambda seg: seg.score, reverse=True)
        for candidate in candidates:
            if len(segments) >= max_segments:
                break
            overlap = False
            for existing in segments:
                overlap_amount = min(existing.end, candidate.end) - max(existing.start, candidate.start)
                if overlap_amount > 1.0:
                    overlap = True
                    break
            if overlap:
                continue
            segments.append(candidate)

        segments.sort(key=lambda seg: seg.start)
    logger.debug("Detected %d highlight segments for %s", len(segments), video_path.name)
    return segments
