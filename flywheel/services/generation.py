"""Caption, hashtag, and sentiment operations using AI and analytics (Gemini edition)."""

from __future__ import annotations

import json
import logging
from typing import Any

import google.generativeai as genai
import pandas as pd
from google.api_core.exceptions import GoogleAPIError
from pydantic import BaseModel, Field, ValidationError
from tenacity import retry, stop_after_attempt, wait_exponential

from ..config import AppConfig
from ..db import DatabaseManager
from ..utils.secrets import secret_value

logger = logging.getLogger(__name__)


class CaptionResult(BaseModel):
    """Validated payload returned by Gemini caption generation."""

    caption: str
    tone: str


class CaptionVariants(BaseModel):
    """Alternate caption variants for experimentation."""

    captions: list[str]
    emotion_tags: list[str] = Field(default_factory=list)


def _build_gemini_model(config: AppConfig) -> genai.GenerativeModel | None:
    api_key = secret_value(config.gemini_api_key)
    if not api_key:
        logger.warning("GEMINI_API_KEY not set; caption generation limited.")
        return None
    genai.configure(api_key=api_key)
    return genai.GenerativeModel("gemini-2.0-flash")


def generateCaption(config: AppConfig, db: DatabaseManager) -> None:
    """Craft captions using humour and trending keywords via Gemini."""
    client = _build_gemini_model(config)
    if not client:
        return

    prompt = (
        "Write a witty caption for a meme about productivity hacks. "
        "Include trending slang and keep it under 200 characters. "
        "Respond strictly in JSON containing 'caption' and 'tone'."
    )
    payload = _call_gemini(client, prompt)
    if not payload:
        return

    try:
        result = CaptionResult.model_validate(payload)
    except ValidationError as exc:  # pragma: no cover - defensive
        logger.warning("Gemini returned unexpected schema: %s", exc)
        return

    db.log_event("INFO", "generateCaption", "Generated caption", result.model_dump())
    db.record_metric("generation", "captions_written", 1.0)


def captionSpin(config: AppConfig, db: DatabaseManager) -> None:
    """Create alternate caption variants for A/B testing."""
    client = _build_gemini_model(config)
    if not client:
        return

    base_caption = "When the meeting could have been an email."
    prompt = (
        f"Provide three alternate meme captions riffing on: '{base_caption}'. "
        "Return strictly JSON with 'captions' (list) and 'emotion_tags'."
    )
    payload = _call_gemini(client, prompt)
    if not payload:
        return

    try:
        variants = CaptionVariants.model_validate(payload)
    except ValidationError:
        logger.warning("Gemini returned malformed caption variants.")
        return

    db.log_event("INFO", "captionSpin", "Generated caption variants", variants.model_dump())
    db.record_metric("generation", "caption_variants", float(len(variants.captions)))


def hashtagEvolve(config: AppConfig, db: DatabaseManager) -> None:
    """Update hashtag sets based on performance and topical clusters."""
    logger.info("Running hashtagEvolve analytics.")
    metrics_path = config.analytics_dir / "hashtag_metrics.csv"
    if not metrics_path.exists():
        seed_data = pd.DataFrame(
            [
                {"hashtag": "#meme", "ctr": 0.12},
                {"hashtag": "#funny", "ctr": 0.15},
                {"hashtag": "#relatable", "ctr": 0.09},
            ]
        )
        seed_data.to_csv(metrics_path, index=False)
        return

    df = pd.read_csv(metrics_path)
    top_tags = df.sort_values("ctr", ascending=False).head(10)
    db.log_event("INFO", "hashtagEvolve", "Top hashtags updated", top_tags.to_json(orient="records"))
    for hashtag, ctr in zip(top_tags["hashtag"], top_tags["ctr"]):
        db.record_metric("analytics", "hashtag_ctr", float(ctr), context=str(hashtag))


def sentimentGuard(config: AppConfig, db: DatabaseManager) -> None:
    """Monitor sentiment and flag potentially risky captions."""
    logger.info("Running sentimentGuard checks.")
    captions_path = config.analytics_dir / "captions.json"
    if not captions_path.exists():
        logger.debug("No captions recorded; skipping sentiment guard.")
        return

    try:
        captions = json.loads(captions_path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        logger.warning("captions.json is malformed; skipping sentiment guard.")
        return

    flagged = [caption for caption in captions if _is_risky_caption(caption)]
    for caption in flagged:
        db.log_event("WARNING", "sentimentGuard", f"Flagged caption: {caption}")
    db.record_metric("safety", "captions_flagged", float(len(flagged)))


def _is_risky_caption(text: str) -> bool:
    risky_tokens = ("cancel", "offend", "lawsuit", "strike")
    lowered = text.lower()
    return any(token in lowered for token in risky_tokens)


@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=2, max=20))
def _call_gemini(client: genai.GenerativeModel, prompt: str) -> dict[str, Any]:
    """Safe Gemini wrapper returning parsed JSON or fallback text."""
    try:
        response = client.generate_content(prompt)
    except GoogleAPIError as exc:
        logger.error("Gemini API error: %s", exc)
        return {}
    except Exception:
        logger.exception("Unexpected Gemini error")
        return {}

    text = getattr(response, "text", None)
    if not text:
        logger.error("Gemini response contained no text.")
        return {}

    text = text.strip()
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        logger.warning("Gemini returned non-JSON text; wrapping it.")
        return {"text": text}
