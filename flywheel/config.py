"""Configuration loader for the Infinity Flywheel system (Pydantic edition)."""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Annotated, Iterable, Literal, Sequence

from dotenv import load_dotenv
from pydantic import AliasChoices, Field, SecretStr, ValidationError, field_validator, model_validator
from pydantic_settings import BaseSettings, NoDecode, SettingsConfigDict

LOGGER = logging.getLogger(__name__)
DEFAULT_REDDIT_SUBS: tuple[str, ...] = ("memes", "dankmemes")
CRAWLER_REDDIT_SUBS_ENV = "APP_CRAWLER_REDDIT_SUBS"


class ConfigError(RuntimeError):
    """Raised when configuration cannot be loaded safely."""


class AppConfig(BaseSettings):
    """Strongly typed runtime configuration loaded from environment variables."""

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
        frozen=True,
        validate_default=True,
    )

    environment: Literal["development", "staging", "production"] = Field(
        default="development",
        validation_alias=AliasChoices("APP_ENVIRONMENT", "APP_ENV"),
    )
    database_path: Path = Field(
        default=Path("flywheel.db"),
        validation_alias=AliasChoices("APP_DATABASE_PATH", "DATABASE_PATH"),
    )
    log_path: Path = Field(
        default=Path("logs/flywheel.log"),
        validation_alias=AliasChoices("APP_LOG_PATH", "LOG_PATH"),
    )

    # Scheduling cadences
    scrape_interval_minutes: int = Field(15, ge=1, validation_alias=AliasChoices("APP_SCRAPE_INTERVAL", "APP_SCRAPE_INTERVAL_MINUTES"))
    trend_interval_minutes: int = Field(30, ge=1, validation_alias=AliasChoices("APP_TREND_INTERVAL", "APP_TREND_INTERVAL_MINUTES"))
    generation_interval_minutes: int = Field(45, ge=1, validation_alias=AliasChoices("APP_GENERATION_INTERVAL", "APP_GENERATION_INTERVAL_MINUTES"))
    edit_interval_minutes: int = Field(20, ge=1, validation_alias=AliasChoices("APP_EDIT_INTERVAL", "APP_EDIT_INTERVAL_MINUTES"))
    template_refresh_hour: int = Field(3, ge=0, le=23, validation_alias="APP_TEMPLATE_REFRESH_HOUR")
    caption_interval_minutes: int = Field(10, ge=1, validation_alias=AliasChoices("APP_CAPTION_INTERVAL", "APP_CAPTION_INTERVAL_MINUTES"))
    caption_spin_interval_minutes: int = Field(30, ge=1, validation_alias="APP_CAPTION_SPIN_INTERVAL")
    hashtag_evolve_interval_minutes: int = Field(60, ge=1, validation_alias="APP_HASHTAG_EVOLVE_INTERVAL")
    sentiment_guard_interval_minutes: int = Field(45, ge=1, validation_alias="APP_SENTIMENT_GUARD_INTERVAL")
    best_time_cron_minute: int = Field(5, ge=0, le=59, validation_alias="APP_BEST_TIME_MINUTE")
    upload_interval_minutes: int = Field(15, ge=1, validation_alias="APP_UPLOAD_INTERVAL")
    viral_hashlock_interval_minutes: int = Field(25, ge=1, validation_alias="APP_VIRAL_HASHLOCK_INTERVAL")
    crosspost_interval_minutes: int = Field(40, ge=1, validation_alias="APP_CROSSPOST_INTERVAL")
    story_reel_clone_minutes: int = Field(60, ge=1, validation_alias="APP_STORY_REEL_INTERVAL")
    comment_reply_minutes: int = Field(12, ge=1, validation_alias="APP_COMMENT_REPLY_INTERVAL")
    dm_welcome_minutes: int = Field(20, ge=1, validation_alias="APP_DM_WELCOME_INTERVAL")
    auto_collab_minutes: int = Field(180, ge=1, validation_alias="APP_AUTO_COLLAB_INTERVAL")
    ban_shield_minutes: int = Field(240, ge=1, validation_alias="APP_BAN_SHIELD_INTERVAL")
    ad_rev_hour: int = Field(8, ge=0, le=23, validation_alias="APP_AD_REV_HOUR")
    auto_delete_minutes: int = Field(30, ge=1, validation_alias="APP_AUTO_DELETE_INTERVAL")
    auto_drop_minutes: int = Field(90, ge=1, validation_alias="APP_AUTO_DROP_INTERVAL")
    engagement_loop_minutes: int = Field(20, ge=1, validation_alias="APP_ENGAGEMENT_LOOP_INTERVAL")
    analytics_interval_minutes: int = Field(60, ge=1, validation_alias="APP_ANALYTICS_INTERVAL")
    self_optimize_minutes: int = Field(120, ge=1, validation_alias=AliasChoices("APP_SELF_OPTIMIZE_INTERVAL", "APP_SELF_OPTIMISE_INTERVAL"))
    roi_report_hour: int = Field(22, ge=0, le=23, validation_alias="APP_ROI_REPORT_HOUR")
    human_touch_hour: int = Field(11, ge=0, le=23, validation_alias="APP_HUMAN_TOUCH_HOUR")

    # Filesystem layout
    meme_cache_dir: Path = Field(default=Path("data/memes"), validation_alias="APP_MEME_CACHE_DIR")
    render_cache_dir: Path = Field(default=Path("data/renders"), validation_alias="APP_RENDER_CACHE_DIR")
    analytics_dir: Path = Field(default=Path("data/analytics"), validation_alias="APP_ANALYTICS_DIR")
    crawler_output_dir: Path = Field(default=Path("data/raw_downloads"), validation_alias="APP_CRAWLER_OUTPUT_DIR")

    # Credentials
    gemini_api_key: SecretStr | None = Field(default=None, validation_alias="GEMINI_API_KEY")
    openai_api_key: SecretStr | None = Field(default=None, validation_alias="OPENAI_API_KEY")
    reddit_user_agent: str = Field(default="infinity-flywheel/0.1", validation_alias=AliasChoices("APP_REDDIT_USER_AGENT", "REDDIT_USER_AGENT"))
    reddit_client_id: str | None = Field(default=None, validation_alias="REDDIT_CLIENT_ID")
    reddit_client_secret: str | None = Field(default=None, validation_alias="REDDIT_CLIENT_SECRET")
    instagram_session_id: str | None = Field(default=None, validation_alias="INSTAGRAM_SESSION_ID")
    tiktok_session_id: str | None = Field(default=None, validation_alias="TIKTOK_SESSION_ID")
    youtube_api_key: SecretStr | None = Field(default=None, validation_alias="YOUTUBE_API_KEY")
    tiktok_access_token: SecretStr | None = Field(default=None, validation_alias="TIKTOK_ACCESS_TOKEN")
    tiktok_client_key: SecretStr | None = Field(default=None, validation_alias="TIKTOK_CLIENT_KEY")
    instagram_access_token: SecretStr | None = Field(default=None, validation_alias="INSTAGRAM_ACCESS_TOKEN")
    instagram_business_account_id: str | None = Field(default=None, validation_alias="INSTAGRAM_BUSINESS_ACCOUNT_ID")
    twitter_bearer_token: SecretStr | None = Field(default=None, validation_alias="TWITTER_BEARER_TOKEN")
    youtube_client_id: SecretStr | None = Field(default=None, validation_alias="YOUTUBE_CLIENT_ID")
    youtube_client_secret: SecretStr | None = Field(default=None, validation_alias="YOUTUBE_CLIENT_SECRET")
    youtube_refresh_token: SecretStr | None = Field(default=None, validation_alias="YOUTUBE_REFRESH_TOKEN")
    youtube_access_token: SecretStr | None = Field(default=None, validation_alias="YOUTUBE_ACCESS_TOKEN")
    youtube_token_uri: str = Field(default="https://oauth2.googleapis.com/token", validation_alias="YOUTUBE_TOKEN_URI")

    # Account ingestion
    ingest_instagram_accounts: Annotated[tuple[str, ...], NoDecode] = Field(
        default_factory=tuple,
        validation_alias=AliasChoices("APP_INGEST_INSTAGRAM_ACCOUNTS", "INGEST_INSTAGRAM_ACCOUNTS"),
    )
    ingest_youtube_channels: Annotated[tuple[str, ...], NoDecode] = Field(
        default_factory=tuple,
        validation_alias=AliasChoices("APP_INGEST_YOUTUBE_CHANNELS", "INGEST_YOUTUBE_CHANNELS"),
    )
    ingest_tiktok_accounts: Annotated[tuple[str, ...], NoDecode] = Field(
        default_factory=tuple,
        validation_alias=AliasChoices("APP_INGEST_TIKTOK_ACCOUNTS", "INGEST_TIKTOK_ACCOUNTS"),
    )

    # Crawler settings
    crawler_max_results: int = Field(20, ge=1, validation_alias="APP_CRAWLER_MAX_RESULTS")
    crawler_youtube_query: str = Field("viral memes", validation_alias="APP_CRAWLER_YOUTUBE_QUERY")
    crawler_movie_mode: bool = Field(False, validation_alias="APP_CRAWLER_MOVIE_MODE")
    crawler_reddit_subs: Annotated[tuple[str, ...], NoDecode] = Field(
        DEFAULT_REDDIT_SUBS, validation_alias=CRAWLER_REDDIT_SUBS_ENV
    )
    crawler_tiktok_query: str = Field("memes", validation_alias="APP_CRAWLER_TIKTOK_QUERY")
    crawler_instagram_hashtag: str = Field("memes", validation_alias="APP_CRAWLER_INSTAGRAM_HASHTAG")
    crawler_freshness_hours: int | None = Field(None, ge=1, le=24 * 14, validation_alias="APP_CRAWLER_FRESHNESS_HOURS")
    crawler_dry_run: bool = Field(False, validation_alias="APP_CRAWLER_DRY_RUN")
    crawler_min_duration: int = Field(20, ge=1, validation_alias="APP_CRAWLER_MIN_DURATION")
    crawler_max_duration: int = Field(35, ge=1, validation_alias="APP_CRAWLER_MAX_DURATION")
    crawler_min_likes: int = Field(10_000, ge=0, validation_alias="APP_CRAWLER_MIN_LIKES")
    enable_tiktok_scraper: bool = Field(False, validation_alias="APP_ENABLE_TIKTOK_SCRAPER")

    @field_validator(
        "database_path",
        "log_path",
        "meme_cache_dir",
        "render_cache_dir",
        "analytics_dir",
        "crawler_output_dir",
        mode="after",
    )
    @classmethod
    def _expand_path(cls, value: Path) -> Path:
        expanded = value.expanduser()
        return expanded if expanded.is_absolute() else (Path.cwd() / expanded).resolve()

    @field_validator(
        "ingest_instagram_accounts",
        "ingest_youtube_channels",
        "ingest_tiktok_accounts",
        mode="before",
    )
    @classmethod
    def _parse_account_list(cls, value: str | Sequence[str] | None) -> tuple[str, ...]:
        if value is None:
            return ()
        if isinstance(value, str):
            tokens = [part.strip() for part in value.replace("\n", ",").split(",") if part.strip()]
            return tuple(tokens)
        return tuple(value)

    @field_validator("crawler_reddit_subs", mode="before")
    @classmethod
    def _split_subs(cls, value: str | Sequence[str] | None) -> tuple[str, ...]:
        if value is None:
            return DEFAULT_REDDIT_SUBS
        if isinstance(value, str):
            tokens = [part.strip() for part in value.split(",") if part.strip()]
            return tuple(tokens) if tokens else DEFAULT_REDDIT_SUBS
        return tuple(value)

    @model_validator(mode="after")
    def _validate_durations(self) -> "AppConfig":
        if self.crawler_min_duration > self.crawler_max_duration:
            raise ConfigError("crawler_min_duration cannot exceed crawler_max_duration")
        return self

    def ensure_runtime_directories(self) -> None:
        """Create directories required for runtime operation."""
        _ensure_directories(
            (
                self.meme_cache_dir,
                self.render_cache_dir,
                self.analytics_dir,
                self.log_path.parent,
                self.crawler_output_dir,
            )
        )

    @property
    def has_reddit_credentials(self) -> bool:
        return bool(self.reddit_client_id and self.reddit_client_secret)

    @property
    def has_instagram_business(self) -> bool:
        return bool(self.instagram_access_token and self.instagram_business_account_id)


def _ensure_directories(paths: Iterable[Path]) -> None:
    for directory in paths:
        directory.mkdir(parents=True, exist_ok=True)


def load_config(env_path: Path | None = None) -> AppConfig:
    """Load configuration from .env/environment with validation."""
    load_kwargs: dict[str, str] = {}
    if env_path is not None:
        load_dotenv(env_path, override=False)
        load_kwargs["_env_file"] = str(env_path)
    else:
        load_dotenv(override=False)
    try:
        config = AppConfig(**load_kwargs)
    except ValidationError as exc:  # pragma: no cover - exercised in integration tests
        raise ConfigError("Invalid configuration") from exc

    config.ensure_runtime_directories()

    LOGGER.info(
        "AppConfig loaded",
        extra={
            "event": "config.loaded",
            "environment": config.environment,
            "paths": {
                "database": str(config.database_path),
                "log": str(config.log_path),
                "memes": str(config.meme_cache_dir),
            },
        },
    )
    return config
