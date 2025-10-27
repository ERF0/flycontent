"""Top-level application controller for the Infinity Flywheel system."""

from __future__ import annotations

import json
import logging
import signal
import threading
from dataclasses import dataclass
from types import FrameType
from typing import Any, Callable, Final, Literal, Optional

from .config import AppConfig, load_config
from .db import DatabaseManager
from .logging_utils import configure_logging
from .scheduler import SchedulerManager
from .services.analytics import (
    analyticsOracle,
    autoDeleteFlop,
    autoDrop,
    engagementLoop,
    roiPrint,
    selfOptimise,
)
from .services.community import (
    autoCollabDM,
    banShield,
    commentReplyGPT,
    dmWelcomeFunnel,
    humanTouch,
)
from .services.content import (
    autoAesthetic,
    autoTrend,
    highlightForge,
    scrapMeme,
    storyReelClone,
    templateBreeder,
)
from .services.distribution import (
    adRevSpinup,
    crossPostTikTok,
    uploadMemes,
    viralHashlock,
)
from .services.generation import (
    captionSpin,
    generateCaption,
    hashtagEvolve,
    sentimentGuard,
)
from .services.timing import bestTimeOrion

logger = logging.getLogger(__name__)

JobTrigger = Literal["interval", "cron"]


def _log_event(level: int, event: str, **fields: Any) -> None:
    """Emit structured log events with consistent metadata."""
    payload = {"event": event, **fields}
    logger.log(level, json.dumps(payload, default=str, separators=(",", ":")))


@dataclass(frozen=True, slots=True)
class JobSpec:
    """Describes a single scheduled job and its config-driven cadence."""

    func: Callable[[AppConfig, DatabaseManager], None]
    trigger: JobTrigger
    job_id: str
    schedule_fields: tuple[tuple[str, str], ...]

    def build_schedule_kwargs(self, config: AppConfig) -> dict[str, int]:
        """Read scheduler keyword arguments from the AppConfig instance."""
        return {key: getattr(config, attr) for key, attr in self.schedule_fields}


JOB_SPECS: Final[tuple[JobSpec, ...]] = (
    JobSpec(scrapMeme, "interval", "scrape_meme", (("minutes", "scrape_interval_minutes"),)),
    JobSpec(autoTrend, "interval", "auto_trend", (("minutes", "trend_interval_minutes"),)),
    JobSpec(highlightForge, "interval", "highlight_pipeline", (("minutes", "generation_interval_minutes"),)),
    JobSpec(autoAesthetic, "interval", "auto_aesthetic", (("minutes", "edit_interval_minutes"),)),
    JobSpec(templateBreeder, "cron", "template_breeder", (("hour", "template_refresh_hour"),)),
    JobSpec(generateCaption, "interval", "generate_caption", (("minutes", "caption_interval_minutes"),)),
    JobSpec(captionSpin, "interval", "caption_spin", (("minutes", "caption_spin_interval_minutes"),)),
    JobSpec(hashtagEvolve, "interval", "hashtag_evolve", (("minutes", "hashtag_evolve_interval_minutes"),)),
    JobSpec(sentimentGuard, "interval", "sentiment_guard", (("minutes", "sentiment_guard_interval_minutes"),)),
    JobSpec(bestTimeOrion, "cron", "best_time_orion", (("minute", "best_time_cron_minute"),)),
    JobSpec(uploadMemes, "interval", "upload_memes", (("minutes", "upload_interval_minutes"),)),
    JobSpec(viralHashlock, "interval", "viral_hashlock", (("minutes", "viral_hashlock_interval_minutes"),)),
    JobSpec(crossPostTikTok, "interval", "crosspost_tiktok", (("minutes", "crosspost_interval_minutes"),)),
    JobSpec(storyReelClone, "interval", "story_reel_clone", (("minutes", "story_reel_clone_minutes"),)),
    JobSpec(commentReplyGPT, "interval", "comment_reply_gpt", (("minutes", "comment_reply_minutes"),)),
    JobSpec(dmWelcomeFunnel, "interval", "dm_welcome_funnel", (("minutes", "dm_welcome_minutes"),)),
    JobSpec(autoCollabDM, "interval", "auto_collab_dm", (("minutes", "auto_collab_minutes"),)),
    JobSpec(banShield, "interval", "ban_shield", (("minutes", "ban_shield_minutes"),)),
    JobSpec(adRevSpinup, "cron", "ad_rev_spinup", (("hour", "ad_rev_hour"),)),
    JobSpec(autoDeleteFlop, "interval", "auto_delete_flop", (("minutes", "auto_delete_minutes"),)),
    JobSpec(autoDrop, "interval", "auto_drop", (("minutes", "auto_drop_minutes"),)),
    JobSpec(engagementLoop, "interval", "engagement_loop", (("minutes", "engagement_loop_minutes"),)),
    JobSpec(analyticsOracle, "interval", "analytics_oracle", (("minutes", "analytics_interval_minutes"),)),
    JobSpec(selfOptimise, "interval", "self_optimise", (("minutes", "self_optimize_minutes"),)),
    JobSpec(roiPrint, "cron", "roi_print", (("hour", "roi_report_hour"),)),
    JobSpec(humanTouch, "cron", "human_touch", (("hour", "human_touch_hour"),)),
)


class MemeFlywheel:
    """Coordinates scheduling, execution, and graceful shutdown for the meme system."""

    def __init__(self, config: Optional[AppConfig] = None) -> None:
        self.config = config or load_config()
        configure_logging(self.config)
        self.db = DatabaseManager(self.config)
        self.scheduler = SchedulerManager(self.config, self.db)
        self._stop_event = threading.Event()
        self._lifecycle_lock = threading.Lock()
        self._is_running = False
        self._signals_installed = False
        self._configure_jobs()
        _log_event(logging.INFO, "flywheel.initialized", environment=self.config.environment)

    def _configure_jobs(self) -> None:
        """Register scheduled jobs with the background scheduler."""
        for spec in JOB_SPECS:
            schedule_kwargs = spec.build_schedule_kwargs(self.config)
            try:
                self.scheduler.add_recurring_job(
                    func=spec.func,
                    trigger=spec.trigger,
                    id=spec.job_id,
                    **schedule_kwargs,
                )
            except Exception as exc:  # pragma: no cover - unexpected scheduler failure
                _log_event(
                    logging.CRITICAL,
                    "flywheel.job_registration_failed",
                    job_id=spec.job_id,
                    trigger=spec.trigger,
                    schedule=schedule_kwargs,
                    error=str(exc),
                )
                raise RuntimeError(f"Failed to register job {spec.job_id}") from exc
            else:
                _log_event(
                    logging.DEBUG,
                    "flywheel.job_registered",
                    job_id=spec.job_id,
                    trigger=spec.trigger,
                    schedule=schedule_kwargs,
                )

    def _install_signal_handlers(self) -> None:
        """Attach SIGTERM/SIGINT handlers when running on the main thread."""
        if self._signals_installed:
            return
        if threading.current_thread() is not threading.main_thread():
            _log_event(logging.WARNING, "flywheel.signal_handlers_skipped", reason="not_main_thread")
            return

        signal.signal(signal.SIGTERM, self._handle_signal)
        signal.signal(signal.SIGINT, self._handle_signal)
        self._signals_installed = True
        _log_event(logging.INFO, "flywheel.signal_handlers_installed")

    def _handle_signal(self, signum: int, frame: FrameType | None) -> None:
        """Signal handler that forwards into the graceful stop logic."""
        _log_event(logging.WARNING, "flywheel.signal_received", signal=signum)
        self.stop()

    def start(self) -> None:
        """Start the scheduler and block until termination is requested."""
        with self._lifecycle_lock:
            if self._is_running:
                _log_event(logging.INFO, "flywheel.start_ignored", reason="already_running")
                return
            self._is_running = True
            self._stop_event.clear()

        self._install_signal_handlers()
        _log_event(logging.INFO, "flywheel.starting", jobs=len(JOB_SPECS))

        try:
            self.scheduler.start()
            _log_event(logging.INFO, "flywheel.started")
            self.db.record_health(component="flywheel", status="pass", detail="scheduler_started")
            self._stop_event.wait()
        except Exception as exc:
            _log_event(logging.CRITICAL, "flywheel.start_failed", error=str(exc))
            self.db.record_health(component="flywheel", status="fail", detail=str(exc))
            raise
        finally:
            self._shutdown_resources()

    def _shutdown_resources(self) -> None:
        """Shut down scheduler and database connections safely."""
        try:
            self.scheduler.shutdown()
            _log_event(logging.INFO, "flywheel.scheduler_shutdown")
        except Exception as exc:
            _log_event(logging.ERROR, "flywheel.scheduler_shutdown_failed", error=str(exc))
        finally:
            with self._lifecycle_lock:
                self._is_running = False

        try:
            self.db.close()
            _log_event(logging.INFO, "flywheel.database_closed")
        except Exception as exc:  # pragma: no cover - relies on db backend
            _log_event(logging.ERROR, "flywheel.database_close_failed", error=str(exc))

        self._stop_event.clear()

    def stop(self) -> None:
        """Signal the application to stop."""
        with self._lifecycle_lock:
            if not self._is_running:
                _log_event(logging.INFO, "flywheel.stop_ignored", reason="not_running")
                return
            if self._stop_event.is_set():
                _log_event(logging.DEBUG, "flywheel.stop_redundant")
                return
            self._stop_event.set()
            _log_event(logging.WARNING, "flywheel.stop_requested")
            self.db.record_health(component="flywheel", status="warn", detail="stop_requested")

    def health_snapshot(self) -> dict[str, Any]:
        """Return current health metadata for dashboards/CLI calls."""
        snapshot = self.scheduler.snapshot()
        return {
            "environment": self.config.environment,
            "scheduler": {
                "total_jobs": snapshot.total_jobs,
                "running": snapshot.running,
                "next_runs": snapshot.next_runs,
            },
        }
