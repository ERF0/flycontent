"""Scheduler management with observability and persistence hooks."""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Callable

from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.schedulers.base import STATE_RUNNING
from apscheduler.triggers.cron import CronTrigger
from apscheduler.triggers.interval import IntervalTrigger

from .config import AppConfig
from .db import DatabaseManager

logger = logging.getLogger(__name__)


JobCallable = Callable[[AppConfig, DatabaseManager], None]


@dataclass(frozen=True, slots=True)
class SchedulerSnapshot:
    """Snapshot of job registrations and high-level runtime state."""

    total_jobs: int
    running: bool
    next_runs: dict[str, str | None]


class SchedulerManager:
    """Wrap APScheduler with structured logging and health tracking."""

    def __init__(self, config: AppConfig, db: DatabaseManager) -> None:
        self.config = config
        self.db = db
        self.scheduler = BackgroundScheduler(
            job_defaults={
                "coalesce": True,
                "max_instances": 1,
                "misfire_grace_time": 90,
            }
        )

    def start(self) -> None:
        self.scheduler.start()
        logger.info("Scheduler started with %d jobs", len(self.scheduler.get_jobs()))
        self.publish_health()

    def shutdown(self) -> None:
        self.scheduler.shutdown(wait=True)
        logger.info("Scheduler shutdown complete.")
        self.publish_health()

    def add_recurring_job(
        self,
        func: JobCallable,
        *,
        trigger: str,
        id: str,
        **trigger_kwargs,
    ) -> None:
        if trigger == "interval":
            trig = IntervalTrigger(**trigger_kwargs)
        elif trigger == "cron":
            trig = CronTrigger(**trigger_kwargs)
        else:
            raise ValueError(f"Unsupported trigger type: {trigger}")

        def wrapped_job() -> None:
            start_time = datetime.now(timezone.utc)
            try:
                logger.debug("Running job %s", id)
                func(self.config, self.db)
                duration_ms = (datetime.now(timezone.utc) - start_time).total_seconds() * 1000
                logger.debug("Job %s completed in %.2fms", id, duration_ms)
                self.db.record_job_run(
                    job_id=id,
                    status="success",
                    started_at=start_time,
                    duration_ms=duration_ms,
                )
                self.db.record_health(component=f"job:{id}", status="pass", detail=f"{duration_ms:.2f}ms")
            except Exception as exc:
                duration_ms = (datetime.now(timezone.utc) - start_time).total_seconds() * 1000
                logger.exception("Job %s failed", id)
                self.db.record_job_run(
                    job_id=id,
                    status="failure",
                    started_at=start_time,
                    duration_ms=duration_ms,
                    error=str(exc),
                )
                self.db.record_health(component=f"job:{id}", status="fail", detail=str(exc))

        next_run = datetime.now(self.scheduler.timezone) if trigger == "interval" else None
        self.scheduler.add_job(
            wrapped_job,
            trig,
            id=id,
            replace_existing=True,
            max_instances=1,
            next_run_time=next_run,
        )
        logger.info("Registered job %s with trigger %s", id, trigger)

    def snapshot(self) -> SchedulerSnapshot:
        """Return a snapshot of scheduler state for external health checks."""
        jobs = self.scheduler.get_jobs()
        next_runs = {
            job.id: job.next_run_time.isoformat() if job.next_run_time else None for job in jobs
        }
        running = self.scheduler.state == STATE_RUNNING
        return SchedulerSnapshot(total_jobs=len(jobs), running=running, next_runs=next_runs)

    def publish_health(self) -> None:
        """Persist scheduler health into the database for dashboards."""
        snapshot = self.snapshot()
        status = "pass" if snapshot.running else "fail"
        detail = json.dumps({"next_runs": snapshot.next_runs}) if snapshot.next_runs else None
        self.db.record_health(component="scheduler", status=status, detail=detail)
