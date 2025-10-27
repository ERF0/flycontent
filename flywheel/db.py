"""SQLite database helpers for persistence, analytics, and health tracking."""

from __future__ import annotations

import json
import logging
import sqlite3
import threading
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterator, Literal, Optional

from .config import AppConfig

logger = logging.getLogger(__name__)

SCHEMA = """
CREATE TABLE IF NOT EXISTS logs (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    timestamp TEXT NOT NULL,
    level TEXT NOT NULL,
    component TEXT NOT NULL,
    message TEXT NOT NULL,
    payload TEXT
);

CREATE TABLE IF NOT EXISTS metrics (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    timestamp TEXT NOT NULL,
    platform TEXT NOT NULL,
    metric TEXT NOT NULL,
    value REAL NOT NULL,
    context TEXT
);

CREATE TABLE IF NOT EXISTS posts (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    platform TEXT NOT NULL,
    external_id TEXT,
    status TEXT NOT NULL,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    performance_score REAL DEFAULT 0,
    metadata TEXT
);

CREATE INDEX IF NOT EXISTS idx_logs_timestamp ON logs(timestamp);
CREATE INDEX IF NOT EXISTS idx_metrics_platform ON metrics(platform);
CREATE INDEX IF NOT EXISTS idx_posts_platform ON posts(platform);
CREATE TABLE IF NOT EXISTS job_runs (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    job_id TEXT NOT NULL,
    status TEXT NOT NULL,
    started_at TEXT NOT NULL,
    duration_ms REAL NOT NULL,
    error TEXT,
    created_at TEXT NOT NULL DEFAULT (datetime('now'))
);
CREATE INDEX IF NOT EXISTS idx_job_runs_job_id ON job_runs(job_id);
CREATE TABLE IF NOT EXISTS health_checks (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    component TEXT NOT NULL,
    status TEXT NOT NULL,
    detail TEXT,
    observed_at TEXT NOT NULL DEFAULT (datetime('now'))
);
CREATE INDEX IF NOT EXISTS idx_health_component ON health_checks(component);
"""


class DatabaseManager:
    """Thread-safe SQLite manager for Flywheel."""

    def __init__(self, config: AppConfig) -> None:
        self.path = Path(config.database_path)
        self._local = threading.local()
        self._init_schema_once()

    # ------------------------------------------------------------------ #
    # Connection handling
    # ------------------------------------------------------------------ #

    def _get_conn(self) -> sqlite3.Connection:
        """Return a per-thread connection to the database."""
        if not hasattr(self._local, "conn"):
            conn = sqlite3.connect(self.path, check_same_thread=False, isolation_level=None)
            conn.row_factory = sqlite3.Row
            self._local.conn = conn
            logger.debug("Opened thread-local DB connection at %s", self.path)
        return self._local.conn

    def _init_schema_once(self) -> None:
        """Ensure the schema exists once at startup."""
        conn = sqlite3.connect(self.path)
        conn.executescript(SCHEMA)
        conn.commit()
        conn.close()
        logger.debug("Database schema ensured at %s", self.path)

    # ------------------------------------------------------------------ #
    # Context manager
    # ------------------------------------------------------------------ #

    @contextmanager
    def cursor(self) -> Iterator[sqlite3.Cursor]:
        """Provide a safe transactional cursor."""
        conn = self._get_conn()
        cur = conn.cursor()
        try:
            yield cur
            conn.commit()
        except Exception:
            conn.rollback()
            logger.exception("Database operation failed; rolled back transaction.")
            raise
        finally:
            cur.close()

    # ------------------------------------------------------------------ #
    # Public methods
    # ------------------------------------------------------------------ #

    def log_event(self, level: str, component: str, message: str, payload: Any | None = None) -> None:
        with self.cursor() as cur:
            cur.execute(
                "INSERT INTO logs(timestamp, level, component, message, payload) "
                "VALUES(datetime('now'), ?, ?, ?, ?)",
                (
                    str(level),
                    str(component),
                    str(message),
                    self._normalize_payload(payload),
                ),
            )

    def record_metric(self, platform: str, metric: str, value: float, context: str | None = None) -> None:
        with self.cursor() as cur:
            cur.execute(
                "INSERT INTO metrics(timestamp, platform, metric, value, context) "
                "VALUES(datetime('now'), ?, ?, ?, ?)",
                (platform, metric, value, context),
            )

    def record_job_run(
        self,
        *,
        job_id: str,
        status: Literal["success", "failure"],
        started_at: datetime,
        duration_ms: float,
        error: str | None = None,
    ) -> None:
        """Persist job execution metadata for analytics and health checks."""
        with self.cursor() as cur:
            cur.execute(
                """
                INSERT INTO job_runs(job_id, status, started_at, duration_ms, error)
                VALUES(?, ?, ?, ?, ?)
                """,
                (
                    job_id,
                    status,
                    started_at.astimezone(timezone.utc).isoformat().replace("+00:00", "Z"),
                    float(duration_ms),
                    error,
                ),
            )

    def record_health(
        self,
        *,
        component: str,
        status: Literal["pass", "warn", "fail"],
        detail: str | None = None,
    ) -> None:
        """Store health-check snapshots for external dashboards."""
        with self.cursor() as cur:
            cur.execute(
                """
                INSERT INTO health_checks(component, status, detail)
                VALUES(?, ?, ?)
                """,
                (component, status, detail),
            )

    def update_post_status(
        self,
        platform: str,
        status: str,
        external_id: str | None = None,
        performance_score: float | None = None,
        metadata: str | None = None,
    ) -> None:
        with self.cursor() as cur:
            if external_id:
                cur.execute(
                    """
                    UPDATE posts
                    SET
                        status = ?,
                        updated_at = datetime('now'),
                        performance_score = COALESCE(?, performance_score),
                        metadata = COALESCE(?, metadata)
                    WHERE platform = ? AND external_id = ?
                    """,
                    (status, performance_score, metadata, platform, external_id),
                )
                if cur.rowcount:
                    return

            cur.execute(
                """
                INSERT INTO posts(platform, external_id, status, created_at, updated_at,
                                  performance_score, metadata)
                VALUES(?, ?, ?, datetime('now'), datetime('now'), COALESCE(?, 0), ?)
                """,
                (platform, external_id, status, performance_score, metadata),
            )

    def close(self) -> None:
        conn: Optional[sqlite3.Connection] = getattr(self._local, "conn", None)
        if conn:
            conn.close()
            del self._local.conn
            logger.debug("Thread-local database connection closed.")

    # ------------------------------------------------------------------ #
    # Utility
    # ------------------------------------------------------------------ #

    @staticmethod
    def _normalize_payload(payload: Any | None) -> str | None:
        if payload is None:
            return None
        if isinstance(payload, str):
            return payload
        try:
            return json.dumps(payload, default=str)
        except TypeError:
            return str(payload)
