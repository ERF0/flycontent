"""Entry point for the Infinity Flywheel autonomous meme system."""

from __future__ import annotations

import json
import logging
import os
import socket
import threading
import time
import uuid
from contextlib import suppress
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Final

from flywheel.app import MemeFlywheel

LOGGER = logging.getLogger(__name__)
_RUN_GUARD: Final[threading.Lock] = threading.Lock()
_IS_RUNNING = False


@dataclass(frozen=True, slots=True)
class RunContext:
    """Captures immutable metadata for a single flywheel invocation."""

    trace_id: str
    instance_id: str
    wall_clock_ns: int
    monotonic_ns: int

    @property
    def started_at_iso(self) -> str:
        """Return the ISO8601 timestamp (UTC) for when the run began."""
        seconds = self.wall_clock_ns / 1_000_000_000
        return datetime.fromtimestamp(seconds, tz=timezone.utc).isoformat().replace("+00:00", "Z")


def _build_run_context() -> RunContext:
    """Construct a trace and instance aware context for the current process."""
    trace_id = os.getenv("FLYWHEEL_TRACE_ID") or uuid.uuid4().hex
    instance_id = os.getenv("FLYWHEEL_INSTANCE_ID") or socket.gethostname()
    return RunContext(
        trace_id=trace_id,
        instance_id=instance_id,
        wall_clock_ns=time.time_ns(),
        monotonic_ns=time.perf_counter_ns(),
    )


def _log_event(level: int, event: str, context: RunContext, **fields: Any) -> None:
    """Emit structured JSON logs with consistent tracing metadata."""
    payload: dict[str, Any] = {
        "event": event,
        "trace_id": context.trace_id,
        "instance_id": context.instance_id,
        "started_at": context.started_at_iso,
        **fields,
    }
    LOGGER.log(level, json.dumps(payload, default=str, separators=(",", ":")))


def _emit_metric(name: str, value: float, unit: str, context: RunContext, **labels: Any) -> None:
    """Record lightweight metrics via structured logs for downstream scraping."""
    metric_fields = {"metric_name": name, "value": value, "unit": unit, **labels}
    _log_event(logging.INFO, "metric", context, **metric_fields)


def _acquire_run_guard() -> bool:
    """Enforce idempotent process startup to avoid double-scheduling."""
    global _IS_RUNNING
    with _RUN_GUARD:
        if _IS_RUNNING:
            return False
        _IS_RUNNING = True
        return True


def _release_run_guard() -> None:
    """Release the idempotency guard so future invocations can proceed."""
    global _IS_RUNNING
    with _RUN_GUARD:
        _IS_RUNNING = False


def _stop_app(app: MemeFlywheel, context: RunContext, reason: str) -> None:
    """Best-effort request for the scheduler to halt gracefully."""
    _log_event(logging.WARNING, "flywheel.stop_requested", context, reason=reason)
    with suppress(Exception):
        app.stop()


def main() -> None:
    """Bootstrap, observe, and run the meme flywheel application."""
    context = _build_run_context()
    if not _acquire_run_guard():
        _log_event(logging.INFO, "flywheel.already_running", context, detail="duplicate_main_invocation")
        return

    app: MemeFlywheel | None = None
    bootstrap_start_ns = time.perf_counter_ns()

    try:
        _log_event(logging.INFO, "flywheel.bootstrap_start", context)
        app = MemeFlywheel()
        bootstrap_ms = (time.perf_counter_ns() - bootstrap_start_ns) / 1_000_000
        _emit_metric("bootstrap_duration_ms", bootstrap_ms, "milliseconds", context)
        _log_event(logging.INFO, "flywheel.bootstrap_complete", context, duration_ms=round(bootstrap_ms, 2))

        run_start_ns = time.perf_counter_ns()
        app.start()
        runtime_ms = (time.perf_counter_ns() - run_start_ns) / 1_000_000
        _emit_metric("run_duration_ms", runtime_ms, "milliseconds", context)
        _log_event(logging.INFO, "flywheel.run_completed", context, duration_ms=round(runtime_ms, 2))
    except KeyboardInterrupt:
        if app is not None:
            _stop_app(app, context, reason="keyboard_interrupt")
        _log_event(logging.WARNING, "flywheel.interrupted", context, signal="SIGINT")
    except Exception as exc:
        if app is not None:
            _stop_app(app, context, reason="unhandled_exception")
        error_fields = {"error_type": type(exc).__name__, "error_message": str(exc)}
        _emit_metric("run_failure", 1.0, "count", context, **error_fields)
        _log_event(logging.CRITICAL, "flywheel.run_failed", context, **error_fields)
        raise
    finally:
        _release_run_guard()


if __name__ == "__main__":
    main()
