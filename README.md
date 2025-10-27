# Infinity Flywheel – Autonomous Meme Flywheel

Infinity Flywheel is a production-ready automation stack that crawls, edits, captions, and uploads viral-ready short-form content 24/7. It now ships with typed configuration, hardened schedulers, resilient HTTP clients, and monetisation-grade upload flows.

## Topology
```
main.py                # Run-loop bootstrap + tracing metrics
flywheel/
  app.py               # Lifecycle + graceful shutdown + health snapshots
  config.py            # Pydantic settings loader (.env driven)
  db.py                # Thread-safe SQLite w/ job + health tables
  logging_utils.py     # Structured console + JSON file output
  scheduler.py         # APScheduler wrapper w/ job instrumentation
  services/
    content.py         # Account ingestion + highlight pipeline + trend analysis
    generation.py      # Gemini caption + hashtag intelligence
    distribution.py    # Instagram/TikTok stubs + YouTube Shorts uploader
    analytics.py       # ROI, oracle, optimisation loops
    community.py       # Replies, DMs, ban shield, human touch
    timing.py          # Best-time computation
  integrations/
    instagram_accounts.py | youtube_channels.py | tiktok_accounts.py
    viral_crawler         # Legacy async CC crawler bridge
  utils/
    media.py | secrets.py | highlights.py | overlay_renderer.py
```
All scheduled jobs accept `(AppConfig, DatabaseManager)` so they can emit structured logs, metrics, and durable status updates.

## Requirements
- Python 3.11+
- FFmpeg (for MoviePy)
- SQLite (bundled)
- Credentials for the platforms you plan to automate

Install dependencies:
```bash
python -m venv .venv
source .venv/bin/activate  # or .\.venv\Scripts\activate on Windows
pip install --upgrade pip
pip install -r requirements.txt
```

## Configuration (.env)
Every runtime setting is validated via `pydantic`. Create a `.env` file (see `flywheel/config.py` for defaults/aliases):

```
APP_ENVIRONMENT=production
APP_LOG_PATH=logs/flywheel.log
APP_MEME_CACHE_DIR=data/memes
APP_RENDER_CACHE_DIR=data/renders
APP_CRAWLER_OUTPUT_DIR=data/raw_downloads

# Account-based ingestion (comma-separated lists)
APP_INGEST_INSTAGRAM_ACCOUNTS=funnyfails,sportscenter
APP_INGEST_YOUTUBE_CHANNELS=UCxH9v3vC,UCz0pXYZ
APP_INGEST_TIKTOK_ACCOUNTS=@funnypets

# Platform credentials
OPENAI_API_KEY=sk-...
REDDIT_CLIENT_ID=...
REDDIT_CLIENT_SECRET=...
REDDIT_USER_AGENT=infinity-flywheel/1.0
INSTAGRAM_SESSION_ID=...
INSTAGRAM_ACCESS_TOKEN=...
INSTAGRAM_BUSINESS_ACCOUNT_ID=...
TIKTOK_SESSION_ID=...
TIKTOK_ACCESS_TOKEN=...
TIKTOK_CLIENT_KEY=...
TWITTER_BEARER_TOKEN=...
GEMINI_API_KEY=...
GEMINI_API_KEY=...

# YouTube Shorts upload OAuth (required for automatic uploads)
YOUTUBE_CLIENT_ID=...
YOUTUBE_CLIENT_SECRET=...
YOUTUBE_REFRESH_TOKEN=...
YOUTUBE_ACCESS_TOKEN=...   # optional once refresh token works

# Scheduler cadence overrides (examples)
APP_SCRAPE_INTERVAL=1440    # run nightly
APP_GENERATION_INTERVAL_MINUTES=60
APP_UPLOAD_INTERVAL=15
```
All other cadence knobs (`APP_*_INTERVAL`) plus crawler filters (`APP_CRAWLER_MIN_DURATION`, `APP_CRAWLER_MIN_LIKES`, etc.) can be overridden the same way.

## Running & Monitoring
```bash
python main.py
```
- Structured JSON logs -> `logs/flywheel.log`
- Rotating console logs for operators
- Job + health telemetry -> `flywheel.db` tables `job_runs` and `health_checks`
- Use `sqlite3 flywheel.db 'SELECT * FROM job_runs ORDER BY id DESC LIMIT 5;'` for quick audits

`MemeFlywheel.health_snapshot()` can be invoked from a REPL or integration test to retrieve scheduler status (job counts, next run times) while the app is live.

## What's New (2025 readiness)
- **Pydantic config** with typed secrets + automatic directory provisioning.
- **Resilient scheduler** that records every job run, emits health checks, and guards against multi-start.
- **Account-based ingestion** hitting YouTube channels, Instagram accounts, and TikTok handles on a nightly cadence.
- **Autonomous highlight forge** that cuts 3–10s high-energy segments, transcribes with Whisper, and renders overlays in a single job.
- **YouTube monetisation** via OAuth credentials, metadata-rich uploads (title/description/hashtags) with automatic retries and DB metrics.
- **Observability**: every critical action emits metrics via `DatabaseManager.record_metric`, making it trivial to chart uploads, engagement loops, and safety checks.
- **Tooling**: `mypy --strict` and `ruff check .` are both clean thanks to the new `mypy.ini` and lint-friendly code style.

## Cron / Daemonising
Use systemd, pm2, or cron to keep the flywheel alive:
```
@reboot /path/to/python /path/to/flycontent/main.py >> /var/log/flywheel.out 2>&1
```
All jobs are idempotent and maintain their own cache files/DB rows, so restarts recover automatically.

## Health & Recovery
- SIGINT/SIGTERM triggers a graceful stop via `MemeFlywheel.stop()`
- Scheduler + app health snapshots are written to the database for dashboards
- Job failures emit metrics + log rows for post-mortems
- Account ingestors skip unavailable credentials and record metrics so you can catch gaps quickly

## Testing & QA
```
ruff check .
mypy --strict
```
Add targeted unit tests around new services as needed. For live validation, tail `logs/flywheel.log` and inspect `flywheel.db` metrics.

---
Need a new platform or monetisation strategy? Drop a service in `flywheel/services/`, add it to `JOB_SPECS`, and you inherit config, logging, metrics, and scheduler guarantees for free.
