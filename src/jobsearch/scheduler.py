"""Background task scheduler for auto-refresh and alerts."""

from __future__ import annotations

import logging
import sqlite3
from datetime import datetime, timezone
from typing import Optional

from apscheduler.schedulers.background import BackgroundScheduler

from jobsearch.scraper.engine import ScraperEngine
from jobsearch import ats_db

logger = logging.getLogger(__name__)

_scheduler_instance: Optional[BackgroundScheduler] = None


def get_scheduler() -> BackgroundScheduler:
    """Get or create the background scheduler."""
    global _scheduler_instance
    if _scheduler_instance is None:
        _scheduler_instance = BackgroundScheduler()
        _scheduler_instance.start()
        logger.info("Background scheduler started")
    return _scheduler_instance


def stop_scheduler() -> None:
    """Stop the background scheduler gracefully."""
    global _scheduler_instance
    if _scheduler_instance and _scheduler_instance.running:
        _scheduler_instance.shutdown(wait=False)
        _scheduler_instance = None
        logger.info("Background scheduler stopped")


def start_auto_refresh(
    conn: sqlite3.Connection,
    interval_hours: int = 1,
    job_id: str = "auto_refresh",
) -> None:
    """
    Start periodic auto-refresh of job search.

    Args:
        conn: Database connection
        interval_hours: How often to run scraper (default 1 hour)
        job_id: Unique identifier for this scheduled job
    """
    if interval_hours <= 0:
        stop_auto_refresh(job_id)
        return

    scheduler = get_scheduler()

    # Remove existing job if present
    try:
        scheduler.remove_job(job_id)
    except Exception:
        pass

    def _run_scraper():
        try:
            import yaml
            from jobsearch.config.settings import settings
            
            logger.info(f"Starting auto-refresh (interval={interval_hours}h)")
            
            # Load preferences and companies
            with settings.prefs_yaml.open("r", encoding="utf-8") as h:
                prefs = yaml.safe_load(h) or {}
            with settings.companies_yaml.open("r", encoding="utf-8") as h:
                comps = (yaml.safe_load(h) or {}).get("companies", [])
            
            engine = ScraperEngine(prefs, comps)
            engine.run()
            _update_last_run(conn)
            _check_for_high_score_alerts(conn)
            logger.info("Auto-refresh completed successfully")
        except Exception as e:
            logger.error(f"Auto-refresh failed: {e}", exc_info=True)

    # Schedule the job
    scheduler.add_job(
        _run_scraper,
        "interval",
        hours=interval_hours,
        id=job_id,
        name="Auto Job Search Refresh",
        replace_existing=True,
        coalesce=True,
        max_instances=1,
    )
    logger.info(f"Auto-refresh scheduled: every {interval_hours} hour(s)")


def stop_auto_refresh(job_id: str = "auto_refresh") -> None:
    """Stop the auto-refresh job."""
    scheduler = get_scheduler()
    try:
        scheduler.remove_job(job_id)
        logger.info("Auto-refresh stopped")
    except Exception:
        pass


def _update_last_run(conn: sqlite3.Connection) -> None:
    """Update last_run_at timestamp in settings."""
    now = datetime.now(timezone.utc).isoformat()
    ats_db.set_setting(conn, "last_auto_refresh_run", now)


def get_last_run(conn: sqlite3.Connection) -> Optional[str]:
    """Get ISO timestamp of last auto-refresh run."""
    return ats_db.get_setting(conn, "last_auto_refresh_run")


def get_next_run(conn: sqlite3.Connection, job_id: str = "auto_refresh") -> Optional[str]:
    """Get ISO timestamp of next scheduled auto-refresh run."""
    scheduler = get_scheduler()
    try:
        job = scheduler.get_job(job_id)
        if job and job.next_run_time:
            return job.next_run_time.isoformat()
    except Exception:
        pass
    return None


def _check_for_high_score_alerts(conn: sqlite3.Connection) -> None:
    """Check for new high-score jobs and store alert count."""
    try:
        # Get apply_now threshold from settings
        apply_now_threshold = float(
            ats_db.get_setting(conn, "apply_now_threshold") or "85"
        )

        # Count new jobs above threshold discovered in last run
        high_score_jobs = conn.execute(
            """
            SELECT COUNT(*) as cnt
            FROM applications
            WHERE score >= ?
              AND status = 'considering'
              AND date_discovered >= datetime('now', '-1 hour')
            """,
            (apply_now_threshold,),
        ).fetchone()

        count = high_score_jobs[0] if high_score_jobs else 0
        if count > 0:
            ats_db.set_setting(conn, "pending_alerts", str(count))
            logger.info(f"Found {count} new high-score jobs")
    except Exception as e:
        logger.error(f"Error checking for alerts: {e}", exc_info=True)


def get_and_clear_alerts(conn: sqlite3.Connection) -> int:
    """Get pending alert count and clear it."""
    try:
        count = int(ats_db.get_setting(conn, "pending_alerts") or "0")
        ats_db.set_setting(conn, "pending_alerts", "0")
        return count
    except Exception:
        return 0
