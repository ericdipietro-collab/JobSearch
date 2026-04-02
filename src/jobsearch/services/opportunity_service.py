"""Service for managing Job/Application records in the unified database."""

from __future__ import annotations
import sqlite3
from datetime import datetime, timezone
from typing import List, Optional, Dict, Any
from ..scraper.models import Job

def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()

_BUCKET_TO_STATUS: Dict[str, str] = {
    "APPLY NOW":     "considering",
    "REVIEW TODAY":  "considering",
    "WATCH":         "considering",
    "MANUAL REVIEW": "considering",
    "IGNORE":        "withdrawn",
}

def _insert_stage_history(
    conn: sqlite3.Connection,
    app_id: int,
    from_stage: Optional[str],
    to_stage: str,
    note: str = "",
) -> None:
    conn.execute(
        """
        INSERT INTO stage_history (application_id, from_stage, to_stage, timestamp, note)
        VALUES (?, ?, ?, ?, ?)
        """,
        (app_id, from_stage, to_stage, _now_iso(), note),
    )

def upsert_job(conn: sqlite3.Connection, job: Job) -> bool:
    """
    Insert or update a Job from the scraper.
    Returns True if inserted, False if updated.
    """
    existing = conn.execute(
        "SELECT id, status FROM applications WHERE scraper_key = ?", (job.id,)
    ).fetchone()

    now = _now_iso()
    
    if existing is None:
        # INSERT
        cur = conn.execute(
            """
            INSERT INTO applications (
                company, role, role_normalized, job_url, source, scraper_key,
                status, score, fit_band, matched_keywords, penalized_keywords,
                decision_reason, description_excerpt, location, is_remote,
                salary_text, salary_low, salary_high, date_discovered,
                user_priority, notes, created_at, updated_at
            ) VALUES (
                :company, :role, :role_normalized, :url, :source, :id,
                :status, :score, :fit_band, :matched_keywords, :penalized_keywords,
                :decision_reason, :description_excerpt, :location, :is_remote,
                :salary_text, :salary_min, :salary_max, :date_discovered,
                :user_priority, :notes, :created_at, :updated_at
            )
            """,
            {
                "company": job.company,
                "role": job.role_title_raw,
                "role_normalized": job.role_title_normalized,
                "url": job.url,
                "source": job.source,
                "id": job.id,
                "status": "considering",
                "score": job.score,
                "fit_band": job.fit_band,
                "matched_keywords": job.matched_keywords,
                "penalized_keywords": job.penalized_keywords,
                "decision_reason": job.decision_reason,
                "description_excerpt": job.description_excerpt,
                "location": job.location,
                "is_remote": int(job.is_remote),
                "salary_text": job.salary_text,
                "salary_min": job.salary_min,
                "salary_max": job.salary_max,
                "date_discovered": job.date_discovered or now,
                "user_priority": job.user_priority,
                "notes": job.notes,
                "created_at": now,
                "updated_at": now,
            }
        )
        _insert_stage_history(conn, cur.lastrowid, None, "considering", "Scraper discovery")
        return True
    else:
        # UPDATE scraper-owned fields
        app_id = existing["id"]
        conn.execute(
            """
            UPDATE applications SET
                score = :score,
                fit_band = :fit_band,
                matched_keywords = :matched_keywords,
                penalized_keywords = :penalized_keywords,
                decision_reason = :decision_reason,
                description_excerpt = :description_excerpt,
                updated_at = :updated_at
            WHERE id = :app_id
            """,
            {
                "app_id": app_id,
                "score": job.score,
                "fit_band": job.fit_band,
                "matched_keywords": job.matched_keywords,
                "penalized_keywords": job.penalized_keywords,
                "decision_reason": job.decision_reason,
                "description_excerpt": job.description_excerpt,
                "updated_at": now,
            }
        )
        return False
