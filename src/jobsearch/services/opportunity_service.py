"""Service for managing Job/Application records in the unified database."""

from __future__ import annotations
import sqlite3
from datetime import datetime, timezone
from typing import List, Optional, Dict, Any
from jobsearch.scraper.models import Job

def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()

def _safe_str(val: Any) -> str:
    """Ensure a value is a clean string."""
    if pd.isna(val) or val is None:
        return ""
    return str(val).strip()

def _safe_float(val: Any) -> Optional[float]:
    """Convert value to float safely."""
    if pd.isna(val) or val is None:
        return None
    try:
        return float(val)
    except (ValueError, TypeError):
        return None

import pandas as pd

_BUCKET_TO_STATUS: Dict[str, str] = {
    "APPLY NOW":     "considering",
    "REVIEW TODAY":  "considering",
    "WATCH":         "considering",
    "MANUAL REVIEW": "considering",
    "IGNORE":        "withdrawn",
}

_STAGE_TO_STATUS: Dict[str, str] = {
    "new": "exploring",
    "scored": "considering",
    "shortlisted": "considering",
    "applied": "applied",
    "recruiter screen": "screening",
    "phone screen": "screening",
    "screening": "screening",
    "hiring manager": "interviewing",
    "panel": "interviewing",
    "final round": "interviewing",
    "interviewing": "interviewing",
    "offer": "offer",
    "accepted": "accepted",
    "rejected": "rejected",
    "archived": "withdrawn",
    "withdrawn": "withdrawn",
    "considering": "considering",
    "exploring": "exploring",
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


def _normalize_status_and_stage(job: Job) -> tuple[str, str]:
    raw_stage = str(getattr(job, "current_stage", "") or "").strip()
    if not raw_stage or raw_stage.lower() == "new":
        return "considering", "considering"

    return _STAGE_TO_STATUS.get(raw_stage.lower(), raw_stage.lower()), raw_stage

def upsert_job(conn: sqlite3.Connection, job: Job) -> tuple[bool, int]:
    """
    Insert or update a Job from the scraper.
    Returns (inserted, app_id).
    """
    existing = conn.execute(
        "SELECT id, status FROM applications WHERE scraper_key = ?", (job.id,)
    ).fetchone()

    now = _now_iso()
    status, stage_label = _normalize_status_and_stage(job)
    
    if existing is None:
        # INSERT
        cur = conn.execute(
            """
            INSERT INTO applications (
                company, role, role_normalized, job_url, source, scraper_key,
                status, score, fit_band, matched_keywords, penalized_keywords,
                decision_reason, description_excerpt, location, is_remote,
                salary_text, salary_low, salary_high, date_discovered, date_applied,
                user_priority, tier, notes, created_at, updated_at
            ) VALUES (
                :company, :role, :role_normalized, :url, :source, :id,
                :status, :score, :fit_band, :matched_keywords, :penalized_keywords,
                :decision_reason, :description_excerpt, :location, :is_remote,
                :salary_text, :salary_min, :salary_max, :date_discovered, :date_applied,
                :user_priority, :tier, :notes, :created_at, :updated_at
            )
            """,
            {
                "company": job.company,
                "role": job.role_title_raw,
                "role_normalized": job.role_title_normalized,
                "url": job.url,
                "source": job.source,
                "id": job.id,
                "status": status,
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
                "date_applied": job.date_applied,
                "user_priority": job.user_priority,
                "tier": job.tier,
                "notes": job.notes,
                "created_at": now,
                "updated_at": now,
            }
        )
        app_id = cur.lastrowid
        _insert_stage_history(conn, app_id, None, stage_label, "Scraper discovery")
        return True, app_id
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
        return False, app_id
