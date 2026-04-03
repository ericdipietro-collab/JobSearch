"""Service for managing Job/Application records in the unified database."""

from __future__ import annotations
import sqlite3
import hashlib
from datetime import datetime, timezone
from difflib import SequenceMatcher
from typing import List, Optional, Dict, Any
from jobsearch.scraper.models import Job
from jobsearch import ats_db

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


def _normalize_jd_blob(description_excerpt: str, salary_text: str, location: str) -> str:
    parts = [str(description_excerpt or "").strip().lower(), str(salary_text or "").strip().lower(), str(location or "").strip().lower()]
    return " | ".join(part for part in parts if part)


def _jd_fingerprint(description_excerpt: str, salary_text: str, location: str) -> str:
    blob = _normalize_jd_blob(description_excerpt, salary_text, location)
    return hashlib.sha256(blob.encode("utf-8")).hexdigest() if blob else ""


def _jd_change_summary(old_excerpt: str, new_excerpt: str, old_salary: str, new_salary: str, old_location: str, new_location: str) -> str:
    notes = []
    if old_salary != new_salary:
        notes.append(f"salary: {old_salary or 'n/a'} -> {new_salary or 'n/a'}")
    if old_location != new_location:
        notes.append(f"location: {old_location or 'n/a'} -> {new_location or 'n/a'}")
    if old_excerpt and new_excerpt:
        ratio = SequenceMatcher(None, old_excerpt, new_excerpt).ratio()
        if ratio < 0.82:
            notes.append(f"description changed materially ({ratio:.0%} similarity)")
    elif old_excerpt != new_excerpt:
        notes.append("description excerpt changed")
    return "; ".join(notes) or "job description changed"


def _is_material_jd_change(old_excerpt: str, new_excerpt: str, old_salary: str, new_salary: str, old_location: str, new_location: str) -> bool:
    if old_salary != new_salary or old_location != new_location:
        return True
    if not old_excerpt or not new_excerpt:
        return old_excerpt != new_excerpt
    ratio = SequenceMatcher(None, old_excerpt, new_excerpt).ratio()
    return ratio < 0.82


def _record_jd_change(conn: sqlite3.Connection, app_id: int, summary: str, timestamp: str) -> None:
    conn.execute(
        """
        INSERT INTO events (application_id, event_type, event_date, title, notes, created_at)
        VALUES (?, ?, ?, ?, ?, ?)
        """,
        (app_id, "jd_changed", timestamp[:10], "Job description changed", summary, timestamp),
    )


def _record_job_observation(conn: sqlite3.Connection, app_id: int, job: Job, timestamp: str, jd_fingerprint: str) -> None:
    latest_seen_at = ats_db.latest_job_observation_date(conn, app_id)
    if latest_seen_at and latest_seen_at[:10] == timestamp[:10]:
        return
    ats_db.add_job_observation(
        conn,
        application_id=app_id,
        seen_at=timestamp,
        score=job.score,
        description_excerpt=job.description_excerpt,
        salary_text=job.salary_text,
        location=job.location,
        jd_fingerprint=jd_fingerprint,
    )

def upsert_job(conn: sqlite3.Connection, job: Job) -> tuple[bool, int]:
    """
    Insert or update a Job from the scraper.
    Returns (inserted, app_id).
    """
    existing = conn.execute(
        """
        SELECT id, status, description_excerpt, salary_text, location, jd_fingerprint
        FROM applications
        WHERE scraper_key = ?
        """,
        (job.id,),
    ).fetchone()

    now = _now_iso()
    status, stage_label = _normalize_status_and_stage(job)
    new_jd_fingerprint = _jd_fingerprint(job.description_excerpt, job.salary_text, job.location)
    
    if existing is None:
        # INSERT
        cur = conn.execute(
            """
            INSERT INTO applications (
                company, role, role_normalized, job_url, source, scraper_key,
                status, score, fit_band, matched_keywords, penalized_keywords,
                decision_reason, description_excerpt, location, is_remote,
                jd_fingerprint,
                salary_text, salary_low, salary_high, work_type, compensation_unit,
                hourly_rate, hours_per_week, weeks_per_year, normalized_compensation_usd,
                date_discovered, date_applied,
                user_priority, tier, notes, created_at, updated_at
            ) VALUES (
                :company, :role, :role_normalized, :url, :source, :id,
                :status, :score, :fit_band, :matched_keywords, :penalized_keywords,
                :decision_reason, :description_excerpt, :location, :is_remote, :jd_fingerprint,
                :salary_text, :salary_min, :salary_max, :work_type, :compensation_unit,
                :hourly_rate, :hours_per_week, :weeks_per_year, :normalized_compensation_usd,
                :date_discovered, :date_applied,
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
                "jd_fingerprint": new_jd_fingerprint,
                "salary_text": job.salary_text,
                "salary_min": job.salary_min,
                "salary_max": job.salary_max,
                "work_type": job.work_type,
                "compensation_unit": job.compensation_unit,
                "hourly_rate": job.hourly_rate,
                "hours_per_week": job.hours_per_week,
                "weeks_per_year": job.weeks_per_year,
                "normalized_compensation_usd": job.normalized_compensation_usd,
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
        _record_job_observation(conn, app_id, job, now, new_jd_fingerprint)
        _insert_stage_history(conn, app_id, None, stage_label, "Scraper discovery")
        return True, app_id
    else:
        # UPDATE scraper-owned fields
        app_id = existing["id"]
        old_excerpt = str(existing["description_excerpt"] or "")
        old_salary = str(existing["salary_text"] or "")
        old_location = str(existing["location"] or "")
        old_fingerprint = str(existing["jd_fingerprint"] or "")
        tracked_status = str(existing["status"] or "").lower()
        active_tracked_statuses = {"applied", "screening", "interviewing", "offer"}
        material_jd_change = (
            tracked_status in active_tracked_statuses
            and bool(old_fingerprint)
            and old_fingerprint != new_jd_fingerprint
            and _is_material_jd_change(old_excerpt, job.description_excerpt, old_salary, job.salary_text, old_location, job.location)
        )
        jd_change_summary = (
            _jd_change_summary(old_excerpt, job.description_excerpt, old_salary, job.salary_text, old_location, job.location)
            if material_jd_change
            else None
        )
        conn.execute(
            """
            UPDATE applications SET
                score = :score,
                fit_band = :fit_band,
                matched_keywords = :matched_keywords,
                penalized_keywords = :penalized_keywords,
                decision_reason = :decision_reason,
                description_excerpt = :description_excerpt,
                jd_fingerprint = :jd_fingerprint,
                jd_last_changed_at = CASE WHEN :jd_needs_review = 1 THEN :updated_at ELSE jd_last_changed_at END,
                jd_change_summary = CASE WHEN :jd_needs_review = 1 THEN :jd_change_summary ELSE jd_change_summary END,
                jd_needs_review = CASE WHEN :jd_needs_review = 1 THEN 1 ELSE jd_needs_review END,
                salary_text = :salary_text,
                salary_low = :salary_min,
                salary_high = :salary_max,
                work_type = :work_type,
                compensation_unit = :compensation_unit,
                hourly_rate = :hourly_rate,
                hours_per_week = :hours_per_week,
                weeks_per_year = :weeks_per_year,
                normalized_compensation_usd = :normalized_compensation_usd,
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
                "jd_fingerprint": new_jd_fingerprint,
                "jd_change_summary": jd_change_summary,
                "jd_needs_review": 1 if material_jd_change else 0,
                "salary_text": job.salary_text,
                "salary_min": job.salary_min,
                "salary_max": job.salary_max,
                "work_type": job.work_type,
                "compensation_unit": job.compensation_unit,
                "hourly_rate": job.hourly_rate,
                "hours_per_week": job.hours_per_week,
                "weeks_per_year": job.weeks_per_year,
                "normalized_compensation_usd": job.normalized_compensation_usd,
                "updated_at": now,
            }
        )
        _record_job_observation(conn, app_id, job, now, new_jd_fingerprint)
        if material_jd_change:
            _record_jd_change(conn, app_id, jd_change_summary or "job description changed", now)
        return False, app_id
