"""Service for managing Job/Application records in the unified database."""

from __future__ import annotations
import sqlite3
import hashlib
import json
from datetime import datetime, timezone
from difflib import SequenceMatcher
from typing import List, Optional, Dict, Any
import pandas as pd
from jobsearch.scraper.models import Job
from jobsearch import ats_db
from jobsearch.services.job_canonicalization_service import JobCanonicalizationService, CanonicalDecision
from jobsearch.scraper.normalization import WorkTypeNormalizer

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
    Insert or update a Job from the scraper with content-hash sync and inline canonicalization.
    """
    from jobsearch.db.repository import compute_content_hash

    # Compute content hash for change detection
    new_hash = compute_content_hash(job.role_title_raw, job.url, job.description_excerpt)

    # 1. Run Canonicalization logic upstream
    canon_service = JobCanonicalizationService(conn)
    decision = canon_service.canonicalize_incremental(job)

    def _row(r):
        return dict(r) if r is not None else None

    # Check if this exact source record already exists
    existing = _row(conn.execute(
        "SELECT * FROM applications WHERE scraper_key = ?", (job.id,)
    ).fetchone())

    now = _now_iso()
    status, stage_label = _normalize_status_and_stage(job)
    new_jd_fingerprint = _jd_fingerprint(job.description_excerpt, job.salary_text, job.location)

    # Handle Superseding: if this new job is better than current canonical, demote existing
    if decision.is_canonical == 1 and decision.superseded_id:
        conn.execute(
            "UPDATE applications SET is_canonical = 0, canonical_merge_rationale = 'Superseded by higher-trust source' WHERE id = ?",
            (decision.superseded_id,)
        )

    if existing is None:
        # INSERT
        cur = conn.execute(
            """
            INSERT INTO applications (
                company, role, role_normalized, job_url, source, source_lane, canonical_job_url, scraper_key,
                status, score, fit_band, apply_now_eligible, matched_keywords, penalized_keywords,
                decision_reason, description_excerpt, location, is_remote,
                jd_fingerprint, content_hash,
                salary_text, salary_low, salary_high, work_type, compensation_unit,
                hourly_rate, hours_per_week, weeks_per_year, normalized_compensation_usd,
                date_discovered, date_applied,
                extraction_method, extraction_confidence,
                canonical_group_id, is_canonical, canonical_merge_rationale,
                req_id, notes, created_at, updated_at
            ) VALUES (
                :company, :role, :role_normalized, :url, :source, :source_lane, :canonical_job_url, :id,
                :status, :score, :fit_band, :apply_now_eligible, :matched_keywords, :penalized_keywords,
                :decision_reason, :description_excerpt, :location, :is_remote, :jd_fingerprint, :content_hash,
                :salary_text, :salary_min, :salary_max, :work_type, :compensation_unit,
                :hourly_rate, :hours_per_week, :weeks_per_year, :normalized_compensation_usd,
                :date_discovered, :date_applied,
                :extraction_method, :extraction_confidence,
                :canon_gid, :is_canonical, :merge_rationale,
                :req_id, :notes, :created_at, :updated_at
            )
            """,
            {
                "company": job.company,
                "role": job.role_title_raw,
                "role_normalized": job.role_title_normalized,
                "url": job.url,
                "source": job.source,
                "source_lane": str(job.source_lane or "employer_ats"),
                "canonical_job_url": str(job.canonical_job_url or ""),
                "id": job.id,
                "status": status,
                "score": job.score,
                "fit_band": job.fit_band,
                "apply_now_eligible": int(getattr(job, "apply_now_eligible", True)),
                "matched_keywords": job.matched_keywords,
                "penalized_keywords": job.penalized_keywords,
                "decision_reason": job.decision_reason,
                "description_excerpt": job.description_excerpt,
                "location": job.location,
                "is_remote": int(job.is_remote),
                "jd_fingerprint": new_jd_fingerprint,
                "content_hash": new_hash,
                "salary_text": job.salary_text,
                "salary_min": job.salary_min,
                "salary_max": job.salary_max,
                "work_type": WorkTypeNormalizer.normalize(job.work_type),
                "compensation_unit": job.compensation_unit,
                "hourly_rate": job.hourly_rate,
                "hours_per_week": job.hours_per_week,
                "weeks_per_year": job.weeks_per_year,
                "normalized_compensation_usd": job.normalized_compensation_usd,
                "date_discovered": job.date_discovered or now,
                "date_applied": job.date_applied,
                "extraction_method": getattr(job, "extraction_method", ""),
                "extraction_confidence": getattr(job, "extraction_confidence", 0.0),
                "canon_gid": decision.canonical_group_id,
                "is_canonical": decision.is_canonical,
                "merge_rationale": decision.rationale,
                "req_id": getattr(job, "req_id", None),
                "notes": (job.notes or "") + (f"\nConflicts: {json.dumps(decision.conflicts)}" if decision.conflicts else ""),
                "created_at": now,
                "updated_at": now,
            }
        )
        app_id = cur.lastrowid
        _record_job_observation(conn, app_id, job, now, new_jd_fingerprint)
        _insert_stage_history(conn, app_id, None, stage_label, "Scraper discovery")
        return True, app_id
    else:
        # UPDATE
        app_id = existing["id"]
        old_hash = existing.get("content_hash")

        if old_hash == new_hash:
            _record_job_observation(conn, app_id, job, now, old_hash)
            return False, app_id

        if not str(job.description_excerpt or "").strip() and existing.get("description_excerpt"):
            _record_job_observation(conn, app_id, job, now, old_hash)
            return False, app_id

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
        
        merged_salary_text = job.salary_text or old_salary
        merged_salary_min = job.salary_min if job.salary_min is not None else existing["salary_low"]
        merged_salary_max = job.salary_max if job.salary_max is not None else existing["salary_high"]
        merged_work_type = WorkTypeNormalizer.normalize(job.work_type or existing["work_type"])
        
        conn.execute(
            """
            UPDATE applications SET
                score = :score, fit_band = :fit_band, apply_now_eligible = :apply_now_eligible,
                matched_keywords = :matched_keywords, penalized_keywords = :penalized_keywords,
                decision_reason = :decision_reason, description_excerpt = :description_excerpt,
                jd_fingerprint = :jd_fingerprint,
                jd_last_changed_at = CASE WHEN :jd_needs_review = 1 THEN :updated_at ELSE jd_last_changed_at END,
                jd_change_summary = CASE WHEN :jd_needs_review = 1 THEN :jd_change_summary ELSE jd_change_summary END,
                jd_needs_review = CASE WHEN :jd_needs_review = 1 THEN 1 ELSE jd_needs_review END,
                salary_text = :salary_text, salary_low = :salary_min, salary_high = :salary_max,
                work_type = :work_type,
                extraction_method = :extraction_method, extraction_confidence = :extraction_confidence,
                canonical_group_id = :canon_gid, is_canonical = :is_canonical, 
                canonical_merge_rationale = :merge_rationale,
                updated_at = :updated_at
            WHERE id = :app_id
            """,
            {
                "app_id": app_id,
                "score": job.score,
                "fit_band": job.fit_band,
                "apply_now_eligible": int(getattr(job, "apply_now_eligible", True)),
                "matched_keywords": job.matched_keywords,
                "penalized_keywords": job.penalized_keywords,
                "decision_reason": job.decision_reason,
                "description_excerpt": job.description_excerpt,
                "jd_fingerprint": new_jd_fingerprint,
                "jd_change_summary": jd_change_summary,
                "jd_needs_review": 1 if material_jd_change else 0,
                "salary_text": merged_salary_text,
                "salary_min": merged_salary_min,
                "salary_max": merged_salary_max,
                "work_type": merged_work_type,
                "extraction_method": getattr(job, "extraction_method", ""),
                "extraction_confidence": getattr(job, "extraction_confidence", 0.0),
                "canon_gid": decision.canonical_group_id,
                "is_canonical": decision.is_canonical,
                "merge_rationale": decision.rationale,
                "updated_at": now,
            }
        )
        _record_job_observation(conn, app_id, job, now, new_jd_fingerprint)
        return False, app_id
