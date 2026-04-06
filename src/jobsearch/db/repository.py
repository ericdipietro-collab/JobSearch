"""Repository layer for smart upserts with content-hash sync."""

from __future__ import annotations

import hashlib
import sqlite3
from typing import Any, Dict, Optional

# Field ownership sets for content-hash sync
SCRAPER_OWNED = {
    "company",
    "role",
    "location",
    "job_url",
    "source",
    "source_lane",
    "canonical_job_url",
    "score",
    "fit_band",
    "matched_keywords",
    "penalized_keywords",
    "decision_reason",
    "description_excerpt",
    "enriched_data",
    "is_remote",
    "salary_text",
    "salary_low",
    "salary_high",
    "work_type",
    "compensation_unit",
    "hourly_rate",
    "hours_per_week",
    "weeks_per_year",
    "normalized_compensation_usd",
    "jd_summary",
    "job_description",
    "content_hash",
}

USER_OWNED = {
    "status",
    "notes",
    "user_priority",
    "fit_stars",
    "date_applied",
    "date_closed",
    "follow_up_date",
    "follow_up_notes",
    "resume_version",
    "cover_letter_notes",
    "resume_url",
    "cover_letter_url",
    "referral",
    "prep_company",
    "prep_why",
    "prep_tyabt",
    "prep_questions",
    "prep_notes",
    "offer_base",
    "offer_bonus_pct",
    "offer_equity",
    "offer_signing",
    "offer_pto_days",
    "offer_k401_match",
    "offer_remote_policy",
    "offer_start_date",
    "offer_expiry_date",
    "offer_notes",
}


def compute_content_hash(title: str, url: str, description_excerpt: Optional[str] = None) -> str:
    """Compute hash of job content for change detection."""
    content = f"{title}||{url}||{description_excerpt or ''}"
    return hashlib.md5(content.encode()).hexdigest()


def upsert_application_with_sync(
    conn: sqlite3.Connection,
    scraper_key: str,
    job_data: Dict[str, Any],
) -> int:
    """
    Upsert job with content-hash sync to preserve user annotations.

    If job content hasn't changed (same hash), skips update entirely.
    If job content changed, updates only SCRAPER_OWNED fields, preserving USER_OWNED fields.

    Returns: application id
    """
    cur = conn.cursor()

    # Compute content hash for change detection
    title = job_data.get("role", "")
    url = job_data.get("job_url", "")
    description = job_data.get("description_excerpt", "")
    new_hash = compute_content_hash(title, url, description)
    job_data["content_hash"] = new_hash

    # Check if this job exists
    cur.execute("SELECT id, content_hash FROM applications WHERE scraper_key = ?", (scraper_key,))
    existing = cur.fetchone()

    if existing:
        existing_id, existing_hash = existing

        # If hash unchanged, skip update entirely
        if existing_hash == new_hash:
            return existing_id

        # Hash changed: update only scraper-owned fields, preserve user-owned
        update_fields = []
        update_values = []
        for field, value in job_data.items():
            if field in SCRAPER_OWNED and field != "scraper_key":
                update_fields.append(f"{field} = ?")
                update_values.append(value)

        if update_fields:
            update_values.append(existing_id)
            query = f"UPDATE applications SET {', '.join(update_fields)} WHERE id = ?"
            cur.execute(query, update_values)

        conn.commit()
        return existing_id

    # New job: insert all fields
    columns = list(job_data.keys()) + ["created_at", "updated_at"]
    placeholders = ["?" for _ in columns]

    # Add timestamps
    import datetime
    now = datetime.datetime.now(datetime.timezone.utc).isoformat()
    values = [job_data.get(col) for col in columns[:-2]] + [now, now]

    query = f"INSERT INTO applications ({', '.join(columns)}) VALUES ({', '.join(placeholders)})"
    cur.execute(query, values)
    conn.commit()

    return cur.lastrowid
