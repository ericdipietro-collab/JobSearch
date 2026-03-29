"""services/importer.py — Import ApplicationTracker.csv into the opportunities DB."""

from __future__ import annotations

import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, Optional

import pandas as pd

from db.models import Activity, Opportunity
from services.opportunity_service import upsert_opportunity, _safe_str, _safe_float


# ── Helpers ───────────────────────────────────────────────────────────────────

def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


# Map tracker Stage values to pipeline stages
_TRACKER_STAGE_MAP: Dict[str, str] = {
    "Applied":           "Applied",
    "Interviewing":      "Recruiter Screen",
    "Phone Screen":      "Recruiter Screen",
    "Recruiter Screen":  "Recruiter Screen",
    "Hiring Manager":    "Hiring Manager",
    "Panel":             "Panel",
    "Final Round":       "Final Round",
    "Offer":             "Offer",
    "Rejected":          "Rejected",
    "Archived":          "Archived",
}

# Map star ratings or text fit values to a numeric score hint
_FIT_SCORE_HINT: Dict[str, float] = {
    "high":  80.0,
    "med":   55.0,
    "medium":55.0,
    "low":   30.0,
    # Star emoji variants the tracker uses
    "⭐⭐⭐⭐⭐": 90.0,
    "⭐⭐⭐⭐":  75.0,
    "⭐⭐⭐":   55.0,
    "⭐⭐":    35.0,
    "⭐":     20.0,
}


def _fit_to_score(fit_val: str) -> float:
    """Convert a Fit cell value into an approximate score."""
    cleaned = fit_val.strip()
    # Try exact match first (handles star strings)
    if cleaned in _FIT_SCORE_HINT:
        return _FIT_SCORE_HINT[cleaned]
    # Try lowercase text match
    lower = cleaned.lower()
    return _FIT_SCORE_HINT.get(lower, 50.0)


def _fit_to_band(fit_val: str) -> str:
    score = _fit_to_score(fit_val)
    if score >= 75:
        return "Strong"
    if score >= 50:
        return "Moderate"
    return "Weak"


def _parse_date(val: str) -> Optional[str]:
    """Try to parse a loose date string into ISO format. Returns None on failure."""
    val = val.strip()
    if not val or val in ("-", "—", "N/A", "n/a"):
        return None
    for fmt in ("%b %d", "%B %d", "%m/%d/%Y", "%m/%d/%y", "%Y-%m-%d"):
        try:
            dt = datetime.strptime(val, fmt)
            # strptime with no year defaults to 1900 — use current year
            if dt.year == 1900:
                dt = dt.replace(year=datetime.now().year)
            return dt.date().isoformat()
        except ValueError:
            continue
    return None


def _fuzzy_match_opportunity(
    conn: sqlite3.Connection, company: str, title: str
) -> Optional[str]:
    """
    Look for an existing opportunity with an exact company match and a title
    that contains key words from the supplied title string.

    Returns the opportunity id if found, otherwise None.
    """
    # Try exact company + partial title (first 20 chars)
    title_fragment = title[:20].strip() if title else ""
    row = conn.execute(
        """
        SELECT id FROM opportunities
        WHERE company = ? AND role_title_raw LIKE ?
        LIMIT 1
        """,
        (company, f"%{title_fragment}%"),
    ).fetchone()
    return row["id"] if row else None


# ── Main import function ──────────────────────────────────────────────────────

def import_tracker_csv(conn: sqlite3.Connection, csv_path: Path) -> dict:
    """
    Read ApplicationTracker.csv and upsert each row as an Opportunity.

    Column mapping:
        Company         → company
        Role            → role_title_raw
        Applied Date    → date_applied
        Stage           → current_stage (via _TRACKER_STAGE_MAP)
        Notes           → notes
        Interview Date  → Activity (type=screen) if present
        Contact         → notes annotation about interviewer
        Salary Range    → salary_text
        Fit             → fit_band + score hint

    Returns {"inserted": N, "updated": N, "skipped": N, "errors": N}.
    """
    summary = {"inserted": 0, "updated": 0, "skipped": 0, "errors": 0}

    if not csv_path.exists():
        return summary

    try:
        df = pd.read_csv(csv_path, dtype=str)
    except Exception:
        summary["errors"] += 1
        return summary

    today_iso = datetime.now(timezone.utc).date().isoformat()

    for _, row in df.iterrows():
        company = _safe_str(row.get("Company"))
        role_raw = _safe_str(row.get("Role"))

        if not company or not role_raw:
            summary["skipped"] += 1
            continue

        try:
            fit_raw = _safe_str(row.get("Fit"))
            score_hint = _fit_to_score(fit_raw) if fit_raw else 50.0
            fit_band = _fit_to_band(fit_raw) if fit_raw else "Moderate"

            stage_raw = _safe_str(row.get("Stage"))
            current_stage = _TRACKER_STAGE_MAP.get(stage_raw, "Applied")

            applied_date_raw = _safe_str(row.get("Applied Date"))
            date_applied = _parse_date(applied_date_raw) if applied_date_raw else None

            salary_text = _safe_str(row.get("Salary Range"))

            notes_parts = []
            notes_val = _safe_str(row.get("Notes"))
            if notes_val:
                notes_parts.append(notes_val)

            contact = _safe_str(row.get("Contact"))
            if contact and contact not in ("-", "—"):
                notes_parts.append(f"Contact: {contact}")

            referral = _safe_str(row.get("Referral"))
            if referral and referral not in ("-", "—"):
                notes_parts.append(f"Referral: {referral}")

            jd_summary = _safe_str(row.get("JD Summary / Key Requirements"))

            notes_combined = "\n".join(notes_parts)

            # Use empty URL since tracker doesn't have URLs
            opp_id = Opportunity.make_id(company, role_raw, "")

            # Try fuzzy match against existing records with a URL
            existing_id = _fuzzy_match_opportunity(conn, company, role_raw)
            if existing_id:
                opp_id = existing_id

            opp = Opportunity(
                id=opp_id,
                company=company,
                role_title_raw=role_raw,
                role_title_normalized=role_raw,
                location="",
                is_remote=False,
                source="ApplicationTracker.csv",
                url="",
                salary_min=None,
                salary_max=None,
                salary_text=salary_text,
                score=score_hint,
                fit_band=fit_band,
                current_stage=current_stage,
                date_discovered=date_applied or today_iso,
                last_updated=_now_iso(),
                date_applied=date_applied,
                adapter="tracker_csv",
                tier="",
                matched_keywords="",
                penalized_keywords="",
                decision_reason=f"Imported from ApplicationTracker.csv",
                description_excerpt=jd_summary,
                notes=notes_combined,
            )

            inserted = upsert_opportunity(conn, opp)
            if inserted:
                summary["inserted"] += 1
            else:
                summary["updated"] += 1

            # Log an interview activity if Interview Date is present
            interview_date_raw = _safe_str(row.get("Interview Date"))
            interview_date = _parse_date(interview_date_raw) if interview_date_raw else None
            if interview_date:
                conn.execute(
                    """
                    INSERT INTO activities
                        (opportunity_id, activity_type, scheduled_date, interviewer, notes)
                    VALUES (?, ?, ?, ?, ?)
                    """,
                    (
                        opp_id,
                        "screen",
                        interview_date,
                        contact if contact not in ("-", "—", "") else "",
                        f"Imported from ApplicationTracker.csv (Stage: {stage_raw})",
                    ),
                )

        except Exception as exc:
            summary["errors"] += 1
            continue

    conn.commit()
    return summary
