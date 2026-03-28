"""services/opportunity_service.py — CRUD and sync operations for Opportunity records."""

from __future__ import annotations

import json
import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from typing import List, Optional

import pandas as pd

from db.models import Opportunity, StageHistory


# ── Helpers ───────────────────────────────────────────────────────────────────

def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _safe_str(val) -> str:
    """Convert a value that may be NaN/None/float to a clean string."""
    if val is None:
        return ""
    try:
        if pd.isna(val):
            return ""
    except (TypeError, ValueError):
        pass
    return str(val).strip()


def _safe_float(val) -> Optional[float]:
    """Return a float or None for missing/invalid values."""
    try:
        if pd.isna(val):
            return None
    except (TypeError, ValueError):
        pass
    try:
        return float(val)
    except (TypeError, ValueError):
        return None


def _safe_bool(val) -> bool:
    if isinstance(val, bool):
        return val
    s = str(val).strip().lower()
    return s in ("true", "1", "yes")


def _row_to_opportunity(row: sqlite3.Row) -> Opportunity:
    d = dict(row)
    return Opportunity(
        id=d["id"],
        company=d["company"],
        role_title_raw=d["role_title_raw"],
        role_title_normalized=d["role_title_normalized"],
        location=d["location"],
        is_remote=bool(d["is_remote"]),
        source=d["source"],
        url=d["url"],
        salary_min=d["salary_min"],
        salary_max=d["salary_max"],
        salary_text=d["salary_text"],
        score=float(d["score"] or 0.0),
        fit_band=d["fit_band"],
        current_stage=d["current_stage"],
        date_discovered=d["date_discovered"],
        last_updated=d["last_updated"],
        date_applied=d["date_applied"],
        adapter=d["adapter"],
        tier=d["tier"],
        matched_keywords=d["matched_keywords"],
        penalized_keywords=d["penalized_keywords"],
        decision_reason=d["decision_reason"],
        description_excerpt=d["description_excerpt"],
        user_priority=int(d["user_priority"] or 0),
        notes=d["notes"],
    )


def _insert_stage_history(
    conn: sqlite3.Connection,
    opp_id: str,
    from_stage: Optional[str],
    to_stage: str,
    note: str = "",
) -> None:
    conn.execute(
        """
        INSERT INTO stage_history (opportunity_id, from_stage, to_stage, timestamp, note)
        VALUES (?, ?, ?, ?, ?)
        """,
        (opp_id, from_stage, to_stage, _now_iso(), note),
    )


# ── Upsert ────────────────────────────────────────────────────────────────────

def upsert_opportunity(conn: sqlite3.Connection, opp: Opportunity) -> bool:
    """
    Insert or update an Opportunity.

    Returns True if a new row was inserted, False if an existing row was updated.

    On INSERT: all fields written; stage_history seeded with from_stage=None.
    On UPDATE: only scraper-owned fields are refreshed. User-controlled fields
               (current_stage — unless still "New", notes, user_priority,
               date_applied) are preserved.
    """
    existing = conn.execute(
        "SELECT current_stage FROM opportunities WHERE id = ?", (opp.id,)
    ).fetchone()

    now = _now_iso()

    if existing is None:
        # ── INSERT ────────────────────────────────────────────────────────────
        conn.execute(
            """
            INSERT INTO opportunities (
                id, company, role_title_raw, role_title_normalized,
                location, is_remote, source, url,
                salary_min, salary_max, salary_text,
                score, fit_band, current_stage,
                date_discovered, last_updated, date_applied,
                adapter, tier, matched_keywords, penalized_keywords,
                decision_reason, description_excerpt,
                user_priority, notes
            ) VALUES (
                :id, :company, :role_title_raw, :role_title_normalized,
                :location, :is_remote, :source, :url,
                :salary_min, :salary_max, :salary_text,
                :score, :fit_band, :current_stage,
                :date_discovered, :last_updated, :date_applied,
                :adapter, :tier, :matched_keywords, :penalized_keywords,
                :decision_reason, :description_excerpt,
                :user_priority, :notes
            )
            """,
            {
                "id": opp.id,
                "company": opp.company,
                "role_title_raw": opp.role_title_raw,
                "role_title_normalized": opp.role_title_normalized,
                "location": opp.location,
                "is_remote": int(opp.is_remote),
                "source": opp.source,
                "url": opp.url,
                "salary_min": opp.salary_min,
                "salary_max": opp.salary_max,
                "salary_text": opp.salary_text,
                "score": opp.score,
                "fit_band": opp.fit_band,
                "current_stage": opp.current_stage,
                "date_discovered": opp.date_discovered or now,
                "last_updated": now,
                "date_applied": opp.date_applied,
                "adapter": opp.adapter,
                "tier": opp.tier,
                "matched_keywords": opp.matched_keywords,
                "penalized_keywords": opp.penalized_keywords,
                "decision_reason": opp.decision_reason,
                "description_excerpt": opp.description_excerpt,
                "user_priority": opp.user_priority,
                "notes": opp.notes,
            },
        )
        _insert_stage_history(conn, opp.id, None, opp.current_stage, "Created by sync")
        return True

    # ── UPDATE (scraper-owned fields only) ────────────────────────────────────
    prev_stage = existing["current_stage"]

    # Only advance stage if it hasn't been touched by the user yet
    new_stage_sql = (
        "CASE WHEN current_stage = 'New' THEN :new_stage ELSE current_stage END"
    )

    conn.execute(
        f"""
        UPDATE opportunities SET
            score               = :score,
            salary_min          = :salary_min,
            salary_max          = :salary_max,
            salary_text         = :salary_text,
            decision_reason     = :decision_reason,
            matched_keywords    = :matched_keywords,
            penalized_keywords  = :penalized_keywords,
            fit_band            = :fit_band,
            description_excerpt = :description_excerpt,
            last_updated        = :last_updated,
            current_stage       = {new_stage_sql}
        WHERE id = :id
        """,
        {
            "id": opp.id,
            "score": opp.score,
            "salary_min": opp.salary_min,
            "salary_max": opp.salary_max,
            "salary_text": opp.salary_text,
            "decision_reason": opp.decision_reason,
            "matched_keywords": opp.matched_keywords,
            "penalized_keywords": opp.penalized_keywords,
            "fit_band": opp.fit_band,
            "description_excerpt": opp.description_excerpt,
            "last_updated": now,
            "new_stage": opp.current_stage,
        },
    )

    # Log stage transition if stage actually changed
    if prev_stage == "New" and opp.current_stage != "New":
        _insert_stage_history(conn, opp.id, prev_stage, opp.current_stage, "Scraper re-score")

    return False


# ── Read operations ───────────────────────────────────────────────────────────

def get_opportunity(conn: sqlite3.Connection, opp_id: str) -> Optional[Opportunity]:
    row = conn.execute(
        "SELECT * FROM opportunities WHERE id = ?", (opp_id,)
    ).fetchone()
    return _row_to_opportunity(row) if row else None


def list_opportunities(
    conn: sqlite3.Connection,
    stage: Optional[str] = None,
    min_score: float = 0,
) -> List[Opportunity]:
    if stage:
        rows = conn.execute(
            "SELECT * FROM opportunities WHERE current_stage = ? AND score >= ? ORDER BY score DESC",
            (stage, min_score),
        ).fetchall()
    else:
        rows = conn.execute(
            "SELECT * FROM opportunities WHERE score >= ? ORDER BY score DESC",
            (min_score,),
        ).fetchall()
    return [_row_to_opportunity(r) for r in rows]


# ── Excel / JSON sync ─────────────────────────────────────────────────────────

# Map scraper action_bucket values to initial pipeline stages
_BUCKET_TO_STAGE: dict[str, str] = {
    "APPLY NOW":     "Shortlisted",
    "REVIEW TODAY":  "Shortlisted",
    "WATCH":         "Scored",
    "MANUAL REVIEW": "Scored",
    "IGNORE":        "Archived",
}

# Map job_status.json user_status values to pipeline stages
_STATUS_TO_STAGE: dict[str, str] = {
    "Applied":       "Applied",
    "Rejected":      "Rejected",
    "APPLY NOW":     "Shortlisted",
    "REVIEW TODAY":  "Shortlisted",
    "WATCH":         "Scored",
    "MANUAL REVIEW": "Scored",
}


def sync_from_excel(
    conn: sqlite3.Connection,
    xlsx_path: Path,
    status_json_path: Path,
) -> dict:
    """
    Read 'All Jobs' sheet from the scraper Excel, upsert each row into the
    opportunities table, then apply job_status.json overrides.

    Returns {"inserted": N, "updated": N, "skipped": N}.
    """
    summary = {"inserted": 0, "updated": 0, "skipped": 0}

    if not xlsx_path.exists():
        return summary

    # Load status overrides
    status_overrides: dict = {}
    if status_json_path.exists():
        try:
            status_overrides = json.loads(status_json_path.read_text(encoding="utf-8"))
        except Exception:
            pass

    try:
        df = pd.read_excel(xlsx_path, sheet_name="All Jobs", dtype=str)
    except Exception:
        return summary

    today_iso = datetime.now(timezone.utc).date().isoformat()

    for _, row in df.iterrows():
        company = _safe_str(row.get("company"))
        title_raw = _safe_str(row.get("title"))
        title_norm = _safe_str(row.get("normalized_title")) or title_raw
        url = _safe_str(row.get("url"))

        if not company and not title_raw:
            summary["skipped"] += 1
            continue

        opp_id = Opportunity.make_id(company, title_norm, url)

        bucket = _safe_str(row.get("action_bucket")).upper()
        initial_stage = _BUCKET_TO_STAGE.get(bucket, "Scored")

        date_discovered = _safe_str(row.get("seen_first_at")) or today_iso

        salary_low = _safe_float(row.get("salary_low"))
        salary_high = _safe_float(row.get("salary_high"))
        salary_text = _safe_str(row.get("salary_range"))

        opp = Opportunity(
            id=opp_id,
            company=company,
            role_title_raw=title_raw,
            role_title_normalized=title_norm,
            location=_safe_str(row.get("location")),
            is_remote=_safe_bool(row.get("is_remote")),
            source=_safe_str(row.get("source")),
            url=url,
            salary_min=salary_low,
            salary_max=salary_high,
            salary_text=salary_text,
            score=_safe_float(row.get("score")) or 0.0,
            fit_band=_safe_str(row.get("fit_band")),
            current_stage=initial_stage,
            date_discovered=date_discovered,
            last_updated=_now_iso(),
            date_applied=_safe_str(row.get("applied_date")) or None,
            adapter=_safe_str(row.get("source")),
            tier=_safe_str(row.get("tier")),
            matched_keywords=_safe_str(row.get("matched_keywords")),
            penalized_keywords=_safe_str(row.get("penalized_keywords")),
            decision_reason=_safe_str(row.get("decision_reason")),
            description_excerpt=_safe_str(row.get("description_excerpt")),
        )

        inserted = upsert_opportunity(conn, opp)
        if inserted:
            summary["inserted"] += 1
        else:
            summary["updated"] += 1

        # Apply job_status.json overrides
        # The key format used by app.py: "company||title||url"
        status_key = f"{company}||{title_raw}||{url}"
        override = status_overrides.get(status_key, {})
        user_status = _safe_str(override.get("user_status"))

        if user_status:
            new_stage = _STATUS_TO_STAGE.get(user_status, "")
            if new_stage:
                current = conn.execute(
                    "SELECT current_stage FROM opportunities WHERE id = ?", (opp_id,)
                ).fetchone()
                prev_stage = current["current_stage"] if current else ""

                if prev_stage != new_stage:
                    conn.execute(
                        "UPDATE opportunities SET current_stage = ?, last_updated = ? WHERE id = ?",
                        (new_stage, _now_iso(), opp_id),
                    )
                    _insert_stage_history(
                        conn, opp_id, prev_stage, new_stage, "Imported from job_status.json"
                    )

            # Also record date_applied when status is Applied
            if user_status == "Applied":
                applied_at = _safe_str(override.get("applied_at"))
                if applied_at:
                    conn.execute(
                        "UPDATE opportunities SET date_applied = ? WHERE id = ? AND date_applied IS NULL",
                        (applied_at, opp_id),
                    )

    conn.commit()
    return summary


# ── User-editable fields ──────────────────────────────────────────────────────

def update_notes(conn: sqlite3.Connection, opp_id: str, notes: str) -> None:
    conn.execute(
        "UPDATE opportunities SET notes = ?, last_updated = ? WHERE id = ?",
        (notes, _now_iso(), opp_id),
    )
    conn.commit()


def update_priority(conn: sqlite3.Connection, opp_id: str, priority: int) -> None:
    conn.execute(
        "UPDATE opportunities SET user_priority = ?, last_updated = ? WHERE id = ?",
        (priority, _now_iso(), opp_id),
    )
    conn.commit()
