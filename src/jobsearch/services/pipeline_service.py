"""services/pipeline_service.py — Stage definitions and transition logic."""

from __future__ import annotations

import sqlite3
from datetime import datetime, timezone
from typing import Dict, List, Optional, Set

from jobsearch.ats_db import StageHistory

# ── Stage registry ────────────────────────────────────────────────────────────

STAGES: List[str] = [
    "New",
    "Scored",
    "Shortlisted",
    "Applied",
    "Recruiter Screen",
    "Hiring Manager",
    "Panel",
    "Final Round",
    "Offer",
    "Rejected",
    "Archived",
]

VALID_TRANSITIONS: Dict[str, List[str]] = {
    "New":             ["Scored", "Shortlisted", "Rejected", "Archived"],
    "Scored":          ["Shortlisted", "Applied", "Rejected", "Archived"],
    "Shortlisted":     ["Applied", "Scored", "Rejected", "Archived"],
    "Applied":         ["Recruiter Screen", "Hiring Manager", "Rejected", "Archived"],
    "Recruiter Screen":["Hiring Manager", "Panel", "Rejected", "Archived"],
    "Hiring Manager":  ["Panel", "Final Round", "Rejected", "Archived"],
    "Panel":           ["Final Round", "Rejected", "Archived"],
    "Final Round":     ["Offer", "Rejected", "Archived"],
    "Offer":           ["Archived"],
    "Rejected":        ["Archived"],
    "Archived":        [],
}

# Stages that represent an active candidacy (not terminal)
ACTIVE_STAGES: List[str] = [s for s in STAGES if s not in ("Rejected", "Archived")]

# Stages where no further progression is expected
TERMINAL_STAGES: Set[str] = {"Offer", "Rejected", "Archived"}

STATUS_TO_STAGE: Dict[str, str] = {
    "exploring": "New",
    "considering": "Scored",
    "applied": "Applied",
    "screening": "Recruiter Screen",
    "interviewing": "Hiring Manager",
    "offer": "Offer",
    "accepted": "Offer",
    "rejected": "Rejected",
    "withdrawn": "Archived",
}

STAGE_TO_STATUS: Dict[str, str] = {
    "New": "exploring",
    "Scored": "considering",
    "Shortlisted": "considering",
    "Applied": "applied",
    "Recruiter Screen": "screening",
    "Hiring Manager": "interviewing",
    "Panel": "interviewing",
    "Final Round": "interviewing",
    "Offer": "offer",
    "Rejected": "rejected",
    "Archived": "withdrawn",
}


# ── Helpers ───────────────────────────────────────────────────────────────────

def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


# ── Public API ────────────────────────────────────────────────────────────────

def can_transition(from_stage: str, to_stage: str) -> bool:
    """Return True if moving from_stage → to_stage is allowed by VALID_TRANSITIONS."""
    return to_stage in VALID_TRANSITIONS.get(from_stage, [])


def _current_stage_for_application(
    conn: sqlite3.Connection,
    app_id: str,
) -> Optional[str]:
    row = conn.execute(
        """
        SELECT a.status, sh.to_stage
        FROM applications a
        LEFT JOIN stage_history sh
          ON sh.id = (
              SELECT id
              FROM stage_history
              WHERE application_id = a.id
              ORDER BY timestamp DESC, id DESC
              LIMIT 1
          )
        WHERE a.id = ?
        """,
        (app_id,),
    ).fetchone()
    if row is None:
        return None
    return row["to_stage"] or STATUS_TO_STAGE.get(row["status"], "Scored")


def transition_stage(
    conn: sqlite3.Connection,
    opp_id: str,
    to_stage: str,
    note: str = "",
) -> bool:
    """
    Validate and execute a stage transition for a single opportunity.

    Writes the new current_stage to the opportunities table and appends a row
    to stage_history.

    Returns False (without writing) if:
      - The opportunity does not exist.
      - The transition is not in VALID_TRANSITIONS.
    """
    from_stage = _current_stage_for_application(conn, opp_id)
    if from_stage is None:
        return False

    if not can_transition(from_stage, to_stage):
        return False

    now = _now_iso()
    conn.execute(
        "UPDATE applications SET status = ?, updated_at = ? WHERE id = ?",
        (STAGE_TO_STATUS[to_stage], now, opp_id),
    )
    conn.execute(
        """
        INSERT INTO stage_history (application_id, from_stage, to_stage, timestamp, note)
        VALUES (?, ?, ?, ?, ?)
        """,
        (opp_id, from_stage, to_stage, now, note),
    )
    conn.commit()
    return True


def get_stage_history(
    conn: sqlite3.Connection, opp_id: str
) -> List[StageHistory]:
    """Return the full stage history for an opportunity, oldest first."""
    rows = conn.execute(
        """
        SELECT id, application_id, from_stage, to_stage, timestamp, note
        FROM stage_history
        WHERE application_id = ?
        ORDER BY timestamp ASC
        """,
        (opp_id,),
    ).fetchall()

    return [
        StageHistory(
            id=r["id"],
            opportunity_id=str(r["application_id"]),
            from_stage=r["from_stage"],
            to_stage=r["to_stage"],
            timestamp=r["timestamp"],
            note=r["note"],
        )
        for r in rows
    ]


def bulk_transition(
    conn: sqlite3.Connection,
    opp_ids: List[str],
    to_stage: str,
    note: str = "",
) -> int:
    """
    Attempt to transition all listed opportunities to to_stage.

    Skips any that fail validation (wrong current stage, invalid target).
    Returns the count of successful transitions.
    """
    count = 0
    for opp_id in opp_ids:
        if transition_stage(conn, opp_id, to_stage, note=note):
            count += 1
    return count
