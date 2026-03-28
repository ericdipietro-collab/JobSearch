"""services/pipeline_service.py — Stage definitions and transition logic."""

from __future__ import annotations

import sqlite3
from datetime import datetime, timezone
from typing import Dict, List, Optional, Set

from db.models import StageHistory

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


# ── Helpers ───────────────────────────────────────────────────────────────────

def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


# ── Public API ────────────────────────────────────────────────────────────────

def can_transition(from_stage: str, to_stage: str) -> bool:
    """Return True if moving from_stage → to_stage is allowed by VALID_TRANSITIONS."""
    return to_stage in VALID_TRANSITIONS.get(from_stage, [])


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
    row = conn.execute(
        "SELECT current_stage FROM opportunities WHERE id = ?", (opp_id,)
    ).fetchone()

    if row is None:
        return False

    from_stage: str = row["current_stage"]

    if not can_transition(from_stage, to_stage):
        return False

    now = _now_iso()
    conn.execute(
        "UPDATE opportunities SET current_stage = ?, last_updated = ? WHERE id = ?",
        (to_stage, now, opp_id),
    )
    conn.execute(
        """
        INSERT INTO stage_history (opportunity_id, from_stage, to_stage, timestamp, note)
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
        SELECT id, opportunity_id, from_stage, to_stage, timestamp, note
        FROM stage_history
        WHERE opportunity_id = ?
        ORDER BY timestamp ASC
        """,
        (opp_id,),
    ).fetchall()

    return [
        StageHistory(
            id=r["id"],
            opportunity_id=r["opportunity_id"],
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
