"""services/analytics_service.py — Aggregation queries for the Analytics page."""

from __future__ import annotations

import sqlite3
from datetime import datetime, timezone
from typing import Dict

import pandas as pd

from services.pipeline_service import STAGES

# ── Helpers ───────────────────────────────────────────────────────────────────

_INTERVIEWED_STAGES = (
    "Recruiter Screen",
    "Hiring Manager",
    "Panel",
    "Final Round",
    "Offer",
)


def _stage_order_map() -> Dict[str, int]:
    return {stage: idx for idx, stage in enumerate(STAGES)}


# ── Public analytics functions ────────────────────────────────────────────────

def funnel_counts(conn: sqlite3.Connection) -> pd.DataFrame:
    """
    Return a DataFrame with columns [stage, count] ordered by STAGES list.
    Stages with zero opportunities are still included for a complete funnel view.
    """
    rows = conn.execute(
        "SELECT current_stage AS stage, COUNT(*) AS count FROM opportunities GROUP BY current_stage"
    ).fetchall()

    counts: Dict[str, int] = {r["stage"]: r["count"] for r in rows}

    data = [{"stage": s, "count": counts.get(s, 0)} for s in STAGES]
    return pd.DataFrame(data)


def conversion_rates(conn: sqlite3.Connection) -> dict:
    """
    Return high-level funnel conversion metrics.

    Keys:
        total_discovered, total_applied, total_interviewed, total_offer,
        applied_rate, interview_rate, offer_rate
    """
    total_row = conn.execute("SELECT COUNT(*) AS n FROM opportunities").fetchone()
    total_discovered = total_row["n"] if total_row else 0

    applied_stages = (
        "Applied",
        "Recruiter Screen",
        "Hiring Manager",
        "Panel",
        "Final Round",
        "Offer",
    )
    placeholders = ",".join("?" * len(applied_stages))

    applied_row = conn.execute(
        f"SELECT COUNT(*) AS n FROM opportunities WHERE current_stage IN ({placeholders})",
        applied_stages,
    ).fetchone()
    total_applied = applied_row["n"] if applied_row else 0

    inter_placeholders = ",".join("?" * len(_INTERVIEWED_STAGES))
    inter_row = conn.execute(
        f"SELECT COUNT(*) AS n FROM opportunities WHERE current_stage IN ({inter_placeholders})",
        _INTERVIEWED_STAGES,
    ).fetchone()
    total_interviewed = inter_row["n"] if inter_row else 0

    offer_row = conn.execute(
        "SELECT COUNT(*) AS n FROM opportunities WHERE current_stage = 'Offer'"
    ).fetchone()
    total_offer = offer_row["n"] if offer_row else 0

    def _rate(numerator: int, denominator: int) -> float:
        return round(numerator / denominator * 100, 1) if denominator else 0.0

    return {
        "total_discovered": total_discovered,
        "total_applied": total_applied,
        "total_interviewed": total_interviewed,
        "total_offer": total_offer,
        "applied_rate": _rate(total_applied, total_discovered),
        "interview_rate": _rate(total_interviewed, total_applied),
        "offer_rate": _rate(total_offer, total_interviewed),
    }


def avg_score_by_stage(conn: sqlite3.Connection) -> pd.DataFrame:
    """Return [stage, avg_score, count] ordered by STAGES list."""
    rows = conn.execute(
        """
        SELECT current_stage AS stage,
               ROUND(AVG(score), 1) AS avg_score,
               COUNT(*) AS count
        FROM opportunities
        GROUP BY current_stage
        """
    ).fetchall()

    order = _stage_order_map()
    data = sorted(
        [{"stage": r["stage"], "avg_score": r["avg_score"], "count": r["count"]} for r in rows],
        key=lambda x: order.get(x["stage"], 99),
    )
    return pd.DataFrame(data) if data else pd.DataFrame(columns=["stage", "avg_score", "count"])


def time_in_stage(conn: sqlite3.Connection) -> pd.DataFrame:
    """
    For each opportunity, compute days_in_stage = days since the most recent
    stage_history entry.

    Returns [opportunity_id, company, role_title_raw, current_stage, days_in_stage].
    """
    rows = conn.execute(
        """
        SELECT
            o.id            AS opportunity_id,
            o.company,
            o.role_title_raw AS title,
            o.current_stage,
            MAX(sh.timestamp) AS last_transition
        FROM opportunities o
        LEFT JOIN stage_history sh ON sh.opportunity_id = o.id
        GROUP BY o.id
        """
    ).fetchall()

    now = datetime.now(timezone.utc)
    result = []
    for r in rows:
        days = None
        if r["last_transition"]:
            try:
                ts = datetime.fromisoformat(r["last_transition"])
                if ts.tzinfo is None:
                    ts = ts.replace(tzinfo=timezone.utc)
                days = (now - ts).days
            except ValueError:
                pass
        result.append(
            {
                "opportunity_id": r["opportunity_id"],
                "company": r["company"],
                "title": r["title"],
                "current_stage": r["current_stage"],
                "days_in_stage": days,
            }
        )

    return pd.DataFrame(result) if result else pd.DataFrame(
        columns=["opportunity_id", "company", "title", "current_stage", "days_in_stage"]
    )


def high_score_not_applied(conn: sqlite3.Connection, min_score: float = 70) -> pd.DataFrame:
    """
    Opportunities with score >= min_score that are still in early pipeline stages
    (New, Scored, Shortlisted) — i.e., not yet actioned.
    """
    rows = conn.execute(
        """
        SELECT id, company, role_title_raw AS title, score, fit_band,
               current_stage, location, salary_text, url
        FROM opportunities
        WHERE score >= ?
          AND current_stage IN ('New', 'Scored', 'Shortlisted')
        ORDER BY score DESC
        """,
        (min_score,),
    ).fetchall()

    return (
        pd.DataFrame([dict(r) for r in rows])
        if rows
        else pd.DataFrame(
            columns=[
                "id", "company", "title", "score", "fit_band",
                "current_stage", "location", "salary_text", "url",
            ]
        )
    )


def company_pipeline(conn: sqlite3.Connection) -> pd.DataFrame:
    """
    Return [company, total, applied, interviewed, offer] counts.
    """
    applied_stages = (
        "Applied", "Recruiter Screen", "Hiring Manager",
        "Panel", "Final Round", "Offer",
    )
    inter_stages = ("Recruiter Screen", "Hiring Manager", "Panel", "Final Round", "Offer")

    rows = conn.execute("SELECT company, current_stage FROM opportunities").fetchall()

    if not rows:
        return pd.DataFrame(columns=["company", "total", "applied", "interviewed", "offer"])

    df = pd.DataFrame([dict(r) for r in rows])

    agg = (
        df.groupby("company")
        .apply(
            lambda g: pd.Series(
                {
                    "total": len(g),
                    "applied": g["current_stage"].isin(applied_stages).sum(),
                    "interviewed": g["current_stage"].isin(inter_stages).sum(),
                    "offer": (g["current_stage"] == "Offer").sum(),
                }
            )
        )
        .reset_index()
    )
    return agg.sort_values("total", ascending=False)


def score_vs_outcome(conn: sqlite3.Connection) -> pd.DataFrame:
    """
    Average score and count for terminal stages plus Applied and above.
    """
    stages = (
        "Applied",
        "Recruiter Screen",
        "Hiring Manager",
        "Panel",
        "Final Round",
        "Offer",
        "Rejected",
        "Archived",
    )
    placeholders = ",".join("?" * len(stages))
    rows = conn.execute(
        f"""
        SELECT current_stage AS stage,
               ROUND(AVG(score), 1) AS avg_score,
               COUNT(*) AS count
        FROM opportunities
        WHERE current_stage IN ({placeholders})
        GROUP BY current_stage
        """,
        stages,
    ).fetchall()

    order = _stage_order_map()
    data = sorted(
        [{"stage": r["stage"], "avg_score": r["avg_score"], "count": r["count"]} for r in rows],
        key=lambda x: order.get(x["stage"], 99),
    )
    return pd.DataFrame(data) if data else pd.DataFrame(columns=["stage", "avg_score", "count"])
