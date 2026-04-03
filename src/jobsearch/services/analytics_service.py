"""services/analytics_service.py — Aggregation queries for the Analytics page (Unified DB)."""

from __future__ import annotations
import sqlite3
from datetime import datetime, timezone
from typing import Dict, List, Optional
import pandas as pd

# Standard stages for funnel ordering
STAGES = (
    "New",
    "Scored",
    "Shortlisted",
    "Applied",
    "Recruiter Screen",
    "Hiring Manager",
    "Panel",
    "Final Round",
    "Offer",
    "Accepted",
    "Rejected",
    "Archived",
    "Withdrawn",
)

_INTERVIEWED_STAGES = (
    "Recruiter Screen",
    "Hiring Manager",
    "Panel",
    "Final Round",
    "Offer",
)

def _stage_order_map() -> Dict[str, int]:
    return {stage: idx for idx, stage in enumerate(STAGES)}

def funnel_counts(conn: sqlite3.Connection) -> pd.DataFrame:
    """Return a DataFrame with columns [stage, count] ordered by STAGES list."""
    rows = conn.execute(
        "SELECT status AS stage, COUNT(*) AS count FROM applications GROUP BY status"
    ).fetchall()

    counts: Dict[str, int] = {str(r["stage"]).capitalize(): r["count"] for r in rows}
    
    data = []
    for s in STAGES:
        count = counts.get(s, counts.get(s.lower(), 0))
        data.append({"stage": s, "count": count})
        
    return pd.DataFrame(data)

def conversion_rates(conn: sqlite3.Connection) -> dict:
    total_row = conn.execute("SELECT COUNT(*) AS n FROM applications").fetchone()
    total_discovered = total_row["n"] if total_row else 0

    applied_stages = ("applied", "screening", "interviewing", "offer", "accepted", "rejected")
    placeholders = ",".join("?" * len(applied_stages))
    
    applied_row = conn.execute(
        f"SELECT COUNT(*) AS n FROM applications WHERE status IN ({placeholders})",
        applied_stages,
    ).fetchone()
    total_applied = applied_row["n"] if applied_row else 0

    inter_row = conn.execute(
        "SELECT COUNT(DISTINCT application_id) AS n FROM interviews"
    ).fetchone()
    total_interviewed = inter_row["n"] if inter_row else 0

    offer_row = conn.execute(
        "SELECT COUNT(*) AS n FROM applications WHERE status IN ('offer', 'accepted')"
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
    rows = conn.execute(
        """
        SELECT status AS stage,
               ROUND(AVG(score), 1) AS avg_score,
               COUNT(*) AS count
        FROM applications
        GROUP BY status
        """
    ).fetchall()

    order = _stage_order_map()
    data = []
    for r in rows:
        label = str(r["stage"]).capitalize()
        data.append({"stage": label, "avg_score": r["avg_score"], "count": r["count"]})
        
    data.sort(key=lambda x: order.get(x["stage"], 99))
    return pd.DataFrame(data) if data else pd.DataFrame(columns=["stage", "avg_score", "count"])

def time_in_stage(conn: sqlite3.Connection) -> pd.DataFrame:
    rows = conn.execute(
        """
        SELECT
            a.id            AS application_id,
            a.company,
            a.role          AS title,
            a.status        AS current_stage,
            MAX(sh.timestamp) AS last_transition
        FROM applications a
        LEFT JOIN stage_history sh ON sh.application_id = a.id
        GROUP BY a.id
        """
    ).fetchall()

    now = datetime.now(timezone.utc)
    result = []
    for r in rows:
        days = None
        if r["last_transition"]:
            try:
                ts_str = r["last_transition"]
                if " " in ts_str and "T" not in ts_str:
                    ts_str = ts_str.replace(" ", "T")
                ts = datetime.fromisoformat(ts_str)
                if ts.tzinfo is None:
                    ts = ts.replace(tzinfo=timezone.utc)
                days = (now - ts).days
            except (ValueError, TypeError):
                pass
        result.append(
            {
                "application_id": r["application_id"],
                "company": r["company"],
                "title": r["title"],
                "current_stage": r["current_stage"],
                "days_in_stage": days,
            }
        )

    return pd.DataFrame(result) if result else pd.DataFrame(
        columns=["application_id", "company", "title", "current_stage", "days_in_stage"]
    )

def high_score_not_applied(conn: sqlite3.Connection, min_score: float = 70) -> pd.DataFrame:
    rows = conn.execute(
        """
        SELECT id, company, role AS title, score, fit_band,
               status AS current_stage, location, salary_text, job_url AS url
        FROM applications
        WHERE score >= ?
          AND status = 'considering'
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
    rows = conn.execute("SELECT company, status FROM applications").fetchall()

    if not rows:
        return pd.DataFrame(columns=["company", "total", "applied", "interviewed", "offer"])

    df = pd.DataFrame([dict(r) for r in rows])
    applied_statuses = ("applied", "screening", "interviewing", "offer", "accepted", "rejected")

    agg = (
        df.groupby("company")
        .apply(
            lambda g: pd.Series(
                {
                    "total": len(g),
                    "applied": g["status"].isin(applied_statuses).sum(),
                    "interviewed": g["status"].isin(["screening", "interviewing", "offer"]).sum(),
                    "offer": g["status"].isin(["offer", "accepted"]).sum(),
                }
            )
        )
        .reset_index()
    )
    return agg.sort_values("total", ascending=False)

def score_vs_outcome(conn: sqlite3.Connection) -> pd.DataFrame:
    statuses = ("applied", "screening", "interviewing", "offer", "accepted", "rejected", "withdrawn")
    placeholders = ",".join("?" * len(statuses))
    rows = conn.execute(
        f"""
        SELECT status AS stage,
               ROUND(AVG(score), 1) AS avg_score,
               COUNT(*) AS count
        FROM applications
        WHERE status IN ({placeholders})
        GROUP BY status
        """,
        statuses,
    ).fetchall()

    order = _stage_order_map()
    data = []
    for r in rows:
        label = str(r["stage"]).capitalize()
        data.append({"stage": label, "avg_score": r["avg_score"], "count": r["count"]})
        
    data.sort(key=lambda x: order.get(x["stage"], 99))
    return pd.DataFrame(data) if data else pd.DataFrame(columns=["stage", "avg_score", "count"])
