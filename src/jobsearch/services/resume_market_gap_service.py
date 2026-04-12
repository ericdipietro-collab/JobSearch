from __future__ import annotations
import logging
import sqlite3
import pandas as pd
from typing import List, Dict, Any, Optional, Set

from jobsearch import ats_db

logger = logging.getLogger(__name__)

class ResumeMarketGapService:
    """Strategic comparison of user's resume vs target market signals."""

    THEMES = {
        "API & Platform": ["api", "platform", "integrations", "developer experience", "dx", "sdk", "saas", "technical product"],
        "Data Engineering": ["etl", "data warehouse", "snowflake", "databricks", "sql", "pipeline", "data vault", "bi"],
        "Fintech Depth": ["wealth", "brokerage", "capital markets", "custodian", "ledger", "reconciliation", "tax-lot", "portfolio"],
        "Leadership": ["strategy", "roadmap", "stakeholder", "cross-functional", "leadership", "transformation", "influence"],
        "Product Operations": ["agile", "scrum", "sdlc", "jira", "documentation", "process", "metrics", "kpi"]
    }

    def __init__(self, conn: sqlite3.Connection):
        self.conn = conn

    def get_market_keywords(self, top_n: int = 50) -> Dict[str, int]:
        """Aggregate matched keywords from high-score jobs (>= 75)."""
        rows = self.conn.execute(
            "SELECT matched_keywords FROM applications WHERE score >= 75 AND matched_keywords IS NOT NULL"
        ).fetchall()
        
        counts = {}
        for r in rows:
            kws = str(r['matched_keywords']).lower().split('|')
            for kw in kws:
                kw = kw.strip()
                if kw: counts[kw] = counts.get(kw, 0) + 1
                
        # Sort and return top N
        sorted_counts = sorted(counts.items(), key=lambda x: x[1], reverse=True)
        return dict(sorted_counts[:top_n])

    def analyze_gaps(self) -> Dict[str, Any]:
        """Compare top market keywords vs base resume."""
        resume_text = ats_db.get_setting(self.conn, "base_resume_text", "").lower()
        market_kws = self.get_market_keywords()
        
        aligned = []
        gaps = []
        
        for kw, count in market_kws.items():
            if kw in resume_text:
                aligned.append({"keyword": kw, "market_frequency": count})
            else:
                gaps.append({"keyword": kw, "market_frequency": count})
                
        # Theme rollups
        theme_gaps = {}
        for theme, kws in self.THEMES.items():
            theme_total = sum(1 for k in kws if k in [g['keyword'] for g in gaps])
            if theme_total > 0:
                theme_gaps[theme] = theme_total

        return {
            "aligned": aligned[:15],
            "gaps": gaps[:15],
            "theme_gaps": theme_gaps,
            "total_market_jobs_analyzed": len(market_kws)
        }
