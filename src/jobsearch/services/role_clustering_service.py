from __future__ import annotations
import logging
import sqlite3
import pandas as pd
from dataclasses import dataclass, field
from typing import List, Dict, Any, Optional

from jobsearch import ats_db

logger = logging.getLogger(__name__)

@dataclass
class ClusterDefinition:
    label: str
    keywords: List[str]
    title_terms: List[str]
    description: str

class RoleClusteringService:
    """Service for grouping jobs into interpretable market segments."""

    DEFINITIONS = [
        ClusterDefinition(
            "Fintech & Wealthtech",
            ["wealth management", "brokerage", "custodian", "tax-lot", "portfolio", "trading", "capital markets", "fintech"],
            ["wealth", "fintech", "brokerage", "trading"],
            "Roles focused on financial systems, investment platforms, and wealth management."
        ),
        ClusterDefinition(
            "Payments & Ledgers",
            ["payments", "ledger", "reconciliation", "disbursement", "transaction", "payout", "stripe", "fiserv"],
            ["payments", "ledger", "reconciliation"],
            "Roles focused on money movement, accounting ledgers, and payment processing."
        ),
        ClusterDefinition(
            "API & Platform",
            ["api platform", "developer experience", "integrations", "saas platform", "extensibility", "sdk", "webhook"],
            ["platform", "api", "integrations", "technical product"],
            "Internal or external platform roles focused on technical infrastructure and developer tools."
        ),
        ClusterDefinition(
            "Data & Analytics",
            ["etl", "data warehouse", "snowflake", "databricks", "reporting", "analytics", "data vault", "business intelligence"],
            ["data", "analytics", "bi", "reporting"],
            "Roles focused on data movement, warehousing, and downstream analytics."
        ),
        ClusterDefinition(
            "Solutions & Client Success",
            ["solutions architecture", "client facing", "technical implementation", "pre-sales", "post-sales", "onboarding"],
            ["solutions", "implementation", "success", "client"],
            "Technical roles involving direct customer interaction and specialized delivery."
        ),
        ClusterDefinition(
            "Leadership & Strategy",
            ["product strategy", "roadmap", "stakeholder management", "cross-functional", "leadership", "transformation"],
            ["lead", "head of", "director", "senior manager", "principal"],
            "High-level roles focused on organizational influence and strategic direction."
        )
    ]

    def __init__(self, conn: sqlite3.Connection):
        self.conn = conn

    def cluster_job(self, job_data: Dict[str, Any]) -> tuple[str, List[str], str]:
        """Assigns a job to a primary cluster and optional secondary tags."""
        title = str(job_data.get('role') or "").lower()
        desc = str(job_data.get('description_excerpt') or "").lower()
        keywords = str(job_data.get('matched_keywords') or "").lower().split('|')
        
        scores = {}
        for dfn in self.DEFINITIONS:
            score = 0
            # Title matches are strong
            if any(t in title for t in dfn.title_terms):
                score += 10
            
            # Keyword matches
            matched_kws = [k for k in dfn.keywords if k in desc or k in title or k in keywords]
            score += len(matched_kws) * 2
            
            if score > 0:
                scores[dfn.label] = score
                
        if not scores:
            return "General / Unclassified", [], "No specific cluster patterns detected."
            
        # Sort by score
        sorted_clusters = sorted(scores.items(), key=lambda x: x[1], reverse=True)
        primary = sorted_clusters[0][0]
        secondary = [c[0] for c in sorted_clusters[1:] if c[1] >= (sorted_clusters[0][1] * 0.5)]
        
        rationale = f"Assigned to '{primary}' because of title/keyword alignment. "
        if secondary:
            rationale += f"Also tagged with: {', '.join(secondary)}."
            
        return primary, secondary, rationale

    def run_batch_clustering(self):
        """Processes all unclustered applications."""
        rows = self.conn.execute("SELECT id, role, description_excerpt, matched_keywords FROM applications WHERE role_cluster IS NULL").fetchall()
        for row in rows:
            primary, secondary, rationale = self.cluster_job(dict(row))
            self.conn.execute(
                "UPDATE applications SET role_cluster = ?, notes = notes || ? WHERE id = ?",
                (primary, f"\nCluster Rationale: {rationale}", row['id'])
            )
        self.conn.commit()

    def get_market_map(self) -> pd.DataFrame:
        """Aggregate analytics by cluster."""
        rows = self.conn.execute(
            """
            SELECT 
                role_cluster as cluster,
                COUNT(*) as total_jobs,
                AVG(score) as avg_fit_score,
                SUM(CASE WHEN status IN ('screening', 'interviewing', 'offer', 'accepted') THEN 1 ELSE 0 END) as interviews,
                SUM(CASE WHEN status = 'rejected' THEN 1 ELSE 0 END) as rejections
            FROM applications
            WHERE role_cluster IS NOT NULL
            GROUP BY role_cluster
            ORDER BY total_jobs DESC
            """
        ).fetchall()
        
        df = pd.DataFrame([dict(r) for r in rows])
        if not df.empty:
            df['interview_rate'] = (df['interviews'] / df['total_jobs'] * 100).round(1)
        return df
