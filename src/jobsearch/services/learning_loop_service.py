from __future__ import annotations
import logging
import sqlite3
import pandas as pd
from typing import Dict, List, Any, Optional

from jobsearch import ats_db

logger = logging.getLogger(__name__)

class LearningLoopService:
    """Analyzes search outcomes to calibrate scoring and prioritization logic."""

    def __init__(self, conn: sqlite3.Connection):
        self.conn = conn

    def get_score_vs_outcome(self) -> pd.DataFrame:
        """Analyze how fit scores correlate with interview and offer rates."""
        rows = self.conn.execute(
            """
            SELECT 
                CASE 
                    WHEN score >= 90 THEN '90-100'
                    WHEN score >= 80 THEN '80-89'
                    WHEN score >= 70 THEN '70-79'
                    WHEN score >= 60 THEN '60-69'
                    ELSE 'Below 60'
                END as score_band,
                COUNT(*) as total,
                SUM(CASE WHEN status IN ('screening', 'interviewing', 'offer', 'accepted') THEN 1 ELSE 0 END) as interested,
                SUM(CASE WHEN status IN ('offer', 'accepted') THEN 1 ELSE 0 END) as offers
            FROM applications
            WHERE status != 'considering'
            GROUP BY score_band
            ORDER BY score_band DESC
            """
        ).fetchall()
        
        df = pd.DataFrame([dict(r) for r in rows])
        if not df.empty:
            df['interest_rate'] = (df['interested'] / df['total'] * 100).round(1)
            df['offer_rate'] = (df['offers'] / df['total'] * 100).round(1)
        return df

    def get_dimension_conversion(self, dimension: str = "company") -> pd.DataFrame:
        """Analyze conversion rates by company or title family."""
        column = "company" if dimension == "company" else "v2_canonical_title"
        
        # We only look at applied roles
        rows = self.conn.execute(
            f"""
            SELECT 
                {column} as dimension,
                COUNT(*) as applied,
                SUM(CASE WHEN status IN ('screening', 'interviewing', 'offer', 'accepted') THEN 1 ELSE 0 END) as interviews,
                SUM(CASE WHEN status = 'rejected' THEN 1 ELSE 0 END) as rejections
            FROM applications
            WHERE status != 'considering'
            GROUP BY {column}
            HAVING applied >= 2
            ORDER BY interviews DESC, applied DESC
            LIMIT 20
            """
        ).fetchall()
        
        df = pd.DataFrame([dict(r) for r in rows])
        if not df.empty:
            df['conversion_rate'] = (df['interviews'] / df['applied'] * 100).round(1)
        return df

    def get_keyword_correlation(self) -> pd.DataFrame:
        """Correlate matched keywords with interview outcomes."""
        rows = self.conn.execute(
            """
            SELECT matched_keywords, status 
            FROM applications 
            WHERE status != 'considering' AND matched_keywords IS NOT NULL
            """
        ).fetchall()
        
        keyword_stats = {}
        for r in rows:
            kws = str(r['matched_keywords']).lower().split('|')
            is_success = r['status'] in ('screening', 'interviewing', 'offer', 'accepted')
            for kw in kws:
                kw = kw.strip()
                if not kw: continue
                if kw not in keyword_stats:
                    keyword_stats[kw] = {'total': 0, 'success': 0}
                keyword_stats[kw]['total'] += 1
                if is_success:
                    keyword_stats[kw]['success'] += 1
                    
        data = []
        for kw, stats in keyword_stats.items():
            if stats['total'] >= 3: # Minimum sample size
                rate = (stats['success'] / stats['total'] * 100)
                data.append({'keyword': kw, 'applied': stats['total'], 'success': stats['success'], 'rate': round(rate, 1)})
                
        df = pd.DataFrame(data)
        if not df.empty:
            df = df.sort_values('rate', ascending=False).head(20)
        return df

    def get_calibration_insights(self) -> List[Dict[str, Any]]:
        """Generate explainable insights for system calibration."""
        insights = []
        
        # 1. Under-valued bands
        score_df = self.get_score_vs_outcome()
        if not score_df.empty:
            low_band = score_df[score_df['score_band'].isin(['70-79', '60-69'])]
            high_band = score_df[score_df['score_band'] == '90-100']
            
            if not low_band.empty and not high_band.empty:
                avg_low_rate = low_band['interest_rate'].mean()
                high_rate = high_band['interest_rate'].iloc[0]
                if avg_low_rate > high_rate:
                    insights.append({
                        "type": "warning",
                        "title": "Score Inversion Detected",
                        "reason": f"Lower score bands ({avg_low_rate:.1f}%) are converting better than top-tier matches ({high_rate:.1f}%). Scoring weights may need calibration."
                    })

        # 2. High performing families
        family_df = self.get_dimension_conversion("title")
        if not family_df.empty:
            top_family = family_df.iloc[0]
            if top_family['conversion_rate'] > 50:
                insights.append({
                    "type": "success",
                    "title": f"Strong Title Family: {top_family['dimension']}",
                    "reason": f"This role type has a {top_family['conversion_rate']}% interview rate. Prioritize these discovery leads."
                })
                
        return insights

    def get_submission_friction_stats(self) -> pd.DataFrame:
        """Analyze why 'ready' applications fail to be submitted."""
        rows = self.conn.execute(
            """
            SELECT title as blocker_reason, COUNT(*) as count
            FROM events
            WHERE title LIKE 'Submission Blocked:%'
            GROUP BY title
            ORDER BY count DESC
            """
        ).fetchall()
        
        data = []
        for r in rows:
            reason = r['blocker_reason'].replace("Submission Blocked: ", "")
            data.append({"reason": reason, "count": r['count']})
            
        return pd.DataFrame(data)

    def get_final_mile_funnel(self) -> Dict[str, int]:
        """Measure conversion from prepared to applied."""
        # 'prepared' is a newer status, so we also look at events
        prepared = self.conn.execute(
            "SELECT COUNT(*) FROM applications WHERE status = 'prepared'"
        ).fetchone()[0]
        
        # Total ever prepared (including those now applied)
        total_prepared = self.conn.execute(
            "SELECT COUNT(DISTINCT application_id) FROM events WHERE title = 'Marked as Prepared'"
        ).fetchone()[0]
        
        total_applied = self.conn.execute(
            "SELECT COUNT(*) FROM applications WHERE status != 'considering' AND status != 'exploring' AND status != 'prepared'"
        ).fetchone()[0]
        
        return {
            "currently_prepared": prepared,
            "ever_prepared": max(total_prepared, prepared),
            "total_submitted": total_applied
        }

    def get_ats_friction_report(self) -> pd.DataFrame:
        """Identify ATS families with the highest submission drop-off."""
        rows = self.conn.execute(
            """
            SELECT a.extraction_method, COUNT(e.id) as blocker_count
            FROM applications a
            JOIN events e ON e.application_id = a.id
            WHERE e.title LIKE 'Submission Blocked:%'
            GROUP BY a.extraction_method
            ORDER BY blocker_count DESC
            """
        ).fetchall()
        return pd.DataFrame([dict(r) for r in rows])
