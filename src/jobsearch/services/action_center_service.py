from __future__ import annotations
import logging
import json
import sqlite3
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import List, Dict, Any, Optional

from jobsearch import ats_db
from jobsearch.services.readiness_service import ReadinessService
from jobsearch.services.submission_review_service import SubmissionReviewService

logger = logging.getLogger(__name__)

@dataclass
class ActionRecommendation:
    entity_type: str  # 'job', 'application', 'contact', 'interview'
    entity_id: str
    company: str
    role_title: str
    action_key: str  # 'apply', 'follow_up', 'outreach', 'prep', 'archive', 'reexport', 'revision'
    action_label: str
    urgency_score: float  # 0-100
    impact_score: float   # 0-100
    reason: str
    due_date: Optional[str] = None
    metadata: Dict[str, Any] = field(default_factory=dict)

    @property
    def total_score(self) -> float:
        return (self.urgency_score * 0.6) + (self.impact_score * 0.4)

class ActionCenterService:
    """Engine for generating and managing prioritized user actions."""

    def __init__(self, conn: sqlite3.Connection, readiness_service: Optional[ReadinessService] = None, submission_service: Optional[SubmissionReviewService] = None):
        self.conn = conn
        self.readiness_service = readiness_service or ReadinessService(conn)
        self.submission_service = submission_service or SubmissionReviewService(conn)

    def get_recommendations(self) -> List[ActionRecommendation]:
        """Generate and return all active recommendations, sorted by priority."""
        recommendations: List[ActionRecommendation] = []
        
        # 1. Fetch persistent states (dismissed/snoozed)
        user_actions = {
            (r["entity_type"], str(r["entity_id"]), r["action_key"]): r 
            for r in ats_db.get_user_actions(self.conn)
        }
        
        # 2. Rules
        recommendations.extend(self._rule_apply_now())
        recommendations.extend(self._rule_follow_ups())
        recommendations.extend(self._rule_interview_prep())
        recommendations.extend(self._rule_networking_outreach())
        recommendations.extend(self._rule_archive_stale())
        recommendations.extend(self._rule_revision_and_recovery())

        # 3. Filter out dismissed/snoozed
        active_recs = []
        for rec in recommendations:
            key = (rec.entity_type, str(rec.entity_id), rec.action_key)
            if key in user_actions:
                # If it's in user_actions but status is 'active' (due to snoozed_until expiration), keep it
                active_recs.append(rec)
            elif any(ua_key == key for ua_key in user_actions):
                # Was dismissed or completed
                continue
            else:
                # New recommendation
                active_recs.append(rec)

        # 4. Sort
        active_recs.sort(key=lambda x: x.total_score, reverse=True)
        return active_recs

    def _rule_apply_now(self) -> List[ActionRecommendation]:
        """High-score unreviewed jobs."""
        recs = []
        rows = self.conn.execute(
            """
            SELECT id, company, role, score, fit_band, date_discovered 
            FROM applications 
            WHERE status = 'considering' AND fit_band = 'APPLY NOW'
            AND date_discovered >= datetime('now', '-7 days')
            ORDER BY score DESC LIMIT 15
            """
        ).fetchall()
        
        for row in rows:
            recs.append(ActionRecommendation(
                entity_type='job',
                entity_id=str(row['id']),
                company=row['company'],
                role_title=row['role'],
                action_key='apply',
                action_label='Apply Now',
                urgency_score=80,
                impact_score=row['score'],
                reason=f"Top tier match ({row['fit_band']}) discovered recently."
            ))
        return recs

    def _rule_revision_and_recovery(self) -> List[ActionRecommendation]:
        """Auto-trigger actions for stale, outdated, or revision-needed applications."""
        recs = []
        # Check 'prepared' or high-score 'considering' apps
        rows = self.conn.execute(
            """
            SELECT id, company, role, status, score, last_exported_at, updated_at
            FROM applications 
            WHERE status IN ('prepared', 'considering')
            AND (status = 'prepared' OR score >= 70)
            """
        ).fetchall()
        
        for row in rows:
            app_id = row['id']
            freshness = self.submission_service.evaluate_freshness(app_id)
            readiness = self.readiness_service.evaluate(app_id)
            
            # Case 1: Outdated after edit (High Urgency)
            if freshness['status'] == 'outdated_after_edit':
                recs.append(ActionRecommendation(
                    entity_type='application',
                    entity_id=str(app_id),
                    company=row['company'],
                    role_title=row['role'],
                    action_key='reexport_required',
                    action_label='Refresh Export',
                    urgency_score=90,
                    impact_score=row['score'],
                    reason=f"Tailored materials were updated. Re-export before submitting."
                ))
            
            # Case 2: Stale Export (Prepared but old)
            elif row['status'] == 'prepared' and freshness['status'] == 'stale':
                recs.append(ActionRecommendation(
                    entity_type='application',
                    entity_id=str(app_id),
                    company=row['company'],
                    role_title=row['role'],
                    action_key='freshness_warning',
                    action_label='Review Stale Export',
                    urgency_score=75,
                    impact_score=max(0, row['score'] - 10), # Modest decay
                    reason=f"Export is {freshness['reason']}. Review before submitting."
                ))

            # Case 3: Blocked by placeholder (Critical)
            elif readiness.status == 'blocked' and 'placeholder' in readiness.reason.lower():
                recs.append(ActionRecommendation(
                    entity_type='application',
                    entity_id=str(app_id),
                    company=row['company'],
                    role_title=row['role'],
                    action_key='readiness_blocked',
                    action_label='Fix Placeholders',
                    urgency_score=85,
                    impact_score=row['score'],
                    reason=f"Critical issue: {readiness.reason}"
                ))

            # Case 4: Explicit Needs Revision (from events)
            # Find last event title for this app
            last_event = self.conn.execute(
                "SELECT title FROM events WHERE application_id = ? ORDER BY id DESC LIMIT 1",
                (app_id,)
            ).fetchone()
            if last_event and "Revision" in last_event['title']:
                recs.append(ActionRecommendation(
                    entity_type='application',
                    entity_id=str(app_id),
                    company=row['company'],
                    role_title=row['role'],
                    action_key='revision_required',
                    action_label='Complete Revision',
                    urgency_score=95,
                    impact_score=row['score'],
                    reason="Application marked for revision during review."
                ))

        return recs

    def _safe_parse_date(self, date_str: Optional[str]) -> Optional[datetime]:
        """Safely parse a date string which might be in ISO or human format."""
        if not date_str:
            return None
        try:
            # Try ISO first
            return datetime.fromisoformat(date_str.split('T')[0])
        except (ValueError, TypeError):
            try:
                # Fallback to pandas if available for robust parsing
                import pandas as pd
                return pd.to_datetime(date_str).to_pydatetime()
            except:
                return None

    def _rule_follow_ups(self) -> List[ActionRecommendation]:
        """Overdue follow-ups for applied roles."""
        recs = []
        # Join with events to ensure no recent activity
        rows = self.conn.execute(
            """
            SELECT a.id, a.company, a.role, a.date_applied, a.follow_up_date
            FROM applications a
            WHERE a.status = 'applied'
            AND (
                a.follow_up_date <= date('now')
                OR (a.date_applied <= date('now', '-7 days') AND a.follow_up_date IS NULL)
            )
            """
        ).fetchall()
        
        for row in rows:
            days_since = 7
            applied_dt = self._safe_parse_date(row['date_applied'])
            if applied_dt:
                if applied_dt.tzinfo is not None:
                    applied_dt = applied_dt.astimezone(timezone.utc).replace(tzinfo=None)
                days_since = (datetime.now() - applied_dt).days
            
            recs.append(ActionRecommendation(
                entity_type='application',
                entity_id=str(row['id']),
                company=row['company'],
                role_title=row['role'],
                action_key='follow_up',
                action_label='Send Follow-up',
                urgency_score=min(100, 50 + (days_since * 5)),
                impact_score=70,
                reason=f"No activity recorded since application ({days_since} days ago)."
            ))
        return recs

    def _rule_interview_prep(self) -> List[ActionRecommendation]:
        """Upcoming interviews within 48 hours."""
        recs = []
        rows = self.conn.execute(
            """
            SELECT i.id, i.scheduled_at, a.company, a.role, a.id as app_id
            FROM interviews i
            JOIN applications a ON i.application_id = a.id
            WHERE i.scheduled_at >= datetime('now')
            AND i.scheduled_at <= datetime('now', '+2 days')
            AND i.outcome = 'pending'
            """
        ).fetchall()
        
        for row in rows:
            recs.append(ActionRecommendation(
                entity_type='interview',
                entity_id=str(row['id']),
                company=row['company'],
                role_title=row['role'],
                action_key='prep',
                action_label='Prepare for Interview',
                urgency_score=95,
                impact_score=100,
                reason=f"Interview scheduled for {row['scheduled_at']}."
            ))
        return recs

    def _rule_networking_outreach(self) -> List[ActionRecommendation]:
        """Contacts at target companies with no recent touchpoint."""
        recs = []
        rows = self.conn.execute(
            """
            SELECT id, name, company, title, relationship, last_contact_date, follow_up_date
            FROM network_contacts
            WHERE follow_up_date <= date('now')
            OR (last_contact_date <= date('now', '-21 days') AND last_contact_date IS NOT NULL)
            LIMIT 10
            """
        ).fetchall()
        
        for row in rows:
            recs.append(ActionRecommendation(
                entity_type='contact',
                entity_id=str(row['id']),
                company=row['company'] or "Unknown",
                role_title=row['name'],
                action_key='outreach',
                action_label='Networking Outreach',
                urgency_score=60,
                impact_score=85,
                reason=f"Follow-up date reached or no contact in 21+ days."
            ))
        return recs

    def _rule_archive_stale(self) -> List[ActionRecommendation]:
        """Stale low-score considering roles."""
        recs = []
        rows = self.conn.execute(
            """
            SELECT id, company, role, score, date_discovered
            FROM applications
            WHERE status = 'considering'
            AND score < 50
            AND date_discovered <= datetime('now', '-30 days')
            LIMIT 10
            """
        ).fetchall()
        
        for row in rows:
            recs.append(ActionRecommendation(
                entity_type='application',
                entity_id=str(row['id']),
                company=row['company'],
                role_title=row['role'],
                action_key='archive',
                action_label='Archive Stale Role',
                urgency_score=30,
                impact_score=20,
                reason="Low fit score and discovered >30 days ago."
            ))
        return recs

    def complete_action(self, rec: ActionRecommendation):
        """Mark an action as completed."""
        ats_db.upsert_user_action(
            self.conn, 
            rec.entity_type, 
            rec.entity_id, 
            rec.action_key, 
            status='completed'
        )

    def dismiss_action(self, rec: ActionRecommendation):
        """Permanently dismiss a recommendation."""
        ats_db.upsert_user_action(
            self.conn, 
            rec.entity_type, 
            rec.entity_id, 
            rec.action_key, 
            status='dismissed'
        )

    def snooze_action(self, rec: ActionRecommendation, days: int = 3):
        """Snooze a recommendation for N days."""
        until = (datetime.now(timezone.utc) + timedelta(days=days)).isoformat()
        ats_db.upsert_user_action(
            self.conn, 
            rec.entity_type, 
            rec.entity_id, 
            rec.action_key, 
            status='active',
            snoozed_until=until
        )
