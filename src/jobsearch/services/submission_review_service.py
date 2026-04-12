from __future__ import annotations
import logging
import json
import sqlite3
from dataclasses import dataclass, field
from datetime import datetime, timezone, timedelta
from typing import List, Dict, Any, Optional

from jobsearch import ats_db
from jobsearch.config.settings import settings
from jobsearch.services.readiness_service import ReadinessService, ReadinessState

logger = logging.getLogger(__name__)

@dataclass
class ChecklistItem:
    label: str
    is_required: bool
    status: bool  # True = pass, False = fail/missing
    description: str

@dataclass
class SubmissionQueueItem:
    application_id: int
    company: str
    role: str
    status: str
    readiness: ReadinessState
    apply_url: Optional[str]
    last_exported_at: Optional[str]
    checklist: List[ChecklistItem]

class SubmissionReviewService:
    """Service for managing the manual submission workflow and queue."""

    BLOCKER_REASONS = {
        "friction_portal": "Portal too long / too much friction",
        "friction_questions": "Custom questions required more work",
        "weak_review": "Job looked weaker on final review",
        "stale_posting": "Stale or duplicate posting",
        "comp_mismatch": "Compensation mismatch",
        "location_surprise": "Onsite/hybrid surprise",
        "revision_needed": "Resume or cover letter needs revision",
        "missing_info": "Missing required information for form",
        "link_broken": "Closed posting / bad link",
        "other": "Other reason (see notes)"
    }

    def __init__(self, conn: sqlite3.Connection):
        self.conn = conn
        self.readiness_service = ReadinessService(conn)

    def get_queue(self, statuses: List[str] = ["considering", "prepared"]) -> List[SubmissionQueueItem]:
        """Fetch applications eligible for submission review."""
        # We prioritize 'prepared' items, then high-readiness 'considering' items
        query = f"""
            SELECT id, company, role, status, job_url, last_exported_at 
            FROM applications 
            WHERE status IN ({','.join(['?']*len(statuses))})
            ORDER BY CASE WHEN status = 'prepared' THEN 0 ELSE 1 END, updated_at DESC
        """
        rows = self.conn.execute(query, statuses).fetchall()
        
        queue = []
        for row in rows:
            app_id = row['id']
            readiness = self.readiness_service.evaluate(app_id)
            
            # Build Checklist
            checklist = [
                ChecklistItem("Tailored Resume (JSON)", True, readiness.has_resume, "A tailored resume variant must exist."),
                ChecklistItem("Apply Link Present", True, bool(row['job_url']), "The direct apply URL must be saved."),
                ChecklistItem("Placeholders Cleared", True, readiness.status != 'blocked', readiness.reason),
                ChecklistItem("Cover Letter", False, readiness.has_cover_letter, "Optional but recommended."),
                ChecklistItem("Recruiter Note", False, readiness.has_outreach, "Optional but recommended."),
            ]
            
            queue.append(SubmissionQueueItem(
                application_id=app_id,
                company=row['company'],
                role=row['role'],
                status=row['status'],
                readiness=readiness,
                apply_url=row['job_url'],
                last_exported_at=row['last_exported_at'],
                checklist=checklist
            ))
        return queue

    def mark_prepared(self, application_id: int):
        """Move application to 'prepared' state."""
        ats_db.update_application(self.conn, application_id, status="prepared")
        ats_db.add_event(
            self.conn, 
            application_id, 
            "note", 
            datetime.now(timezone.utc).isoformat(),
            title="Marked as Prepared",
            notes="User reviewed materials and moved to preparation stage."
        )

    def mark_submitted(self, application_id: int, notes: Optional[str] = None):
        """Final confirmation of manual submission."""
        ats_db.update_application(
            self.conn, 
            application_id, 
            status="applied", 
            date_applied=datetime.now(timezone.utc).date().isoformat()
        )
        ats_db.add_event(
            self.conn, 
            application_id, 
            "applied", 
            datetime.now(timezone.utc).isoformat(),
            title="Submitted (Confirmed)",
            notes=notes
        )

    def log_blocker(self, application_id: int, reason_key: str, notes: Optional[str] = None):
        """Record why a submission was blocked or deferred."""
        reason_text = self.BLOCKER_REASONS.get(reason_key, "Unknown blocker")
        ats_db.add_event(
            self.conn, 
            application_id, 
            "note", 
            datetime.now(timezone.utc).isoformat(),
            title=f"Submission Blocked: {reason_text}",
            notes=notes
        )
        if reason_key == "revision_needed":
            ats_db.update_application(self.conn, application_id, status="considering")

    def record_export(self, application_id: int):
        """Update last_exported_at metadata."""
        now = datetime.now(timezone.utc).isoformat()
        self.conn.execute(
            "UPDATE applications SET last_exported_at = ?, updated_at = ? WHERE id = ?",
            (now, now, application_id)
        )
        self.conn.commit()

    def evaluate_freshness(self, application_id: int) -> Dict[str, Any]:
        """Check if the submission package is up to date."""
        app_row = ats_db.get_application(self.conn, application_id)
        if not app_row:
            return {"status": "error", "reason": "Application not found"}
            
        app = dict(app_row)
        last_exported = app.get("last_exported_at")
        if not last_exported:
            return {"status": "missing_export", "reason": "No export generated yet for this application."}
            
        exp_dt = datetime.fromisoformat(last_exported.replace("Z", "+00:00"))
        now = datetime.now(timezone.utc)
        
        # 1. Age check
        max_age_hours = settings.tailoring_export_max_age_hours
        if (now - exp_dt) > timedelta(hours=max_age_hours):
            return {
                "status": "stale", 
                "reason": f"Export is older than {max_age_hours} hours. Consider a quick re-review.",
                "last_exported": last_exported
            }
            
        # 2. Artifact edit check
        artifacts = ats_db.get_tailored_artifacts_for_application(self.conn, application_id)
        for art in artifacts:
            art_dt = datetime.fromisoformat(art["updated_at"].replace("Z", "+00:00"))
            if art_dt > exp_dt:
                return {
                    "status": "outdated_after_edit", 
                    "reason": f"Tailored {art['artifact_type']} was edited after last export.",
                    "last_exported": last_exported,
                    "edit_time": art["updated_at"]
                }
                
        return {"status": "fresh", "reason": "Export package is current.", "last_exported": last_exported}
