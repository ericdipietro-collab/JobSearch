import unittest
import sqlite3
import json
from datetime import datetime, timedelta, timezone
from jobsearch.db.schema import init_db
from jobsearch import ats_db
from jobsearch.services.action_center_service import ActionCenterService
from jobsearch.services.readiness_service import ReadinessService
from jobsearch.services.submission_review_service import SubmissionReviewService

class TestActionCenterV2(unittest.TestCase):
    def setUp(self):
        self.conn = sqlite3.connect(":memory:")
        self.conn.row_factory = sqlite3.Row
        init_db(self.conn)
        ats_db.init_db(self.conn)
        
        self.readiness = ReadinessService(self.conn)
        self.submission = SubmissionReviewService(self.conn)
        self.service = ActionCenterService(self.conn, self.readiness, self.submission)

        # Setup base app
        self.conn.execute(
            "INSERT INTO applications (id, scraper_key, company, role, status, score, created_at, updated_at) "
            "VALUES (1, 'key1', 'RecoveryCo', 'Lead', 'prepared', 90, '2026-01-01', '2026-01-01')"
        )
        self.conn.commit()

    def test_revision_trigger(self):
        # Mark as Needs Revision via event
        ats_db.add_event(self.conn, 1, "note", "2026-01-01", title="Submission Blocked: Needs Revision")
        
        recs = self.service._rule_revision_and_recovery()
        rev_rec = next((r for r in recs if r.action_key == 'revision_required'), None)
        self.assertIsNotNone(rev_rec)
        self.assertEqual(rev_rec.urgency_score, 95)

    def test_outdated_export_trigger(self):
        # 1. Export 1 hour ago
        now = datetime.now(timezone.utc)
        exp_time = (now - timedelta(hours=1)).isoformat()
        self.conn.execute("UPDATE applications SET last_exported_at = ? WHERE id = 1", (exp_time,))
        
        # 2. Add artifact edited NOW
        ats_db.add_tailored_artifact(self.conn, 1, "resume_json_warthog", "{}")
        self.conn.execute("UPDATE tailored_artifacts SET updated_at = ?", (now.isoformat(),))
        self.conn.commit()
        
        recs = self.service._rule_revision_and_recovery()
        outdated = next((r for r in recs if r.action_key == 'reexport_required'), None)
        self.assertIsNotNone(outdated)
        self.assertEqual(outdated.urgency_score, 90)

    def test_stale_export_decay(self):
        # Export 48 hours ago
        stale_time = (datetime.now(timezone.utc) - timedelta(hours=48)).isoformat()
        self.conn.execute("UPDATE applications SET last_exported_at = ? WHERE id = 1", (stale_time,))
        self.conn.commit()
        
        recs = self.service._rule_revision_and_recovery()
        stale = next((r for r in recs if r.action_key == 'freshness_warning'), None)
        self.assertIsNotNone(stale)
        # 90 base impact - 10 decay = 80
        self.assertEqual(stale.impact_score, 80)

if __name__ == "__main__":
    unittest.main()
