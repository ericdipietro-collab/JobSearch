import unittest
import sqlite3
import json
from datetime import datetime, timedelta, timezone
from unittest.mock import MagicMock

from jobsearch.db.schema import init_db
from jobsearch import ats_db
from jobsearch.services.learning_loop_service import LearningLoopService
from jobsearch.services.submission_review_service import SubmissionReviewService

class TestFrictionAndFreshness(unittest.TestCase):
    def setUp(self):
        self.conn = sqlite3.connect(":memory:")
        self.conn.row_factory = sqlite3.Row
        init_db(self.conn)
        ats_db.init_db(self.conn)
        
        # Setup mock application
        self.conn.execute(
            "INSERT INTO applications (id, scraper_key, company, role, status, extraction_method, created_at, updated_at) "
            "VALUES (1, 'key1', 'FrictionCo', 'Lead', 'prepared', 'direct_api', '2026-01-01', '2026-01-01')"
        )
        self.conn.commit()

    def test_friction_metrics(self):
        # Record a blocker event
        ats_db.add_event(self.conn, 1, "note", "2026-01-01", 
                         title="Submission Blocked: friction_portal", notes="Way too many questions.")
        
        service = LearningLoopService(self.conn)
        stats = service.get_submission_friction_stats()
        self.assertEqual(len(stats), 1)
        self.assertEqual(stats.iloc[0]['reason'], "friction_portal")
        
        report = service.get_ats_friction_report()
        self.assertEqual(len(report), 1)
        self.assertEqual(report.iloc[0]['extraction_method'], "direct_api")

    def test_freshness_stale(self):
        # Mark as exported 48 hours ago
        stale_time = (datetime.now(timezone.utc) - timedelta(hours=48)).isoformat()
        self.conn.execute("UPDATE applications SET last_exported_at = ? WHERE id = 1", (stale_time,))
        self.conn.commit()
        
        service = SubmissionReviewService(self.conn)
        res = service.evaluate_freshness(1)
        self.assertEqual(res["status"], "stale")
        self.assertIn("older than 24 hours", res["reason"])

    def test_freshness_outdated_after_edit(self):
        # 1. Export 1 hour ago
        export_time = (datetime.now(timezone.utc) - timedelta(hours=1)).isoformat()
        self.conn.execute("UPDATE applications SET last_exported_at = ? WHERE id = 1", (export_time,))
        
        # 2. Update artifact NOW
        ats_db.add_tailored_artifact(self.conn, 1, "cover_letter", "New content")
        # Ensure updated_at is definitely after export_time
        self.conn.execute("UPDATE tailored_artifacts SET updated_at = ?", (datetime.now(timezone.utc).isoformat(),))
        self.conn.commit()
        
        service = SubmissionReviewService(self.conn)
        res = service.evaluate_freshness(1)
        self.assertEqual(res["status"], "outdated_after_edit")
        self.assertIn("edited after last export", res["reason"])

    def test_freshness_missing(self):
        # No export at all
        service = SubmissionReviewService(self.conn)
        res = service.evaluate_freshness(1)
        self.assertEqual(res["status"], "missing_export")

if __name__ == "__main__":
    unittest.main()
