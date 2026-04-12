import unittest
import sqlite3
import json
from datetime import datetime, timezone
from jobsearch.db.schema import init_db
from jobsearch import ats_db
from jobsearch.services.submission_review_service import SubmissionReviewService

class TestSubmissionReview(unittest.TestCase):
    def setUp(self):
        self.conn = sqlite3.connect(":memory:")
        self.conn.row_factory = sqlite3.Row
        init_db(self.conn)
        ats_db.init_db(self.conn)
        
        # Setup mock application
        self.conn.execute(
            "INSERT INTO applications (id, scraper_key, company, role, status, job_url, created_at, updated_at) "
            "VALUES (1, 'key1', 'SubmitCo', 'Reviewer', 'considering', 'https://apply.com', '2026-01-01', '2026-01-01')"
        )
        self.conn.commit()
        self.service = SubmissionReviewService(self.conn)

    def test_queue_filtering(self):
        # Initial: should be in queue
        queue = self.service.get_queue()
        self.assertEqual(len(queue), 1)
        self.assertEqual(queue[0].company, "SubmitCo")

    def test_mark_prepared(self):
        self.service.mark_prepared(1)
        app = ats_db.get_application(self.conn, 1)
        self.assertEqual(app['status'], 'prepared')
        
        # Verify event
        events = ats_db.get_events(self.conn, 1)
        self.assertTrue(any("Prepared" in e['title'] for e in events))

    def test_mark_submitted(self):
        self.service.mark_submitted(1, notes="Clicked apply button.")
        app = ats_db.get_application(self.conn, 1)
        self.assertEqual(app['status'], 'applied')
        self.assertIsNotNone(app['date_applied'])

    def test_log_blocker(self):
        self.service.log_blocker(1, "link_broken", "Page not found.")
        
        # Verify event recorded
        events = ats_db.get_events(self.conn, 1)
        # Reason title is "Submission Blocked: Closed posting / bad link"
        self.assertTrue(any("Closed posting / bad link" in e['title'] for e in events))

    def test_checklist_generation(self):
        # Add valid resume JSON to pass that check
        resume_data = {
            "header": {"name": "A", "email": "a@b.com", "phone": "1", "portfolio_url": "", "linkedin_url": ""},
            "experience": [{"role": "R", "company": "C", "location": "L", "dates": "D", "bullets": ["B"]}]
        }
        ats_db.add_tailored_artifact(self.conn, 1, "resume_json_warthog", json.dumps(resume_data))
        
        queue = self.service.get_queue()
        item = queue[0]
        
        # Find resume check
        resume_check = next(c for c in item.checklist if "Resume" in c.label)
        self.assertTrue(resume_check.status)
        
        # Find link check
        link_check = next(c for c in item.checklist if "Link" in c.label)
        self.assertTrue(link_check.status)

if __name__ == "__main__":
    unittest.main()
