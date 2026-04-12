import unittest
import sqlite3
import json
from unittest.mock import MagicMock, patch
from jobsearch.db.schema import init_db
from jobsearch import ats_db
from jobsearch.services.readiness_service import ReadinessService
from jobsearch.services.resume_renderer import ValidationIssue

class TestReadinessAndExport(unittest.TestCase):
    def setUp(self):
        self.conn = sqlite3.connect(":memory:")
        self.conn.row_factory = sqlite3.Row
        init_db(self.conn)
        ats_db.init_db(self.conn)
        
        # Setup mock application
        self.conn.execute(
            "INSERT INTO applications (id, scraper_key, company, role, status, created_at, updated_at) "
            "VALUES (1, 'key1', 'TestCo', 'Engineer', 'applied', '2026-01-01', '2026-01-01')"
        )
        self.conn.commit()
        self.service = ReadinessService(self.conn)

    def test_readiness_blocked_missing_resume(self):
        # No artifacts added yet
        state = self.service.evaluate(1)
        self.assertEqual(state.status, 'blocked')
        self.assertIn("Missing tailored resume", state.reason)

    def test_readiness_blocked_critical_issue(self):
        # Add resume JSON with missing name (critical)
        resume_data = {
            "header": {
                "name": "", 
                "email": "test@test.com",
                "phone": "123",
                "portfolio_url": "",
                "linkedin_url": ""
            },
            "experience": [{
                "role": "Dev", 
                "company": "Co", 
                "location": "NY",
                "dates": "2020",
                "bullets": ["B1"]
            }]
        }
        ats_db.add_tailored_artifact(self.conn, 1, "resume_json_warthog", json.dumps(resume_data))
        
        state = self.service.evaluate(1)
        self.assertEqual(state.status, 'blocked')
        self.assertIn("Name is missing", state.reason)

    def test_readiness_ready_full_package(self):
        # Add valid resume
        resume_data = {
            "header": {
                "name": "Alice", 
                "email": "test@test.com",
                "phone": "123",
                "portfolio_url": "",
                "linkedin_url": ""
            },
            "experience": [{
                "role": "Dev", 
                "company": "Co", 
                "location": "NY",
                "dates": "2020",
                "bullets": ["B1"]
            }]
        }
        ats_db.add_tailored_artifact(self.conn, 1, "resume_json_warthog", json.dumps(resume_data))
        # Add CL and Outreach
        ats_db.add_tailored_artifact(self.conn, 1, "cover_letter", "CL")
        ats_db.add_tailored_artifact(self.conn, 1, "outreach_note", "Outreach")
        
        state = self.service.evaluate(1)
        self.assertEqual(state.status, 'ready')
        self.assertTrue(state.has_resume)
        self.assertTrue(state.has_cover_letter)
        self.assertTrue(state.has_outreach)

    def test_readiness_draft_partial(self):
        # Valid resume but no other artifacts
        resume_data = {
            "header": {
                "name": "Alice", 
                "email": "test@test.com",
                "phone": "123",
                "portfolio_url": "",
                "linkedin_url": ""
            },
            "experience": [{
                "role": "Dev", 
                "company": "Co", 
                "location": "NY",
                "dates": "2020",
                "bullets": ["B1"]
            }]
        }
        ats_db.add_tailored_artifact(self.conn, 1, "resume_json_warthog", json.dumps(resume_data))
        
        state = self.service.evaluate(1)
        self.assertEqual(state.status, 'draft')

    def test_readiness_blocked_placeholder(self):
        # Resume with placeholder text [Company Name]
        resume_data = {
            "header": {
                "name": "Alice", 
                "email": "test@test.com",
                "phone": "123",
                "portfolio_url": "",
                "linkedin_url": ""
            },
            "summary": "I am excited to work at [Company Name].",
            "experience": [{
                "role": "Dev", "company": "Co", "location": "NY", "dates": "2020", "bullets": ["B1"]
            }]
        }
        ats_db.add_tailored_artifact(self.conn, 1, "resume_json_warthog", json.dumps(resume_data))
        
        state = self.service.evaluate(1)
        self.assertEqual(state.status, 'blocked')
        self.assertIn("placeholder", state.reason)

if __name__ == "__main__":
    unittest.main()
