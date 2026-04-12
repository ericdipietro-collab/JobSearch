import unittest
import sqlite3
import json
from pathlib import Path
from datetime import datetime, timedelta, timezone
from unittest.mock import MagicMock, patch

from jobsearch.scraper.ats_routing import FailureClassifier, fingerprint_ats
from jobsearch.scraper.adapters.generic import GenericAdapter
from jobsearch.scraper.engine import ScraperEngine
from jobsearch.db.schema import init_db
from jobsearch import ats_db
from deep_search.playwright_adapter import _collect_jobs_from_api_payload

FIXTURES_DIR = Path(__file__).parent / "fixtures"

class TestPlatformHardening(unittest.TestCase):
    def setUp(self):
        self.conn = sqlite3.connect(":memory:")
        self.conn.row_factory = sqlite3.Row
        init_db(self.conn)
        # Manually ensure ats_db init also runs if it has extra logic
        ats_db.init_db(self.conn)

    def test_failure_classification_blocked(self):
        html = (FIXTURES_DIR / "envestnet_blocked.html").read_text()
        cls = FailureClassifier.classify(status_code=403, html=html)
        self.assertEqual(cls, FailureClassifier.BLOCKED)

    def test_eightfold_api_parsing(self):
        payload = json.loads((FIXTURES_DIR / "eightfold_search.json").read_text())
        jobs = _collect_jobs_from_api_payload(payload, "https://example.eightfold.ai", "Test")
        self.assertEqual(len(jobs), 2)
        self.assertEqual(jobs[0]["title"], "Software Engineer")
        self.assertEqual(jobs[0]["location"], "New York, NY")
        self.assertIn("id=123", jobs[0]["url"])

    def test_phenom_ddo_parsing(self):
        html = (FIXTURES_DIR / "phenom_ddo.html").read_text()
        adapter = GenericAdapter(session=MagicMock(), scorer=MagicMock())
        jobs = adapter._extract_phenom_jobs(html, base_url="https://example.com", company_name="Test", tier=4)
        self.assertEqual(len(jobs), 1)
        self.assertEqual(jobs[0].role_title_raw, "Phenom Job 1")
        self.assertEqual(jobs[0].location, "Philadelphia, PA")

    def test_board_health_persistence(self):
        ats_db.update_board_health(
            self.conn,
            company="Test Company",
            adapter="generic",
            board_state="blocked_bot_protection",
            last_http_status=403,
            cooldown_until="2026-04-18"
        )
        health = ats_db.get_board_health(self.conn, "Test Company")
        self.assertIsNotNone(health)
        self.assertEqual(health["board_state"], "blocked_bot_protection")
        self.assertEqual(health["last_http_status"], 403)
        self.assertEqual(health["cooldown_until"], "2026-04-18")

    def test_application_metadata_persistence(self):
        from jobsearch.scraper.models import Job
        job = Job(
            id="test-job-1",
            company="Test Company",
            role_title_raw="Test Role",
            url="https://example.com/job/1",
            location="Remote",
            extraction_method="intercepted_api",
            extraction_confidence=0.85
        )
        from jobsearch.services.opportunity_service import upsert_job
        upsert_job(self.conn, job)
        
        row = self.conn.execute("SELECT extraction_method, extraction_confidence FROM applications WHERE scraper_key = ?", (job.id,)).fetchone()
        self.assertEqual(row["extraction_method"], "intercepted_api")
        self.assertEqual(row["extraction_confidence"], 0.85)

    def test_cooldown_logic_in_engine(self):
        engine = ScraperEngine(preferences={}, companies=[])
        company = {"name": "BlockedCo", "careers_url": "https://example.com/careers"}
        
        # Manually put on cooldown (far in the future)
        future_date = (datetime.now(timezone.utc) + timedelta(days=7)).isoformat()
        ats_db.update_board_health(
            self.conn,
            company="BlockedCo",
            cooldown_until=future_date,
            suppression_reason="blocked_bot_protection"
        )
        
        # Patch db.get_connection to return our in-memory conn for the engine's internal calls
        with patch("jobsearch.ats_db.get_connection", return_value=self.conn):
            with patch("jobsearch.db.connection.get_connection", return_value=self.conn):
                adapter, reason = engine._cooldown_reason(company)
                self.assertIn("Cooldown until", reason)
                self.assertIn("blocked_bot_protection", reason)

if __name__ == "__main__":
    unittest.main()
