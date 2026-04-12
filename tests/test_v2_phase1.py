import unittest
import sqlite3
import json
from unittest.mock import MagicMock, patch
from jobsearch.db.schema import init_db
from jobsearch import ats_db
from jobsearch.services.evaluation_service import EvaluationService
from jobsearch.services.health_monitor import HealthMonitor
from jobsearch.scraper.models import Job

class TestV2Phase1(unittest.TestCase):
    def setUp(self):
        self.conn = sqlite3.connect(":memory:")
        self.conn.row_factory = sqlite3.Row
        ats_db.init_db(self.conn)
        
        self.prefs = {
            "scoring": {"min_score_to_keep": 5},
            "titles": {"positive_weights": [["Python Manager", 10]]},
            "keywords": {"body_positive": [["aws", 5]]}
        }

    def test_evaluation_service_scores_pending_jobs(self):
        # 1. Insert unscored job
        self.conn.execute(
            "INSERT INTO applications (scraper_key, company, role, description_excerpt, status, score, fit_band, "
            "salary_low, salary_high, location, tier, source_lane, date_discovered, created_at, updated_at) "
            "VALUES ('key1', 'TestCo', 'Python Manager', 'Needs python and aws experts', 'considering', 0.0, 'Pending Evaluation', "
            "170000, 200000, 'Remote', 1, 'employer_ats', '2026-01-01', '2026-01-01', '2026-01-01')"
        )
        self.conn.commit()
        
        service = EvaluationService(self.prefs)
        count, rejected = service.evaluate_pending_jobs(self.conn)
        
        self.assertEqual(count, 1)
        self.assertEqual(len(rejected), 0)
        
        # Verify score is updated
        row = self.conn.execute("SELECT score, fit_band FROM applications WHERE scraper_key = 'key1'").fetchone()
        self.assertGreater(row['score'], 0.0)
        self.assertNotEqual(row['fit_band'], 'Pending Evaluation')

    def test_evaluation_service_rejects_low_scores(self):
        # Insert job that will score 0
        self.conn.execute(
            "INSERT INTO applications (scraper_key, company, role, status, score, fit_band, created_at, updated_at) "
            "VALUES ('key2', 'BadCo', 'Janitor', 'considering', 0.0, 'Pending Evaluation', '2026-01-01', '2026-01-01')"
        )
        self.conn.commit()
        
        service = EvaluationService(self.prefs)
        count, rejected = service.evaluate_pending_jobs(self.conn)
        
        self.assertEqual(count, 0)
        self.assertEqual(len(rejected), 1)
        self.assertEqual(rejected[0]['company'], 'BadCo')
        
        # Verify deleted from DB
        row = self.conn.execute("SELECT count(*) FROM applications WHERE scraper_key = 'key2'").fetchone()
        self.assertEqual(row[0], 0)

    def test_health_monitor_centralized_write(self):
        policy = MagicMock()
        policy.calculate_cooldown.return_value = (7, "")
        monitor = HealthMonitor(policy)
        
        company = {"name": "HealthCo", "careers_url": "https://health.com"}
        
        # Simulate a block
        monitor.update_scrape_health(
            self.conn, company, "generic", "blocked", 100.0, "Blocked by CF", 0,
            status_code=403, html="Cloudflare access denied"
        )
        
        # Verify board_health table
        health = self.conn.execute("SELECT board_state, cooldown_until FROM board_health WHERE company = 'HealthCo'").fetchone()
        self.assertEqual(health['board_state'], 'blocked_bot_protection')
        self.assertIsNotNone(health['cooldown_until'])

if __name__ == "__main__":
    unittest.main()
