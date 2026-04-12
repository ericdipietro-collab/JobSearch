import unittest
import sqlite3
import json
from datetime import datetime, timedelta, timezone
from jobsearch.db.schema import init_db
from jobsearch import ats_db
from jobsearch.services.action_center_service import ActionCenterService

class TestActionCenter(unittest.TestCase):
    def setUp(self):
        self.conn = sqlite3.connect(":memory:")
        self.conn.row_factory = sqlite3.Row
        init_db(self.conn)
        ats_db.init_db(self.conn)
        self.service = ActionCenterService(self.conn)

    def test_apply_now_recommendation(self):
        # Create a high-score job found 2 days ago
        now = datetime.now(timezone.utc).isoformat()
        discovered = (datetime.now(timezone.utc) - timedelta(days=2)).isoformat()
        self.conn.execute(
            "INSERT INTO applications (scraper_key, company, role, score, fit_band, status, date_discovered, created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
            ('key1', 'Google', 'SRE', 95, 'APPLY NOW', 'considering', discovered, now, now)
        )
        
        recs = self.service._rule_apply_now()
        self.assertEqual(len(recs), 1)
        self.assertEqual(recs[0].action_key, 'apply')
        self.assertEqual(recs[0].company, 'Google')

    def test_follow_up_recommendation(self):
        # Applied 10 days ago, no follow-up date set
        now = datetime.now(timezone.utc).isoformat()
        # Use 11 days ago to ensure we get at least 10 days back from the calculation
        applied = (datetime.now(timezone.utc) - timedelta(days=11)).isoformat()
        self.conn.execute(
            "INSERT INTO applications (scraper_key, company, role, status, date_applied, created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?)",
            ('key2', 'Meta', 'SWE', 'applied', applied, now, now)
        )
        
        recs = self.service._rule_follow_ups()
        self.assertEqual(len(recs), 1)
        self.assertEqual(recs[0].action_key, 'follow_up')
        # Check for 10 or 11 days depending on exact clock time
        self.assertTrue("10 days ago" in recs[0].reason or "11 days ago" in recs[0].reason)

    def test_interview_prep_recommendation(self):
        # Interview tomorrow
        now = datetime.now(timezone.utc).isoformat()
        tomorrow = (datetime.now(timezone.utc) + timedelta(days=1)).isoformat()
        self.conn.execute(
            "INSERT INTO applications (id, scraper_key, company, role, status, created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?)",
            (1, 'key3', 'Apple', 'PM', 'interviewing', now, now)
        )
        self.conn.execute(
            "INSERT INTO interviews (application_id, scheduled_at, created_at, updated_at) VALUES (?, ?, ?, ?)",
            (1, tomorrow, now, now)
        )
        
        recs = self.service._rule_interview_prep()
        self.assertEqual(len(recs), 1)
        self.assertEqual(recs[0].action_key, 'prep')

    def test_snooze_behavior(self):
        rec = MagicMock()
        rec.entity_type = 'job'
        rec.entity_id = 'key1'
        rec.action_key = 'apply'
        
        # Snooze for 3 days
        self.service.snooze_action(rec, days=3)
        
        # Should be in user_actions
        actions = ats_db.get_user_actions(self.conn)
        self.assertEqual(len(actions), 0) # Because it's snoozed and get_user_actions respects snooze by default
        
        # But should exist in raw table
        row = self.conn.execute("SELECT * FROM user_actions").fetchone()
        self.assertIsNotNone(row)
        self.assertEqual(row['status'], 'active')
        self.assertIsNotNone(row['snoozed_until'])

from unittest.mock import MagicMock

if __name__ == "__main__":
    unittest.main()
