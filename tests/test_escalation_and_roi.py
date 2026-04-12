import unittest
import sqlite3
import json
from datetime import datetime, timedelta, timezone
from unittest.mock import MagicMock

from jobsearch.db.schema import init_db
from jobsearch import ats_db
from jobsearch.scraper.scheduler_policy import EscalationPolicy
from jobsearch.scraper.ats_routing import FailureClassifier

class TestEscalationAndROI(unittest.TestCase):
    def setUp(self):
        self.conn = sqlite3.connect(":memory:")
        self.conn.row_factory = sqlite3.Row
        init_db(self.conn)
        ats_db.init_db(self.conn)
        
        self.settings = MagicMock()
        self.settings.health_escalation_window_days = 7
        self.settings.health_escalation_multiplier_2x = 2
        self.settings.health_escalation_multiplier_4x = 4
        self.settings.health_escalation_max_days = 30
        
        self.policy = EscalationPolicy(self.settings)

    def test_escalation_no_history(self):
        # First failure, no history
        days, reason = self.policy.calculate_cooldown(
            self.conn, "FirstCo", 7, FailureClassifier.BLOCKED
        )
        self.assertEqual(days, 7)
        self.assertEqual(reason, "")

    def test_escalation_repeat_failure(self):
        company = "RepeatCo"
        # Add one recent failure event (manually insert to avoid 4h dedupe)
        self.conn.execute(
            "INSERT INTO board_health_events (timestamp, company, to_state, trigger_subsystem) VALUES (?, ?, ?, ?)",
            ((datetime.now(timezone.utc) - timedelta(hours=1)).isoformat(), company, FailureClassifier.BLOCKED, "scrape_run")
        )
        
        days, reason = self.policy.calculate_cooldown(
            self.conn, company, 7, FailureClassifier.BLOCKED
        )
        self.assertEqual(days, 14) # 7 * 2
        self.assertIn("Escalated 2x", reason)

    def test_escalation_high_recurrence(self):
        company = "HighCo"
        # Add 3 recent failure events with different timestamps
        for i in range(1, 4):
            self.conn.execute(
                "INSERT INTO board_health_events (timestamp, company, to_state, trigger_subsystem) VALUES (?, ?, ?, ?)",
                ((datetime.now(timezone.utc) - timedelta(hours=i*5)).isoformat(), company, FailureClassifier.BLOCKED, "scrape_run")
            )
        
        days, reason = self.policy.calculate_cooldown(
            self.conn, company, 7, FailureClassifier.BLOCKED
        )
        self.assertEqual(days, 28) # 7 * 4
        self.assertIn("Escalated 4x", reason)

    def test_escalation_respects_cap(self):
        company = "CapCo"
        for i in range(1, 4):
            self.conn.execute(
                "INSERT INTO board_health_events (timestamp, company, to_state, trigger_subsystem) VALUES (?, ?, ?, ?)",
                ((datetime.now(timezone.utc) - timedelta(hours=i*5)).isoformat(), company, FailureClassifier.BLOCKED, "scrape_run")
            )
        
        # 14 * 4 = 56, should cap at 30
        days, reason = self.policy.calculate_cooldown(
            self.conn, company, 14, FailureClassifier.BLOCKED
        )
        self.assertEqual(days, 30)

    def test_healer_roi_metrics(self):
        # Recovery 1: Stale URL fixed
        ats_db.record_board_health_event(
            self.conn, "Co1", "healthy", "healer", 
            from_state="stale_url", adapter="greenhouse"
        )
        # Recovery 2: Blocked fixed
        ats_db.record_board_health_event(
            self.conn, "Co2", "healthy", "healer", 
            from_state="blocked_bot_protection", adapter="lever"
        )
        # Random non-recovery event
        ats_db.record_board_health_event(
            self.conn, "Co3", "broken", "scrape_run"
        )
        
        roi = ats_db.get_healer_roi_metrics(self.conn, days=30)
        self.assertEqual(roi["total_recoveries"], 2)
        self.assertEqual(roi["stale_recoveries"], 1)
        self.assertEqual(roi["blocked_recoveries"], 1)
        self.assertEqual(len(roi["by_family"]), 2)
        self.assertEqual(roi["manual_hours_saved"], 1.0) # 2 * 0.5

if __name__ == "__main__":
    unittest.main()
