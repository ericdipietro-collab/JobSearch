import unittest
import sqlite3
import json
from datetime import datetime, timedelta, timezone
from jobsearch.db.schema import init_db
from jobsearch import ats_db

class TestBoardHistory(unittest.TestCase):
    def setUp(self):
        self.conn = sqlite3.connect(":memory:")
        self.conn.row_factory = sqlite3.Row
        init_db(self.conn)
        ats_db.init_db(self.conn)

    def test_record_board_health_event(self):
        ats_db.record_board_health_event(
            self.conn,
            company="TestCo",
            to_state="blocked_bot_protection",
            trigger_subsystem="scrape_run",
            from_state="healthy",
            reason="Cloudflare 403"
        )
        
        events = ats_db.get_board_health_history(self.conn, "TestCo")
        self.assertEqual(len(events), 1)
        self.assertEqual(events[0]["to_state"], "blocked_bot_protection")
        self.assertEqual(events[0]["trigger_subsystem"], "scrape_run")

    def test_event_deduplication(self):
        # Record first event
        ats_db.record_board_health_event(
            self.conn,
            company="DedupeCo",
            to_state="broken_site",
            trigger_subsystem="scrape_run"
        )
        
        # Record same event immediately (should be deduped)
        ats_db.record_board_health_event(
            self.conn,
            company="DedupeCo",
            to_state="broken_site",
            trigger_subsystem="scrape_run"
        )
        
        events = ats_db.get_board_health_history(self.conn, "DedupeCo")
        self.assertEqual(len(events), 1)

    def test_healer_always_records(self):
        # Healer events shouldn't be deduped as aggressively for visibility
        ats_db.record_board_health_event(
            self.conn,
            company="HealCo",
            to_state="healthy",
            trigger_subsystem="healer"
        )
        ats_db.record_board_health_event(
            self.conn,
            company="HealCo",
            to_state="healthy",
            trigger_subsystem="healer"
        )
        
        events = ats_db.get_board_health_history(self.conn, "HealCo")
        self.assertEqual(len(events), 2)

    def test_update_board_health_triggers_event(self):
        # Initial create
        ats_db.update_board_health(
            self.conn,
            company="TransitionCo",
            board_state="healthy",
            trigger_subsystem="scrape_run"
        )
        
        # State change
        ats_db.update_board_health(
            self.conn,
            company="TransitionCo",
            board_state="blocked_bot_protection",
            trigger_subsystem="scrape_run"
        )
        
        events = ats_db.get_board_health_history(self.conn, "TransitionCo")
        self.assertEqual(len(events), 2)
        self.assertEqual(events[0]["to_state"], "blocked_bot_protection")
        self.assertEqual(events[0]["from_state"], "healthy")

    def test_analytics_queries(self):
        # Add some mock history
        ats_db.record_board_health_event(self.conn, "FlapCo", "healthy", "scrape_run")
        ats_db.record_board_health_event(self.conn, "FlapCo", "broken", "scrape_run")
        ats_db.record_board_health_event(self.conn, "FlapCo", "healthy", "healer")
        
        ats_db.record_board_health_event(self.conn, "StableCo", "healthy", "scrape_run")
        
        flapping = ats_db.get_flapping_boards(self.conn)
        self.assertEqual(len(flapping), 1)
        self.assertEqual(flapping[0]["company"], "FlapCo")
        
        recoveries = ats_db.get_healer_recoveries(self.conn)
        self.assertEqual(len(recoveries), 1)
        self.assertEqual(recoveries[0]["company"], "FlapCo")

if __name__ == "__main__":
    unittest.main()
