import unittest
import sqlite3
from unittest.mock import MagicMock, patch
from jobsearch import ats_db
from jobsearch.services.ats_discovery_service import ATSDiscoveryService
from jobsearch.services.watch_service import WatchService

class TestV2Phase4(unittest.TestCase):
    def setUp(self):
        self.conn = sqlite3.connect(":memory:")
        self.conn.row_factory = sqlite3.Row
        ats_db.init_db(self.conn)

    def test_ats_candidate_review_transitions(self):
        discovery_service = ATSDiscoveryService(self.conn)
        
        # Add candidate
        cid = discovery_service.harvest_from_url("https://boards.greenhouse.io/test", company_hint="Test", source="manual")
        self.assertIsNotNone(cid)
        
        pending = discovery_service.get_pending_candidates()
        self.assertEqual(len(pending), 1)
        self.assertEqual(pending[0]['validation_status'], "pending")
        
        # Test approval transition
        ats_db.update_candidate_status(self.conn, cid, "approved")
        row = self.conn.execute("SELECT validation_status FROM ats_candidates WHERE id = ?", (cid,)).fetchone()
        self.assertEqual(row['validation_status'], "approved")

        # Test rejection transition
        ats_db.update_candidate_status(self.conn, cid, "rejected")
        row = self.conn.execute("SELECT validation_status FROM ats_candidates WHERE id = ?", (cid,)).fetchone()
        self.assertEqual(row['validation_status'], "rejected")

    def test_priority_watch_ui_state(self):
        watch_service = WatchService(self.conn)
        
        # Must insert into board_health to be watched
        self.conn.execute(
            "INSERT INTO board_health (company, updated_at) VALUES ('WatchCo', '2026-01-01')"
        )
        
        # Add to watchlist
        watch_service.add_to_watchlist("WatchCo")
        watchlist = watch_service.get_watchlist()
        self.assertEqual(len(watchlist), 1)
        self.assertEqual(watchlist[0]['company'], "WatchCo")
        self.assertEqual(watchlist[0]['is_watched'], 1)

if __name__ == "__main__":
    unittest.main()
