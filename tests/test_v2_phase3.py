import unittest
import sqlite3
import json
from unittest.mock import MagicMock, patch
from jobsearch import ats_db
from jobsearch.services.watch_service import WatchService
from jobsearch.services.ats_discovery_service import ATSDiscoveryService
from jobsearch.scraper.models import Job
from jobsearch.scraper.normalization import SourceLaneRegistry

class TestV2Phase3(unittest.TestCase):
    def setUp(self):
        self.conn = sqlite3.connect(":memory:")
        self.conn.row_factory = sqlite3.Row
        ats_db.init_db(self.conn)

    def test_priority_watch_management(self):
        watch_service = WatchService(self.conn)
        
        # Add a company to board_health first (normally happens via scraper or healer)
        self.conn.execute(
            "INSERT INTO board_health (company, updated_at) VALUES ('WatchCo', '2026-01-01')"
        )
        
        watch_service.add_to_watchlist("WatchCo")
        watchlist = watch_service.get_watchlist()
        self.assertEqual(len(watchlist), 1)
        self.assertEqual(watchlist[0]['company'], "WatchCo")
        self.assertEqual(watchlist[0]['is_watched'], 1)
        
        watch_service.remove_from_watchlist("WatchCo")
        watchlist = watch_service.get_watchlist()
        self.assertEqual(len(watchlist), 0)

    def test_ats_discovery_harvester(self):
        discovery_service = ATSDiscoveryService(self.conn)
        
        # Test pattern matching and candidate creation
        url = "https://boards.greenhouse.io/vimeo"
        cid = discovery_service.harvest_from_url(url, company_hint="Vimeo", source="test")
        
        self.assertIsNotNone(cid)
        candidates = discovery_service.get_pending_candidates()
        self.assertEqual(len(candidates), 1)
        self.assertEqual(candidates[0]['ats_family_guess'], "greenhouse")
        self.assertEqual(candidates[0]['company_name'], "Vimeo")

    @patch("jobsearch.scraper.adapters.search_recall.SearchRecallAdapter.scrape_recall")
    def test_search_recall_processing(self, mock_recall):
        from jobsearch.scraper.engine import ScraperEngine
        
        # Mock SearchRecallAdapter output
        mock_recall.return_value = [Job(
            id="recall1",
            company="RecallCo",
            role_title_raw="Software Lead",
            url="https://jobs.lever.co/recallco/123",
            location="Remote",
            source="recall_linkedin",
            source_lane=SourceLaneRegistry.LANE_SEARCH_RECALL,
            description_excerpt="A job description needing Software Lead",
            salary_low=150000,
            salary_high=200000,
            tier=1,
            date_discovered="2026-01-01"
        )]
        
        prefs = {
            "scoring": {"minimum_score_to_keep": 10},
            "titles": {"positive_weights": [["Software Lead", 10]]},
            "keywords": {"body_positive": []}
        }
        
        engine = ScraperEngine(prefs, [], db_conn=self.conn)
        results = engine.run_search_recall(["Software Lead"])
        
        # Verify job was scored
        self.assertGreaterEqual(results['scored'], 1)
        
        # Verify harvester caught the candidate
        candidates = ats_db.get_pending_candidates(self.conn)
        self.assertEqual(len(candidates), 1)
        self.assertEqual(candidates[0]['company_name'], "RecallCo")
        self.assertEqual(candidates[0]['ats_family_guess'], "lever")

if __name__ == "__main__":
    unittest.main()
