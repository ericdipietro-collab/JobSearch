"""Priority Watch service for high-frequency monitoring of top companies."""

from __future__ import annotations
import logging
import sqlite3
from typing import List, Dict, Any, Optional
from datetime import datetime, timezone

from jobsearch import ats_db
from jobsearch.config.settings import settings
from jobsearch.scraper.engine import ScraperEngine

logger = logging.getLogger(__name__)

class WatchService:
    """Service for high-frequency polling of priority companies."""

    def __init__(self, conn: sqlite3.Connection):
        self.conn = conn

    def get_watchlist(self) -> List[Dict[str, Any]]:
        """Returns the current list of watched companies from board_health."""
        rows = ats_db.get_watched_companies(self.conn)
        return [dict(r) for r in rows]

    def add_to_watchlist(self, company: str) -> None:
        """Adds a company to the watchlist."""
        ats_db.set_company_watch(self.conn, company, True)

    def remove_from_watchlist(self, company: str) -> None:
        """Removes a company from the watchlist."""
        ats_db.set_company_watch(self.conn, company, False)

    def poll_watched(self, preferences: Dict[str, Any], companies_registry: List[Dict[str, Any]]) -> Dict[str, Any]:
        """
        Runs a focused scrape for all watched companies.
        Surface only watched companies that exist in the provided registry.
        """
        watchlist = self.get_watchlist()
        watch_names = {w['company'] for w in watchlist}
        
        # Filter registry to only include watched companies
        targets_to_poll = [
            c for c in companies_registry 
            if c['name'] in watch_names
        ]
        
        if not targets_to_poll:
            return {"status": "empty", "message": "No watched companies found in active registry."}

        logger.info(f"Priority Watch: Polling {len(targets_to_poll)} companies...")
        
        # Use ScraperEngine to run the focused scrape
        # Note: ScraperEngine expects the full registry, but we pass only targets
        engine = ScraperEngine(preferences, targets_to_poll, full_refresh=False)
        engine.run(max_workers=min(len(targets_to_poll), 8))
        
        return {
            "status": "success",
            "polled_count": len(targets_to_poll),
            "timestamp": datetime.now(timezone.utc).isoformat()
        }
