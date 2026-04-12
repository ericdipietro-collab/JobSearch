"""ATS Registry Discovery service for identifying potential board roots."""

from __future__ import annotations
import logging
import sqlite3
import re
from typing import List, Dict, Any, Optional
from urllib.parse import urlparse

from jobsearch import ats_db

logger = logging.getLogger(__name__)

class ATSDiscoveryService:
    """Service for discovering and normalizing potential ATS board roots."""

    # Patterns for detecting ATS families from URLs
    ATS_PATTERNS = {
        "greenhouse": [r"boards\.greenhouse\.io/([^/]+)"],
        "lever": [r"jobs\.lever\.co/([^/]+)"],
        "ashby": [r"jobs\.ashbyhq\.com/([^/]+)"],
        "workday": [r"([^.]+)\.myworkdayjobs\.com/([^/]+)"],
        "smartrecruiters": [r"smartrecruiters\.com/([^/]+)"],
        "rippling": [r"([^.]+)\.rippling-ats\.com/([^/]+)"],
        "jobvite": [r"jobs\.jobvite\.com/([^/]+)"],
        "bamboohr": [r"([^.]+)\.bamboohr\.com/jobs"],
        "workable": [r"([^.]+)\.workable\.com"],
        "breezy": [r"([^.]+)\.breezy\.hr"],
    }

    def __init__(self, conn: sqlite3.Connection):
        self.conn = conn

    def harvest_from_url(self, url: str, company_hint: Optional[str] = None, source: str = "manual") -> Optional[int]:
        """
        Analyzes a URL to see if it matches known ATS patterns.
        If it does, it normalizes the root and adds it as a candidate.
        """
        url_lower = url.lower()
        
        for family, patterns in self.ATS_PATTERNS.items():
            for pattern in patterns:
                match = re.search(pattern, url_lower)
                if match:
                    # Normalize root (keep the match part)
                    # e.g. https://jobs.lever.co/company -> https://jobs.lever.co/company
                    # We'll just store the full URL as candidate_url and the guess.
                    
                    confidence = 0.9 # High confidence if pattern matches
                    rationale = f"Matched pattern for {family} in URL."
                    
                    return ats_db.add_ats_candidate(
                        self.conn,
                        company_name=company_hint or match.group(1),
                        candidate_url=url,
                        normalized_root=url, # Basic normalization for now
                        ats_family_guess=family,
                        confidence=confidence,
                        source=source,
                        rationale=rationale
                    )
        
        return None

    def process_recall_results(self, jobs: List[Dict[str, Any]]) -> int:
        """
        Processes jobs found in Search Recall to discover new ATS roots.
        """
        found_count = 0
        for job in jobs:
            url = job.get("url")
            if not url: continue
            
            candidate_id = self.harvest_from_url(
                url, 
                company_hint=job.get("company"),
                source="search_recall"
            )
            if candidate_id:
                found_count += 1
                
        return found_count

    def get_pending_candidates(self) -> List[Dict[str, Any]]:
        """Returns pending candidates for review/validation."""
        rows = ats_db.get_pending_candidates(self.conn)
        return [dict(r) for r in rows]
