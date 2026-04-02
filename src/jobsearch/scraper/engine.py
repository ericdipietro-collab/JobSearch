"""Scraper Engine: Orchestrates multi-threaded scraping and persistence."""

import time
import random
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import List, Dict, Any, Type
import requests

from .adapters.base import BaseAdapter
from .adapters.greenhouse import GreenhouseAdapter
from .adapters.lever import LeverAdapter
from .adapters.ashby import AshbyAdapter
from .adapters.workday import WorkdayAdapter
from .adapters.generic import GenericAdapter
from .adapters.rippling import RipplingAdapter
from .scoring import Scorer
from ..db.connection import get_connection
from ..services.opportunity_service import upsert_job
from ..config.settings import settings

logger = logging.getLogger(__name__)

class ScraperEngine:
    ADAPTER_MAP: Dict[str, Type[BaseAdapter]] = {
        "greenhouse": GreenhouseAdapter,
        "lever": LeverAdapter,
        "ashby": AshbyAdapter,
        "workday": WorkdayAdapter,
        "rippling": RipplingAdapter,
        "generic": GenericAdapter,
        "custom_manual": GenericAdapter,
    }

    def __init__(self, preferences: Dict[str, Any], companies: List[Dict[str, Any]], deep_search: bool = False):
        self.prefs = preferences
        self.companies = [c for c in companies if c.get("active", True)]
        self.scorer = Scorer(preferences)
        self.deep_search = deep_search
        self.session = requests.Session()
        
        # Modern Browser Headers
        self.session.headers.update({
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.9",
            "Accept-Encoding": "gzip, deflate, br",
            "DNT": "1",
            "Connection": "keep-alive",
            "Upgrade-Insecure-Requests": "1",
        })

    def run(self, max_workers: int = 5):
        """Execute the full scraping pipeline."""
        logger.info(f"🚀 Starting scraper engine with {len(self.companies)} companies...")
        
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = {
                executor.submit(self._scrape_company_with_retry, company): company 
                for company in self.companies
            }
            
            for future in as_completed(futures):
                company = futures[future]
                try:
                    jobs = future.result()
                    self._process_and_save_jobs(company, jobs)
                except Exception as e:
                    logger.error(f"❌ Failed to scrape {company.get('name')}: {e}")

    def _scrape_company_with_retry(self, company: Dict[str, Any]) -> List[Any]:
        """Scrape company with jitter and optional browser fallback."""
        # Random jitter between companies to avoid burst patterns
        time.sleep(random.uniform(1.0, 5.0))
        
        try:
            jobs = self._scrape_company(company)
            if not jobs and self.deep_search:
                # If static failed, try deep search if enabled
                return self._deep_scrape(company)
            return jobs
        except Exception as e:
            if "403" in str(e) or "blocked" in str(e).lower():
                if self.deep_search:
                    logger.info(f"⚠️ Static scrape blocked for {company.get('name')}. Attempting deep search...")
                    return self._deep_scrape(company)
            raise e

    def _scrape_company(self, company: Dict[str, Any]) -> List[Any]:
        adapter_name = company.get("adapter", "custom_manual").lower()
        careers_url = company.get("careers_url", "").lower()
        
        # Auto-detect adapter if it looks like a standard one but isn't set
        if adapter_name == "custom_manual" or not adapter_name:
            if "greenhouse.io" in careers_url: adapter_name = "greenhouse"
            elif "lever.co" in careers_url: adapter_name = "lever"
            elif "ashbyhq.com" in careers_url: adapter_name = "ashby"
            elif "myworkdayjobs.com" in careers_url: adapter_name = "workday"
            elif "rippling.com" in careers_url: adapter_name = "rippling"
            else: adapter_name = "generic"

        adapter_cls = self.ADAPTER_MAP.get(adapter_name, GenericAdapter)
        adapter = adapter_cls(session=self.session)
        return adapter.scrape(company)

    def _deep_scrape(self, company: Dict[str, Any]) -> List[Any]:
        """Browser-based fallback for jobs."""
        try:
            from deep_search import playwright_adapter
            if not playwright_adapter.is_available():
                return []
            
            # This would call a method in playwright_adapter designed for full listing scraping
            # For now, we return empty until we implement the full listing bridge
            return []
        except ImportError:
            return []

    def _process_and_save_jobs(self, company: Dict[str, Any], jobs: List[Any]):
        """Score jobs and persist them to the unified database."""
        if not jobs:
            return

        conn = get_connection()
        saved_count = 0
        
        for job in jobs:
            # Prepare scoring data
            scoring_data = {
                "title": job.role_title_raw,
                "description": job.description_excerpt,
                "tier": int(company.get("tier", 4)),
                "location": job.location,
            }
            
            # Run Scorer
            score_results = self.scorer.score_job(scoring_data)
            
            # Update job object with score results
            job.score = score_results["score"]
            job.fit_band = score_results["fit_band"]
            job.matched_keywords = score_results["matched_keywords"]
            job.penalized_keywords = score_results["penalized_keywords"]
            job.decision_reason = score_results["decision_reason"]
            
            # Persist via service
            try:
                if upsert_job(conn, job):
                    saved_count += 1
            except Exception as e:
                logger.error(f"Error saving job {job.id}: {e}")
        
        conn.close()
        if saved_count > 0:
            print(f"✅ {company.get('name')}: Saved {saved_count} new jobs out of {len(jobs)} found.")
