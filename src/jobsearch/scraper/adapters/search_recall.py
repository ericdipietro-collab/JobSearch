"""Search Recall adapter for broad discovery via JobSpy/Search Engines."""

from __future__ import annotations
import logging
from typing import List, Dict, Any, Optional
from datetime import datetime, timedelta

from .base import BaseAdapter
from jobsearch.scraper.models import Job
from jobsearch.scraper.normalization import SourceLaneRegistry

logger = logging.getLogger(__name__)

class SearchRecallAdapter(BaseAdapter):
    """Supplemental lane for broad search-engine based job recall."""

    def scrape(self, company_name: str, **kwargs) -> List[Job]:
        """
        Company-focused recall. 
        In a full implementation, this might run a 'site:careers.company.com' query.
        """
        # For now, we'll implement the query-based recall below.
        return []

    def scrape_recall(self, query: str, location: str = "", days_old: int = 3) -> List[Job]:
        """
        Runs a broad discovery query across many boards using JobSpy.
        """
        try:
            from jobspy import scrape_jobs
        except ImportError:
            logger.warning("jobspy not installed, SearchRecallAdapter disabled.")
            return []

        try:
            # Note: This is an expensive/noisy operation
            # We limit results to keep quality manageable
            df = scrape_jobs(
                site_name=["linkedin", "indeed", "glassdoor", "google"],
                search_term=query,
                location=location,
                results_wanted=20,
                hours_old=days_old * 24,
                country_indeed='USA'
            )
            
            if df.empty:
                return []
                
            return self.parse_df(df)
        except Exception as e:
            logger.error(f"SearchRecall failed for query '{query}': {e}")
            return []

    def parse_df(self, df: Any) -> List[Job]:
        jobs = []
        for _, row in df.iterrows():
            try:
                # Map JobSpy columns to our Job model
                job = Job(
                    company=str(row.get("company", "Unknown")),
                    role_title_raw=str(row.get("title", "Unknown")),
                    url=str(row.get("job_url", "")),
                    location=str(row.get("location", "")),
                    req_id=str(row.get("job_id", "") or ""),
                    source=f"recall_{row.get('site', 'unknown')}",
                    source_lane=SourceLaneRegistry.LANE_SEARCH_RECALL,
                    description_excerpt=str(row.get("description", ""))[:1000]
                )
                jobs.append(job)
            except Exception as e:
                logger.debug(f"Failed to parse recall row: {e}")
        return jobs
