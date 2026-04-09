import logging
import re
from typing import List, Dict, Any, Optional
from urllib.parse import quote

from .base import BaseAdapter
from jobsearch.scraper.models import Job
from jobsearch.config.settings import get_headers

logger = logging.getLogger(__name__)

class RemotiveAdapter(BaseAdapter):
    """
    Adapter for the Remotive.com API.
    Provides access to high-quality, remote-only tech job listings.
    """
    
    BASE_URL = "https://remotive.com/api/remote-jobs"

    def scrape(self, company_config: Dict[str, Any]) -> List[Job]:
        """
        Scrapes jobs from Remotive API.
        We fetch the full list and filter locally for better reliability.
        """
        target_category = company_config.get("category", "product").lower()
        url = self.BASE_URL # Get all recent jobs
        
        headers = get_headers()
        headers["Accept"] = "application/json"

        try:
            import requests
            response = requests.get(url, headers=headers, timeout=20)
            response.raise_for_status()
            data = response.json()
            
            raw_jobs = data.get("jobs", [])
            if not raw_jobs:
                return []

            jobs = []
            for rj in raw_jobs:
                # Local filter by category
                job_category = str(rj.get("category", "")).lower()
                if target_category not in job_category and target_category != "all":
                    continue

                company = rj.get("company_name", "Unknown")
                role = rj.get("title", "Unknown Position")
                url = rj.get("url", "")

                job = Job(
                    id=Job.make_id(company, role, url),
                    company=company,
                    role_title_raw=role,
                    url=url,
                    location="Remote",
                    description_excerpt=rj.get("description", ""),
                    salary_text=rj.get("salary", ""), 
                    source_lane="aggregator",
                    adapter="remotive",
                    tier=str(company_config.get("tier", 4))
                )
                # Cleanup HTML from description if present
                if job.description_excerpt:
                    job.description_excerpt = re.sub(r'<[^>]*>', '', job.description_excerpt)
                
                jobs.append(job)
                
            return jobs

        except Exception as e:
            logger.error(f"Remotive scrape failed for category '{category}': {e}")
            return []
