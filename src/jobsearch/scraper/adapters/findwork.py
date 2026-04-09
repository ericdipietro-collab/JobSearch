import logging
import re
from typing import List, Dict, Any, Optional
from .base import BaseAdapter
from jobsearch.scraper.models import Job
from jobsearch.config.settings import get_headers

logger = logging.getLogger(__name__)

class FindworkAdapter(BaseAdapter):
    """
    Adapter for the Findwork.dev API.
    Niche aggregator for tech-only roles.
    """
    
    BASE_URL = "https://findwork.dev/api/jobs"

    def scrape(self, company_config: Dict[str, Any]) -> List[Job]:
        """
        Scrapes jobs from Findwork.dev.
        Example queries: 'product manager', 'software engineer'.
        """
        query = company_config.get("query", "product manager")
        
        # Build request URL
        url = f"{self.BASE_URL}/?search={query}&sort_by=relevance"
        
        headers = get_headers()
        # Findwork.dev requires a specific Accept header for their API
        headers["Accept"] = "application/json"

        try:
            import requests
            response = requests.get(url, headers=headers, timeout=20)
            response.raise_for_status()
            data = response.json()
            
            raw_jobs = data.get("results", [])
            if not raw_jobs:
                logger.info(f"Findwork: No jobs found for query '{query}'")
                return []

            jobs = []
            for rj in raw_jobs:
                # Findwork specific mapping
                job = Job(
                    company=rj.get("company_name", "Unknown"),
                    role=rj.get("role", "Unknown Position"),
                    url=rj.get("url", ""),
                    location=rj.get("location", "Remote"),
                    description_excerpt=rj.get("text", ""),
                    salary_text="", # Not always available in structured form
                    source_lane="aggregator",
                    adapter="findwork",
                    tier=str(company_config.get("tier", 4))
                )
                # Cleanup HTML if present
                if job.description_excerpt:
                    job.description_excerpt = re.sub(r'<[^>]*>', '', job.description_excerpt)
                
                jobs.append(job)
                
            return jobs

        except Exception as e:
            logger.error(f"Findwork scrape failed for query '{query}': {e}")
            return []
