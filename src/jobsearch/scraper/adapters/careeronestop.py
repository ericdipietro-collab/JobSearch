import logging
import re
from typing import List, Dict, Any, Optional
from urllib.parse import quote

from .base import BaseAdapter
from jobsearch.scraper.models import Job
from jobsearch.config.settings import settings, get_headers

logger = logging.getLogger(__name__)

class CareerOneStopAdapter(BaseAdapter):
    """
    Adapter for the CareerOneStop (National Labor Exchange) API.
    Provides access to verified, quality-controlled job listings.
    """
    
    BASE_URL = "https://api.careeronestop.org/v2/jobsearch"

    def scrape(self, company_config: Dict[str, Any]) -> List[Job]:
        """
        Scrapes jobs from CareerOneStop V2.
        """
        user_id = settings.careeronestop_userid
        token = settings.careeronestop_token
        
        if not user_id or not token:
            logger.error("CareerOneStop API credentials missing")
            return []

        # Use company name as search keyword
        keyword = company_config.get("name", "")
        if not keyword:
            return []

        # Build request URL (V2 Path)
        # /{userId}/{keyword}/{location}/{radius}/{sortColumns}/{sortOrder}/{startRecord}/{pageSize}/{days}
        location = "US" 
        radius = "0"
        sort_col = "0"
        sort_order = "0"
        start_item = "0" # 0-based index as per explorer
        page_size = "50"
        days = "7"
        
        url = f"{self.BASE_URL}/{user_id}/{quote(keyword)}/{location}/{radius}/{sort_col}/{sort_order}/{start_item}/{page_size}/{days}"
        
        # V2 Docs suggest query params for snippet
        params = {
            "showFilters": "false",
            "enableJobDescriptionSnippet": "true",
            "enableMetaData": "false"
        }
        
        headers = get_headers()
        headers["Authorization"] = f"Bearer {token}"
        headers["Accept"] = "application/json"
        headers["Content-Type"] = "application/json"

        try:
            import requests
            response = requests.get(url, headers=headers, params=params, timeout=20)
            
            if response.status_code == 401:
                logger.error("CareerOneStop API: Unauthorized. Check your Token.")
                return []
            
            response.raise_for_status()
            data = response.json()
            
            raw_jobs = data.get("Jobs", [])
            if not raw_jobs:
                logger.info(f"CareerOneStop: No jobs found for '{keyword}'")
                return []

            jobs = []
            for rj in raw_jobs:
                # CareerOneStop specific mapping
                job_id = rj.get("JvId")
                if not job_id: continue
                
                # Check if this is a direct employer post or third party
                is_direct = rj.get("Company", "").lower() not in ["adzuna", "indeed", "ziprecruiter"]
                
                job = Job(
                    company=rj.get("Company", "Unknown"),
                    role=rj.get("JobTitle", "Unknown Position"),
                    url=rj.get("URL", ""),
                    location=rj.get("Location", ""),
                    # We store the JvId in description_excerpt initially so enrichment 
                    # can fetch the full JD later if needed, or we use the snippet.
                    description_excerpt=rj.get("Description", ""),
                    # CareerOneStop doesn't always provide raw salary, usually O*NET averages
                    # but we'll take what's in the snippet if it looks like a range.
                    salary_text="", 
                    source_lane="aggregator",
                    adapter="careeronestop",
                    tier=str(company_config.get("tier", 4))
                )
                jobs.append(job)
                
            return jobs

        except Exception as e:
            logger.error(f"CareerOneStop scrape failed for '{keyword}': {e}")
            return []
