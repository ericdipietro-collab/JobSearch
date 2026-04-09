import logging
import re
from typing import List, Dict, Any, Optional
from .base import BaseAdapter
from jobsearch.scraper.models import Job
from jobsearch.config.settings import get_headers

logger = logging.getLogger(__name__)

class RemoteOKAdapter(BaseAdapter):
    """
    Adapter for the RemoteOK.com API.
    Uses the .json exploit to bypass Cloudflare and fetch clean data.
    """
    
    BASE_URL = "https://remoteok.com/api"

    def scrape(self, company_config: Dict[str, Any]) -> List[Job]:
        """
        Scrapes jobs from RemoteOK.
        Queries by tag (e.g. 'product-manager', 'data-analyst').
        """
        tag = company_config.get("tag", "product-manager").lower().replace(" ", "-")
        url = f"{self.BASE_URL}?tag={tag}"
        
        # RemoteOK likes a real-looking User-Agent
        headers = get_headers()
        
        try:
            import requests
            response = requests.get(url, headers=headers, timeout=20)
            
            # If RemoteOK returns a 1-item list with just legal info, it means no results
            data = response.json()
            if not isinstance(data, list) or len(data) <= 1:
                logger.info(f"RemoteOK: No jobs found for tag '{tag}'")
                return []

            jobs = []
            # Skip first element as it's usually metadata/legal
            for rj in data[1:]:
                if not isinstance(rj, dict): continue
                
                # RemoteOK provides explicit salary info
                salary = ""
                if rj.get("salary_min") and rj.get("salary_max"):
                    salary = f"${rj['salary_min']:,} - ${rj['salary_max']:,}"
                elif rj.get("salary"):
                    salary = str(rj["salary"])

                job = Job(
                    company=rj.get("company", "Unknown"),
                    role=rj.get("position", "Unknown Position"),
                    url=rj.get("url", ""),
                    location="Remote",
                    description_excerpt=rj.get("description", ""),
                    salary_text=salary,
                    source_lane="aggregator",
                    adapter="remoteok",
                    tier=str(company_config.get("tier", 4))
                )
                # Cleanup HTML
                if job.description_excerpt:
                    job.description_excerpt = re.sub(r'<[^>]*>', '', job.description_excerpt)
                
                jobs.append(job)
                
            return jobs

        except Exception as e:
            logger.error(f"RemoteOK scrape failed for tag '{tag}': {e}")
            return []
