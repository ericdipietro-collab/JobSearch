from typing import List, Dict, Any, Optional
from .base import BaseAdapter
from jobsearch.scraper.models import Job
import hashlib
import logging

logger = logging.getLogger(__name__)

class RipplingAdapter(BaseAdapter):
    def scrape(self, company_config: Dict[str, Any]) -> List[Job]:
        adapter_key = company_config.get("adapter_key")
        if not adapter_key:
            return []
            
        # Rippling uses a paginated API
        # URL format: https://ats.rippling.com/api/v1/jobs?company_slug={key}&page={page}
        jobs: List[Job] = []
        page = 0
        
        while page < 10: # Safety break
            try:
                url = f"https://ats.rippling.com/api/v1/jobs?company_slug={adapter_key}&page={page}"
                data = self.fetch_json(url)
                
                results = data.get("results") or []
                if not results:
                    break
                    
                for raw in results:
                    title = raw.get("title", "")
                    location = raw.get("location", "")
                    # Direct URL construction
                    job_url = f"https://ats.rippling.com/{adapter_key}/jobs/{raw.get('id')}"
                    
                    company_name = company_config.get("name", "Unknown")
                    job_id = hashlib.md5(f"{company_name}{title}{job_url}".encode()).hexdigest()

                    job = Job(
                        id=job_id,
                        company=company_name,
                        role_title_raw=title,
                        location=location,
                        url=job_url,
                        source="Rippling",
                        adapter="rippling",
                        tier=str(company_config.get("tier", 4)),
                        description_excerpt=raw.get("description", "")[:1000]
                    )
                    jobs.append(job)
                
                if not data.get("next"):
                    break
                page += 1
            except Exception as e:
                logger.error(f"Rippling scrape error: {e}")
                break
                
        return jobs
