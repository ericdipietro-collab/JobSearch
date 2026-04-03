from typing import List, Dict, Any, Optional
from .base import BaseAdapter
from jobsearch.scraper.models import Job
import hashlib
import logging

logger = logging.getLogger(__name__)

class SmartRecruitersAdapter(BaseAdapter):
    def scrape(self, company_config: Dict[str, Any]) -> List[Job]:
        adapter_key = company_config.get("adapter_key")
        if not adapter_key:
            return []
            
        # SmartRecruiters API
        # URL format: https://api.smartrecruiters.com/v1/companies/{key}/postings
        jobs: List[Job] = []
        
        try:
            url = f"https://api.smartrecruiters.com/v1/companies/{adapter_key}/postings"
            data = self.fetch_json(url)
            
            content = data.get("content") or []
            for raw in content:
                title = raw.get("name", "")
                location_data = raw.get("location") or {}
                location = f"{location_data.get('city', '')}, {location_data.get('region', '')}".strip(", ")
                
                # Public job URL
                job_url = f"https://jobs.smartrecruiters.com/{adapter_key}/{raw.get('id')}"
                
                company_name = company_config.get("name", "Unknown")
                job_id = hashlib.md5(f"{company_name}{title}{job_url}".encode()).hexdigest()

                job = Job(
                    id=job_id,
                    company=company_name,
                    role_title_raw=title,
                    location=location,
                    url=job_url,
                    source="SmartRecruiters",
                    adapter="smartrecruiters",
                    tier=str(company_config.get("tier", 4)),
                    description_excerpt=f"Department: {raw.get('department', {}).get('label', 'N/A')}"
                )
                jobs.append(job)
                
        except Exception as e:
            logger.error(f"SmartRecruiters scrape error: {e}")
                
        return jobs
