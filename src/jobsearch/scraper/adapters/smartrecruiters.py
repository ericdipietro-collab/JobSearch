from typing import List, Dict, Any, Optional
from .base import BaseAdapter
from jobsearch.scraper.models import Job
import hashlib
import logging

logger = logging.getLogger(__name__)

class SmartRecruitersAdapter(BaseAdapter):
    def scrape(self, company_config: Dict[str, Any]) -> List[Job]:
        adapter_key = company_config.get("adapter_key")
        if adapter_key is None or not str(adapter_key).strip():
            return []
            
        # SmartRecruiters API
        # URL format: https://api.smartrecruiters.com/v1/companies/{key}/postings
        jobs: List[Job] = []
        
        try:
            limit = 100
            offset = 0
            while True:
                url = f"https://api.smartrecruiters.com/v1/companies/{adapter_key}/postings?limit={limit}&offset={offset}"
                data = self.fetch_json(url)

                content = data.get("content") or []
                if not content:
                    break
                for raw in content:
                    title = raw.get("name", "")
                    location_data = raw.get("location") or {}
                    location = f"{location_data.get('city', '')}, {location_data.get('region', '')}".strip(", ")

                    job_url = f"https://jobs.smartrecruiters.com/{adapter_key}/{raw.get('id')}"

                    company_name = company_config.get("name", "Unknown")
                    job_id = hashlib.md5(f"{company_name}{title}{job_url}".encode()).hexdigest()

                    department = raw.get("department", {}).get("label", "N/A")
                    description = raw.get("jobAd", {}).get("sections", {}) if isinstance(raw.get("jobAd"), dict) else {}
                    description_parts = []
                    if isinstance(description, dict):
                        for section in description.values():
                            if isinstance(section, dict):
                                text = section.get("text")
                                if text:
                                    description_parts.append(str(text))
                    if not description_parts:
                        description_parts.append(f"Department: {department}")

                    job = Job(
                        id=job_id,
                        company=company_name,
                        role_title_raw=title,
                        location=location,
                        url=job_url,
                        source="SmartRecruiters",
                        adapter="smartrecruiters",
                        tier=str(company_config.get("tier", 4)),
                        description_excerpt=" ".join(description_parts)
                    )
                    jobs.append(job)

                if len(content) < limit:
                    break
                offset += limit
                
        except Exception as e:
            logger.error(f"SmartRecruiters scrape error: {e}")
                
        return jobs
