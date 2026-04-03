from typing import List, Dict, Any, Optional
from .base import BaseAdapter
from jobsearch.scraper.models import Job

class GreenhouseAdapter(BaseAdapter):
    def scrape(self, company_config: Dict[str, Any]) -> List[Job]:
        adapter_key = company_config.get("adapter_key")
        if not adapter_key:
            return []
            
        url = f"https://boards-api.greenhouse.io/v1/boards/{adapter_key}/jobs?content=true"
        data = self.fetch_json(url)
        jobs: List[Job] = []

        for raw in data.get("jobs", []):
            # Mapping logic similar to job_search_v6.py
            job_url = raw.get("absolute_url", "")
            company_name = company_config.get("name", "Unknown")
            title = raw.get("title", "")
            
            # Simple unique ID generation for the job
            import hashlib
            job_id = hashlib.md5(f"{company_name}{title}{job_url}".encode()).hexdigest()
            
            job = Job(
                id=job_id,
                company=company_name,
                role_title_raw=title,
                location=(raw.get("location") or {}).get("name", ""),
                url=job_url,
                source="Greenhouse",
                adapter="greenhouse",
                tier=str(company_config.get("tier", 4)),
                description_excerpt=raw.get("content", "")[:1000] # Excerpt for summary
            )
            jobs.append(job)

        return jobs
