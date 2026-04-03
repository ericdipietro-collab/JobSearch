from typing import List, Dict, Any, Optional
from .base import BaseAdapter
from jobsearch.scraper.models import Job
import hashlib

class LeverAdapter(BaseAdapter):
    def scrape(self, company_config: Dict[str, Any]) -> List[Job]:
        adapter_key = company_config.get("adapter_key")
        if not adapter_key:
            return []
            
        url = f"https://api.lever.co/v0/postings/{adapter_key}?mode=json"
        data = self.fetch_json(url)
        if not isinstance(data, list):
            return []
        jobs: List[Job] = []

        for raw in data:
            if not isinstance(raw, dict):
                continue
            lever_location = (raw.get("categories") or {}).get("location", "")
            lever_workplace = (raw.get("workplaceType") or "").lower()
            if lever_workplace == "remote" and "remote" not in lever_location.lower():
                lever_location = f"{lever_location} / Remote" if lever_location else "Remote"
            
            job_url = raw.get("hostedUrl", "")
            company_name = company_config.get("name", "Unknown")
            title = raw.get("text", "")
            job_id = hashlib.md5(f"{company_name}{title}{job_url}".encode()).hexdigest()

            job = Job(
                id=job_id,
                company=company_name,
                role_title_raw=title,
                location=lever_location,
                is_remote=(lever_workplace == "remote"),
                url=job_url,
                source="Lever",
                adapter="lever",
                tier=str(company_config.get("tier", 4)),
                description_excerpt=raw.get("descriptionPlain", "")[:1000]
            )
            jobs.append(job)

        return jobs
