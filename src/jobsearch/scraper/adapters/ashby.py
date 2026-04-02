from typing import List, Dict, Any, Optional
from .base import BaseAdapter
from ..models import Job
import hashlib

class AshbyAdapter(BaseAdapter):
    def scrape(self, company_config: Dict[str, Any]) -> List[Job]:
        adapter_key = company_config.get("adapter_key")
        if not adapter_key:
            return []
            
        url = f"https://api.ashbyhq.com/posting-api/job-board/{adapter_key}?includeCompensation=true"
        data = self.fetch_json(url)
        jobs: List[Job] = []

        for raw in data.get("jobs", []):
            locations = [str(raw.get("location") or "").strip()]
            for item in raw.get("secondaryLocations") or []:
                loc = str(item.get("location") or "").strip()
                if loc:
                    locations.append(loc)
            
            # De-dupe while preserving order
            seen_locs = set()
            unique_locs = []
            for l in locations:
                if l and l not in seen_locs:
                    unique_locs.append(l)
                    seen_locs.add(l)
            
            location = " / ".join(unique_locs)
            
            is_remote_flag = raw.get("isRemote") or (raw.get("workplaceType") or "").lower() == "remote"
            if is_remote_flag and "remote" not in location.lower():
                location = f"{location} / Remote" if location else "Remote"
            
            comp = raw.get("compensation") or {}
            desc = (raw.get("descriptionHtml") or raw.get("descriptionPlain") or "")
            if comp.get("summary"):
                desc += " " + comp.get("summary")
            
            job_url = raw.get("jobUrl", "")
            company_name = company_config.get("name", "Unknown")
            title = raw.get("title", "")
            job_id = hashlib.md5(f"{company_name}{title}{job_url}".encode()).hexdigest()

            job = Job(
                id=job_id,
                company=company_name,
                role_title_raw=title,
                location=location,
                is_remote=bool(is_remote_flag),
                url=job_url,
                source="Ashby",
                adapter="ashby",
                tier=str(company_config.get("tier", 4)),
                description_excerpt=desc[:1000]
            )
            jobs.append(job)

        return jobs
