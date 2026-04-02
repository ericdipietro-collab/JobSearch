from typing import List, Dict, Any, Optional
from .base import BaseAdapter
from ..models import Job
import hashlib
import logging

logger = logging.getLogger(__name__)

class WorkdayAdapter(BaseAdapter):
    def scrape(self, company_config: Dict[str, Any]) -> List[Job]:
        careers_url = company_config.get("careers_url")
        if not careers_url or "myworkdayjobs" not in careers_url:
            return []
            
        host, tenant, sites = self.workday_context(careers_url)
        jobs: List[Job] = []
        seen_urls = set()

        for site in sites:
            endpoint = f"https://{host}/wday/cxs/{tenant}/{site}/jobs"
            offset = 0
            limit = 20
            
            while offset < 200: # Limit offset for performance
                try:
                    data = self._fetch_listing(endpoint, limit, offset, careers_url)
                except Exception as e:
                    logger.debug(f"Workday listing failed for {endpoint}: {e}")
                    break
                    
                postings = data.get("jobPostings") or []
                if not postings:
                    break
                    
                for raw in postings:
                    title = raw.get("title") or raw.get("jobTitle") or ""
                    location = raw.get("locationsText") or raw.get("location") or ""
                    external_path = (raw.get("externalPath") or "").lstrip('/')
                    
                    if external_path.startswith('job/'):
                        external_path = external_path[4:]
                    
                    job_url = f"https://{host}/en-US/{site}/job/{external_path}"
                    if job_url in seen_urls:
                        continue
                    seen_urls.add(job_url)
                    
                    # Fetch details for description
                    description = ""
                    try:
                        detail_endpoint = f"https://{host}/wday/cxs/{tenant}/{site}/job/{external_path}"
                        detail = self.fetch_json_post(detail_endpoint, {}, referer=endpoint)
                        info = detail.get("jobPostingInfo") or detail.get("jobPosting") or {}
                        description = " ".join(filter(None, [
                            info.get("jobDescription"),
                            info.get("jobResponsibilities"),
                            info.get("requiredQualifications")
                        ]))
                    except Exception:
                        pass # Description is optional
                    
                    company_name = company_config.get("name", "Unknown")
                    job_id = hashlib.md5(f"{company_name}{title}{job_url}".encode()).hexdigest()

                    job = Job(
                        id=job_id,
                        company=company_name,
                        role_title_raw=title,
                        location=location,
                        url=job_url,
                        source="Workday",
                        adapter="workday",
                        tier=str(company_config.get("tier", 4)),
                        description_excerpt=description[:1000]
                    )
                    jobs.append(job)
                
                offset += limit
                if len(postings) < limit:
                    break
            
            if jobs: # If we found jobs on one site variant, we're likely done
                break
                
        return jobs

    def _fetch_listing(self, endpoint: str, limit: int, offset: int, referer: str) -> Dict[str, Any]:
        """Try multiple payload variants as Workday is picky."""
        variants = [
            {"limit": limit, "offset": offset, "appliedFacets": {}},
            {"limit": limit, "offset": offset},
        ]
        
        last_err = None
        for payload in variants:
            try:
                return self.fetch_json_post(endpoint, payload, referer=referer)
            except Exception as e:
                last_err = e
                continue
        raise last_err
