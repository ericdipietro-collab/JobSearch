from typing import List, Dict, Any, Optional
from .base import BaseAdapter
from jobsearch.scraper.models import Job
import hashlib
import logging
import re

from bs4 import BeautifulSoup

logger = logging.getLogger(__name__)

class SmartRecruitersAdapter(BaseAdapter):
    def _should_fetch_detail(self, company_config: Dict[str, Any], title: str, location: str) -> bool:
        if not self.scorer:
            return False
        try:
            tier = int(company_config.get("tier", 4) or 4)
            # Tier 1/2 companies always fetch — their tier bonus covers borderline titles.
            if tier <= 2:
                return True
            pre = self.scorer.score_job(
                {
                    "title": title,
                    "description": "",
                    "tier": tier,
                    "location": str(location or ""),
                }
            )
            score = float(pre.get("score") or 0.0)
            threshold = float(self.scorer.min_score_to_keep) * 0.15
            if score < threshold:
                logger.info(
                    "pre-screen drop | adapter=smartrecruiters company=%s title=%r score=%.1f threshold=%.1f",
                    company_config.get("name", "?"), title, score, threshold,
                )
                return False
            return True
        except Exception:
            return False

    def _fetch_detail_description(self, job_url: str) -> str:
        try:
            html = self.fetch_text(job_url)
        except Exception:
            return ""
        if not html:
            return ""
        soup = BeautifulSoup(html[:500_000], "html.parser")
        for tag in soup(["script", "style", "noscript"]):
            tag.decompose()
        text = soup.get_text(" ", strip=True)
        text = re.sub(r"\s+", " ", text).strip()
        return text[:8000]

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
                    is_remote = bool(location_data.get("remote") or location_data.get("telecommuting"))
                    location = f"{location_data.get('city', '')}, {location_data.get('region', '')}".strip(", ")
                    if is_remote and "remote" not in location.lower():
                        location = f"{location} / Remote" if location else "Remote"

                    job_url = f"https://jobs.smartrecruiters.com/{adapter_key}/{raw.get('id')}"

                    company_name = company_config.get("name", "Unknown")
                    job_id = hashlib.md5(f"{company_name}{title}{job_url}".encode()).hexdigest()

                    department = raw.get("department", {}).get("label", "N/A")
                    description = f"Department: {department}"
                    if self._should_fetch_detail(company_config, title, location):
                        detail_text = self._fetch_detail_description(job_url)
                        if detail_text:
                            description = detail_text

                    job = Job(
                        id=job_id,
                        company=company_name,
                        role_title_raw=title,
                        location=location,
                        url=job_url,
                        source="SmartRecruiters",
                        adapter="smartrecruiters",
                        tier=str(company_config.get("tier", 4)),
                        description_excerpt=description,
                    )
                    jobs.append(job)

                if len(content) < limit:
                    break
                offset += limit
                
        except Exception as e:
            logger.error(f"SmartRecruiters scrape error: {e}")
                
        return jobs
