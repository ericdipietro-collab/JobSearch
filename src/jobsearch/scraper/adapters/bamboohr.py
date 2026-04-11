import hashlib
import logging
from typing import Any, Dict, List

from .base import BaseAdapter
from jobsearch.scraper.models import Job

logger = logging.getLogger(__name__)


class BambooHRAdapter(BaseAdapter):
    """Scrape open jobs from BambooHR public careers boards.

    BambooHR exposes a public JSON endpoint at:
        https://{slug}.bamboohr.com/careers/list
    which returns an array of open positions without authentication.

    The adapter_key is the company's BambooHR subdomain (e.g. "acme" for
    acme.bamboohr.com).  The healer's EMBED_MARKERS detection writes this
    value automatically when it identifies a BambooHR embed on a careers page.
    """

    _BASE_URL = "https://{slug}.bamboohr.com/careers/list"
    _JOB_URL = "https://{slug}.bamboohr.com/careers/{job_id}"

    def scrape(self, company_config: Dict[str, Any]) -> List[Job]:
        adapter_key = company_config.get("adapter_key")
        if not adapter_key or not str(adapter_key).strip():
            return []

        slug = str(adapter_key).strip().lower()
        url = self._BASE_URL.format(slug=slug)

        try:
            data = self.fetch_json(url)
        except Exception as exc:
            logger.debug("BambooHR fetch failed for %s: %s", slug, exc)
            return []

        # API returns either a list directly or {"result": [...]}
        if isinstance(data, dict):
            raw_jobs = data.get("result") or data.get("jobs") or []
        elif isinstance(data, list):
            raw_jobs = data
        else:
            return []

        company_name = company_config.get("name", "Unknown")
        jobs: List[Job] = []

        for raw in raw_jobs:
            if not isinstance(raw, dict):
                continue

            title = str(raw.get("title") or raw.get("jobTitle") or "").strip()
            if not title:
                continue

            job_id_raw = str(raw.get("id") or "").strip()
            job_url = (
                raw.get("url")
                or (self._JOB_URL.format(slug=slug, job_id=job_id_raw) if job_id_raw else "")
            )
            if not job_url:
                continue

            # Location — BambooHR returns location as a nested object or string
            loc_raw = raw.get("location") or raw.get("jobLocation") or {}
            if isinstance(loc_raw, dict):
                location = str(loc_raw.get("name") or loc_raw.get("city") or "").strip()
            else:
                location = str(loc_raw).strip()

            is_remote = "remote" in location.lower() or bool(raw.get("isRemote"))

            description = str(raw.get("description") or raw.get("jobOpeningDescription") or "").strip()
            department = str(raw.get("departmentLabel") or raw.get("department") or "").strip()
            if department and description:
                description = f"[{department}] {description}"
            elif department:
                description = department

            job_hash = hashlib.md5(f"{company_name}{title}{job_url}".encode()).hexdigest()
            jobs.append(
                Job(
                    id=job_hash,
                    company=company_name,
                    role_title_raw=title,
                    location=location,
                    is_remote=is_remote,
                    url=job_url,
                    source="BambooHR",
                    adapter="bamboohr",
                    tier=str(company_config.get("tier", 4)),
                    description_excerpt=description[:4000],
                )
            )

        return jobs
