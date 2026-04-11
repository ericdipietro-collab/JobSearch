import hashlib
import logging
import re
from typing import Any, Dict, List

from .base import BaseAdapter
from jobsearch.scraper.models import Job

logger = logging.getLogger(__name__)


class JobviteAdapter(BaseAdapter):
    """Scrape open jobs from Jobvite career boards via the public JSON API.

    Jobvite exposes an unauthenticated JSON endpoint at:
        https://jobs.jobvite.com/{slug}/api/v3/jobListings
    which returns paginated job listings.

    The adapter_key is the company's Jobvite slug (e.g. "acme" for
    jobs.jobvite.com/acme/). The healer's EMBED_MARKERS detection writes
    this value automatically when it identifies a Jobvite board.
    """

    _API_URL = "https://jobs.jobvite.com/{slug}/api/v3/jobListings"
    _JOB_URL = "https://jobs.jobvite.com/{slug}/job/{job_id}"

    def scrape(self, company_config: Dict[str, Any]) -> List[Job]:
        slug = self._resolve_slug(company_config)
        if not slug:
            return []

        company_name = company_config.get("name", "Unknown")
        jobs: List[Job] = []
        start = 0
        page_size = 100

        while True:
            url = self._API_URL.format(slug=slug)
            try:
                data = self.fetch_json(url, params={"start": start, "count": page_size})
            except Exception as exc:
                logger.debug("Jobvite fetch failed for %s (start=%d): %s", slug, start, exc)
                break

            if not isinstance(data, dict):
                break

            raw_jobs = data.get("requisitions") or data.get("jobs") or data.get("jobListings") or []
            if not raw_jobs:
                break

            for raw in raw_jobs:
                if not isinstance(raw, dict):
                    continue

                title = str(raw.get("title") or "").strip()
                if not title:
                    continue

                job_id = str(raw.get("id") or raw.get("jobId") or "").strip()
                job_url = str(raw.get("applyUrl") or raw.get("url") or "").strip()
                if not job_url and job_id:
                    job_url = self._JOB_URL.format(slug=slug, job_id=job_id)
                if not job_url:
                    continue

                location = self._extract_location(raw)
                description = str(raw.get("description") or raw.get("briefDescription") or "").strip()
                department = str(raw.get("category") or raw.get("department") or "").strip()
                if department and not description:
                    description = department

                job_hash = hashlib.md5(f"{company_name}{title}{job_url}".encode()).hexdigest()
                jobs.append(
                    Job(
                        id=job_hash,
                        company=company_name,
                        role_title_raw=title,
                        location=location,
                        url=job_url,
                        source="Jobvite",
                        adapter="jobvite",
                        tier=str(company_config.get("tier", 4)),
                        description_excerpt=description[:4000],
                    )
                )

            total = data.get("total") or data.get("count") or 0
            start += len(raw_jobs)
            if start >= total or len(raw_jobs) < page_size:
                break

        return jobs

    def _resolve_slug(self, company_config: Dict[str, Any]) -> str:
        """Return the Jobvite slug from adapter_key or careers_url."""
        adapter_key = str(company_config.get("adapter_key") or "").strip()
        if adapter_key:
            return adapter_key.strip("/")

        careers_url = str(company_config.get("careers_url") or "").strip()
        if "jobvite.com" in careers_url.lower():
            match = re.search(r"jobs\.jobvite\.com/([^/?#\s]+)", careers_url, re.IGNORECASE)
            if match:
                return match.group(1).rstrip("/")

        return ""

    def _extract_location(self, raw: Dict[str, Any]) -> str:
        loc = raw.get("location") or raw.get("city") or ""
        if isinstance(loc, str):
            return loc.strip()
        if isinstance(loc, dict):
            parts = [
                str(loc.get("city") or "").strip(),
                str(loc.get("state") or loc.get("region") or "").strip(),
            ]
            return ", ".join(p for p in parts if p)
        remote = str(raw.get("remote") or "").lower()
        if remote in ("true", "1", "yes"):
            return "Remote"
        return ""
