import hashlib
import logging
import re
from typing import Any, Dict, List

from .base import BaseAdapter
from jobsearch.scraper.models import Job

logger = logging.getLogger(__name__)


class BreezyAdapter(BaseAdapter):
    """Scrape open jobs from Breezy HR career boards via the public JSON feed.

    Breezy exposes an unauthenticated JSON endpoint at:
        https://{slug}.breezy.hr/json
    which returns the full list of open positions.

    The adapter_key is the company's Breezy subdomain (e.g. "acme" for
    acme.breezy.hr). The healer's EMBED_MARKERS detection writes this
    value automatically when it identifies a Breezy board.
    """

    _API_URL = "https://{slug}.breezy.hr/json"
    _JOB_URL = "https://{slug}.breezy.hr/p/{job_id}"

    def scrape(self, company_config: Dict[str, Any]) -> List[Job]:
        slug = self._resolve_slug(company_config)
        if not slug:
            return []

        url = self._API_URL.format(slug=slug)
        try:
            data = self.fetch_json(url)
        except Exception as exc:
            logger.debug("Breezy fetch failed for %s: %s", slug, exc)
            return []

        # API returns a list of job objects directly
        if isinstance(data, dict):
            raw_jobs = data.get("jobs") or data.get("results") or []
        elif isinstance(data, list):
            raw_jobs = data
        else:
            return []

        company_name = company_config.get("name", "Unknown")
        jobs: List[Job] = []

        for raw in raw_jobs:
            if not isinstance(raw, dict):
                continue

            title = str(raw.get("name") or raw.get("title") or "").strip()
            if not title:
                continue

            job_id_raw = str(raw.get("_id") or raw.get("id") or "").strip()
            job_url = str(raw.get("url") or "").strip()
            if not job_url and job_id_raw:
                job_url = self._JOB_URL.format(slug=slug, job_id=job_id_raw)
            if not job_url:
                continue

            location = self._extract_location(raw)
            description = str(raw.get("description") or "").strip()
            department = str((raw.get("department") or {}).get("name") or "").strip()
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
                    source="Breezy HR",
                    adapter="breezy",
                    tier=str(company_config.get("tier", 4)),
                    description_excerpt=description[:4000],
                )
            )

        return jobs

    def _resolve_slug(self, company_config: Dict[str, Any]) -> str:
        """Return the Breezy slug from adapter_key or careers_url."""
        adapter_key = str(company_config.get("adapter_key") or "").strip()
        if adapter_key:
            return adapter_key.strip("/")

        careers_url = str(company_config.get("careers_url") or "").strip()
        if "breezy.hr" in careers_url.lower():
            match = re.search(r"https?://([^.]+)\.breezy\.hr", careers_url, re.IGNORECASE)
            if match:
                return match.group(1)

        return ""

    def _extract_location(self, raw: Dict[str, Any]) -> str:
        loc = raw.get("location") or {}
        if isinstance(loc, str):
            return loc.strip()
        if isinstance(loc, dict):
            parts = [
                str(loc.get("city") or "").strip(),
                str(loc.get("state") or loc.get("region") or "").strip(),
                str(loc.get("country") or "").strip(),
            ]
            location = ", ".join(p for p in parts if p)
            if raw.get("type") == "remote":
                location = f"{location} / Remote" if location else "Remote"
            return location
        return ""
