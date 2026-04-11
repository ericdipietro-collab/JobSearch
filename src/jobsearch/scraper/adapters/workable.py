import hashlib
import logging
import re
from typing import Any, Dict, List

from .base import BaseAdapter
from jobsearch.scraper.models import Job

logger = logging.getLogger(__name__)


class WorkableAdapter(BaseAdapter):
    """Scrape open jobs from Workable career boards via the public JSON API.

    Workable exposes a paginated POST endpoint at:
        https://apply.workable.com/api/v3/accounts/{slug}/jobs
    which returns published jobs without authentication.

    The adapter_key is the company's Workable slug (e.g. "corcentric" for
    apply.workable.com/corcentric). The healer's EMBED_MARKERS detection
    writes this value automatically when it identifies a Workable board.
    """

    _API_URL = "https://apply.workable.com/api/v3/accounts/{slug}/jobs"
    _JOB_URL = "https://apply.workable.com/{slug}/j/{shortcode}"

    def scrape(self, company_config: Dict[str, Any]) -> List[Job]:
        slug = self._resolve_slug(company_config)
        if not slug:
            return []

        jobs: List[Job] = []
        offset = 0
        limit = 100

        while True:
            url = self._API_URL.format(slug=slug)
            payload = {"limit": limit, "offset": offset, "status": "published"}
            try:
                data = self.fetch_json_post(url, payload)
            except Exception as exc:
                logger.debug("Workable API fetch failed for %s: %s", slug, exc)
                break

            if not isinstance(data, dict):
                break

            results = data.get("results") or []
            if not results:
                break

            company_name = company_config.get("name", "Unknown")

            for raw in results:
                if not isinstance(raw, dict):
                    continue

                title = str(raw.get("title") or "").strip()
                if not title:
                    continue

                shortcode = str(raw.get("shortcode") or "").strip()
                job_url = (
                    raw.get("url")
                    or (self._JOB_URL.format(slug=slug, shortcode=shortcode) if shortcode else "")
                )
                if not job_url:
                    continue

                location = self._extract_location(raw)
                description = str(raw.get("description") or raw.get("full_description") or "").strip()
                department = str((raw.get("department") or {}).get("title") or "").strip()
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
                        source="Workable",
                        adapter="workable",
                        tier=str(company_config.get("tier", 4)),
                        description_excerpt=description[:4000],
                    )
                )

            total = data.get("total") or 0
            offset += limit
            if offset >= total or len(results) < limit:
                break

        return jobs

    def _resolve_slug(self, company_config: Dict[str, Any]) -> str:
        """Return the Workable slug from adapter_key or careers_url."""
        adapter_key = str(company_config.get("adapter_key") or "").strip()
        if adapter_key:
            return adapter_key.strip("/")

        careers_url = str(company_config.get("careers_url") or "").strip()
        if "workable.com" in careers_url.lower():
            # https://apply.workable.com/{slug}[/...]
            match = re.search(r"workable\.com/([^/?#]+)", careers_url, re.IGNORECASE)
            if match:
                return match.group(1).strip("/")

        return ""

    def _extract_location(self, raw: Dict[str, Any]) -> str:
        loc = raw.get("location") or {}
        if isinstance(loc, str):
            return loc.strip()
        if isinstance(loc, dict):
            parts = [
                str(loc.get("city") or "").strip(),
                str(loc.get("region") or "").strip(),
                str(loc.get("country") or "").strip(),
            ]
            location = ", ".join(p for p in parts if p)
            if raw.get("remote") or loc.get("telecommuting"):
                location = f"{location} / Remote" if location else "Remote"
            return location
        return ""
