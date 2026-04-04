from typing import List, Dict, Any
from .base import BaseAdapter
from jobsearch.scraper.models import Job
import hashlib
import logging
import re
from urllib.parse import urljoin, urlparse

logger = logging.getLogger(__name__)

class RipplingAdapter(BaseAdapter):
    INVALID_ADAPTER_KEYS = {"", "_next", "jobs", "job", "careers", "career", "apply", "positions", "position"}

    def scrape(self, company_config: Dict[str, Any]) -> List[Job]:
        careers_url = str(company_config.get("careers_url") or "").strip()
        slugs = self._candidate_slugs(company_config)
        if not slugs and not careers_url:
            return []

        for slug in slugs:
            jobs = self._scrape_api(company_config, slug)
            if jobs:
                return jobs

        if careers_url:
            try:
                return self._scrape_html(company_config, careers_url)
            except Exception as e:
                logger.error(f"Rippling scrape error: {e}")
        return []

    def _candidate_slugs(self, company_config: Dict[str, Any]) -> List[str]:
        candidates: List[str] = []
        adapter_key = str(company_config.get("adapter_key") or "").strip()
        if adapter_key and adapter_key.lower() not in self.INVALID_ADAPTER_KEYS:
            candidates.append(adapter_key.strip("/"))

        careers_url = str(company_config.get("careers_url") or "").strip()
        if careers_url:
            parsed = urlparse(careers_url if careers_url.startswith("http") else f"https://{careers_url}")
            segments = [segment for segment in parsed.path.split("/") if segment]
            if segments:
                slug = segments[0].strip("/")
                if slug.lower() not in self.INVALID_ADAPTER_KEYS:
                    candidates.append(slug)

        company_slug = re.sub(r"[^a-z0-9]+", "-", str(company_config.get("name") or "").lower()).strip("-")
        if company_slug:
            candidates.append(company_slug)

        unique: List[str] = []
        seen = set()
        for candidate in candidates:
            if candidate and candidate not in seen:
                unique.append(candidate)
                seen.add(candidate)
        return unique

    def _scrape_api(self, company_config: Dict[str, Any], slug: str) -> List[Job]:
        jobs: List[Job] = []
        seen_urls = set()
        page = 0

        while page < 10:
            url = f"https://ats.rippling.com/api/v1/jobs?company_slug={slug}&page={page}"
            try:
                data = self.fetch_json(url)
            except Exception as exc:
                logger.debug("Rippling API fetch failed for %s page %s: %s", slug, page, exc)
                break

            results = self._extract_api_results(data)
            if not results:
                break

            for raw in results:
                job = self._job_from_api_payload(company_config, slug, raw)
                if not job or job.url in seen_urls:
                    continue
                seen_urls.add(job.url)
                jobs.append(job)

            if not self._has_next_page(data):
                break
            page += 1

        return jobs

    def _extract_api_results(self, data: Any) -> List[Dict[str, Any]]:
        if isinstance(data, list):
            return [item for item in data if isinstance(item, dict)]
        if isinstance(data, dict):
            for key in ("results", "items", "jobs", "data"):
                value = data.get(key)
                if isinstance(value, list):
                    return [item for item in value if isinstance(item, dict)]
        return []

    def _has_next_page(self, data: Any) -> bool:
        if isinstance(data, dict):
            next_value = data.get("next")
            if next_value:
                return True
            pagination = data.get("pagination")
            if isinstance(pagination, dict):
                return bool(pagination.get("next_page") or pagination.get("has_next"))
        return False

    def _job_from_api_payload(self, company_config: Dict[str, Any], slug: str, raw: Dict[str, Any]) -> Job | None:
        title = str(raw.get("title") or raw.get("name") or "").strip()
        if not title:
            return None

        location = self._normalize_location(raw.get("location"))
        raw_url = raw.get("job_url") or raw.get("url") or raw.get("hosted_url")
        if raw_url:
            job_url = str(raw_url)
        else:
            raw_id = raw.get("id") or raw.get("job_id")
            if not raw_id:
                return None
            job_url = f"https://ats.rippling.com/{slug}/jobs/{raw_id}"

        company_name = company_config.get("name", "Unknown")
        job_id = hashlib.md5(f"{company_name}{title}{job_url}".encode()).hexdigest()
        description = (
            raw.get("description")
            or raw.get("descriptionPlain")
            or raw.get("description_plain")
            or raw.get("summary")
            or ""
        )

        return Job(
            id=job_id,
            company=company_name,
            role_title_raw=title,
            location=location,
            url=job_url,
            source="Rippling",
            adapter="rippling",
            tier=str(company_config.get("tier", 4)),
            description_excerpt=str(description),
        )

    def _normalize_location(self, location: Any) -> str:
        if isinstance(location, str):
            return location.strip()
        if isinstance(location, dict):
            parts = [str(location.get(key) or "").strip() for key in ("city", "region", "country")]
            return ", ".join(part for part in parts if part)
        return ""

    def _scrape_html(self, company_config: Dict[str, Any], careers_url: str) -> List[Job]:
        from bs4 import BeautifulSoup

        html = self.fetch_text(careers_url)
        if not html:
            return []

        soup = BeautifulSoup(html, "html.parser")
        company_name = company_config.get("name", "Unknown")
        jobs: List[Job] = []
        seen_urls = set()

        for anchor in soup.find_all("a", href=True):
            href = str(anchor.get("href") or "")
            if "/jobs/" not in href.lower():
                continue
            title = re.sub(r"\s+", " ", anchor.get_text(" ", strip=True)).strip()
            if len(title.split()) < 2:
                continue

            full_url = urljoin(careers_url, href)
            if full_url in seen_urls:
                continue
            seen_urls.add(full_url)
            job_id = hashlib.md5(f"{company_name}{title}{full_url}".encode()).hexdigest()
            jobs.append(
                Job(
                    id=job_id,
                    company=company_name,
                    role_title_raw=title,
                    location="",
                    url=full_url,
                    source="Rippling",
                    adapter="rippling",
                    tier=str(company_config.get("tier", 4)),
                    description_excerpt=title,
                )
            )

        return jobs
