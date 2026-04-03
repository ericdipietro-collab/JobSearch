from typing import List, Dict, Any, Tuple
import hashlib
import logging
import re
from urllib.parse import urljoin

from bs4 import BeautifulSoup

from .base import BaseAdapter
from jobsearch.scraper.models import Job

logger = logging.getLogger(__name__)


class WorkdayAdapter(BaseAdapter):
    def scrape(self, company_config: Dict[str, Any]) -> List[Job]:
        careers_url = company_config.get("careers_url") or ""
        adapter_key = company_config.get("adapter_key") or ""
        if "myworkdayjobs" not in careers_url and "myworkdayjobs" not in adapter_key:
            return []

        jobs: List[Job] = []
        seen_urls = set()

        for host, tenant, site in self._candidate_contexts(careers_url, adapter_key):
            endpoint = f"https://{host}/wday/cxs/{tenant}/{site}/jobs"
            site_jobs = self._scrape_endpoint(company_config, host, tenant, site, endpoint, careers_url, seen_urls)
            if site_jobs:
                jobs.extend(site_jobs)
                break

        if jobs:
            return jobs

        return self._scrape_html_fallback(company_config, careers_url or f"https://{adapter_key}")

    def _candidate_contexts(self, careers_url: str, adapter_key: str) -> List[Tuple[str, str, str]]:
        contexts: List[Tuple[str, str, str]] = []
        seen = set()

        for source in [adapter_key, careers_url]:
            if not source or "myworkdayjobs" not in source:
                continue
            host, tenant, sites = self.workday_context(source)
            for site in sites:
                key = (host, tenant, site)
                if key not in seen:
                    contexts.append(key)
                    seen.add(key)

        if not contexts and careers_url:
            host, tenant, sites = self.workday_context(careers_url)
            for site in sites:
                key = (host, tenant, site)
                if key not in seen:
                    contexts.append(key)
                    seen.add(key)

        return contexts

    def _scrape_endpoint(
        self,
        company_config: Dict[str, Any],
        host: str,
        tenant: str,
        site: str,
        endpoint: str,
        referer: str,
        seen_urls: set,
    ) -> List[Job]:
        jobs: List[Job] = []
        offset = 0
        limit = 20

        while offset < 400:
            try:
                data = self._fetch_listing(endpoint, limit, offset, referer)
            except Exception as exc:
                logger.debug("Workday listing failed for %s: %s", endpoint, exc)
                break

            postings = data.get("jobPostings") if isinstance(data, dict) else None
            if not isinstance(postings, list) or not postings:
                break

            for raw in postings:
                if not isinstance(raw, dict):
                    continue

                title = str(raw.get("title") or raw.get("jobTitle") or "").strip()
                if not title:
                    continue
                if self.scorer and self.scorer.is_disqualified(title):
                    continue

                location = raw.get("locationsText") or raw.get("location") or ""
                external_path = (raw.get("externalPath") or "").lstrip("/")
                if external_path.startswith("job/"):
                    external_path = external_path[4:]
                if not external_path:
                    continue

                job_url = f"https://{host}/en-US/{site}/job/{external_path}"
                if job_url in seen_urls:
                    continue
                seen_urls.add(job_url)

                description = self._fetch_detail_description(host, tenant, site, external_path, endpoint)
                company_name = company_config.get("name", "Unknown")
                job_id = hashlib.md5(f"{company_name}{title}{job_url}".encode()).hexdigest()
                jobs.append(
                    Job(
                        id=job_id,
                        company=company_name,
                        role_title_raw=title,
                        location=location,
                        url=job_url,
                        source="Workday",
                        adapter="workday",
                        tier=str(company_config.get("tier", 4)),
                        description_excerpt=description[:1000],
                    )
                )

            offset += limit
            if len(postings) < limit:
                break

        return jobs

    def _fetch_detail_description(self, host: str, tenant: str, site: str, external_path: str, referer: str) -> str:
        detail_endpoint = f"https://{host}/wday/cxs/{tenant}/{site}/job/{external_path}"
        try:
            detail = self.fetch_json_post(detail_endpoint, {}, referer=referer)
        except Exception:
            return ""

        info = detail.get("jobPostingInfo") if isinstance(detail, dict) else None
        if not isinstance(info, dict):
            info = detail.get("jobPosting") if isinstance(detail, dict) else None
        if not isinstance(info, dict):
            return ""

        return " ".join(
            filter(
                None,
                [
                    info.get("jobDescription"),
                    info.get("jobResponsibilities"),
                    info.get("requiredQualifications"),
                ],
            )
        )

    def _fetch_listing(self, endpoint: str, limit: int, offset: int, referer: str) -> Dict[str, Any]:
        variants = [
            {"limit": limit, "offset": offset, "appliedFacets": {}, "searchText": ""},
            {"limit": limit, "offset": offset, "appliedFacets": {}},
            {"limit": limit, "offset": offset},
        ]

        last_error = None
        for payload in variants:
            try:
                data = self.fetch_json_post(endpoint, payload, referer=referer)
                if isinstance(data, dict):
                    return data
            except Exception as exc:
                last_error = exc
                continue

        if last_error:
            raise last_error
        raise RuntimeError(f"Workday listing returned no usable payload for {endpoint}")

    def _scrape_html_fallback(self, company_config: Dict[str, Any], careers_url: str) -> List[Job]:
        if not careers_url:
            return []

        try:
            html = self.fetch_text(careers_url)
        except Exception:
            return []

        soup = BeautifulSoup(html, "html.parser")
        company_name = company_config.get("name", "Unknown")
        jobs: List[Job] = []
        seen_urls = set()

        for anchor in soup.find_all("a", href=True):
            href = anchor["href"]
            full_url = urljoin(careers_url, href)
            if full_url in seen_urls:
                continue
            if "myworkdayjobs.com" not in full_url.lower():
                continue
            if "/job/" not in full_url.lower():
                continue

            title = self._extract_title(anchor)
            if not title:
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
                    source="Workday HTML",
                    adapter="workday",
                    tier=str(company_config.get("tier", 4)),
                    description_excerpt=title,
                )
            )

        return jobs

    def _extract_title(self, anchor) -> str:
        text = anchor.get_text(" ", strip=True) or anchor.get("title") or anchor.get("aria-label") or ""
        text = re.sub(r"\s+", " ", str(text)).strip()
        if len(text.split()) < 2:
            return ""
        return text
