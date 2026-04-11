from typing import List, Dict, Any, Tuple
import hashlib
import logging
import re
import time
from urllib.parse import urljoin

from bs4 import BeautifulSoup

from .base import BaseAdapter
from jobsearch.config.settings import settings
from jobsearch.scraper.models import Job

logger = logging.getLogger(__name__)


class WorkdayAdapter(BaseAdapter):
    def __init__(self, session=None, scorer=None):
        super().__init__(session=session, scorer=scorer)
        # Workday makes many small API calls — 30s per-request is too long.
        # Cap at 10s so empty companies fail fast rather than burning 90s per company.
        self.timeout = 10
        self.last_status = "ok"
        self.last_note = ""

    def scrape(self, company_config: Dict[str, Any]) -> List[Job]:
        careers_url = company_config.get("careers_url") or ""
        adapter_key = company_config.get("adapter_key") or ""
        if "myworkdayjobs" not in careers_url and "myworkdayjobs" not in adapter_key:
            return []

        self.last_status = "ok"
        self.last_note = ""
        jobs: List[Job] = []
        seen_urls = set()
        started_at = time.perf_counter()
        budget_override = company_config.get("scrape_budget_ms")
        budget_ms = int(settings.workday_scrape_budget_ms if budget_override is None else budget_override)
        html_reserve_ms = min(settings.workday_html_fallback_budget_ms, max(budget_ms // 3, 0))
        api_budget_ms = max(budget_ms - html_reserve_ms, 0)
        api_started_at = time.perf_counter()
        company_name = company_config.get("name", "")

        for host, tenant, site in self._candidate_contexts(careers_url, adapter_key, company_name):
            if self._budget_exhausted(api_started_at, api_budget_ms):
                self.last_status = "budget_exhausted"
                self.last_note = f"API budget exhausted before listing fetch ({api_budget_ms} ms)"
                break
            endpoint = f"https://{host}/wday/cxs/{tenant}/{site}/jobs"
            site_jobs = self._scrape_endpoint(company_config, host, tenant, site, endpoint, careers_url, seen_urls, api_started_at, api_budget_ms)
            if site_jobs:
                jobs.extend(site_jobs)
                break

        if jobs:
            self.last_status = "ok"
            self.last_note = ""
            return jobs

        html_jobs = self._scrape_html_fallback(
            company_config,
            careers_url or f"https://{adapter_key}",
            self._candidate_contexts(careers_url, adapter_key, company_name),
            time.perf_counter(),
            html_reserve_ms,
        )
        if html_jobs:
            self.last_status = "ok"
            self.last_note = ""
            return html_jobs
        if not html_jobs:
            if self.last_status == "budget_exhausted" or self._budget_exhausted(started_at, budget_ms):
                self.last_status = "budget_exhausted"
                self.last_note = f"Budget exhausted after HTML fallback ({budget_ms} ms)"
            else:
                self.last_status = "empty"
                self.last_note = "No Workday jobs detected"
        return html_jobs

    def _candidate_contexts(self, careers_url: str, adapter_key: str, company_name: str = "") -> List[Tuple[str, str, str]]:
        contexts: List[Tuple[str, str, str]] = []
        seen = set()

        for source in [adapter_key, careers_url]:
            if not source or "myworkdayjobs" not in source:
                continue
            host, tenant, sites = self.workday_context(source)
            # Prepend name-based candidates before the generic sweep so they're tried first
            name_sites = self._name_site_candidates(company_name) if company_name else []
            for site in name_sites + sites:
                key = (host, tenant, site)
                if key not in seen:
                    contexts.append(key)
                    seen.add(key)

        if not contexts and careers_url:
            host, tenant, sites = self.workday_context(careers_url)
            name_sites = self._name_site_candidates(company_name) if company_name else []
            for site in name_sites + sites:
                key = (host, tenant, site)
                if key not in seen:
                    contexts.append(key)
                    seen.add(key)

        return contexts

    @staticmethod
    def _name_site_candidates(company_name: str) -> List[str]:
        """Generate Workday site ID candidates from the company display name.

        Many Workday site IDs follow the pattern 'Company_Name' (title-cased words
        joined by underscores) or 'CompanyName' (CamelCase). Generating these from
        the YAML company name catches misconfigurations where the adapter_key was
        set to the hostname tenant rather than the actual site ID.
        """
        words = re.sub(r"[^a-zA-Z0-9 ]", "", company_name).split()
        if not words:
            return []
        underscore = "_".join(w.capitalize() for w in words)
        camel = "".join(w.capitalize() for w in words)
        candidates = []
        for base in dict.fromkeys([underscore, camel]):
            candidates.append(base)
            candidates.append(f"{base}_External")
            candidates.append(f"{base}_Careers")
        return candidates

    def _scrape_endpoint(
        self,
        company_config: Dict[str, Any],
        host: str,
        tenant: str,
        site: str,
        endpoint: str,
        referer: str,
        seen_urls: set,
        started_at: float,
        budget_ms: int,
    ) -> List[Job]:
        jobs: List[Job] = []
        offset = 0
        limit = 20

        while True:
            if self._budget_exhausted(started_at, budget_ms):
                break
            try:
                data = self._fetch_listing(endpoint, limit, offset, referer, self._remaining_timeout_seconds(started_at, budget_ms))
            except Exception as exc:
                logger.debug("Workday listing failed for %s: %s", endpoint, exc)
                break

            postings = self._extract_postings(data)
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

                location = raw.get("locationsText") or raw.get("location") or raw.get("bulletFields") or ""
                if isinstance(location, list):
                    location = ", ".join(str(item) for item in location if item)
                external_path = (raw.get("externalPath") or "").lstrip("/")
                if not external_path:
                    external_path = (raw.get("externalUrl") or raw.get("url") or "").lstrip("/")
                if external_path.startswith("job/"):
                    external_path = external_path[4:]
                if not external_path:
                    continue

                job_url = f"https://{host}/en-US/{site}/job/{external_path}"
                if job_url in seen_urls:
                    continue
                seen_urls.add(job_url)

                # Incremental Scrape: Skip detail fetch if we already have this job and it's fresh.
                # 'Fresh' means we've seen it in the last 3 days. This satisfies the requirement
                # to catch JD changes eventually while skipping them on daily runs.
                if self.known_urls and job_url in self.known_urls:
                    from datetime import datetime, timezone, timedelta
                    last_updated = self.known_urls[job_url]
                    if datetime.now(timezone.utc) - last_updated < timedelta(days=3):
                        logger.debug("Incremental scrape: skipping detail fetch for %s", job_url)
                        # Return a minimal job entry.
                        jobs.append(
                            Job(
                                id=hashlib.md5(f"{company_config.get('name', 'Unknown')}{title}{job_url}".encode()).hexdigest(),
                                company=company_config.get("name", "Unknown"),
                                role_title_raw=title,
                                location=location,
                                url=job_url,
                                source="Workday",
                                adapter="workday",
                                tier=str(company_config.get("tier", 4)),
                                description_excerpt="", # Empty signals to upsert_job to keep existing description
                            )
                        )
                        continue

                # Pre-screen: score with no description to skip detail fetches for
                # titles that have no keyword signal and can't reach the keep threshold.
                # Tier 1/2 companies bypass this gate entirely — their tier bonus ensures
                # even borderline titles are worth fetching.
                if self.scorer:
                    tier = int(company_config.get("tier", 4) or 4)
                    if tier > 2:
                        pre = self.scorer.score_job({
                            "title": title, "description": "",
                            "tier": tier, "location": str(location),
                        })
                        if pre["score"] < self.scorer.min_score_to_keep * 0.15:
                            logger.info(
                                "pre-screen drop | adapter=workday company=%s title=%r score=%.1f threshold=%.1f",
                                company_config.get("name", "?"), title,
                                pre["score"], self.scorer.min_score_to_keep * 0.15,
                            )
                            continue

                description = self._fetch_detail_description(
                    host,
                    tenant,
                    site,
                    external_path,
                    endpoint,
                    self._remaining_timeout_seconds(started_at, budget_ms),
                )
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
                        description_excerpt=description,
                    )
                )

            offset += limit
            if len(postings) < limit:
                break

        return jobs

    def _fetch_detail_description(
        self,
        host: str,
        tenant: str,
        site: str,
        external_path: str,
        referer: str,
        timeout_seconds: float,
    ) -> str:
        detail_endpoint = f"https://{host}/wday/cxs/{tenant}/{site}/job/{external_path}"
        try:
            detail = self.fetch_json_post(detail_endpoint, {}, referer=referer, timeout=timeout_seconds)
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

    def _fetch_listing(self, endpoint: str, limit: int, offset: int, referer: str, timeout_seconds: float) -> Dict[str, Any]:
        variants = [
            {"limit": limit, "offset": offset, "appliedFacets": {}, "searchText": ""},
            {"limit": limit, "offset": offset, "appliedFacets": {}},
            {"limit": limit, "offset": offset},
        ]

        last_error = None
        for payload in variants:
            try:
                data = self.fetch_json_post(endpoint, payload, referer=referer, timeout=timeout_seconds)
                if isinstance(data, dict):
                    return data
            except Exception as exc:
                last_error = exc
                continue

        if last_error:
            raise last_error
        raise RuntimeError(f"Workday listing returned no usable payload for {endpoint}")

    def _extract_postings(self, data: Dict[str, Any] | None) -> List[Dict[str, Any]] | None:
        if not isinstance(data, dict):
            return None
        if isinstance(data.get("jobPostings"), list):
            return data.get("jobPostings")
        if isinstance(data.get("jobs"), list):
            return data.get("jobs")
        if isinstance(data.get("content"), list):
            return data.get("content")
        body = data.get("body")
        if isinstance(body, dict):
            if isinstance(body.get("jobPostings"), list):
                return body.get("jobPostings")
            if isinstance(body.get("jobs"), list):
                return body.get("jobs")
        data_block = data.get("data")
        if isinstance(data_block, dict):
            if isinstance(data_block.get("jobPostings"), list):
                return data_block.get("jobPostings")
            if isinstance(data_block.get("jobs"), list):
                return data_block.get("jobs")
        return None

    def _scrape_html_fallback(
        self,
        company_config: Dict[str, Any],
        careers_url: str,
        contexts: List[Tuple[str, str, str]],
        started_at: float,
        budget_ms: int,
    ) -> List[Job]:
        if not careers_url:
            return []
        company_name = company_config.get("name", "Unknown")
        jobs: List[Job] = []
        seen_urls = set()
        candidate_urls = [careers_url]
        for host, _tenant, site in contexts:
            candidate_urls.extend(
                [
                    f"https://{host}/{site}",
                    f"https://{host}/en-US/{site}",
                    f"https://{host}/en-US/{site}/jobs",
                ]
            )
        for url in dict.fromkeys(candidate_urls):
            if self._budget_exhausted(started_at, budget_ms):
                break
            try:
                html = self.fetch_text(url, timeout=self._remaining_timeout_seconds(started_at, budget_ms))
            except Exception:
                continue
            soup = BeautifulSoup(html, "html.parser")
            for anchor in soup.find_all("a", href=True):
                href = anchor["href"]
                full_url = urljoin(url, href)
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
            if jobs:
                break
        return jobs

    def _budget_exhausted(self, started_at: float, budget_ms: int) -> bool:
        return ((time.perf_counter() - started_at) * 1000) >= max(1, budget_ms)

    def _remaining_timeout_seconds(self, started_at: float, budget_ms: int) -> float:
        remaining_ms = max(1000, budget_ms - ((time.perf_counter() - started_at) * 1000))
        return max(1.0, min(self.timeout, remaining_ms / 1000.0))

    def _extract_title(self, anchor) -> str:
        text = anchor.get_text(" ", strip=True) or anchor.get("title") or anchor.get("aria-label") or ""
        text = re.sub(r"\s+", " ", str(text)).strip()
        if len(text.split()) < 2:
            return ""
        return text
