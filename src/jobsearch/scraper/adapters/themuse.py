from __future__ import annotations

import re
from typing import Any, Dict, List

from jobsearch.config.settings import get_runtime_setting, settings
from jobsearch.scraper.adapters.base import BaseAdapter
from jobsearch.scraper.models import Job
from jobsearch.scraper.query_tiers import max_query_tier, search_queries_for_tier


class TheMuseAdapter(BaseAdapter):
    SEARCH_URL = "https://www.themuse.com/api/public/jobs"

    @staticmethod
    def _first_non_empty(*values: Any) -> str:
        for value in values:
            if value not in (None, ""):
                text = str(value).strip()
                if text:
                    return text
        return ""

    @staticmethod
    def _matches_query(query: str, title: str, company: str, description: str) -> bool:
        query = (query or "").strip().lower()
        if not query:
            return True
        haystack = " ".join((title or "", company or "", description or "")).lower()
        terms = [term for term in re.split(r"\s+", query) if term]
        if not terms:
            return True
        return all(term in haystack for term in terms[:4])

    def _fetch_page(self, company_config: Dict[str, Any], page: int) -> Dict[str, Any]:
        params = {"page": page}
        if settings.themuse_api_key:
            params["api_key"] = settings.themuse_api_key
        location_filter = str(company_config.get("location_filter") or "").strip()
        if location_filter:
            params["location"] = location_filter
        response = self._request("get", self.SEARCH_URL, params=params)
        return response.json()

    def scrape(self, company_config: Dict[str, Any]) -> List[Job]:
        jobs: list[Job] = []
        request_cap = max(
            1,
            int(
                company_config.get("max_requests_per_run")
                or get_runtime_setting("themuse_max_requests_per_run", str(settings.themuse_max_requests_per_run))
                or settings.themuse_max_requests_per_run
            ),
        )
        preferences = getattr(self.scorer, "prefs", {}) or {}
        queries = search_queries_for_tier(
            company_config.get("search_queries"),
            max_query_tier(preferences, "aggregator"),
        )
        if not queries:
            queries = [""]
        max_results = int(company_config.get("max_results_per_query", 50) or 50)
        page = 1
        requests_made = 0
        seen_urls: set[str] = set()
        while len(jobs) < max_results and requests_made < request_cap:
            payload = self._fetch_page(company_config, page)
            requests_made += 1
            results = payload.get("results") or []
            if not isinstance(results, list) or not results:
                break
            for item in results:
                title = self._first_non_empty(item.get("name"))
                refs = item.get("refs") or {}
                url = self._first_non_empty(refs.get("landing_page"), refs.get("job_details"), item.get("id"))
                if not title or not url or url in seen_urls:
                    continue
                company = self._first_non_empty((item.get("company") or {}).get("name"), company_config.get("name"), "The Muse")
                locations = item.get("locations") or []
                location = ", ".join(
                    str(loc.get("name") or "").strip()
                    for loc in locations
                    if isinstance(loc, dict) and str(loc.get("name") or "").strip()
                )
                description = self._first_non_empty(item.get("contents"), item.get("short_name"))
                if not any(self._matches_query(query, title, company, description) for query in queries):
                    continue
                job = Job(
                    id=str(item.get("id") or Job.make_id(company, title, url)),
                    company=company,
                    role_title_raw=title,
                    url=url,
                    source="The Muse",
                    adapter="themuse",
                    tier=str(company_config.get("tier") or 4),
                    description_excerpt=description,
                    location=location,
                    is_remote="remote" in location.lower(),
                    salary_text="",
                )
                job.source_lane = "aggregator"
                job.canonical_job_url = ""
                jobs.append(job)
                seen_urls.add(url)
                if len(jobs) >= max_results:
                    break
            page_count = int(payload.get("page_count") or 0)
            if (page_count and page >= page_count) or len(results) == 0:
                break
            page += 1
        self.last_status = "ok" if jobs else "empty"
        self.last_note = f"Imported {len(jobs)} The Muse result(s)" if jobs else "No The Muse results"
        return jobs[:max_results]
