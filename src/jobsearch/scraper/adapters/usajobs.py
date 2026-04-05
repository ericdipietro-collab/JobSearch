from __future__ import annotations

from typing import Any, Dict, List

from jobsearch.config.settings import get_runtime_setting, settings
from jobsearch.scraper.adapters.base import BaseAdapter
from jobsearch.scraper.models import Job


class USAJobsAdapter(BaseAdapter):
    SEARCH_URL = "https://data.usajobs.gov/api/Search"

    def _headers(self) -> Dict[str, str] | None:
        api_key = get_runtime_setting("usajobs_api_key", settings.usajobs_api_key).strip()
        user_agent = get_runtime_setting("usajobs_user_agent", settings.usajobs_user_agent).strip()
        if not api_key or not user_agent:
            self.last_status = "empty"
            self.last_note = "USAJobs credentials missing (App Settings or JOBSEARCH_USAJOBS_API_KEY / JOBSEARCH_USAJOBS_USER_AGENT)"
            return None
        return {
            "Host": "data.usajobs.gov",
            "User-Agent": user_agent,
            "Authorization-Key": api_key,
            "Accept": "application/json",
        }

    @staticmethod
    def _location(descriptor: Dict[str, Any]) -> str:
        display = str(descriptor.get("PositionLocationDisplay") or "").strip()
        if display:
            return display
        locations = descriptor.get("PositionLocation") or []
        if isinstance(locations, list) and locations:
            first = locations[0] or {}
            return str(first.get("LocationName") or "").strip()
        return ""

    @staticmethod
    def _salary_range(descriptor: Dict[str, Any]) -> tuple[Any, Any]:
        pay = descriptor.get("PositionRemuneration") or []
        if isinstance(pay, list) and pay:
            first = pay[0] or {}
            return first.get("MinimumRange"), first.get("MaximumRange")
        return None, None

    def _query_jobs(self, company_config: Dict[str, Any], keyword: str, page: int, headers: Dict[str, str]) -> Dict[str, Any]:
        params = {
            "Keyword": keyword,
            "ResultsPerPage": min(int(company_config.get("max_results_per_query", 50) or 50), 500),
            "Page": page,
            "WhoMayApply": "Public",
            "Fields": "Full",
        }
        location_filter = str(company_config.get("location_filter") or "").strip()
        if location_filter:
            params["LocationName"] = location_filter
        response = self._request("get", self.SEARCH_URL, headers=headers, params=params)
        return response.json()

    def scrape(self, company_config: Dict[str, Any]) -> List[Job]:
        headers = self._headers()
        if headers is None:
            return []
        jobs: list[Job] = []
        queries = [str(q).strip() for q in (company_config.get("search_queries") or []) if str(q).strip()]
        if not queries:
            queries = [""]
        max_results = int(company_config.get("max_results_per_query", 50) or 50)
        per_page = min(max_results, 500)
        for query in queries:
            page = 1
            while len(jobs) < max_results:
                payload = self._query_jobs(company_config, query, page, headers)
                search_result = payload.get("SearchResult") or {}
                items = search_result.get("SearchResultItems") or []
                if not isinstance(items, list) or not items:
                    break
                for item in items:
                    descriptor = (item or {}).get("MatchedObjectDescriptor") or {}
                    title = str(descriptor.get("PositionTitle") or "").strip()
                    canonical = str(descriptor.get("PositionURI") or "").strip()
                    apply_uris = descriptor.get("ApplyURI") or []
                    url = canonical or (apply_uris[0] if isinstance(apply_uris, list) and apply_uris else "")
                    if not title or not url:
                        continue
                    salary_min, salary_max = self._salary_range(descriptor)
                    description = str(
                        descriptor.get("UserArea", {}).get("Details", {}).get("JobSummary")
                        or descriptor.get("QualificationSummary")
                        or ""
                    ).strip()
                    job = Job(
                        id=str(item.get("MatchedObjectId") or Job.make_id(str(descriptor.get("OrganizationName") or ""), title, canonical or url)),
                        company=str(descriptor.get("OrganizationName") or company_config.get("name") or "USAJobs").strip(),
                        role_title_raw=title,
                        url=url,
                        source="USAJobs",
                        adapter="usajobs",
                        tier=str(company_config.get("tier") or 4),
                        description_excerpt=description,
                        location=self._location(descriptor),
                        is_remote=bool(descriptor.get("RemoteIndicator") or False),
                        salary_text="",
                        salary_min=salary_min,
                        salary_max=salary_max,
                    )
                    job.source_lane = "aggregator"
                    job.canonical_job_url = canonical
                    jobs.append(job)
                    if len(jobs) >= max_results:
                        break
                total = int(search_result.get("SearchResultCountAll") or 0)
                if len(items) < per_page or (total and page * per_page >= total):
                    break
                page += 1
        self.last_status = "ok" if jobs else "empty"
        self.last_note = f"Imported {len(jobs)} USAJobs result(s)" if jobs else "No USAJobs results"
        return jobs[:max_results]
