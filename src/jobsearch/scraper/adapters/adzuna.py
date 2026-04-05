from __future__ import annotations

from typing import Any, Dict, List

from jobsearch.config.settings import get_runtime_setting, settings
from jobsearch.scraper.adapters.base import BaseAdapter
from jobsearch.scraper.models import Job


class AdzunaAdapter(BaseAdapter):
    def _credentials_missing(self) -> bool:
        app_id = get_runtime_setting("adzuna_app_id", settings.adzuna_app_id).strip()
        app_key = get_runtime_setting("adzuna_app_key", settings.adzuna_app_key).strip()
        missing = not app_id or not app_key
        if missing:
            self.last_status = "empty"
            self.last_note = "Adzuna credentials missing (App Settings or JOBSEARCH_ADZUNA_APP_ID / JOBSEARCH_ADZUNA_APP_KEY)"
        return missing

    def _fetch_page(self, company_config: Dict[str, Any], keyword: str, page: int) -> Dict[str, Any]:
        country = get_runtime_setting("adzuna_country", settings.adzuna_country).strip().lower() or "us"
        app_id = get_runtime_setting("adzuna_app_id", settings.adzuna_app_id).strip()
        app_key = get_runtime_setting("adzuna_app_key", settings.adzuna_app_key).strip()
        url = f"https://api.adzuna.com/v1/api/jobs/{country}/search/{page}"
        params = {
            "app_id": app_id,
            "app_key": app_key,
            "results_per_page": min(int(company_config.get("max_results_per_query", 50) or 50), 50),
            "what": keyword,
            "content-type": "application/json",
        }
        location_filter = str(company_config.get("location_filter") or "").strip()
        if location_filter:
            params["where"] = location_filter
        response = self._request("get", url, params=params)
        return response.json()

    def scrape(self, company_config: Dict[str, Any]) -> List[Job]:
        if self._credentials_missing():
            return []
        jobs: list[Job] = []
        request_cap = max(
            1,
            int(
                company_config.get("max_requests_per_run")
                or get_runtime_setting("adzuna_max_requests_per_run", str(settings.adzuna_max_requests_per_run))
                or settings.adzuna_max_requests_per_run
            ),
        )
        requests_made = 0
        queries = [str(q).strip() for q in (company_config.get("search_queries") or []) if str(q).strip()]
        if not queries:
            queries = [""]
        max_results = int(company_config.get("max_results_per_query", 50) or 50)
        per_page = min(max_results, 50)
        for query in queries:
            page = 1
            while len(jobs) < max_results and requests_made < request_cap:
                payload = self._fetch_page(company_config, query, page)
                requests_made += 1
                results = payload.get("results") or []
                if not isinstance(results, list) or not results:
                    break
                for item in results:
                    title = str(item.get("title") or "").strip()
                    url = str(item.get("redirect_url") or item.get("url") or "").strip()
                    if not title or not url:
                        continue
                    company = str((item.get("company") or {}).get("display_name") or company_config.get("name") or "Adzuna").strip()
                    location = str((item.get("location") or {}).get("display_name") or "").strip()
                    description = str(item.get("description") or "").strip()
                    canonical = "" if "adzuna." in url.lower() else url
                    job = Job(
                        id=str(item.get("id") or Job.make_id(company, title, canonical or url)),
                        company=company,
                        role_title_raw=title,
                        url=url,
                        source="Adzuna",
                        adapter="adzuna",
                        tier=str(company_config.get("tier") or 4),
                        description_excerpt=description,
                        location=location,
                        is_remote="remote" in location.lower(),
                        salary_text="",
                        salary_min=item.get("salary_min"),
                        salary_max=item.get("salary_max"),
                    )
                    job.source_lane = "aggregator"
                    job.canonical_job_url = canonical
                    jobs.append(job)
                    if len(jobs) >= max_results:
                        break
                if len(results) < per_page:
                    break
                page += 1
        self.last_status = "ok" if jobs else "empty"
        self.last_note = f"Imported {len(jobs)} Adzuna result(s)" if jobs else "No Adzuna results"
        return jobs[:max_results]
