from __future__ import annotations

from typing import Any, Dict, List

from jobsearch.config.settings import get_runtime_setting, settings
from jobsearch.scraper.adapters.base import BaseAdapter
from jobsearch.scraper.models import Job
from jobsearch.scraper.query_tiers import max_query_tier, search_queries_for_tier


class JoobleAdapter(BaseAdapter):
    def scrape(self, company_config: Dict[str, Any]) -> List[Job]:
        api_key = get_runtime_setting("jooble_api_key", settings.jooble_api_key).strip()
        if not api_key:
            self.last_status = "empty"
            self.last_note = "Jooble API key missing (App Settings or JOBSEARCH_JOOBLE_API_KEY)"
            return []
        jobs: list[Job] = []
        request_cap = max(
            1,
            int(
                company_config.get("max_requests_per_run")
                or get_runtime_setting("jooble_max_requests_per_run", str(settings.jooble_max_requests_per_run))
                or settings.jooble_max_requests_per_run
            ),
        )
        requests_made = 0
        preferences = getattr(self.scorer, "prefs", {}) or {}
        queries = search_queries_for_tier(
            company_config.get("search_queries"),
            max_query_tier(preferences, "aggregator"),
        )
        if not queries:
            queries = [""]
        max_results = int(company_config.get("max_results_per_query", 50) or 50)
        per_page = min(max_results, 50)
        url = f"https://jooble.org/api/{api_key}"
        for query in queries:
            page = 1
            while len(jobs) < max_results and requests_made < request_cap:
                payload = {
                    "keywords": query,
                    "location": str(company_config.get("location_filter") or "").strip(),
                    "page": str(page),
                    "ResultOnPage": str(per_page),
                }
                data = self.fetch_json_post(url, payload)
                requests_made += 1
                results = data.get("jobs") or []
                if not isinstance(results, list) or not results:
                    break
                for item in results:
                    title = str(item.get("title") or "").strip()
                    link = str(item.get("link") or "").strip()
                    if not title or not link:
                        continue
                    company = str(item.get("company") or company_config.get("name") or "Jooble").strip()
                    location = str(item.get("location") or "").strip()
                    description = str(item.get("snippet") or "").strip()
                    canonical = "" if "jooble." in link.lower() else link
                    job = Job(
                        id=str(item.get("id") or Job.make_id(company, title, canonical or link)),
                        company=company,
                        role_title_raw=title,
                        url=link,
                        source="Jooble",
                        adapter="jooble",
                        tier=str(company_config.get("tier") or 4),
                        description_excerpt=description,
                        location=location,
                        is_remote="remote" in location.lower(),
                        salary_text=str(item.get("salary") or "").strip(),
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
        self.last_note = f"Imported {len(jobs)} Jooble result(s)" if jobs else "No Jooble results"
        return jobs[:max_results]
