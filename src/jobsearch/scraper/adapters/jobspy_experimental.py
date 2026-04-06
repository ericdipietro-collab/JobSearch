from __future__ import annotations

import importlib
from typing import Any, Dict, Iterable, List

from jobsearch.config.settings import get_runtime_setting, settings
from jobsearch.scraper.adapters.base import BaseAdapter
from jobsearch.scraper.models import Job


class JobSpyExperimentalAdapter(BaseAdapter):
    @staticmethod
    def _first_non_empty(record: Dict[str, Any], *keys: str) -> str:
        for key in keys:
            value = record.get(key)
            if value not in (None, ""):
                text = str(value).strip()
                if text:
                    return text
        return ""

    @staticmethod
    def _site_names(company_config: Dict[str, Any]) -> list[str]:
        raw = (
            company_config.get("site_names")
            or get_runtime_setting("jobspy_site_names", settings.jobspy_site_names)
            or settings.jobspy_site_names
        )
        if isinstance(raw, (list, tuple)):
            return [str(item).strip() for item in raw if str(item).strip()]
        return [part.strip() for part in str(raw or "google").split(",") if part.strip()]

    @staticmethod
    def _iter_records(payload: Any) -> Iterable[Dict[str, Any]]:
        if payload is None:
            return []
        if hasattr(payload, "to_dict"):
            try:
                records = payload.to_dict("records")
                if isinstance(records, list):
                    return [row for row in records if isinstance(row, dict)]
            except Exception:
                pass
        if isinstance(payload, list):
            return [row for row in payload if isinstance(row, dict)]
        if isinstance(payload, dict):
            for key in ("jobs", "results", "data"):
                value = payload.get(key)
                if isinstance(value, list):
                    return [row for row in value if isinstance(row, dict)]
        return []

    @staticmethod
    def _canonical_url(record: Dict[str, Any], fallback_url: str) -> str:
        canonical = JobSpyExperimentalAdapter._first_non_empty(
            record,
            "direct_url",
            "apply_url",
            "canonical_job_url",
            "employer_job_url",
        )
        if canonical:
            return canonical
        return ""

    def scrape(self, company_config: Dict[str, Any]) -> List[Job]:
        try:
            module = importlib.import_module("jobspy")
        except Exception:
            self.last_status = "empty"
            self.last_note = "jobspy not installed; experimental source skipped"
            return []

        scrape_jobs = getattr(module, "scrape_jobs", None)
        if not callable(scrape_jobs):
            self.last_status = "empty"
            self.last_note = "jobspy module is missing scrape_jobs()"
            return []

        results_wanted = int(
            company_config.get("results_wanted")
            or get_runtime_setting("jobspy_results_per_run", str(settings.jobspy_results_per_run))
            or settings.jobspy_results_per_run
        )
        hours_old = int(
            company_config.get("hours_old")
            or get_runtime_setting("jobspy_hours_old", str(settings.jobspy_hours_old))
            or settings.jobspy_hours_old
        )
        country_indeed = str(
            company_config.get("country_indeed")
            or get_runtime_setting("jobspy_country_indeed", settings.jobspy_country_indeed)
            or settings.jobspy_country_indeed
        ).strip() or "USA"
        location = str(company_config.get("location_filter") or "").strip()
        queries = [str(q).strip() for q in (company_config.get("search_queries") or []) if str(q).strip()]
        if not queries:
            queries = [str(company_config.get("name") or "").strip() or "jobs"]

        jobs: list[Job] = []
        for query in queries:
            payload = scrape_jobs(
                site_name=self._site_names(company_config),
                search_term=query,
                location=location or None,
                results_wanted=max(1, results_wanted),
                hours_old=max(1, hours_old),
                country_indeed=country_indeed,
            )
            for record in self._iter_records(payload):
                title = self._first_non_empty(record, "title", "job_title", "name")
                url = self._first_non_empty(record, "job_url", "url", "link")
                if not title or not url:
                    continue
                company = self._first_non_empty(record, "company", "company_name", "employer") or str(company_config.get("name") or "JobSpy").strip()
                canonical = self._canonical_url(record, url)
                job = Job(
                    id=str(record.get("id") or Job.make_id(company, title, canonical or url)),
                    company=company,
                    role_title_raw=title,
                    url=url,
                    source="JobSpy",
                    adapter="jobspy",
                    tier=str(company_config.get("tier") or 4),
                    description_excerpt=self._first_non_empty(record, "description", "description_text", "snippet"),
                    location=self._first_non_empty(record, "location", "job_location"),
                    is_remote=bool(record.get("is_remote")) if isinstance(record.get("is_remote"), bool) else ("remote" in self._first_non_empty(record, "location", "job_location").lower()),
                    salary_text=self._first_non_empty(record, "salary_text", "salary"),
                    salary_min=record.get("min_amount") or record.get("salary_min"),
                    salary_max=record.get("max_amount") or record.get("salary_max"),
                )
                job.source_lane = "jobspy_experimental"
                job.canonical_job_url = canonical
                jobs.append(job)
                if len(jobs) >= results_wanted:
                    break
            if len(jobs) >= results_wanted:
                break

        self.last_status = "ok" if jobs else "empty"
        self.last_note = f"Imported {len(jobs)} JobSpy result(s)" if jobs else "No JobSpy results"
        return jobs[:results_wanted]
