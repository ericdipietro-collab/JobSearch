from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict, Iterable, List

from jobsearch.scraper.adapters.base import BaseAdapter
from jobsearch.scraper.models import Job
from jobsearch.config.settings import settings


class IndeedConnectorAdapter(BaseAdapter):
    """Import-backed adapter for the first aggregator lane.

    The packaged app cannot call a chat-only MCP/connector directly, so this
    adapter reads normalized JSON exported into the runtime results folder.
    """

    @staticmethod
    def _resolve_cache_path(company_config: Dict[str, Any]) -> Path:
        raw = str(company_config.get("cache_file") or "").strip()
        if not raw:
            return settings.aggregator_import_dir / "indeed_connector_results.json"
        path = Path(raw)
        if not path.is_absolute():
            path = settings.runtime_dir / path
        return path

    @staticmethod
    def _records_from_payload(payload: Any) -> list[dict]:
        if isinstance(payload, list):
            return [item for item in payload if isinstance(item, dict)]
        if not isinstance(payload, dict):
            return []
        for key in ("jobs", "results", "organic_results", "data"):
            value = payload.get(key)
            if isinstance(value, list):
                return [item for item in value if isinstance(item, dict)]
        return []

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
    def _canonical_url(record: Dict[str, Any], fallback_url: str) -> str:
        canonical = IndeedConnectorAdapter._first_non_empty(
            record,
            "canonical_job_url",
            "canonical_url",
            "employer_job_url",
            "apply_url",
            "job_apply_link",
        )
        if canonical and "indeed." not in canonical.lower():
            return canonical
        return fallback_url if fallback_url and "indeed." not in fallback_url.lower() else ""

    def _iter_jobs(self, company_config: Dict[str, Any], records: Iterable[Dict[str, Any]]) -> List[Job]:
        jobs: list[Job] = []
        default_company = str(company_config.get("name") or "Indeed").strip() or "Indeed"
        default_tier = str(company_config.get("tier") or 4)
        for record in records:
            title = self._first_non_empty(record, "title", "job_title", "name")
            job_url = self._first_non_empty(record, "job_url", "url", "job_link", "link")
            if not title or not job_url:
                continue
            company = self._first_non_empty(record, "company", "company_name", "employer") or default_company
            location = self._first_non_empty(record, "location", "formatted_location", "job_location")
            description = self._first_non_empty(record, "description", "snippet", "summary")
            salary_text = self._first_non_empty(record, "salary_text", "salary", "compensation")
            canonical = self._canonical_url(record, job_url)
            remote_flag = record.get("is_remote")
            is_remote = bool(remote_flag) if isinstance(remote_flag, bool) else ("remote" in location.lower())
            job = Job(
                id=Job.make_id(company, title, canonical or job_url),
                company=company,
                role_title_raw=title,
                url=job_url,
                source="Indeed",
                adapter="indeed_connector",
                tier=default_tier,
                description_excerpt=description,
                location=location,
                is_remote=is_remote,
                salary_text=salary_text,
                salary_min=record.get("salary_min"),
                salary_max=record.get("salary_max"),
                date_discovered=self._first_non_empty(record, "date_discovered", "discovered_at", "created_at"),
                notes=self._first_non_empty(record, "notes"),
            )
            job.source_lane = "aggregator"
            job.canonical_job_url = canonical
            jobs.append(job)
        return jobs

    def scrape(self, company_config: Dict[str, Any]) -> List[Job]:
        cache_path = self._resolve_cache_path(company_config)
        if not cache_path.exists():
            self.last_status = "empty"
            self.last_note = f"Indeed import cache not found: {cache_path.name}"
            return []
        try:
            payload = json.loads(cache_path.read_text(encoding="utf-8"))
        except Exception as exc:
            self.last_status = "empty"
            self.last_note = f"Indeed import cache unreadable: {exc}"
            return []
        jobs = self._iter_jobs(company_config, self._records_from_payload(payload))
        self.last_status = "ok" if jobs else "empty"
        self.last_note = f"Imported {len(jobs)} Indeed result(s) from cache" if jobs else "Indeed import cache contained no jobs"
        return jobs
