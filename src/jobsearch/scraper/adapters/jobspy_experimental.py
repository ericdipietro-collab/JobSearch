from __future__ import annotations

import importlib
import json
import time
from concurrent.futures import ThreadPoolExecutor, TimeoutError as FuturesTimeoutError
from typing import Any, Dict, Iterable, List

from jobsearch.scraper.adapters.base import BaseAdapter
from jobsearch.scraper.jobspy_metrics import JobSpyMetrics
from jobsearch.scraper.jobspy_normalization import cluster_jobspy_records
from jobsearch.scraper.query_tiers import max_query_tier, search_queries_for_tier
from jobsearch.scraper.jobspy_validation import (
    load_jobspy_settings,
    validate_jobspy_settings,
)
from jobsearch.scraper.models import Job


class JobSpyExperimentalAdapter(BaseAdapter):
    @staticmethod
    def _glassdoor_location(location: str) -> str:
        text = str(location or "").strip()
        if not text:
            return ""
        return text.split(",")[0].strip()

    @staticmethod
    def _is_transient_error(exc: Exception) -> bool:
        text = str(exc or "").lower()
        return any(token in text for token in ("timeout", "429", "connection", "reset", "refused", "substring not found"))

    def _scrape_jobs_with_retry(self, scrape_jobs, *, max_retries: int = 2, backoff_seconds: float = 5.0, **kwargs):
        last_exc: Exception | None = None
        for attempt in range(max_retries + 1):
            try:
                return scrape_jobs(**kwargs)
            except Exception as exc:
                last_exc = exc
                if attempt >= max_retries or not self._is_transient_error(exc):
                    raise
                time.sleep(backoff_seconds * (attempt + 1))
        if last_exc:
            raise last_exc
        return None

    def _location_allowed(self, record: Dict[str, Any], config: Dict[str, Any]) -> bool:
        scorer = getattr(self, "scorer", None)
        if scorer is None or not all(
            hasattr(scorer, attr)
            for attr in (
                "us_only",
                "allow_international_remote",
                "_is_international_remote",
                "_is_remote_role",
                "_is_hybrid_role",
                "_is_onsite_role",
                "_matches_local_area",
                "local_hybrid_enabled",
                "location_policy",
            )
        ):
            return True
        location = self._first_non_empty(record, "location", "job_location")
        description = self._first_non_empty(record, "description", "description_text", "snippet")
        is_remote = record.get("is_remote")
        if scorer.us_only and not scorer.allow_international_remote and scorer._is_international_remote(location):
            return False
        if scorer._is_remote_role(location, description, is_remote):
            return True
        if scorer._is_hybrid_role(location, description) or scorer._is_onsite_role(location, description) or location.strip():
            local_hybrid_allowed = (
                scorer.local_hybrid_enabled
                and scorer.location_policy in {"remote_only", "hybrid_only", "remote_or_hybrid"}
                and scorer._matches_local_area(location, description)
            )
            return local_hybrid_allowed
        return True

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

    @staticmethod
    def _derive_queries_from_preferences(preferences: Dict[str, Any]) -> List[str]:
        """
        Auto-derive search queries from job title preferences when no search_queries
        are configured. Picks the top high-weight title keywords as search terms,
        paired with industry context for precision.
        """
        titles = (preferences or {}).get("titles", {})
        weights = titles.get("positive_weights") or []
        keywords = [
            k for k, w in (weights if isinstance(weights[0], (list, tuple)) else [])
            if isinstance(w, (int, float)) and w >= 8
        ] if weights else []

        # Fall back to positive_keywords if no weighted titles
        if not keywords:
            keywords = (titles.get("positive_keywords") or [])[:10]

        # Build diverse queries: title + domain modifier
        domain_suffix = "fintech"
        queries = []
        for kw in keywords[:6]:
            queries.append(f"{kw} {domain_suffix}")
        return queries or []

    @staticmethod
    def _query_for_site(query: str, site: str, config: Dict[str, Any]) -> str:
        if site == "google":
            template = str(config.get("google_search_term_template") or "{query}").strip() or "{query}"
            return template.replace("{query}", query)
        return query

    def scrape(self, company_config: Dict[str, Any]) -> List[Job]:
        try:
            module = importlib.import_module("jobspy_enhanced")
        except Exception:
            self.last_status = "empty"
            self.last_note = "jobspy-enhanced-scraper not installed; experimental source skipped"
            return []

        scrape_jobs = getattr(module, "scrape_jobs", None)
        if not callable(scrape_jobs):
            self.last_status = "empty"
            self.last_note = "jobspy_enhanced module is missing scrape_jobs()"
            return []

        preferences = getattr(self.scorer, "prefs", {}) or {}
        config = load_jobspy_settings(preferences, company_config)
        issues = validate_jobspy_settings(config)
        if issues:
            self.last_status = "empty"
            self.last_note = "; ".join(issues)
            return []

        results_wanted = int(config.get("results_wanted_per_site") or 20)
        hours_old = int(config.get("hours_old") or 72)
        country_indeed = str(config.get("country_indeed") or "USA").strip() or "USA"
        location = str(company_config.get("location_filter") or "").strip()
        queries = search_queries_for_tier(
            company_config.get("search_queries"),
            max_query_tier(preferences, "jobspy_experimental"),
        )
        if not queries:
            queries = self._derive_queries_from_preferences(preferences)
        if not queries:
            queries = [str(company_config.get("name") or "").strip() or "jobs"]
        max_total_results = int(config.get("max_total_results") or results_wanted)
        continue_on_failure = bool(config.get("continue_on_site_failure", True))
        # Per-site hard timeout: default 10 minutes. Prevents LinkedIn from running for hours.
        per_site_timeout_s = float(config.get("per_site_timeout_ms") or company_config.get("per_site_timeout_ms") or 600_000) / 1000.0
        metrics = JobSpyMetrics()

        normalized_records: list[dict] = []
        for site in list(config.get("enabled_sites") or []):
            if len(normalized_records) >= max_total_results:
                break
            metrics.mark_requested(site)
            started_at = time.perf_counter()
            try:
                metrics.mark_attempted(site)
                raw_payload = []
                site_timed_out = False
                for query in queries:
                    site_location = location or None
                    if site == "glassdoor":
                        site_location = self._glassdoor_location(location) or None
                    # Build kwargs for enhanced scraper
                    scrape_kwargs = {
                        "site_name": [site],
                        "location": site_location,
                        "results_wanted": max(1, results_wanted),
                        "hours_old": max(1, hours_old),
                        "country_indeed": country_indeed,
                        "linkedin_fetch_description": bool(config.get("linkedin_fetch_description", False)),
                        "is_remote": bool(config.get("is_remote", False)),
                        "job_type": str(config.get("job_type") or "") or None,
                        "proxies": config.get("proxies") or None,
                        "rate_limit_mode": "aggressive",  # Enhanced scraper feature for better LinkedIn/Indeed handling
                    }
                    # Use google_search_term for Google searches (jobspy-enhanced supports this)
                    if site == "google":
                        scrape_kwargs["google_search_term"] = self._query_for_site(query, site, config)
                    else:
                        scrape_kwargs["search_term"] = self._query_for_site(query, site, config)

                    # Run with hard per-site timeout to prevent LinkedIn from blocking for hours
                    try:
                        with ThreadPoolExecutor(max_workers=1) as executor:
                            future = executor.submit(self._scrape_jobs_with_retry, scrape_jobs, **scrape_kwargs)
                            payload = future.result(timeout=per_site_timeout_s)
                    except FuturesTimeoutError:
                        import logging as _logging
                        _logging.getLogger(__name__).warning(
                            f"[jobspy] {site} timed out after {per_site_timeout_s:.0f}s for query '{query}'"
                        )
                        metrics.mark_failure(site, runtime_ms=round(per_site_timeout_s * 1000, 1))
                        site_timed_out = True
                        break
                    raw_payload.extend(list(self._iter_records(payload)))
                    if len(raw_payload) >= results_wanted:
                        break
                if site_timed_out:
                    if not continue_on_failure:
                        break
                    continue
                raw_results = len(raw_payload)
                site_records = []
                for record in raw_payload:
                    record = dict(record)
                    record["_source_site"] = site
                    title = self._first_non_empty(record, "title", "job_title", "name")
                    url = self._first_non_empty(record, "job_url", "url", "link")
                    if not title or not url:
                        continue
                    if not self._location_allowed(record, config):
                        continue
                    record["canonical_job_url"] = self._canonical_url(record, url)
                    site_records.append(record)
                normalized_results = len(site_records)
                clustered = cluster_jobspy_records(site_records)
                metrics.mark_success(
                    site,
                    runtime_ms=round((time.perf_counter() - started_at) * 1000, 1),
                    raw_results=raw_results,
                    normalized_results=normalized_results,
                    deduped_results=max(0, normalized_results - len(clustered)),
                )
                normalized_records.extend(clustered)
            except Exception:
                metrics.mark_failure(site, runtime_ms=round((time.perf_counter() - started_at) * 1000, 1))
                if not continue_on_failure:
                    break

        clustered_records = cluster_jobspy_records(normalized_records)[:max_total_results]
        jobs: list[Job] = []
        for record in clustered_records:
            title = self._first_non_empty(record, "title", "job_title", "name")
            url = self._first_non_empty(record, "job_url", "url", "link")
            if not title or not url:
                continue
            company = self._first_non_empty(record, "company", "company_name", "employer") or str(company_config.get("name") or "JobSpy").strip()
            canonical = self._canonical_url(record, url)
            variants = list(record.get("_source_site_variants") or [])
            provenance = {
                "source_site": record.get("_source_site") or "",
                "source_site_variants": variants,
                "source_site_count": int(record.get("_source_site_count") or len(variants)),
                "direct_apply_confidence": record.get("_direct_apply_confidence") or ("high" if canonical else "low"),
            }
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
                notes=json.dumps(provenance, sort_keys=True),
            )
            job.source_lane = "jobspy_experimental"
            job.canonical_job_url = canonical
            jobs.append(job)

        if not jobs:
            self.last_status = "empty"
            self.last_note = "No JobSpy results"
            if metrics.boards:
                self.last_note = f"{self.last_note} | {metrics.summary_text()}"
            return []

        self.last_status = "ok"
        self.last_note = f"Imported {len(jobs)} JobSpy result(s) | {metrics.summary_text()}"
        return jobs[:max_total_results]
