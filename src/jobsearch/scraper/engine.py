"""Scraper engine: orchestrates multi-threaded scraping and persistence."""

import csv
import logging
import random
import time
from concurrent.futures import FIRST_COMPLETED, ThreadPoolExecutor, wait
from datetime import datetime
from typing import Any, Dict, List, Type

from jobsearch.config.settings import BASE_DIR, get_shared_session, rotate_log_file, settings
from jobsearch.db.connection import get_connection
from jobsearch.scraper.adapters.ashby import AshbyAdapter
from jobsearch.scraper.adapters.base import BaseAdapter, BlockedSiteError
from jobsearch.scraper.adapters.generic import GenericAdapter
from jobsearch.scraper.adapters.greenhouse import GreenhouseAdapter
from jobsearch.scraper.adapters.lever import LeverAdapter
from jobsearch.scraper.adapters.rippling import RipplingAdapter
from jobsearch.scraper.adapters.smartrecruiters import SmartRecruitersAdapter
from jobsearch.scraper.adapters.workday import WorkdayAdapter
from jobsearch.scraper.scoring import Scorer
from jobsearch.services.opportunity_service import upsert_job

logger = logging.getLogger(__name__)


class ScraperEngine:
    ADAPTER_MAP: Dict[str, Type[BaseAdapter]] = {
        "greenhouse": GreenhouseAdapter,
        "lever": LeverAdapter,
        "ashby": AshbyAdapter,
        "workday": WorkdayAdapter,
        "workday_manual": WorkdayAdapter,
        "rippling": RipplingAdapter,
        "smartrecruiters": SmartRecruitersAdapter,
        "generic": GenericAdapter,
        "custom_manual": GenericAdapter,
        "custom_blackrock": GenericAdapter,
        "custom_schwab": GenericAdapter,
        "custom_spglobal": GenericAdapter,
    }

    def __init__(self, preferences: Dict[str, Any], companies: List[Dict[str, Any]], deep_search: bool = False):
        self.prefs = preferences
        self.companies = [
            company
            for company in companies
            if isinstance(company, dict) and company.get("active", True) and company.get("name")
        ]
        self.scorer = Scorer(preferences)
        self.deep_search = deep_search
        self.session = get_shared_session()

    def run(self, max_workers: int = 12):
        log_path = settings.log_file
        log_path.parent.mkdir(parents=True, exist_ok=True)
        rotate_log_file(log_path)
        try:
            log_path.touch(exist_ok=True)
        except Exception:
            pass

        def log_msg(message: str):
            print(message, flush=True)
            try:
                with log_path.open("a", encoding="utf-8") as handle:
                    handle.write(f"{datetime.now().strftime('%H:%M:%S')} | {message}\n")
            except Exception:
                pass

        total_companies = len(self.companies)
        log_msg(f"Starting scraper engine: {total_companies} companies...")

        total_evaluated = 0
        total_persisted = 0
        total_inserted = 0
        done_companies = 0
        total_scrape_ms = 0.0
        total_process_ms = 0.0
        adapter_metrics: Dict[str, Dict[str, float]] = {}
        slowest_companies: List[Dict[str, Any]] = []
        blocked_companies: List[Dict[str, Any]] = []
        rejected_rows: List[Dict[str, Any]] = []
        run_started_at = time.perf_counter()
        rejected_csv_path = settings.rejected_csv
        try:
            rejected_csv_path.parent.mkdir(parents=True, exist_ok=True)
            with rejected_csv_path.open("w", newline="", encoding="utf-8") as handle:
                writer = csv.DictWriter(
                    handle,
                    fieldnames=[
                        "company",
                        "title",
                        "location",
                        "url",
                        "source",
                        "adapter",
                        "tier",
                        "score",
                        "fit_band",
                        "work_type",
                        "compensation_unit",
                        "normalized_compensation_usd",
                        "drop_reason",
                        "decision_reason",
                        "matched_keywords",
                        "penalized_keywords",
                    ],
                )
                writer.writeheader()
        except Exception:
            pass

        log_msg(
            f"Pipeline run start | companies={total_companies} deep_search={self.deep_search} "
            f"workers={max_workers} prefs={settings.preferences_yaml.name} companies_yaml={settings.companies_yaml.name}"
        )

        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures_map = {
                executor.submit(self._scrape_company_with_retry, company): company
                for company in self.companies
            }
            while futures_map:
                done, _ = wait(futures_map.keys(), timeout=10, return_when=FIRST_COMPLETED)
                for future in done:
                    company = futures_map.pop(future)
                    done_companies += 1
                    try:
                        scrape_result = future.result()
                        jobs = scrape_result["jobs"]
                        adapter_name = scrape_result["adapter"]
                        scrape_ms = scrape_result["scrape_ms"]
                        used_deep_search = scrape_result["used_deep_search"]
                        scrape_status = scrape_result.get("status", "ok")
                        scrape_note = scrape_result.get("note", "")
                        persisted, inserted, dropped, evaluated, process_ms, score_stats, company_rejected_rows = self._process_and_save_jobs(company, jobs)
                        total_evaluated += evaluated
                        total_persisted += persisted
                        total_inserted += inserted
                        total_scrape_ms += scrape_ms
                        total_process_ms += process_ms
                        rejected_rows.extend(company_rejected_rows)
                        adapter_bucket = adapter_metrics.setdefault(
                            adapter_name, {"companies": 0, "evaluated": 0, "persisted": 0, "scrape_ms": 0.0, "process_ms": 0.0}
                        )
                        adapter_bucket["companies"] += 1
                        adapter_bucket["evaluated"] += evaluated
                        adapter_bucket["persisted"] += persisted
                        adapter_bucket["scrape_ms"] += scrape_ms
                        adapter_bucket["process_ms"] += process_ms
                        if scrape_status == "blocked":
                            blocked_companies.append(
                                {
                                    "name": company.get("name", "Unknown"),
                                    "adapter": adapter_name,
                                    "url": company.get("careers_url", ""),
                                    "note": scrape_note or "Blocked by site protection",
                                }
                            )
                        slowest_companies.append(
                            {
                                "name": company.get("name", "Unknown"),
                                "adapter": adapter_name,
                                "elapsed_ms": round(scrape_ms + process_ms, 1),
                                "scrape_ms": round(scrape_ms, 1),
                                "process_ms": round(process_ms, 1),
                                "deep": used_deep_search,
                            }
                        )
                        slowest_companies = sorted(slowest_companies, key=lambda item: item["elapsed_ms"], reverse=True)[:10]
                        log_msg(
                            f"[{done_companies}/{total_companies}] OK {company.get('name', 'Unknown'):<24} "
                            f"| adapter={adapter_name:<15} evaluated={evaluated:<3} persisted={persisted:<3} "
                            f"new={inserted:<3} dropped={dropped:<3} scrape_ms={scrape_ms:.1f} process_ms={process_ms:.1f} "
                            f"avg_score={score_stats['avg_score']:.1f} keep_rate={score_stats['keep_rate']:.2%} "
                            f"deep={used_deep_search} status={scrape_status}"
                            + (f" note={scrape_note}" if scrape_note else "")
                        )
                    except Exception as exc:
                        log_msg(f"[{done_companies}/{total_companies}] FAIL {company.get('name', 'Unknown'):<24} | {exc}")

        total_elapsed_ms = round((time.perf_counter() - run_started_at) * 1000, 1)
        log_msg(
            f"Pipeline complete. Evaluated: {total_evaluated} | Persisted: {total_persisted} | New: {total_inserted} "
            f"| elapsed_ms={total_elapsed_ms} scrape_ms={round(total_scrape_ms, 1)} process_ms={round(total_process_ms, 1)}"
        )
        for adapter_name, bucket in sorted(adapter_metrics.items(), key=lambda item: item[1]["scrape_ms"] + item[1]["process_ms"], reverse=True):
            log_msg(
                f"Adapter metrics | adapter={adapter_name} companies={int(bucket['companies'])} "
                f"evaluated={int(bucket['evaluated'])} persisted={int(bucket['persisted'])} "
                f"scrape_ms={bucket['scrape_ms']:.1f} process_ms={bucket['process_ms']:.1f}"
            )
        for item in slowest_companies:
            log_msg(
                f"Slowest company | company={item['name']} adapter={item['adapter']} "
                f"elapsed_ms={item['elapsed_ms']} scrape_ms={item['scrape_ms']} process_ms={item['process_ms']} deep={item['deep']}"
            )
        if rejected_rows:
            try:
                with rejected_csv_path.open("a", newline="", encoding="utf-8") as handle:
                    writer = csv.DictWriter(
                        handle,
                        fieldnames=[
                            "company",
                            "title",
                            "location",
                            "url",
                            "source",
                            "adapter",
                            "tier",
                            "score",
                            "fit_band",
                            "work_type",
                            "compensation_unit",
                            "normalized_compensation_usd",
                            "drop_reason",
                            "decision_reason",
                            "matched_keywords",
                            "penalized_keywords",
                        ],
                    )
                    writer.writerows(rejected_rows)
            except Exception:
                pass
            log_msg(f"Rejected jobs saved: {len(rejected_rows)} -> {settings.rejected_csv.name}")
        if blocked_companies:
            try:
                manual_review_path = settings.manual_review_file
                manual_review_path.parent.mkdir(parents=True, exist_ok=True)
                with manual_review_path.open("w", encoding="utf-8") as handle:
                    handle.write("Blocked sites requiring manual review\n")
                    handle.write(f"Generated: {datetime.now().isoformat()}\n\n")
                    for item in sorted(blocked_companies, key=lambda row: row["name"].lower()):
                        handle.write(
                            f"{item['name']} | adapter={item['adapter']} | note={item['note']} | url={item['url']}\n"
                        )
            except Exception:
                pass
            log_msg(f"Manual review required: {len(blocked_companies)} blocked companies -> {settings.manual_review_file.name}")

    def _scrape_company_with_retry(self, company: Dict[str, Any]) -> Dict[str, Any]:
        time.sleep(random.uniform(0.5, 2.0))
        started_at = time.perf_counter()
        try:
            jobs, adapter_name = self._scrape_company(company)
            if not jobs and self.deep_search:
                deep_jobs = self._deep_scrape(company)
                return {
                    "jobs": deep_jobs,
                    "adapter": "deep_search" if deep_jobs else adapter_name,
                    "scrape_ms": round((time.perf_counter() - started_at) * 1000, 1),
                    "used_deep_search": bool(deep_jobs),
                    "status": "ok" if deep_jobs or jobs else "empty",
                }
            return {
                "jobs": jobs,
                "adapter": adapter_name,
                "scrape_ms": round((time.perf_counter() - started_at) * 1000, 1),
                "used_deep_search": False,
                "status": "ok" if jobs else "empty",
            }
        except BlockedSiteError as exc:
            return {
                "jobs": [],
                "adapter": str(company.get("adapter", "unknown")),
                "scrape_ms": round((time.perf_counter() - started_at) * 1000, 1),
                "used_deep_search": False,
                "status": "blocked",
                "note": exc.reason,
            }
        except Exception as exc:
            message = str(exc).lower()
            if any(token in message for token in ["403", "blocked", "forbidden"]) and self.deep_search:
                deep_jobs = self._deep_scrape(company)
                return {
                    "jobs": deep_jobs,
                    "adapter": "deep_search" if deep_jobs else str(company.get("adapter", "unknown")),
                    "scrape_ms": round((time.perf_counter() - started_at) * 1000, 1),
                    "used_deep_search": bool(deep_jobs),
                    "status": "ok" if deep_jobs else "empty",
                }
            raise

    def _scrape_company(self, company: Dict[str, Any]):
        if not isinstance(company, dict):
            raise TypeError(f"Expected company config dict, received {type(company).__name__}")

        adapter_name = str(company.get("adapter", "custom_manual") or "custom_manual").lower()
        careers_url = str(company.get("careers_url", "") or "").lower()
        if adapter_name in {"custom_manual", "custom_blackrock", "custom_schwab", "custom_spglobal"} or not adapter_name:
            if "greenhouse.io" in careers_url:
                adapter_name = "greenhouse"
            elif "lever.co" in careers_url:
                adapter_name = "lever"
            elif "ashbyhq.com" in careers_url:
                adapter_name = "ashby"
            elif "myworkdayjobs.com" in careers_url:
                adapter_name = "workday"
            elif "rippling.com" in careers_url:
                adapter_name = "rippling"
            elif "smartrecruiters.com" in careers_url:
                adapter_name = "smartrecruiters"
            else:
                adapter_name = "generic"

        adapter_cls = self.ADAPTER_MAP.get(adapter_name, GenericAdapter)
        adapter = adapter_cls(session=self.session, scorer=self.scorer)
        jobs = adapter.scrape(company)
        if not isinstance(jobs, list):
            raise TypeError(f"{adapter_cls.__name__}.scrape returned {type(jobs).__name__}, expected list")
        return jobs, adapter_name

    def _deep_scrape(self, company: Dict[str, Any]) -> List[Any]:
        from jobsearch.ats_db import Job
        import hashlib

        careers_url = company.get("careers_url")
        name = company.get("name", "Unknown")
        if not careers_url:
            return []

        try:
            from deep_search import playwright_adapter

            if not playwright_adapter.is_available():
                return []

            raw_jobs = playwright_adapter.scrape_jobs_generic(careers_url, name)
            jobs: List[Job] = []
            for raw in raw_jobs:
                title = raw.get("title", "")
                url = raw.get("url", "")
                job_id = hashlib.md5(f"{name}{title}{url}".encode()).hexdigest()
                jobs.append(
                    Job(
                        id=job_id,
                        company=name,
                        role_title_raw=title,
                        location=raw.get("location", ""),
                        url=url,
                        source=raw.get("source", "Deep Search"),
                        adapter="deep_search",
                        tier=str(company.get("tier", 4)),
                        description_excerpt=raw.get("description", "")[:1000],
                    )
                )
            return jobs
        except Exception as exc:
            logger.error("Deep scrape failed for %s: %s", name, exc)
            return []

    def _process_and_save_jobs(self, company: Dict[str, Any], jobs: List[Any]):
        if not jobs:
            return 0, 0, 0, 0, 0.0, {"avg_score": 0.0, "max_score": 0.0, "keep_rate": 0.0}, []

        started_at = time.perf_counter()
        conn = get_connection()
        persisted_count = 0
        inserted_count = 0
        dropped_count = 0
        score_values: List[float] = []
        rejected_rows: List[Dict[str, Any]] = []

        for job in jobs:
            scoring_data = {
                "title": job.role_title_raw,
                "description": job.description_excerpt,
                "tier": int(company.get("tier", 4)),
                "location": job.location,
                "salary_min": job.salary_min,
                "salary_max": job.salary_max,
                "salary_text": job.salary_text,
                "work_type": getattr(job, "work_type", ""),
                "compensation_unit": getattr(job, "compensation_unit", ""),
                "hourly_rate": getattr(job, "hourly_rate", None),
                "hours_per_week": getattr(job, "hours_per_week", None),
                "weeks_per_year": getattr(job, "weeks_per_year", None),
            }
            score_results = self.scorer.score_job(scoring_data)
            job.score = score_results["score"]
            job.fit_band = score_results["fit_band"]
            job.matched_keywords = score_results["matched_keywords"]
            job.penalized_keywords = score_results["penalized_keywords"]
            job.decision_reason = score_results["decision_reason"]
            job.work_type = score_results.get("work_type", getattr(job, "work_type", ""))
            job.compensation_unit = score_results.get("compensation_unit", getattr(job, "compensation_unit", ""))
            job.hourly_rate = score_results.get("hourly_rate", getattr(job, "hourly_rate", None))
            job.hours_per_week = score_results.get("hours_per_week", getattr(job, "hours_per_week", None))
            job.weeks_per_year = score_results.get("weeks_per_year", getattr(job, "weeks_per_year", None))
            job.normalized_compensation_usd = score_results.get(
                "normalized_compensation_usd",
                getattr(job, "normalized_compensation_usd", None),
            )
            score_values.append(float(job.score or 0.0))

            if job.score >= self.scorer.min_score_to_keep:
                try:
                    inserted, _ = upsert_job(conn, job)
                    persisted_count += 1
                    if inserted:
                        inserted_count += 1
                except Exception as exc:
                    logger.error("Error saving job %s: %s", job.id, exc)
            else:
                dropped_count += 1
                rejected_rows.append(
                    {
                        "company": job.company,
                        "title": job.role_title_raw,
                        "location": job.location,
                        "url": job.url,
                        "source": job.source,
                        "adapter": job.adapter,
                        "tier": company.get("tier", 4),
                        "score": job.score,
                        "fit_band": job.fit_band,
                        "work_type": job.work_type,
                        "compensation_unit": job.compensation_unit,
                        "normalized_compensation_usd": job.normalized_compensation_usd,
                        "drop_reason": "score_below_threshold",
                        "decision_reason": job.decision_reason,
                        "matched_keywords": job.matched_keywords,
                        "penalized_keywords": job.penalized_keywords,
                    }
                )

        conn.commit()
        conn.close()
        evaluated_count = len(jobs)
        process_ms = round((time.perf_counter() - started_at) * 1000, 1)
        keep_rate = (persisted_count / evaluated_count) if evaluated_count else 0.0
        score_stats = {
            "avg_score": (sum(score_values) / len(score_values)) if score_values else 0.0,
            "max_score": max(score_values) if score_values else 0.0,
            "keep_rate": keep_rate,
        }
        return persisted_count, inserted_count, dropped_count, evaluated_count, process_ms, score_stats, rejected_rows
