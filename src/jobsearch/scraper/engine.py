"""Scraper engine: orchestrates multi-threaded scraping and persistence."""

import csv
import json
import logging
import os
import random
import re
import time
from concurrent.futures import FIRST_COMPLETED, ThreadPoolExecutor, wait
from datetime import datetime, timezone
from threading import BoundedSemaphore
from typing import Any, Dict, List, Type
from urllib.parse import urlparse

from jobsearch.config.settings import BASE_DIR, get_runtime_setting, get_shared_session, rotate_log_file, settings
from jobsearch import ats_db as db
from jobsearch.db.connection import get_connection
from jobsearch.scraper.adapters.ashby import AshbyAdapter
from jobsearch.scraper.adapters.adzuna import AdzunaAdapter
from jobsearch.scraper.adapters.base import BaseAdapter, BlockedSiteError
from jobsearch.scraper.adapters.crawl4ai_adapter import Crawl4AIAdapter
from jobsearch.scraper.adapters.dice import DiceAdapter
from jobsearch.scraper.adapters.generic import GenericAdapter
from jobsearch.scraper.adapters.google_careers import GoogleCareersAdapter
from jobsearch.scraper.adapters.greenhouse import GreenhouseAdapter
from jobsearch.scraper.adapters.indeed_connector import IndeedConnectorAdapter
from jobsearch.scraper.adapters.jooble import JoobleAdapter
from jobsearch.scraper.adapters.lever import LeverAdapter
from jobsearch.scraper.adapters.motionrecruitment import MotionRecruitmentAdapter
from jobsearch.scraper.adapters.rippling import RipplingAdapter
from jobsearch.scraper.adapters.jobspy_experimental import JobSpyExperimentalAdapter
from jobsearch.scraper.adapters.smartrecruiters import SmartRecruitersAdapter
from jobsearch.scraper.adapters.themuse import TheMuseAdapter
from jobsearch.scraper.adapters.usajobs import USAJobsAdapter
from jobsearch.scraper.adapters.workday import WorkdayAdapter
from jobsearch.scraper.scoring import Scorer
from jobsearch.services.opportunity_service import upsert_job
from jobsearch.services.enrichment_service import EnrichmentService

logger = logging.getLogger(__name__)

GENERIC_POSITIVE_URL_MARKERS = (
    "/careers",
    "/career",
    "/jobs",
    "/job-search",
    "/join-us",
    "/openings",
    "/positions",
    "/work-with-us",
)
GENERIC_NEGATIVE_URL_MARKERS = (
    "/about",
    "/leadership",
    "/company",
    "/solutions",
    "/solution",
    "/platform",
    "/products",
    "/product",
    "/integrations",
    "/integration",
    "/segments",
    "/segment",
    "/investor",
    "/news",
    "/blog",
    "/press",
    "/customers",
    "/contact",
)

_REJECTED_CSV_FIELDS = [
    "company", "title", "location", "url", "source", "adapter",
    "tier", "score", "fit_band", "work_type", "compensation_unit",
    "normalized_compensation_usd", "drop_reason", "decision_reason",
    "matched_keywords", "penalized_keywords",
]


class ScraperEngine:
    ADAPTER_MAP: Dict[str, Type[BaseAdapter]] = {
        "greenhouse": GreenhouseAdapter,
        "lever": LeverAdapter,
        "motionrecruitment": MotionRecruitmentAdapter,
        "ashby": AshbyAdapter,
        "workday": WorkdayAdapter,
        "workday_manual": WorkdayAdapter,
        "rippling": RipplingAdapter,
        "smartrecruiters": SmartRecruitersAdapter,
        "usajobs": USAJobsAdapter,
        "adzuna": AdzunaAdapter,
        "jooble": JoobleAdapter,
        "themuse": TheMuseAdapter,
        "indeed_connector": IndeedConnectorAdapter,
        "jobspy": JobSpyExperimentalAdapter,
        "google_careers": GoogleCareersAdapter,
        "dice": DiceAdapter,
        "crawl4ai": Crawl4AIAdapter,
        "generic": GenericAdapter,
        "custom_manual": GenericAdapter,
        "custom_site": GenericAdapter,
        "custom_blackrock": GenericAdapter,
        "custom_schwab": GenericAdapter,
        "custom_spglobal": GenericAdapter,
    }

    def __init__(self, preferences: Dict[str, Any], companies: List[Dict[str, Any]], deep_search: bool = False, full_refresh: bool = False):
        self.prefs = preferences
        self.deep_search = deep_search
        self.full_refresh = full_refresh
        self.companies = [
            company
            for company in companies
            if (
                isinstance(company, dict)
                and company.get("active", True)
                and not company.get("manual_only", False)
                and company.get("name")
            )
        ]
        self.scorer = Scorer(preferences)
        self.session = get_shared_session()
        self._adapter_semaphores = self._init_semaphores()
        self.bulk_cooldowns: Dict[str, datetime] = {}
        self.known_urls: Dict[str, datetime] = {}

        self._deep_playwright = None
        if self.deep_search:
            try:
                from deep_search import playwright_adapter

                if playwright_adapter.is_available():
                    self._deep_playwright = playwright_adapter
            except Exception:
                self._deep_playwright = None
        self._adapter_semaphores = self._build_adapter_semaphores()

    def _build_adapter_semaphores(self) -> Dict[str, BoundedSemaphore]:
        limits = {
            "workday": settings.scrape_workday_concurrency,
            "generic": settings.scrape_generic_concurrency,
            "greenhouse": settings.scrape_greenhouse_concurrency,
            "lever": settings.scrape_lever_concurrency,
            "ashby": settings.scrape_ashby_concurrency,
            "rippling": settings.scrape_rippling_concurrency,
            "smartrecruiters": settings.scrape_smartrecruiters_concurrency,
            "usajobs": settings.scrape_usajobs_concurrency,
            "adzuna": settings.scrape_adzuna_concurrency,
            "jooble": settings.scrape_jooble_concurrency,
            "themuse": settings.scrape_themuse_concurrency,
            "indeed_connector": settings.scrape_indeed_connector_concurrency,
            "jobspy": int(get_runtime_setting("jobspy_concurrency", str(settings.scrape_jobspy_concurrency)) or settings.scrape_jobspy_concurrency),
            "google_careers": 2, # Small limit for Google
            "dice": settings.scrape_dice_concurrency,
            "motionrecruitment": settings.scrape_motionrecruitment_concurrency,
            "deep_search": settings.scrape_deep_search_concurrency,
        }
        semaphores: Dict[str, BoundedSemaphore] = {}
        for adapter_name, limit in limits.items():
            safe_limit = max(1, int(limit))
            semaphores[adapter_name] = BoundedSemaphore(safe_limit)
        return semaphores

    def _resolve_adapter_name(self, company: Dict[str, Any]) -> str:
        adapter_name = str(company.get("adapter", "custom_manual") or "custom_manual").lower()
        careers_url = str(company.get("careers_url", "") or "").lower()
        if adapter_name in {"custom_manual", "custom_site", "custom_blackrock", "custom_schwab", "custom_spglobal"} or not adapter_name:
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
                adapter_name = "crawl4ai"
        return adapter_name

    def run(self, max_workers: int = 12):
        log_path = settings.log_file
        log_path.parent.mkdir(parents=True, exist_ok=True)
        rotate_log_file(log_path)
        try:
            log_path.touch(exist_ok=True)
        except Exception:
            pass

        total_companies = len(self.companies)
        rejected_csv_path = settings.rejected_csv
        main_conn = get_connection()

        # Pre-fetch all cooldowns to avoid per-worker DB overhead
        self.bulk_cooldowns: Dict[str, datetime] = {}
        self.known_urls: Dict[str, datetime] = {}
        try:
            now = datetime.now(timezone.utc)
            for table in ["workday_target_health", "generic_target_health"]:
                rows = main_conn.execute(f"SELECT company, cooldown_until FROM {table} WHERE cooldown_until IS NOT NULL").fetchall()
                for r in rows:
                    try:
                        until = datetime.fromisoformat(str(r["cooldown_until"]))
                        if until.tzinfo is None:
                            until = until.replace(tzinfo=timezone.utc)
                        if until > now:
                            self.bulk_cooldowns[r["company"]] = until
                    except Exception:
                        pass
            
            # Pre-load known job URLs to support incremental scraping
            if not self.full_refresh:
                job_rows = main_conn.execute("SELECT job_url, updated_at FROM applications WHERE job_url IS NOT NULL").fetchall()
                for r in job_rows:
                    try:
                        updated_at = datetime.fromisoformat(str(r["updated_at"]))
                        if updated_at.tzinfo is None:
                            updated_at = updated_at.replace(tzinfo=timezone.utc)
                        self.known_urls[r["job_url"]] = updated_at
                    except Exception:
                        pass
                if self.known_urls:
                    logger.info("Loaded %d known URLs for incremental scraping", len(self.known_urls))
        except Exception as e:
            logger.warning("Failed to pre-fetch cooldowns or known URLs: %s", e)

        try:
            with log_path.open("a", encoding="utf-8") as log_handle:
                def log_msg(message: str):
                    print(message, flush=True)
                    try:
                        log_handle.write(f"{datetime.now().strftime('%H:%M:%S')} | {message}\n")
                        log_handle.flush()
                    except Exception:
                        pass

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
                try:
                    rejected_csv_path.parent.mkdir(parents=True, exist_ok=True)
                    with rejected_csv_path.open("w", newline="", encoding="utf-8") as handle:
                        writer = csv.DictWriter(handle, fieldnames=_REJECTED_CSV_FIELDS)
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
                                persisted, inserted, dropped, evaluated, process_ms, score_stats, company_rejected_rows = self._process_and_save_jobs(main_conn, company, jobs)
                                self._update_scrape_health(main_conn, company, adapter_name, scrape_status, scrape_ms, scrape_note, evaluated)
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
                slowest_companies = sorted(slowest_companies, key=lambda item: item["elapsed_ms"], reverse=True)[:10]
                for item in slowest_companies:
                    log_msg(
                        f"Slowest company | company={item['name']} adapter={item['adapter']} "
                        f"elapsed_ms={item['elapsed_ms']} scrape_ms={item['scrape_ms']} process_ms={item['process_ms']} deep={item['deep']}"
                    )
                if rejected_rows:
                    try:
                        with rejected_csv_path.open("a", newline="", encoding="utf-8") as handle:
                            writer = csv.DictWriter(handle, fieldnames=_REJECTED_CSV_FIELDS)
                            writer.writerows(rejected_rows)
                    except Exception:
                        pass
                    log_msg(f"Rejected jobs saved: {len(rejected_rows)} -> {settings.rejected_csv.name}")
                
                # Update last run timestamp in settings table
                try:
                    db.set_setting(main_conn, "last_pipeline_run", datetime.now(timezone.utc).isoformat())
                except Exception:
                    pass

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
        finally:
            main_conn.close()

    def _scrape_company_with_retry(self, company: Dict[str, Any]) -> Dict[str, Any]:
        min_jitter = min(settings.scrape_jitter_min_ms, settings.scrape_jitter_max_ms) / 1000.0
        max_jitter = max(settings.scrape_jitter_min_ms, settings.scrape_jitter_max_ms) / 1000.0
        time.sleep(random.uniform(min_jitter, max_jitter))
        started_at = time.perf_counter()
        name = company.get("name", "")
        adapter_hint = self._resolve_adapter_name(company)

        # FAST EXIT: Check pre-fetched cooldowns
        if hasattr(self, "bulk_cooldowns") and name in self.bulk_cooldowns:
            until = self.bulk_cooldowns[name]
            return {
                "jobs": [],
                "adapter": adapter_hint,
                "scrape_ms": 0.0,
                "used_deep_search": False,
                "status": "cooldown",
                "note": f"Cooldown until {until.isoformat()}",
            }

        cooldown_adapter, cooldown_reason = self._cooldown_reason(company)
        if cooldown_reason:
            return {
                "jobs": [],
                "adapter": cooldown_adapter,
                "scrape_ms": 0.0,
                "used_deep_search": False,
                "status": "cooldown",
                "note": cooldown_reason,
            }
        generic_low_signal_reason = self._generic_low_signal_reason(company)
        if generic_low_signal_reason:
            return {
                "jobs": [],
                "adapter": "generic",
                "scrape_ms": 0.0,
                "used_deep_search": False,
                "status": "low_signal",
                "note": generic_low_signal_reason,
            }
        scrape_semaphore = self._adapter_semaphores.get(adapter_hint)
        if scrape_semaphore is None:
            logger.warning("No semaphore configured for adapter %r; defaulting to concurrency=1", adapter_hint)
            scrape_semaphore = BoundedSemaphore(1)
        with scrape_semaphore:
            started_at = time.perf_counter()
            try:
                jobs, adapter_name, adapter_status, adapter_note = self._scrape_company(company)
                if not jobs and self.deep_search:
                    deep_jobs = self._deep_scrape(company)
                    return {
                        "jobs": deep_jobs,
                        "adapter": "deep_search" if deep_jobs else adapter_name,
                        "scrape_ms": round((time.perf_counter() - started_at) * 1000, 1),
                        "used_deep_search": bool(deep_jobs),
                        "status": "ok" if deep_jobs or jobs else adapter_status,
                        "note": "" if deep_jobs else adapter_note,
                    }
                return {
                    "jobs": jobs,
                    "adapter": adapter_name,
                    "scrape_ms": round((time.perf_counter() - started_at) * 1000, 1),
                    "used_deep_search": False,
                    "status": "ok" if jobs else adapter_status,
                    "note": adapter_note,
                }
            except BlockedSiteError as exc:
                if self.deep_search:
                    deep_jobs = self._deep_scrape(company)
                    return {
                        "jobs": deep_jobs,
                        "adapter": "deep_search" if deep_jobs else str(company.get("adapter", "unknown")),
                        "scrape_ms": round((time.perf_counter() - started_at) * 1000, 1),
                        "used_deep_search": bool(deep_jobs),
                        "status": "ok" if deep_jobs else "blocked",
                        "note": "" if deep_jobs else exc.reason,
                    }
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

        adapter_name = self._resolve_adapter_name(company)

        adapter_cls = self.ADAPTER_MAP.get(adapter_name, GenericAdapter)
        adapter = adapter_cls(session=self.session, scorer=self.scorer)
        
        # Support incremental scraping if the adapter allows it
        if hasattr(adapter, "set_known_urls") and hasattr(self, "known_urls"):
            adapter.set_known_urls(self.known_urls)
            
        jobs = adapter.scrape(company)
        if not isinstance(jobs, list):
            raise TypeError(f"{adapter_cls.__name__}.scrape returned {type(jobs).__name__}, expected list")
        adapter_status = getattr(adapter, "last_status", "empty" if not jobs else "ok")
        adapter_note = getattr(adapter, "last_note", "")
        return jobs, adapter_name, adapter_status, adapter_note

    def _cooldown_reason(self, company: Dict[str, Any]):
        """Return (adapter, reason) if the company is on cooldown, else ('', '').

        Opens one DB connection per company instead of the previous two.
        """
        careers_url = str(company.get("careers_url", "") or "").lower()
        adapter_name = str(company.get("adapter", "") or "").lower()
        name = company.get("name", "")
        now = datetime.now(timezone.utc)

        is_workday = "myworkdayjobs.com" in careers_url or adapter_name in {"workday", "workday_manual"}
        is_generic = adapter_name == "generic" and not is_workday
        if not is_workday and not is_generic:
            return "", ""

        conn = db.get_connection()
        try:
            if is_workday:
                row = db.get_workday_target_health(conn, name)
                if row and row["cooldown_until"]:
                    try:
                        until = datetime.fromisoformat(str(row["cooldown_until"]))
                        if until.tzinfo is None:
                            until = until.replace(tzinfo=timezone.utc)
                        if until > now:
                            return "workday", f"Cooldown until {until.isoformat()}"
                    except Exception:
                        pass
            if is_generic:
                row = db.get_generic_target_health(conn, name)
                if row and row["cooldown_until"]:
                    try:
                        until = datetime.fromisoformat(str(row["cooldown_until"]))
                        if until.tzinfo is None:
                            until = until.replace(tzinfo=timezone.utc)
                        if until > now:
                            return "generic", f"Cooldown until {until.isoformat()}"
                    except Exception:
                        pass
        finally:
            conn.close()
        return "", ""

    def _generic_low_signal_reason(self, company: Dict[str, Any]) -> str:
        careers_url = str(company.get("careers_url", "") or "").strip()
        adapter_name = str(company.get("adapter", "") or "").lower()
        if adapter_name != "generic" or not careers_url:
            return ""
        if company.get("contractor_source"):
            return ""

        url_l = careers_url.lower()
        if any(marker in url_l for marker in GENERIC_POSITIVE_URL_MARKERS):
            return ""
        if any(marker in url_l for marker in GENERIC_NEGATIVE_URL_MARKERS):
            return "Low-signal generic URL"

        parsed = urlparse(careers_url)
        path = parsed.path or "/"
        clean_path = re.sub(r"/+", "/", path).rstrip("/") or "/"
        if clean_path == "/":
            return "Generic URL points to site root"
        return ""

    def _update_scrape_health(
        self,
        conn,
        company: Dict[str, Any],
        adapter_name: str,
        scrape_status: str,
        scrape_ms: float,
        scrape_note: str,
        evaluated_count: int,
    ) -> None:
        if adapter_name != "workday":
            if adapter_name == "generic":
                self._update_generic_scrape_health(conn, company, scrape_status, scrape_ms, scrape_note, evaluated_count)
            return
        cooldown_days = 0
        existing = db.get_workday_target_health(conn, company.get("name", ""))
        success_count = int(existing["success_count"]) if existing else 0
        streak = int(existing["empty_streak"]) if existing else 0
        if (
            scrape_status == "budget_exhausted"
            and evaluated_count == 0
            and success_count == 0
            and streak + 1 >= settings.workday_empty_cooldown_threshold
        ):
            cooldown_days = settings.workday_empty_cooldown_days
        elif (
            scrape_status == "blocked"
            and evaluated_count == 0
            and streak + 1 >= settings.workday_empty_cooldown_threshold
        ):
            cooldown_days = settings.workday_empty_cooldown_days
        db.update_workday_target_health(
            conn,
            company=company.get("name", ""),
            careers_url=str(company.get("careers_url", "") or ""),
            status=scrape_status if scrape_status in {"ok", "empty", "budget_exhausted", "cooldown", "blocked"} else "ok",
            elapsed_ms=scrape_ms,
            evaluated_count=evaluated_count,
            cooldown_days=cooldown_days,
            notes=scrape_note,
        )

    def _update_generic_scrape_health(
        self,
        conn,
        company: Dict[str, Any],
        scrape_status: str,
        scrape_ms: float,
        scrape_note: str,
        evaluated_count: int,
    ) -> None:
        cooldown_days = 0
        existing = db.get_generic_target_health(conn, company.get("name", ""))
        success_count = int(existing["success_count"]) if existing else 0
        streak = int(existing["empty_streak"]) if existing else 0
        low_priority = str(company.get("priority", "") or "").lower() in {"low", "medium", ""}
        if (
            scrape_status == "empty"
            and evaluated_count == 0
            and scrape_ms >= settings.generic_slow_empty_ms
            and success_count == 0
            and streak + 1 >= settings.generic_empty_cooldown_threshold
            and low_priority
        ):
            cooldown_days = settings.generic_empty_cooldown_days
        elif (
            scrape_status == "low_signal"
            and evaluated_count == 0
            and success_count == 0
            and low_priority
        ):
            cooldown_days = settings.generic_low_signal_cooldown_days
        elif (
            scrape_status == "blocked"
            and evaluated_count == 0
            and streak + 1 >= settings.generic_empty_cooldown_threshold
            and low_priority
        ):
            cooldown_days = settings.generic_empty_cooldown_days
        db.update_generic_target_health(
            conn,
            company=company.get("name", ""),
            careers_url=str(company.get("careers_url", "") or ""),
            status=scrape_status if scrape_status in {"ok", "empty", "cooldown", "low_signal", "blocked"} else "ok",
            elapsed_ms=scrape_ms,
            evaluated_count=evaluated_count,
            cooldown_days=cooldown_days,
            notes=scrape_note,
        )

    def _deep_scrape(self, company: Dict[str, Any]) -> List[Any]:
        from jobsearch.ats_db import Job
        import hashlib

        careers_url = company.get("careers_url")
        name = company.get("name", "Unknown")
        if not careers_url:
            return []

        try:
            if not self._deep_playwright:
                return []

            deep_semaphore = self._adapter_semaphores.get("deep_search")
            if deep_semaphore is None:
                deep_semaphore = BoundedSemaphore(1)
            with deep_semaphore:
                raw_jobs = self._deep_playwright.scrape_jobs_generic(careers_url, name)
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
                        description_excerpt=raw.get("description", ""),
                    )
                )
            return jobs
        except Exception as exc:
            logger.error("Deep scrape failed for %s: %s", name, exc)
            return []

    def _process_and_save_jobs(self, conn, company: Dict[str, Any], jobs: List[Any]):
        if not jobs:
            return 0, 0, 0, 0, 0.0, {"avg_score": 0.0, "max_score": 0.0, "keep_rate": 0.0}, []

        started_at = time.perf_counter()
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
                "source_lane": str(company.get("source_lane") or "employer_ats"),
                "canonical_job_url": getattr(job, "canonical_job_url", ""),
            }
            score_results = self.scorer.score_job(scoring_data)
            job.score = score_results["score"]
            job.fit_band = score_results["fit_band"]
            job.apply_now_eligible = score_results.get("apply_now_eligible", True)
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
            job.source_lane = str(company.get("source_lane") or getattr(job, "source_lane", "employer_ats") or "employer_ats")
            job.canonical_job_url = str(getattr(job, "canonical_job_url", "") or "")
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
                        "drop_reason": "disqualified" if job.fit_band == "Disqualified" else "score_below_threshold",
                        "decision_reason": job.decision_reason,
                        "matched_keywords": job.matched_keywords,
                        "penalized_keywords": job.penalized_keywords,
                    }
                )

        conn.commit()
        evaluated_count = len(jobs)
        process_ms = round((time.perf_counter() - started_at) * 1000, 1)
        keep_rate = (persisted_count / evaluated_count) if evaluated_count else 0.0
        score_stats = {
            "avg_score": (sum(score_values) / len(score_values)) if score_values else 0.0,
            "max_score": max(score_values) if score_values else 0.0,
            "keep_rate": keep_rate,
        }
        return persisted_count, inserted_count, dropped_count, evaluated_count, process_ms, score_stats, rejected_rows

    def enrich_jobs_with_ai(self, min_score_threshold: float = 60.0, max_jobs: int = 50) -> Dict[str, Any]:
        """
        Enriches high-scoring jobs with AI analysis for visa sponsorship, tech stack, and IC vs Manager.
        This is an optional post-processing step that improves scoring accuracy.

        Args:
            min_score_threshold: Only enrich jobs with score >= this value
            max_jobs: Maximum number of jobs to enrich (LLM calls are expensive)

        Returns:
            Dict with enrichment statistics
        """
        # Extract user skills from preferences for missing-skills detection
        user_skills = self.prefs.get("profile", {}).get("user_skills", [])

        # Get API keys from database settings or environment
        enricher_conn = get_connection()
        google_api_key = db.get_setting(enricher_conn, "google_api_key", default=os.getenv("GOOGLE_API_KEY", ""))
        openai_api_key = db.get_setting(enricher_conn, "openai_api_key", default=os.getenv("OPENAI_API_KEY", ""))
        preferred_provider = db.get_setting(enricher_conn, "llm_provider", default="")
        ollama_base_url = db.get_setting(enricher_conn, "ollama_base_url", default="")
        ollama_model = db.get_setting(enricher_conn, "ollama_model", default="")

        enricher = EnrichmentService(
            google_api_key=google_api_key,
            openai_api_key=openai_api_key,
            preferred_provider=preferred_provider or None,
            ollama_base_url=ollama_base_url or None,
            ollama_model=ollama_model or None,
            user_skills=user_skills,
            db_conn=enricher_conn,
        )
        conn = enricher_conn
        cursor = conn.cursor()

        # Fetch high-scoring jobs that haven't been enriched yet
        cursor.execute(
            """
            SELECT id, company, role_title_raw, description_excerpt
            FROM applications
            WHERE score >= ? AND enriched_data IS NULL
            ORDER BY score DESC
            LIMIT ?
            """,
            (min_score_threshold, max_jobs),
        )
        jobs_to_enrich = cursor.fetchall()
        logger.info(f"Enriching {len(jobs_to_enrich)} jobs with AI analysis")

        enriched_count = 0
        failed_count = 0

        for job_id, company, title, description in jobs_to_enrich:
            if not description:
                logger.warning(f"Job {job_id} has no description, skipping enrichment")
                continue

            try:
                enriched_data = enricher.enrich_job(title, description)
                enriched_json = json.dumps(enriched_data)

                # Update the database with enriched data
                cursor.execute(
                    "UPDATE applications SET enriched_data = ? WHERE id = ?",
                    (enriched_json, job_id),
                )

                # Now apply enrichment adjustments to the score
                if enriched_data.get("enrichment_status") == "success":
                    cursor.execute("SELECT * FROM applications WHERE id = ?", (job_id,))
                    job_row = cursor.fetchone()
                    if job_row:
                        # Get current score result
                        cursor.execute(
                            "SELECT score, fit_band, decision_reason FROM applications WHERE id = ?",
                            (job_id,),
                        )
                        score, fit_band, decision_reason = cursor.fetchone()
                        score_result = {
                            "score": float(score or 0.0),
                            "fit_band": fit_band,
                            "decision_reason": decision_reason,
                        }

                        # Apply enrichment adjustments
                        adjusted_result = self.scorer.apply_enrichment_adjustments(
                            score_result, enriched_data
                        )

                        if adjusted_result.get("enrichment_adjustment") != 0:
                            cursor.execute(
                                "UPDATE applications SET score = ?, fit_band = ?, decision_reason = ? WHERE id = ?",
                                (
                                    adjusted_result["score"],
                                    adjusted_result["fit_band"],
                                    adjusted_result["decision_reason"],
                                    job_id,
                                ),
                            )
                            logger.info(
                                f"Job {job_id}: enrichment adjustment={adjusted_result['enrichment_adjustment']}, "
                                f"new_score={adjusted_result['score']}"
                            )

                enriched_count += 1
            except Exception as exc:
                logger.error(f"Failed to enrich job {job_id}: {exc}")
                failed_count += 1

        conn.commit()
        conn.close()

        return {
            "total_enriched": enriched_count,
            "failed": failed_count,
            "jobs_processed": len(jobs_to_enrich),
        }
