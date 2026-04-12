"""Scraper engine: orchestrates multi-threaded scraping and persistence."""

import csv
import json
import logging
import os
import random
import re
import sqlite3
import time
from concurrent.futures import FIRST_COMPLETED, ThreadPoolExecutor, wait
from datetime import datetime, timezone, timedelta
from threading import BoundedSemaphore
from typing import Any, Dict, List, Type, Optional
from urllib.parse import urlparse

from jobsearch.config.settings import BASE_DIR, get_runtime_setting, get_shared_session, get_headers, rotate_log_file, settings
from jobsearch.services.heal_evidence import HealEvidenceWriter
from jobsearch import ats_db
from jobsearch import ats_db as db
from jobsearch.db.connection import get_connection
from jobsearch.scraper.adapters.ashby import AshbyAdapter
from jobsearch.scraper.adapters.bamboohr import BambooHRAdapter
from jobsearch.scraper.adapters.breezy import BreezyAdapter
from jobsearch.scraper.adapters.workable import WorkableAdapter
from jobsearch.scraper.adapters.adzuna import AdzunaAdapter
from jobsearch.scraper.adapters.base import BaseAdapter, BlockedSiteError
from jobsearch.scraper.adapters.crawl4ai_adapter import Crawl4AIAdapter
from jobsearch.scraper.adapters.careeronestop import CareerOneStopAdapter
from jobsearch.scraper.adapters.remotive import RemotiveAdapter
from jobsearch.scraper.adapters.remoteok import RemoteOKAdapter
from jobsearch.scraper.adapters.wwr import WWRAdapter
from jobsearch.scraper.adapters.findwork import FindworkAdapter
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
from jobsearch.scraper.adapters.jobvite import JobviteAdapter
from jobsearch.scraper.adapters.smartrecruiters import SmartRecruitersAdapter
from jobsearch.scraper.adapters.themuse import TheMuseAdapter
from jobsearch.scraper.adapters.usajobs import USAJobsAdapter
from jobsearch.scraper.adapters.workday import WorkdayAdapter
from jobsearch.scraper.ats_routing import choose_extraction_route, fingerprint_ats, FailureClassifier
from jobsearch.scraper.scheduler_policy import SchedulerPolicy, EscalationPolicy
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
        "bamboohr": BambooHRAdapter,
        "breezy": BreezyAdapter,
        "workable": WorkableAdapter,
        "jobvite": JobviteAdapter,
        "workday": WorkdayAdapter,
        "workday_manual": WorkdayAdapter,
        "rippling": RipplingAdapter,
        "smartrecruiters": SmartRecruitersAdapter,
        "usajobs": USAJobsAdapter,
        "adzuna": AdzunaAdapter,
        "jooble": JoobleAdapter,
        "themuse": TheMuseAdapter,
        "indeed_connector": IndeedConnectorAdapter,
        "careeronestop": CareerOneStopAdapter,
        "remotive": RemotiveAdapter,
        "remoteok": RemoteOKAdapter,
        "wwr": WWRAdapter,
        "findwork": FindworkAdapter,
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
        self.policy = SchedulerPolicy(settings)
        self.escalation_policy = EscalationPolicy(settings)
        self.session = get_shared_session()
        self.bulk_cooldowns: Dict[str, datetime] = {}
        self.known_urls: Dict[str, datetime] = {}

        # V2 scoring — built once at startup, used per-job in _process_and_save_jobs
        try:
            from jobsearch.scraper.scoring_v2 import build_v2_config_from_prefs
            self._v2_cfg, self._v2_title_index, self._v2_pw, self._v2_ftbase, self._v2_ftmin = (
                build_v2_config_from_prefs(preferences)
            )
            self._v2_available = True
        except Exception as _v2_init_err:
            logger.warning("V2 scoring unavailable: %s", _v2_init_err)
            self._v2_available = False

        self._deep_playwright = None
        if self.deep_search:
            try:
                from deep_search import playwright_adapter

                if playwright_adapter.is_available():
                    self._deep_playwright = playwright_adapter
            except Exception:
                self._deep_playwright = None
        self._adapter_semaphores = self._build_adapter_semaphores()
        self._evidence_writer = HealEvidenceWriter()

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
            "careeronestop": settings.scrape_careeronestop_concurrency,
            "remotive": 1, # Remotive is very fast and doesn't need high concurrency
            "remoteok": 1,
            "wwr": 1,
            "findwork": 1,
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

    @staticmethod
    def _normalize_deep_scrape_result(result: Any) -> tuple[List[Any], Dict[str, Any]]:
        if isinstance(result, tuple) and len(result) == 2:
            jobs, evidence = result
            return list(jobs or []), dict(evidence or {})
        return list(result or []), {}

    def _resolve_adapter_name(self, company: Dict[str, Any]) -> str:
        adapter_name = str(company.get("adapter", "") or "generic").lower()
        careers_url = str(company.get("careers_url", "") or "").lower()
        
        # If the adapter is already a known specific one, keep it
        if adapter_name in self.ADAPTER_MAP and adapter_name not in {
            "generic", "custom_manual", "custom_site", "custom_blackrock", 
            "custom_schwab", "custom_spglobal", "crawl4ai"
        }:
            return adapter_name
            
        # Try to resolve from careers_url
        if "greenhouse.io" in careers_url:
            return "greenhouse"
        if "lever.co" in careers_url:
            return "lever"
        if "ashbyhq.com" in careers_url:
            return "ashby"
        if "myworkdayjobs.com" in careers_url:
            return "workday"
        if "rippling.com" in careers_url:
            return "rippling"
        if "smartrecruiters.com" in careers_url:
            return "smartrecruiters"
            
        # Custom known endpoints
        if "schwab" in careers_url:
            return "generic"
        if "snowflake" in careers_url:
            return "generic"
        if "cwan.com" in careers_url:
            return "generic"
            
        # Default fallback
        if adapter_name == "generic":
            return "generic"
            
        return "crawl4ai"

    def _log_scheduling_decision(self, decision: Any):
        """Log scheduling decision to a structured JSONL file."""
        try:
            path = settings.results_dir / "scheduling_decisions.jsonl"
            path.parent.mkdir(parents=True, exist_ok=True)
            with path.open("a", encoding="utf-8") as f:
                f.write(json.dumps({
                    "timestamp": datetime.now(timezone.utc).isoformat(),
                    "company": decision.company,
                    "action": decision.action,
                    "priority": round(decision.priority_score, 2),
                    "reason": decision.reason,
                    "cooldown_active": decision.cooldown_active,
                    "manual_review": decision.manual_review_required,
                    "queue_healer": decision.queue_healer,
                    **decision.metadata
                }) + "\n")
        except Exception:
            pass

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

        # 1. Pre-fetch health and known URLs
        self.known_urls: Dict[str, datetime] = {}
        all_health: Dict[str, Dict[str, Any]] = {}
        try:
            health_rows = ats_db.get_all_board_health(main_conn)
            all_health = {r["company"]: dict(r) for r in health_rows}
            
            if not self.full_refresh:
                job_rows = main_conn.execute(
                    "SELECT job_url, updated_at FROM applications "
                    "WHERE job_url IS NOT NULL AND updated_at >= datetime('now', '-30 days')"
                ).fetchall()
                for r in job_rows:
                    try:
                        updated_at = datetime.fromisoformat(str(r["updated_at"]))
                        if updated_at.tzinfo is None:
                            updated_at = updated_at.replace(tzinfo=timezone.utc)
                        self.known_urls[r["job_url"]] = updated_at
                    except Exception:
                        pass
        except Exception as e:
            logger.warning("Failed to pre-fetch health or known URLs: %s", e)

        # 2. Generate scheduling decisions and sort queue
        scheduled_tasks = []
        healer_tasks = []
        for company in self.companies:
            health = all_health.get(company["name"])
            decision = self.policy.decide(company, health, force_override=self.full_refresh)
            self._log_scheduling_decision(decision)
            
            if decision.action == "skip":
                continue
            
            if decision.action == "heal" and settings.scheduler_healer_auto_trigger:
                healer_tasks.append((company, decision))
                continue
                
            scheduled_tasks.append((company, decision))
            
        # Sort by priority score (descending)
        scheduled_tasks.sort(key=lambda x: x[1].priority_score, reverse=True)
        total_to_run = len(scheduled_tasks)

        try:
            with log_path.open("a", encoding="utf-8") as log_handle:
                def log_msg(message: str):
                    print(message, flush=True)
                    try:
                        log_handle.write(f"{datetime.now().strftime('%H:%M:%S')} | {message}\n")
                        log_handle.flush()
                    except Exception:
                        pass

                log_msg(f"Starting health-aware scraper engine: {total_to_run}/{total_companies} companies scheduled.")
                if healer_tasks:
                    log_msg(f"Auto-triggering healer for {len(healer_tasks)} companies.")

                # 3. Handle Auto-Healer tasks (sequential or small pool to avoid browser thrashing)
                if healer_tasks:
                    from jobsearch.services.healer_service import ATSHealer
                    healer = ATSHealer()
                    for company, decision in healer_tasks:
                        log_msg(f"Auto-heal: {company['name']} ({decision.reason})")
                        try:
                            healer.discover(company, force=True, ignore_cooldown=True)
                        except Exception as e:
                            logger.error("Auto-heal failed for %s: %s", company["name"], e)

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
                    f"Pipeline run start | scheduled={total_to_run} deep_search={self.deep_search} "
                    f"workers={max_workers} prefs={settings.preferences_yaml.name}"
                )

                with ThreadPoolExecutor(max_workers=max_workers) as executor:
                    futures_map = {
                        executor.submit(self._scrape_company_with_retry, company, decision): company
                        for company, decision in scheduled_tasks
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
                                route_info = scrape_result.get("route") or {}
                                browser_evidence = scrape_result.get("browser_evidence") or {}
                                persisted, inserted, dropped, evaluated, process_ms, score_stats, company_rejected_rows = self._process_and_save_jobs(main_conn, company, jobs)
                                self._update_scrape_health(
                                    main_conn,
                                    company,
                                    adapter_name,
                                    scrape_status,
                                    scrape_ms,
                                    scrape_note,
                                    evaluated,
                                    extraction_method=scrape_result.get("extraction_method", ""),
                                    status_code=scrape_result.get("status_code"),
                                    html=scrape_result.get("html_sample", ""),
                                    ats_family=route_info.get("ats_family", "unknown"),
                                    final_url=str(company.get("careers_url", "") or ""),
                                )

                                failure_reason = self._classify_empty_result(scrape_status, scrape_note, company) if evaluated == 0 else None
                                self._evidence_writer.write(
                                    company=str(company.get("name", "")),
                                    input_url=str(company.get("careers_url", "") or ""),
                                    adapter=adapter_name,
                                    adapter_key=str(company.get("adapter_key", "") or ""),
                                    final_url=str(company.get("careers_url", "") or ""),
                                    status=scrape_status,
                                    board_state=scrape_note,
                                    failure_reason=failure_reason,
                                    jobs_found=evaluated,
                                    elapsed_ms=scrape_ms,
                                    candidates_tried=scrape_result.get("candidates_tried") or [],
                                    ats_family=str(route_info.get("ats_family") or fingerprint_ats(str(company.get("careers_url", "") or ""))),
                                    extraction_method=scrape_result.get("extraction_method", str(route_info.get("decision") or adapter_name)),
                                    extraction_confidence=scrape_result.get("extraction_confidence", 0.0),
                                    route_decision=str(route_info.get("decision") or ""),
                                    screenshot_path=str(browser_evidence.get("screenshot_path") or ""),
                                    html_snapshot_path=str(browser_evidence.get("html_snapshot_path") or ""),
                                    top_network_response_urls=list(browser_evidence.get("network_response_urls") or [])[:8],
                                    timing_metrics={
                                        "scrape_ms": scrape_ms,
                                        "process_ms": process_ms,
                                    },
                                    detail=scrape_note,
                                )
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
                                target_adapter = str(company.get("adapter", "generic")).lower()
                                adapter_display = f"{adapter_name}"
                                if adapter_name != target_adapter and target_adapter != "generic":
                                    adapter_display = f"{adapter_name}/{target_adapter}"

                                log_msg(
                                    f"[{done_companies}/{total_to_run}] OK {company.get('name', 'Unknown'):<24} "
                                    f"| priority={decision.priority_score:>3.0f} adapter={adapter_display:<15} "
                                    f"evaluated={evaluated:<3} persisted={persisted:<3} new={inserted:<3} "
                                    f"scrape_ms={scrape_ms:.1f} deep={used_deep_search} status={scrape_status} "
                                    f"reason='{decision.reason}'"
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

    def _scrape_company_with_retry(self, company: Dict[str, Any], decision: Any) -> Dict[str, Any]:
        min_jitter = min(settings.scrape_jitter_min_ms, settings.scrape_jitter_max_ms) / 1000.0
        max_jitter = max(settings.scrape_jitter_min_ms, settings.scrape_jitter_max_ms) / 1000.0
        time.sleep(random.uniform(min_jitter, max_jitter))
        
        started_at = time.perf_counter()
        name = company.get("name", "")
        careers_url = str(company.get("careers_url", "") or "")
        ats_family = fingerprint_ats(careers_url)
        
        # 1. Choose route and define ladder
        # We check for JSON-LD and hidden API signals if we have any prior evidence or if deep search is on
        has_jsonld = False 
        has_hidden_api = False 
        
        routing = choose_extraction_route(
            ats_family=ats_family,
            has_jsonld=has_jsonld,
            has_hidden_api=has_hidden_api
        )
        
        try:
            result = self._scrape_with_ladder(company, routing)
            scrape_ms = round((time.perf_counter() - started_at) * 1000, 1)
            result["scrape_ms"] = scrape_ms
            return result
        except Exception as e:
            logger.error("Ladder scrape failed for %s: %s", name, e, exc_info=True)
            return {
                "jobs": [],
                "adapter": self._resolve_adapter_name(company),
                "scrape_ms": round((time.perf_counter() - started_at) * 1000, 1),
                "used_deep_search": False,
                "status": "error",
                "note": str(e),
                "route": routing.to_dict(),
            }

    def _scrape_with_ladder(self, company: Dict[str, Any], routing: Any) -> Dict[str, Any]:
        """Attempt extraction methods in the order specified by the routing ladder."""
        name = company.get("name", "")
        careers_url = str(company.get("careers_url", "") or "")
        
        final_jobs: List[Any] = []
        final_adapter = routing.ats_family
        final_status = "ok"
        final_note = ""
        final_method = ""
        final_confidence = 0.0
        used_deep = False
        browser_evidence = {}
        status_code = 0
        html_sample = ""
        
        for method in routing.extraction_methods:
            if final_jobs:
                break
                
            logger.debug("Trying extraction method '%s' for %s", method, name)
            
            if method == "direct_api":
                try:
                    jobs, adapter_name, status, note = self._scrape_company(company)
                    if jobs:
                        final_jobs = jobs
                        final_adapter = adapter_name
                        final_status = status
                        final_note = note
                        final_method = "direct_api"
                        final_confidence = 1.0
                    else:
                        final_note = note or "No jobs found via Direct API"
                except Exception as e:
                    logger.debug("Direct API failed for %s: %s", name, e)
                    
            elif method == "jsonld":
                try:
                    # Fetch HTML and extract JSON-LD
                    resp = self.session.get(careers_url, headers=get_headers(), timeout=10)
                    status_code = resp.status_code
                    if resp.status_code == 200:
                        html_sample = resp.text[:5000]
                        from jobsearch.scraper.jsonld_extractor import jsonld_jobs_to_canonical
                        jobs = jsonld_jobs_to_canonical(
                            resp.text, 
                            base_url=resp.url, 
                            company_name=name, 
                            adapter="jsonld"
                        )
                        if jobs:
                            final_jobs = jobs
                            final_adapter = "jsonld"
                            final_status = "ok"
                            final_method = "jsonld"
                            final_confidence = 0.9
                except Exception as e:
                    logger.debug("JSON-LD extraction failed for %s: %s", name, e)

            elif method == "network_interception":
                if self.deep_search and self._deep_playwright:
                    try:
                        deep_res = self._deep_scrape(company)
                        jobs, evidence = self._normalize_deep_scrape_result(deep_res)
                        browser_evidence = evidence
                        used_deep = True
                        status_code = evidence.get("status_code") or status_code
                        if jobs:
                            final_jobs = jobs
                            final_adapter = evidence.get("ats_family") or "deep_search"
                            final_status = "ok"
                            final_method = "intercepted_api"
                            final_confidence = 0.85
                    except Exception as e:
                        logger.debug("Network interception failed for %s: %s", name, e)

            elif method == "dom_render":
                if self.deep_search and self._deep_playwright:
                    # If network interception already ran, we might have DOM results already 
                    # from the deep_scrape result.
                    if not used_deep:
                        try:
                            deep_res = self._deep_scrape(company)
                            jobs, evidence = self._normalize_deep_scrape_result(deep_res)
                            browser_evidence = evidence
                            used_deep = True
                            status_code = evidence.get("status_code") or status_code
                            if jobs:
                                final_jobs = jobs
                                final_adapter = evidence.get("ats_family") or "deep_search"
                                final_status = "ok"
                                final_method = "dom_render"
                                final_confidence = 0.7
                        except Exception as e:
                            logger.debug("DOM render failed for %s: %s", name, e)
                    elif browser_evidence and not final_jobs:
                        final_method = browser_evidence.get("extraction_method") or "dom_render"
                        final_confidence = 0.7

            elif method == "classify_failure":
                from jobsearch.scraper.ats_routing import FailureClassifier
                # Final fallback: classify why we found nothing
                status_code = browser_evidence.get("status_code") or status_code
                final_status = "error"
                final_note = FailureClassifier.classify(
                    status_code=status_code,
                    html=html_sample,
                    jobs_found=0,
                    ats_family=routing.ats_family,
                    extraction_method=routing.decision,
                    final_url=careers_url
                )
                final_method = "none"
                final_confidence = 0.0

        # Update job objects with metadata
        for job in final_jobs:
            if not getattr(job, "extraction_method", ""):
                job.extraction_method = final_method
            if not getattr(job, "extraction_confidence", 0.0):
                job.extraction_confidence = final_confidence

        return {
            "jobs": final_jobs,
            "adapter": final_adapter,
            "used_deep_search": used_deep,
            "status": final_status,
            "note": final_note,
            "extraction_method": final_method,
            "extraction_confidence": final_confidence,
            "status_code": status_code,
            "html_sample": html_sample,
            "route": routing.to_dict(),
            "browser_evidence": browser_evidence,
            "candidates_tried": company.get("discovery_candidates") or [],
        }

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
        """Return (adapter, reason) if the company is on cooldown, else ('', '')."""
        careers_url = str(company.get("careers_url", "") or "").lower()
        adapter_name = str(company.get("adapter", "") or "").lower()
        name = company.get("name", "")
        now = datetime.now(timezone.utc)

        conn = db.get_connection()
        try:
            # 1. Check unified board_health first (New)
            health = ats_db.get_board_health(conn, name)
            if health and health["cooldown_until"]:
                try:
                    until = datetime.fromisoformat(str(health["cooldown_until"]))
                    if until.tzinfo is None:
                        until = until.replace(tzinfo=timezone.utc)
                    if until > now:
                        reason = health["suppression_reason"] or "Board on cooldown"
                        return adapter_name or health["adapter"] or "unknown", f"Cooldown until {until.isoformat()} ({reason})"
                except Exception:
                    pass

            is_workday = "myworkdayjobs.com" in careers_url or adapter_name in {"workday", "workday_manual"}
            is_generic = adapter_name == "generic" and not is_workday

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
            # Universal cooldown table for greenhouse, lever, ashby, smartrecruiters, rippling, etc.
            # (bulk_cooldowns prefetch covers most cases; this is the fallback for cache misses)
            try:
                row = conn.execute(
                    "SELECT cooldown_until FROM company_cooldowns WHERE company = ?", (name,)
                ).fetchone()
                if row and row["cooldown_until"]:
                    until = datetime.fromisoformat(str(row["cooldown_until"]))
                    if until.tzinfo is None:
                        until = until.replace(tzinfo=timezone.utc)
                    if until > now:
                        return adapter_name or "unknown", f"Cooldown until {until.isoformat()} (healer)"
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

    @staticmethod
    def _classify_empty_result(scrape_status: str, scrape_note: str, company: Dict[str, Any]) -> str | None:
        """Classify why a scrape returned 0 jobs into a machine-readable reason code.

        Returns one of:
          blocked_bot_protection, auth_required, broken_site, wrong_url,
          no_openings, hidden_api_not_parsed, selector_miss, geo_or_cookie_gate,
          unsupported_ats, or unknown.
        """
        note_lower = str(scrape_note or "").lower()
        careers_url = str(company.get("careers_url") or "").lower()

        if scrape_status == "blocked":
            if any(tok in note_lower for tok in ("unsupported ats", "unsupported vendor", "eightfold", "icims", "taleo", "successfactors")):
                return "unsupported_ats"
            if any(tok in note_lower for tok in ("401", "403", "login", "auth", "sign in")):
                return "auth_required"
            if any(tok in note_lower for tok in ("cookie", "consent", "gdpr", "region", "geo", "country not supported")):
                return "geo_or_cookie_gate"
            return "blocked_bot_protection"

        if scrape_status in {"empty", "budget_exhausted", "low_signal", ""}:
            if any(tok in note_lower for tok in ("500", "502", "503", "504", "connection", "timeout", "refused")):
                return "broken_site"
            if any(tok in note_lower for tok in ("redirect", "wrong domain", "not a careers", "homepage")):
                return "wrong_url"
            if any(tok in note_lower for tok in ("no open", "no current", "no position", "no job", "0 opening")):
                return "no_openings"
            if any(tok in note_lower for tok in ("xhr", "hidden api", "json endpoint not parsed")):
                return "hidden_api_not_parsed"
            if any(tok in note_lower for tok in ("selector", "no matching element", "low_signal")):
                return "selector_miss"
            if any(tok in note_lower for tok in ("cookie", "consent", "gdpr", "region", "geo", "country not supported")):
                return "geo_or_cookie_gate"
            if any(tok in note_lower for tok in ("icims", "taleo", "successfactors", "oracle hcm", "sap", "unsupported")):
                return "unsupported_ats"
            # If careers_url points to a known unsupported ATS vendor homepage
            for vendor in ("icims.com", "taleo.net", "successfactors.com", "oraclecloud.com"):
                if vendor in careers_url:
                    return "unsupported_ats"

        return "unknown"

    def _get_cooldown_days(self, classification: str) -> int:
        from jobsearch.scraper.ats_routing import FailureClassifier
        if classification == FailureClassifier.BLOCKED:
            return settings.cooldown_days_blocked
        if classification == FailureClassifier.BROKEN_SITE:
            return settings.cooldown_days_broken
        if classification == FailureClassifier.STALE_URL:
            return settings.cooldown_days_stale
        if classification == FailureClassifier.AUTH_REQUIRED:
            return settings.cooldown_days_auth
        if classification == FailureClassifier.GEO_GATE:
            return settings.cooldown_days_geo_gate
        if classification == FailureClassifier.NO_OPENINGS:
            return settings.cooldown_days_no_openings
        if classification == FailureClassifier.SELECTOR_MISS:
            return settings.cooldown_days_selector_miss
        return 0

    def _update_scrape_health(
        self,
        conn: sqlite3.Connection,
        company: Dict[str, Any],
        adapter_name: str,
        scrape_status: str,
        scrape_ms: float,
        scrape_note: str,
        evaluated_count: int,
        extraction_method: str = "",
        status_code: Optional[int] = None,
        html: str = "",
        ats_family: str = "unknown",
        final_url: str = "",
    ) -> None:
        from jobsearch.scraper.ats_routing import FailureClassifier
        name = company.get("name", "")
        
        # 1. Classify the failure using the refined FailureClassifier
        classification = FailureClassifier.classify(
            status_code=status_code,
            html=html,
            jobs_found=evaluated_count,
            ats_family=ats_family,
            extraction_method=extraction_method,
            final_url=final_url,
        )
        
        # 2. Determine cooldown from config-driven settings
        cooldown_days = self._get_cooldown_days(classification)
        escalation_reason = ""
        if evaluated_count == 0 and cooldown_days > 0:
            cooldown_days, escalation_reason = self.escalation_policy.calculate_cooldown(
                conn, name, cooldown_days, classification
            )
            if escalation_reason:
                scrape_note = f"{scrape_note} | {escalation_reason}".strip()

        cooldown_until = None
        if cooldown_days > 0:
            cooldown_until = (datetime.now(timezone.utc) + timedelta(days=cooldown_days)).strftime("%Y-%m-%d")

        # 3. Update unified board_health
        existing_health = ats_db.get_board_health(conn, name)
        consecutive_failures = int(existing_health["consecutive_failures"]) if existing_health else 0
        if evaluated_count == 0 and scrape_status != "success":
            consecutive_failures += 1
        else:
            consecutive_failures = 0
            
        last_success_at = existing_health["last_success_at"] if existing_health else None
        if evaluated_count > 0:
            last_success_at = datetime.now().isoformat()

        ats_db.update_board_health(
            conn,
            company=name,
            adapter=adapter_name,
            careers_url=company.get("careers_url", ""),
            board_state="healthy" if evaluated_count > 0 else (classification or "unknown"),
            last_success_method=extraction_method if evaluated_count > 0 else (existing_health["last_success_method"] if existing_health else None),
            last_http_status=status_code,
            manual_review_required=1 if classification in {FailureClassifier.BLOCKED, FailureClassifier.AUTH_REQUIRED} else 0,
            suppression_reason=classification if cooldown_days > 0 else None,
            consecutive_failures=consecutive_failures,
            last_success_at=last_success_at,
            last_attempt_at=datetime.now().isoformat(),
            cooldown_until=cooldown_until,
            notes=scrape_note,
        )

        # 4. Fallback to legacy health tables (maintaining backward compatibility)
        if adapter_name == "workday":
            self._update_workday_scrape_health_legacy(conn, company, scrape_status, scrape_ms, scrape_note, evaluated_count)
        elif adapter_name == "generic":
            self._update_generic_scrape_health_legacy(conn, company, scrape_status, scrape_ms, scrape_note, evaluated_count)

    def _update_workday_scrape_health_legacy(
        self,
        conn,
        company: Dict[str, Any],
        scrape_status: str,
        scrape_ms: float,
        scrape_note: str,
        evaluated_count: int,
    ) -> None:
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
        elif (
            scrape_status == "empty"
            and evaluated_count == 0
            and streak + 1 >= settings.workday_empty_cooldown_threshold
        ):
            cooldown_days = 3 if success_count > 0 else settings.workday_empty_cooldown_days
        failure_reason = self._classify_empty_result(scrape_status, scrape_note, company) if evaluated_count == 0 else None
        db.update_workday_target_health(
            conn,
            company=company.get("name", ""),
            careers_url=str(company.get("careers_url", "") or ""),
            status=scrape_status if scrape_status in {"ok", "empty", "budget_exhausted", "cooldown", "blocked"} else "ok",
            elapsed_ms=scrape_ms,
            evaluated_count=evaluated_count,
            cooldown_days=cooldown_days,
            notes=scrape_note,
            failure_reason=failure_reason,
        )

    def _update_generic_scrape_health_legacy(
        self,
        conn,
        company: Dict[str, Any],
        scrape_status: str,
        scrape_ms: float,
        scrape_note: str,
        evaluated_count: int,
    ) -> None:
        _GENERIC_SUPER_SLOW_MS = 75_000
        cooldown_days = 0
        existing = db.get_generic_target_health(conn, company.get("name", ""))
        success_count = int(existing["success_count"]) if existing else 0
        streak = int(existing["empty_streak"]) if existing else 0
        low_priority = str(company.get("priority", "") or "").lower() in {"low", "medium", ""}
        
        if evaluated_count == 0 and scrape_ms >= _GENERIC_SUPER_SLOW_MS:
            cooldown_days = max(cooldown_days, 3)
        if (
            scrape_status == "empty"
            and evaluated_count == 0
            and scrape_ms >= settings.generic_slow_empty_ms
            and success_count == 0
            and streak + 1 >= settings.generic_empty_cooldown_threshold
            and low_priority
        ):
            cooldown_days = max(cooldown_days, settings.generic_empty_cooldown_days)
        elif (
            scrape_status == "low_signal"
            and evaluated_count == 0
            and success_count == 0
            and low_priority
        ):
            cooldown_days = max(cooldown_days, settings.generic_low_signal_cooldown_days)
        elif (
            scrape_status == "blocked"
            and evaluated_count == 0
            and streak + 1 >= settings.generic_empty_cooldown_threshold
            and low_priority
        ):
            cooldown_days = max(cooldown_days, settings.generic_empty_cooldown_days)
        failure_reason = self._classify_empty_result(scrape_status, scrape_note, company) if evaluated_count == 0 else None
        db.update_generic_target_health(
            conn,
            company=company.get("name", ""),
            careers_url=str(company.get("careers_url", "") or ""),
            status=scrape_status if scrape_status in {"ok", "empty", "cooldown", "low_signal", "blocked"} else "ok",
            elapsed_ms=scrape_ms,
            evaluated_count=evaluated_count,
            cooldown_days=cooldown_days,
            notes=scrape_note,
            failure_reason=failure_reason,
        )

    def _deep_scrape(self, company: Dict[str, Any]) -> tuple[List[Any], Dict[str, Any]]:
        from jobsearch.ats_db import Job
        import hashlib

        careers_url = company.get("careers_url")
        name = company.get("name", "Unknown")
        if not careers_url:
            return [], {}

        try:
            if not self._deep_playwright:
                return [], {}

            deep_semaphore = self._adapter_semaphores.get("deep_search")
            if deep_semaphore is None:
                deep_semaphore = BoundedSemaphore(1)
            with deep_semaphore:
                # Route to company-specific Playwright function when one exists
                name_key = re.sub(r"[^a-z0-9]", "", name.lower())
                if "blackrock" in name_key and hasattr(self._deep_playwright, "scrape_jobs_blackrock"):
                    raw_jobs = self._deep_playwright.scrape_jobs_blackrock(careers_url, name)
                elif ("schwab" in name_key or "charlesschwab" in name_key) and hasattr(self._deep_playwright, "scrape_jobs_schwab"):
                    raw_jobs = self._deep_playwright.scrape_jobs_schwab(careers_url, name)
                elif ("spglobal" in name_key or "sandpglobal" in name_key) and hasattr(self._deep_playwright, "scrape_jobs_spglobal"):
                    raw_jobs = self._deep_playwright.scrape_jobs_spglobal(careers_url, name)
                else:
                    raw_jobs = self._deep_playwright.scrape_jobs_generic(careers_url, name)
                evidence = {}
                if hasattr(self._deep_playwright, "get_last_run_evidence"):
                    try:
                        evidence = self._deep_playwright.get_last_run_evidence() or {}
                    except Exception:
                        evidence = {}
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
            return jobs, evidence
        except Exception as exc:
            logger.error("Deep scrape failed for %s: %s", name, exc)
            return [], {}

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
            # Populate role_title_normalized if not already set.
            # Strips seniority prefixes and trailing noise so the opportunity_service
            # secondary dedup can match variants like "Sr. Product Manager" == "Product Manager".
            if not getattr(job, "role_title_normalized", None):
                _raw = str(job.role_title_raw or "").strip()
                _norm = _raw.lower()
                _norm = re.sub(
                    r"^(senior|sr\.?|principal|staff|lead|head of|director of|vp of|vice president of)\s+",
                    "",
                    _norm,
                    flags=re.IGNORECASE,
                )
                _norm = re.sub(r"\s*[–\-]\s*(remote|hybrid|onsite|on-site|us only|usa).*$", "", _norm, flags=re.IGNORECASE)
                _norm = re.sub(r"\s*\(?(remote|hybrid|onsite)\)?$", "", _norm, flags=re.IGNORECASE)
                _norm = re.sub(r"\s+(ii|iii|iv|v)\s*$", "", _norm, flags=re.IGNORECASE)
                job.role_title_normalized = _norm.strip()

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
            # V1 handles hard gates (disqualifiers, experience check) and compensation
            # parsing.  Pull compensation fields regardless of whether V1 disqualified.
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

            # If V1 hard-dropped the job, honour that decision.
            if score_results["fit_band"] in ("Disqualified", "Filtered Out"):
                job.score = 0.0
                job.fit_band = score_results["fit_band"]
            elif self._v2_available:
                # V2 is primary scorer: use its final_score and fit_band.
                try:
                    from jobsearch.scraper.scoring_v2 import score_job_v2, v2_title_pts
                    _tpts = v2_title_pts(
                        job.role_title_raw or "",
                        self._v2_title_index,
                        self._v2_cfg,
                        self._v2_pw,
                        self._v2_ftbase,
                        self._v2_ftmin,
                    )
                    _v2 = score_job_v2(
                        job.role_title_raw or "",
                        job.description_excerpt or "",
                        _tpts,
                        self._v2_cfg,
                        self._v2_title_index,
                    )
                    job.score = round(_v2.final_score, 2)
                    job.fit_band = _v2.fit_band
                    job.decision_reason = (
                        f"V2: {_v2.fit_band} | title={_v2.canonical_title or 'unresolved'}"
                        f" | seniority={_v2.seniority.band}"
                        f" | anchor={round(_v2.keyword.anchor_score,1)}"
                        f" base={round(_v2.keyword.baseline_score,1)}"
                        f" neg={round(_v2.keyword.negative_score,1)}"
                    )
                    # Store V2 breakdown so the Scoring Lab columns are populated
                    # at scrape time rather than requiring a separate backfill run.
                    job._v2_result = _v2
                except Exception as _v2_err:
                    logger.debug("V2 scoring failed for %s/%s: %s", job.company, job.role_title_raw, _v2_err)
                    job.score = score_results["score"]
                    job.fit_band = score_results["fit_band"]
                    job._v2_result = None
            else:
                job.score = score_results["score"]
                job.fit_band = score_results["fit_band"]
                job._v2_result = None

            score_values.append(float(job.score or 0.0))

            if job.score >= self.scorer.min_score_to_keep:
                try:
                    inserted, app_id = upsert_job(conn, job)
                    persisted_count += 1
                    if inserted:
                        inserted_count += 1
                    # Write V2 breakdown columns alongside the primary score.
                    _v2r = getattr(job, "_v2_result", None)
                    if _v2r is not None and app_id:
                        import json as _json
                        conn.execute(
                            """
                            UPDATE applications
                            SET score_v2 = ?, fit_band_v2 = ?, v2_canonical_title = ?,
                                v2_seniority_band = ?, v2_anchor_score = ?,
                                v2_baseline_score = ?, v2_flags = ?
                            WHERE id = ?
                            """,
                            (
                                round(_v2r.final_score, 2),
                                _v2r.fit_band,
                                _v2r.canonical_title,
                                _v2r.seniority.band,
                                round(_v2r.keyword.anchor_score, 2),
                                round(_v2r.keyword.baseline_score, 2),
                                _json.dumps(_v2r.flags),
                                app_id,
                            ),
                        )
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
