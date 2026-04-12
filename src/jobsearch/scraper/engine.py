"""Scraper engine: orchestrates multi-threaded scraping and persistence."""

import csv
import json
import logging
import os
import random
import re
import sqlite3
import time
from concurrent.futures import FIRST_COMPLETED, ThreadPoolExecutor, wait, as_completed
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
from jobsearch.services.health_monitor import HealthMonitor
from jobsearch.services.evaluation_service import EvaluationService

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
        self.health_monitor = HealthMonitor(self.escalation_policy)
        self.evaluation_service = EvaluationService(preferences)
        self.session = get_shared_session()
        self.bulk_cooldowns: Dict[str, datetime] = {}
        self.known_urls: Dict[str, datetime] = {}

        # V2 scoring — built once at startup
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
            "remotive": 1,
            "remoteok": 1,
            "wwr": 1,
            "findwork": 1,
            "google_careers": 2,
            "dice": settings.scrape_dice_concurrency,
            "motionrecruitment": settings.scrape_motionrecruitment_concurrency,
            "deep_search": settings.scrape_deep_search_concurrency,
        }
        semaphores: Dict[str, BoundedSemaphore] = {}
        for adapter_name, limit in limits.items():
            safe_limit = max(1, int(limit))
            semaphores[adapter_name] = BoundedSemaphore(safe_limit)
        return semaphores

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

    def run(self, max_workers: int = 12, score_only: bool = False):
        log_path = settings.log_file
        log_path.parent.mkdir(parents=True, exist_ok=True)
        rotate_log_file(log_path)
        try:
            log_path.touch(exist_ok=True)
        except Exception:
            pass

        rejected_csv_path = settings.rejected_csv
        main_conn = get_connection()

        try:
            with log_path.open("a", encoding="utf-8") as log_handle:
                def log_msg(message: str):
                    print(message, flush=True)
                    try:
                        log_handle.write(f"{datetime.now().strftime('%H:%M:%S')} | {message}\n")
                        log_handle.flush()
                    except Exception:
                        pass

                if score_only:
                    log_msg("Evaluation pass start (score-only mode)")
                    scored_count, eval_rejected_rows = self.evaluation_service.evaluate_pending_jobs(main_conn)
                    log_msg(f"Evaluation complete. Scored {scored_count} jobs. Rejected {len(eval_rejected_rows)}.")
                    return

                total_companies = len(self.companies)
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

                if healer_tasks:
                    log_msg(f"Triggering auto-healer for {len(healer_tasks)} companies...")
                    from jobsearch.services.healer_service import ATSHealer
                    healer = ATSHealer()
                    for company, decision in healer_tasks:
                        try:
                            healer.heal_company(company["name"])
                        except Exception as e:
                            logger.error("Auto-healer failed for %s: %s", company["name"], e)

                total_evaluated = 0
                total_persisted = 0
                total_inserted = 0
                done_companies = 0
                total_scrape_ms = 0.0
                total_process_ms = 0.0
                adapter_metrics: Dict[str, Dict[str, Any]] = {}
                slowest_companies: List[Dict[str, Any]] = []
                blocked_companies: List[Dict[str, Any]] = []
                rejected_rows: List[Dict[str, Any]] = []
                run_started_at = time.perf_counter()

                log_msg(f"Queue size: {total_to_run} companies (max_workers={max_workers})")
                
                # 3. Execution pass: Acquisition
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
                                jobs = scrape_result.get("jobs", [])
                                adapter_name = scrape_result.get("adapter", "unknown")
                                scrape_status = scrape_result.get("status", "error")
                                scrape_ms = scrape_result.get("scrape_ms", 0.0)
                                used_deep_search = scrape_result.get("used_deep_search", False)
                                scrape_note = scrape_result.get("note", "")
                                route_info = scrape_result.get("route") or {}
                                browser_evidence = scrape_result.get("browser_evidence") or {}
                                
                                # Acquisition: Persist jobs unscored first
                                persisted, inserted, evaluated, process_ms = self._process_and_save_jobs_acquisition(main_conn, company, jobs)
                                
                                self.health_monitor.update_scrape_health(
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
                                    timing_metrics={"scrape_ms": scrape_ms, "process_ms": process_ms},
                                    detail=scrape_note,
                                )
                                total_evaluated += evaluated
                                total_persisted += persisted
                                total_inserted += inserted
                                total_scrape_ms += scrape_ms
                                total_process_ms += process_ms
                                
                                adapter_bucket = adapter_metrics.setdefault(
                                    adapter_name, {"companies": 0, "evaluated": 0, "persisted": 0, "scrape_ms": 0.0, "process_ms": 0.0}
                                )
                                adapter_bucket["companies"] += 1
                                adapter_bucket["evaluated"] += evaluated
                                adapter_bucket["persisted"] += persisted
                                adapter_bucket["scrape_ms"] += scrape_ms
                                adapter_bucket["process_ms"] += process_ms
                                if scrape_status == "blocked":
                                    blocked_companies.append({
                                        "name": company.get("name", "Unknown"),
                                        "adapter": adapter_name,
                                        "url": company.get("careers_url", ""),
                                        "note": scrape_note or "Blocked by site protection",
                                    })
                                slowest_companies.append({
                                    "name": company.get("name", "Unknown"),
                                    "adapter": adapter_name,
                                    "elapsed_ms": round(scrape_ms + process_ms, 1),
                                    "scrape_ms": round(scrape_ms, 1),
                                    "process_ms": round(process_ms, 1),
                                    "deep": used_deep_search,
                                })
                                target_adapter = str(company.get("adapter", "generic")).lower()
                                adapter_display = f"{adapter_name}"
                                if adapter_name != target_adapter and target_adapter != "generic":
                                    adapter_display = f"{adapter_name}/{target_adapter}"

                                log_msg(
                                    f"[{done_companies}/{total_to_run}] OK {company.get('name', 'Unknown'):<24} "
                                    f"| adapter={adapter_display:<15} "
                                    f"evaluated={evaluated:<3} persisted={persisted:<3} new={inserted:<3} "
                                    f"scrape_ms={scrape_ms:.1f} deep={used_deep_search} status={scrape_status}"
                                )
                            except Exception as exc:
                                log_msg(f"[{done_companies}/{total_companies}] FAIL {company.get('name', 'Unknown'):<24} | {exc}")

                # 4. Evaluation pass
                log_msg("Starting evaluation pass for newly acquired jobs...")
                eval_started_at = time.perf_counter()
                total_scored, eval_rejected_rows = self.evaluation_service.evaluate_pending_jobs(main_conn)
                eval_elapsed_ms = round((time.perf_counter() - eval_started_at) * 1000, 1)
                rejected_rows.extend(eval_rejected_rows)
                log_msg(f"Evaluation complete. Scored {total_scored} jobs in {eval_elapsed_ms}ms. Rejected {len(eval_rejected_rows)}.")

                if rejected_rows:
                    try:
                        rejected_csv_path.parent.mkdir(parents=True, exist_ok=True)
                        with rejected_csv_path.open("a", newline="", encoding="utf-8") as handle:
                            writer = csv.DictWriter(handle, fieldnames=_REJECTED_CSV_FIELDS)
                            if os.path.getsize(rejected_csv_path) == 0:
                                writer.writeheader()
                            writer.writerows(rejected_rows)
                    except Exception as e:
                        logger.error("Failed to write rejected jobs to CSV: %s", e)

                total_elapsed_ms = round((time.perf_counter() - run_started_at) * 1000, 1)
                log_msg(f"\nPipeline complete in {total_elapsed_ms/1000:.1f}s.")
                log_msg(f"Evaluated: {total_evaluated} | Persisted: {total_persisted} | New: {total_inserted} | Scored: {total_scored}")

        except Exception as exc:
            print(f"Engine failed: {exc}")
            import traceback
            traceback.print_exc()
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
        routing = choose_extraction_route(ats_family=ats_family, has_jsonld=False, has_hidden_api=False)
        
        try:
            result = self._scrape_with_ladder(company, routing)
            result["scrape_ms"] = round((time.perf_counter() - started_at) * 1000, 1)
            return result
        except Exception as e:
            return {
                "jobs": [], "adapter": self._resolve_adapter_name(company),
                "scrape_ms": round((time.perf_counter() - started_at) * 1000, 1),
                "used_deep_search": False, "status": "error", "note": str(e),
                "route": routing.to_dict(),
            }

    def _scrape_with_ladder(self, company: Dict[str, Any], routing: Any) -> Dict[str, Any]:
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
            if final_jobs: break
            if method == "direct_api":
                try:
                    jobs, adapter_name, status, note = self._scrape_company(company)
                    if jobs:
                        final_jobs, final_adapter, final_status, final_note, final_method, final_confidence = jobs, adapter_name, status, note, "direct_api", 1.0
                except Exception: pass
            elif method == "jsonld":
                try:
                    resp = self.session.get(careers_url, headers=get_headers(), timeout=10)
                    status_code = resp.status_code
                    if resp.status_code == 200:
                        html_sample = resp.text[:5000]
                        from jobsearch.scraper.jsonld_extractor import jsonld_jobs_to_canonical
                        jobs = jsonld_jobs_to_canonical(resp.text, base_url=resp.url, company_name=name, adapter="jsonld")
                        if jobs:
                            final_jobs, final_adapter, final_status, final_method, final_confidence = jobs, "jsonld", "ok", "jsonld", 0.9
                except Exception: pass
            elif method in ("network_interception", "dom_render"):
                if self.deep_search and self._deep_playwright:
                    try:
                        deep_res = self._deep_scrape(company)
                        jobs, evidence = self._normalize_deep_scrape_result(deep_res)
                        browser_evidence, used_deep = evidence, True
                        status_code = evidence.get("status_code") or status_code
                        if jobs:
                            final_jobs, final_adapter, final_status, final_method, final_confidence = jobs, evidence.get("ats_family") or "deep_search", "ok", method, 0.8
                    except Exception: pass

        for job in final_jobs:
            job.extraction_method = getattr(job, "extraction_method", None) or final_method
            job.extraction_confidence = getattr(job, "extraction_confidence", None) or final_confidence

        return {
            "jobs": final_jobs, "adapter": final_adapter, "used_deep_search": used_deep,
            "status": final_status, "note": final_note, "extraction_method": final_method,
            "extraction_confidence": final_confidence, "status_code": status_code,
            "html_sample": html_sample, "route": routing.to_dict(), "browser_evidence": browser_evidence,
            "candidates_tried": company.get("discovery_candidates") or [],
        }

    def _scrape_company(self, company: Dict[str, Any]):
        adapter_name = self._resolve_adapter_name(company)
        adapter_cls = self.ADAPTER_MAP.get(adapter_name, GenericAdapter)
        adapter = adapter_cls(session=self.session, scorer=self.scorer)
        if hasattr(adapter, "set_known_urls"): adapter.set_known_urls(self.known_urls)
        jobs = adapter.scrape(company)
        return jobs, adapter_name, getattr(adapter, "last_status", "ok"), getattr(adapter, "last_note", "")

    def _process_and_save_jobs_acquisition(self, conn, company: Dict[str, Any], jobs: List[Any]):
        if not jobs: return 0, 0, 0, 0.0
        started_at = time.perf_counter()
        persisted_count = inserted_count = 0
        for job in jobs:
            if not getattr(job, "role_title_normalized", None):
                _norm = str(job.role_title_raw or "").lower().strip()
                _norm = re.sub(r"^(senior|sr\.?|principal|staff|lead|head of|director of|vp of|vice president of)\s+", "", _norm, flags=re.IGNORECASE)
                _norm = re.sub(r"\s*[–\-]\s*(remote|hybrid|onsite|on-site|us only|usa).*$", "", _norm, flags=re.IGNORECASE)
                job.role_title_normalized = _norm.strip()
            job.score, job.fit_band = 0.0, "Pending Evaluation"
            try:
                inserted, app_id = upsert_job(conn, job)
                persisted_count += 1
                if inserted: inserted_count += 1
            except Exception: pass
        conn.commit()
        return persisted_count, inserted_count, len(jobs), round((time.perf_counter() - started_at) * 1000, 1)

    def _classify_empty_result(self, scrape_status: str, scrape_note: str, company: Dict[str, Any]) -> str | None:
        note_lower = str(scrape_note or "").lower()
        if scrape_status == "blocked":
            if "auth" in note_lower or "login" in note_lower: return "auth_required"
            return "blocked_bot_protection"
        if scrape_status == "error": return "broken_site"
        return "no_openings" if scrape_status == "empty" else "unknown"

    def _deep_scrape(self, company: Dict[str, Any]) -> tuple[List[Any], Dict[str, Any]]:
        from jobsearch.ats_db import Job
        import hashlib
        careers_url, name = company.get("careers_url"), company.get("name", "Unknown")
        if not careers_url or not self._deep_playwright: return [], {}
        try:
            raw_jobs = self._deep_playwright.scrape_jobs_generic(careers_url, name)
            evidence = self._deep_playwright.get_last_run_evidence() if hasattr(self._deep_playwright, "get_last_run_evidence") else {}
            jobs = [Job(id=hashlib.md5(f"{name}{r.get('title')}{r.get('url')}".encode()).hexdigest(),
                        company=name, role_title_raw=r.get("title", ""), location=r.get("location", ""),
                        url=r.get("url", ""), source="Deep Search", adapter="deep_search",
                        tier=str(company.get("tier", 4)), description_excerpt=r.get("description", ""))
                    for r in raw_jobs]
            return jobs, evidence or {}
        except Exception: return [], {}

    def enrich_jobs_with_ai(self, min_score_threshold: float = 60.0, max_jobs: int = 50) -> Dict[str, Any]:
        """
        Enriches high-scoring jobs with AI analysis for visa sponsorship, tech stack, and IC vs Manager.
        This is an optional post-processing step that improves scoring accuracy.
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
