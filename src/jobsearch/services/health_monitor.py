"""Shared HealthMonitor service for tracking board and scraper health."""

from __future__ import annotations
import sqlite3
import logging
from datetime import datetime, timezone, timedelta
from typing import Any, Dict, Optional

from jobsearch import ats_db
from jobsearch import ats_db as db
from jobsearch.config.settings import settings
from jobsearch.scraper.ats_routing import FailureClassifier

logger = logging.getLogger(__name__)

class HealthMonitor:
    """Central service for tracking ATS board health and scraper performance."""

    def __init__(self, escalation_policy: Any):
        self.escalation_policy = escalation_policy

    def _get_cooldown_days(self, classification: str) -> int:
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

    def update_scrape_health(
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
        """Update unified board health and legacy tables."""
        name = company.get("name", "")
        
        # 1. Classify the failure
        classification = FailureClassifier.classify(
            status_code=status_code,
            html=html,
            jobs_found=evaluated_count,
            ats_family=ats_family,
            extraction_method=extraction_method,
            final_url=final_url,
        )
        
        # 2. Determine cooldown
        cooldown_days = self._get_cooldown_days(classification)
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

        # 4. Fallback to legacy health tables
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
        
        if (scrape_status == "budget_exhausted" and evaluated_count == 0 and success_count == 0 and streak + 1 >= settings.workday_empty_cooldown_threshold):
            cooldown_days = settings.workday_empty_cooldown_days
        elif (scrape_status == "blocked" and evaluated_count == 0 and streak + 1 >= settings.workday_empty_cooldown_threshold):
            cooldown_days = settings.workday_empty_cooldown_days
        elif (scrape_status == "empty" and evaluated_count == 0 and streak + 1 >= settings.workday_empty_cooldown_threshold):
            cooldown_days = 3 if success_count > 0 else settings.workday_empty_cooldown_days
            
        cooldown_until = None
        if cooldown_days > 0:
            cooldown_until = (datetime.now(timezone.utc) + timedelta(days=cooldown_days)).strftime("%Y-%m-%d")

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
        if (scrape_status == "empty" and evaluated_count == 0 and scrape_ms >= settings.generic_slow_empty_ms and success_count == 0 and streak + 1 >= settings.generic_empty_cooldown_threshold and low_priority):
            cooldown_days = max(cooldown_days, settings.generic_empty_cooldown_days)
        elif (scrape_status == "low_signal" and evaluated_count == 0 and success_count == 0 and low_priority):
            cooldown_days = max(cooldown_days, settings.generic_low_signal_cooldown_days)
        elif (scrape_status == "blocked" and evaluated_count == 0 and streak + 1 >= settings.generic_empty_cooldown_threshold and low_priority):
            cooldown_days = max(cooldown_days, settings.generic_empty_cooldown_days)
            
        cooldown_until = None
        if cooldown_days > 0:
            cooldown_until = (datetime.now(timezone.utc) + timedelta(days=cooldown_days)).strftime("%Y-%m-%d")

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
