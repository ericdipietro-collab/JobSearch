from __future__ import annotations
import logging
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from jobsearch.scraper.ats_routing import FailureClassifier
from jobsearch import ats_db

logger = logging.getLogger(__name__)

@dataclass
class SchedulingDecision:
    company: str
    action: str  # 'scrape', 'heal', 'skip', 'manual_review'
    priority_score: float
    reason: str
    cooldown_active: bool = False
    manual_review_required: bool = False
    queue_healer: bool = False
    metadata: Dict[str, Any] = field(default_factory=dict)

class SchedulerPolicy:
    """
    Determines the next action and priority for a company board based on health state.
    """

    ACTION_SCRAPE = "scrape"
    ACTION_HEAL = "heal"
    ACTION_SKIP = "skip"
    ACTION_MANUAL_REVIEW = "manual_review"

    def __init__(self, settings: Any):
        self.settings = settings

    def decide(
        self, 
        company: Dict[str, Any], 
        health: Optional[Dict[str, Any]] = None, 
        force_override: bool = False
    ) -> SchedulingDecision:
        name = company.get("name", "Unknown")
        tier = int(company.get("tier", 4))
        now = datetime.now(timezone.utc)
        
        # 1. Initialize variables
        action = self.ACTION_SCRAPE
        priority = self._base_tier_priority(tier)
        reason = "Standard scrape"
        cooldown_active = False
        manual_review_required = False
        queue_healer = False
        
        # 2. Extract health metrics
        state = (health.get("board_state") or "unknown").lower() if health else "new"
        fail_streak = int(health.get("consecutive_failures") or 0) if health else 0
        last_method = health.get("last_success_method") if health else None
        last_failure_class = health.get("suppression_reason") if health else None
        
        # Check cooldown
        if health and health.get("cooldown_until"):
            try:
                until = datetime.fromisoformat(str(health["cooldown_until"]).replace("Z", "+00:00"))
                if until.tzinfo is None:
                    until = until.replace(tzinfo=timezone.utc)
                if until > now:
                    cooldown_active = True
            except Exception:
                pass

        # 3. Decision Logic
        
        # Override
        if force_override:
            action = self.ACTION_SCRAPE
            priority += 50
            reason = "Forced override"
            return SchedulingDecision(name, action, priority, reason, cooldown_active=cooldown_active)

        # Cooldown skip
        if cooldown_active:
            action = self.ACTION_SKIP
            reason = f"On cooldown ({last_failure_class or 'unknown'})"
            priority = -100
            return SchedulingDecision(name, action, priority, reason, cooldown_active=True)

        # Manual Review
        if health and health.get("manual_review_required"):
            action = self.ACTION_SKIP # Don't auto-scrape if manual review is flagged
            reason = "Blocked/Broken - Manual review required"
            priority = -50
            return SchedulingDecision(name, action, priority, reason, manual_review_required=True)

        # Auto-Healer Routing
        if state in {FailureClassifier.STALE_URL, FailureClassifier.WRONG_URL}:
            action = self.ACTION_HEAL
            queue_healer = True
            reason = f"Healer recommended: {state}"
            priority += 20
        elif state == FailureClassifier.SELECTOR_MISS and fail_streak >= 2:
            action = self.ACTION_HEAL
            queue_healer = True
            reason = "Repeated selector misses - healing required"
            priority += 10
        elif state == "healthy" and fail_streak >= 3:
             # Previously healthy but now repeatedly empty/failing
             action = self.ACTION_HEAL
             queue_healer = True
             reason = "Healthy board now repeatedly failing - checking for URL change"
             priority += 5

        # Priority Adjustments
        if state == "new":
            priority += 15
            reason = "New company - discovery run"
        elif state == "healthy":
            priority += 10
            # Boost for reliable API methods
            if last_method in {"direct_api", "intercepted_api"}:
                priority += 10
        
        # Penalty for persistent failures
        if fail_streak > 0:
            priority -= (fail_streak * 5)

        return SchedulingDecision(
            company=name,
            action=action,
            priority_score=priority,
            reason=reason,
            manual_review_required=manual_review_required,
            queue_healer=queue_healer,
            metadata={
                "board_state": state,
                "fail_streak": fail_streak,
                "tier": tier
            }
        )

    def _base_tier_priority(self, tier: int) -> float:
        # Tier 1 = 80, Tier 2 = 60, Tier 3 = 40, Tier 4 = 20
        return max(0, (5 - tier) * 20)


class EscalationPolicy:
    """
    Determines if a cooldown should be escalated based on transition history.
    """

    def __init__(self, settings: Any):
        self.settings = settings

    def calculate_cooldown(self, conn: Any, company: str, base_days: int, failure_class: str) -> tuple[int, str]:
        """Returns (escalated_days, reason)."""
        if base_days <= 0:
            return base_days, ""

        # Only escalate for meaningful failures
        if failure_class not in {FailureClassifier.BLOCKED, FailureClassifier.BROKEN_SITE}:
            return base_days, ""

        window = self.settings.health_escalation_window_days
        recent_count = ats_db.count_recent_events(conn, company, failure_class, days=window)
        
        # first event is the one just being recorded (or about to be), 
        # but count_recent_events looks at the DB history.
        # If count >= 2 in last 7 days, it's a repeat.
        
        multiplier = 1
        reason = ""
        
        if recent_count >= 3:
            multiplier = self.settings.health_escalation_multiplier_4x
            reason = f"Escalated {multiplier}x (high recurrence: {recent_count} events in {window}d)"
        elif recent_count >= 1: # Already has at least one previous event in window
            multiplier = self.settings.health_escalation_multiplier_2x
            reason = f"Escalated {multiplier}x (repeat failure in {window}d)"
            
        escalated_days = min(base_days * multiplier, self.settings.health_escalation_max_days)
        
        return escalated_days, reason
