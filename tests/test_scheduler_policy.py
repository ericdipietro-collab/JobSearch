import unittest
from unittest.mock import MagicMock
from datetime import datetime, timedelta, timezone
from jobsearch.scraper.scheduler_policy import SchedulerPolicy
from jobsearch.scraper.ats_routing import FailureClassifier

class TestSchedulerPolicy(unittest.TestCase):
    def setUp(self):
        self.settings = MagicMock()
        self.policy = SchedulerPolicy(self.settings)

    def test_base_priority(self):
        # Tier 1 = 80, Tier 2 = 60, Tier 3 = 40, Tier 4 = 20
        self.assertEqual(self.policy._base_tier_priority(1), 80)
        self.assertEqual(self.policy._base_tier_priority(2), 60)
        self.assertEqual(self.policy._base_tier_priority(4), 20)

    def test_cooldown_active(self):
        company = {"name": "TestCo", "tier": 2}
        future_date = (datetime.now(timezone.utc) + timedelta(days=1)).isoformat()
        health = {
            "cooldown_until": future_date,
            "suppression_reason": FailureClassifier.BLOCKED
        }
        decision = self.policy.decide(company, health)
        self.assertEqual(decision.action, "skip")
        self.assertTrue(decision.cooldown_active)
        self.assertEqual(decision.priority_score, -100)

    def test_stale_url_triggers_healer(self):
        company = {"name": "StaleCo", "tier": 1}
        health = {"board_state": FailureClassifier.STALE_URL}
        decision = self.policy.decide(company, health)
        self.assertEqual(decision.action, "heal")
        self.assertTrue(decision.queue_healer)
        # Base 80 + 20 boost
        self.assertEqual(decision.priority_score, 100)

    def test_healthy_api_bonus(self):
        company = {"name": "HealthyCo", "tier": 2}
        health = {
            "board_state": "healthy",
            "last_success_method": "direct_api"
        }
        decision = self.policy.decide(company, health)
        self.assertEqual(decision.action, "scrape")
        # Base 60 + 10 (healthy) + 10 (api bonus)
        self.assertEqual(decision.priority_score, 80)

    def test_fail_streak_penalty(self):
        company = {"name": "FailingCo", "tier": 2}
        health = {
            "board_state": "broken_site",
            "consecutive_failures": 3
        }
        decision = self.policy.decide(company, health)
        # Base 60 - (3 * 5) = 45
        self.assertEqual(decision.priority_score, 45)

    def test_force_override_bypasses_cooldown(self):
        company = {"name": "OverrideCo", "tier": 3}
        future_date = (datetime.now(timezone.utc) + timedelta(days=1)).isoformat()
        health = {"cooldown_until": future_date}
        decision = self.policy.decide(company, health, force_override=True)
        self.assertEqual(decision.action, "scrape")
        # Base 40 + 50 boost
        self.assertEqual(decision.priority_score, 90)

if __name__ == "__main__":
    unittest.main()
