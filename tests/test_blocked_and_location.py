import sys
import unittest
from datetime import date, datetime, timezone
from email.message import EmailMessage
from pathlib import Path
import sqlite3
import io
import zipfile
import tempfile

from bs4 import BeautifulSoup

BASE_DIR = Path(__file__).resolve().parents[1]
SRC_DIR = BASE_DIR / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from jobsearch.scraper.adapters.base import BaseAdapter, BlockedSiteError
from jobsearch.scraper.adapters.dice import DiceAdapter
from jobsearch.scraper.adapters.generic import GenericAdapter
from jobsearch.scraper.engine import ScraperEngine
from jobsearch.scraper.scoring import Scorer
from jobsearch.services.email_signal_service import (
    classify_email_signal,
    infer_interview_change_type,
    infer_interview_type,
    signal_resolution_for_existing_application,
)
from jobsearch.cli import (
    _apply_heal_failure_policy,
    _healer_cooldown_reason,
    _parse_iso_datetime,
    _reset_heal_failure_state,
)
from jobsearch.services.gmail_sync_service import _parse_imap_message
from jobsearch.services.opportunity_service import _is_material_jd_change, _jd_fingerprint
from jobsearch.services.healer_service import ATSHealer
from jobsearch import ats_db as db
from jobsearch.config.settings import settings
from jobsearch.app_main import (
    _annualized_compensation_preview,
    _apply_work_type_filter,
    _disable_company_in_registry,
    _decorate_role_velocity,
    _extract_resume_text,
    _normalize_work_type,
    _parse_manual_review_lines,
    _role_velocity_summary,
    _sidebar_metrics_for_df,
    _work_type_label,
)
from jobsearch.views.analytics_page import (
    _parse_keyword_blob,
    _resume_contains_keyword,
    _resume_gap_rows,
    _title_family,
)
from jobsearch.views.tracker_page import (
    _default_follow_up_date,
    _follow_up_template_note,
    _formal_tracker_rows,
    _negotiation_counter_draft,
    _negotiation_playbook_lines,
    _offer_comparison_markdown,
    _offer_comparison_rows,
    _resume_contains_phrase,
    _summary_metrics_for_rows,
    _tailor_resume_keywords,
    _tailored_resume_summary,
)
import pandas as pd


class _FakeAdapter(BaseAdapter):
    def scrape(self, company_config):
        return []


class _Resp:
    def __init__(self, text="", status_code=200, url="https://example.com"):
        self.text = text
        self.status_code = status_code
        self.url = url


class _UploadedFileStub:
    def __init__(self, name: str, content: bytes):
        self.name = name
        self._content = content

    def getvalue(self) -> bytes:
        return self._content


class _FakeDiceAdapter(DiceAdapter):
    def __init__(self, html: str):
        super().__init__(session=None, scorer=None)
        self._html = html

    def fetch_text(self, url: str) -> str:
        return self._html


class BlockedAndLocationTests(unittest.TestCase):
    def test_heal_cooldown_parser_normalizes_naive_datetime_to_utc(self):
        parsed = _parse_iso_datetime("2026-04-03T12:00:00")
        self.assertIsNotNone(parsed)
        self.assertEqual(parsed.tzinfo, timezone.utc)

    def test_heal_failure_policy_sets_cooldown_and_suggests_manual_only(self):
        company = {"name": "Low Priority Co", "priority": "low", "tier": 4}
        promoted = False
        detail = "No board detected"
        now_utc = datetime(2026, 4, 3, 12, 0, tzinfo=timezone.utc)
        for _ in range(3):
            promoted, detail = _apply_heal_failure_policy(company, "NOT_FOUND", detail, now_utc)
        self.assertFalse(promoted)
        self.assertTrue(company["manual_only_suggested"])
        self.assertEqual(company["status"], "broken")
        self.assertIsNotNone(_healer_cooldown_reason(company, now_utc))

    def test_heal_failure_policy_promotes_low_priority_company_to_manual_only(self):
        company = {"name": "Low Priority Co", "priority": "low", "tier": 4, "active": True}
        promoted = False
        detail = "Blocked by site protection"
        now_utc = datetime(2026, 4, 3, 12, 0, tzinfo=timezone.utc)
        for _ in range(5):
            promoted, detail = _apply_heal_failure_policy(company, "BLOCKED", detail, now_utc)
        self.assertTrue(promoted)
        self.assertTrue(company["manual_only"])
        self.assertFalse(company["active"])
        self.assertEqual(company["status"], "manual_only")
        self.assertIn("manual_only", detail)

    def test_heal_failure_policy_does_not_promote_high_priority_company(self):
        company = {"name": "High Priority Co", "priority": "high", "tier": 1, "active": True}
        promoted = False
        detail = "Blocked by site protection"
        now_utc = datetime(2026, 4, 3, 12, 0, tzinfo=timezone.utc)
        for _ in range(5):
            promoted, detail = _apply_heal_failure_policy(company, "BLOCKED", detail, now_utc)
        self.assertFalse(promoted)
        self.assertNotEqual(company.get("status"), "manual_only")
        self.assertTrue(company["manual_only_suggested"])

    def test_heal_failure_state_resets_after_success(self):
        company = {
            "heal_failure_streak": 4,
            "cooldown_until": "2026-04-10T12:00:00+00:00",
            "manual_only_suggested": True,
            "heal_last_failure_detail": "Blocked",
        }
        _reset_heal_failure_state(company)
        self.assertEqual(company["heal_failure_streak"], 0)
        self.assertFalse(company["manual_only_suggested"])
        self.assertNotIn("cooldown_until", company)
        self.assertNotIn("heal_last_failure_detail", company)

    def test_healer_discovery_budget_exhausts_immediately(self):
        healer = ATSHealer(session=None)
        healer.discovery_budget_ms = 0
        result = healer.discover({"name": "SlowCo", "domain": "slowco.example", "careers_url": ""}, force=False, deep=False)
        self.assertEqual(result.status, "NOT_FOUND")
        self.assertEqual(result.detail, "Discovery budget exhausted")

    def test_workday_target_health_cooldown_skips_company(self):
        original_db_path = db.DB_PATH
        try:
            with tempfile.TemporaryDirectory() as tmpdir:
                temp_db = Path(tmpdir) / "workday_health.db"
                db.DB_PATH = temp_db
                conn = db.get_connection()
                try:
                    db.update_workday_target_health(
                        conn,
                        company="CooldownCo",
                        careers_url="https://cooldownco.wd1.myworkdayjobs.com/External",
                        status="budget_exhausted",
                        elapsed_ms=510000.0,
                        cooldown_days=7,
                        notes="Budget exhausted after HTML fallback",
                    )
                finally:
                    conn.close()
                engine = ScraperEngine(
                    preferences={},
                    companies=[
                        {
                            "name": "CooldownCo",
                            "active": True,
                            "adapter": "workday",
                            "careers_url": "https://cooldownco.wd1.myworkdayjobs.com/External",
                        }
                    ],
                )
                reason = engine._workday_cooldown_reason(engine.companies[0])
                self.assertIn("Cooldown until", reason)
        finally:
            db.DB_PATH = original_db_path

    def test_workday_target_health_tracks_successful_tenants_without_cooldown(self):
        conn = sqlite3.connect(":memory:")
        conn.row_factory = sqlite3.Row
        db.init_db(conn)
        db.update_workday_target_health(
            conn,
            company="HealthyWorkday",
            careers_url="https://healthy.wd1.myworkdayjobs.com/External",
            status="ok",
            elapsed_ms=12000.0,
            evaluated_count=42,
        )
        row = db.get_workday_target_health(conn, "HealthyWorkday")
        self.assertEqual(int(row["success_count"]), 1)
        self.assertEqual(int(row["empty_streak"]), 0)
        self.assertEqual(int(row["last_evaluated"]), 42)
        self.assertIsNone(row["cooldown_until"])
        conn.close()

    def test_tracker_summary_excludes_scraper_only_considering_rows(self):
        rows = [
            {"status": "considering"},
            {"status": "exploring"},
            {"status": "applied"},
            {"status": "interviewing"},
            {"status": "rejected"},
        ]
        filtered = _formal_tracker_rows(rows)
        metrics = _summary_metrics_for_rows(filtered)
        self.assertEqual(len(filtered), 4)
        self.assertEqual(metrics["total"], 4)
        self.assertEqual(metrics["active"], 2)
        self.assertEqual(metrics["interviewing"], 1)
        self.assertEqual(metrics["rejected"], 1)

    def test_follow_up_scheduler_defaults(self):
        self.assertIsNone(_default_follow_up_date("considering"))
        self.assertEqual(str(_default_follow_up_date("applied", date(2026, 4, 3))), "2026-04-10")
        self.assertEqual(str(_default_follow_up_date("screening", date(2026, 4, 3))), "2026-04-06")
        self.assertEqual(str(_default_follow_up_date("interviewing", date(2026, 4, 3))), "2026-04-05")
        self.assertEqual(_follow_up_template_note("applied"), "Follow up on application status")

    def test_sidebar_metrics_are_explicit_and_consistent(self):
        df = pd.DataFrame(
            [
                {"status": "considering"},
                {"status": "considering"},
                {"status": "applied"},
                {"status": "screening"},
                {"status": "rejected"},
            ]
        )
        metrics = _sidebar_metrics_for_df(df)
        self.assertEqual(metrics["scraped_leads"], 2)
        self.assertEqual(metrics["tracked"], 3)
        self.assertEqual(metrics["active"], 2)

    def test_work_type_helpers_normalize_and_filter_contract_roles(self):
        df = pd.DataFrame(
            [
                {"title": "A", "work_type": "fte"},
                {"title": "B", "work_type": "W2"},
                {"title": "C", "work_type": "1099_contract"},
                {"title": "D", "work_type": ""},
            ]
        )
        self.assertEqual(_normalize_work_type("W2"), "w2_contract")
        self.assertEqual(_work_type_label("1099_contract"), "1099 hourly")
        filtered = _apply_work_type_filter(df, "Contract Only")
        self.assertEqual(filtered["title"].tolist(), ["B", "C"])
        unknown_only = _apply_work_type_filter(df, "Unknown Only")
        self.assertEqual(unknown_only["title"].tolist(), ["D"])

    def test_work_type_unknown_rows_are_countable(self):
        df = pd.DataFrame(
            [
                {"effective_bucket": "APPLY NOW", "work_type": ""},
                {"effective_bucket": "REVIEW TODAY", "work_type": "fte"},
                {"effective_bucket": "WATCH", "work_type": None},
                {"effective_bucket": "MANUAL REVIEW", "work_type": "w2"},
                {"effective_bucket": "Applied", "work_type": "fte"},
            ]
        )
        match_population = df[df["effective_bucket"].isin(["APPLY NOW", "REVIEW TODAY", "WATCH", "MANUAL REVIEW"])].copy()
        work_type_series = match_population["work_type"].map(_normalize_work_type)
        self.assertEqual(int((work_type_series == "unknown").sum()), 2)
        self.assertEqual(int((work_type_series == "fte").sum()), 1)
        self.assertEqual(int(work_type_series.isin({"w2_contract", "1099_contract", "c2c_contract", "contract"}).sum()), 1)

    def test_work_type_filter_leaves_manual_review_rows_without_work_type(self):
        df = pd.DataFrame([{"company": "ADP", "adapter": "generic"}])
        filtered = _apply_work_type_filter(df, "Contract Only")
        self.assertEqual(filtered.to_dict("records"), [{"company": "ADP", "adapter": "generic"}])

    def test_generic_adapter_rejects_marketing_and_leadership_links(self):
        adapter = GenericAdapter()
        self.assertFalse(
            adapter._is_probable_job_link("Leadership and Corporate Governance", "https://cetera.com/about-cetera/leadership")
        )
        self.assertFalse(
            adapter._is_probable_job_link("FUND & INVESTOR SOLUTIONS", "https://www.bny.com/corporate/global/en/solutions/platforms/fund-investor-solutions")
        )
        self.assertFalse(
            adapter._is_probable_job_link("Automate and Streamline Your Finance Operation", "https://tipalti.com/integrations/")
        )

    def test_generic_adapter_keeps_real_role_titles(self):
        adapter = GenericAdapter()
        self.assertTrue(
            adapter._is_probable_job_link("Associate Product Manager, FIC Support", "https://jobs.lever.co/trustly/abc123")
        )

    def test_generic_adapter_skips_static_pages_without_opening_signals(self):
        adapter = GenericAdapter()
        html = """
        <html><body>
          <h1>Our Platform</h1>
          <p>Learn about our embedded finance infrastructure and integrations.</p>
        </body></html>
        """
        soup = BeautifulSoup(html, "html.parser")
        self.assertFalse(
            adapter._page_looks_like_openings_page("https://example.com/platform", soup, html, {"name": "ExampleCo"})
        )

    def test_generic_adapter_accepts_careers_pages_with_opening_markers(self):
        adapter = GenericAdapter()
        html = """
        <html><body>
          <h1>Careers</h1>
          <p>Explore our current openings and join our team.</p>
        </body></html>
        """
        soup = BeautifulSoup(html, "html.parser")
        self.assertTrue(
            adapter._page_looks_like_openings_page("https://example.com/company", soup, html, {"name": "ExampleCo"})
        )

    def test_contractor_board_rejects_category_pages(self):
        adapter = GenericAdapter()
        self.assertFalse(
            adapter._is_probable_job_link(
                "Project / Program Management Project / Program Management",
                "https://motionrecruitment.com/tech-jobs/contract?specialties=project-program-management",
                {"contractor_source": True},
            )
        )
        self.assertFalse(
            adapter._is_probable_job_link(
                "Jobs Directory Jobs Directory",
                "https://www.dice.com/jobs/browse-jobs",
                {"contractor_source": True},
            )
        )

    def test_contractor_board_keeps_detail_links(self):
        adapter = GenericAdapter()
        self.assertTrue(
            adapter._is_probable_job_link(
                "Associate Product Manager, FIC Support",
                "https://www.dice.com/job-detail/12345678-abcd-1234-abcd-1234567890ab",
                {"contractor_source": True},
            )
        )

    def test_dice_adapter_extracts_job_detail_cards(self):
        html = """
        <html><body>
          <a href="/job-detail/12345678-abcd-1234-abcd-1234567890ab">View Details for Senior Technical Product Manager</a>
          <div>Remote</div>
          <a href="/jobs/browse-jobs">Browse Jobs</a>
        </body></html>
        """
        adapter = _FakeDiceAdapter(html)
        jobs = adapter.scrape(
            {
                "name": "Dice Contract",
                "careers_url": "https://www.dice.com/jobs/jtype-Contracts--jobs",
                "tier": 4,
            }
        )
        self.assertEqual(len(jobs), 1)
        self.assertEqual(jobs[0].adapter, "dice")
        self.assertEqual(jobs[0].source, "Dice")
        self.assertEqual(jobs[0].work_type, "w2_contract")
        self.assertIn("/job-detail/", jobs[0].url)
        self.assertEqual(jobs[0].role_title_raw, "Senior Technical Product Manager")

    def test_happydance_is_detected_as_blocked(self):
        adapter = _FakeAdapter()
        with self.assertRaises(BlockedSiteError):
            adapter._raise_if_blocked(
                _Resp(
                    text="Sorry, you have been blocked. You are unable to access happydance.website",
                    status_code=403,
                    url="https://jobs.adp.com/en/jobs/",
                ),
                "https://jobs.adp.com/en/jobs/",
            )

    def test_international_remote_allowed_when_enabled(self):
        prefs = {
            "search": {
                "geography": {"us_only": True, "allow_international_remote": True},
                "compensation": {"min_salary_usd": 165000, "target_salary_usd": 165000},
                "contractor": {
                    "include_contract_roles": True,
                    "allow_w2_hourly": True,
                    "allow_1099_hourly": True,
                    "default_hours_per_week": 40,
                    "default_w2_weeks_per_year": 50,
                    "default_1099_weeks_per_year": 46,
                    "benefits_replacement_usd": 18000,
                    "w2_benefits_gap_usd": 6000,
                    "overhead_1099_pct": 0.18,
                },
            },
            "titles": {"positive_weights": {"solution architect": 8}},
            "keywords": {},
            "scoring": {"minimum_score_to_keep": 35},
        }
        scorer = Scorer(prefs)
        result = scorer.score_job(
            {
                "title": "Senior Solution Architect",
                "description": "Enterprise architecture and API integration",
                "tier": 1,
                "location": "Remote - Australia",
            }
        )
        self.assertEqual(result["score_components"]["location_penalty"], 0)

    def test_w2_hourly_normalizes_to_annual_equivalent(self):
        prefs = {
            "search": {
                "geography": {"us_only": False, "allow_international_remote": True},
                "compensation": {"min_salary_usd": 165000, "target_salary_usd": 165000, "allow_missing_salary": True},
                "contractor": {
                    "include_contract_roles": True,
                    "allow_w2_hourly": True,
                    "allow_1099_hourly": True,
                    "default_hours_per_week": 40,
                    "default_w2_weeks_per_year": 50,
                    "default_1099_weeks_per_year": 46,
                    "benefits_replacement_usd": 18000,
                    "w2_benefits_gap_usd": 6000,
                    "overhead_1099_pct": 0.18,
                },
            },
            "titles": {"positive_weights": {"technical product manager": 8}},
            "keywords": {},
            "scoring": {"minimum_score_to_keep": 35},
        }
        scorer = Scorer(prefs)
        result = scorer.score_job(
            {
                "title": "Technical Product Manager",
                "description": "W2 contract, hourly role, API integration",
                "tier": 1,
                "location": "Remote",
                "salary_text": "$100/hr W2",
            }
        )
        self.assertEqual(result["work_type"], "w2_contract")
        self.assertEqual(result["compensation_unit"], "hourly")
        self.assertAlmostEqual(result["hourly_rate"], 100.0)
        self.assertAlmostEqual(result["normalized_compensation_usd"], 194000.0)

    def test_work_type_inference_handles_part_time_temp_and_internship(self):
        prefs = {
            "search": {
                "geography": {"us_only": False, "allow_international_remote": True},
                "compensation": {"min_salary_usd": 100000, "target_salary_usd": 100000, "allow_missing_salary": True},
                "contractor": {
                    "include_contract_roles": True,
                    "allow_w2_hourly": True,
                    "allow_1099_hourly": True,
                },
            },
            "titles": {"positive_weights": {"product manager": 8}},
            "keywords": {},
            "scoring": {"minimum_score_to_keep": 35},
        }
        scorer = Scorer(prefs)
        self.assertEqual(scorer.score_job({"title": "Product Manager Intern", "description": "", "location": "Remote"})["work_type"], "internship")
        self.assertEqual(scorer.score_job({"title": "Part-Time Product Manager", "description": "", "location": "Remote"})["work_type"], "part_time")
        self.assertEqual(scorer.score_job({"title": "Product Manager", "description": "Temporary assignment", "location": "Remote"})["work_type"], "temporary")

    def test_1099_hourly_applies_overhead_and_benefits(self):
        prefs = {
            "search": {
                "geography": {"us_only": False, "allow_international_remote": True},
                "compensation": {"min_salary_usd": 165000, "target_salary_usd": 165000, "allow_missing_salary": True},
                "contractor": {
                    "include_contract_roles": True,
                    "allow_w2_hourly": True,
                    "allow_1099_hourly": True,
                    "default_hours_per_week": 40,
                    "default_w2_weeks_per_year": 50,
                    "default_1099_weeks_per_year": 46,
                    "benefits_replacement_usd": 18000,
                    "w2_benefits_gap_usd": 6000,
                    "overhead_1099_pct": 0.18,
                },
            },
            "titles": {"positive_weights": {"solution architect": 8}},
            "keywords": {},
            "scoring": {"minimum_score_to_keep": 35},
        }
        scorer = Scorer(prefs)
        result = scorer.score_job(
            {
                "title": "Senior Solution Architect",
                "description": "1099 contract role, enterprise architecture",
                "tier": 1,
                "location": "Remote",
                "salary_text": "$120/hr 1099",
            }
        )
        self.assertEqual(result["work_type"], "1099_contract")
        self.assertAlmostEqual(result["normalized_compensation_usd"], 163056.0)

    def test_compensation_calculator_matches_scoring_assumptions(self):
        contractor_cfg = {
            "benefits_replacement_usd": 18000,
            "w2_benefits_gap_usd": 6000,
            "overhead_1099_pct": 0.18,
        }
        w2 = _annualized_compensation_preview("w2_hourly", 100.0, 40.0, 50.0, contractor_cfg)
        contract_1099 = _annualized_compensation_preview("1099_hourly", 120.0, 40.0, 46.0, contractor_cfg)
        self.assertAlmostEqual(w2["normalized_compensation_usd"], 194000.0)
        self.assertAlmostEqual(contract_1099["normalized_compensation_usd"], 163056.0)

    def test_role_velocity_labels_and_summary(self):
        df = pd.DataFrame(
            [
                {
                    "date_discovered": "2026-03-28",
                    "first_seen_at": "2026-03-28T00:00:00",
                    "last_seen_at": "2026-04-03T00:00:00",
                    "seen_count": 1,
                },
                {
                    "date_discovered": "2026-02-20",
                    "first_seen_at": "2026-02-20T00:00:00",
                    "last_seen_at": "2026-04-03T00:00:00",
                    "seen_count": 4,
                },
                {
                    "date_discovered": "2026-01-15",
                    "first_seen_at": "2026-01-15T00:00:00",
                    "last_seen_at": "2026-04-03T00:00:00",
                    "seen_count": 5,
                },
                {
                    "date_discovered": "2026-02-10",
                    "first_seen_at": "2026-02-10T00:00:00",
                    "last_seen_at": "2026-03-20T00:00:00",
                    "seen_count": 2,
                },
            ]
        )
        decorated = _decorate_role_velocity(df)
        self.assertEqual(list(decorated["velocity"]), ["New", "Recurring", "Reposted", "Dormant"])
        summary = _role_velocity_summary(decorated)
        self.assertEqual(summary["stale"], 0)
        self.assertEqual(summary["reposted"], 1)
        self.assertEqual(summary["dormant"], 1)

    def test_job_observation_aggregation_is_returned(self):
        conn = sqlite3.connect(":memory:")
        conn.row_factory = sqlite3.Row
        db.init_db(conn)
        app_id = db.add_application(
            conn,
            company="VelocityCo",
            role="Platform Architect",
            status="considering",
            scraper_key="velocity-1",
            date_discovered="2026-03-01",
            created_at="2026-03-01T00:00:00",
            updated_at="2026-03-01T00:00:00",
        )
        db.add_job_observation(conn, app_id, "2026-03-01T12:00:00", score=72.0)
        db.add_job_observation(conn, app_id, "2026-03-10T12:00:00", score=75.0)
        conn.commit()

        row = db.get_application(conn, app_id)
        self.assertEqual(row["seen_count"], 2)
        self.assertEqual(row["first_seen_at"], "2026-03-01T12:00:00")
        self.assertEqual(row["last_seen_at"], "2026-03-10T12:00:00")
        conn.close()

    def test_question_bank_can_link_to_company_and_application(self):
        conn = sqlite3.connect(":memory:")
        conn.row_factory = sqlite3.Row
        db.init_db(conn)
        app_id = db.add_application(
            conn,
            company="Stripe",
            role="Solutions Architect",
            status="interviewing",
            scraper_key="stripe-question-1",
            date_discovered="2026-04-01",
            created_at="2026-04-01T00:00:00",
            updated_at="2026-04-01T00:00:00",
        )
        shared_id = db.add_question(conn, "Tell me about yourself.", "behavioral")
        company_id = db.add_question(conn, "Why Stripe?", "behavioral", company="Stripe")
        app_question_id = db.add_question(conn, "Walk me through an API integration launch.", "technical", company="Stripe", application_id=app_id)

        linked = db.get_questions(conn, company="Stripe", application_id=app_id)
        linked_ids = [row["id"] for row in linked]
        self.assertIn(shared_id, linked_ids)
        self.assertIn(company_id, linked_ids)
        self.assertIn(app_question_id, linked_ids)
        conn.close()

    def test_rejection_pattern_helpers(self):
        self.assertEqual(_title_family("Senior Enterprise Architect"), "Architect")
        self.assertEqual(_title_family("Lead Product Manager"), "Product")
        self.assertEqual(_title_family("Business Systems Analyst"), "Analyst")
        self.assertEqual(_parse_keyword_blob("['mortgage', 'consumer lending']"), ["mortgage", "consumer lending"])
        self.assertEqual(_parse_keyword_blob("mortgage, consumer lending"), ["mortgage", "consumer lending"])

    def test_resume_gap_helpers_detect_missing_keywords(self):
        self.assertTrue(_resume_contains_keyword("Built API integration pipelines and data lineage controls", "API integration"))
        self.assertFalse(_resume_contains_keyword("Built API integration pipelines and data lineage controls", "Data Vault"))

        df = pd.DataFrame(
            [
                {
                    "company": "Advisor360",
                    "title": "Enterprise Solutions Architect",
                    "score": 82.0,
                    "matched_keywords": "['API integration', 'Data Vault', 'capital markets systems']",
                    "status": "considering",
                },
                {
                    "company": "BNY Mellon",
                    "title": "Data Platform Architect",
                    "score": 78.0,
                    "matched_keywords": "['Data Vault', 'data lineage']",
                    "status": "considering",
                },
                {
                    "company": "NoiseCo",
                    "title": "Irrelevant Role",
                    "score": 34.0,
                    "matched_keywords": "['mortgage']",
                    "status": "considering",
                },
            ]
        )
        resume_text = "Enterprise architecture, API integration, and data lineage across wealth platforms."
        gap_df = _resume_gap_rows(df, resume_text, minimum_score=60.0)
        keywords = gap_df["keyword"].tolist()
        self.assertIn("Data Vault", keywords)
        self.assertIn("capital markets systems", keywords)
        self.assertNotIn("API integration", keywords)
        self.assertNotIn("mortgage", keywords)

        data_vault_row = gap_df.loc[gap_df["keyword"] == "Data Vault"].iloc[0]
        self.assertEqual(int(data_vault_row["roles"]), 2)

    def test_resume_gap_helpers_respect_ignore_keywords(self):
        df = pd.DataFrame(
            [
                {
                    "company": "Advisor360",
                    "title": "Enterprise Solutions Architect",
                    "score": 82.0,
                    "matched_keywords": "['Data Vault', 'capital markets systems']",
                    "status": "considering",
                }
            ]
        )
        gap_df = _resume_gap_rows(
            df,
            "Enterprise solutions architecture",
            minimum_score=60.0,
            ignored_keywords=["Data Vault"],
        )
        self.assertEqual(gap_df["keyword"].tolist(), ["capital markets systems"])

    def test_resume_upload_extractor_supports_txt_and_docx(self):
        txt_file = _UploadedFileStub("master_resume.txt", b"API integration\nData lineage\n")
        txt_text, txt_type = _extract_resume_text(txt_file)
        self.assertEqual(txt_type, "txt")
        self.assertIn("API integration", txt_text)

        document_xml = b"""<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
        <w:document xmlns:w="http://schemas.openxmlformats.org/wordprocessingml/2006/main">
          <w:body>
            <w:p><w:r><w:t>Enterprise architecture</w:t></w:r></w:p>
            <w:p><w:r><w:t>Capital markets systems</w:t></w:r></w:p>
          </w:body>
        </w:document>"""
        docx_buffer = io.BytesIO()
        with zipfile.ZipFile(docx_buffer, "w") as zf:
            zf.writestr("word/document.xml", document_xml)
        docx_file = _UploadedFileStub("master_resume.docx", docx_buffer.getvalue())
        docx_text, docx_type = _extract_resume_text(docx_file)
        self.assertEqual(docx_type, "docx")
        self.assertIn("Enterprise architecture", docx_text)
        self.assertIn("Capital markets systems", docx_text)

    def test_email_signal_classifier_detects_three_signal_types(self):
        known = ["Stripe", "Acme"]
        app_signal = classify_email_signal(
            message_id="m1",
            thread_id="t1",
            sender="jobs@stripe.com",
            subject="Thank you for applying to Stripe",
            body="We have received your application for the Product Architect role.",
            received_at="2026-04-03T10:00:00",
            known_companies=known,
        )
        reject_signal = classify_email_signal(
            message_id="m2",
            thread_id="t2",
            sender="careers@acme.com",
            subject="Update on your application",
            body="Unfortunately, we are moving forward with other candidates.",
            received_at="2026-04-03T10:00:00",
            known_companies=known,
        )
        interview_signal = classify_email_signal(
            message_id="m3",
            thread_id="t3",
            sender="recruiting@stripe.com",
            subject="Schedule an interview",
            body="Please share your availability for the next interview for the Solutions Architect role.",
            received_at="2026-04-03T10:00:00",
            known_companies=known,
        )
        self.assertEqual(app_signal["signal_type"], "new_application")
        self.assertEqual(app_signal["company"], "Stripe")
        self.assertEqual(reject_signal["signal_type"], "rejection")
        self.assertEqual(interview_signal["signal_type"], "interview_request")
        self.assertEqual(interview_signal["role"], "Solutions Architect")

    def test_email_signal_storage_and_matching(self):
        conn = sqlite3.connect(":memory:")
        conn.row_factory = sqlite3.Row
        db.init_db(conn)
        app_id = db.add_application(
            conn,
            company="Stripe",
            role="Solutions Architect",
            status="applied",
            scraper_key="stripe-1",
            date_discovered="2026-04-01",
            created_at="2026-04-01T00:00:00",
            updated_at="2026-04-01T00:00:00",
        )
        match = db.find_best_application_match(conn, "Stripe", "Solutions Architect")
        self.assertEqual(match["id"], app_id)
        signal_id = db.upsert_email_signal(
            conn,
            message_id="gmail-1",
            signal_type="interview_request",
            subject="Schedule an interview",
            sender="recruiting@stripe.com",
            company="Stripe",
            role="Solutions Architect",
            application_id=app_id,
        )
        signals = db.get_email_signals(conn, signal_status="new")
        self.assertEqual(len(signals), 1)
        self.assertEqual(signals[0]["id"], signal_id)
        self.assertEqual(signals[0]["linked_company"], "Stripe")
        conn.close()

    def test_interview_signal_extracts_schedule_and_link(self):
        signal = classify_email_signal(
            message_id="msg-1",
            thread_id=None,
            sender="Jane Recruiter <jane@example.com>",
            subject="Interview request for Senior Product Manager",
            body=(
                "We would like to interview you on April 7, 2026 at 2:30 pm with Jane Recruiter. "
                "Please join via https://meet.google.com/example and plan for 45 minutes."
            ),
            received_at="Fri, 03 Apr 2026 10:00:00 -0000",
            known_companies=["Assured"],
        )
        self.assertIsNotNone(signal)
        self.assertEqual(signal["signal_type"], "interview_request")
        self.assertEqual(signal["interviewer_names"], "Jane Recruiter")
        self.assertEqual(signal["interview_duration_mins"], 45)
        self.assertIn("2026-04-07T14:30:00", signal["interview_scheduled_at"])
        self.assertEqual(signal["interview_location"], "https://meet.google.com/example")

    def test_interview_signal_detects_weekday_recruiter_email(self):
        signal = classify_email_signal(
            message_id="msg-2",
            thread_id=None,
            sender="Recruiter <recruiter@advisor360.com>",
            subject="Availability for Monday",
            body=(
                "I'd like to connect on Monday at 11:00 am. "
                "Please confirm your availability and join via Microsoft Teams."
            ),
            received_at="Fri, 03 Apr 2026 10:00:00 -0000",
            known_companies=["Advisor360"],
        )
        self.assertIsNotNone(signal)
        self.assertEqual(signal["signal_type"], "interview_request")
        self.assertEqual(signal["company"], "Advisor360")
        self.assertIn("2026-04-06T11:00:00", signal["interview_scheduled_at"])

    def test_interview_signal_extracts_meet_link_and_duration_from_range(self):
        signal = classify_email_signal(
            message_id="msg-3",
            thread_id=None,
            sender="David Korbel <david@example.com>",
            subject="[Advisor360] Interview confirmation",
            body=(
                "Date and time: Monday, April 6, 2026 at 11:00 AM - 11:30 AM MDT\n"
                "Meeting link: meet.google.com/qsb-jwfi-wae\n"
                "Schedule\n"
                "11:00 AM - 11:30 AM MDT: Drew Norell (Director, Solutions Architecture, Client Onboarding)"
            ),
            received_at="Thu, 02 Apr 2026 14:40:00 -0000",
            known_companies=["Advisor360"],
        )
        self.assertIsNotNone(signal)
        self.assertEqual(signal["interview_duration_mins"], 30)
        self.assertEqual(signal["interview_location"], "https://meet.google.com/qsb-jwfi-wae")
        self.assertEqual(signal["interviewer_names"], "Drew Norell")

    def test_interview_type_inference_prefers_video_for_meet_links(self):
        interview_type = infer_interview_type(
            "[Advisor360] Interview confirmation",
            "Meeting link: https://meet.google.com/qsb-jwfi-wae",
            "David Korbel <mail@ats.rippling.com>",
            "https://meet.google.com/qsb-jwfi-wae",
        )
        self.assertEqual(interview_type, "video")

    def test_parse_imap_message_includes_calendar_payload(self):
        msg = EmailMessage()
        msg["From"] = "recruiting@stripe.com"
        msg["Subject"] = "Interview invite"
        msg["Date"] = "Fri, 03 Apr 2026 10:00:00 -0000"
        msg["Message-ID"] = "<calendar123@example.com>"
        msg.set_content("Please see attached invite.")
        msg.add_attachment(
            "BEGIN:VCALENDAR\nBEGIN:VEVENT\nDTSTART:20260407T143000Z\nLOCATION:https://meet.google.com/example\nEND:VEVENT\nEND:VCALENDAR",
            subtype="calendar",
        )
        parsed = _parse_imap_message(msg.as_bytes())
        self.assertIn("DTSTART:20260407T143000Z", parsed["body"])
        self.assertIn("LOCATION:https://meet.google.com/example", parsed["body"])

    def test_parse_imap_message_extracts_headers_and_plain_text(self):
        msg = EmailMessage()
        msg["From"] = "recruiting@stripe.com"
        msg["Subject"] = "Schedule an interview"
        msg["Date"] = "Fri, 03 Apr 2026 10:00:00 -0000"
        msg["Message-ID"] = "<abc123@example.com>"
        msg.set_content("Please share your availability for the Solutions Architect role.")
        parsed = _parse_imap_message(msg.as_bytes())
        self.assertEqual(parsed["message_id"], "abc123@example.com")
        self.assertEqual(parsed["sender"], "recruiting@stripe.com")
        self.assertEqual(parsed["subject"], "Schedule an interview")
        self.assertIn("Solutions Architect", parsed["body"])

    def test_parse_imap_message_preserves_anchor_hrefs_in_html(self):
        msg = EmailMessage()
        msg["From"] = "recruiting@advisor360.com"
        msg["Subject"] = "[Advisor360] Interview confirmation"
        msg["Date"] = "Thu, 02 Apr 2026 14:40:00 -0000"
        msg["Message-ID"] = "<html123@example.com>"
        msg.add_alternative(
            '<html><body><p>Meeting Link: <a href="https://meet.google.com/qsb-jwfi-wae">Meeting Link</a></p></body></html>',
            subtype="html",
        )
        parsed = _parse_imap_message(msg.as_bytes())
        self.assertIn("https://meet.google.com/qsb-jwfi-wae", parsed["body"])

    def test_find_matching_interview_by_schedule(self):
        conn = sqlite3.connect(":memory:")
        conn.row_factory = sqlite3.Row
        db.init_db(conn)
        app_id = db.add_application(conn, company="Stripe", role="Solutions Architect", status="interviewing", entry_type="application")
        db.add_interview(
            conn,
            app_id,
            round_number=1,
            interview_type="phone_screen",
            scheduled_at="2026-04-07T14:30:00",
            interviewer_names="Jane Recruiter",
            location="https://meet.google.com/example",
        )
        matched = db.find_matching_interview(
            conn,
            app_id,
            scheduled_at="2026-04-07T14:30:00",
            interviewer_names="Jane Recruiter",
            location="https://meet.google.com/example",
        )
        self.assertIsNotNone(matched)

    def test_interview_debrief_fields_persist_and_appear_in_signal_rows(self):
        conn = sqlite3.connect(":memory:")
        conn.row_factory = sqlite3.Row
        db.init_db(conn)
        app_id = db.add_application(
            conn,
            company="Advisor360",
            role="Enterprise Solutions Architect",
            status="interviewing",
            entry_type="application",
        )
        interview_id = db.add_interview(
            conn,
            app_id,
            round_number=2,
            interview_type="video",
            scheduled_at="2026-04-06T11:00:00",
            interviewer_names="Drew Norell",
            location="https://meet.google.com/qsb-jwfi-wae",
        )
        db.update_interview(
            conn,
            interview_id,
            outcome="passed",
            rapport_score=4,
            role_clarity_score=5,
            interviewer_engaged_score=4,
            confidence_score=5,
            next_steps_clear=1,
            timeline_mentioned=1,
            compensation_discussed=0,
            availability_discussed=1,
            debrief_notes="Strong hiring-manager round with clear next steps.",
            outcome_notes="Strong hiring-manager round with clear next steps.",
        )
        row = db.get_interviews(conn, app_id)[0]
        self.assertEqual(row["rapport_score"], 4)
        self.assertEqual(row["role_clarity_score"], 5)
        self.assertEqual(row["confidence_score"], 5)
        self.assertEqual(row["next_steps_clear"], 1)
        self.assertEqual(row["timeline_mentioned"], 1)
        self.assertEqual(row["debrief_notes"], "Strong hiring-manager round with clear next steps.")

        signal_rows = db.get_interview_signal_rows(conn)
        self.assertEqual(len(signal_rows), 1)
        self.assertEqual(signal_rows[0]["company"], "Advisor360")
        self.assertEqual(signal_rows[0]["outcome"], "passed")
        conn.close()

    def test_negotiation_playbook_flags_below_floor_and_market_gap(self):
        app = {
            "offer_base": 175000,
            "offer_expiry_date": "2026-04-10",
            "offer_remote_policy": "Hybrid",
            "offer_equity": "RSU",
            "offer_signing": 15000,
        }
        lines = _negotiation_playbook_lines(
            app,
            target_base=195000,
            walkaway_base=180000,
            market_low=190000,
            market_high=220000,
        )
        rendered = "\n".join(lines)
        self.assertIn("Ask: `$195,000`", rendered)
        self.assertIn("below your walk-away floor", rendered)
        self.assertIn("Offer deadline: `2026-04-10`", rendered)
        self.assertIn("Remote policy is `Hybrid`", rendered)
        self.assertIn("Equity is part of the package", rendered)
        self.assertIn("There is already a sign-on component", rendered)
        self.assertIn("Offer is below your market range", rendered)

    def test_negotiation_counter_draft_includes_target_and_context(self):
        app = {
            "company": "Advisor360",
            "role": "Enterprise Solutions Architect",
            "offer_base": 175000,
            "offer_remote_policy": "Hybrid",
            "offer_equity": "RSU",
        }
        draft = _negotiation_counter_draft(app, target_base=190000, market_low=185000, market_high=215000)
        self.assertIn("Advisor360", draft)
        self.assertIn("Enterprise Solutions Architect", draft)
        self.assertIn("$190,000", draft)
        self.assertIn("market range for comparable roles", draft)
        self.assertIn("Hybrid", draft)

    def test_gmail_sync_flag_is_boolean(self):
        self.assertIsInstance(settings.gmail_sync_enabled, bool)

    def test_offer_comparison_rows_normalize_salary_and_1099(self):
        rows = _offer_comparison_rows(
            [
                {
                    "company": "SalaryCo",
                    "role": "Principal Architect",
                    "work_type": "fte",
                    "compensation_unit": "salary",
                    "offer_base": 190000,
                    "salary_low": None,
                    "salary_high": None,
                    "hourly_rate": None,
                    "hours_per_week": None,
                    "weeks_per_year": None,
                    "offer_bonus_pct": 10,
                    "offer_signing": 15000,
                    "offer_pto_days": 20,
                    "offer_k401_match": "4%",
                    "offer_equity": "RSU",
                    "offer_remote_policy": "Remote",
                    "offer_start_date": None,
                    "offer_expiry_date": None,
                    "offer_notes": "",
                },
                {
                    "company": "ContractCo",
                    "role": "Enterprise Solutions Architect",
                    "work_type": "1099_contract",
                    "compensation_unit": "hourly",
                    "offer_base": 0,
                    "salary_low": None,
                    "salary_high": None,
                    "hourly_rate": 120.0,
                    "hours_per_week": 40.0,
                    "weeks_per_year": 46.0,
                    "offer_bonus_pct": 0,
                    "offer_signing": 0,
                    "offer_pto_days": 0,
                    "offer_k401_match": "",
                    "offer_equity": "",
                    "offer_remote_policy": "Remote",
                    "offer_start_date": None,
                    "offer_expiry_date": None,
                    "offer_notes": "",
                },
            ]
        )
        self.assertEqual(len(rows), 2)
        salary_row = next(row for row in rows if row["Company"] == "SalaryCo")
        contract_row = next(row for row in rows if row["Company"] == "ContractCo")
        self.assertEqual(salary_row["Work Type"], "Full-time salary")
        self.assertAlmostEqual(salary_row["Normalized Annual ($)"], 190000.0)
        self.assertAlmostEqual(salary_row["First-Year Cash ($)"], 224000.0)
        self.assertEqual(contract_row["Work Type"], "1099 hourly")
        self.assertAlmostEqual(contract_row["Normalized Annual ($)"], 163056.0)

    def test_offer_comparison_markdown_includes_company_and_role(self):
        markdown = _offer_comparison_markdown(
            [
                {
                    "company": "SalaryCo",
                    "role": "Principal Architect",
                    "work_type": "fte",
                    "compensation_unit": "salary",
                    "offer_base": 190000,
                    "salary_low": None,
                    "salary_high": None,
                    "hourly_rate": None,
                    "hours_per_week": None,
                    "weeks_per_year": None,
                    "offer_bonus_pct": 10,
                    "offer_signing": 15000,
                    "offer_pto_days": 20,
                    "offer_k401_match": "4%",
                    "offer_equity": "RSU",
                    "offer_remote_policy": "Remote",
                    "offer_start_date": None,
                    "offer_expiry_date": None,
                    "offer_notes": "",
                }
            ]
        )
        self.assertIn("Offer Comparison", markdown)
        self.assertIn("SalaryCo", markdown)
        self.assertIn("Principal Architect", markdown)

    def test_resume_tailoring_prefers_missing_keywords(self):
        app = {
            "company": "Advisor360",
            "role": "Enterprise Solutions Architect",
            "matched_keywords": "capital markets,data lineage,api integration,advisor platform",
            "jd_summary": "Architecture role",
        }
        base_resume = {
            "text": "Built API integration platforms for capital markets and wealth systems.",
            "focus": "advisor platform\ndata lineage",
            "ignore": "wealth systems",
            "name": "Master Resume",
        }
        keywords = _tailor_resume_keywords(app, base_resume)
        lowered = [keyword.lower() for keyword in keywords]
        self.assertIn("advisor platform", lowered)
        self.assertIn("data lineage", lowered)
        self.assertNotIn("api integration", lowered)
        self.assertTrue(_resume_contains_phrase(base_resume["text"], "capital markets"))
        brief = _tailored_resume_summary(app, base_resume)
        self.assertIn("Target the Enterprise Solutions Architect role at Advisor360.", brief)

    def test_resume_tailoring_accepts_sqlite_row_records(self):
        conn = sqlite3.connect(":memory:")
        conn.row_factory = sqlite3.Row
        row = conn.execute(
            "SELECT 'Advisor360' AS company, "
            "'Enterprise Solutions Architect' AS role, "
            "'capital markets,data lineage,api integration' AS matched_keywords, "
            "'Architecture role' AS jd_summary"
        ).fetchone()
        base_resume = {
            "text": "Built API integration platforms for capital markets and wealth systems.",
            "focus": "advisor platform\ndata lineage",
            "ignore": "wealth systems",
            "name": "Master Resume",
        }
        keywords = _tailor_resume_keywords(row, base_resume)
        brief = _tailored_resume_summary(row, base_resume)
        lowered = [keyword.lower() for keyword in keywords]
        self.assertIn("advisor platform", lowered)
        self.assertIn("Advisor360", brief)

    def test_find_reschedulable_interview_matches_pending_round(self):
        conn = sqlite3.connect(":memory:")
        conn.row_factory = sqlite3.Row
        db.init_db(conn)
        app_id = db.add_application(conn, company="Advisor360", role="Enterprise Solutions Architect", status="interviewing")
        db.add_interview(
            conn,
            app_id,
            round_number=2,
            interview_type="video",
            scheduled_at="2026-04-06T11:00:00",
            interviewer_names="Drew Norell",
            location="https://meet.google.com/old-link",
            outcome="pending",
        )
        matched = db.find_reschedulable_interview(
            conn,
            app_id,
            scheduled_at="2026-04-07T12:00:00",
            interviewer_names="Drew Norell",
            location="https://meet.google.com/new-link",
        )
        self.assertIsNotNone(matched)
        self.assertEqual(matched["round_number"], 2)
        conn.close()

    def test_resume_variants_persist_per_application(self):
        conn = sqlite3.connect(":memory:")
        conn.row_factory = sqlite3.Row
        db.init_db(conn)
        app_id = db.add_application(conn, company="Advisor360", role="Enterprise Solutions Architect", status="applied")
        variant_id = db.add_resume_variant(
            conn,
            app_id,
            variant_name="Advisor360 tailored",
            source_resume_name="Master Resume",
            content="# Variant",
            notes="Lean into capital markets",
        )
        rows = db.get_resume_variants(conn, app_id)
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["id"], variant_id)
        db.update_resume_variant(conn, variant_id, notes="Updated note")
        updated = db.get_resume_variant(conn, variant_id)
        self.assertEqual(updated["notes"], "Updated note")
        db.delete_resume_variant(conn, variant_id)
        self.assertEqual(db.get_resume_variants(conn, app_id), [])
        conn.close()

    def test_jd_change_detection_flags_material_excerpt_changes(self):
        original = "Build product roadmap for data integrations, API strategy, and platform governance."
        changed = "Lead enterprise architecture for custody conversion, managed accounts, and operating model redesign."
        fingerprint_a = _jd_fingerprint(original, "$180k-$210k", "Remote")
        fingerprint_b = _jd_fingerprint(changed, "$180k-$210k", "Remote")
        self.assertNotEqual(fingerprint_a, fingerprint_b)
        self.assertTrue(
            _is_material_jd_change(
                original,
                changed,
                "$180k-$210k",
                "$180k-$210k",
                "Remote",
                "Remote",
            )
        )

    def test_company_network_summary_counts_contacts_and_referrals(self):
        conn = sqlite3.connect(":memory:")
        conn.row_factory = sqlite3.Row
        db.init_db(conn)
        db.upsert_company_profile(conn, "TestCo", about="Test company")
        db.add_network_contact(
            conn,
            name="Alice Referrer",
            company="TestCo",
            relationship="referral",
            last_contact_date="2026-04-01",
            follow_up_date="2026-04-02",
        )
        db.add_network_contact(
            conn,
            name="Bob Recruiter",
            company="TestCo",
            relationship="recruiter",
        )
        summary = db.get_network_summary_for_company(conn, "TestCo")
        self.assertEqual(summary["contacts"], 2)
        self.assertEqual(summary["reached_out"], 1)
        self.assertEqual(summary["referrals"], 1)
        self.assertGreaterEqual(summary["leverage_score"], 60)
        self.assertEqual(summary["leverage_band"], "Warm intro ready")

    def test_company_network_summary_flags_reach_out_first(self):
        conn = sqlite3.connect(":memory:")
        conn.row_factory = sqlite3.Row
        db.init_db(conn)
        db.add_network_contact(
            conn,
            name="Warm Contact",
            company="ReachCo",
            relationship="former_colleague",
            follow_up_date="2026-04-10",
        )
        summary = db.get_network_summary_for_company(conn, "ReachCo")
        self.assertEqual(summary["contacts"], 1)
        self.assertEqual(summary["reached_out"], 0)
        self.assertEqual(summary["leverage_band"], "Reach out first")
        self.assertGreater(summary["leverage_score"], 0)

    def test_manual_review_line_parser_structures_items(self):
        rows = _parse_manual_review_lines(
            [
                "ADP | adapter=generic | note=Blocked by site protection | url=https://jobs.adp.com/en/jobs/",
            ]
        )
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["company"], "ADP")
        self.assertEqual(rows[0]["adapter"], "generic")
        self.assertEqual(rows[0]["note"], "Blocked by site protection")
        self.assertEqual(rows[0]["url"], "https://jobs.adp.com/en/jobs/")

    def test_manual_review_actions_persist_resolution(self):
        conn = sqlite3.connect(":memory:")
        conn.row_factory = sqlite3.Row
        db.init_db(conn)
        db.set_manual_review_action(
            conn,
            company="ADP",
            adapter="generic",
            url="https://jobs.adp.com/en/jobs/",
            resolution="disabled",
            notes="Disabled from queue.",
        )
        actions = db.get_manual_review_actions(conn)
        self.assertEqual(len(actions), 1)
        self.assertEqual(actions[0]["company"], "ADP")
        self.assertEqual(actions[0]["resolution"], "disabled")

    def test_disable_company_marks_manual_only(self):
        original = settings.companies_yaml.read_text(encoding="utf-8")
        try:
            settings.companies_yaml.write_text(
                "companies:\n"
                "  - name: ExampleCo\n"
                "    active: true\n"
                "    status: active\n",
                encoding="utf-8",
            )
            changed = _disable_company_in_registry("ExampleCo")
            self.assertTrue(changed)
            updated = settings.companies_yaml.read_text(encoding="utf-8")
            self.assertIn("manual_only: true", updated)
            self.assertIn("status: manual_only", updated)
            self.assertIn("active: false", updated)
        finally:
            settings.companies_yaml.write_text(original, encoding="utf-8")

    def test_email_signal_matches_existing_rejected_application_and_auto_resolves(self):
        conn = sqlite3.connect(":memory:")
        conn.row_factory = sqlite3.Row
        db.init_db(conn)
        app_id = db.add_application(
            conn,
            company="Assured",
            role="Product Lead, Data & Integrations",
            status="rejected",
            entry_type="application",
            date_applied="2026-03-31",
        )
        matched = db.find_best_application_match(conn, "Assured", "Product Lead, Data & Integrations")
        self.assertIsNotNone(matched)
        self.assertEqual(matched["id"], app_id)
        signal_status, note = signal_resolution_for_existing_application("new_application", matched["status"])
        self.assertEqual(signal_status, "resolved")
        self.assertIn("already tracked as rejected", note)

    def test_interview_request_is_not_auto_resolved_just_for_interviewing_status(self):
        conn = sqlite3.connect(":memory:")
        conn.row_factory = sqlite3.Row
        db.init_db(conn)
        app_id = db.add_application(
            conn,
            company="Advisor360",
            role="Enterprise Solutions Architect",
            status="interviewing",
            entry_type="application",
        )
        signal = classify_email_signal(
            message_id="msg-4",
            thread_id=None,
            sender="David Korbel <mail@ats.rippling.com>",
            subject="[Advisor360] Interview confirmation",
            body=(
                "Monday, April 6, 2026 at 11:00 AM - 11:30 AM MDT\n"
                "Meeting link: meet.google.com/qsb-jwfi-wae"
            ),
            received_at="Thu, 02 Apr 2026 19:40:39 +0000 (UTC)",
            known_companies=["Advisor360"],
        )
        self.assertIsNotNone(signal)
        matched = db.find_best_application_match(conn, signal["company"], signal["role"])
        self.assertEqual(matched["id"], app_id)
        self.assertIsNone(
            db.find_matching_interview(
                conn,
                app_id,
                scheduled_at=signal["interview_scheduled_at"],
                interviewer_names=signal["interviewer_names"],
                location=signal["interview_location"],
            )
        )
        signal_status, _ = signal_resolution_for_existing_application("interview_request", matched["status"])
        self.assertEqual(signal_status, "new")

    def test_interview_change_type_detects_reschedule_and_cancel(self):
        self.assertEqual(
            infer_interview_change_type("Interview confirmation", "Your interview has been rescheduled to Monday at 11am"),
            "rescheduled",
        )
        self.assertEqual(
            infer_interview_change_type("Interview confirmation", "Your interview has been canceled."),
            "cancelled",
        )

    def test_classify_email_signal_marks_interview_cancellation(self):
        signal = classify_email_signal(
            message_id="msg-cancel",
            thread_id=None,
            sender="David Korbel <mail@ats.rippling.com>",
            subject="[Advisor360] Interview confirmation",
            body="Your interview has been cancelled. Monday, April 6, 2026 at 11:00 AM MDT",
            received_at="Thu, 02 Apr 2026 19:40:39 +0000 (UTC)",
            known_companies=["Advisor360"],
        )
        self.assertIsNotNone(signal)
        self.assertEqual(signal["signal_type"], "interview_request")
        self.assertEqual(signal["interview_change_type"], "cancelled")

    def test_email_signal_company_match_tolerates_name_suffixes(self):
        conn = sqlite3.connect(":memory:")
        conn.row_factory = sqlite3.Row
        db.init_db(conn)
        app_id = db.add_application(
            conn,
            company="Pinnacle Financial Partners",
            role="Enterprise Architect",
            status="applied",
            entry_type="application",
        )
        matched = db.find_best_application_match(conn, "Pinnacle", "Enterprise Architect")
        self.assertIsNotNone(matched)
        self.assertEqual(matched["id"], app_id)

    def test_generic_target_health_enters_cooldown_for_repeated_slow_empty(self):
        conn = sqlite3.connect(":memory:")
        conn.row_factory = sqlite3.Row
        db.init_db(conn)
        db.update_generic_target_health(
            conn,
            company="Slow Generic",
            careers_url="https://example.com/careers",
            status="empty",
            elapsed_ms=20000,
            evaluated_count=0,
            cooldown_days=0,
            notes="No jobs detected",
        )
        db.update_generic_target_health(
            conn,
            company="Slow Generic",
            careers_url="https://example.com/careers",
            status="empty",
            elapsed_ms=22000,
            evaluated_count=0,
            cooldown_days=7,
            notes="No jobs detected",
        )
        row = db.get_generic_target_health(conn, "Slow Generic")
        self.assertEqual(row["empty_streak"], 2)
        self.assertTrue(row["cooldown_until"])

    def test_generic_target_health_resets_after_success(self):
        conn = sqlite3.connect(":memory:")
        conn.row_factory = sqlite3.Row
        db.init_db(conn)
        db.update_generic_target_health(
            conn,
            company="Recovered Generic",
            careers_url="https://example.com/careers",
            status="empty",
            elapsed_ms=21000,
            evaluated_count=0,
            cooldown_days=7,
            notes="No jobs detected",
        )
        db.update_generic_target_health(
            conn,
            company="Recovered Generic",
            careers_url="https://example.com/careers",
            status="ok",
            elapsed_ms=800,
            evaluated_count=5,
            cooldown_days=0,
            notes="Recovered",
        )
        row = db.get_generic_target_health(conn, "Recovered Generic")
        self.assertEqual(row["empty_streak"], 0)
        self.assertEqual(row["success_count"], 1)


if __name__ == "__main__":
    unittest.main()
