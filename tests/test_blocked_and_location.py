import sys
import unittest
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parents[1]
SRC_DIR = BASE_DIR / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from jobsearch.scraper.adapters.base import BaseAdapter, BlockedSiteError
from jobsearch.scraper.adapters.generic import GenericAdapter
from jobsearch.scraper.scoring import Scorer
from jobsearch.app_main import _annualized_compensation_preview, _sidebar_metrics_for_df
from jobsearch.views.tracker_page import _formal_tracker_rows, _summary_metrics_for_rows
import pandas as pd


class _FakeAdapter(BaseAdapter):
    def scrape(self, company_config):
        return []


class _Resp:
    def __init__(self, text="", status_code=200, url="https://example.com"):
        self.text = text
        self.status_code = status_code
        self.url = url


class BlockedAndLocationTests(unittest.TestCase):
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


if __name__ == "__main__":
    unittest.main()
