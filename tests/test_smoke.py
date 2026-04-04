import tempfile
import unittest
from pathlib import Path
import sys

import yaml

BASE_DIR = Path(__file__).resolve().parents[1]
SRC_DIR = BASE_DIR / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from jobsearch import __version__
from jobsearch.cli import _merge_company_lists
from jobsearch.config.settings import rotate_log_file, settings
from jobsearch.scraper.scoring import Scorer


class SmokeTests(unittest.TestCase):
    def test_version_is_release_aligned(self):
        self.assertEqual(__version__, "2.0.0")

    def test_rotate_log_file_keeps_current_log(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            log_path = Path(tmpdir) / "sample.log"
            log_path.write_text("x" * 32, encoding="utf-8")
            rotate_log_file(log_path, keep=2, max_bytes=4)
            self.assertTrue(log_path.with_name("sample.log.1").exists())

    def test_preferences_yaml_loads_into_scorer(self):
        prefs = yaml.safe_load(settings.preferences_yaml.read_text(encoding="utf-8")) or {}
        self.assertIn("contractor", prefs.get("search", {}))
        scorer = Scorer(prefs)
        result = scorer.score_job(
            {
                "title": "Technical Product Manager",
                "description": "API integration, data platform, acceptance criteria, wealth management",
                "tier": 1,
                "location": "Remote",
            }
        )
        self.assertIn("score", result)
        self.assertGreaterEqual(result["score"], 0)

    def test_tipalti_registry_points_to_jobs_page(self):
        companies_data = yaml.safe_load(settings.companies_yaml.read_text(encoding="utf-8")) or {}
        companies = companies_data.get("companies", [])
        tipalti = next((company for company in companies if company.get("name") == "Tipalti"), None)
        self.assertIsNotNone(tipalti)
        self.assertEqual(tipalti.get("careers_url"), "https://tipalti.com/company/jobs")

    def test_contract_test_registry_exists(self):
        contract_yaml = settings.config_dir / "job_search_companies_contract_test.yaml"
        self.assertTrue(contract_yaml.exists())
        companies_data = yaml.safe_load(contract_yaml.read_text(encoding="utf-8")) or {}
        companies = companies_data.get("companies", [])
        self.assertGreaterEqual(len(companies), 2)
        self.assertEqual(companies[0].get("name"), "Motion Recruitment Contract")

    def test_contract_registry_exists(self):
        contract_yaml = settings.contract_companies_yaml
        self.assertTrue(contract_yaml.exists())
        companies_data = yaml.safe_load(contract_yaml.read_text(encoding="utf-8")) or {}
        companies = companies_data.get("companies", [])
        self.assertGreaterEqual(len(companies), 2)
        self.assertEqual(companies[1].get("name"), "Dice Contract")

    def test_company_merge_deduplicates_by_name_and_url(self):
        merged = _merge_company_lists(
            [
                {"name": "Acme", "careers_url": "https://example.com/jobs"},
                {"name": "Bravo", "careers_url": "https://example.com/bravo"},
            ],
            [
                {"name": "Acme", "careers_url": "https://example.com/jobs"},
                {"name": "ContractCo", "careers_url": "https://example.com/contract"},
            ],
        )
        self.assertEqual(len(merged), 3)
        self.assertEqual(merged[-1]["name"], "ContractCo")


if __name__ == "__main__":
    unittest.main()
