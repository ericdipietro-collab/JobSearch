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
from jobsearch.config.settings import rotate_log_file, settings
from jobsearch.scraper.scoring import Scorer


class SmokeTests(unittest.TestCase):
    def test_version_is_release_aligned(self):
        self.assertEqual(__version__, "1.6.0")

    def test_rotate_log_file_keeps_current_log(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            log_path = Path(tmpdir) / "sample.log"
            log_path.write_text("x" * 32, encoding="utf-8")
            rotate_log_file(log_path, keep=2, max_bytes=4)
            self.assertTrue(log_path.with_name("sample.log.1").exists())

    def test_preferences_yaml_loads_into_scorer(self):
        prefs = yaml.safe_load(settings.preferences_yaml.read_text(encoding="utf-8")) or {}
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


if __name__ == "__main__":
    unittest.main()
