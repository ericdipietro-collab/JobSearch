import sys
import unittest
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parents[1]
SRC_DIR = BASE_DIR / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from jobsearch.scraper.adapters.ashby import AshbyAdapter
from jobsearch.scraper.adapters.lever import LeverAdapter
from jobsearch.scraper.adapters.motionrecruitment import MotionRecruitmentAdapter
from jobsearch.scraper.adapters.smartrecruiters import SmartRecruitersAdapter
from jobsearch.scraper.adapters.workday import WorkdayAdapter
from jobsearch.scraper.engine import ScraperEngine


class _FakeLeverAdapter(LeverAdapter):
    def __init__(self, payload):
        super().__init__(session=None, scorer=None)
        self.payload = payload

    def fetch_json(self, url: str):
        return self.payload


class _FakeSmartRecruitersAdapter(SmartRecruitersAdapter):
    def __init__(self, payload):
        super().__init__(session=None, scorer=None)
        self.payload = payload

    def fetch_json(self, url: str):
        return self.payload


class _FakeAshbyAdapter(AshbyAdapter):
    def __init__(self, html: str):
        super().__init__(session=None, scorer=None)
        self.html = html

    def fetch_json(self, url: str):
        raise RuntimeError("force html fallback")

    def fetch_text(self, url: str) -> str:
        return self.html


class _FakeMotionRecruitmentAdapter(MotionRecruitmentAdapter):
    def __init__(self, html: str):
        super().__init__(session=None, scorer=None)
        self.html = html

    def fetch_text(self, url: str) -> str:
        return self.html


class ScraperAdapterRegressionTests(unittest.TestCase):
    def test_engine_maps_adapter_aliases(self):
        self.assertIn("workday_manual", ScraperEngine.ADAPTER_MAP)
        self.assertIn("custom_blackrock", ScraperEngine.ADAPTER_MAP)
        self.assertIn("dice", ScraperEngine.ADAPTER_MAP)

    def test_lever_adapter_ignores_malformed_payload(self):
        adapter = _FakeLeverAdapter({"unexpected": "shape"})
        jobs = adapter.scrape({"name": "Recurly", "adapter_key": "recurly"})
        self.assertEqual(jobs, [])

    def test_lever_adapter_preserves_remote_location(self):
        payload = [
            {
                "text": "Senior Technical Product Manager",
                "hostedUrl": "https://jobs.lever.co/example/123",
                "categories": {"location": "New York, NY"},
                "workplaceType": "remote",
                "descriptionPlain": "API platform role",
            }
        ]
        adapter = _FakeLeverAdapter(payload)
        jobs = adapter.scrape({"name": "ExampleCo", "adapter_key": "example"})
        self.assertEqual(len(jobs), 1)
        self.assertTrue(jobs[0].is_remote)
        self.assertIn("Remote", jobs[0].location)

    def test_ashby_adapter_html_fallback_extracts_jobs(self):
        html = """
        <html><body>
          <div><a href="https://jobs.ashbyhq.com/example/jobs/abc123">Senior Solutions Architect</a></div>
          <div><a href="https://jobs.ashbyhq.com/example/team">Our Team</a></div>
        </body></html>
        """
        adapter = _FakeAshbyAdapter(html)
        jobs = adapter.scrape({"name": "ExampleCo", "careers_url": "https://jobs.ashbyhq.com/example"})
        self.assertEqual(len(jobs), 1)
        self.assertEqual(jobs[0].adapter, "ashby")
        self.assertIn("/jobs/", jobs[0].url)

    def test_workday_candidate_contexts_use_adapter_key_first(self):
        adapter = WorkdayAdapter(session=None, scorer=None)
        contexts = adapter._candidate_contexts(
            "https://alignmenthealthcare.wd12.myworkdayjobs.com/External",
            "https://alignmenthealthcare.wd12.myworkdayjobs.com/Careers",
        )
        self.assertGreaterEqual(len(contexts), 2)
        self.assertEqual(contexts[0][0], "alignmenthealthcare.wd12.myworkdayjobs.com")
        self.assertEqual(contexts[0][1], "alignmenthealthcare")
        self.assertEqual(contexts[0][2], "Careers")

    def test_smartrecruiters_adapter_builds_public_job_url(self):
        payload = {
            "content": [
                {
                    "id": "123456",
                    "name": "Enterprise Solutions Architect",
                    "location": {"city": "Denver", "region": "CO"},
                    "department": {"label": "Solutions"},
                }
            ]
        }
        adapter = _FakeSmartRecruitersAdapter(payload)
        jobs = adapter.scrape({"name": "Visa", "adapter_key": "Visa"})
        self.assertEqual(len(jobs), 1)
        self.assertEqual(jobs[0].url, "https://jobs.smartrecruiters.com/Visa/123456")
        self.assertEqual(jobs[0].location, "Denver, CO")

    def test_motionrecruitment_adapter_extracts_contract_detail_links(self):
        html = """
        <html><body>
          <div>
            <a href="/tech-jobs/chicago/contract/senior-technical-product-manager/12345">Senior Technical Product Manager</a>
            <span>Remote</span>
          </div>
          <div>
            <a href="/tech-jobs/contract?specialties=project-program-management">Project / Program Management</a>
          </div>
        </body></html>
        """
        adapter = _FakeMotionRecruitmentAdapter(html)
        jobs = adapter.scrape(
            {
                "name": "Motion Recruitment Contract",
                "careers_url": "https://motionrecruitment.com/tech-jobs/contract",
                "tier": 4,
            }
        )
        self.assertEqual(len(jobs), 1)
        self.assertEqual(jobs[0].adapter, "motionrecruitment")
        self.assertEqual(jobs[0].source, "Motion Recruitment")
        self.assertEqual(jobs[0].work_type, "w2_contract")
        self.assertIn("/contract/", jobs[0].url)


if __name__ == "__main__":
    unittest.main()
