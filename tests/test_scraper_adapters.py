import sys
import time
import unittest
import re
import json
import tempfile
import types
from unittest import mock
from pathlib import Path
from urllib.parse import parse_qs, urlparse

BASE_DIR = Path(__file__).resolve().parents[1]
SRC_DIR = BASE_DIR / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from jobsearch.scraper.adapters.ashby import AshbyAdapter
from jobsearch.scraper.adapters.adzuna import AdzunaAdapter
from jobsearch.scraper.adapters.indeed_connector import IndeedConnectorAdapter
from jobsearch.scraper.adapters.jobspy_experimental import JobSpyExperimentalAdapter
from jobsearch.scraper.adapters.jooble import JoobleAdapter
from jobsearch.scraper.adapters.lever import LeverAdapter
from jobsearch.scraper.adapters.motionrecruitment import MotionRecruitmentAdapter
from jobsearch.scraper.adapters.rippling import RipplingAdapter
from jobsearch.scraper.adapters.smartrecruiters import SmartRecruitersAdapter
from jobsearch.scraper.adapters.themuse import TheMuseAdapter
from jobsearch.scraper.adapters.usajobs import USAJobsAdapter
from jobsearch.scraper.adapters.workday import WorkdayAdapter
from jobsearch.scraper.engine import ScraperEngine
from jobsearch.scraper.models import Job
from jobsearch.scraper.query_tiers import normalize_search_queries, search_queries_for_tier
from jobsearch.services.healer_service import ATSHealer
from jobsearch.config.settings import settings


class _FakeLeverAdapter(LeverAdapter):
    def __init__(self, payload):
        super().__init__(session=None, scorer=None)
        self.payload = payload

    def fetch_json(self, url: str):
        return self.payload


class _FakeSmartRecruitersAdapter(SmartRecruitersAdapter):
    def __init__(self, payload, scorer=None, html_by_url=None):
        super().__init__(session=None, scorer=scorer)
        self.payload = payload
        self.html_by_url = html_by_url or {}

    def fetch_json(self, url: str):
        if callable(self.payload):
            return self.payload(url)
        return self.payload

    def fetch_text(self, url: str) -> str:
        return self.html_by_url.get(url, "")


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


class _FakeRipplingAdapter(RipplingAdapter):
    def __init__(self, payload_by_slug=None, html: str = "", scorer=None, html_by_url=None):
        super().__init__(session=None, scorer=scorer)
        self.payload_by_slug = payload_by_slug or {}
        self.html = html
        self.html_by_url = html_by_url or {}

    def fetch_json(self, url: str):
        slug = parse_qs(urlparse(url).query).get("company_slug", [""])[0]
        return self.payload_by_slug.get(slug, {})

    def fetch_text(self, url: str) -> str:
        return self.html_by_url.get(url, self.html)


class _StubScorer:
    min_score_to_keep = 35
    prefs = {}

    def score_job(self, job_data):
        title = str(job_data.get("title") or "").lower()
        if any(token in title for token in ("product", "architect", "manager", "analyst")):
            return {"score": 14}
        return {"score": 0}


class _LocationAwareStubScorer(_StubScorer):
    us_only = True
    allow_international_remote = False
    local_hybrid_enabled = True
    location_policy = "remote_only"

    def _is_international_remote(self, location: str) -> bool:
        return False

    def _is_remote_role(self, location: str, description: str, is_remote=None) -> bool:
        if isinstance(is_remote, bool):
            return is_remote
        return "remote" in str(location or "").lower()

    def _is_hybrid_role(self, location: str, description: str) -> bool:
        return "hybrid" in str(location or "").lower()

    def _is_onsite_role(self, location: str, description: str) -> bool:
        return "onsite" in str(location or "").lower()

    def _matches_local_area(self, location: str, description: str) -> bool:
        return "denver" in str(location or "").lower()


class _FakeWorkdayAdapter(WorkdayAdapter):
    def __init__(self, payload=None, html_by_url=None):
        super().__init__(session=None, scorer=None)
        self.payload = payload or {}
        self.html_by_url = html_by_url or {}

    def fetch_json_post(self, url: str, payload, referer=None, timeout=None):
        return self.payload

    def fetch_text(self, url: str, timeout=None) -> str:
        return self.html_by_url.get(url, "")


class _BudgetedWorkdayAdapter(WorkdayAdapter):
    def __init__(self):
        super().__init__(session=None, scorer=None)

    def _scrape_endpoint(self, company_config, host, tenant, site, endpoint, referer, seen_urls, started_at, budget_ms):
        self.last_status = "budget_exhausted"
        self.last_note = "API budget exhausted"
        return []

    def _scrape_html_fallback(self, company_config, careers_url, contexts, started_at, budget_ms):
        return [
            Job(
                id="html-fallback",
                company=company_config.get("name", "Unknown"),
                role_title_raw="Recovered via HTML",
                location="Remote",
                url="https://example.wd1.myworkdayjobs.com/en-US/Careers/job/recovered",
                source="Workday HTML",
                adapter="workday",
                tier=str(company_config.get("tier", 4)),
                description_excerpt="Recovered description",
            )
        ]


class _FakeUSAJobsAdapter(USAJobsAdapter):
    def __init__(self, payloads):
        super().__init__(session=None, scorer=None)
        self.payloads = list(payloads)

    def _headers(self):
        return {"Host": "data.usajobs.gov", "User-Agent": "tester@example.com", "Authorization-Key": "key"}

    def _query_jobs(self, company_config, keyword, page, headers):
        return self.payloads.pop(0) if self.payloads else {"SearchResult": {"SearchResultItems": []}}


class _FakeAdzunaAdapter(AdzunaAdapter):
    def __init__(self, payloads):
        super().__init__(session=None, scorer=None)
        self.payloads = list(payloads)

    def _credentials_missing(self):
        return False

    def _fetch_page(self, company_config, keyword, page):
        return self.payloads.pop(0) if self.payloads else {"results": []}


class _FakeJoobleAdapter(JoobleAdapter):
    def __init__(self, payloads):
        super().__init__(session=None, scorer=None)
        self.payloads = list(payloads)

    def fetch_json_post(self, url: str, payload, referer=None, timeout=None):
        return self.payloads.pop(0) if self.payloads else {"jobs": []}


class _FakeTheMuseAdapter(TheMuseAdapter):
    def __init__(self, payloads):
        super().__init__(session=None, scorer=None)
        self.payloads = list(payloads)

    def _fetch_page(self, company_config, page):
        return self.payloads.pop(0) if self.payloads else {"results": [], "page_count": 0}


class _FakeJobSpyModule:
    @staticmethod
    def scrape_jobs(**kwargs):
        return [
            {
                "title": "Senior Product Manager",
                "company": "SpyCo",
                "job_url": "https://jobs.spyco.com/roles/1",
                "direct_url": "https://jobs.spyco.com/roles/1",
                "location": "Remote - United States",
                "description": "Experimental discovery role",
                "salary_text": "$180,000 - $210,000",
            }
        ]


class _RecordingJobSpyModule:
    calls = []

    @staticmethod
    def scrape_jobs(**kwargs):
        _RecordingJobSpyModule.calls.append(kwargs)
        return []


def _jobspy_retry_then_success(**kwargs):
    state = getattr(_jobspy_retry_then_success, "_state", {"count": 0})
    state["count"] += 1
    _jobspy_retry_then_success._state = state
    if state["count"] < 2:
        raise RuntimeError("429 too many requests")
    return [
        {
            "title": "Senior Product Manager",
            "company": "RetryCo",
            "job_url": "https://www.indeed.com/viewjob?jk=retry-1",
            "direct_url": "https://jobs.retryco.com/roles/1",
            "location": "Remote - United States",
            "description": "Recovered after retry",
        }
    ]


def _jobspy_location_mix(**kwargs):
    return [
        {
            "title": "Senior Product Manager",
            "company": "GoodCo",
            "job_url": "https://www.indeed.com/viewjob?jk=good-1",
            "direct_url": "https://jobs.goodco.com/roles/1",
            "location": "Remote - United States",
            "description": "Remote role",
        },
        {
            "title": "Senior Product Manager",
            "company": "FarAwayCo",
            "job_url": "https://www.indeed.com/viewjob?jk=bad-1",
            "direct_url": "https://jobs.farawayco.com/roles/1",
            "location": "New York, NY, US",
            "description": "Onsite role",
        },
    ]


def _jobspy_multi_site_scrape_jobs(**kwargs):
    site_name = kwargs.get("site_name") or []
    site = site_name[0] if isinstance(site_name, list) and site_name else str(site_name)
    if site == "linkedin":
        raise RuntimeError("linkedin blocked")
    if site == "google":
        return [
            {
                "title": "Senior Product Manager",
                "company": "SpyCo",
                "job_url": "https://www.google.com/jobs/1",
                "direct_url": "https://jobs.spyco.com/roles/1",
                "location": "Remote - United States",
                "description": "Google board copy",
            }
        ]
    if site == "indeed":
        return [
            {
                "title": "Senior Product Manager",
                "company": "SpyCo",
                "job_url": "https://www.indeed.com/viewjob?jk=1",
                "direct_url": "https://jobs.spyco.com/roles/1",
                "location": "Remote - United States",
                "description": "Indeed board copy with longer description text",
            }
        ]
    return []


class ScraperAdapterRegressionTests(unittest.TestCase):
    def test_query_tier_parser_supports_prefixed_lines(self):
        parsed = normalize_search_queries("exact role\n2: broad role\n3: wide net")
        self.assertEqual(
            parsed,
            [
                {"query": "exact role", "tier": 1},
                {"query": "broad role", "tier": 2},
                {"query": "wide net", "tier": 3},
            ],
        )
        self.assertEqual(search_queries_for_tier(parsed, 2), ["exact role", "broad role"])

    def test_usajobs_adapter_parses_search_results(self):
        payload = {
            "SearchResult": {
                "SearchResultCount": 1,
                "SearchResultCountAll": 1,
                "SearchResultItems": [
                    {
                        "MatchedObjectId": "123",
                        "MatchedObjectDescriptor": {
                            "PositionTitle": "IT Product Manager",
                            "PositionURI": "https://www.usajobs.gov/GetJob/ViewDetails/123",
                            "OrganizationName": "Department of Testing",
                            "PositionLocationDisplay": "Denver, Colorado",
                            "RemoteIndicator": False,
                            "UserArea": {"Details": {"JobSummary": "Lead federal platform work."}},
                        },
                    }
                ],
            }
        }
        adapter = _FakeUSAJobsAdapter([payload])
        jobs = adapter.scrape({"name": "USAJobs API", "search_queries": ["product manager"], "max_results_per_query": 10, "tier": 4})
        self.assertEqual(len(jobs), 1)
        self.assertEqual(jobs[0].source_lane, "aggregator")
        self.assertIn("usajobs.gov", jobs[0].canonical_job_url)

    def test_adzuna_adapter_parses_results(self):
        payload = {
            "results": [
                {
                    "id": "adz-1",
                    "title": "Senior Product Manager",
                    "redirect_url": "https://www.adzuna.com/redirect/1",
                    "company": {"display_name": "AdzunaCo"},
                    "location": {"display_name": "Remote - United States"},
                    "description": "API product role",
                    "salary_min": 180000,
                    "salary_max": 210000,
                }
            ]
        }
        adapter = _FakeAdzunaAdapter([payload])
        adapter.scorer = types.SimpleNamespace(prefs={"search": {"query_tiers": {"aggregator_max_tier": 2}}})
        jobs = adapter.scrape(
            {
                "name": "Adzuna API",
                "search_queries": [
                    {"query": "product manager", "tier": 1},
                    {"query": "broad title", "tier": 3},
                ],
                "max_results_per_query": 10,
                "tier": 4,
            }
        )
        self.assertEqual(len(jobs), 1)
        self.assertEqual(jobs[0].company, "AdzunaCo")
        self.assertEqual(jobs[0].source_lane, "aggregator")

    def test_jooble_adapter_parses_results(self):
        original_key = settings.jooble_api_key
        settings.jooble_api_key = "test-key"
        try:
            payload = {
                "jobs": [
                    {
                        "id": 42,
                        "title": "Solution Architect",
                        "link": "https://jooble.org/jdp/42",
                        "company": "JoobleCo",
                        "location": "Remote",
                        "snippet": "Architecture role",
                        "salary": "$190,000",
                    }
                ]
            }
            adapter = _FakeJoobleAdapter([payload])
            jobs = adapter.scrape({"name": "Jooble API", "search_queries": ["solution architect"], "max_results_per_query": 10, "tier": 4})
            self.assertEqual(len(jobs), 1)
            self.assertEqual(jobs[0].company, "JoobleCo")
            self.assertEqual(jobs[0].source_lane, "aggregator")
        finally:
            settings.jooble_api_key = original_key

    def test_indeed_connector_imports_jobs_from_cache(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            cache_path = Path(tmpdir) / "indeed.json"
            cache_path.write_text(
                json.dumps(
                    {
                        "jobs": [
                            {
                                "company_name": "IndeedCo",
                                "title": "Senior Product Manager",
                                "url": "https://www.indeed.com/viewjob?jk=123",
                                "employer_job_url": "https://jobs.indeedco.com/roles/123",
                                "location": "Remote - United States",
                                "description": "API platform role",
                                "salary_text": "$180,000 - $210,000",
                            }
                        ]
                    }
                ),
                encoding="utf-8",
            )
            adapter = IndeedConnectorAdapter(session=None, scorer=None)
            jobs = adapter.scrape({"name": "Indeed API Pilot", "cache_file": str(cache_path), "tier": 4})
            self.assertEqual(len(jobs), 1)
            self.assertEqual(jobs[0].company, "IndeedCo")
            self.assertEqual(jobs[0].source_lane, "aggregator")
            self.assertEqual(jobs[0].canonical_job_url, "https://jobs.indeedco.com/roles/123")

    def test_themuse_adapter_parses_results(self):
        payload = {
            "page": 1,
            "page_count": 1,
            "results": [
                {
                    "id": 77,
                    "name": "Senior Product Manager",
                    "company": {"name": "MuseCo"},
                    "refs": {"landing_page": "https://www.themuse.com/jobs/museco/spm"},
                    "locations": [{"name": "Remote"}],
                    "contents": "Fintech product role",
                }
            ],
        }
        adapter = _FakeTheMuseAdapter([payload])
        jobs = adapter.scrape({"name": "The Muse API", "search_queries": ["product manager"], "max_results_per_query": 10, "tier": 4})
        self.assertEqual(len(jobs), 1)
        self.assertEqual(jobs[0].company, "MuseCo")
        self.assertEqual(jobs[0].source_lane, "aggregator")

    def test_jobspy_adapter_handles_missing_dependency(self):
        with mock.patch("jobsearch.scraper.adapters.jobspy_experimental.importlib.import_module", side_effect=ImportError("missing")):
            adapter = JobSpyExperimentalAdapter(session=None, scorer=None)
            jobs = adapter.scrape({"name": "JobSpy Experimental", "search_queries": ["product manager"], "results_wanted": 5})
            self.assertEqual(jobs, [])
            self.assertEqual(adapter.last_status, "empty")
            self.assertIn("jobspy not installed", adapter.last_note)

    def test_jobspy_adapter_imports_records_when_module_available(self):
        original = sys.modules.get("jobspy")
        sys.modules["jobspy"] = types.SimpleNamespace(scrape_jobs=_FakeJobSpyModule.scrape_jobs)
        try:
            adapter = JobSpyExperimentalAdapter(session=None, scorer=None)
            jobs = adapter.scrape({"name": "JobSpy Experimental", "search_queries": ["product manager"], "results_wanted": 5, "site_names": "google"})
            self.assertEqual(len(jobs), 1)
            self.assertEqual(jobs[0].company, "SpyCo")
            self.assertEqual(jobs[0].source_lane, "jobspy_experimental")
            self.assertEqual(jobs[0].canonical_job_url, "https://jobs.spyco.com/roles/1")
        finally:
            if original is None:
                sys.modules.pop("jobspy", None)
            else:
                sys.modules["jobspy"] = original

    def test_jobspy_adapter_validates_site_names(self):
        original = sys.modules.get("jobspy")
        sys.modules["jobspy"] = types.SimpleNamespace(scrape_jobs=_FakeJobSpyModule.scrape_jobs)
        try:
            adapter = JobSpyExperimentalAdapter(session=None, scorer=None)
            jobs = adapter.scrape({"name": "JobSpy Experimental", "search_queries": ["product manager"], "site_names": "google,badsite"})
            self.assertEqual(jobs, [])
            self.assertEqual(adapter.last_status, "empty")
            self.assertIn("Unsupported JobSpy sites", adapter.last_note)
        finally:
            if original is None:
                sys.modules.pop("jobspy", None)
            else:
                sys.modules["jobspy"] = original

    def test_jobspy_company_site_names_override_runtime_defaults(self):
        original = sys.modules.get("jobspy")
        sys.modules["jobspy"] = types.SimpleNamespace(scrape_jobs=_FakeJobSpyModule.scrape_jobs)
        try:
            scorer = types.SimpleNamespace(prefs={"jobspy_experimental": {"enabled_sites": ["indeed"]}})
            adapter = JobSpyExperimentalAdapter(session=None, scorer=scorer)
            jobs = adapter.scrape(
                {
                    "name": "JobSpy Google Experimental",
                    "search_queries": ["product manager"],
                    "site_names": "google",
                }
            )
            self.assertEqual(len(jobs), 1)
            self.assertIn('"source_site": "google"', jobs[0].notes)
        finally:
            if original is None:
                sys.modules.pop("jobspy", None)
            else:
                sys.modules["jobspy"] = original

    def test_jobspy_respects_max_query_tier(self):
        original = sys.modules.get("jobspy")
        calls = []

        class _RecordingModule:
            @staticmethod
            def scrape_jobs(**kwargs):
                calls.append(kwargs.get("search_term"))
                return []

        sys.modules["jobspy"] = types.SimpleNamespace(scrape_jobs=_RecordingModule.scrape_jobs)
        try:
            scorer = types.SimpleNamespace(prefs={"search": {"query_tiers": {"jobspy_max_tier": 2}}})
            adapter = JobSpyExperimentalAdapter(session=None, scorer=scorer)
            adapter.scrape(
                {
                    "name": "JobSpy Indeed Experimental",
                    "search_queries": [
                        {"query": "exact fintech", "tier": 1},
                        {"query": "broad finance", "tier": 2},
                        {"query": "wide product", "tier": 3},
                    ],
                    "site_names": "indeed",
                    "results_wanted": 1,
                    "max_total_results": 1,
                }
            )
            self.assertEqual(calls, ["exact fintech", "broad finance"])
        finally:
            if original is None:
                sys.modules.pop("jobspy", None)
            else:
                sys.modules["jobspy"] = original

    def test_jobspy_adapter_clusters_same_job_across_sites_and_continues_on_failure(self):
        original = sys.modules.get("jobspy")
        sys.modules["jobspy"] = types.SimpleNamespace(scrape_jobs=_jobspy_multi_site_scrape_jobs)
        try:
            scorer = types.SimpleNamespace(
                prefs={
                    "jobspy_experimental": {
                        "enabled_sites": ["google", "linkedin", "indeed"],
                        "continue_on_site_failure": True,
                        "max_total_results": 10,
                    }
                }
            )
            adapter = JobSpyExperimentalAdapter(session=None, scorer=scorer)
            jobs = adapter.scrape({"name": "JobSpy Experimental", "search_queries": ["product manager"]})
            self.assertEqual(len(jobs), 1)
            self.assertEqual(jobs[0].canonical_job_url, "https://jobs.spyco.com/roles/1")
            self.assertIn('"source_site_count": 2', jobs[0].notes)
            self.assertIn("linkedin:", adapter.last_note)
            self.assertIn("failed=1", adapter.last_note)
            self.assertIn("google:", adapter.last_note)
            self.assertIn("indeed:", adapter.last_note)
        finally:
            if original is None:
                sys.modules.pop("jobspy", None)
            else:
                sys.modules["jobspy"] = original

    def test_jobspy_glassdoor_uses_city_only_location(self):
        original = sys.modules.get("jobspy")
        _RecordingJobSpyModule.calls = []
        sys.modules["jobspy"] = types.SimpleNamespace(scrape_jobs=_RecordingJobSpyModule.scrape_jobs)
        try:
            adapter = JobSpyExperimentalAdapter(session=None, scorer=_StubScorer())
            adapter.scrape(
                {
                    "name": "JobSpy Glassdoor Experimental",
                    "search_queries": ["product manager"],
                    "site_names": "glassdoor",
                    "location_filter": "San Francisco, CA",
                }
            )
            self.assertEqual(len(_RecordingJobSpyModule.calls), 1)
            self.assertEqual(_RecordingJobSpyModule.calls[0]["location"], "San Francisco")
        finally:
            if original is None:
                sys.modules.pop("jobspy", None)
            else:
                sys.modules["jobspy"] = original

    def test_jobspy_retries_transient_failures(self):
        original = sys.modules.get("jobspy")
        _jobspy_retry_then_success._state = {"count": 0}
        sys.modules["jobspy"] = types.SimpleNamespace(scrape_jobs=_jobspy_retry_then_success)
        try:
            adapter = JobSpyExperimentalAdapter(session=None, scorer=_StubScorer())
            jobs = adapter.scrape({"name": "JobSpy Indeed Experimental", "search_queries": ["product manager"], "site_names": "indeed"})
            self.assertEqual(len(jobs), 1)
            self.assertEqual(_jobspy_retry_then_success._state["count"], 2)
        finally:
            if original is None:
                sys.modules.pop("jobspy", None)
            else:
                sys.modules["jobspy"] = original

    def test_jobspy_prefilters_wrong_location_before_scoring(self):
        original = sys.modules.get("jobspy")
        sys.modules["jobspy"] = types.SimpleNamespace(scrape_jobs=_jobspy_location_mix)
        try:
            adapter = JobSpyExperimentalAdapter(session=None, scorer=_LocationAwareStubScorer())
            jobs = adapter.scrape({"name": "JobSpy Indeed Experimental", "search_queries": ["product manager"], "site_names": "indeed"})
            self.assertEqual(len(jobs), 1)
            self.assertEqual(jobs[0].company, "GoodCo")
        finally:
            if original is None:
                sys.modules.pop("jobspy", None)
            else:
                sys.modules["jobspy"] = original

    def test_indeed_connector_handles_missing_cache(self):
        adapter = IndeedConnectorAdapter(session=None, scorer=None)
        jobs = adapter.scrape({"name": "Indeed API Pilot", "cache_file": "aggregator_imports/missing.json"})
        self.assertEqual(jobs, [])
        self.assertEqual(adapter.last_status, "empty")

    def test_healer_signal_words_keep_short_company_names(self):
        healer = ATSHealer(session=None)
        self.assertEqual(healer._signal_words("ADP"), ["adp"])
        self.assertEqual(healer._signal_words("3M"), ["3m"])

    def test_engine_maps_adapter_aliases(self):
        self.assertIn("workday_manual", ScraperEngine.ADAPTER_MAP)
        self.assertIn("custom_blackrock", ScraperEngine.ADAPTER_MAP)
        self.assertIn("dice", ScraperEngine.ADAPTER_MAP)

    def test_engine_resolves_adapter_from_careers_url(self):
        engine = ScraperEngine(preferences={}, companies=[{"name": "ExampleCo", "active": True}])
        self.assertEqual(
            engine._resolve_adapter_name({"adapter": "custom_manual", "careers_url": "https://jobs.lever.co/example"}),
            "lever",
        )
        self.assertEqual(
            engine._resolve_adapter_name({"adapter": "", "careers_url": "https://boards.greenhouse.io/example"}),
            "greenhouse",
        )

    def test_engine_builds_adapter_semaphores_from_settings(self):
        engine = ScraperEngine(preferences={}, companies=[{"name": "ExampleCo", "active": True}])
        self.assertIn("workday", engine._adapter_semaphores)
        self.assertIn("generic", engine._adapter_semaphores)
        self.assertIn("deep_search", engine._adapter_semaphores)

    def test_engine_skips_manual_only_companies(self):
        engine = ScraperEngine(
            preferences={},
            companies=[
                {"name": "ManualOnlyCo", "active": True, "manual_only": True},
                {"name": "ActiveCo", "active": True},
            ],
        )
        self.assertEqual([company["name"] for company in engine.companies], ["ActiveCo"])

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

    def test_healer_workday_key_normalization_is_case_insensitive(self):
        parsed_url = "https://example.wd1.myworkdayjobs.com/EN-us/Careers"
        clean_path = re.sub(r"/[a-z]{2}-[a-z]{2}/?", "/", urlparse(parsed_url).path, flags=re.I).rstrip("/")
        self.assertEqual(clean_path, "/Careers")

    def test_workday_extract_postings_supports_nested_payload_shapes(self):
        adapter = WorkdayAdapter(session=None, scorer=None)
        postings = adapter._extract_postings({"data": {"jobPostings": [{"title": "Architect"}]}})
        self.assertEqual(len(postings), 1)

    def test_workday_html_fallback_checks_candidate_urls(self):
        adapter = _FakeWorkdayAdapter(
            payload={},
            html_by_url={
                "https://example.wd1.myworkdayjobs.com/en-US/Careers": """
                <html><body>
                  <a href="/en-US/Careers/job/Senior-Architect_R123">Senior Architect</a>
                </body></html>
                """
            },
        )
        jobs = adapter._scrape_html_fallback(
            {"name": "ExampleCo", "tier": 2},
            "https://example.wd1.myworkdayjobs.com/External",
            [("example.wd1.myworkdayjobs.com", "example", "Careers")],
            time.perf_counter(),
            45000,
        )
        self.assertEqual(len(jobs), 1)
        self.assertIn("/job/", jobs[0].url)

    def test_workday_budget_exhaustion_sets_adapter_status(self):
        adapter = WorkdayAdapter(session=None, scorer=None)
        jobs = adapter.scrape(
            {
                "name": "Budgeted Workday",
                "careers_url": "https://example.wd1.myworkdayjobs.com/External",
                "adapter_key": "https://example.wd1.myworkdayjobs.com/Careers",
                "scrape_budget_ms": 0,
            }
        )
        self.assertEqual(jobs, [])
        self.assertEqual(adapter.last_status, "budget_exhausted")

    def test_workday_budget_exhaustion_can_still_use_html_fallback(self):
        adapter = _BudgetedWorkdayAdapter()
        jobs = adapter.scrape(
            {
                "name": "HTML Fallback Workday",
                "careers_url": "https://example.wd1.myworkdayjobs.com/External",
                "adapter_key": "https://example.wd1.myworkdayjobs.com/Careers",
                "tier": 2,
            }
        )
        self.assertEqual(len(jobs), 1)
        self.assertEqual(adapter.last_status, "ok")

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
        self.assertEqual(jobs[0].description_excerpt, "Department: Solutions")

    def test_smartrecruiters_fetches_detail_for_plausible_titles(self):
        payload = {
            "content": [
                {
                    "id": "123456",
                    "name": "Senior Product Manager",
                    "location": {"city": "Denver", "region": "CO"},
                    "department": {"label": "Solutions"},
                }
            ]
        }
        adapter = _FakeSmartRecruitersAdapter(
            payload,
            scorer=_StubScorer(),
            html_by_url={
                "https://jobs.smartrecruiters.com/Visa/123456": "<html><body><div>API platform integrations and roadmap ownership</div></body></html>"
            },
        )
        jobs = adapter.scrape({"name": "Visa", "adapter_key": "Visa", "tier": 2})
        self.assertEqual(len(jobs), 1)
        self.assertIn("integrations", jobs[0].description_excerpt.lower())

    def test_smartrecruiters_adapter_paginates(self):
        def payload(url: str):
            query = parse_qs(urlparse(url).query)
            offset = int(query.get("offset", ["0"])[0])
            if offset == 0:
                return {"content": [{"id": f"job-{i}", "name": f"Role {i}", "location": {"city": "Denver", "region": "CO"}} for i in range(100)]}
            if offset == 100:
                return {"content": [{"id": "job-100", "name": "Role 100", "location": {"city": "Austin", "region": "TX"}}]}
            return {"content": []}

        adapter = _FakeSmartRecruitersAdapter(payload)
        jobs = adapter.scrape({"name": "Visa", "adapter_key": "Visa"})
        self.assertEqual(len(jobs), 101)

    def test_workday_html_reserve_keeps_budget_for_api_phase(self):
        budget_ms = 5000
        html_reserve_ms = min(10000, max(budget_ms // 3, 0))
        api_budget_ms = max(budget_ms - html_reserve_ms, 0)
        self.assertEqual(html_reserve_ms, 1666)
        self.assertEqual(api_budget_ms, 3334)

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

    def test_rippling_adapter_derives_slug_from_careers_url_when_key_is_invalid(self):
        adapter = _FakeRipplingAdapter(
            payload_by_slug={
                "vouch-inc": {
                    "results": [
                        {
                            "id": "abc123",
                            "title": "Senior Solutions Architect",
                            "location": "Remote",
                            "description": "Integrations and platform architecture",
                        }
                    ]
                }
            }
        )
        jobs = adapter.scrape(
            {
                "name": "Vouch Insurance",
                "adapter_key": "_next",
                "careers_url": "https://ats.rippling.com/vouch-inc/jobs",
                "tier": 2,
            }
        )
        self.assertEqual(len(jobs), 1)
        self.assertEqual(jobs[0].adapter, "rippling")
        self.assertIn("/vouch-inc/jobs/abc123", jobs[0].url)

    def test_rippling_fetches_detail_for_plausible_titles(self):
        adapter = _FakeRipplingAdapter(
            payload_by_slug={
                "joinroot": {
                    "results": [
                        {
                            "id": "abc123",
                            "title": "Product Manager, Partnerships",
                            "location": "",
                            "description": "",
                        }
                    ]
                }
            },
            scorer=_StubScorer(),
            html_by_url={
                "https://ats.rippling.com/joinroot/jobs/abc123": "<html><body><section>Remote role focused on integrations and platform partnerships.</section></body></html>"
            },
        )
        jobs = adapter.scrape(
            {
                "name": "Root Insurance",
                "adapter_key": "joinroot",
                "careers_url": "https://ats.rippling.com/joinroot/jobs",
                "tier": 2,
            }
        )
        self.assertEqual(len(jobs), 1)
        self.assertIn("integrations", jobs[0].description_excerpt.lower())

    def test_rippling_adapter_falls_back_to_html_job_links(self):
        adapter = _FakeRipplingAdapter(
            payload_by_slug={},
            html="""
            <html><body>
              <a href="/tilledcareers/jobs/123">Senior Technical Product Manager</a>
              <a href="/tilledcareers/about">About</a>
            </body></html>
            """,
        )
        jobs = adapter.scrape(
            {
                "name": "Tilled",
                "adapter_key": "_next",
                "careers_url": "https://ats.rippling.com/tilledcareers/jobs",
                "tier": 2,
            }
        )
        self.assertEqual(len(jobs), 1)
        self.assertEqual(jobs[0].adapter, "rippling")
        self.assertIn("/tilledcareers/jobs/123", jobs[0].url)

    def test_healer_rejects_generic_marketing_page(self):
        healer = ATSHealer()
        html = """
        <html><head><title>Platform Overview</title></head>
        <body>
          <h1>Our Solutions</h1>
          <p>Request a demo and contact sales to learn more.</p>
        </body></html>
        """
        self.assertFalse(
            healer._is_probable_careers_page(html, "https://example.com/platform", "ExampleCo")
        )

    def test_healer_accepts_generic_careers_page_with_openings_signal(self):
        healer = ATSHealer()
        html = """
        <html><head><title>Careers at ExampleCo</title></head>
        <body>
          <h1>Current openings</h1>
          <p>Join our team at ExampleCo and search jobs below.</p>
        </body></html>
        """
        self.assertTrue(
            healer._is_probable_careers_page(html, "https://example.com/careers", "ExampleCo")
        )


if __name__ == "__main__":
    unittest.main()
