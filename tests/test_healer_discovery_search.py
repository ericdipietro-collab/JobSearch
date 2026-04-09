import sys
import time
import unittest
from pathlib import Path
from urllib.parse import urlparse


BASE_DIR = Path(__file__).resolve().parents[1]
SRC_DIR = BASE_DIR / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))


from jobsearch.services.healer_service import ATSHealer


class _Resp:
    def __init__(self, text="", status_code=200, url="https://example.com"):
        self.text = text
        self.status_code = status_code
        self.url = url


class _FakeSession:
    def __init__(self, responder):
        self._responder = responder
        self.seen_urls = []

    def get(self, url, headers=None, timeout=None, allow_redirects=True):
        self.seen_urls.append(url)
        return self._responder(url)


class HealerDiscoverySearchTests(unittest.TestCase):
    def test_discover_bypasses_persistent_health_skip_when_ignore_cooldown(self):
        def responder(url: str):
            return _Resp(text="<html><body></body></html>", status_code=404, url=url)

        healer = ATSHealer(session=_FakeSession(responder))
        healer._persistent_health_skip_reason = lambda company: "cooldown until 2099-01-01T00:00:00Z"

        # Without ignore_cooldown, we'd immediately "VALID" out.
        res1 = healer.discover({"name": "ExampleCo", "domain": "example.com", "status": "broken"}, force=False, deep=False)
        self.assertEqual(res1.status, "VALID")

        # With ignore_cooldown, discovery continues (may end NOT_FOUND in this test harness).
        res2 = healer.discover({"name": "ExampleCo", "domain": "example.com", "status": "broken"}, force=False, deep=False, ignore_cooldown=True)
        self.assertNotEqual(res2.status, "VALID")

    def test_discover_does_not_call_waterfall_when_disabled(self):
        def responder(url: str):
            # Make sure nothing tries to call out to the network in this test.
            return _Resp(text="<html><body></body></html>", status_code=404, url=url)

        healer = ATSHealer(session=_FakeSession(responder))
        healer._persistent_health_skip_reason = lambda company: None

        called = {"waterfall": 0}

        def boom(domain: str, name: str, deadline=None):
            called["waterfall"] += 1
            raise AssertionError("waterfall should not be called when disable_waterfall=True")

        healer._waterfall_discovery = boom
        # Also stub search to keep the test deterministic and fast.
        healer._search_discovery = lambda name, domain, deadline=None: None

        res = healer.discover(
            {"name": "ExampleCo", "domain": "example.com", "status": "broken"},
            force=False,
            deep=False,
            ignore_cooldown=True,
            disable_waterfall=True,
        )
        self.assertEqual(called["waterfall"], 0)
        self.assertIn(res.status, {"NOT_FOUND", "FALLBACK", "FOUND", "BLOCKED", "VALID"})

    def test_discover_does_not_call_waterfall_when_domain_missing(self):
        def responder(url: str):
            return _Resp(text="<html><body></body></html>", status_code=404, url=url)

        healer = ATSHealer(session=_FakeSession(responder))
        healer._persistent_health_skip_reason = lambda company: None

        called = {"waterfall": 0}

        def boom(domain: str, name: str, deadline=None):
            called["waterfall"] += 1
            raise AssertionError("waterfall should not be called when domain is empty")

        healer._waterfall_discovery = boom
        healer._search_discovery = lambda name, domain, deadline=None: None

        res = healer.discover(
            {"name": "ExampleCo", "domain": "", "status": "broken"},
            force=False,
            deep=False,
            ignore_cooldown=True,
            disable_waterfall=False,
        )
        self.assertEqual(called["waterfall"], 0)
        self.assertIn(res.status, {"NOT_FOUND", "FALLBACK", "FOUND", "BLOCKED", "VALID"})

    def test_ddg_keeps_careersish_links_when_domain_unknown(self):
        ddg_html = """
        <html><body>
          <a href="/l/?uddg=https%3A%2F%2Fcareers.example.org%2Fcareers">Careers</a>
        </body></html>
        """

        def responder(url: str):
            if "duckduckgo.com/html" in url:
                return _Resp(text=ddg_html, status_code=200, url=url)
            return _Resp(text="", status_code=404, url=url)

        healer = ATSHealer(session=_FakeSession(responder))
        deadline = time.perf_counter() + 5.0
        links = healer._search_links_duckduckgo("ExampleCo careers", "", deadline)
        self.assertIn("https://careers.example.org/careers", links)

    def test_discover_scans_existing_careers_url_for_embedded_ats_before_slug_probes(self):
        careers_url = "https://www.example.com/careers"
        board_url = "https://boards.greenhouse.io/exampleco"
        html = """
        <html><body>
          <script src="https://boards.greenhouse.io/embed/job_board/js?for=exampleco"></script>
        </body></html>
        """
        board_html = "<html><head><title>ExampleCo Jobs</title></head><body>ExampleCo Open Positions</body></html>"

        def responder(url: str):
            if url == careers_url:
                return _Resp(text=html, status_code=200, url=url)
            if url == board_url:
                return _Resp(text=board_html, status_code=200, url=url)
            return _Resp(text="", status_code=404, url=url)

        healer = ATSHealer(session=_FakeSession(responder))
        healer._persistent_health_skip_reason = lambda company: None

        # Make sure we fail the later phases if we ever reached them.
        healer._search_discovery = lambda name, domain, deadline=None: None
        healer._probe_direct = lambda *args, **kwargs: (_ for _ in ()).throw(AssertionError("probe_direct should not be called"))
        healer._probe_workday = lambda *args, **kwargs: (_ for _ in ()).throw(AssertionError("probe_workday should not be called"))

        deadline = time.perf_counter() + 5.0
        res = healer.discover(
            {"name": "ExampleCo", "domain": "www.example.com", "careers_url": careers_url, "status": "broken"},
            force=False,
            deep=False,
            ignore_cooldown=True,
            disable_waterfall=True,
        )
        self.assertEqual(res.status, "FOUND")
        self.assertEqual(res.adapter, "greenhouse")
        self.assertEqual(res.adapter_key, "exampleco")

    def test_blocked_existing_careers_url_does_not_prevent_direct_probes(self):
        # Simulate a company site that returns 403, but whose ATS board is still discoverable.
        careers_url = "https://example.com/careers"

        def responder(url: str):
            if url == careers_url:
                return _Resp(text="<html><body>blocked</body></html>", status_code=403, url=url)
            return _Resp(text="", status_code=404, url=url)

        healer = ATSHealer(session=_FakeSession(responder))
        healer._persistent_health_skip_reason = lambda company: None

        # Make search return nothing so the direct probe path is exercised.
        healer._search_discovery = lambda name, domain, deadline=None: None
        healer._probe_workday = lambda *args, **kwargs: None

        # Force the direct probe to succeed.
        healer._probe_direct = lambda *args, **kwargs: type("R", (), {"adapter": "ashby", "adapter_key": "openai", "careers_url": "https://jobs.ashbyhq.com/openai", "status": "FOUND", "detail": "Direct ashby probe"})()

        res = healer.discover(
            {"name": "OpenAI", "domain": "openai.com", "careers_url": careers_url, "status": "broken"},
            force=False,
            deep=False,
            ignore_cooldown=True,
            disable_waterfall=True,
        )
        self.assertEqual(res.status, "FOUND")
        self.assertEqual(res.adapter, "ashby")

    def test_ddg_link_extraction_decodes_uddg_and_keeps_ats_host(self):
        ddg_html = """
        <html><body>
          <a href="/l/?uddg=https%3A%2F%2Fboards.greenhouse.io%2Fexampleco%2Fjobs%2F123">Job</a>
        </body></html>
        """

        def responder(url: str):
            if "html.duckduckgo.com/html" in url:
                return _Resp(text=ddg_html, status_code=200, url=url)
            # Any non-search URL should not be hit for this test.
            return _Resp(text="", status_code=404, url=url)

        healer = ATSHealer(session=_FakeSession(responder))
        deadline = time.perf_counter() + 5.0
        links = healer._search_links_duckduckgo("ExampleCo careers jobs", "example.com", deadline)
        self.assertIn("https://boards.greenhouse.io/exampleco/jobs/123", links)

    def test_domain_slug_candidates_strip_www_and_common_noise(self):
        healer = ATSHealer()
        self.assertEqual(healer._domain_slug_candidates("www.baseten.co"), ["baseten"])
        self.assertEqual(healer._domain_slug_candidates("careers.baseten.co"), ["baseten"])
        self.assertEqual(healer._domain_slug_candidates("jobs.baseten.co"), ["baseten"])
        self.assertEqual(healer._domain_slug_candidates("www.wandb.ai"), ["wandb"])

    def test_ddg_sites_param_is_encoded_for_restricted_sites(self):
        ddg_html = "<html><body></body></html>"

        def responder(url: str):
            if "html.duckduckgo.com/html" in url:
                return _Resp(text=ddg_html, status_code=200, url=url)
            return _Resp(text="", status_code=404, url=url)

        session = _FakeSession(responder)
        healer = ATSHealer(session=session)
        deadline = time.perf_counter() + 5.0
        healer._search_links_duckduckgo(
            "ExampleCo careers",
            "example.com",
            deadline,
            restricted_sites=["boards.greenhouse.io", "jobs.lever.co"],
        )
        self.assertTrue(any("&sites=" in u for u in session.seen_urls))
        sites_url = next(u for u in session.seen_urls if "&sites=" in u)
        self.assertIn("sites=boards.greenhouse.io%2Cjobs.lever.co", sites_url)

    def test_search_discovery_fast_returns_supported_ats_host(self):
        ddg_html = """
        <html><body>
          <a href="/l/?uddg=https%3A%2F%2Fboards.greenhouse.io%2Fexampleco">Careers</a>
        </body></html>
        """

        def responder(url: str):
            if "html.duckduckgo.com/html" in url:
                return _Resp(text=ddg_html, status_code=200, url=url)
            return _Resp(text="", status_code=404, url=url)

        healer = ATSHealer(session=_FakeSession(responder))
        deadline = time.perf_counter() + 5.0
        res = healer._search_discovery("ExampleCo", "example.com", deadline)
        self.assertIsNotNone(res)
        self.assertEqual(res.status, "FOUND")
        self.assertEqual(res.adapter, "greenhouse")
        self.assertEqual(res.adapter_key, "exampleco")
        self.assertEqual(urlparse(res.careers_url).netloc, "boards.greenhouse.io")

    def test_search_discovery_returns_blocked_for_unsupported_ats_vendor(self):
        ddg_html = """
        <html><body>
          <a href="/l/?uddg=https%3A%2F%2Fexample.taleo.net%2Fcareersection%2Fjobs">Careers</a>
        </body></html>
        """

        def responder(url: str):
            if "html.duckduckgo.com/html" in url:
                return _Resp(text=ddg_html, status_code=200, url=url)
            return _Resp(text="", status_code=404, url=url)

        healer = ATSHealer(session=_FakeSession(responder))
        deadline = time.perf_counter() + 5.0
        res = healer._search_discovery("ExampleCo", "example.com", deadline)
        self.assertIsNotNone(res)
        self.assertEqual(res.status, "BLOCKED")
        self.assertIn("taleo.net", (res.careers_url or "").lower())

    def test_search_discovery_uses_yahoo_fallback_when_ddg_empty(self):
        yahoo_html = """
        <html><body>
          <div>
            <h3><a href="https://jobs.lever.co/exampleco">ExampleCo Jobs</a></h3>
          </div>
        </body></html>
        """

        def responder(url: str):
            if "html.duckduckgo.com/html" in url:
                return _Resp(text="<html><body></body></html>", status_code=200, url=url)
            if "search.yahoo.com/search" in url:
                return _Resp(text=yahoo_html, status_code=200, url=url)
            return _Resp(text="", status_code=404, url=url)

        healer = ATSHealer(session=_FakeSession(responder))
        deadline = time.perf_counter() + 5.0
        res = healer._search_discovery("ExampleCo", "example.com", deadline)
        self.assertIsNotNone(res)
        self.assertEqual(res.status, "FOUND")
        self.assertEqual(res.adapter, "lever")
        self.assertEqual(res.adapter_key, "exampleco")
        self.assertEqual(urlparse(res.careers_url).netloc, "jobs.lever.co")

    def test_search_circuit_breaker_trips_and_skips_provider_calls(self):
        def responder(url: str):
            # Simulate repeated provider connection drops.
            raise ConnectionError("RemoteDisconnected")

        session = _FakeSession(responder)
        healer = ATSHealer(session=session)
        healer.SEARCH_BREAKER_FAIL_THRESHOLD = 2
        healer.SEARCH_BREAKER_WINDOW_S = 60.0
        healer.SEARCH_BREAKER_OPEN_S = 600.0

        deadline = time.perf_counter() + 5.0
        healer._search_links_duckduckgo("ExampleCo careers", "example.com", deadline)
        healer._search_links_duckduckgo("ExampleCo careers", "example.com", deadline)
        before = len(session.seen_urls)

        # Now that the breaker is open, further calls should short-circuit without a network call.
        healer._search_links_duckduckgo("ExampleCo careers", "example.com", deadline)
        after = len(session.seen_urls)
        self.assertEqual(before, after)

    def test_waterfall_stops_probing_bad_base_after_ssl_error(self):
        import requests

        calls = {"jobs": 0, "other": 0}

        def responder(url: str):
            if "https://jobs.progressive.com" in url:
                calls["jobs"] += 1
                raise requests.exceptions.SSLError("hostname 'jobs.progressive.com' doesn't match")
            calls["other"] += 1
            return _Resp(text="<html><body></body></html>", status_code=404, url=url)

        healer = ATSHealer(session=_FakeSession(responder))
        healer._persistent_health_skip_reason = lambda company: None

        deadline = time.perf_counter() + 3.0
        healer._waterfall_discovery("progressive.com", "Progressive", deadline)

        # The threadpool may race a couple requests before the base is marked bad,
        # but it should not probe every path on the bad base.
        self.assertLessEqual(calls["jobs"], 4)


if __name__ == "__main__":
    unittest.main()
