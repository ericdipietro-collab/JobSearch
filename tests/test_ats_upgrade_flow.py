import sys
import unittest
from pathlib import Path


BASE_DIR = Path(__file__).resolve().parents[1]
SRC_DIR = BASE_DIR / "src"
if str(BASE_DIR) not in sys.path:
    sys.path.insert(0, str(BASE_DIR))
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))


from deep_search.playwright_adapter import _parse_jobs_from_api_response
from jobsearch.scraper.ats_routing import (
    CandidateJobURL,
    choose_extraction_route,
    fingerprint_ats,
    rank_candidates,
    score_candidate_url,
)
from jobsearch.scraper.engine import ScraperEngine
from jobsearch.scraper.jsonld_extractor import extract_jobposting_objects, jsonld_jobs_to_canonical


class ATSUpgradeFlowTests(unittest.TestCase):
    def test_fingerprint_ats_detects_known_families(self):
        self.assertEqual(fingerprint_ats("https://boards.greenhouse.io/example"), "greenhouse")
        self.assertEqual(fingerprint_ats("https://example.com/jobs", html='<script src="https://jobs.ashbyhq.com/widget.js"></script>'), "ashby")
        self.assertEqual(fingerprint_ats("https://company.example", response_urls=["https://api.smartrecruiters.com/v1/companies/acme/postings"]), "smartrecruiters")

    def test_candidate_ranking_prefers_supported_careers_url(self):
        weak = CandidateJobURL(
            source="search",
            url="https://example.com/about",
            final_url="https://example.com/about",
            status_code=200,
            ats_family="unknown",
            confidence_score=score_candidate_url("https://example.com/about", source="search", company_domain="example.com"),
        )
        strong = CandidateJobURL(
            source="existing_url",
            url="https://boards.greenhouse.io/example",
            final_url="https://boards.greenhouse.io/example",
            status_code=200,
            ats_family="greenhouse",
            confidence_score=score_candidate_url(
                "https://boards.greenhouse.io/example",
                source="existing_url",
                ats_family="greenhouse",
                status_code=200,
                company_domain="example.com",
                reason_flags=["ats_host", "careersish_path"],
            ),
        )
        ranked = rank_candidates([weak, strong], limit=2)
        self.assertEqual(ranked[0].final_url, strong.final_url)

    def test_choose_extraction_route_prefers_direct_api_for_supported_ats(self):
        route = choose_extraction_route(ats_family="greenhouse")
        self.assertEqual(route.decision, "direct_api")
        self.assertIn("direct_api", route.extraction_methods)

        route = choose_extraction_route(ats_family="phenom", has_hidden_api=True)
        self.assertEqual(route.decision, "intercepted_api")

    def test_jsonld_extractor_normalizes_jobposting(self):
        html = """
        <html><body>
          <script type="application/ld+json">
          {
            "@context": "https://schema.org",
            "@graph": [
              {
                "@type": "JobPosting",
                "title": "Senior Product Manager",
                "description": "<p>Lead platform roadmap</p>",
                "url": "/jobs/pm-1",
                "jobLocation": {
                  "address": {
                    "addressLocality": "Denver",
                    "addressRegion": "CO",
                    "addressCountry": "US"
                  }
                },
                "baseSalary": {
                  "@type": "MonetaryAmount",
                  "currency": "USD",
                  "value": {
                    "@type": "QuantitativeValue",
                    "minValue": 180000,
                    "maxValue": 210000,
                    "unitText": "YEAR"
                  }
                }
              }
            ]
          }
          </script>
        </body></html>
        """
        objs = extract_jobposting_objects(html)
        self.assertEqual(len(objs), 1)
        jobs = jsonld_jobs_to_canonical(
            html,
            base_url="https://example.com/careers",
            company_name="ExampleCo",
            adapter="generic",
            tier=2,
        )
        self.assertEqual(len(jobs), 1)
        self.assertEqual(jobs[0].role_title_raw, "Senior Product Manager")
        self.assertEqual(jobs[0].location, "Denver, CO, US")
        self.assertEqual(jobs[0].salary_min, 180000)
        self.assertTrue(jobs[0].url.startswith("https://example.com/jobs/pm-1"))

    def test_hidden_api_detection_extracts_jobs_from_mocked_payload(self):
        payload = """
        {
          "jobs": [
            {
              "title": "Solutions Architect",
              "location": "Remote",
              "url": "/jobs/sa-1",
              "description": "Platform integrations"
            }
          ]
        }
        """
        jobs = _parse_jobs_from_api_response(
            "https://careers.example.com/api/jobs",
            "application/json",
            payload,
            base_url="https://careers.example.com",
            source_label="Deep Search",
        )
        self.assertEqual(len(jobs), 1)
        self.assertEqual(jobs[0]["title"], "Solutions Architect")
        self.assertEqual(jobs[0]["url"], "https://careers.example.com/jobs/sa-1")

    def test_failure_classifier_covers_geo_gate_and_unknown(self):
        self.assertEqual(
            ScraperEngine._classify_empty_result("blocked", "Cookie consent required for this region", {"careers_url": "https://example.com/jobs"}),
            "geo_or_cookie_gate",
        )
        self.assertEqual(
            ScraperEngine._classify_empty_result("empty", "No parsable signal", {"careers_url": "https://example.com/jobs"}),
            "unknown",
        )


if __name__ == "__main__":
    unittest.main()
