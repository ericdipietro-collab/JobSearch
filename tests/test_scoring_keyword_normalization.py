import unittest

from jobsearch.scraper.scoring import Scorer


class ScoringKeywordNormalizationTests(unittest.TestCase):
    def test_keyword_matching_handles_separators_and_html_entities(self):
        prefs = {
            "titles": {"positive_weights": {}, "negative_disqualifiers": []},
            "keywords": {
                "body_positive": {
                    "cross functional": 5,
                    "data platform api": 4,
                    "customer discovery": 5,
                },
                "body_negative": {},
            },
            "scoring": {"keyword_matching": {"positive_keyword_cap": 99, "negative_keyword_cap": 99}},
            "search": {"location_preferences": {"remote_us": {"enabled": False}, "local_hybrid": {"enabled": False}}},
        }
        scorer = Scorer(prefs)
        res = scorer.score_job(
            {
                "title": "Role",
                "description": "We do customer&nbsp;discovery weekly. Cross-functional execution. data_platform/api ownership.",
                "location": "",
            }
        )
        # 5 + 4 + 5 = 14
        self.assertEqual(res["score_components"]["body_positive_points"], 14)

    def test_pluralization_matches_multi_word_terms(self):
        prefs = {
            "titles": {"positive_weights": {}, "negative_disqualifiers": []},
            "keywords": {"body_positive": {"data platform": 5}, "body_negative": {}},
            "scoring": {"keyword_matching": {"positive_keyword_cap": 99, "negative_keyword_cap": 99}},
            "search": {"location_preferences": {"remote_us": {"enabled": False}, "local_hybrid": {"enabled": False}}},
        }
        scorer = Scorer(prefs)
        res = scorer.score_job({"title": "Role", "description": "Experience building data platforms at scale.", "location": ""})
        self.assertEqual(res["score_components"]["body_positive_points"], 5)

    def test_title_constraint_penalty_deprioritizes_generic_titles(self):
        prefs = {
            "titles": {
                "positive_weights": {},
                "positive_keywords": [],
                "negative_disqualifiers": [],
                "constraints": {
                    "product_manager_requires_modifier": True,
                    "product_manager_allowed_modifiers": ["technical", "platform"],
                },
            },
            "keywords": {"body_positive": {"sql": 10}, "body_negative": {}},
            "scoring": {"keyword_matching": {"positive_keyword_cap": 99, "negative_keyword_cap": 99}},
            "search": {"location_preferences": {"remote_us": {"enabled": False}, "local_hybrid": {"enabled": False}}},
        }
        scorer = Scorer(prefs)
        res = scorer.score_job({"title": "Product Manager", "description": "SQL required.", "location": ""})

        # Constraint penalty should be applied and prevent APPLY NOW eligibility
        self.assertGreater(res["score_components"]["title_constraint_penalty"], 0)
        self.assertFalse(res["apply_now_eligible"])
        self.assertIn("title_constraint:product_manager_missing_modifier", res["penalized_keywords"])

        # A modified title should not be penalized
        res_ok = scorer.score_job({"title": "Technical Product Manager", "description": "SQL required.", "location": ""})
        self.assertEqual(res_ok["score_components"]["title_constraint_penalty"], 0)


if __name__ == "__main__":
    unittest.main()
