import unittest
from jobsearch.scraper.scoring import Scorer

class TestComprehensiveScoring(unittest.TestCase):
    def setUp(self):
        self.prefs = {
            "titles": {
                "positive_weights": {
                    "senior": 10,
                    "product manager": 12,
                    "software engineer": 8,
                    "python": 5
                },
                "negative_disqualifiers": ["junior", "intern", "associate"],
                "fast_track_min_weight": 10,
                "fast_track_base_score": 50,
                "title_max_points": 25,
                "require_one_positive_keyword": False
            },
            "keywords": {
                "body_positive": {
                    "aws": 5,
                    "kubernetes": 5,
                    "react": 5,
                    "sql": 3,
                    "docker": 3,
                    "terraform": 3
                },
                "body_negative": {
                    "php": 10,
                    "wordpress": 10,
                    "jquery": 5
                }
            },
            "scoring": {
                "minimum_score_to_keep": 35,
                "keyword_matching": {
                    "positive_keyword_cap": 20,
                    "negative_keyword_cap": 15
                },
                "tier_bonuses": {1: 15, 2: 8, 3: 4},
                "adjustments": {
                    "missing_salary_penalty": 6,
                    "salary_at_or_above_target_bonus": 6,
                    "salary_meets_floor_bonus": 2,
                    "salary_below_target_penalty": 12,
                    "contract_role_penalty": 5,
                    "contractor_target_bonus": 4
                }
            },
            "search": {
                "compensation": {
                    "target_salary_usd": 150000,
                    "min_salary_usd": 120000,
                    "allow_missing_salary": True,
                    "location_policy": "remote_or_hybrid"
                },
                "geography": {
                    "us_only": True,
                    "allow_international_remote": False
                },
                "location_preferences": {
                    "remote_us": {"enabled": True, "bonus": 10},
                    "local_hybrid": {
                        "enabled": True, 
                        "bonus": 5, 
                        "allow_if_salary_at_least_usd": 130000,
                        "markers": ["austin", "texas"]
                    }
                },
                "experience": {
                    "years": 5,
                    "gap_tolerance": 2
                },
                "contractor": {
                    "include_contract_roles": True,
                    "allow_w2_hourly": True,
                    "allow_1099_hourly": True,
                    "overhead_1099_pct": 0.15,
                    "benefits_replacement_usd": 15000,
                    "w2_benefits_gap_usd": 5000,
                    "default_1099_weeks_per_year": 46
                }
            },
            "requirements": {
                "visa_sponsorship_required": False,
                "preferred_tech_stack": ["Python", "AWS", "Kubernetes"],
                "role_type": "individual_contributor"
            }
        }
        self.scorer = Scorer(self.prefs)

    def test_title_disqualification(self):
        job = {"title": "Junior Python Developer", "description": "test", "location": "Remote"}
        res = self.scorer.score_job(job)
        self.assertEqual(res["score"], 0.0)
        self.assertEqual(res["fit_band"], "Disqualified")

    def test_experience_hard_drop(self):
        # User has 5 years, tolerance 2. 8 years requirement should drop.
        job = {
            "title": "Senior Engineer", 
            "description": "Requires 8+ years of experience in Python", 
            "location": "Remote"
        }
        res = self.scorer.score_job(job)
        self.assertEqual(res["score"], 0.0)
        self.assertEqual(res["fit_band"], "Filtered Out")
        self.assertIn("Experience gap", res["decision_reason"])

    def test_experience_soft_penalty(self):
        # User has 5 years, tolerance 2. 6 years requirement should apply soft penalty (-5)
        job = {
            "title": "Senior Engineer", 
            "description": "Requires 6 years of experience", 
            "location": "Remote"
        }
        res = self.scorer.score_job(job)
        # Fast track (50) + Remote bonus (10) - Exp penalty (5) = 55
        self.assertEqual(res["score"], 55.0)
        self.assertIn("Exp Penalty -5", res["decision_reason"])

    def test_international_remote_drop(self):
        job = {"title": "Python Developer", "description": "test", "location": "Remote, UK"}
        res = self.scorer.score_job(job)
        self.assertEqual(res["score"], 0.0)
        self.assertEqual(res["fit_band"], "Filtered Out")

    def test_location_mismatch_onsite(self):
        # Onsite in NYC (not in markers)
        job = {"title": "Python Developer", "description": "test", "location": "New York, NY"}
        res = self.scorer.score_job(job)
        self.assertEqual(res["score"], 0.0)
        self.assertEqual(res["fit_band"], "Filtered Out")

    def test_local_hybrid_success(self):
        # Hybrid in Austin, salary meets floor
        job = {
            "title": "Python Developer", 
            "description": "hybrid role", 
            "location": "Austin, TX",
            "salary_min": 140000
        }
        res = self.scorer.score_job(job)
        # Title python(5) + Hybrid bonus(5) + Meets floor bonus(2) = 12
        # Note: python weight 5 < fast_track_min_weight 10, so no fast track
        self.assertEqual(res["score"], 12.0)

    def test_local_hybrid_salary_too_low(self):
        # Hybrid in Austin, salary below floor (130k)
        job = {
            "title": "Python Developer", 
            "description": "hybrid role", 
            "location": "Austin, TX",
            "salary_min": 125000
        }
        res = self.scorer.score_job(job)
        self.assertEqual(res["score"], 0.0)
        self.assertEqual(res["fit_band"], "Filtered Out")

    def test_fast_track_and_caps(self):
        job = {
            "title": "Senior Product Manager",
            "description": "Experience with AWS, Kubernetes, React, SQL, Docker, Terraform. No PHP please.",
            "location": "Remote"
        }
        res = self.scorer.score_job(job)
        # Title: senior(10) + product manager(12) = 22. 
        # Fast track triggered (min 10) -> base_score 50.
        # title_points = min(22, 25) = 22.
        # score = max(50, 22) = 50.
        # Body keywords: aws(5), k8s(5), react(5), sql(3), docker(3), terraform(3) = 24.
        # Body positive capped at 20.
        # Body negative: php(10) -> -10.
        # Remote bonus: 10.
        # Total: 50 + 20 - 10 + 10 = 70.
        self.assertEqual(res["score"], 70.0)
        self.assertEqual(res["fit_band"], "Good Match")

    def test_tier_bonuses(self):
        job = {
            "title": "Software Engineer Python",
            "description": "Developer role",
            "location": "Remote",
            "tier": 1
        }
        res = self.scorer.score_job(job)
        # Title: software engineer(8), python(5) = 13.
        # No fast track (none >= 10).
        # Body positive: 0.
        # Remote bonus: 10.
        # Tier 1 bonus: 15.
        # Total: 13 + 10 + 15 = 38.
        self.assertEqual(res["score"], 38.0)

    def test_salary_extraction_edge_cases(self):
        # Case: Salary range without explicit "annual" keywords but has "$" and "k"
        # Now it SHOULD extract.
        job = {
            "title": "Engineer",
            "description": "Pay: $140k - $160k",
            "location": "Remote"
        }
        res = self.scorer.score_job(job)
        self.assertEqual(res["normalized_compensation_usd"], 150000.0)

    def test_experience_extraction_edge_cases(self):
        # "at least 5 years"
        job = {"title": "Eng", "description": "at least 5 years of experience", "location": "Remote"}
        years = self.scorer._extract_required_years_experience(job["title"], job["description"])
        self.assertEqual(years, 5.0)

        # "5-7 years" 
        job["description"] = "5-7 years of experience"
        years = self.scorer._extract_required_years_experience(job["title"], job["description"])
        self.assertEqual(years, 7.0)

        # "5+ years"
        job["description"] = "5+ years"
        years = self.scorer._extract_required_years_experience(job["title"], job["description"])
        self.assertEqual(years, 5.0)

    def test_compensation_adjustments(self):
        # Salary above target (150k) -> +6
        job = {
            "title": "Senior Product Manager", 
            "location": "Remote", 
            "salary_min": 160000
        }
        res = self.scorer.score_job(job)
        # Fast track (50) + Remote (10) + Salary bonus (6) = 66
        self.assertEqual(res["score"], 66.0)

        # Salary below target but above floor (120k) -> +2
        job["salary_min"] = 130000
        res = self.scorer.score_job(job)
        # 50 + 10 + 2 = 62
        self.assertEqual(res["score"], 62.0)

        # Salary below floor -> -12
        job["salary_min"] = 110000
        res = self.scorer.score_job(job)
        # 50 + 10 - 12 = 48
        self.assertEqual(res["score"], 48.0)

    def test_c2c_contract_scoring(self):
        # C2C contract
        job = {
            "title": "Senior Python Developer",
            "description": "C2C corp-to-corp opportunity",
            "location": "Remote",
            "salary_text": "$100 per hour"
        }
        res = self.scorer.score_job(job)
        # Work type: c2c_contract
        # Hourly: 100. Gross: 100 * 40 * 46 (now uses 1099/c2c weeks) = 184,000.
        # Normalized: 184,000 * (1 - 0.15) - 15,000 = 156,400 - 15,000 = 141,400.
        self.assertEqual(res["work_type"], "c2c_contract")
        self.assertEqual(res["normalized_compensation_usd"], 141400.0)

    def test_enrichment_adjustments(self):
        job = {"title": "Senior Python Developer", "description": "test", "location": "Remote"}
        res = self.scorer.score_job(job)
        # Base: 50 (fast track) + 10 (remote) = 60.

        # Enrichment: tech stack match
        enriched = {
            "enrichment_status": "success",
            "tech_stack": ["Python", "AWS", "SQL"],
            "visa_sponsor": None,
            "ic_vs_manager": "individual_contributor"
        }
        res_enriched = self.scorer.apply_enrichment_adjustments(res, enriched)
        # Tech match: Python, AWS match -> 2 matches * 3 = 6.
        # IC match: +6.
        # Total enrichment: +12.
        # Final: 60 + 12 = 72.
        self.assertEqual(res_enriched["score"], 72.0)
        self.assertIn("tech_stack_match(2)", res_enriched["decision_reason"])
        self.assertIn("ic_preference_match", res_enriched["decision_reason"])

    def test_visa_sponsorship_penalty(self):
        # Preference: visa required
        self.prefs["requirements"]["visa_sponsorship_required"] = True
        self.scorer = Scorer(self.prefs)
        
        job = {"title": "Senior Python Developer", "description": "test", "location": "Remote"}
        res = self.scorer.score_job(job) # 60
        
        # Case 1: Sponsor match
        enriched_yes = {"enrichment_status": "success", "visa_sponsor": True}
        res_yes = self.scorer.apply_enrichment_adjustments(res, enriched_yes)
        self.assertEqual(res_yes["score"], 68.0) # 60 + 8
        
        # Case 2: Sponsor mismatch -> Hard-Drop
        enriched_no = {"enrichment_status": "success", "visa_sponsor": False}
        res_no = self.scorer.apply_enrichment_adjustments(res, enriched_no)
        self.assertEqual(res_no["score"], 0.0)
        self.assertEqual(res_no["fit_band"], "Filtered Out")

    def test_engineer_family_is_hard_dropped_when_not_targeted(self):
        prefs = {
            "titles": {
                "positive_weights": {
                    "solution architect": 10,
                    "technical product manager": 10,
                },
                "positive_keywords": [
                    "solution architect",
                    "technical product manager",
                    "data architect",
                ],
                "negative_disqualifiers": ["software engineer"],
                "require_one_positive_keyword": True,
            },
            "keywords": {
                "body_positive": {
                    "brokerage": 10,
                    "api integration": 10,
                    "sql": 10,
                },
                "body_negative": {},
            },
            "search": {
                "compensation": {
                    "target_salary_usd": 180000,
                    "min_salary_usd": 170000,
                    "allow_missing_salary": True,
                    "enforce_min_salary": True,
                },
                "geography": {"us_only": True, "allow_international_remote": False},
                "location_preferences": {
                    "remote_us": {"enabled": True, "bonus": 10},
                    "local_hybrid": {"enabled": True, "bonus": 4, "allow_if_salary_at_least_usd": 175000, "markers": ["boulder"]},
                },
                "experience": {"years": 18, "gap_tolerance": 2},
                "contractor": {},
            },
            "policy": {
                "title_rescue": {
                    "adjacent_title_min_score_to_keep": 26,
                    "strong_body_domain_markers": ["brokerage", "api"],
                    "adjacent_title_markers": ["implementation", "solutions"],
                    "analyst_variant_markers": ["technical analyst"],
                    "adjacent_title_auto_rescue_patterns": ["implementation specialist"],
                }
            },
        }
        scorer = Scorer(prefs)
        job = {
            "title": "Senior Full-Stack Engineer - Trading API",
            "description": "Strong brokerage, api integration, and sql requirements",
            "location": "Remote - United States",
        }
        res = scorer.score_job(job)
        self.assertEqual(res["score"], 0.0)
        self.assertEqual(res["fit_band"], "Disqualified")
        self.assertIn("engineer family", res["decision_reason"])

    def test_product_manager_for_developer_experience_is_not_hard_dropped(self):
        prefs = {
            "titles": {
                "positive_weights": {
                    "staff product manager": 9,
                },
                "positive_keywords": [
                    "staff product manager",
                    "technical product manager",
                ],
                "negative_disqualifiers": [],
                "require_one_positive_keyword": True,
            },
            "keywords": {
                "body_positive": {"sql": 10, "api integration": 10, "developer tools": 5},
                "body_negative": {},
            },
            "search": {
                "compensation": {"target_salary_usd": 180000, "min_salary_usd": 170000, "allow_missing_salary": True},
                "geography": {"us_only": True, "allow_international_remote": False},
                "location_preferences": {
                    "remote_us": {"enabled": True, "bonus": 10},
                    "local_hybrid": {"enabled": False, "bonus": 0, "allow_if_salary_at_least_usd": 0, "markers": []},
                },
                "experience": {"years": 18, "gap_tolerance": 2},
                "contractor": {},
            },
            "policy": {"title_rescue": {"adjacent_title_min_score_to_keep": 26}},
        }
        scorer = Scorer(prefs)
        job = {
            "title": "Staff Product Manager - Developer Experience",
            "description": "Strong sql and api integration requirements",
            "location": "Remote - United States",
        }
        res = scorer.score_job(job)
        self.assertGreater(res["score"], 0.0)
        self.assertNotEqual(res["fit_band"], "Disqualified")

    def test_known_below_floor_salary_is_filtered_when_enforced(self):
        prefs = {
            "titles": {
                "positive_weights": {"solution architect": 10},
                "positive_keywords": ["solution architect"],
                "negative_disqualifiers": [],
                "require_one_positive_keyword": True,
            },
            "keywords": {
                "body_positive": {"brokerage": 10, "api integration": 10, "sql": 10},
                "body_negative": {},
            },
            "search": {
                "compensation": {
                    "target_salary_usd": 180000,
                    "min_salary_usd": 170000,
                    "allow_missing_salary": True,
                    "enforce_min_salary": True,
                },
                "geography": {"us_only": True, "allow_international_remote": False},
                "location_preferences": {
                    "remote_us": {"enabled": True, "bonus": 10},
                    "local_hybrid": {"enabled": True, "bonus": 4, "allow_if_salary_at_least_usd": 175000, "markers": ["boulder"]},
                },
                "experience": {"years": 18, "gap_tolerance": 2},
                "contractor": {},
            },
            "policy": {"title_rescue": {"adjacent_title_min_score_to_keep": 26}},
        }
        scorer = Scorer(prefs)
        job = {
            "title": "Solution Architect",
            "description": "brokerage api integration sql",
            "location": "Remote - United States",
            "salary_min": 140000,
            "salary_max": 155000,
        }
        res = scorer.score_job(job)
        self.assertEqual(res["score"], 0.0)
        self.assertEqual(res["fit_band"], "Filtered Out")
        self.assertIn("below minimum requirement", res["decision_reason"])

    def test_high_domain_but_unaligned_title_is_filtered_when_required(self):
        prefs = {
            "titles": {
                "positive_weights": {"solution architect": 10},
                "positive_keywords": ["solution architect", "technical product manager"],
                "negative_disqualifiers": [],
                "require_one_positive_keyword": True,
            },
            "keywords": {
                "body_positive": {"brokerage": 10, "api integration": 10, "sql": 10, "data lineage": 8},
                "body_negative": {},
            },
            "search": {
                "compensation": {
                    "target_salary_usd": 180000,
                    "min_salary_usd": 170000,
                    "allow_missing_salary": True,
                },
                "geography": {"us_only": True, "allow_international_remote": False},
                "location_preferences": {
                    "remote_us": {"enabled": True, "bonus": 10},
                    "local_hybrid": {"enabled": True, "bonus": 4, "allow_if_salary_at_least_usd": 175000, "markers": ["boulder"]},
                },
                "experience": {"years": 18, "gap_tolerance": 2},
                "contractor": {},
            },
            "policy": {
                "title_rescue": {
                    "adjacent_title_min_score_to_keep": 26,
                    "strong_body_domain_markers": ["brokerage", "api", "data lineage"],
                    "adjacent_title_markers": ["implementation", "solutions"],
                    "analyst_variant_markers": ["technical analyst"],
                    "adjacent_title_auto_rescue_patterns": ["implementation specialist"],
                }
            },
        }
        scorer = Scorer(prefs)
        job = {
            "title": "Platform",
            "description": "brokerage api integration sql data lineage",
            "location": "Remote - United States",
        }
        res = scorer.score_job(job)
        self.assertEqual(res["score"], 0.0)
        self.assertEqual(res["fit_band"], "Filtered Out")
        self.assertIn("title does not match target roles", res["decision_reason"])

    def test_generic_revenue_strategy_analyst_title_is_filtered(self):
        prefs = {
            "titles": {
                "positive_weights": {
                    "solution architect": 10,
                    "technical product manager": 10,
                    "business systems analyst": 8,
                },
                "positive_keywords": [
                    "solution architect",
                    "technical product manager",
                    "business systems analyst",
                    "senior business systems analyst",
                ],
                "negative_disqualifiers": [],
                "require_one_positive_keyword": True,
            },
            "keywords": {
                "body_positive": {
                    "sql": 10,
                    "data lineage": 8,
                    "cross functional": 3,
                    "financial systems": 10,
                },
                "body_negative": {
                    "salesforce": 20,
                    "gong": 20,
                    "zoominfo": 20,
                    "revenue technology": 20,
                },
            },
            "search": {
                "compensation": {
                    "target_salary_usd": 180000,
                    "min_salary_usd": 170000,
                    "allow_missing_salary": True,
                },
                "geography": {"us_only": True, "allow_international_remote": False},
                "location_preferences": {
                    "remote_us": {"enabled": True, "bonus": 10},
                    "local_hybrid": {"enabled": True, "bonus": 4, "allow_if_salary_at_least_usd": 175000, "markers": ["boulder"]},
                },
                "experience": {"years": 18, "gap_tolerance": 2},
                "contractor": {},
            },
            "policy": {
                "title_rescue": {
                    "adjacent_title_min_score_to_keep": 26,
                    "strong_body_domain_markers": ["financial systems", "data lineage"],
                    "adjacent_title_markers": ["implementation", "solutions"],
                    "analyst_variant_markers": ["technical analyst", "implementation analyst"],
                    "adjacent_title_auto_rescue_patterns": ["implementation specialist"],
                }
            },
        }
        scorer = Scorer(prefs)
        job = {
            "title": "Senior Revenue Strategy Analyst",
            "description": "sql cross functional analysis for salesforce, gong, and zoominfo revenue technology stack",
            "location": "Remote - United States",
        }
        res = scorer.score_job(job)
        self.assertEqual(res["score"], 0.0)
        self.assertEqual(res["fit_band"], "Filtered Out")
        self.assertIn("title does not match target roles", res["decision_reason"])

    def test_billing_analyst_does_not_inherit_business_analyst_credit(self):
        prefs = {
            "titles": {
                "positive_weights": {
                    "senior business analyst": 8,
                    "senior systems analyst": 8,
                    "business systems analyst": 8,
                    "solution architect": 10,
                },
                "positive_keywords": [
                    "senior business analyst",
                    "senior systems analyst",
                    "business systems analyst",
                    "solution architect",
                ],
                "negative_disqualifiers": [],
                "require_one_positive_keyword": True,
            },
            "keywords": {
                "body_positive": {"financial systems": 10, "reconciliation": 8, "sox": 8},
                "body_negative": {},
            },
            "search": {
                "compensation": {"target_salary_usd": 180000, "min_salary_usd": 170000, "allow_missing_salary": True},
                "geography": {"us_only": True, "allow_international_remote": False},
                "location_preferences": {
                    "remote_us": {"enabled": True, "bonus": 10},
                    "local_hybrid": {"enabled": False, "bonus": 0, "allow_if_salary_at_least_usd": 0, "markers": []},
                },
                "experience": {"years": 18, "gap_tolerance": 2},
                "contractor": {},
            },
            "policy": {
                "title_rescue": {
                    "adjacent_title_min_score_to_keep": 26,
                    "strong_body_domain_markers": ["financial systems", "reconciliation", "sox"],
                    "adjacent_title_markers": ["implementation", "solutions"],
                    "analyst_variant_markers": ["technical analyst", "implementation analyst"],
                }
            },
        }
        scorer = Scorer(prefs)
        job = {
            "title": "Senior Billing Analyst",
            "description": "financial systems reconciliation sox",
            "location": "Remote - United States",
        }
        res = scorer.score_job(job)
        self.assertEqual(res["score"], 0.0)
        self.assertEqual(res["fit_band"], "Filtered Out")

    def test_product_management_director_does_not_inherit_product_manager_credit(self):
        prefs = {
            "titles": {
                "positive_weights": {
                    "technical product manager": 10,
                    "senior technical product manager": 10,
                    "wealth management product manager": 8,
                },
                "positive_keywords": [
                    "technical product manager",
                    "senior technical product manager",
                    "wealth management product manager",
                ],
                "negative_disqualifiers": [],
                "require_one_positive_keyword": True,
            },
            "keywords": {
                "body_positive": {"fintech": 10, "wealth management": 10, "cross functional": 3},
                "body_negative": {},
            },
            "search": {
                "compensation": {"target_salary_usd": 180000, "min_salary_usd": 170000, "allow_missing_salary": True},
                "geography": {"us_only": True, "allow_international_remote": False},
                "location_preferences": {
                    "remote_us": {"enabled": True, "bonus": 10},
                    "local_hybrid": {"enabled": False, "bonus": 0, "allow_if_salary_at_least_usd": 0, "markers": []},
                },
                "experience": {"years": 18, "gap_tolerance": 2},
                "contractor": {},
            },
            "policy": {
                "title_rescue": {
                    "adjacent_title_min_score_to_keep": 26,
                    "strong_body_domain_markers": ["fintech", "wealth management"],
                    "adjacent_title_markers": ["implementation", "solutions"],
                    "analyst_variant_markers": ["technical analyst"],
                }
            },
        }
        scorer = Scorer(prefs)
        job = {
            "title": "Senior Director, Product Management",
            "description": "fintech wealth management cross functional leadership",
            "location": "Remote - United States",
        }
        res = scorer.score_job(job)
        self.assertEqual(res["score"], 0.0)
        self.assertEqual(res["fit_band"], "Filtered Out")

if __name__ == "__main__":
    unittest.main()
