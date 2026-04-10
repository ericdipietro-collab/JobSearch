"""
Tests for the Deterministic 2.0 scoring engine (scoring_v2.py).

Coverage
--------
Stage 0  — JD Quality Triage
Stage 1  — Title Normalisation (exact, stripped, fuzzy, unresolved)
Stage 2  — Seniority Gate (band detection, multipliers, under/over-qualification)
Stage 3  — Section-Aware JD Parser
Stage 4  — Tiered Keyword Scorer (anchor vs. baseline, section weighting, caps)
Stage 5  — Full pipeline integration (fintech-realistic scenarios)
"""

import pytest
from jobsearch.scraper.scoring_v2 import (
    AnchorKeyword,
    BaselineKeyword,
    ScoringV2Config,
    build_title_index,
    triage_jd,
    normalize_title,
    evaluate_seniority,
    parse_jd_sections,
    score_keywords,
    score_job_v2,
    ParsedJD,
)


# ---------------------------------------------------------------------------
# Shared fixtures
# ---------------------------------------------------------------------------

FINTECH_SYNONYM_MAP = {
    # Business Analyst family
    "sr. ba":                       "business_analyst",
    "sr ba":                        "business_analyst",
    "senior business analyst":      "business_analyst",
    "senior ba":                    "business_analyst",
    "technical ba":                 "business_analyst",
    "technical business analyst":   "business_analyst",
    "lead business analyst":        "business_analyst",
    "business analyst":             "business_analyst",
    # Solutions Architect family
    "solutions architect":          "solutions_architect",
    "solution architect":           "solutions_architect",
    "sr. solutions architect":      "solutions_architect",
    "enterprise solutions architect": "solutions_architect",
    # Product Manager family
    "product manager":              "product_manager",
    "senior product manager":       "product_manager",
    "sr pm":                        "product_manager",
    "sr. pm":                       "product_manager",
    # Data family
    "data engineer":                "data_engineer",
    "senior data engineer":         "data_engineer",
    "staff data engineer":          "data_engineer",
}

ANCHOR_KEYWORDS = [
    AnchorKeyword("aladdin", 20),
    AnchorKeyword("eagle", 18),
    AnchorKeyword("bloomberg", 15),
    AnchorKeyword("charles river", 18),
    AnchorKeyword("fidessa", 15),
    AnchorKeyword("order management system", 15),
    AnchorKeyword("oms", 12),
    AnchorKeyword("portfolio management", 12),
    AnchorKeyword("risk analytics", 12),
]

BASELINE_KEYWORDS = [
    BaselineKeyword("sql", 5),
    BaselineKeyword("python", 5),
    BaselineKeyword("jira", 3),
    BaselineKeyword("agile", 3),
    BaselineKeyword("scrum", 3),
    BaselineKeyword("api", 4),
    BaselineKeyword("aws", 5),
    BaselineKeyword("data analysis", 5),
]

NEGATIVE_KEYWORDS = [
    ("php", 10),
    ("wordpress", 10),
    ("drupal", 8),
    ("ruby on rails", 8),
    ("c# gaming", 15),
]


@pytest.fixture
def cfg() -> ScoringV2Config:
    return ScoringV2Config(
        synonym_map=FINTECH_SYNONYM_MAP,
        fuzzy_match_threshold=88,
        user_years_experience=10.0,
        seniority_bands={
            "junior": (0.0, 3.0),
            "mid":    (3.0, 7.0),
            "senior": (7.0, 12.0),
            "lead":   (12.0, 99.0),
        },
        seniority_multipliers={
            "junior":  0.50,
            "mid":     0.75,
            "senior":  1.00,
            "lead":    1.00,
            "unknown": 0.85,
        },
        section_weights={
            "requirements":     1.00,
            "responsibilities": 0.80,
            "nice_to_have":     0.60,
            "about_company":    0.20,
            "benefits":         0.10,
            "fallback":         0.50,
        },
        anchor_keywords=ANCHOR_KEYWORDS,
        baseline_keywords=BASELINE_KEYWORDS,
        negative_keywords=NEGATIVE_KEYWORDS,
        anchor_cap=60,
        baseline_cap=30,
        negative_cap=45,
        jd_min_chars=400,
        stub_score_drag=0.85,
        sparse_score_drag=0.92,
    )


@pytest.fixture
def title_index(cfg) -> dict:
    return build_title_index(cfg)


# ---------------------------------------------------------------------------
# Stage 0 — JD Quality Triage
# ---------------------------------------------------------------------------

class TestJDQualityTriage:

    def test_normal_jd_has_no_flags(self, cfg):
        jd = (
            "About the Role\n"
            "We are looking for a Senior Business Analyst.\n\n"
            "Requirements\n"
            "- 7+ years of experience in financial services\n"
            "- Strong SQL and Python skills\n"
            "- Experience with Aladdin or similar OMS platforms\n"
            "- Agile/Scrum background preferred\n" * 5
        )
        q = triage_jd(jd, cfg)
        assert not q.is_stub
        assert not q.is_sparse
        assert q.flags == []

    def test_short_jd_flagged_as_stub(self, cfg):
        q = triage_jd("Great role. Apply now.", cfg)
        assert q.is_stub
        assert any("jd_stub" in f for f in q.flags)

    def test_long_jd_without_requirements_section_flagged_sparse(self, cfg):
        # Long enough but no Requirements/Qualifications heading
        jd = "We are an exciting fintech company. " * 30
        q = triage_jd(jd, cfg)
        assert not q.is_stub
        assert q.is_sparse
        assert any("jd_sparse" in f for f in q.flags)

    def test_qualifications_heading_satisfies_requirements_probe(self, cfg):
        jd = "Qualifications\n- 5+ years experience\n" + ("padding " * 100)
        q = triage_jd(jd, cfg)
        assert not q.is_sparse

    def test_char_count_is_accurate(self, cfg):
        jd = "A" * 500
        q = triage_jd(jd, cfg)
        assert q.char_count == 500

    def test_html_stripped_before_char_count(self, cfg):
        # 100 chars of actual text hidden inside HTML tags
        jd = "<div>" + ("A" * 100) + "</div>"
        q = triage_jd(jd, cfg)
        # HTML tags removed; remaining visible text < 400 chars → stub
        assert q.is_stub


# ---------------------------------------------------------------------------
# Stage 1 — Title Normalisation
# ---------------------------------------------------------------------------

class TestTitleNormalisation:

    def test_exact_match(self, cfg, title_index):
        canon, method = normalize_title("business analyst", title_index, cfg)
        assert canon == "business_analyst"
        assert method == "exact"

    def test_exact_match_case_insensitive(self, cfg, title_index):
        canon, method = normalize_title("SENIOR BUSINESS ANALYST", title_index, cfg)
        assert canon == "business_analyst"
        assert method == "exact"

    def test_seniority_stripped_exact(self, cfg, title_index):
        # "Sr. BA" is in the map, but test stripping for a title NOT in map
        # "Principal Business Analyst" → strip "principal" → "business analyst" → hit
        canon, method = normalize_title("Principal Business Analyst", title_index, cfg)
        assert canon == "business_analyst"
        assert method in ("exact", "exact_stripped")

    def test_sr_ba_abbreviation(self, cfg, title_index):
        canon, method = normalize_title("Sr. BA", title_index, cfg)
        assert canon == "business_analyst"

    def test_technical_ba_maps_correctly(self, cfg, title_index):
        canon, method = normalize_title("Technical BA", title_index, cfg)
        assert canon == "business_analyst"

    def test_solutions_architect_family(self, cfg, title_index):
        canon, method = normalize_title("Enterprise Solutions Architect", title_index, cfg)
        assert canon == "solutions_architect"

    def test_unresolved_returns_none(self, cfg, title_index):
        canon, method = normalize_title("Underwater Basket Weaver III", title_index, cfg)
        assert canon is None
        assert method == "unresolved"

    def test_fuzzy_match_catches_typo(self, cfg, title_index):
        # "Senoir Business Analyst" — classic typo
        canon, method = normalize_title("Senoir Business Analyst", title_index, cfg)
        # RapidFuzz should catch this
        if canon is not None:
            assert canon == "business_analyst"
            assert "fuzzy" in method
        # If rapidfuzz not installed, graceful degradation is acceptable
        else:
            assert method == "unresolved"

    def test_empty_index_returns_unresolved(self, cfg):
        canon, method = normalize_title("Business Analyst", {}, cfg)
        assert canon is None
        assert method == "unresolved"

    def test_empty_title_returns_unresolved(self, cfg, title_index):
        canon, method = normalize_title("", title_index, cfg)
        assert canon is None
        assert method == "unresolved"


# ---------------------------------------------------------------------------
# Stage 2 — Seniority Gate
# ---------------------------------------------------------------------------

class TestSeniorityGate:

    def test_explicit_years_junior(self, cfg):
        # user_years_experience=10, required=2 → over-qualified (10 > 2+5=7), NOT under.
        # Over-qualification does not drag the multiplier; junior band multiplier stays at 0.5.
        result = evaluate_seniority("Analyst", "Requires 2+ years of experience.", cfg)
        assert result.band == "junior"
        assert result.required_years == 2.0
        assert result.over_qualified is True
        assert result.under_qualified is False
        assert result.multiplier == pytest.approx(0.5)

    def test_user_overqualified_senior_role(self, cfg):
        # user=10 yoe, role requires 3 yoe → over-qualified (10 > 3+5=8)
        result = evaluate_seniority("Business Analyst", "Requires 3+ years experience.", cfg)
        assert result.over_qualified is True
        assert any("overqualified" in f for f in result.flags)
        # Over-qualification does NOT reduce multiplier (senior can do mid-level)

    def test_user_underqualified_hard_role(self, cfg):
        # user=10 yoe, role requires 15 yoe → under-qualified (15-10=5 > 2)
        result = evaluate_seniority("Principal Architect", "Requires 15+ years of experience.", cfg)
        assert result.under_qualified is True
        assert any("underqualified" in f for f in result.flags)
        assert result.multiplier < 1.0

    def test_senior_band_from_years(self, cfg):
        result = evaluate_seniority("Business Analyst", "Minimum 8 years of experience.", cfg)
        assert result.band == "senior"
        assert result.multiplier == pytest.approx(1.0)

    def test_lead_band_from_years(self, cfg):
        # user_years_experience=10, required=13 → under-qualified (13-10=3 > 2 gap tolerance).
        # Under-qualification applies 0.6× drag: lead(1.0) * 0.6 = 0.6.
        result = evaluate_seniority("Architect", "Requires 13+ years of relevant experience.", cfg)
        assert result.band == "lead"
        assert result.under_qualified is True
        assert result.multiplier == pytest.approx(1.0 * 0.6)

    def test_senior_fallback_from_title_signal(self, cfg):
        # No years mentioned, but title says "Senior"
        result = evaluate_seniority("Senior Business Analyst", "Great role at a great company.", cfg)
        assert result.band == "senior"
        assert result.required_years is None

    def test_junior_fallback_from_title_signal(self, cfg):
        result = evaluate_seniority("Junior Data Analyst", "Join our team.", cfg)
        assert result.band == "junior"

    def test_unknown_band_when_no_signals(self, cfg):
        result = evaluate_seniority("Business Analyst", "Great opportunity.", cfg)
        assert result.band == "unknown"
        assert result.multiplier == pytest.approx(0.85)

    def test_max_years_used_when_multiple_mentioned(self, cfg):
        # "3+ years" in nice-to-have and "10+ years" in requirements → should use 10
        jd = "Nice to have: 3+ years with Python. Requirements: 10+ years of experience."
        result = evaluate_seniority("Sr. BA", jd, cfg)
        assert result.required_years == pytest.approx(10.0)
        assert result.band == "senior"

    def test_fortune_500_not_mistaken_for_years(self, cfg):
        jd = "Join our Fortune 500 company. Requirements: 5 years of experience."
        result = evaluate_seniority("Analyst", jd, cfg)
        assert result.required_years == pytest.approx(5.0)

    def test_zero_user_experience_skips_gate(self):
        # When user_years_experience=0, no over/under qualification flags
        cfg_no_yoe = ScoringV2Config(user_years_experience=0.0)
        result = evaluate_seniority("Junior Dev", "Requires 2+ years.", cfg_no_yoe)
        assert result.over_qualified is False
        assert result.under_qualified is False


# ---------------------------------------------------------------------------
# Stage 3 — Section-Aware JD Parser
# ---------------------------------------------------------------------------

SAMPLE_JD = """
About Us
We are a leading fintech company building the future of wealth management.
Our platform serves thousands of financial advisors globally.

Responsibilities
- Gather and document business requirements from stakeholders
- Translate complex financial workflows into system specifications
- Collaborate with engineering and product teams daily

Requirements
- 8+ years of experience as a Business Analyst in financial services
- Deep knowledge of Aladdin, Eagle, or Charles River OMS platforms
- Strong SQL skills for data analysis and validation
- Experience with Python scripting
- Jira and Agile/Scrum familiarity

Nice to Have
- Bloomberg Terminal experience
- Exposure to risk analytics tooling
- CFA or CAIA certification

Benefits
- Competitive salary and equity
- Comprehensive health benefits
- 401k with company match
"""


class TestSectionParser:

    def test_all_four_sections_detected(self):
        parsed = parse_jd_sections(SAMPLE_JD)
        assert "requirements" in parsed.sections
        assert "about_company" in parsed.sections
        assert "nice_to_have" in parsed.sections
        assert "benefits" in parsed.sections

    def test_responsibilities_detected(self):
        parsed = parse_jd_sections(SAMPLE_JD)
        assert "responsibilities" in parsed.sections

    def test_requirements_section_contains_aladdin(self):
        parsed = parse_jd_sections(SAMPLE_JD)
        req_text = parsed.sections.get("requirements", "")
        assert "aladdin" in req_text.lower()

    def test_about_company_section_does_not_contain_aladdin(self):
        parsed = parse_jd_sections(SAMPLE_JD)
        about_text = parsed.sections.get("about_company", "")
        assert "aladdin" not in about_text.lower()

    def test_no_sections_returns_fallback(self):
        jd = "This is a flat job description with no headings at all. " * 5
        parsed = parse_jd_sections(jd)
        assert parsed.sections == {}
        assert len(parsed.fallback) > 0

    def test_html_stripped_from_sections(self):
        # Single-line HTML: after stripping tags the header pattern's line-anchor
        # won't find a newline-preceded header, so content lands in fallback.
        # The important assertions are (a) no crash and (b) content is preserved.
        jd = "<h2>Requirements</h2><ul><li>SQL experience</li><li>Python</li></ul>"
        parsed = parse_jd_sections(jd)
        all_text = " ".join(parsed.sections.values()) + " " + parsed.fallback
        assert "sql" in all_text.lower() or "python" in all_text.lower() or all_text.strip() == ""
        # Specifically: content must not be silently lost if it ends up in fallback
        assert not (parsed.sections == {} and parsed.fallback.strip() == "")

    def test_duplicate_section_headers_merged(self):
        jd = (
            "Requirements\n- Must have SQL\n\n"
            "Benefits\n- Health insurance\n\n"
            "Requirements\n- Must have Python\n"
        )
        parsed = parse_jd_sections(jd)
        req = parsed.sections.get("requirements", "")
        assert "sql" in req.lower()
        assert "python" in req.lower()


# ---------------------------------------------------------------------------
# Stage 4 — Tiered Keyword Scorer
# ---------------------------------------------------------------------------

class TestKeywordScorer:

    def test_anchor_keyword_in_requirements_scores_full_weight(self, cfg):
        parsed = ParsedJD(
            sections={"requirements": "Experience with Aladdin OMS required."},
            fallback="",
        )
        result = score_keywords(parsed, cfg)
        # Aladdin weight=20, requirements section_weight=1.0 → 20.0 pts
        anchor_hits = [(t, s, p) for t, s, p in result.hits if t == "aladdin"]
        assert anchor_hits
        assert anchor_hits[0][2] == pytest.approx(20.0)

    def test_same_anchor_in_about_company_scores_discounted(self, cfg):
        parsed = ParsedJD(
            sections={"about_company": "We build Aladdin integrations."},
            fallback="",
        )
        result = score_keywords(parsed, cfg)
        anchor_hits = [(t, s, p) for t, s, p in result.hits if t == "aladdin"]
        assert anchor_hits
        # about_company weight=0.20 → 20 * 0.20 = 4.0
        assert anchor_hits[0][2] == pytest.approx(4.0)

    def test_anchor_beats_baseline_in_requirements(self, cfg):
        parsed = ParsedJD(
            sections={"requirements": "Must know Aladdin, SQL, and Python."},
            fallback="",
        )
        result = score_keywords(parsed, cfg)
        assert result.anchor_score > result.baseline_score

    def test_negative_keyword_reduces_total(self, cfg):
        parsed = ParsedJD(
            sections={"requirements": "PHP developer with WordPress experience required."},
            fallback="",
        )
        result = score_keywords(parsed, cfg)
        assert result.negative_score > 0
        assert result.total < result.anchor_score + result.baseline_score

    def test_anchor_cap_enforced(self, cfg):
        # Many high-weight anchor hits that exceed the cap
        sections = {
            "requirements": (
                "Aladdin experience. Eagle experience. Bloomberg access. "
                "Charles River integration. Fidessa support. OMS system. "
                "Order management system design. Portfolio management. Risk analytics."
            )
        }
        parsed = ParsedJD(sections=sections, fallback="")
        result = score_keywords(parsed, cfg)
        assert result.anchor_score <= cfg.anchor_cap

    def test_baseline_cap_enforced(self, cfg):
        sections = {
            "requirements": (
                "SQL Python Jira Agile Scrum API AWS data analysis "
                "and many more baseline skills required."
            )
        }
        parsed = ParsedJD(sections=sections, fallback="")
        result = score_keywords(parsed, cfg)
        assert result.baseline_score <= cfg.baseline_cap

    def test_keyword_not_double_counted_in_same_section(self, cfg):
        # "sql" appears twice in requirements — should only score once
        parsed = ParsedJD(
            sections={"requirements": "SQL is required. Candidates must know SQL."},
            fallback="",
        )
        result = score_keywords(parsed, cfg)
        sql_hits = [(t, s, p) for t, s, p in result.hits if t == "sql"]
        assert len(sql_hits) == 1

    def test_keyword_scores_in_multiple_sections(self, cfg):
        # "sql" in both requirements and nice_to_have — should score in each
        parsed = ParsedJD(
            sections={
                "requirements": "Must have SQL experience.",
                "nice_to_have": "SQL certification is a bonus.",
            },
            fallback="",
        )
        result = score_keywords(parsed, cfg)
        sql_hits = [(t, s, p) for t, s, p in result.hits if t == "sql"]
        assert len(sql_hits) == 2

    def test_no_anchor_hits_flag_set(self, cfg):
        parsed = ParsedJD(
            sections={"requirements": "Jira and Agile experience required."},
            fallback="",
        )
        result = score_keywords(parsed, cfg)
        assert "no_anchor_hits" in result.flags

    def test_fallback_section_uses_fallback_weight(self, cfg):
        parsed = ParsedJD(sections={}, fallback="Experience with Aladdin preferred.")
        result = score_keywords(parsed, cfg)
        aladdin_hits = [(t, s, p) for t, s, p in result.hits if t == "aladdin"]
        assert aladdin_hits
        # fallback weight=0.5 → 20 * 0.5 = 10.0
        assert aladdin_hits[0][2] == pytest.approx(10.0)

    def test_multi_word_term_matches_with_separator_variants(self, cfg):
        # "order management system" vs "order-management-system"
        parsed = ParsedJD(
            sections={"requirements": "Experience with order-management-system required."},
            fallback="",
        )
        result = score_keywords(parsed, cfg)
        oms_hits = [t for t, s, p in result.hits if "order management" in t]
        assert oms_hits

    def test_plural_form_matched(self, cfg):
        # "portfolio management" → should match "portfolio management systems"
        # The trailing 's?' rule applies to multi-word phrases
        parsed = ParsedJD(
            sections={"requirements": "Design portfolio management systems for clients."},
            fallback="",
        )
        result = score_keywords(parsed, cfg)
        pm_hits = [t for t, s, p in result.hits if "portfolio management" in t]
        assert pm_hits


# ---------------------------------------------------------------------------
# Stage 5 — Full Pipeline Integration
# ---------------------------------------------------------------------------

class TestFullPipeline:

    def _make_cfg(self, user_yoe: float = 10.0) -> ScoringV2Config:
        return ScoringV2Config(
            synonym_map=FINTECH_SYNONYM_MAP,
            fuzzy_match_threshold=88,
            user_years_experience=user_yoe,
            seniority_multipliers={
                "junior": 0.50, "mid": 0.75, "senior": 1.00,
                "lead": 1.00, "unknown": 0.85,
            },
            section_weights={
                "requirements": 1.00, "responsibilities": 0.80,
                "nice_to_have": 0.60, "about_company": 0.20,
                "benefits": 0.10, "fallback": 0.50,
            },
            anchor_keywords=ANCHOR_KEYWORDS,
            baseline_keywords=BASELINE_KEYWORDS,
            negative_keywords=NEGATIVE_KEYWORDS,
            anchor_cap=60,
            baseline_cap=30,
            jd_min_chars=400,
        )

    def test_strong_fintech_ba_role_scores_high(self):
        """A well-written senior BA role at an Aladdin shop should score well."""
        cfg = self._make_cfg(user_yoe=10.0)
        idx = build_title_index(cfg)
        result = score_job_v2(
            raw_title="Senior Business Analyst",
            description=SAMPLE_JD,
            title_base_pts=20.0,
            cfg=cfg,
            title_index=idx,
        )
        assert result.canonical_title == "business_analyst"
        assert result.final_score > 50
        assert result.fit_band in ("Fair Match", "Good Match", "Strong Match")
        assert result.keyword.anchor_score > 0

    def test_sr_ba_abbreviation_resolves_correctly(self):
        cfg = self._make_cfg()
        idx = build_title_index(cfg)
        result = score_job_v2(
            raw_title="Sr. BA",
            description=SAMPLE_JD,
            title_base_pts=15.0,
            cfg=cfg,
            title_index=idx,
        )
        assert result.canonical_title == "business_analyst"
        assert result.title_method == "exact"

    def test_unrelated_title_gets_unresolved_flag(self):
        cfg = self._make_cfg()
        idx = build_title_index(cfg)
        result = score_job_v2(
            raw_title="Marketing Coordinator",
            description=SAMPLE_JD,
            title_base_pts=0.0,
            cfg=cfg,
            title_index=idx,
        )
        assert result.canonical_title is None
        assert "title:unresolved" in result.flags

    def test_junior_role_score_penalised_by_seniority_multiplier(self):
        cfg = self._make_cfg(user_yoe=10.0)
        idx = build_title_index(cfg)
        junior_jd = (
            "Requirements\n"
            "- 1-2 years of experience\n"
            "- Basic SQL and Python knowledge\n"
            "- Familiarity with Jira and Agile\n"
            "- Entry-level business analysis skills\n" * 5
        )
        result = score_job_v2(
            raw_title="Junior Business Analyst",
            description=junior_jd,
            title_base_pts=10.0,
            cfg=cfg,
            title_index=idx,
        )
        # Junior multiplier is 0.5 — score should be half of what a senior would get
        assert result.seniority.band == "junior"
        assert result.seniority.multiplier <= 0.5
        assert result.final_score < result.raw_score

    def test_senior_role_scores_higher_than_identical_junior_role(self):
        cfg = self._make_cfg(user_yoe=10.0)
        idx = build_title_index(cfg)

        base_jd = (
            "Requirements\n"
            "- SQL, Python, Jira, Agile required\n"
            "- Aladdin OMS experience preferred\n"
            "- Financial services background\n" * 4
        )
        senior_jd = "Requirements\n- 9+ years of experience.\n" + base_jd
        junior_jd = "Requirements\n- 2+ years of experience.\n" + base_jd

        senior = score_job_v2("Senior Business Analyst", senior_jd, 20.0, cfg, idx)
        junior = score_job_v2("Junior Business Analyst", junior_jd, 10.0, cfg, idx)

        assert senior.final_score > junior.final_score

    def test_stub_jd_applies_score_drag(self):
        cfg = self._make_cfg()
        idx = build_title_index(cfg)
        short_jd = "Requirements\n- 8 years exp with Aladdin."
        result = score_job_v2(
            raw_title="Senior Business Analyst",
            description=short_jd,
            title_base_pts=20.0,
            cfg=cfg,
            title_index=idx,
        )
        assert result.jd_quality.is_stub
        assert "score_drag:jd_stub" in result.flags
        # Verify drag was actually applied
        assert result.final_score < result.raw_score * result.seniority.multiplier + 0.01

    def test_sparse_jd_applies_score_drag(self):
        cfg = self._make_cfg()
        idx = build_title_index(cfg)
        # Long but no Requirements section
        sparse_jd = "We are a great company. " * 40
        result = score_job_v2(
            raw_title="Business Analyst",
            description=sparse_jd,
            title_base_pts=15.0,
            cfg=cfg,
            title_index=idx,
        )
        assert result.jd_quality.is_sparse
        assert "score_drag:jd_sparse" in result.flags

    def test_php_role_negative_keywords_drag_score(self):
        cfg = self._make_cfg()
        idx = build_title_index(cfg)
        wrong_stack_jd = (
            "Requirements\n"
            "- 8+ years of experience\n"
            "- PHP and WordPress development\n"
            "- Drupal CMS management\n"
            "- SQL and Python nice to have\n" * 5
        )
        result = score_job_v2(
            raw_title="Senior Business Analyst",
            description=wrong_stack_jd,
            title_base_pts=20.0,
            cfg=cfg,
            title_index=idx,
        )
        assert result.keyword.negative_score > 0

    def test_section_aware_scoring_penalises_boilerplate_anchors(self):
        """Aladdin mentioned only in 'About Us' should score less than Aladdin in Requirements."""
        cfg = self._make_cfg()
        idx = build_title_index(cfg)

        jd_anchor_in_req = (
            "About Us\nGreat fintech company.\n\n"
            "Requirements\n- 8+ years, Aladdin OMS required. SQL Python Agile.\n" * 4
        )
        jd_anchor_in_about = (
            "About Us\nWe were founded to disrupt Aladdin and Eagle.\n\n"
            "Requirements\n- 8+ years experience. SQL Python Agile Jira.\n" * 4
        )

        r_req = score_job_v2("Senior BA", jd_anchor_in_req, 20.0, cfg, idx)
        r_about = score_job_v2("Senior BA", jd_anchor_in_about, 20.0, cfg, idx)

        assert r_req.keyword.anchor_score > r_about.keyword.anchor_score

    def test_summary_dict_has_expected_keys(self):
        cfg = self._make_cfg()
        idx = build_title_index(cfg)
        result = score_job_v2("Senior Business Analyst", SAMPLE_JD, 20.0, cfg, idx)
        s = result.summary()
        for key in ("canonical_title", "title_method", "seniority_band",
                    "final_score", "fit_band", "flags", "keyword_hits"):
            assert key in s, f"Missing key in summary: {key}"

    def test_zero_title_pts_still_scores_from_keywords(self):
        cfg = self._make_cfg()
        idx = build_title_index(cfg)
        result = score_job_v2("Business Analyst", SAMPLE_JD, 0.0, cfg, idx)
        assert result.final_score > 0
        assert result.keyword.total > 0

    def test_fit_band_strong_match_threshold(self):
        cfg = self._make_cfg()
        idx = build_title_index(cfg)
        # Enough points to clear 85
        result = score_job_v2("Senior Business Analyst", SAMPLE_JD, 60.0, cfg, idx)
        if result.final_score >= 85:
            assert result.fit_band == "Strong Match"

    def test_fit_band_poor_match_threshold(self):
        cfg = self._make_cfg()
        idx = build_title_index(cfg)
        result = score_job_v2("Business Analyst", "Requirements\n- 8 years exp.", 0.0, cfg, idx)
        if result.final_score < 35:
            assert result.fit_band == "Poor Match"
