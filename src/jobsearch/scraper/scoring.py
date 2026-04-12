"""Refined scoring logic including contractor normalization."""

from __future__ import annotations

import logging
import html
import re
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

logger = logging.getLogger(__name__)


def normalize_compensation(preferences: Dict[str, Any], job_data: Dict[str, Any]) -> Dict[str, Any]:
    """Public helper for salary/W2/1099 normalization outside the scraper."""
    scorer = Scorer(preferences)
    work_type = scorer._derive_work_type(job_data)
    unit = scorer._compensation_unit(job_data, work_type)
    return scorer._normalize_compensation(job_data, work_type, unit)


class Scorer:
    def __init__(self, preferences: Dict[str, Any]):
        self.prefs = preferences

        title_cfg = self.prefs.get("titles", {})
        self.title_positive_weights = self._pref_weight_pairs(title_cfg.get("positive_weights"), [])
        self.title_positive_keywords = [str(item).strip().lower() for item in title_cfg.get("positive_keywords", []) if str(item).strip()]
        self.negative_disqualifiers = title_cfg.get("negative_disqualifiers", [])
        self.must_have_modifiers = title_cfg.get("must_have_modifiers", [])
        self.title_constraints = title_cfg.get("constraints", {}) or {}
        self.fast_track_base_score = float(title_cfg.get("fast_track_base_score", 0))
        self.fast_track_min_weight = int(title_cfg.get("fast_track_min_weight", 8))
        self.title_max_points = int(title_cfg.get("title_max_points", 25))
        self._positive_title_terms = {
            phrase
            for phrase in ([term for term, _ in self.title_positive_weights] + self.title_positive_keywords)
            if str(phrase).strip()
        }

        kw_cfg = self.prefs.get("keywords", {})
        self.body_positive = self._pref_weight_pairs(kw_cfg.get("body_positive"), [])
        self.body_negative = self._pref_weight_pairs(kw_cfg.get("body_negative"), [])

        self.scoring_cfg = self.prefs.get("scoring", {})
        self.apply_now_cfg = self.scoring_cfg.get("apply_now", {})
        keyword_matching_cfg = self.scoring_cfg.get("keyword_matching", {})
        self.min_score_to_keep = self.scoring_cfg.get("minimum_score_to_keep", 35)
        self.source_trust_cfg = self.scoring_cfg.get("source_trust", {})
        self.positive_keyword_cap = int(keyword_matching_cfg.get("positive_keyword_cap", 60))
        self.negative_keyword_cap = int(keyword_matching_cfg.get("negative_keyword_cap", 45))
        adjustments_cfg = self.scoring_cfg.get("adjustments", {})

        self.missing_salary_penalty = int(adjustments_cfg.get("missing_salary_penalty", 6))
        self.salary_at_or_above_target_bonus = int(adjustments_cfg.get("salary_at_or_above_target_bonus", 6))
        self.salary_meets_floor_bonus = int(adjustments_cfg.get("salary_meets_floor_bonus", 2))
        self.salary_below_target_penalty = int(adjustments_cfg.get("salary_below_target_penalty", 12))
        self.contract_role_penalty = int(adjustments_cfg.get("contract_role_penalty", 0))
        self.contractor_target_bonus = int(adjustments_cfg.get("contractor_target_bonus", 4))

        search_cfg = self.prefs.get("search", {})
        comp_cfg = search_cfg.get("compensation", {})
        geo_cfg = search_cfg.get("geography", {})
        contractor_cfg = search_cfg.get("contractor", {})
        location_pref_cfg = search_cfg.get("location_preferences", {})
        title_rescue_cfg = ((self.prefs.get("policy") or {}).get("title_rescue") or {})
        remote_us_cfg = location_pref_cfg.get("remote_us", {})
        local_hybrid_cfg = location_pref_cfg.get("local_hybrid", {})

        self.target_salary_usd = float(comp_cfg.get("target_salary_usd", 165000))
        self.min_salary_usd = float(comp_cfg.get("min_salary_usd", 165000))
        self.enforce_min_salary = bool(comp_cfg.get("enforce_min_salary", False))
        self.allow_missing_salary = bool(comp_cfg.get("allow_missing_salary", True))
        self.remote_only = comp_cfg.get("remote_only", True)
        self.location_policy = str(search_cfg.get("location_policy", "remote_only")).strip().lower() or "remote_only"
        self.us_only = bool(geo_cfg.get("us_only", True))
        self.allow_international_remote = bool(geo_cfg.get("allow_international_remote", False))
        self.require_one_positive_keyword = bool(title_cfg.get("require_one_positive_keyword", False))
        self.remote_us_enabled = bool(remote_us_cfg.get("enabled", True))
        self.remote_us_bonus = int(remote_us_cfg.get("bonus", 14))
        self.local_hybrid_enabled = bool(local_hybrid_cfg.get("enabled", True))
        self.local_hybrid_bonus = int(local_hybrid_cfg.get("bonus", 4))
        self.local_hybrid_salary_floor = float(local_hybrid_cfg.get("allow_if_salary_at_least_usd", self.target_salary_usd))
        self.local_hybrid_markers = [str(item).strip().lower() for item in local_hybrid_cfg.get("markers", []) if str(item).strip()]

        # Experience tolerance
        exp_cfg = search_cfg.get("experience", {})
        self.user_years_experience = float(exp_cfg.get("years", 0))
        self.experience_gap_tolerance = float(exp_cfg.get("gap_tolerance", 0))

        self.include_contract_roles = bool(contractor_cfg.get("include_contract_roles", True))
        self.allow_w2_hourly = bool(contractor_cfg.get("allow_w2_hourly", True))
        self.allow_1099_hourly = bool(contractor_cfg.get("allow_1099_hourly", True))
        self.default_hours_per_week = float(contractor_cfg.get("default_hours_per_week", 40))
        self.default_w2_weeks_per_year = float(contractor_cfg.get("default_w2_weeks_per_year", 50))
        self.default_1099_weeks_per_year = float(contractor_cfg.get("default_1099_weeks_per_year", 46))
        self.default_w2_hourly_weeks_per_year = float(contractor_cfg.get("default_w2_hourly_weeks_per_year", self.default_w2_weeks_per_year))
        self.benefits_replacement_usd = float(contractor_cfg.get("benefits_replacement_usd", 18000))
        self.w2_benefits_gap_usd = float(contractor_cfg.get("w2_benefits_gap_usd", 6000))
        self.overhead_1099_pct = float(contractor_cfg.get("overhead_1099_pct", 0.18))
        self.adjacent_title_min_score_to_keep = float(title_rescue_cfg.get("adjacent_title_min_score_to_keep", 26))
        self.strong_body_domain_markers = [str(item).strip().lower() for item in title_rescue_cfg.get("strong_body_domain_markers", []) if str(item).strip()]
        self.adjacent_title_markers = [str(item).strip().lower() for item in title_rescue_cfg.get("adjacent_title_markers", []) if str(item).strip()]
        self.analyst_variant_markers = [str(item).strip().lower() for item in title_rescue_cfg.get("analyst_variant_markers", []) if str(item).strip()]
        self.adjacent_title_auto_rescue_patterns = [str(item).strip().lower() for item in title_rescue_cfg.get("adjacent_title_auto_rescue_patterns", []) if str(item).strip()]

        # Pre-compiled regex patterns — built once, reused for every job scored.
        self._compiled_title_weights = self._compile_weighted_terms(self.title_positive_weights)
        self._compiled_body_positive = self._compile_weighted_terms(self.body_positive)
        self._compiled_body_negative = self._compile_weighted_terms(self.body_negative)
        self._compiled_negative_titles = [
            (re.compile(self._kw_pattern(str(term).strip().lower()), re.IGNORECASE), str(term).strip().lower())
            for term in self.negative_disqualifiers
            if str(term).strip()
        ]
        self._targets_engineer_family = any(re.search(r"\b(engineer|developer)\b", term) for term in self._positive_title_terms)
        self._targets_developer_family = self._targets_engineer_family
        self._positive_role_family_exemptions = (
            "product manager",
            "architect",
            "analyst",
            "consultant",
            "specialist",
        )

        self.partial_title_markers = {
            "architect": 5,
            "owner": 3,
            "platform": 3,
            "api": 3,
            "integration": 4,
            "integrations": 4,
            "consultant": 4,
            "business systems": 5,
            "systems analyst": 5,
            "solution": 3,
            "solutions": 3,
        }
        self._non_us_remote_markers = {
            "australia", "canada", "united kingdom", "uk", "europe", "germany", "france",
            "ireland", "singapore", "india", "new zealand", "japan", "netherlands",
        }
        self._onsite_markers = {"onsite", "on-site", "on site", "in office", "office-based", "office based"}
        self._hybrid_markers = {"hybrid", "hybrid remote", "hybrid schedule", "remote/hybrid", "hybrid role"}

    # Aggregator-owned domains that do NOT constitute a canonical employer URL.
    # A JobSpy job whose URL points to one of these is still "unverified"; one
    # pointing elsewhere (e.g. bankofamerica.com) has a real canonical URL.
    _AGGREGATOR_DOMAINS = frozenset({
        "google.com", "linkedin.com", "indeed.com", "glassdoor.com",
        "ziprecruiter.com", "jobget.com", "upwork.com", "simplyhired.com",
        "careerbuilder.com", "monster.com", "dice.com",
    })

    def _source_trust_key(self, job_data: Dict[str, Any]) -> str:
        source_lane = str(job_data.get("source_lane") or "employer_ats").strip().lower() or "employer_ats"
        if source_lane == "jobspy_experimental":
            # If the job URL points to an employer domain (not a job aggregator),
            # treat it as having a canonical URL — waive the full jobspy penalty.
            url = str(job_data.get("url") or job_data.get("canonical_job_url") or "").strip().lower()
            if url:
                from urllib.parse import urlparse
                try:
                    host = urlparse(url).netloc.lstrip("www.")
                    if not any(host == d or host.endswith("." + d) for d in self._AGGREGATOR_DOMAINS):
                        return "aggregator_with_canonical"
                except Exception:
                    pass
            return "jobspy_experimental"
        if source_lane == "aggregator":
            canonical = str(job_data.get("canonical_job_url") or "").strip()
            return "aggregator_with_canonical" if canonical else "aggregator_without_canonical"
        if source_lane == "contractor":
            return "contractor"
        return "employer_ats"

    def _source_trust_policy(self, job_data: Dict[str, Any]) -> Dict[str, Any]:
        key = self._source_trust_key(job_data)
        policy = self.source_trust_cfg.get(key, {}) or {}
        return {
            "key": key,
            "score_penalty": int(policy.get("score_penalty", 0) or 0),
            "apply_now_eligible": bool(policy.get("apply_now_eligible", True)),
            "cap_bucket": str(policy.get("cap_bucket") or "").strip().upper() or None,
        }

    @staticmethod
    def _pref_weight_pairs(raw: Any, default: List[Tuple[str, int]]) -> List[Tuple[str, int]]:
        pairs: List[Tuple[str, int]] = []
        if isinstance(raw, dict):
            for key, value in raw.items():
                phrase = str(key or "").strip().lower()
                if not phrase:
                    continue
                try:
                    pairs.append((phrase, int(float(value))))
                except (ValueError, TypeError):
                    continue
        elif isinstance(raw, list):
            for item in raw:
                if isinstance(item, str):
                    pairs.append((item.lower().strip(), 5))
                elif isinstance(item, dict):
                    phrase = str(item.get("keyword") or item.get("phrase") or "").strip().lower()
                    if not phrase:
                        continue
                    pairs.append((phrase, int(float(item.get("weight") or item.get("points") or 5))))
        return pairs or default

    @staticmethod
    def _kw_pattern(keyword: str) -> str:
        """Return the regex pattern string for a keyword."""
        normalized = re.sub(r"[-_/]+", " ", keyword)
        normalized = re.sub(r"\s+", " ", normalized).strip()
        # Treat common separators in the job text (spaces, hyphens, underscores, slashes)
        # as equivalent word boundaries for multi-word terms.
        escaped = re.escape(normalized).replace(r"\ ", r"[\s\-_\/]+")
        # Improve hit-rate for common plural forms on multi-word phrases
        # (e.g., "data platform" should match "data platforms").
        if " " in normalized and re.search(r"[a-zA-Z]$", normalized) and not normalized.endswith("s"):
            escaped = escaped + "s?"
        if re.match(r"^\w", normalized) and re.search(r"\w$", normalized):
            return rf"\b{escaped}\b"
        return rf"(?<!\w){escaped}(?!\w)"

    @staticmethod
    def _compile_weighted_terms(pairs: List[Tuple[str, int]]):
        """Pre-compile patterns for a list of (keyword, points) pairs."""
        compiled = []
        for term, pts in pairs:
            try:
                compiled.append((re.compile(Scorer._kw_pattern(term), re.IGNORECASE), term, pts))
            except re.error:
                pass
        return compiled

    def _match_compiled(self, blob: str, compiled_terms) -> List[Tuple[str, int]]:
        """Match pre-compiled patterns against blob; returns [(term, pts), ...]."""
        if not blob:
            return []
        haystack = self.clean(blob).lower()
        return [(term, pts) for pattern, term, pts in compiled_terms if pattern.search(haystack)]

    @staticmethod
    def clean(value: Optional[str]) -> str:
        if not value:
            return ""

        s = str(value)

        # Job descriptions often contain HTML and entities (e.g., "&nbsp;"). We normalize
        # lightly here to improve keyword match coverage without requiring full parsing.
        s = html.unescape(s)
        s = re.sub(r"<[^>]+>", " ", s)

        return re.sub(r"\s+", " ", s).strip()

    def _title_constraint_penalties(self, title_l: str) -> Tuple[int, List[str]]:
        """Apply preference-specified title constraints as a penalty (not a hard drop).

        This keeps borderline roles visible while preventing broad, generic titles
        (e.g., "Product Manager") from scoring too highly unless domain keywords support them.
        """
        if not title_l:
            return 0, []

        constraints = self.title_constraints or {}
        flags: List[str] = []
        penalty = 0

        def _has_any_modifier(mods: List[str]) -> bool:
            return any(str(m).strip().lower() in title_l for m in mods if str(m).strip())

        # Penalty sizes. Larger than the legacy 12pt default so that unmodified PM/BA/Architect
        # titles can't reach REVIEW TODAY without compensating domain body-signal.
        # Wrong-domain hard-drops (e.g. healthcare PM, fleet PM) are handled separately
        # via negative_disqualifiers in the title config — these are soft penalties only.
        missing_modifier_penalty = 20
        program_manager_penalty = 12

        if constraints.get("product_manager_requires_modifier") and "product manager" in title_l:
            allowed = constraints.get("product_manager_allowed_modifiers", [])
            if not _has_any_modifier(allowed):
                penalty += missing_modifier_penalty
                flags.append("title_constraint:product_manager_missing_modifier")

        if constraints.get("business_analyst_requires_modifier") and "business analyst" in title_l:
            allowed = constraints.get("business_analyst_allowed_modifiers", [])
            if not _has_any_modifier(allowed):
                penalty += missing_modifier_penalty
                flags.append("title_constraint:business_analyst_missing_modifier")

        if constraints.get("architect_requires_modifier") and "architect" in title_l:
            allowed = constraints.get("architect_allowed_modifiers", [])
            if not _has_any_modifier(allowed):
                penalty += missing_modifier_penalty
                flags.append("title_constraint:architect_missing_modifier")

        if constraints.get("consultant_requires_modifier") and "consultant" in title_l:
            allowed = constraints.get("consultant_allowed_modifiers", [])
            if not _has_any_modifier(allowed):
                penalty += missing_modifier_penalty
                flags.append("title_constraint:consultant_missing_modifier")

        if constraints.get("deprioritize_program_manager") and "program manager" in title_l:
            allowed = constraints.get("program_manager_allowed_modifiers", [])
            if not _has_any_modifier(allowed):
                penalty += program_manager_penalty
                flags.append("title_constraint:program_manager_missing_modifier")

        return penalty, flags

    @staticmethod
    def _to_float(value: Any) -> Optional[float]:
        if value in (None, ""):
            return None
        try:
            return float(value)
        except (TypeError, ValueError):
            return None

    def _kw_match(self, kw: str, blob: str) -> bool:
        if not kw or not blob:
            return False
        keyword = self.clean(kw).lower().strip()
        haystack = self.clean(blob).lower()
        if not keyword or not haystack:
            return False

        escaped = re.escape(keyword).replace(r"\ ", r"\s+")
        normalized = re.sub(r"[-_/]+", " ", keyword)
        normalized = re.sub(r"\s+", " ", normalized).strip()
        escaped = re.escape(normalized).replace(r"\ ", r"[\s\-_\/]+")
        if " " in normalized and re.search(r"[a-zA-Z]$", normalized) and not normalized.endswith("s"):
            escaped = escaped + "s?"
        if re.match(r"^\w", normalized) and re.search(r"\w$", normalized):
            pattern = rf"\b{escaped}\b"
        else:
            pattern = rf"(?<!\w){escaped}(?!\w)"
        return re.search(pattern, haystack, flags=re.IGNORECASE) is not None

    def _unique_weighted_hits(self, blob: str, weighted_terms: List[Tuple[str, int]]) -> List[Tuple[str, int]]:
        return [(term, pts) for term, pts in weighted_terms if self._kw_match(term, blob)]

    @staticmethod
    def fit_band(score: float) -> str:
        if score >= 85:
            return "Strong Match"
        if score >= 70:
            return "Good Match"
        if score >= 50:
            return "Fair Match"
        if score >= 35:
            return "Weak Match"
        return "Poor Match"

    def _disqualifier_reason(self, title: str) -> Optional[str]:
        title_l = self.clean(title).lower()
        if not title_l:
            return None

        for pattern, label in self._compiled_negative_titles:
            if pattern.search(title_l):
                return label

        has_positive_family_exemption = any(marker in title_l for marker in self._positive_role_family_exemptions)
        if not self._targets_engineer_family and not has_positive_family_exemption:
            if re.search(r"\bengineer(?:ing)?\b", title_l):
                return "engineer family"
        if not self._targets_developer_family and not has_positive_family_exemption:
            if re.search(r"\bdeveloper\b", title_l):
                return "developer family"
        return None

    def is_disqualified(self, title: str) -> bool:
        return self._disqualifier_reason(title) is not None

    @staticmethod
    def _title_token_count(title: str) -> int:
        return len(re.findall(r"[a-z0-9]+", title.lower()))

    @staticmethod
    def _strip_seniority_modifiers(phrase: str) -> str:
        tokens = [token for token in phrase.lower().split() if token]
        prefix_modifiers = {
            "senior",
            "sr",
            "principal",
            "staff",
            "lead",
            "head",
            "director",
            "vp",
            "vice",
            "president",
        }
        while tokens and tokens[0] in prefix_modifiers:
            tokens = tokens[1:]
        return " ".join(tokens)

    def _has_strong_body_domain_signal(self, jd_blob: str, body_positive_points: float) -> bool:
        if body_positive_points >= self.adjacent_title_min_score_to_keep:
            return True
        return any(self._kw_match(marker, jd_blob) for marker in self.strong_body_domain_markers)

    def _is_title_aligned(
        self,
        title_l: str,
        jd_blob: str,
        title_hits: List[Tuple[str, int]],
        partial_hits: List[Tuple[str, int]],
        body_positive_points: float,
    ) -> bool:
        if title_hits:
            return True
        if self._title_token_count(title_l) < 2:
            return False
        if not self._has_strong_body_domain_signal(jd_blob, body_positive_points):
            return False
        if any(marker in title_l for marker in self.adjacent_title_auto_rescue_patterns):
            return True
        if any(marker in title_l for marker in self.adjacent_title_markers):
            return True
        if any(marker in title_l for marker in self.analyst_variant_markers):
            return True

        return any(label.startswith("title:") for label, _ in partial_hits)

    def _partial_title_hits(self, title: str, exact_hits: List[Tuple[str, int]]) -> List[Tuple[str, int]]:
        if exact_hits:
            return []
        hits: List[Tuple[str, int]] = []
        seen = set()

        for phrase in self.title_positive_keywords:
            normalized_phrase = self._strip_seniority_modifiers(phrase)
            if not normalized_phrase:
                continue

            # Keep partial-title rescue phrase-aware: matching a target role family
            # requires the same family to appear in the title.
            if "product manager" in normalized_phrase and "product manager" not in title:
                continue
            if "architect" in normalized_phrase and "architect" not in title:
                continue
            if "consultant" in normalized_phrase and "consultant" not in title:
                continue
            if "analyst" in normalized_phrase and normalized_phrase not in title:
                continue

            if normalized_phrase in title:
                label = f"title:{phrase}"
                if label not in seen:
                    hits.append((label, 3))
                    seen.add(label)

        for marker, points in self.partial_title_markers.items():
            if marker in title and marker not in seen:
                hits.append((marker, points))
                seen.add(marker)

        return hits[:4]

    def _is_international_remote(self, location: str) -> bool:
        location_l = self.clean(location).lower()
        if "remote" not in location_l:
            return False
        return any(marker in location_l for marker in self._non_us_remote_markers)

    def _location_blob(self, location: str, description: str) -> str:
        return self.clean(" ".join([location or "", description or ""])).lower()

    def _is_remote_role(self, location: str, description: str, is_remote: Any = None) -> bool:
        if isinstance(is_remote, bool):
            return is_remote
        location_l = self.clean(location).lower()
        if "remote" in location_l:
            return True
        # If location explicitly says onsite, don't look for remote keywords in description boilerplate
        if any(marker in location_l for marker in self._onsite_markers):
            return False
        
        # Check for remote in description but ONLY if it looks like a primary designation, 
        # not just mentioned in a list of office locations.
        blob = self.clean(description).lower()
        # Look for explicit "Remote - US" or "Remote, United States" or "This is a remote position"
        # Using word boundaries to avoid catching substrings in longer words
        if re.search(r"\bthis is a remote (position|role)\b", blob, flags=re.IGNORECASE):
            return True
        if re.search(r"\bremote\s*-\s*u\.?s\.?\b", blob, flags=re.IGNORECASE):
            return True
        if re.search(r"\bremote\s*,\s*united states\b", blob, flags=re.IGNORECASE):
            return True
        
        return False

    def _is_hybrid_role(self, location: str, description: str) -> bool:
        location_l = self.clean(location).lower()
        if any(marker in location_l for marker in self._hybrid_markers):
            return True
        if any(marker in location_l for marker in self._onsite_markers):
            return False
        # Do not check description for hybrid - too much boilerplate noise
        return False

    def _is_onsite_role(self, location: str, description: str) -> bool:
        location_l = self.clean(location).lower()
        if any(marker in location_l for marker in self._onsite_markers):
            return True
        # If it has a location but not remote/hybrid, it's onsite by default
        if location_l and not "remote" in location_l and not any(m in location_l for m in self._hybrid_markers):
            return True
        return False

    def _matches_local_area(self, location: str, description: str) -> bool:
        # STRICT: Only check the location field for local markers.
        # Checking description causes too many false positives from "Our offices in Austin..." boilerplate.
        location_l = self.clean(location).lower()
        if not location_l:
            return False
        return any(marker in location_l for marker in self.local_hybrid_markers)

    def _extract_required_years_experience(self, title: str, description: str) -> Optional[float]:
        """Extract minimum required years of experience from job posting."""
        # Comprehensive patterns for experience extraction
        text = f"{title} {description}".lower()
        patterns = [
            # "X+ years", "X+ years of experience"
            r"(\d+)\s*\+\s*years?(?:\s+(?:of\s+)?(?:experience|exp|working|managing|developing))?",
            # "at least X years"
            r"at\s+least\s+(\d+)\s+years?",
            # "minimum X years"
            r"minimum\s+(\d+)\s+years?",
            # "X years of experience/exp/development"
            r"(\d+)\s+years?\s+of\s+(?:experience|exp|development)",
            # "X years experience" (no "of")
            r"(\d+)\s+years?\s+(?:experience|exp)",
            # "X years working/managing/developing"
            r"(\d+)\s+years?\s+(?:working|managing|developing|building)",
            # "require X years", "need X years"
            r"(?:require|need|requires|needs)\s+(?:at\s+least\s+)?(\d+)\s+years?",
        ]

        matches = []
        for pattern in patterns:
            for match in re.finditer(pattern, text):
                try:
                    years = float(match.group(1))
                    matches.append(years)
                except (ValueError, IndexError):
                    pass

        # Return the maximum (most demanding) requirement found
        return max(matches) if matches else None

    def _check_experience_fit(self, title: str, description: str) -> Tuple[bool, Optional[str]]:
        """
        Check if job's experience requirement fits user's profile.
        Returns (passes_check, penalty_reason).
        """
        if self.user_years_experience <= 0:
            # User hasn't set their experience level, skip check
            return True, None

        required_years = self._extract_required_years_experience(title, description)
        if required_years is None:
            # No experience requirement found, allow
            return True, None

        gap = required_years - self.user_years_experience
        if gap <= 0:
            # User has enough or more experience
            return True, None

        # User is short on experience
        if gap <= self.experience_gap_tolerance:
            # Within tolerance — apply soft penalty later in scoring
            return True, f"Soft penalty: {gap:.1f} years below requirement"
        else:
            # Beyond tolerance — hard drop
            return False, f"Experience gap: {gap:.1f} years beyond tolerance ({self.experience_gap_tolerance} years)"

    def _derive_work_type(self, job_data: Dict[str, Any]) -> str:
        explicit = self.clean(job_data.get("work_type")).lower()
        if explicit:
            return explicit
        blob = " ".join(
            [
                self.clean(job_data.get("title")),
                self.clean(job_data.get("description")),
                self.clean(job_data.get("salary_text")),
            ]
        ).lower()
        if re.search(r"\b(internship|intern|co-op|co op)\b", blob, flags=re.IGNORECASE):
            return "internship"
        if re.search(r"\b(part-time|part time)\b", blob, flags=re.IGNORECASE):
            return "part_time"
        if re.search(r"\b(temporary|temp-to-hire|temp to hire|seasonal)\b", blob, flags=re.IGNORECASE):
            return "temporary"
        if re.search(r"\b(full-time|full time|permanent|regular full time)\b", blob, flags=re.IGNORECASE):
            return "fte"
        if re.search(r"\b(1099|independent contractor|self employed|self-employed)\b", blob, flags=re.IGNORECASE):
            return "1099_contract"
        if re.search(r"\b(c2c|corp-to-corp|corp to corp|contract-to-hire|contract to hire)\b", blob, flags=re.IGNORECASE):
            return "c2c_contract"
        if re.search(r"\b(w2|w-2|hourly contract|contract role|contract position|contract opportunity|contract assignment|contractor role)\b", blob, flags=re.IGNORECASE):
            return "w2_contract"
        if "hourly" in blob:
            return "w2_contract"
        return "fte"

    def _compensation_unit(self, job_data: Dict[str, Any], work_type: str) -> str:
        explicit = self.clean(job_data.get("compensation_unit")).lower()
        if explicit:
            return explicit
        blob = " ".join([self.clean(job_data.get("salary_text")), self.clean(job_data.get("description"))]).lower()
        if any(marker in blob for marker in ["/hr", "per hour", "hourly", "hr rate", "$/hour"]):
            return "hourly"
        if work_type in {"w2_contract", "1099_contract", "c2c_contract"}:
            return "hourly"
        return "salary"

    def _derive_hourly_rate(self, job_data: Dict[str, Any], unit: str) -> Optional[float]:
        explicit = self._to_float(job_data.get("hourly_rate"))
        if explicit is not None:
            return explicit
        if unit != "hourly":
            return None
        salary_min = self._to_float(job_data.get("salary_min"))
        salary_max = self._to_float(job_data.get("salary_max"))
        if salary_min is not None and salary_max is not None:
            return (salary_min + salary_max) / 2.0
        if salary_min is not None:
            return salary_min
        text = " ".join([self.clean(job_data.get("salary_text")), self.clean(job_data.get("description"))]).strip()
        sanitized = re.sub(r"\b(1099|w2|w-2|c2c|corp[-\s]?to[-\s]?corp)\b", " ", text, flags=re.IGNORECASE)
        matches = [float(match.replace(",", "")) for match in re.findall(r"\$?\s*([0-9]{1,3}(?:,[0-9]{3})*(?:\.\d{1,2})?)", sanitized)]
        if not matches:
            return None
        return sum(matches[:2]) / min(2, len(matches))

    def _extract_annual_salary_range(self, text: str) -> tuple[Optional[float], Optional[float]]:
        text = self.clean(text)
        if not text:
            return None, None
        
        # More liberal annual context check. If we see large numbers with $ and k, 
        # we can be reasonably sure it's annual unless "hour" or "day" is nearby.
        is_annual = bool(
            re.search(r"\b(salary|compensation|pay range|base pay|base salary|annual|annually|per year|yearly|ote|on.target)\b", text, flags=re.IGNORECASE)
        )
        is_hourly = bool(re.search(r"\b(hour|hourly|hr|per hour)\b", text, flags=re.IGNORECASE))
        
        # If it looks like annual (large numbers) and doesn't look like hourly, proceed.
        if not is_annual and is_hourly:
            return None, None
            
        raw_matches = re.findall(r"\$?\s*([0-9]{1,3}(?:,\d{3})*(?:\.\d{1,2})?|[0-9]{2,3}(?:\.\d{1,2})?)\s*([kK]?)", text)
        values: list[float] = []
        for amount_raw, k_suffix in raw_matches:
            try:
                amount = float(amount_raw.replace(",", ""))
            except ValueError:
                continue
            if k_suffix:
                amount *= 1000.0
            
            # If it's between 30,000 and 1,000,000, it's likely annual.
            if amount >= 30000:
                values.append(amount)
            # Small numbers with 'k' are also annual (e.g. 150k)
            elif amount >= 30 and amount < 1000 and k_suffix:
                # Already multiplied by 1000 above if k_suffix was found
                values.append(amount)
        
        if not values:
            return None, None
        if len(values) >= 2:
            # Sort to ensure [min, max]
            sorted_vals = sorted(values)
            return sorted_vals[0], sorted_vals[-1]
        return values[0], None

    def _normalize_compensation(self, job_data: Dict[str, Any], work_type: str, unit: str) -> Dict[str, Any]:
        salary_min = self._to_float(job_data.get("salary_min"))
        salary_max = self._to_float(job_data.get("salary_max"))
        if salary_min is None and salary_max is None:
            derived_min, derived_max = self._extract_annual_salary_range(
                " ".join([self.clean(job_data.get("salary_text")), self.clean(job_data.get("description"))]).strip()
            )
            salary_min = derived_min
            salary_max = derived_max
        hourly_rate = self._derive_hourly_rate(job_data, unit)

        hours_per_week = self._to_float(job_data.get("hours_per_week")) or self.default_hours_per_week
        if work_type in {"1099_contract", "c2c_contract"}:
            weeks_per_year = self._to_float(job_data.get("weeks_per_year")) or self.default_1099_weeks_per_year
        elif work_type == "w2_contract":
            weeks_per_year = self._to_float(job_data.get("weeks_per_year")) or self.default_w2_hourly_weeks_per_year
        else:
            weeks_per_year = self._to_float(job_data.get("weeks_per_year")) or 52.0

        gross_annual: Optional[float]
        normalized: Optional[float]
        if unit == "hourly" and hourly_rate is not None:
            gross_annual = hourly_rate * hours_per_week * weeks_per_year
            if work_type in {"1099_contract", "c2c_contract"}:
                normalized = gross_annual * (1.0 - self.overhead_1099_pct) - self.benefits_replacement_usd
            elif work_type == "w2_contract":
                normalized = gross_annual - self.w2_benefits_gap_usd
            else:
                normalized = gross_annual
        else:
            gross_annual = None
            if salary_min is not None and salary_max is not None:
                normalized = (salary_min + salary_max) / 2.0
            elif salary_min is not None:
                normalized = salary_min
            elif salary_max is not None:
                normalized = salary_max
            else:
                normalized = None

        return {
            "work_type": work_type,
            "compensation_unit": unit,
            "hourly_rate": hourly_rate,
            "hours_per_week": hours_per_week if unit == "hourly" else None,
            "weeks_per_year": weeks_per_year if unit == "hourly" else None,
            "gross_annual_compensation_usd": gross_annual,
            "normalized_compensation_usd": normalized,
        }

    def score_job(self, job_data: Dict[str, Any]) -> Dict[str, Any]:
        title = job_data.get("title", "")
        description = job_data.get("description", "")
        company_tier = int(job_data.get("tier", 4) or 4)
        location = str(job_data.get("location", "") or "")

        title_l = self.clean(title).lower()
        jd_blob = self.clean(description).lower()

        disqualifier_reason = self._disqualifier_reason(title)
        if disqualifier_reason:
            source_key = self._source_trust_key(job_data)
            _work_type = self._derive_work_type(job_data)
            _comp_unit = self._compensation_unit(job_data, _work_type)
            return {
                "score": 0.0,
                "fit_band": "Disqualified",
                "matched_keywords": "",
                "penalized_keywords": "Title Disqualified",
                "decision_reason": f"Hard-Drop: disqualifier in title ({disqualifier_reason})",
                "score_components": {
                    "source_trust_key": source_key,
                    "base_score": 0.0,
                    "title_points": 0,
                    "body_positive_points": 0,
                    "body_negative_points": 0,
                    "tier_bonus": 0,
                    "partial_title_bonus": 0,
                    "positive_keyword_gate_bonus": 0,
                    "location_penalty": 0,
                    "compensation_adjustment": 0,
                    "contract_adjustment": 0,
                    "source_penalty": 0,
                },
                "work_type": _work_type,
                "compensation_unit": _comp_unit,
                "hourly_rate": self._derive_hourly_rate(job_data, _comp_unit),
                "hours_per_week": None,
                "weeks_per_year": None,
                "normalized_compensation_usd": None,
            }

        # Check experience fit
        exp_passes_check, exp_penalty_reason = self._check_experience_fit(title, description)
        if not exp_passes_check:
            # Hard-drop: experience gap exceeds tolerance
            source_key = self._source_trust_key(job_data)
            _work_type = self._derive_work_type(job_data)
            _comp_unit = self._compensation_unit(job_data, _work_type)
            return {
                "score": 0.0,
                "fit_band": "Filtered Out",
                "matched_keywords": "",
                "penalized_keywords": "Experience Gap",
                "decision_reason": f"Hard-Drop: {exp_penalty_reason}",
                "score_components": {
                    "source_trust_key": source_key,
                    "base_score": 0.0,
                    "title_points": 0,
                    "body_positive_points": 0,
                    "body_negative_points": 0,
                    "tier_bonus": 0,
                    "partial_title_bonus": 0,
                    "positive_keyword_gate_bonus": 0,
                    "location_penalty": 0,
                    "compensation_adjustment": 0,
                    "contract_adjustment": 0,
                    "source_penalty": 0,
                },
                "work_type": _work_type,
                "compensation_unit": _comp_unit,
                "hourly_rate": self._derive_hourly_rate(job_data, _comp_unit),
                "hours_per_week": None,
                "weeks_per_year": None,
                "normalized_compensation_usd": None,
            }

        # Store experience penalty reason for later application as soft penalty
        exp_soft_penalty = 0
        if exp_penalty_reason:
            # Apply a soft penalty for being under experience requirement
            exp_soft_penalty = -5

        score = 0.0

        title_hits = self._match_compiled(title_l, self._compiled_title_weights)
        title_points = sum(pts for _, pts in title_hits)
        partial_hits = self._partial_title_hits(title_l, title_hits)
        partial_title_points = sum(pts for _, pts in partial_hits)
        title_points += partial_title_points

        base_score = 0.0
        if self.fast_track_base_score > 0 and any(pts >= self.fast_track_min_weight for _, pts in title_hits):
            base_score = self.fast_track_base_score

        title_points = min(title_points, self.title_max_points)
        
        # Use either the fast-track base or the calculated title points, but not both stacked.
        # This prevents title matches from immediately hitting 75+ points.
        score = max(base_score, title_points)

        title_constraint_penalty, title_constraint_flags = self._title_constraint_penalties(title_l)
        if title_constraint_penalty:
            score -= title_constraint_penalty

        jd_pos_hits = self._match_compiled(jd_blob, self._compiled_body_positive)
        jd_neg_hits = self._match_compiled(jd_blob, self._compiled_body_negative)
        body_positive_points = min(sum(pts for _, pts in jd_pos_hits), self.positive_keyword_cap)

        # Negatives are uncapped by default (negative_keyword_cap=0 means unlimited)
        raw_negative_points = sum(pts for _, pts in jd_neg_hits)
        if self.negative_keyword_cap > 0:
            body_negative_points = min(raw_negative_points, self.negative_keyword_cap)
        else:
            body_negative_points = raw_negative_points

        score += body_positive_points
        score -= body_negative_points

        # Domain coherence gate: when negative domain signals heavily outweigh positive
        # signals, the job is in the wrong domain regardless of individual keyword hits.
        # Cap the score so mismatched-domain jobs can't reach APPLY NOW / REVIEW TODAY.
        domain_penalty_cfg = self.scoring_cfg.get("domain_penalty_cap", {})
        if domain_penalty_cfg:
            neg_lead = domain_penalty_cfg.get("neg_points_exceed_pos_by", 0)
            cap_to = domain_penalty_cfg.get("cap_score_to", 100)
            if neg_lead > 0 and body_negative_points > 0:
                if (body_negative_points - body_positive_points) >= neg_lead:
                    score = min(score, cap_to)

        title_aligned = self._is_title_aligned(title_l, jd_blob, title_hits, partial_hits, body_positive_points)
        if self.require_one_positive_keyword and not title_aligned:
            source_key = self._source_trust_key(job_data)
            _work_type = self._derive_work_type(job_data)
            _comp_unit = self._compensation_unit(job_data, _work_type)
            return {
                "score": 0.0,
                "fit_band": "Filtered Out",
                "matched_keywords": ", ".join(sorted(set(term for term, _ in jd_pos_hits))),
                "penalized_keywords": "Weak Title Alignment",
                "decision_reason": "Hard-Drop: title does not match target roles strongly enough",
                "score_components": {
                    "source_trust_key": source_key,
                    "base_score": base_score,
                    "title_points": title_points,
                    "body_positive_points": body_positive_points,
                    "body_negative_points": body_negative_points,
                    "tier_bonus": 0,
                    "partial_title_bonus": partial_title_points,
                    "positive_keyword_gate_bonus": 0,
                    "location_penalty": 0,
                    "compensation_adjustment": 0,
                    "contract_adjustment": 0,
                    "source_penalty": 0,
                    "title_aligned": False,
                },
                "work_type": _work_type,
                "compensation_unit": _comp_unit,
                "hourly_rate": self._derive_hourly_rate(job_data, _comp_unit),
                "hours_per_week": None,
                "weeks_per_year": None,
                "normalized_compensation_usd": None,
            }

        location_penalty = 0
        location_bonus = 0
        tier_bonus_cfg = self.scoring_cfg.get("tier_bonuses", {1: 15, 2: 8, 3: 4})
        tier_bonus = tier_bonus_cfg.get(company_tier, tier_bonus_cfg.get(str(company_tier), 0))
        score += tier_bonus

        positive_keyword_gate_bonus = 0
        if self.require_one_positive_keyword and title_aligned and not title_hits and partial_hits:
            score += 6
            positive_keyword_gate_bonus = 6

        comp_data = self._normalize_compensation(
            job_data,
            self._derive_work_type(job_data),
            self._compensation_unit(job_data, self._derive_work_type(job_data)),
        )
        compensation_adjustment = 0
        contract_adjustment = 0
        source_penalty = 0

        work_type = comp_data["work_type"]
        normalized_comp = comp_data["normalized_compensation_usd"]
        source_trust = self._source_trust_policy(job_data)

        is_remote_role = self._is_remote_role(location, description, job_data.get("is_remote"))
        is_hybrid_role = self._is_hybrid_role(location, description)
        is_onsite_role = self._is_onsite_role(location, description)
        local_match = self._matches_local_area(location, description)

        # Priority resolution for type
        # If it's explicitly onsite, it's NOT remote even if boilerplate says so
        if is_onsite_role:
            is_remote_role = False
        
        if self.us_only and not self.allow_international_remote and self._is_international_remote(location):
            source_key = self._source_trust_key(job_data)
            return {
                "score": 0.0,
                "fit_band": "Filtered Out",
                "matched_keywords": ", ".join(sorted(set(term for term, _ in title_hits + partial_hits + jd_pos_hits))),
                "penalized_keywords": "international remote",
                "decision_reason": "Hard-Drop: location mismatch (international remote)",
                "score_components": {
                    "source_trust_key": source_key,
                    "base_score": base_score,
                    "title_points": title_points,
                    "body_positive_points": body_positive_points,
                    "body_negative_points": body_negative_points,
                    "tier_bonus": tier_bonus,
                    "partial_title_bonus": partial_title_points,
                    "positive_keyword_gate_bonus": positive_keyword_gate_bonus,
                    "location_penalty": 100,
                    "location_bonus": 0,
                    "compensation_adjustment": 0,
                    "contract_adjustment": 0,
                    "source_penalty": 0,
                },
                **comp_data,
            }

        if is_remote_role:
            if self.remote_us_enabled and self.location_policy in {"remote_only", "remote_or_hybrid"}:
                location_bonus += self.remote_us_bonus
            else:
                # If remote is not allowed by policy
                source_key = self._source_trust_key(job_data)
                return {
                    "score": 0.0,
                    "fit_band": "Filtered Out",
                    "matched_keywords": ", ".join(sorted(set(term for term, _ in title_hits + partial_hits + jd_pos_hits))),
                    "penalized_keywords": "location mismatch (remote policy)",
                    "decision_reason": "Hard-Drop: location mismatch (remote policy)",
                    "score_components": {
                        "source_trust_key": source_key,
                        "base_score": base_score,
                        "title_points": title_points,
                        "body_positive_points": body_positive_points,
                        "body_negative_points": body_negative_points,
                        "tier_bonus": tier_bonus,
                        "partial_title_bonus": partial_title_points,
                        "positive_keyword_gate_bonus": positive_keyword_gate_bonus,
                        "location_penalty": 100,
                        "location_bonus": 0,
                        "compensation_adjustment": 0,
                        "contract_adjustment": 0,
                        "source_penalty": 0,
                    },
                    **comp_data,
                }
        elif location.strip():
            # If there's a location but it's not remote, it MUST be a local hybrid match
            local_hybrid_salary_ok = (
                normalized_comp is None # Allow if salary is missing but location matches
                or normalized_comp >= self.local_hybrid_salary_floor
            )
            local_hybrid_allowed = (
                self.local_hybrid_enabled
                and self.location_policy in {"remote_only", "hybrid_only", "remote_or_hybrid"}
                and local_match
                and local_hybrid_salary_ok
            )
            
            if local_hybrid_allowed:
                location_bonus += self.local_hybrid_bonus
            else:
                # This is the hard-drop for on-site or hybrid roles in the wrong city
                return {
                    "score": 0.0,
                    "fit_band": "Filtered Out",
                    "matched_keywords": ", ".join(sorted(set(term for term, _ in title_hits + partial_hits + jd_pos_hits))),
                    "penalized_keywords": "location mismatch",
                    "decision_reason": f"Hard-Drop: location mismatch ({location})",
                    "score_components": {
                        "base_score": base_score,
                        "title_points": title_points,
                        "body_positive_points": body_positive_points,
                        "body_negative_points": body_negative_points,
                        "tier_bonus": tier_bonus,
                        "partial_title_bonus": partial_title_points,
                        "positive_keyword_gate_bonus": positive_keyword_gate_bonus,
                        "location_penalty": 100,
                        "location_bonus": 0,
                        "compensation_adjustment": 0,
                        "contract_adjustment": 0,
                    },
                    **comp_data,
                }
        # If no location text at all, we proceed but don't give a bonus

        score += location_bonus

        if work_type in {"w2_contract", "1099_contract", "c2c_contract"}:
            if not self.include_contract_roles:
                contract_adjustment -= 20
            if work_type == "w2_contract" and not self.allow_w2_hourly:
                contract_adjustment -= 20
            if work_type in {"1099_contract", "c2c_contract"} and not self.allow_1099_hourly:
                contract_adjustment -= 20
            if self.contract_role_penalty:
                contract_adjustment -= self.contract_role_penalty
            if normalized_comp is not None and normalized_comp >= self.target_salary_usd:
                contract_adjustment += self.contractor_target_bonus

        score += contract_adjustment

        if self.enforce_min_salary and normalized_comp is not None and normalized_comp < self.min_salary_usd:
            return {
                "score": 0.0,
                "fit_band": "Filtered Out",
                "matched_keywords": ", ".join(sorted(set(term for term, _ in title_hits + partial_hits + jd_pos_hits))),
                "penalized_keywords": "Salary Below Floor",
                "decision_reason": (
                    f"Hard-Drop: compensation below minimum requirement "
                    f"(${normalized_comp:,.0f} < ${self.min_salary_usd:,.0f})"
                ),
                "score_components": {
                    "source_trust_key": source_trust.get("key"),
                    "base_score": base_score,
                    "title_points": title_points,
                    "body_positive_points": body_positive_points,
                    "body_negative_points": body_negative_points,
                    "tier_bonus": tier_bonus,
                    "partial_title_bonus": partial_title_points,
                    "positive_keyword_gate_bonus": positive_keyword_gate_bonus,
                    "title_constraint_penalty": title_constraint_penalty,
                    "location_penalty": location_penalty,
                    "location_bonus": location_bonus,
                    "compensation_adjustment": 0,
                    "contract_adjustment": contract_adjustment,
                    "source_penalty": 0,
                    "exp_soft_penalty": exp_soft_penalty,
                    "title_aligned": title_aligned,
                },
                **comp_data,
            }

        if normalized_comp is None:
            if not self.allow_missing_salary:
                compensation_adjustment -= self.missing_salary_penalty
        else:
            if normalized_comp >= self.target_salary_usd:
                compensation_adjustment += self.salary_at_or_above_target_bonus
            elif normalized_comp >= self.min_salary_usd:
                compensation_adjustment += self.salary_meets_floor_bonus
            else:
                compensation_adjustment -= self.salary_below_target_penalty

        score += compensation_adjustment

        # Apply source-based trust policy
        source_penalty = source_trust.get("score_penalty", 0)
        score -= source_penalty

        # Experience soft penalty
        score += exp_soft_penalty

        # Freshness adjustment (configurable; does not override hard-drops)
        freshness_cfg = (self.scoring_cfg.get("freshness") or {}) if hasattr(self, "scoring_cfg") else {}
        if freshness_cfg.get("enabled", False):
            freshness_pts = self._freshness_adjustment(job_data.get("date_discovered"), freshness_cfg)
            score += freshness_pts
        else:
            freshness_pts = 0

        # Final score bounding
        score = max(0.0, min(100.0, score))

        # Action Bucket Logic (internal flags for the dashboard)
        apply_now_eligible = source_trust.get("apply_now_eligible", True)
        if title_constraint_penalty:
            apply_now_eligible = False
        if self.apply_now_cfg.get("require_strong_title", True):
            # Check for strong title markers if enabled
            direct_markers = self.apply_now_cfg.get("direct_title_markers", [])
            has_direct = any(m.lower() in title_l for m in direct_markers) if direct_markers else True
            # Also check if title points alone meet a minimum (e.g. 6.0)
            if title_points < self.apply_now_cfg.get("min_role_alignment", 6.0) and not has_direct:
                apply_now_eligible = False

        fit_band = self.fit_band(score)
        
        # Apply hard-gate: if salary is known and below floor, it can never be APPLY NOW
        if normalized_comp is not None and normalized_comp < self.min_salary_usd:
            apply_now_eligible = False

        # Build decision reason
        reasons = []
        if base_score > 0: reasons.append(f"Title Fast-Track +{base_score} ({[t for t,p in title_hits if p >= self.fast_track_min_weight][0]})")
        elif title_points > 0: reasons.append(f"Title Match +{title_points} ({', '.join(sorted(set(t for t,p in title_hits + partial_hits)))})")
        
        if body_positive_points > 0: reasons.append(f"JD Keywords +{body_positive_points} ({', '.join(sorted(set(t for t,p in jd_pos_hits)))})")
        if body_negative_points > 0: reasons.append(f"Negative JD -{body_negative_points} ({', '.join(sorted(set(term for term, _ in jd_neg_hits)))})")
        if title_constraint_penalty > 0: reasons.append(f"Title Constraint -{title_constraint_penalty}")
         
        if location_bonus > 0: reasons.append(f"Location Bonus +{location_bonus}")
        if tier_bonus > 0: reasons.append(f"Tier {company_tier} Bonus +{tier_bonus}")
        if contract_adjustment != 0: reasons.append(f"Contract Adj {contract_adjustment:+}")
        if compensation_adjustment != 0: reasons.append(f"Comp Adj {compensation_adjustment:+}")
        if source_penalty > 0: reasons.append(f"Source Penalty -{source_penalty}")
        if exp_soft_penalty < 0: reasons.append(f"Exp Penalty {exp_soft_penalty} ({exp_penalty_reason})")
        
        decision_reason = "; ".join(reasons)

        score_result = {
            "score": score,
            "fit_band": fit_band,
            "matched_keywords": ", ".join(sorted(set(term for term, _ in title_hits + partial_hits + jd_pos_hits))),
            "penalized_keywords": ", ".join(sorted(set([term for term, _ in jd_neg_hits] + title_constraint_flags))),
            "decision_reason": decision_reason,
            "apply_now_eligible": apply_now_eligible,
            "score_components": {
                "source_trust_key": source_trust.get("key"),
                "base_score": base_score,
                "title_points": title_points,
                "body_positive_points": body_positive_points,
                "body_negative_points": body_negative_points,
                "tier_bonus": tier_bonus,
                "partial_title_bonus": partial_title_points,
                "positive_keyword_gate_bonus": positive_keyword_gate_bonus,
                "title_constraint_penalty": title_constraint_penalty,
                "location_penalty": location_penalty,
                "location_bonus": location_bonus,
                "compensation_adjustment": compensation_adjustment,
                "contract_adjustment": contract_adjustment,
                # Store the signed delta for downstream explanations (negative = penalty).
                "source_penalty": -source_penalty,
                "exp_soft_penalty": exp_soft_penalty,
                "freshness_adjustment": freshness_pts,
                "title_aligned": title_aligned,
            },
            **comp_data,
        }

        return score_result

    @staticmethod
    def _freshness_adjustment(date_discovered: Optional[str], freshness_cfg: Optional[Dict[str, Any]] = None) -> int:
        """Return a score adjustment based on how many days ago the job was discovered.

        Default thresholds (overridable via scoring.freshness config):
          Age < 3 days:   +4
          Age 3–14 days:   0
          Age 14–30 days: -6
          Age > 30 days: -14
          Age unknown:     0
        """
        if not date_discovered:
            return 0
        cfg = freshness_cfg or {}
        thresholds = cfg.get("thresholds") or {}
        adjustments = cfg.get("adjustments") or {}
        new_days = int(thresholds.get("new_days", 3))
        neutral_days = int(thresholds.get("neutral_days", 14))
        stale_days = int(thresholds.get("stale_days", 30))
        adj_new = int(adjustments.get("new", 4))
        adj_aging = int(adjustments.get("aging", -6))
        adj_stale = int(adjustments.get("stale", -14))

        try:
            # Accept ISO8601 or SQLite datetime format
            dt_str = str(date_discovered).strip().replace(" ", "T")
            if "+" not in dt_str and not dt_str.endswith("Z"):
                dt_str += "Z"
            discovered = datetime.fromisoformat(dt_str.replace("Z", "+00:00"))
            age_days = (datetime.now(timezone.utc) - discovered).days
        except (ValueError, TypeError):
            return 0

        if age_days < new_days:
            return adj_new
        if age_days <= neutral_days:
            return 0
        if age_days <= stale_days:
            return adj_aging
        return adj_stale

    def apply_enrichment_adjustments(
        self, score_result: Dict[str, Any], enriched_data: Optional[Dict[str, Any]]
    ) -> Dict[str, Any]:
        """
        Applies AI-based enrichment adjustments to a scoring result.
        Adjusts score based on visa sponsorship, tech stack match, and IC vs Manager preference.

        Args:
            score_result: The result dict from score_job()
            enriched_data: Dict with keys: visa_sponsor (bool|None), tech_stack (list), ic_vs_manager (str|None)

        Returns:
            Updated score_result with enrichment adjustments applied
        """
        if not enriched_data or enriched_data.get("enrichment_status") != "success":
            return score_result

        enrichment_adjustment = 0
        enrichment_reasons = []

        # Check visa sponsorship
        visa_pref = self.prefs.get("requirements", {}).get("visa_sponsorship_required", False)
        visa_sponsor = enriched_data.get("visa_sponsor")

        if visa_pref and visa_sponsor is True:
            enrichment_adjustment += 8
            enrichment_reasons.append("+8 visa_sponsor_match")
        elif visa_pref and visa_sponsor is False:
            # Hard-drop: if sponsorship is required but NOT provided
            score_result = dict(score_result)
            score_result["score"] = 0.0
            score_result["fit_band"] = "Filtered Out"
            score_result["decision_reason"] = (
                f"{score_result.get('decision_reason', '')} [Hard-Drop: No visa sponsorship provided]"
            )
            return score_result

        # Check tech stack match
        tech_stack = enriched_data.get("tech_stack", [])
        preferred_techs = self.prefs.get("requirements", {}).get("preferred_tech_stack", [])

        if preferred_techs and tech_stack:
            preferred_lower = [t.lower() for t in preferred_techs]
            tech_lower = [t.lower() for t in tech_stack]
            matches = len([t for t in tech_lower if any(p in t for p in preferred_lower)])
            if matches > 0:
                tech_bonus = min(matches * 3, 12)  # Max 12 points for tech stack
                enrichment_adjustment += tech_bonus
                enrichment_reasons.append(f"+{tech_bonus} tech_stack_match({matches})")
            else:
                enrichment_adjustment -= 5
                enrichment_reasons.append("-5 tech_stack_mismatch")

        # Check IC vs Manager preference
        ic_vs_manager_pref = self.prefs.get("requirements", {}).get("role_type", "any")
        ic_vs_manager = enriched_data.get("ic_vs_manager")

        if ic_vs_manager_pref == "individual_contributor" and ic_vs_manager == "individual_contributor":
            enrichment_adjustment += 6
            enrichment_reasons.append("+6 ic_preference_match")
        elif ic_vs_manager_pref == "manager" and ic_vs_manager == "manager":
            enrichment_adjustment += 6
            enrichment_reasons.append("+6 manager_preference_match")
        elif ic_vs_manager_pref != "any" and ic_vs_manager == "mixed":
            enrichment_adjustment -= 3
            enrichment_reasons.append("-3 mixed_role_preference")

        # Apply adjustment to score
        if enrichment_adjustment != 0:
            original_score = score_result.get("score", 0.0)
            adjusted_score = max(0.0, min(100.0, original_score + enrichment_adjustment))
            score_result = dict(score_result)
            score_result["score"] = adjusted_score
            score_result["fit_band"] = self.fit_band(adjusted_score)

            # Update decision reason with enrichment info
            enrichment_str = "; ".join(enrichment_reasons)
            score_result["decision_reason"] = (
                f"{score_result.get('decision_reason', '')} [enrichment: {enrichment_str}]"
            )

            # Store enrichment details
            score_result["enrichment_adjustment"] = enrichment_adjustment
            score_result["enrichment_reasons"] = enrichment_reasons

        return score_result
