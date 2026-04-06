"""Refined scoring logic including contractor normalization."""

from __future__ import annotations

import logging
import re
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
        self.fast_track_base_score = float(title_cfg.get("fast_track_base_score", 0))
        self.fast_track_min_weight = int(title_cfg.get("fast_track_min_weight", 8))
        self.title_max_points = int(title_cfg.get("title_max_points", 25))

        kw_cfg = self.prefs.get("keywords", {})
        self.body_positive = self._pref_weight_pairs(kw_cfg.get("body_positive"), [])
        self.body_negative = self._pref_weight_pairs(kw_cfg.get("body_negative"), [])

        scoring_cfg = self.prefs.get("scoring", {})
        keyword_matching_cfg = scoring_cfg.get("keyword_matching", {})
        self.min_score_to_keep = scoring_cfg.get("minimum_score_to_keep", 35)
        self.source_trust_cfg = scoring_cfg.get("source_trust", {})
        self.positive_keyword_cap = int(keyword_matching_cfg.get("positive_keyword_cap", 60))
        self.negative_keyword_cap = int(keyword_matching_cfg.get("negative_keyword_cap", 45))
        adjustments_cfg = scoring_cfg.get("adjustments", {})
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
        remote_us_cfg = location_pref_cfg.get("remote_us", {})
        local_hybrid_cfg = location_pref_cfg.get("local_hybrid", {})

        self.target_salary_usd = float(comp_cfg.get("target_salary_usd", 165000))
        self.min_salary_usd = float(comp_cfg.get("min_salary_usd", 165000))
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

        # Pre-compiled regex patterns — built once, reused for every job scored.
        self._compiled_title_weights = self._compile_weighted_terms(self.title_positive_weights)
        self._compiled_body_positive = self._compile_weighted_terms(self.body_positive)
        self._compiled_body_negative = self._compile_weighted_terms(self.body_negative)

        self.partial_title_markers = {
            "architect": 5,
            "product": 3,
            "owner": 3,
            "platform": 3,
            "api": 3,
            "integration": 4,
            "integrations": 4,
            "consultant": 4,
            "analyst": 3,
            "business systems": 5,
            "systems analyst": 5,
            "solution": 3,
            "solutions": 3,
            "technical": 2,
            "data": 2,
        }
        self._non_us_remote_markers = {
            "australia", "canada", "united kingdom", "uk", "europe", "germany", "france",
            "ireland", "singapore", "india", "new zealand", "japan", "netherlands",
        }
        self._onsite_markers = {"onsite", "on-site", "on site", "in office", "office-based", "office based"}
        self._hybrid_markers = {"hybrid", "hybrid remote", "hybrid schedule", "remote/hybrid", "hybrid role"}

    def _source_trust_key(self, job_data: Dict[str, Any]) -> str:
        source_lane = str(job_data.get("source_lane") or "employer_ats").strip().lower() or "employer_ats"
        if source_lane == "jobspy_experimental":
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
        escaped = re.escape(keyword).replace(r"\ ", r"\s+")
        if re.match(r"^\w", keyword) and re.search(r"\w$", keyword):
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
        return re.sub(r"\s+", " ", str(value)).strip()

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
        if re.match(r"^\w", keyword) and re.search(r"\w$", keyword):
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

    def is_disqualified(self, title: str) -> bool:
        title_l = title.lower()
        return any(dq.lower() in title_l for dq in self.negative_disqualifiers)

    def _partial_title_hits(self, title: str, exact_hits: List[Tuple[str, int]]) -> List[Tuple[str, int]]:
        if exact_hits:
            return []
        hits: List[Tuple[str, int]] = []
        seen = set()

        for phrase in self.title_positive_keywords:
            tokens = [token for token in phrase.split() if len(token) > 2]
            matched = [token for token in tokens if token in title]
            if len(tokens) >= 2 and len(matched) >= min(2, len(tokens)):
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
        if location_l:
            return "remote" in location_l
        blob = self._location_blob(location, description)
        return "remote" in blob

    def _is_hybrid_role(self, location: str, description: str) -> bool:
        location_l = self.clean(location).lower()
        if location_l:
            return any(marker in location_l for marker in self._hybrid_markers)
        blob = self._location_blob(location, description)
        return any(marker in blob for marker in self._hybrid_markers)

    def _is_onsite_role(self, location: str, description: str) -> bool:
        location_l = self.clean(location).lower()
        if location_l:
            return any(marker in location_l for marker in self._onsite_markers)
        blob = self._location_blob(location, description)
        return any(marker in blob for marker in self._onsite_markers)

    def _matches_local_area(self, location: str, description: str) -> bool:
        location_l = self.clean(location).lower()
        if location_l:
            return any(marker in location_l for marker in self.local_hybrid_markers)
        blob = self._location_blob(location, description)
        if not blob:
            return False
        return any(marker in blob for marker in self.local_hybrid_markers)

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
        if any(marker in blob for marker in ["internship", "co-op", "co op"]) or re.search(r"\bintern\b", blob):
            return "internship"
        if any(marker in blob for marker in ["part-time", "part time"]):
            return "part_time"
        if any(marker in blob for marker in ["temporary", "temp-to-hire", "temp to hire", "seasonal"]):
            return "temporary"
        if any(marker in blob for marker in ["full-time", "full time", "permanent", "regular full time"]):
            return "fte"
        if any(marker in blob for marker in ["1099", "independent contractor", "self employed", "self-employed"]):
            return "1099_contract"
        if any(marker in blob for marker in ["c2c", "corp-to-corp", "corp to corp", "contract-to-hire", "contract to hire"]):
            return "c2c_contract"
        if any(marker in blob for marker in ["w2", "w-2", "hourly contract", "contract role", "contract position", "contract opportunity", "contract assignment", "contractor role"]):
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
        annual_context = bool(
            re.search(r"\b(salary|compensation|pay range|base pay|base salary|annual|annually|per year|yearly|ote|on.target)\b", text, flags=re.IGNORECASE)
        )
        if not annual_context:
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
            if amount >= 1000:
                values.append(amount)
        if not values:
            return None, None
        if len(values) >= 2:
            return values[0], values[1]
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
        if work_type == "1099_contract":
            weeks_per_year = self._to_float(job_data.get("weeks_per_year")) or self.default_1099_weeks_per_year
        elif work_type in {"w2_contract", "c2c_contract"}:
            weeks_per_year = self._to_float(job_data.get("weeks_per_year")) or self.default_w2_hourly_weeks_per_year
        else:
            weeks_per_year = self._to_float(job_data.get("weeks_per_year")) or 52.0

        gross_annual: Optional[float]
        normalized: Optional[float]
        if unit == "hourly" and hourly_rate is not None:
            gross_annual = hourly_rate * hours_per_week * weeks_per_year
            if work_type == "1099_contract":
                normalized = gross_annual * (1.0 - self.overhead_1099_pct) - self.benefits_replacement_usd
            elif work_type in {"w2_contract", "c2c_contract"}:
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

        title_l = title.lower()
        jd_blob = description.lower()

        if self.is_disqualified(title):
            _work_type = self._derive_work_type(job_data)
            _comp_unit = self._compensation_unit(job_data, _work_type)
            return {
                "score": 0.0,
                "fit_band": "Disqualified",
                "matched_keywords": "",
                "penalized_keywords": "Title Disqualified",
                "decision_reason": "Hard-Drop: Disqualifier in title",
                "score_components": {
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
            _work_type = self._derive_work_type(job_data)
            _comp_unit = self._compensation_unit(job_data, _work_type)
            return {
                "score": 0.0,
                "fit_band": "Filtered Out",
                "matched_keywords": "",
                "penalized_keywords": "Experience Gap",
                "decision_reason": f"Hard-Drop: {exp_penalty_reason}",
                "score_components": {
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
            score = self.fast_track_base_score
            base_score = self.fast_track_base_score

        title_points = min(title_points, self.title_max_points)
        score += title_points

        jd_pos_hits = self._match_compiled(jd_blob, self._compiled_body_positive)
        jd_neg_hits = self._match_compiled(jd_blob, self._compiled_body_negative)
        body_positive_points = min(sum(pts for _, pts in jd_pos_hits), self.positive_keyword_cap)
        body_negative_points = min(sum(pts for _, pts in jd_neg_hits), self.negative_keyword_cap)
        score += body_positive_points
        score -= body_negative_points

        location_penalty = 0
        location_bonus = 0
        tier_bonus = {1: 15, 2: 8, 3: 4}.get(company_tier, 0)
        score += tier_bonus

        positive_keyword_gate_bonus = 0
        if self.require_one_positive_keyword and not title_hits and partial_hits:
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

        if self.us_only and not self.allow_international_remote and self._is_international_remote(location):
            return {
                "score": 0.0,
                "fit_band": "Filtered Out",
                "matched_keywords": ", ".join(sorted(set(term for term, _ in title_hits + partial_hits + jd_pos_hits))),
                "penalized_keywords": "international remote",
                "decision_reason": "Hard-Drop: location mismatch (international remote)",
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

        if is_remote_role:
            if self.remote_us_enabled and self.location_policy in {"remote_only", "remote_or_hybrid"}:
                location_bonus += self.remote_us_bonus
        elif is_hybrid_role or is_onsite_role or location.strip():
            local_hybrid_salary_ok = (
                normalized_comp is not None and normalized_comp >= self.local_hybrid_salary_floor
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
                return {
                    "score": 0.0,
                    "fit_band": "Filtered Out",
                    "matched_keywords": ", ".join(sorted(set(term for term, _ in title_hits + partial_hits + jd_pos_hits))),
                    "penalized_keywords": "location mismatch",
                    "decision_reason": "Hard-Drop: location mismatch",
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

        source_penalty -= int(source_trust["score_penalty"])
        score += compensation_adjustment + contract_adjustment + location_bonus + source_penalty + exp_soft_penalty
        score = max(0.0, min(100.0, score))

        matched_str = ", ".join(sorted(set(term for term, _ in title_hits + partial_hits + jd_pos_hits)))
        penalized_str = ", ".join(sorted(set(term for term, _ in jd_neg_hits)))
        if exp_penalty_reason and exp_soft_penalty < 0:
            penalized_str += (", " if penalized_str else "") + exp_penalty_reason

        return {
            "score": score,
            "fit_band": self.fit_band(score),
            "matched_keywords": matched_str,
            "penalized_keywords": penalized_str,
            "decision_reason": (
                f"score={score:.1f} base={base_score:.1f} "
                f"title={title_points} body+={body_positive_points} body-={body_negative_points} "
                f"tier={tier_bonus} partial={partial_title_points} gate={positive_keyword_gate_bonus} "
                f"location-={location_penalty} location+={location_bonus} comp={compensation_adjustment} contract={contract_adjustment} "
                f"experience={exp_soft_penalty} source={source_penalty} source_lane={source_trust['key']} "
                f"work_type={work_type} normalized_comp={normalized_comp if normalized_comp is not None else 'na'} "
                f"hits={len(title_hits) + len(partial_hits) + len(jd_pos_hits)}"
            ),
            "score_components": {
                "base_score": base_score,
                "title_points": title_points,
                "body_positive_points": body_positive_points,
                "body_negative_points": body_negative_points,
                "tier_bonus": tier_bonus,
                "partial_title_bonus": partial_title_points,
                "positive_keyword_gate_bonus": positive_keyword_gate_bonus,
                "location_penalty": location_penalty,
                "location_bonus": location_bonus,
                "compensation_adjustment": compensation_adjustment,
                "contract_adjustment": contract_adjustment,
                "source_penalty": source_penalty,
                "experience_penalty": exp_soft_penalty,
                "source_trust_key": source_trust["key"],
            },
            **comp_data,
        }

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
            enrichment_adjustment -= 15
            enrichment_reasons.append("-15 no_visa_sponsorship")

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
