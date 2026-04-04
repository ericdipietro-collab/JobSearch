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
        self.min_score_to_keep = scoring_cfg.get("minimum_score_to_keep", 35)
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

        self.target_salary_usd = float(comp_cfg.get("target_salary_usd", 165000))
        self.min_salary_usd = float(comp_cfg.get("min_salary_usd", 165000))
        self.allow_missing_salary = bool(comp_cfg.get("allow_missing_salary", True))
        self.remote_only = comp_cfg.get("remote_only", True)
        self.us_only = bool(geo_cfg.get("us_only", True))
        self.allow_international_remote = bool(geo_cfg.get("allow_international_remote", False))
        self.require_one_positive_keyword = bool(title_cfg.get("require_one_positive_keyword", False))

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
        if any(marker in blob for marker in ["intern", "internship", "co-op", "co op"]):
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
        text = self.clean(job_data.get("salary_text"))
        sanitized = re.sub(r"\b(1099|w2|w-2|c2c|corp[-\s]?to[-\s]?corp)\b", " ", text, flags=re.IGNORECASE)
        matches = [float(match.replace(",", "")) for match in re.findall(r"\$?\s*([0-9]{1,3}(?:,[0-9]{3})*(?:\.\d{1,2})?)", sanitized)]
        if not matches:
            return None
        return sum(matches[:2]) / min(2, len(matches))

    def _normalize_compensation(self, job_data: Dict[str, Any], work_type: str, unit: str) -> Dict[str, Any]:
        salary_min = self._to_float(job_data.get("salary_min"))
        salary_max = self._to_float(job_data.get("salary_max"))
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
        body_positive_points = sum(pts for _, pts in jd_pos_hits)
        body_negative_points = sum(pts for _, pts in jd_neg_hits)
        score += body_positive_points
        score -= body_negative_points

        location_penalty = 0
        if self.us_only and not self.allow_international_remote and self._is_international_remote(location):
            location_penalty = 12
            score -= location_penalty
            jd_neg_hits.append(("international remote", location_penalty))

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

        work_type = comp_data["work_type"]
        normalized_comp = comp_data["normalized_compensation_usd"]

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

        score += compensation_adjustment + contract_adjustment
        score = max(0.0, min(100.0, score))

        matched_str = ", ".join(sorted(set(term for term, _ in title_hits + partial_hits + jd_pos_hits)))
        penalized_str = ", ".join(sorted(set(term for term, _ in jd_neg_hits)))

        return {
            "score": score,
            "fit_band": self.fit_band(score),
            "matched_keywords": matched_str,
            "penalized_keywords": penalized_str,
            "decision_reason": (
                f"score={score:.1f} base={base_score:.1f} "
                f"title={title_points} body+={body_positive_points} body-={body_negative_points} "
                f"tier={tier_bonus} partial={partial_title_points} gate={positive_keyword_gate_bonus} "
                f"location-={location_penalty} comp={compensation_adjustment} contract={contract_adjustment} "
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
                "compensation_adjustment": compensation_adjustment,
                "contract_adjustment": contract_adjustment,
            },
            **comp_data,
        }
