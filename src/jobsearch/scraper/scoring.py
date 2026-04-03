"""src/jobsearch/scraper/scoring.py — Refined Scoring Logic for Deep YAML."""

import re
import logging
from typing import List, Tuple, Dict, Any, Optional

logger = logging.getLogger(__name__)

class Scorer:
    def __init__(self, preferences: Dict[str, Any]):
        self.prefs = preferences
        
        # 1. Title Evaluation Settings (Nested under 'titles')
        title_cfg = self.prefs.get("titles", {})
        self.title_positive_weights = self._pref_weight_pairs(title_cfg.get("positive_weights"), [])
        self.title_positive_keywords = [str(item).strip().lower() for item in title_cfg.get("positive_keywords", []) if str(item).strip()]
        self.negative_disqualifiers = title_cfg.get("negative_disqualifiers", [])
        self.must_have_modifiers = title_cfg.get("must_have_modifiers", [])
        
        # 2. JD Evaluation Settings (Nested under 'keywords')
        kw_cfg = self.prefs.get("keywords", {})
        self.body_positive = self._pref_weight_pairs(kw_cfg.get("body_positive"), [])
        self.body_negative = self._pref_weight_pairs(kw_cfg.get("body_negative"), [])
        
        # 3. Policy & Scoring Thresholds
        scoring_cfg = self.prefs.get("scoring", {})
        self.min_score_to_keep = scoring_cfg.get("minimum_score_to_keep", 35)
        
        # 4. Compensation & Location (Nested under 'search')
        search_cfg = self.prefs.get("search", {})
        comp_cfg = search_cfg.get("compensation", {})
        geo_cfg = search_cfg.get("geography", {})
        self.min_salary_usd = comp_cfg.get("min_salary_usd", 165000)
        self.remote_only = comp_cfg.get("remote_only", True)
        self.us_only = bool(geo_cfg.get("us_only", True))
        self.allow_international_remote = bool(geo_cfg.get("allow_international_remote", False))
        self.require_one_positive_keyword = bool(title_cfg.get("require_one_positive_keyword", False))
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
                if not phrase: continue
                try:
                    weight = int(float(value))
                    pairs.append((phrase, weight))
                except (ValueError, TypeError): continue
        elif isinstance(raw, list):
            # If it's a simple list, assign a default weight of 5
            for item in raw:
                if isinstance(item, str):
                    pairs.append((item.lower().strip(), 5))
                elif isinstance(item, dict):
                    phrase = str(item.get("keyword") or item.get("phrase") or "").strip().lower()
                    if not phrase: continue
                    weight = int(float(item.get("weight") or item.get("points") or 5))
                    pairs.append((phrase, weight))
        return pairs or default

    def clean(self, value: Optional[str]) -> str:
        if not value: return ""
        return re.sub(r'\s+', ' ', str(value)).strip()

    def _kw_match(self, kw: str, blob: str) -> bool:
        if not kw or not blob: return False
        keyword = self.clean(kw).lower().strip()
        haystack = self.clean(blob).lower()
        if not keyword or not haystack: return False
        
        escaped = re.escape(keyword).replace(r"\ ", r"\s+")
        if re.match(r"^\w", keyword) and re.search(r"\w$", keyword):
            pattern = rf"\b{escaped}\b"
        else:
            pattern = rf"(?<!\w){escaped}(?!\w)"
        return re.search(pattern, haystack, flags=re.IGNORECASE) is not None

    def _unique_weighted_hits(self, blob: str, weighted_terms: List[Tuple[str, int]]) -> List[Tuple[str, int]]:
        hits = []
        for term, pts in weighted_terms:
            if self._kw_match(term, blob):
                hits.append((term, pts))
        return hits

    def fit_band(self, score: float) -> str:
        if score >= 85: return "Strong Match"
        if score >= 70: return "Good Match"
        if score >= 50: return "Fair Match"
        if score >= 35: return "Weak Match"
        return "Poor Match"

    def is_disqualified(self, title: str) -> bool:
        """Check if a title matches any hard disqualifiers."""
        t_l = title.lower()
        for dq in self.negative_disqualifiers:
            if dq.lower() in t_l:
                return True
        return False

    def score_job(self, job_data: Dict[str, Any]) -> Dict[str, Any]:
        """Full scoring logic synchronized with YAML structure."""
        title = job_data.get("title", "")
        description = job_data.get("description", "")
        company_tier = job_data.get("tier", 4)
        location = str(job_data.get("location", "") or "")
        
        title_l = title.lower()
        jd_blob = description.lower()
        
        # 1. Soft-Drop Phase: Hard disqualification
        if self.is_disqualified(title):
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
            },
        }

        score = 0.0
        
        # 2. Title Scoring
        title_hits = self._unique_weighted_hits(title_l, self.title_positive_weights)
        title_points = sum(pts for _, pts in title_hits)
        partial_hits = self._partial_title_hits(title_l, title_hits)
        partial_title_points = sum(pts for _, pts in partial_hits)
        title_points += partial_title_points
        
        # Apply base score if high-weight title match found (Fast-Track)
        base_score = 0.0
        if any(pts >= 8 for _, pts in title_hits):
            score = 50.0
            base_score = 50.0
        elif title_hits:
            score = max(score, 15.0)
            base_score = score
        elif partial_hits:
            score = max(score, 12.0)
            base_score = score
        
        score += title_points
        
        # 3. JD Scoring
        jd_pos_hits = self._unique_weighted_hits(jd_blob, self.body_positive)
        jd_neg_hits = self._unique_weighted_hits(jd_blob, self.body_negative)
        
        body_positive_points = sum(pts for _, pts in jd_pos_hits)
        body_negative_points = sum(pts for _, pts in jd_neg_hits)
        score += body_positive_points
        score -= body_negative_points

        location_penalty = 0
        if self.us_only and not self.allow_international_remote and self._is_international_remote(location):
            location_penalty = 12
            score -= location_penalty
            jd_neg_hits.append(("international remote", location_penalty))
        
        # 4. Tier bonus
        tier_bonus = {1: 15, 2: 8, 3: 4}.get(company_tier, 0)
        score += tier_bonus

        positive_keyword_gate_bonus = 0
        if self.require_one_positive_keyword and not title_hits and partial_hits:
            score += 6
            positive_keyword_gate_bonus = 6
        
        # 5. Final constraints & Normalization
        score = max(0.0, min(100.0, score))
        
        matched_str = ", ".join(set(t for t, _ in title_hits + partial_hits + jd_pos_hits))
        penalized_str = ", ".join(set(t for t, _ in jd_neg_hits))
        
        return {
            "score": score,
            "fit_band": self.fit_band(score),
            "matched_keywords": matched_str,
            "penalized_keywords": penalized_str,
            "decision_reason": (
                f"score={score:.1f} base={base_score:.1f} "
                f"title={title_points} body+={body_positive_points} body-={body_negative_points} "
                f"tier={tier_bonus} partial={partial_title_points} gate={positive_keyword_gate_bonus} "
                f"location-={location_penalty} "
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
            },
        }

    def _partial_title_hits(self, title: str, exact_hits: List[Tuple[str, int]]) -> List[Tuple[str, int]]:
        if exact_hits:
            return []
        exact_terms = {term for term, _ in exact_hits}
        exact_text = " ".join(exact_terms)
        hits: List[Tuple[str, int]] = []
        seen = set()

        for phrase in self.title_positive_keywords:
            if phrase in exact_terms:
                continue
            tokens = [token for token in phrase.split() if len(token) > 2]
            matched = [token for token in tokens if token in title]
            if len(tokens) >= 2 and len(matched) >= min(2, len(tokens)):
                label = f"title:{phrase}"
                if label not in seen:
                    hits.append((label, 3))
                    seen.add(label)

        for marker, points in self.partial_title_markers.items():
            if marker in exact_text:
                continue
            if marker in title and marker not in seen:
                hits.append((marker, points))
                seen.add(marker)

        return hits[:4]

    def _is_international_remote(self, location: str) -> bool:
        location_l = self.clean(location).lower()
        if "remote" not in location_l:
            return False
        return any(marker in location_l for marker in self._non_us_remote_markers)
