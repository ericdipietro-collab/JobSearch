import re
import json
from datetime import datetime, timezone
from typing import List, Tuple, Dict, Any, Optional

class Scorer:
    def __init__(self, preferences: Dict[str, Any]):
        self.prefs = preferences
        
        # Extract weights and settings from prefs
        self.title_positive_weights = self._pref_weight_pairs(self.prefs.get("title_weights"), [])
        self.positive_keywords = self._pref_weight_pairs(self.prefs.get("positive_keywords"), [])
        self.negative_keywords = self._pref_weight_pairs(self.prefs.get("negative_keywords"), [])
        self.sweet_spot_phrases = self._pref_weight_pairs(self.prefs.get("sweet_spot_phrases"), [])
        
        self.min_salary_usd = self.prefs.get("min_salary_usd", 100000)
        self.max_job_age_days = self.prefs.get("max_job_age_days", 21)
        self.min_score_to_keep = self.prefs.get("min_score_to_keep", 35)

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
            for item in raw:
                if not isinstance(item, dict): continue
                phrase = str(item.get("keyword") or item.get("phrase") or "").strip().lower()
                if not phrase: continue
                try:
                    weight = int(float(item.get("weight") or item.get("points") or 0))
                    pairs.append((phrase, weight))
                except (ValueError, TypeError): continue
        return pairs or default

    def clean(self, value: Optional[str]) -> str:
        if not value: return ""
        # Simplified clean, can be expanded to match v6 exactly if needed
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

    def score_job(self, job_data: Dict[str, Any]) -> Dict[str, Any]:
        """Simplified version of score_job for Phase 2."""
        title = job_data.get("title", "")
        description = job_data.get("description", "")
        company_tier = job_data.get("tier", 4)
        location = job_data.get("location", "")
        
        title_l = title.lower()
        jd_blob = description.lower()
        
        score = 0.0
        
        # Title hits
        title_hits = self._unique_weighted_hits(title_l, self.title_positive_weights)
        title_points = sum(pts for _, pts in title_hits)
        score += title_points
        
        # JD hits
        jd_pos_hits = self._unique_weighted_hits(jd_blob, self.positive_keywords)
        jd_neg_hits = self._unique_weighted_hits(jd_blob, self.negative_keywords)
        
        score += sum(pts for _, pts in jd_pos_hits)
        score -= sum(pts for _, pts in jd_neg_hits)
        
        # Tier bonus
        score += {1: 10, 2: 5, 3: 2}.get(company_tier, 0)
        
        # Final constraints
        score = max(0.0, min(100.0, score))
        
        return {
            "score": score,
            "fit_band": self.fit_band(score),
            "matched_keywords": ", ".join(t for t, _ in title_hits + jd_pos_hits),
            "penalized_keywords": ", ".join(t for t, _ in jd_neg_hits),
            "decision_reason": f"Score {score:.1f} based on keyword matching."
        }
