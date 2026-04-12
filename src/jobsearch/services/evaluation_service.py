"""Evaluation service for scoring and ranking jobs."""

from __future__ import annotations
import sqlite3
import logging
import json
from typing import Any, Dict, List, Optional

from jobsearch.scraper.scoring import Scorer
from jobsearch.acquisition.contracts import EvaluationResult

logger = logging.getLogger(__name__)

class EvaluationService:
    """Service for computing job scores and fit bands."""

    def __init__(self, preferences: Dict[str, Any]):
        self.prefs = preferences
        self.scorer = Scorer(preferences)
        self._v2_available = False
        try:
            from jobsearch.scraper.scoring_v2 import build_v2_config_from_prefs
            self._v2_cfg, self._v2_title_index, self._v2_pw, self._v2_ftbase, self._v2_ftmin = (
                build_v2_config_from_prefs(preferences)
            )
            self._v2_available = True
        except Exception as e:
            logger.warning("V2 scoring unavailable in EvaluationService: %s", e)

    def evaluate_job(self, job_data: Dict[str, Any]) -> EvaluationResult:
        """Compute V1 and V2 scores for a single job."""
        # 1. Run V1 Scoring (Hard gates and baseline)
        v1_results = self.scorer.score_job(job_data)
        
        final_score = v1_results["score"]
        final_fit_band = v1_results["fit_band"]
        decision_reason = v1_results["decision_reason"]
        v2_breakdown = None

        # 2. Run V2 Scoring (if applicable)
        if final_fit_band not in ("Disqualified", "Filtered Out") and self._v2_available:
            try:
                from jobsearch.scraper.scoring_v2 import score_job_v2, v2_title_pts
                _tpts = v2_title_pts(
                    job_data.get("title", ""),
                    self._v2_title_index,
                    self._v2_cfg,
                    self._v2_pw,
                    self._v2_ftbase,
                    self._v2_ftmin,
                )
                _v2 = score_job_v2(
                    job_data.get("title", ""),
                    job_data.get("description", ""),
                    _tpts,
                    self._v2_cfg,
                    self._v2_title_index,
                )
                final_score = round(_v2.final_score, 2)
                final_fit_band = _v2.fit_band
                decision_reason = (
                    f"V2: {_v2.fit_band} | title={_v2.canonical_title or 'unresolved'}"
                    f" | seniority={_v2.seniority.band}"
                    f" | anchor={round(_v2.keyword.anchor_score,1)}"
                    f" base={round(_v2.keyword.baseline_score,1)}"
                    f" neg={round(_v2.keyword.negative_score,1)}"
                )
                v2_breakdown = {
                    "score_v2": round(_v2.final_score, 2),
                    "fit_band_v2": _v2.fit_band,
                    "v2_canonical_title": _v2.canonical_title,
                    "v2_seniority_band": _v2.seniority.band,
                    "v2_anchor_score": round(_v2.keyword.anchor_score, 2),
                    "v2_baseline_score": round(_v2.keyword.baseline_score, 2),
                    "v2_flags": _v2.flags,
                }
            except Exception as e:
                logger.debug("V2 scoring failed in EvaluationService: %s", e)

        return EvaluationResult(
            score=final_score,
            fit_band=final_fit_band,
            decision_reason=decision_reason,
            matched_keywords=v1_results["matched_keywords"],
            penalized_keywords=v1_results["penalized_keywords"],
            apply_now_eligible=v1_results.get("apply_now_eligible", True),
            v2_breakdown=v2_breakdown
        )

    def evaluate_pending_jobs(self, conn: sqlite3.Connection) -> tuple[int, List[Dict[str, Any]]]:
        """Find jobs with 'Pending Evaluation' status and evaluate them. Returns (count, rejected_rows)."""
        rows = conn.execute(
            "SELECT id, company, role, description_excerpt, location, tier, "
            "salary_low, salary_high, salary_text, work_type, "
            "source_lane, canonical_job_url, job_url, source "
            "FROM applications "
            "WHERE fit_band = 'Pending Evaluation' "
            "AND status IN ('considering', 'exploring')"
        ).fetchall()
        
        count = 0
        rejected_rows = []
        for r in rows:
            try:
                job_data = {
                    "title": r["role"],
                    "description": r["description_excerpt"],
                    "tier": r["tier"],
                    "location": r["location"],
                    "salary_min": r["salary_low"],
                    "salary_max": r["salary_high"],
                    "salary_text": r["salary_text"],
                    "work_type": r["work_type"],
                    "source_lane": r["source_lane"],
                    "canonical_job_url": r["canonical_job_url"],
                }
                res = self.evaluate_job(job_data)
                
                if res.score < self.scorer.min_score_to_keep:
                    # Reject: Delete from DB and add to rejected_rows
                    rejected_rows.append({
                        "company": r["company"],
                        "title": r["role"],
                        "location": r["location"],
                        "url": r["job_url"],
                        "source": r["source"],
                        "tier": r["tier"],
                        "score": res.score,
                        "fit_band": res.fit_band,
                        "work_type": r["work_type"],
                        "compensation_unit": "",
                        "normalized_compensation_usd": None,
                        "drop_reason": "disqualified" if res.fit_band == "Disqualified" else "score_below_threshold",
                        "decision_reason": res.decision_reason,
                        "matched_keywords": res.matched_keywords,
                        "penalized_keywords": res.penalized_keywords,
                    })
                    conn.execute("DELETE FROM applications WHERE id = ?", (r["id"],))
                    continue

                # Update application with scores
                update_sql = """
                    UPDATE applications SET
                        score = ?, fit_band = ?, decision_reason = ?,
                        matched_keywords = ?, penalized_keywords = ?,
                        apply_now_eligible = ?
                """
                params = [
                    res.score, res.fit_band, res.decision_reason,
                    res.matched_keywords, res.penalized_keywords,
                    1 if res.apply_now_eligible else 0
                ]
                
                if res.v2_breakdown:
                    update_sql += """,
                        score_v2 = ?, fit_band_v2 = ?, v2_canonical_title = ?,
                        v2_seniority_band = ?, v2_anchor_score = ?,
                        v2_baseline_score = ?, v2_flags = ?
                    """
                    params.extend([
                        res.v2_breakdown["score_v2"],
                        res.v2_breakdown["fit_band_v2"],
                        res.v2_breakdown["v2_canonical_title"],
                        res.v2_breakdown["v2_seniority_band"],
                        res.v2_breakdown["v2_anchor_score"],
                        res.v2_breakdown["v2_baseline_score"],
                        json.dumps(res.v2_breakdown["v2_flags"])
                    ])
                
                update_sql += " WHERE id = ?"
                params.append(r["id"])
                
                conn.execute(update_sql, params)
                count += 1
            except Exception as e:
                print(f"DEBUG: Error evaluating job {r['id']}: {e}")
                import traceback
                traceback.print_exc()
            
        conn.commit()
        return count, rejected_rows
