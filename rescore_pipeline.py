#!/usr/bin/env python3
"""
Re-score all jobs in the persistent store using the current preferences.

Run directly:
    python rescore_pipeline.py

Or triggered by the dashboard via "Re-score with Current Preferences" button.
Uses description_excerpt (first ~600 chars) — keyword coverage is slightly
lower than a fresh scrape, but all title/location/tier signals are intact.
"""
from __future__ import annotations

import json
import sys
from datetime import datetime
from pathlib import Path

# Run from repo root so imports work
sys.path.insert(0, str(Path(__file__).resolve().parent))

# Importing job_search_v6 reads the current prefs YAML at module load time,
# so scoring globals reflect whatever is saved in config/.
from job_search_v6 import (  # noqa: E402
    Company,
    action_bucket,
    fit_band,
    score_job,
)

STORE_JSON = Path("results/job_search_store.json")


def _to_bool(val) -> bool:
    if isinstance(val, bool):
        return val
    return str(val).strip().lower() in ("true", "1", "yes")


def _to_float(val, default=None):
    try:
        return float(val)
    except (TypeError, ValueError):
        return default


def _to_list(val) -> list:
    if not val:
        return []
    if isinstance(val, list):
        return val
    return [x.strip() for x in str(val).split(",") if x.strip()]


def _parse_posted(posted_at_str: str):
    for fmt in ("%Y-%m-%dT%H:%M:%S", "%Y-%m-%d"):
        try:
            return datetime.strptime(str(posted_at_str)[:19], fmt)
        except (ValueError, TypeError):
            pass
    return None


def rescore_store(store_path: Path = STORE_JSON) -> dict:
    """Re-score every job in the store. Returns {"updated": N, "total": N}."""
    if not store_path.exists():
        print("No store found — nothing to rescore.", flush=True)
        return {"updated": 0, "total": 0}

    store: dict = json.loads(store_path.read_text(encoding="utf-8"))
    updated = 0

    for key, job in store.items():
        title        = str(job.get("title") or "")
        location     = str(job.get("location") or "")
        description  = str(job.get("description_excerpt") or "")
        source       = str(job.get("source") or "")
        url          = str(job.get("url") or "")
        company_name = str(job.get("company") or "")
        tier         = int(_to_float(job.get("tier"), 2) or 2)

        company_stub = Company(
            name=company_name,
            tier=tier,
            priority="standard",
            adapter="custom_manual",
            domain="",
            industry=[],
        )

        result = score_job(
            company=company_stub,
            title=title,
            location=location,
            description=description,
            posted_dt=_parse_posted(str(job.get("posted_at") or "")),
            is_manual=False,
            apply_hits=_to_list(job.get("apply_domains_hit")),
            skip_hits=_to_list(job.get("skip_domains_hit")),
            source=source,
            url=url,
        )

        new_score = float(result.get("score", 0.0))
        new_bucket = action_bucket(
            score=new_score,
            is_remote=_to_bool(result.get("is_remote")),
            is_hybrid=_to_bool(result.get("is_hybrid")),
            is_non_us=_to_bool(result.get("is_non_us")),
            manual_review=False,
            tier=tier,
            role_alignment_score=float(result.get("role_alignment_score", 0.0)),
            title=title,
            location=location,
            salary_low=_to_float(job.get("salary_low")),
            salary_high=_to_float(job.get("salary_high")),
            salary_period=str(job.get("salary_period") or ""),
            salary_currency=str(job.get("salary_currency") or ""),
        )

        job.update({
            "score":               new_score,
            "fit_band":            fit_band(new_score),
            "action_bucket":       new_bucket,
            "matched_keywords":    result.get("matched_keywords", ""),
            "penalized_keywords":  result.get("penalized_keywords", ""),
            "decision_reason":     result.get("decision_reason", ""),
            "role_alignment_score": result.get("role_alignment_score", 0.0),
            "role_alignment_label": result.get("role_alignment_label", ""),
            "lane_fit_score":      result.get("lane_fit_score", 0.0),
            "lane_fit_label":      result.get("lane_fit_label", ""),
            "benchmark_lane":      result.get("benchmark_lane", ""),
            "benchmark_match_score": result.get("benchmark_match_score", 0.0),
        })
        updated += 1
        if updated % 50 == 0:
            print(f"  Re-scored {updated} / {len(store)}…", flush=True)

    store_path.write_text(
        json.dumps(store, ensure_ascii=False, default=str, indent=None),
        encoding="utf-8",
    )
    total = len(store)
    print(f"Re-score complete: {updated} of {total} jobs updated.", flush=True)
    return {"updated": updated, "total": total}


if __name__ == "__main__":
    rescore_store()
