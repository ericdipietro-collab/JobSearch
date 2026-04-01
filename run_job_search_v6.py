"""Unified launcher for JobSearch v6.

Use this file as the single entrypoint for running the repository while the
main `job_search_v6.py` script is being stabilized.

After pulling the repo, run:
    python run_job_search_v6.py [args...]
"""

from __future__ import annotations

import builtins
import sys
from pathlib import Path
from typing import Optional

BASE_DIR = Path(__file__).resolve().parent


def _extract_arg_value(flag: str) -> Optional[str]:
    argv = sys.argv[1:]
    for i, token in enumerate(argv):
        if token == flag and i + 1 < len(argv):
            return argv[i + 1]
        if token.startswith(flag + "="):
            return token.split("=", 1)[1]
    return None


def _has_flag(flag: str) -> bool:
    return any(token == flag or token.startswith(flag + "=") for token in sys.argv[1:])


def _resolve_candidate(path_str: Optional[str]) -> Optional[Path]:
    if not path_str:
        return None
    p = Path(path_str).expanduser()
    if not p.is_absolute():
        p = (BASE_DIR / p).resolve()
    else:
        p = p.resolve()
    return p


def _prepare_builtins() -> None:
    prefs_override = _resolve_candidate(_extract_arg_value("--prefs") or _extract_arg_value("--preferences"))
    companies_override = _resolve_candidate(_extract_arg_value("--companies"))
    if companies_override is None and _has_flag("--test-companies"):
        companies_override = (BASE_DIR / "config" / "job_search_companies_test.yaml").resolve()

    builtins.CLI_PREFERENCES_PATH_OVERRIDE = str(prefs_override) if prefs_override else None
    builtins.CLI_COMPANIES_PATH_OVERRIDE = str(companies_override) if companies_override else None
    builtins.DEEP_SEARCH_ENABLED = _has_flag("--deep-search")

    builtins.COMPANY_REGISTRY_FILE_CANDIDATES = [
        p for p in [
            companies_override,
            BASE_DIR / "config" / "job_search_companies.yaml",
            BASE_DIR / "job_search_companies.yaml",
            BASE_DIR / "config" / "companies.yaml",
            BASE_DIR / "companies.yaml",
        ]
        if p is not None
    ]

    builtins.PREFERENCES_FILE_CANDIDATES = [
        p for p in [
            prefs_override,
            BASE_DIR / "config" / "job_search_preferences_tiered_softdrop_final_patched2.yaml",
            BASE_DIR / "config" / "job_search_preferences_tiered_softdrop_final_patched.yaml",
            BASE_DIR / "config" / "job_search_preferences_tiered_softdrop_final.yaml",
            BASE_DIR / "config" / "job_search_preferences_transparent_final.yaml",
            BASE_DIR / "config" / "job_search_preferences.yaml",
            BASE_DIR / "job_search_preferences.yaml",
            BASE_DIR / "config" / "preferences.yaml",
            BASE_DIR / "preferences.yaml",
            BASE_DIR / "config" / "job_search_preferences.example.yaml",
        ]
        if p is not None
    ]


def _replace_once(source: str, target: str, replacement: str) -> str:
    if target in source and replacement not in source:
        return source.replace(target, replacement, 1)
    return source


def _patch_source(source: str) -> str:
    # Note: test-companies path and is_hybrid unpacking are now fixed in source; patches removed.
    source = _replace_once(
        source,
        "def _decision_reason_parts(\n    title_fast_track_hits: List[Tuple[str, int]],\n    title_weight_hits: List[Tuple[str, int]],\n    jd_positive_hits: List[Tuple[str, int]],\n    jd_negative_hits: List[Tuple[str, int]],\n    *,\n    title_points: float,\n    jd_positive_points: float,\n    jd_negative_points: float,\n    jd_positive_multiplier: float,\n    location_label: str = \"\",\n    extra_parts: Optional[List[str]] = None,\n) -> str:\n",
        "def _decision_reason_parts(\n    title_fast_track_hits: List[Tuple[str, int]],\n    title_weight_hits: List[Tuple[str, int]],\n    jd_positive_hits: List[Tuple[str, int]],\n    jd_negative_hits: List[Tuple[str, int]],\n    *,\n    title_points: float = 0.0,\n    jd_positive_points: float = 0.0,\n    jd_negative_points: float = 0.0,\n    jd_positive_multiplier: float = 1.0,\n    location_label: str = \"\",\n    extra_parts: Optional[List[str]] = None,\n) -> str:\n",
    )
    source = _replace_once(
        source,
        "    decision_score = float(scored.get(\"decision_score\", scored.get(\"score\", 0.0)))\n    keep = decision_score >= MIN_SCORE_TO_KEEP\n\n    return {\n        \"keep\": keep,\n        \"drop_stage\": \"\" if keep else \"Score Threshold\",\n        \"drop_reason\": \"\" if keep else f\"{decision_score:.1f} < {MIN_SCORE_TO_KEEP}\",\n        \"reason\": \"\" if keep else f\"score_below_threshold:{int(MIN_SCORE_TO_KEEP)}\",\n",
        "    score_threshold_used = MIN_SCORE_TO_KEEP\n    if title_rescue_bucket == \"adjacent_title\":\n        score_threshold_used = TITLE_RESCUE_ADJACENT_TITLE_MIN_SCORE_TO_KEEP_CFG\n\n    decision_score = float(scored.get(\"decision_score\", scored.get(\"score\", 0.0)))\n    keep = decision_score >= score_threshold_used\n    threshold_label = (\n        f\"adjacent({int(score_threshold_used)})\"\n        if title_rescue_bucket == \"adjacent_title\"\n        else f\"{int(score_threshold_used)}\"\n    )\n\n    return {\n        \"keep\": keep,\n        \"drop_stage\": \"\" if keep else \"Score Threshold\",\n        \"drop_reason\": \"\" if keep else f\"{decision_score:.1f} < {score_threshold_used}\",\n        \"reason\": \"\" if keep else f\"score_below_threshold:{threshold_label}\",\n",
    )
    source = _replace_once(source, '        "score_threshold_used": MIN_SCORE_TO_KEEP,\n', '        "score_threshold_used": score_threshold_used,\n')
    source = _replace_once(
        source,
        "        summary = pd.concat([evaluated, kept, manual, rejected], axis=1).fillna(0).reset_index()\n",
        "        summary = pd.concat([evaluated, kept, manual, rejected], axis=1).fillna(0).reset_index()\n        if \"index\" in summary.columns and col not in summary.columns:\n            summary = summary.rename(columns={\"index\": col})\n",
    )
    source = _replace_once(
        source,
        "    funnel_df = pd.DataFrame([{\n        \"raw_intake_count\": raw_intake_count,\n        \"unique_evaluated_count\": unique_evaluated_count,\n        \"total_rejected\": total_rejected,\n        \"total_real_jobs_kept\": total_kept,\n        \"total_manual_targets\": total_manual,\n        \"kept_rate_of_unique_evaluated\": round((total_kept / unique_evaluated_count) if unique_evaluated_count else 0.0, 4),\n        \"unique_from_raw_rate\": round((unique_evaluated_count / raw_intake_count) if raw_intake_count else 0.0, 4),\n    }])\n",
        "    funnel_df = pd.DataFrame([{\n        \"raw_postings_seen\": raw_intake_count,\n        \"raw_postings_filtered\": total_rejected,\n        \"deduped_jobs_evaluated\": unique_evaluated_count,\n        \"deduped_real_jobs_kept\": total_kept,\n        \"deduped_manual_targets\": total_manual,\n        \"kept_rate_of_deduped_jobs\": round((total_kept / unique_evaluated_count) if unique_evaluated_count else 0.0, 4),\n        \"deduped_from_raw_rate\": round((unique_evaluated_count / raw_intake_count) if raw_intake_count else 0.0, 4),\n    }])\n",
    )
    source = _replace_once(
        source,
        "    print()\n    print(f\"  Raw intake:           {raw_intake_total}\")\n    print(f\"  Unique evaluated:     {unique_evaluated_total}\")\n    print(f\"  Total real jobs kept: {len(all_jobs)}\")\n    print(f\"  Total jobs rejected:  {len(all_rejected)}\")\n    print(f\"  Apply now:            {len(views['Apply Now'])}\")\n    print(f\"  Review today:         {len(views['Review Today'])}\")\n    print(f\"  Watch:                {len(views['Watch'])}\")\n    print(f\"  Manual targets:       {len(all_manual_targets)}\")\n",
        "    print()\n    print(f\"  Raw postings seen:        {raw_intake_total}\")\n    print(f\"  Raw postings filtered:    {len(all_rejected)}\")\n    print(f\"  Deduped jobs evaluated:   {unique_evaluated_total}\")\n    print(f\"  Deduped real jobs kept:   {len(all_jobs)}\")\n    print(f\"  Apply now:                {len(views['Apply Now'])}\")\n    print(f\"  Review today:             {len(views['Review Today'])}\")\n    print(f\"  Watch:                    {len(views['Watch'])}\")\n    print(f\"  Deduped manual targets:   {len(all_manual_targets)}\")\n",
    )
    return source


def main() -> None:
    # Force UTF-8 I/O so Unicode characters (e.g. ✓) don't crash on Windows
    # consoles that default to cp1252.
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    if hasattr(sys.stderr, "reconfigure"):
        sys.stderr.reconfigure(encoding="utf-8", errors="replace")

    _prepare_builtins()
    script_path = BASE_DIR / "job_search_v6.py"
    source = script_path.read_text(encoding="utf-8")
    source = _patch_source(source)
    globals_dict = {
        "__name__": "__main__",
        "__file__": str(script_path),
        "__package__": None,
        "__cached__": None,
    }
    exec(compile(source, str(script_path), "exec"), globals_dict)


if __name__ == "__main__":
    main()
