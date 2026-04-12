"""src/jobsearch/app_main.py — Full Feature Dashboard with Structural Reinforcements."""

from __future__ import annotations
import logging
import os, re, json, subprocess, sys, io, zipfile
import urllib.parse
from datetime import datetime, timezone
from pathlib import Path
from xml.etree import ElementTree as ET
import pandas as pd
import streamlit as st
import yaml

from jobsearch.config.settings import settings
from jobsearch import ats_db
from jobsearch import __version__
from jobsearch.scraper.scoring import Scorer
from jobsearch.scraper.query_tiers import normalize_search_queries, search_query_text_lines
from jobsearch.scraper.jobspy_validation import ALLOWED_JOBSPY_SITES
from jobsearch.views.style_utils import set_custom_style
from jobsearch.services.export_service import ExcelReportBuilder
from jobsearch.services.search_service import search_jobs
from jobsearch.services.api_service import start_api_server
from jobsearch import scheduler

# ── Import Views ──────────────────────────────────────────────────────────────
from jobsearch.views.home_page              import render_home
from jobsearch.views.tracker_page           import render_tracker
from jobsearch.views.report_page            import render_activity_report
from jobsearch.views.training_page          import render_training
from jobsearch.views.templates_page         import render_templates
from jobsearch.views.journal_page           import render_journal
from jobsearch.views.contacts_page          import render_contacts
from jobsearch.views.question_bank_page     import render_question_bank
from jobsearch.views.company_profiles_page  import render_company_profiles
from jobsearch.views.pipeline_page          import render_pipeline
from jobsearch.views.analytics_page         import render_analytics
from jobsearch.views.action_center_page     import render_action_center
from jobsearch.views.tailoring_studio_page  import render_tailoring_studio
from jobsearch.views.learning_loop_page     import render_learning_loop
from jobsearch.views.submission_review_page  import render_submission_review
from jobsearch.views.market_strategy_page     import render_market_strategy

# ── Constants ──
KNOWN_ADAPTERS = [
    "greenhouse",
    "lever",
    "ashby",
    "workday",
    "rippling",
    "smartrecruiters",
    "usajobs",
    "adzuna",
    "jooble",
    "themuse",
    "indeed_connector",
    "jobspy",
    "google_careers",
    "dice",
    "motionrecruitment",
    "custom_manual",
    "generic",
]
DISPLAY_COLS = [
    "company",
    "url",
    "title",
    "source",
    "source_lane_label",
    "score",
    "fit_band",
    "location",
    "age_days",
    "seen_count",
    "open_days",
    "velocity",
    "tier",
    "matched_keywords",
    "decision_reason",
]

WORK_TYPE_LABELS = {
    "fte": "Full-time",
    "w2_contract": "W2 hourly",
    "1099_contract": "1099 hourly",
    "c2c_contract": "Corp-to-corp",
    "part_time": "Part-time",
    "temporary": "Temporary",
    "internship": "Internship",
    "contract": "Contract",
    "unknown": "Unknown",
}

EXPERIMENTAL_SOURCE_ADAPTERS = {
    "jobspy",
    "google_careers",
    "indeed_connector",
    "adzuna",
    "jooble",
    "themuse",
    "usajobs",
}

SOURCE_LANE_LABELS = {
    "employer_ats": "Employer ATS",
    "contractor": "Contractor",
    "aggregator": "Aggregator",
    "jobspy_experimental": "JobSpy Experimental",
}


def _search_text_match(df: pd.DataFrame, query: str, columns: list[str]) -> pd.DataFrame:
    text = str(query or "").strip().lower()
    if df.empty or not text:
        return df
    haystack = pd.Series([""] * len(df), index=df.index, dtype=str)
    for column in columns:
        if column in df.columns:
            haystack = haystack + " " + df[column].fillna("").astype(str).str.lower()
    return df[haystack.str.contains(re.escape(text), na=False, regex=True)]


def _source_lane_label(value: object) -> str:
    key = str(value or "employer_ats").strip().lower() or "employer_ats"
    return SOURCE_LANE_LABELS.get(key, key.replace("_", " ").title())


def _parse_log_key_values(text: str) -> dict[str, str]:
    return {key: value for key, value in re.findall(r"([a-zA-Z_]+)=([^\s]+)", text or "")}


def _parse_jobspy_board_metrics(note: str) -> list[dict[str, object]]:
    rows: list[dict[str, object]] = []
    for part in [segment.strip() for segment in str(note or "").split("|")]:
        if ":" not in part or "requested=" not in part:
            continue
        board, metrics_text = part.split(":", 1)
        board = board.strip()
        metrics = _parse_log_key_values(metrics_text)
        if not board or not metrics:
            continue
        rows.append(
            {
                "board": board,
                "requested": int(float(metrics.get("requested", "0") or 0)),
                "attempted": int(float(metrics.get("attempted", "0") or 0)),
                "success": int(float(metrics.get("success", "0") or 0)),
                "failed": int(float(metrics.get("failed", "0") or 0)),
                "skipped": int(float(metrics.get("skipped", "0") or 0)),
                "runtime_ms": float(metrics.get("runtime_ms", "0") or 0),
                "raw": int(float(metrics.get("raw", "0") or 0)),
                "normalized": int(float(metrics.get("normalized", "0") or 0)),
                "deduped": int(float(metrics.get("deduped", "0") or 0)),
            }
        )
    return rows


def _parse_experimental_source_log(log_path: Path) -> tuple[pd.DataFrame, pd.DataFrame]:
    if not log_path.exists():
        return pd.DataFrame(), pd.DataFrame()
    try:
        lines = log_path.read_text(encoding="utf-8", errors="ignore").splitlines()
    except Exception:
        return pd.DataFrame(), pd.DataFrame()

    company_rows: list[dict[str, object]] = []
    board_rows: list[dict[str, object]] = []
    company_pattern = re.compile(
        r"^\d{2}:\d{2}:\d{2}\s+\|\s+\[(?P<progress>\d+/\d+)\]\s+(?P<result>OK|FAIL)\s+(?P<company>.*?)\s+\|\s+(?P<fields>.+)$"
    )
    adapter_pattern = re.compile(r"^\d{2}:\d{2}:\d{2}\s+\|\s+Adapter metrics\s+\|\s+(?P<fields>.+)$")

    for line in lines:
        adapter_match = adapter_pattern.match(line.strip())
        if adapter_match:
            fields = _parse_log_key_values(adapter_match.group("fields"))
            adapter = str(fields.get("adapter", "")).strip().lower()
            if adapter not in EXPERIMENTAL_SOURCE_ADAPTERS:
                continue
            company_rows.append(
                {
                    "progress": "summary",
                    "result": "SUMMARY",
                    "company": f"{adapter} adapter totals",
                    "adapter": adapter,
                    "evaluated": int(float(fields.get("evaluated", "0") or 0)),
                    "persisted": int(float(fields.get("persisted", "0") or 0)),
                    "new": 0,
                    "dropped": 0,
                    "status": "",
                    "scrape_ms": float(fields.get("scrape_ms", "0") or 0),
                    "process_ms": float(fields.get("process_ms", "0") or 0),
                    "note": "",
                }
            )
            continue

        match = company_pattern.match(line.strip())
        if not match:
            continue
        fields_text = match.group("fields")
        note = ""
        if " note=" in fields_text:
            fields_text, note = fields_text.split(" note=", 1)
            note = note.strip()
        fields = _parse_log_key_values(fields_text)
        adapter = str(fields.get("adapter", "")).strip().lower()
        if adapter not in EXPERIMENTAL_SOURCE_ADAPTERS:
            continue
        company_rows.append(
            {
                "progress": match.group("progress"),
                "result": match.group("result"),
                "company": match.group("company").strip(),
                "adapter": adapter,
                "evaluated": int(float(fields.get("evaluated", "0") or 0)),
                "persisted": int(float(fields.get("persisted", "0") or 0)),
                "new": int(float(fields.get("new", "0") or 0)),
                "dropped": int(float(fields.get("dropped", "0") or 0)),
                "status": str(fields.get("status", "")).strip(),
                "scrape_ms": float(fields.get("scrape_ms", "0") or 0),
                "process_ms": float(fields.get("process_ms", "0") or 0),
                "note": note,
            }
        )
        if adapter == "jobspy" and note:
            for board_row in _parse_jobspy_board_metrics(note):
                board_row["company"] = match.group("company").strip()
                board_rows.append(board_row)

    return pd.DataFrame(company_rows), pd.DataFrame(board_rows)


def _bucket_thresholds_from_preferences() -> dict[str, float]:
    prefs = load_yaml(settings.prefs_yaml)
    rules = (((prefs.get("scoring") or {}).get("action_buckets") or {}).get("rules") or [])
    source_trust = ((prefs.get("scoring") or {}).get("source_trust") or {})
    apply_now = None
    review_today = None
    watch = None
    for rule in rules:
        label = str(rule.get("label") or "").strip().upper()
        when = rule.get("when") or {}
        min_score = when.get("min_score")
        if min_score is None:
            continue
        try:
            min_score = float(min_score)
        except (TypeError, ValueError):
            continue
        if label == "APPLY NOW":
            apply_now = min(min_score, apply_now) if apply_now is not None else min_score
        elif label == "REVIEW TODAY" and not when.get("tier_in") and not when.get("strong_title"):
            review_today = min(min_score, review_today) if review_today is not None else min_score
        elif label == "WATCH" and when.get("eligible", False):
            watch = min(min_score, watch) if watch is not None else min_score
    # search.bucket_thresholds is written by the Scoring Settings UI — prefer it when present
    bt = (prefs.get("search") or {}).get("bucket_thresholds") or {}
    for _key, _var in [("apply_now", apply_now), ("review_today", review_today), ("watch", watch)]:
        if bt.get(_key) is not None:
            try:
                v = float(bt[_key])
                if _key == "apply_now":
                    apply_now = v
                elif _key == "review_today":
                    review_today = v
                else:
                    watch = v
            except (TypeError, ValueError):
                pass
    comp_cfg = (prefs.get("search") or {}).get("compensation") or {}
    min_salary = float(comp_cfg.get("min_salary_usd") or comp_cfg.get("target_salary_usd") or 165000)
    return {
        "APPLY NOW": float(apply_now if apply_now is not None else 80.0),
        "REVIEW TODAY": float(review_today if review_today is not None else 74.0),
        "WATCH": float(watch if watch is not None else 55.0),
        "min_salary_usd": min_salary,
        "source_trust": source_trust,
    }


def _source_bucket_cap(row: pd.Series, thresholds: dict[str, object]) -> str | None:
    lane = str(row.get("source_lane") or "employer_ats").strip().lower() or "employer_ats"
    source_trust = dict(thresholds.get("source_trust") or {})
    if lane == "aggregator":
        canonical = str(row.get("canonical_job_url") or "").strip()
        key = "aggregator_with_canonical" if canonical else "aggregator_without_canonical"
    elif lane == "contractor":
        key = "contractor"
    else:
        key = "employer_ats"
    policy = dict(source_trust.get(key) or {})
    return str(policy.get("cap_bucket") or "").strip().upper() or None


def _ats_only_df(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty or "source_lane" not in df.columns:
        return df
    lanes = df["source_lane"].fillna("employer_ats").astype(str).str.lower()
    return df[~lanes.isin(["aggregator", "jobspy_experimental"])].copy()


def _format_score_details(raw: object) -> str:
    text = str(raw or "").strip()
    if not text.startswith("score="):
        return text
    pairs = dict(re.findall(r"([a-zA-Z_+-]+)=([^\s]+)", text))
    total = pairs.get("score", "—")
    title = pairs.get("title", "0")
    jd_plus = pairs.get("body+", "0")
    jd_minus = pairs.get("body-", "0")
    tier = pairs.get("tier", "0")
    comp = pairs.get("comp", "0")
    contract = pairs.get("contract", "0")
    location = pairs.get("location-", "0")
    return (
        f"Title {title} | JD +{jd_plus}/-{jd_minus} | Tier {tier} | "
        f"Comp {comp} | Contract {contract} | Location -{location} | Total {total}"
    )

def _safe_render(fn, *args, page_name: str = "", **kwargs):
    try: fn(*args, **kwargs)
    except Exception as e:
        if "Rerun" in type(e).__name__ or "Stop" in type(e).__name__: raise
        st.error(f"Error in {page_name}: {e}")
        import traceback
        st.code(traceback.format_exc())

def _invalidate_data_cache(): 
    _load_jobs_df.clear()
    _load_rejected_jobs_df.clear()


def _sidebar_metrics_for_df(df: pd.DataFrame) -> dict[str, int]:
    df = _ats_only_df(df)
    if df.empty or "status" not in df.columns:
        return {"scraped_leads": 0, "tracked": 0, "active": 0}
    statuses = df["status"].fillna("").astype(str).str.lower()
    entry_types = df.get("entry_type", pd.Series(["application"] * len(df), index=df.index)).fillna("application").astype(str).str.lower()
    effective_buckets = df.get("effective_bucket", pd.Series([""] * len(df), index=df.index)).fillna("").astype(str).str.upper()
    active_applications = int(((entry_types == "application") & statuses.isin(["applied", "screening", "interviewing", "offer"])).sum())
    tracked_non_app = int(((entry_types != "application") & ~statuses.isin(["rejected", "withdrawn", "accepted"])).sum())
    scraped_leads = active_applications + tracked_non_app + int((effective_buckets == "APPLY NOW").sum()) + int((effective_buckets == "REVIEW TODAY").sum())
    return {
        "scraped_leads": scraped_leads,
        "tracked": int((statuses != "considering").sum()),
        "active": active_applications,
    }


def _get_today_token_usage(conn) -> dict:
    """Get today's total LLM token usage and budget info."""
    try:
        today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        cursor = conn.execute(
            "SELECT SUM(tokens_used) as total, COUNT(*) as calls FROM llm_cost_log WHERE run_date = ?",
            (today,)
        )
        row = cursor.fetchone()
        total_tokens = row[0] if row and row[0] else 0
        call_count = row[1] if row and row[1] else 0
        daily_budget = settings.llm_daily_token_budget
        percent_used = 100 * total_tokens / daily_budget if daily_budget > 0 else 0
        return {
            "total_tokens": int(total_tokens),
            "call_count": int(call_count),
            "daily_budget": daily_budget,
            "percent_used": min(100, percent_used),  # Cap at 100 for display
        }
    except Exception:
        return {"total_tokens": 0, "call_count": 0, "daily_budget": 0, "percent_used": 0}


def _coerce_timestamp_series(series: pd.Series) -> pd.Series:
    return pd.to_datetime(series, errors="coerce", utc=True)


def _velocity_label(open_days: int, seen_count: int, days_since_seen: int) -> str:
    if days_since_seen > 7:
        return "Dormant"
    if open_days >= 60 and seen_count >= 3:
        return "Reposted"
    if open_days >= 45:
        return "Stale"
    if seen_count >= 3:
        return "Recurring"
    if open_days <= 7:
        return "New"
    return "Active"


def _decorate_role_velocity(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df
    out = df.copy()
    now = pd.Timestamp.now(tz="UTC")
    first_seen = _coerce_timestamp_series(
        out.get("first_seen_at", pd.Series([None] * len(out), index=out.index))
    )
    last_seen = _coerce_timestamp_series(
        out.get("last_seen_at", pd.Series([None] * len(out), index=out.index))
    )
    discovered = _coerce_timestamp_series(
        out.get("date_discovered", pd.Series([None] * len(out), index=out.index))
    )
    created = _coerce_timestamp_series(
        out.get("created_at", pd.Series([None] * len(out), index=out.index))
    )

    first_seen = first_seen.fillna(discovered).fillna(created)
    last_seen = last_seen.fillna(discovered).fillna(created)

    out["seen_count"] = pd.to_numeric(out.get("seen_count"), errors="coerce").fillna(0).astype(int)
    out.loc[(out["seen_count"] <= 0) & first_seen.notna(), "seen_count"] = 1
    out["open_days"] = first_seen.map(lambda ts: (now - ts).days if pd.notna(ts) else 0)
    out["days_since_seen"] = last_seen.map(lambda ts: (now - ts).days if pd.notna(ts) else 0)
    out["velocity"] = out.apply(
        lambda row: _velocity_label(
            int(row.get("open_days", 0) or 0),
            int(row.get("seen_count", 0) or 0),
            int(row.get("days_since_seen", 0) or 0),
        ),
        axis=1,
    )
    return out


def _role_velocity_summary(df: pd.DataFrame) -> dict[str, int]:
    if df.empty:
        return {"stale": 0, "reposted": 0, "dormant": 0}
    velocity = df.get("velocity", pd.Series([], dtype=str)).astype(str)
    return {
        "stale": int((velocity == "Stale").sum()),
        "reposted": int((velocity == "Reposted").sum()),
        "dormant": int((velocity == "Dormant").sum()),
    }


def _normalize_work_type(value: object) -> str:
    text = str(value or "").strip().lower()
    if not text:
        return "unknown"
    if text in WORK_TYPE_LABELS:
        return text
    if "1099" in text:
        return "1099_contract"
    if "c2c" in text or "corp" in text:
        return "c2c_contract"
    if "part" in text:
        return "part_time"
    if "temp" in text or "seasonal" in text:
        return "temporary"
    if "intern" in text or "co-op" in text:
        return "internship"
    if "w2" in text or "contract" in text:
        return "w2_contract"
    if "full" in text or "fte" in text or "salary" in text:
        return "fte"
    return text


def _work_type_label(value: object) -> str:
    normalized = _normalize_work_type(value)
    return WORK_TYPE_LABELS.get(normalized, normalized.replace("_", " ").title())


def _apply_work_type_filter(df: pd.DataFrame, selection: str) -> pd.DataFrame:
    if df.empty or selection == "All":
        return df
    if "work_type" not in df.columns:
        return df
    work_types = df.get("work_type", pd.Series(["unknown"] * len(df), index=df.index)).map(_normalize_work_type)
    if selection == "Contract Only":
        return df[work_types.isin({"w2_contract", "1099_contract", "c2c_contract", "contract"})]
    if selection == "Full-time Only":
        return df[work_types == "fte"]
    if selection == "Unknown Only":
        return df[work_types == "unknown"]
    target = next((key for key, label in WORK_TYPE_LABELS.items() if label == selection), None)
    if target:
        return df[work_types == target]
    return df

def _cache_stamp(*paths: Path) -> str:
    parts: list[str] = []
    for path in paths:
        try:
            stat = path.stat()
            parts.append(f"{path}:{stat.st_mtime_ns}:{stat.st_size}")
        except FileNotFoundError:
            parts.append(f"{path}:missing")
        # SQLite WAL mode: commits go to the -wal file without changing the main
        # DB file's mtime until a checkpoint.  Include the WAL in the stamp so
        # external writes (bookmarklet injections) invalidate the cache.
        wal = Path(str(path) + "-wal")
        try:
            wstat = wal.stat()
            parts.append(f"{wal}:{wstat.st_mtime_ns}:{wstat.st_size}")
        except FileNotFoundError:
            pass  # no WAL file = no pending writes, nothing to add
    return "|".join(parts)

@st.cache_data(show_spinner=False)
def _load_jobs_df(_stamp: str) -> pd.DataFrame:
    conn = ats_db.get_connection()
    try:
        rows = ats_db.get_applications(conn)
        df = pd.DataFrame([dict(r) for r in rows])
        if df.empty: return df
        
        # Canonical Filter
        if "is_canonical" in df.columns:
            df = df[df["is_canonical"] == 1].copy()
        
        df = df.rename(columns={"role": "title", "job_url": "url", "scraper_key": "_key"})
        ann_rows = ats_db.get_all_annotations(conn)
        ann_map = {str(r["job_key"]): dict(r) for r in ann_rows}
        df = _decorate_role_velocity(df)
        
        now = datetime.now()
        df["age_days"] = df["date_discovered"].apply(
            lambda d: (now - datetime.fromisoformat(str(d).split('T')[0])).days if d and str(d).strip() else 0
        )
        
        thresholds = _bucket_thresholds_from_preferences()

        def bucket(row):
            s = str(row.get("status", "")).lower()
            if s in ("applied", "screening", "interviewing", "offer", "accepted"): return "Applied"
            if s == "rejected": return "Rejected"
            if s == "withdrawn": return "Filtered Out"
            tag = str(ann_map.get(str(row.get("_key", "")), {}).get("tag", "") or "").strip().upper()
            if tag in {"APPLY NOW", "REVIEW TODAY", "WATCH", "FILTERED OUT"}:
                return "Filtered Out" if tag == "FILTERED OUT" else tag
            sc = float(row.get("score", 0) or 0)
            bucket_name = "Filtered Out"
            
            # Check if the job is eligible for APPLY NOW (hard gates from Scorer)
            # We check the decision_reason or similar if we don't have the flag in DB yet.
            # But the most reliable way is if we have 'apply_now_eligible' in the row.
            is_eligible = bool(row.get("apply_now_eligible", True))
            
            if sc >= thresholds["APPLY NOW"] and is_eligible:
                bucket_name = "APPLY NOW"
            elif sc >= thresholds["REVIEW TODAY"]:
                bucket_name = "REVIEW TODAY"
            elif sc >= thresholds["WATCH"]:
                bucket_name = "WATCH"
            cap = _source_bucket_cap(row, thresholds)
            if cap == "WATCH" and bucket_name in {"APPLY NOW", "REVIEW TODAY"}:
                return "WATCH"
            if cap == "REVIEW TODAY" and bucket_name == "APPLY NOW":
                return "REVIEW TODAY"
            return bucket_name
            
        df["effective_bucket"] = df.apply(bucket, axis=1)
        def user_status(row):
            s = str(row.get("status", "")).lower()
            if s in {"applied", "rejected"}:
                return str(s).capitalize()
            tag = str(ann_map.get(str(row.get("_key", "")), {}).get("tag", "") or "").strip().upper()
            if tag == "APPLY NOW":
                return "Apply Now"
            if tag == "REVIEW TODAY":
                return "Review Today"
            if tag == "WATCH":
                return "Watch"
            if tag == "FILTERED OUT":
                return "Filtered Out"
            return "Considering"
        df["user_status"] = df.apply(user_status, axis=1)
        df["note"] = df["_key"].map(lambda k: ann_map.get(str(k), {}).get("note", ""))
        df["source"] = df.get("source", pd.Series([""] * len(df), index=df.index)).fillna("").astype(str)
        df["source_lane_label"] = df.get("source_lane", pd.Series(["employer_ats"] * len(df), index=df.index)).map(_source_lane_label)
        
        for c in ("score", "fit_stars", "salary_low", "salary_high", "tier"):
            if c in df.columns: df[c] = pd.to_numeric(df[c], errors="coerce")
        if "decision_reason" in df.columns:
            df["decision_reason"] = df["decision_reason"].map(_format_score_details)
            
        return df
    finally: conn.close()

@st.cache_data(show_spinner=False)
def _load_rejected_jobs_df(_stamp: str) -> pd.DataFrame:
    path = settings.rejected_csv
    if not path.exists():
        return pd.DataFrame()
    try:
        df = pd.read_csv(path)
    except Exception:
        return pd.DataFrame()
    if df.empty:
        return df
    for col in ["matched_keywords", "penalized_keywords", "decision_reason", "drop_reason", "url"]:
        if col in df.columns:
            df[col] = df[col].astype(str)
    if "decision_reason" in df.columns:
        df["decision_reason"] = df["decision_reason"].map(_format_score_details)
    return df

def _load_manual_review_lines():
    path = settings.manual_review_file
    if not path.exists():
        return []
    try:
        lines = [line.strip() for line in path.read_text(encoding="utf-8").splitlines() if line.strip()]
        return [line for line in lines if "|" in line and not line.startswith("Generated:") and not line.startswith("Blocked sites")]
    except Exception:
        return []


def _parse_manual_review_lines(lines):
    rows = []
    for line in lines:
        parts = [part.strip() for part in line.split("|")]
        if not parts:
            continue
        row = {"company": parts[0], "adapter": "", "note": "", "url": ""}
        for part in parts[1:]:
            if "=" in part:
                key, value = part.split("=", 1)
                row[key.strip()] = value.strip()
        rows.append(row)
    return rows


def _load_registry_companies() -> list[dict]:
    all_companies = []
    for path in settings.get_company_registries():
        if not path.exists():
            continue
        data = load_yaml(path)
        all_companies.extend(list(data.get("companies", []) or []))
    return all_companies


def _registry_company_lookup() -> dict[str, dict]:
    return {
        str(company.get("name", "")).strip().lower(): dict(company)
        for company in _load_registry_companies()
        if str(company.get("name", "")).strip()
    }


def _latest_pipeline_company_statuses() -> list[dict]:
    path = settings.results_dir / "job_search_v6.log"
    if not path.exists():
        return []
    pattern = re.compile(
        r"""
        ^\d{2}:\d{2}:\d{2}\s+\|\s+\[\d+/\d+\]\s+
        (?P<result>OK|FAIL)\s+
        (?P<company>.*?)\s+\|\s+
        (?P<fields>.+)$
        """,
        re.VERBOSE,
    )
    latest: dict[str, dict] = {}
    try:
        lines = path.read_text(encoding="utf-8", errors="replace").splitlines()
    except Exception:
        return []
    for line in lines:
        match = pattern.match(line.strip())
        if not match:
            continue
        company = match.group("company").strip()
        if not company:
            continue
        fields_text = match.group("fields")
        fields: dict[str, str] = {}
        note_match = re.search(r"\snote=(.+)$", fields_text)
        note = note_match.group(1).strip() if note_match else ""
        base_text = fields_text[: note_match.start()] if note_match else fields_text
        for key, value in re.findall(r"([a-zA-Z_]+)=([^\s]+)", base_text):
            fields[key] = value
        latest[company.lower()] = {
            "company": company,
            "result": match.group("result"),
            "adapter": fields.get("adapter", ""),
            "status": fields.get("status", "").strip(),
            "evaluated": int(float(fields.get("evaluated", "0") or 0)),
            "persisted": int(float(fields.get("persisted", "0") or 0)),
            "note": note,
        }
    return list(latest.values())


def _build_manual_review_items() -> list[dict]:
    registry = _registry_company_lookup()
    items: dict[str, dict] = {}

    def _upsert(item: dict) -> None:
        company = str(item.get("company", "")).strip()
        if not company:
            return
        key = company.lower()
        existing = items.get(key)
        if not existing:
            items[key] = item
            return
        for field in ("adapter", "url", "note", "queue_source", "status"):
            if not existing.get(field) and item.get(field):
                existing[field] = item[field]
        if existing.get("queue_source") != "manual_only" and item.get("queue_source") == "manual_only":
            existing["queue_source"] = "manual_only"

    for company in registry.values():
        company_name = str(company.get("name", "")).strip()
        if not company_name:
            continue
        url = str(company.get("careers_url", "") or "")
        adapter = str(company.get("adapter", "") or "")
        status = str(company.get("status", "") or "")
        manual_only = bool(company.get("manual_only"))
        manual_suggested = bool(company.get("manual_only_suggested"))
        notes = str(company.get("notes", "") or "").strip()
        if manual_only or status == "manual_only" or status == "broken" or manual_suggested:
            note = notes
            if not note:
                if status == "broken":
                    note = "Marked broken in registry (healer failed)."
                elif manual_suggested:
                    note = "Manual review suggested in registry."
                else:
                    note = "Marked manual review / manual-only in registry."
            _upsert(
                {
                    "company": company_name,
                    "adapter": adapter,
                    "url": url,
                    "note": note,
                    "queue_source": "manual_only",
                    "status": status if status else "manual_only",
                }
            )

    failing_statuses = {"empty", "blocked", "low_signal", "budget_exhausted"}
    for entry in _latest_pipeline_company_statuses():
        company_name = entry["company"]
        registry_company = registry.get(company_name.lower(), {})
        status = str(entry.get("status", "") or "")
        evaluated = int(entry.get("evaluated", 0) or 0)
        persisted = int(entry.get("persisted", 0) or 0)
        is_failure = (
            entry.get("result") == "FAIL"
            or status in failing_statuses
            or (status == "ok" and evaluated == 0 and persisted == 0)
        )
        if not is_failure:
            continue
        url = str(registry_company.get("careers_url", "") or "")
        adapter = str(entry.get("adapter") or registry_company.get("adapter") or "")
        note = str(entry.get("note") or "").strip()
        if not note:
            if entry.get("result") == "FAIL":
                note = "Latest pipeline run failed to scrape this company."
            elif status == "ok":
                note = "Latest pipeline run found no listings."
            else:
                note = f"Latest pipeline status: {status}"
        _upsert(
            {
                "company": company_name,
                "adapter": adapter,
                "url": url,
                "note": note,
                "queue_source": "latest_run",
                "status": status or entry.get("result", "").lower(),
            }
        )

    for item in _parse_manual_review_lines(_load_manual_review_lines()):
        queue_source = "manual_review_file"
        if not item.get("url"):
            item["url"] = str(registry.get(item["company"].lower(), {}).get("careers_url", "") or "")
        item.setdefault("queue_source", queue_source)
        _upsert(item)

    return sorted(items.values(), key=lambda row: (row.get("company", "").lower(), row.get("queue_source", "")))


def _disable_company_in_registry(company_name: str) -> bool:
    data = load_yaml(settings.companies_yaml)
    companies = data.get("companies", [])
    changed = False
    for company in companies:
        if str(company.get("name", "")).lower() == company_name.lower():
            company["active"] = False
            company["status"] = "manual_only"
            company["manual_only"] = True
            notes = str(company.get("notes", "") or "")
            note_add = "Marked manual-only from manual review queue."
            if note_add not in notes:
                company["notes"] = f"{notes}\n{note_add}".strip()
            changed = True
            break
    if changed:
        data["companies"] = companies
        save_yaml(settings.companies_yaml, data)
    return changed

def _update_company_url_in_registry(company_name: str, new_url: str) -> bool:
    # Try to find which registry has the company
    registries = [
        settings.companies_yaml,
        settings.contract_companies_yaml,
        settings.aggregator_companies_yaml,
        settings.jobspy_companies_yaml,
    ]
    for path in registries:
        if not path.exists(): continue
        data = load_yaml(path)
        companies = data.get("companies", [])
        changed = False
        for company in companies:
            if str(company.get("name", "")).lower() == company_name.lower():
                company["careers_url"] = new_url.strip()
                changed = True
                break
        if changed:
            data["companies"] = companies
            save_yaml(path, data)
            return True
    return False

def _render_apply_now_cards(df):
    for _, r in df.iterrows():
        key, co, ti, sc, url = r["_key"], str(r.get("company","")), str(r.get("title","")), r.get("score"), str(r.get("url",""))
        desc = str(r.get("description_excerpt", "") or "")
        ai_analysis_raw = r.get("ai_analysis")
        ai_score = r.get("ai_match_score")
        enriched_data_raw = r.get("enriched_data")

        c1, c2, c3, c4 = st.columns([4, 1.5, 1.5, 2])
        lane_label = r.get("source_lane_label", "Employer ATS")
        source_name = r.get("source", "Direct")
        age_days = r.get("age_days", 0) or 0
        age_badge = f" <span style='background:#f59e0b;color:#fff;font-size:0.7rem;padding:1px 5px;border-radius:3px'>{int(age_days)}d old</span>" if age_days > 14 else ""
        c1.markdown(f"**{co}** — {ti} <small>score {sc:.0f}</small>{age_badge}  \n<small style='color: #6b7280'>{source_name} • {lane_label}</small>", unsafe_allow_html=True)
        if url: c2.markdown(f"[🔗 Open]({url})")
        
        if c3.button("✨ AI Analysis", key=f"ai_{key}"):
            conn = ats_db.get_connection()
            try:
                google_key = ats_db.get_setting(conn, "google_api_key", default=os.getenv("GOOGLE_API_KEY", ""))
                openai_key = ats_db.get_setting(conn, "openai_api_key", default=os.getenv("OPENAI_API_KEY", ""))
                resume_text = ats_db.get_setting(conn, "base_resume_text", default="")

                if not google_key and not openai_key:
                    st.error("Add a Google API Key or OpenAI API Key in Settings first.")
                elif not resume_text:
                    st.error("Upload a Base Resume in Settings first.")
                else:
                    with st.spinner("AI Analyzing..."):
                        from jobsearch.services.profile_service import ProfileService
                        profiler = ProfileService(google_api_key=google_key, openai_api_key=openai_key)
                        analysis = profiler.analyze_job_fit(ti, desc, resume_text)
                        if analysis:
                            now = datetime.now(timezone.utc).isoformat()
                            conn.execute(
                                "UPDATE applications SET ai_analysis=?, ai_match_score=?, updated_at=? WHERE scraper_key=?",
                                (json.dumps(analysis), analysis.get("match_score"), now, key)
                            )
                            conn.commit()
                            _invalidate_data_cache()
                            st.rerun()
            finally:
                conn.close()

        if c4.button("✅ Apply & Track", key=f"at_{key}"):
            conn = ats_db.get_connection(); now = datetime.now(timezone.utc).isoformat()
            conn.execute("UPDATE applications SET status='applied', date_applied=?, updated_at=? WHERE scraper_key=?", (now[:10], now, key))
            res = conn.execute("SELECT id FROM applications WHERE scraper_key=?", (key,)).fetchone()
            if res: ats_db.add_event(conn, res["id"], "applied", now, title=f"Applied to {co}")
            conn.commit(); conn.close(); _invalidate_data_cache(); st.rerun()
        
        if ai_analysis_raw:
            try:
                analysis = json.loads(ai_analysis_raw)
                score_color = "#10b981" if ai_score and ai_score >= 80 else "#f59e0b" if ai_score and ai_score >= 60 else "#ef4444"
                positioning = analysis.get("positioning_angle", "")
                interview_lead = analysis.get("interview_lead", "")
                key_objection = analysis.get("key_objection", "")
                with st.container(border=True):
                    st.markdown(
                        f"""
                        <div style="display: flex; justify-content: space-between; align-items: center;">
                            <strong style="color: {score_color}">AI Match Score: {ai_score:.0f}%</strong>
                            {"<span style='font-size:0.8rem;color:#6b7280'>" + positioning + "</span>" if positioning else ""}
                        </div>
                        <p style="margin: 5px 0; font-size: 0.9rem;">{analysis.get('summary', '')}</p>
                        <div style="display: flex; gap: 20px; font-size: 0.8rem;">
                            <div><strong style="color: #10b981">Pros:</strong> {', '.join(analysis.get('pros', []))}</div>
                            <div><strong style="color: #ef4444">Cons:</strong> {', '.join(analysis.get('cons', []))}</div>
                        </div>
                        {"<div style='font-size:0.8rem;margin-top:5px;'><strong style='color:#6366f1'>Missing:</strong> " + ', '.join(analysis.get('missing_skills', [])) + "</div>" if analysis.get('missing_skills') else ""}
                        {"<div style='font-size:0.8rem;margin-top:5px;background:#1e293b;padding:5px 8px;border-radius:4px;'><strong style='color:#fbbf24'>Lead with:</strong> " + interview_lead + "</div>" if interview_lead else ""}
                        {"<div style='font-size:0.8rem;margin-top:3px;background:#1e293b;padding:5px 8px;border-radius:4px;'><strong style='color:#f87171'>Prepare for:</strong> " + key_objection + "</div>" if key_objection else ""}
                        """,
                        unsafe_allow_html=True
                    )
            except: pass

        # Display missing skills from enriched data analysis
        if enriched_data_raw:
            try:
                enriched = json.loads(enriched_data_raw)
                missing_skills = enriched.get("missing_skills", [])
                if missing_skills:
                    with st.container(border=True):
                        st.markdown(
                            f"""
                            <strong style="color: #f59e0b">Skills Gap:</strong> {', '.join(missing_skills)}
                            """,
                            unsafe_allow_html=True
                        )
            except: pass

        # Talking Points — generate and cache per job
        talking_points_raw = r.get("talking_points")
        with st.expander("✍️ Talking Points"):
            if talking_points_raw:
                try:
                    tp = json.loads(talking_points_raw)
                    st.markdown(f"**Opening:** {tp.get('opening', '')}")
                    for b in tp.get("bullets", []):
                        if b:
                            st.markdown(f"- {b}")
                    if tp.get("company_line"):
                        st.markdown(f"*{tp['company_line']}*")
                except Exception:
                    st.write(talking_points_raw)
            else:
                if st.button("Generate Talking Points", key=f"tp_{key}"):
                    conn = ats_db.get_connection()
                    try:
                        google_key = ats_db.get_setting(conn, "google_api_key", default=os.getenv("GOOGLE_API_KEY", ""))
                        openai_key = ats_db.get_setting(conn, "openai_api_key", default=os.getenv("OPENAI_API_KEY", ""))
                        resume_text = ats_db.get_setting(conn, "base_resume_text", default="")
                        if not google_key and not openai_key:
                            st.error("Add a Google or OpenAI API key in Settings first.")
                        elif not resume_text:
                            st.error("Upload a Base Resume in Settings first.")
                        else:
                            with st.spinner("Generating talking points..."):
                                from jobsearch.services.talking_points_service import TalkingPointsService
                                svc = TalkingPointsService(google_api_key=google_key, openai_api_key=openai_key)
                                matched_kw = []
                                result = svc.generate(ti, co, desc, resume_text, matched_kw)
                                if result:
                                    now = datetime.now(timezone.utc).isoformat()
                                    conn.execute(
                                        "UPDATE applications SET talking_points=?, updated_at=? WHERE scraper_key=?",
                                        (json.dumps(result), now, key),
                                    )
                                    conn.commit()
                                    _invalidate_data_cache()
                                    st.rerun()
                                else:
                                    st.error("Failed to generate talking points. Check API key and try again.")
                    finally:
                        conn.close()
                else:
                    st.caption("Click to generate AI-powered talking points tailored to this role.")

        st.markdown("<hr style='margin:2px 0;border-color:#374151'>", unsafe_allow_html=True)

def load_yaml(p): return yaml.safe_load(p.read_text(encoding="utf-8")) if p.exists() else {}
def save_yaml(p, d): p.write_text(yaml.safe_dump(d, sort_keys=False, allow_unicode=True), encoding="utf-8")


def _rescore_saved_jobs(conn, prefs_path: Path | None = None) -> int:
    prefs = load_yaml(prefs_path or settings.prefs_yaml)
    scorer = Scorer(prefs)
    try:
        from jobsearch.scraper.scoring_v2 import score_job_v2
        v2_cfg, v2_title_index, _v2_pw, _v2_ftbase, _v2_ftmin = _build_v2_config_from_prefs(prefs)
        _v2_available = True
    except Exception:
        _v2_available = False
        _v2_pw, _v2_ftbase, _v2_ftmin = {}, 50.0, 8
    rows = conn.execute(
        """
        SELECT id, company, role, description_excerpt, location, tier,
               salary_text, salary_low, salary_high, work_type,
               compensation_unit, hourly_rate, hours_per_week, weeks_per_year,
               is_remote, status, score
        FROM applications
        WHERE status = 'considering'
        """
    ).fetchall()
    updated = 0
    now = datetime.now(timezone.utc).isoformat()
    for row in rows:
        result = scorer.score_job(
            {
                "title": row["role"] or "",
                "description": row["description_excerpt"] or "",
                "location": row["location"] or "",
                "tier": row["tier"] or 4,
                "salary_text": row["salary_text"] or "",
                "salary_min": row["salary_low"],
                "salary_max": row["salary_high"],
                # Pass empty work_type so _derive_work_type re-detects from blob —
                # this clears false-positive "internship" tags from the intern substring bug.
                "work_type": "",
                "compensation_unit": row["compensation_unit"] or "",
                "hourly_rate": row["hourly_rate"],
                "hours_per_week": row["hours_per_week"],
                "weeks_per_year": row["weeks_per_year"],
                # If stored as remote, trust it (the API was authoritative at scrape time).
                # If stored as non-remote, pass None so the scorer re-detects from
                # location/description text — this fixes jobs where is_remote was
                # incorrectly stored as 0 (e.g., "Remote within US" stored as 0).
                "is_remote": True if row["is_remote"] else None,
            }
        )
        is_remote_detected = int(
            bool(row["is_remote"]) or
            scorer._is_remote_role(row["location"] or "", row["description_excerpt"] or "")
        )
        new_score = result.get("score") or 0.0
        new_fit_band = result.get("fit_band")
        new_decision_reason = result.get("decision_reason")
        v2_result = None

        # V1 hard gates (Disqualified / Filtered Out) are authoritative — keep score=0.
        # For everything else, promote V2 as the primary score if available.
        v1_disqualified = new_fit_band in ("Disqualified", "Filtered Out")
        if _v2_available and not v1_disqualified:
            try:
                _tpts = _v2_title_pts(row["role"] or "", v2_title_index, v2_cfg, _v2_pw, _v2_ftbase, _v2_ftmin)
                v2_result = score_job_v2(
                    row["role"] or "",
                    row["description_excerpt"] or "",
                    _tpts,
                    v2_cfg,
                    v2_title_index,
                )
                new_score = round(v2_result.final_score, 2)
                new_fit_band = v2_result.fit_band
                import json as _json
                new_decision_reason = (
                    f"V2: {v2_result.fit_band} | title={v2_result.canonical_title or 'unresolved'}"
                    f" | seniority={v2_result.seniority.band}"
                    f" | anchor={round(v2_result.keyword.anchor_score, 1)}"
                    f" base={round(v2_result.keyword.baseline_score, 1)}"
                    f" neg={round(v2_result.keyword.negative_score, 1)}"
                )
            except Exception:
                pass  # fall back to V1 values already set above

        existing_score = row["score"] if "score" in row.keys() else 0.0
        # Never overwrite a non-zero score with zero — zero means the title gate
        # filtered the job, not that it's a bad match. Only write if we computed a
        # real score, or if the job was already zero (nothing to protect).
        if new_score > 0 or (existing_score or 0) == 0:
            conn.execute(
                """
                UPDATE applications
                SET score = ?,
                    fit_band = ?,
                    matched_keywords = ?,
                    penalized_keywords = ?,
                    decision_reason = ?,
                    work_type = ?,
                    compensation_unit = ?,
                    hourly_rate = ?,
                    hours_per_week = ?,
                    weeks_per_year = ?,
                    normalized_compensation_usd = ?,
                    is_remote = ?,
                    updated_at = ?
                WHERE id = ?
                """,
                (
                    new_score,
                    new_fit_band,
                    result.get("matched_keywords"),
                    result.get("penalized_keywords"),
                    new_decision_reason,
                    result.get("work_type"),
                    result.get("compensation_unit"),
                    result.get("hourly_rate"),
                    result.get("hours_per_week"),
                    result.get("weeks_per_year"),
                    result.get("normalized_compensation_usd"),
                    is_remote_detected,
                    now,
                    row["id"],
                ),
            )
        if v2_result is not None:
            import json as _json
            conn.execute(
                """
                UPDATE applications
                SET score_v2 = ?, fit_band_v2 = ?, v2_canonical_title = ?,
                    v2_seniority_band = ?, v2_anchor_score = ?,
                    v2_baseline_score = ?, v2_flags = ?
                WHERE id = ?
                """,
                (
                    round(v2_result.final_score, 2),
                    v2_result.fit_band,
                    v2_result.canonical_title,
                    v2_result.seniority.band,
                    round(v2_result.keyword.anchor_score, 2),
                    round(v2_result.keyword.baseline_score, 2),
                    _json.dumps(v2_result.flags),
                    row["id"],
                ),
            )
        updated += 1
    conn.commit()
    return updated

def _build_v2_config_from_prefs(prefs: dict):
    """Thin wrapper — delegates to the canonical implementation in scoring_v2."""
    from jobsearch.scraper.scoring_v2 import build_v2_config_from_prefs
    return build_v2_config_from_prefs(prefs)


def _v2_title_pts(role, title_index, cfg, positive_weights, fast_track_base, fast_track_min):
    """Thin wrapper — delegates to the canonical implementation in scoring_v2."""
    from jobsearch.scraper.scoring_v2 import v2_title_pts
    return v2_title_pts(role, title_index, cfg, positive_weights, fast_track_base, fast_track_min)


def _normalize_editor_value(value):
    if isinstance(value, list):
        return " | ".join(str(item) for item in value if item is not None)
    if isinstance(value, dict):
        return json.dumps(value, ensure_ascii=False)
    return value

def _parse_pipe_list(value: str):
    return [item.strip() for item in str(value or "").split("|") if item.strip()]


def _coerce_company_editor_value(key: str, value):
    if pd.isna(value):
        return None
    if key in {"active", "manual_only", "manual_only_suggested"}:
        if isinstance(value, bool):
            return value
        return str(value).strip().lower() in {"1", "true", "yes", "y"}
    if key in {"tier", "heal_failure_streak"}:
        text = str(value).strip()
        if not text:
            return 0
        try:
            return int(float(text))
        except (ValueError, TypeError):
            return 0
    if key == "industry":
        text = str(value or "").strip()
        return _parse_pipe_list(text) if "|" in text else text
    if key in {"notes", "sub_industry", "adapter", "adapter_key", "careers_url", "domain", "priority", "status", "discovery_method", "heal_last_failure_detail", "cooldown_until", "last_healed", "name"}:
        return str(value or "").strip()
    return value


def _annualized_compensation_preview(
    comp_type: str,
    amount: float,
    hours_per_week: float,
    weeks_per_year: float,
    contractor_cfg: dict,
) -> dict[str, float]:
    benefits = float(contractor_cfg.get("benefits_replacement_usd", 18000))
    w2_gap = float(contractor_cfg.get("w2_benefits_gap_usd", 6000))
    overhead_1099_pct = float(contractor_cfg.get("overhead_1099_pct", 0.18))

    if comp_type == "salary":
        gross = amount
        normalized = amount
    else:
        gross = amount * hours_per_week * weeks_per_year
        if comp_type == "w2_hourly":
            normalized = gross - w2_gap
        else:
            normalized = gross * (1.0 - overhead_1099_pct) - benefits
    return {"gross_annual_usd": gross, "normalized_compensation_usd": normalized}


def _companies_file_label(path: Path) -> str:
    name = path.name
    if name == settings.companies_yaml.name:
        return f"{name} (main list)"
    if name == "job_search_companies_test.yaml":
        return f"{name} (test list)"
    if name == settings.contract_companies_yaml.name:
        return f"{name} (contractor list)"
    if name == settings.aggregator_companies_yaml.name:
        return f"{name} (aggregator list)"
    if name == settings.jobspy_companies_yaml.name:
        return f"{name} (jobspy experimental list)"
    if name == "job_search_companies_contract_test.yaml":
        return f"{name} (legacy test list)"
    
    # Prettify dynamic registry names: job_search_companies_fintech.yaml -> fintech list
    label = name.replace("job_search_companies_", "").replace(".yaml", "").replace("_", " ")
    return f"{name} ({label} list)"


def _extract_docx_text(file_bytes: bytes) -> str:
    with zipfile.ZipFile(io.BytesIO(file_bytes)) as zf:
        xml_bytes = zf.read("word/document.xml")
    root = ET.fromstring(xml_bytes)
    ns = {"w": "http://schemas.openxmlformats.org/wordprocessingml/2006/main"}
    parts = []
    for para in root.findall(".//w:p", ns):
        texts = [node.text for node in para.findall(".//w:t", ns) if node.text]
        if texts:
            parts.append("".join(texts))
    return "\n".join(parts).strip()


def _extract_pdf_text(file_bytes: bytes) -> str:
    try:
        from pypdf import PdfReader
    except Exception as exc:
        raise RuntimeError("PDF import requires the 'pypdf' package in the runtime environment.") from exc
    reader = PdfReader(io.BytesIO(file_bytes))
    pages = []
    for page in reader.pages:
        text = page.extract_text() or ""
        if text.strip():
            pages.append(text.strip())
    return "\n\n".join(pages).strip()


def _extract_resume_text(uploaded_file) -> tuple[str, str]:
    suffix = Path(uploaded_file.name or "").suffix.lower()
    file_bytes = uploaded_file.getvalue()
    if suffix == ".txt":
        return file_bytes.decode("utf-8", errors="replace"), "txt"
    if suffix == ".docx":
        return _extract_docx_text(file_bytes), "docx"
    if suffix == ".pdf":
        return _extract_pdf_text(file_bytes), "pdf"
    raise ValueError(f"Unsupported resume format: {suffix or 'unknown'}")

def main():
    # Initialize logging for the dashboard session
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
        datefmt="%H:%M:%S",
        handlers=[logging.StreamHandler(sys.stdout)],
    )
    # Reduce noise from third-party libraries
    logging.getLogger("urllib3").setLevel(logging.WARNING)
    logging.getLogger("selenium").setLevel(logging.WARNING)
    logging.getLogger("playwright").setLevel(logging.WARNING)

    start_api_server()
    st.set_page_config(page_title="Job Search", page_icon="💼", layout="wide")
    set_custom_style()
    
    with st.sidebar:
        st.title("💼 Job Search")
        st.caption(f"v{__version__}")
        st.markdown("[☕ Buy Me a Coffee](https://www.buymeacoffee.com/ericdipietro)")
        st.markdown("<div style='margin-bottom: 1rem;'></div>", unsafe_allow_html=True)
        
        # Initialize page state
        if "page" not in st.session_state:
            st.session_state.page = "Action Center"

        def nav_item(label, key=None):
            active = st.session_state.page == label
            if st.button(
                label, 
                key=key or f"nav_{label}", 
                use_container_width=True, 
                type="primary" if active else "secondary"
            ):
                st.session_state.page = label
                st.rerun()

        def nav_heading(text):
            st.markdown(f"<div style='margin-top: 1.5rem; margin-bottom: 0.5rem;'><small style='color: #6b7280; font-weight: 600; text-transform: uppercase;'>{text}</small></div>", unsafe_allow_html=True)

        nav_heading("Work Today")
        nav_item("Action Center")
        nav_item("Job Matches")
        nav_item("My Applications")
        nav_item("Submission Review")
        nav_item("Tailoring Studio")

        nav_heading("Insights")
        nav_item("Dashboard")
        nav_item("Learning Loop")
        nav_item("Market Strategy")
        nav_item("Analytics")

        nav_heading("Company & Network")
        nav_item("Company Profiles")
        nav_item("Contacts")

        nav_heading("Preparation")
        nav_item("Question Bank")
        nav_item("Message Templates")
        nav_item("Training Tracker")
        nav_item("Journal")

        nav_heading("Tracking")
        nav_item("Pipeline")
        nav_item("Weekly Activity")

        nav_heading("Operations")
        # Run Search is a primary action button
        if st.button("🚀 Run Search", use_container_width=True, type="primary" if st.session_state.page == "Run Search" else "secondary"):
            st.session_state.page = "Run Search"
            st.rerun()
        nav_item("Search Settings")
        nav_item("Target Companies")

        st.markdown("---")
        if st.button("↺ Refresh Data", use_container_width=True):
            st.cache_data.clear()
            st.rerun()

        # Check for pending high-score alerts in sidebar
        try:
            sidebar_conn = ats_db.get_connection()
            pending_alerts = int(ats_db.get_setting(sidebar_conn, "pending_alerts") or "0")
            sidebar_conn.close()
            if pending_alerts > 0:
                st.warning(f"🔥 {pending_alerts} new high-match job{'s' if pending_alerts != 1 else ''}")
        except Exception:
            pass

        df_all = _load_jobs_df(_cache_stamp(settings.db_path, settings.prefs_yaml))
        if not df_all.empty:
            sidebar_metrics = _sidebar_metrics_for_df(df_all)
            st.metric("Leads", sidebar_metrics["scraped_leads"])
            st.metric("Active", sidebar_metrics["active"])
    
    # Route to the selected page
    page = st.session_state.page

    if page == "Dashboard":
        conn = ats_db.get_connection(); _safe_render(render_home, conn, page_name="Dashboard")
    
    elif page == "My Applications":
        conn = ats_db.get_connection(); _safe_render(render_tracker, conn, page_name="My Applications")

    elif page == "Job Matches":
        st.title("Job Matches")

        # Check for pending high-score job alerts from auto-refresh
        try:
            alert_conn = ats_db.get_connection()
            pending_alerts = scheduler.get_and_clear_alerts(alert_conn)
            if pending_alerts > 0:
                if pending_alerts == 1:
                    st.toast("🔥 1 new high-match job found!", icon="🔔")
                else:
                    st.toast(f"🔥 {pending_alerts} new high-match jobs found!", icon="🔔")
            alert_conn.close()
        except Exception:
            pass  # Silently ignore scheduler errors

        rejected_df = _load_rejected_jobs_df(_cache_stamp(settings.rejected_csv))
        manual_review_items = _build_manual_review_items()
        review_conn = ats_db.get_connection()
        try:
            review_actions = {row["company"]: dict(row) for row in ats_db.get_manual_review_actions(review_conn)}
        finally:
            review_conn.close()
        unresolved_manual_review = [
            item
            for item in manual_review_items
            if review_actions.get(item["company"], {}).get("resolution", "new") not in {"resolved", "ignored", "disabled"}
        ]
        if df_all.empty and rejected_df.empty and not manual_review_items:
            st.info("No jobs yet. Run the pipeline.")
            return
        # Duplicate and Search Control
        c1, c2 = st.columns(2)
        with c1:
            use_fts_search = st.checkbox(
                "🔎 Use Full-Text Search",
                value=False,
                help="Use database-level full-text search for faster queries on large job lists.",
            )
        with c2:
            hide_duplicates = st.checkbox(
                "🔗 Hide suspected duplicates",
                value=True,
                help="Show only the highest-trust canonical version of each job posting.",
            )

        if hide_duplicates:
            df_all = df_all[df_all.get("is_canonical", 1) == 1].copy()

        velocity_summary = _role_velocity_summary(df_all)
        vm1, vm2, vm3 = st.columns(3)
        vm1.metric("Older Postings", velocity_summary["stale"])
        vm2.metric("Reposted Jobs", velocity_summary["reposted"])
        vm3.metric("No Longer Listed", velocity_summary["dormant"])
        
        # Use filtered df_all for work type metrics
        ats_df = _ats_only_df(df_all)
        match_population = ats_df[ats_df["effective_bucket"].isin(["APPLY NOW", "REVIEW TODAY", "WATCH"])].copy()
        work_type_series = match_population.get("work_type", pd.Series(["unknown"] * len(match_population), index=match_population.index)).map(_normalize_work_type)
        contractor_count = int(work_type_series.isin({"w2_contract", "1099_contract", "c2c_contract", "contract"}).sum())
        fte_count = int((work_type_series == "fte").sum())
        unknown_count = int((work_type_series == "unknown").sum())
        wt1, wt2, wt3, wt4 = st.columns([1, 1, 1, 2])
        wt1.metric("Contract Roles", contractor_count)
        wt2.metric("Full-time Roles", fte_count)
        wt3.metric("Employment Type Unknown", unknown_count)
        work_type_filter = wt4.selectbox(
            "Filter by Employment Type",
            [
                "All",
                "Contract Only",
                "Full-time Only",
                "W2 hourly",
                "1099 hourly",
                "Corp-to-corp",
                "Unknown Only",
            ],
            index=0,
        )

        if use_fts_search:
            fts_query = st.text_input(
                "Full-Text Search",
                value="",
                placeholder="Search by company, role, keywords, location…",
                key="fts_search_input",
            )
            if fts_query.strip():
                try:
                    search_conn = ats_db.get_connection()
                    try:
                        # Pass canonical filter to search service if hiding duplicates
                        canon_only = 1 if hide_duplicates else None
                        fts_results = search_jobs(search_conn, fts_query, limit=500, is_canonical=canon_only)
                        if fts_results:
                            matching_ids = {r["id"] for r in fts_results}
                            df_all = df_all[df_all["id"].isin(matching_ids)].copy()
                            st.write(f"🔍 Found {len(df_all)} jobs matching '{fts_query}'")
                        else:
                            df_all = df_all.iloc[0:0] # Clear if no matches
                            st.info("No jobs match your search query.")
                    finally:
                        search_conn.close()
                except Exception as e:
                    st.error(f"Search failed: {e}")
            st.divider()

        search_query = st.text_input(
            "Filter visible jobs",
            value="",
            placeholder="Filter by company, title, location, keywords, or scoring details…",
        )
        st.caption("Employment type counts are shown for current matches only. Many job postings don't specify a work type, so they appear as Unknown.")
        ann = {r["job_key"]: dict(r) for r in ats_db.get_all_annotations(ats_db.get_connection())}

        # Export buttons
        st.divider()
        export_col1, export_col2 = st.columns([2, 1])
        with export_col1:
            st.caption("💾 Export your matched jobs")
        with export_col2:
            if st.button("📊 Excel", use_container_width=True, help="Download color-coded Excel report with matches, summary, and filtered jobs"):
                try:
                    builder = ExcelReportBuilder()
                    rejected_for_export = rejected_df if "rejected_df" in locals() and not rejected_df.empty else pd.DataFrame()
                    excel_bytes = builder.build_excel(
                        jobs_df=df_all,
                        filtered_jobs_df=rejected_for_export,
                    )
                    st.download_button(
                        label="⬇️ Click to download Excel",
                        data=excel_bytes,
                        file_name=f"job_matches_{datetime.now().strftime('%Y-%m-%d_%H%M%S')}.xlsx",
                        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                        key="excel_export_btn",
                    )
                except Exception as e:
                    st.error(f"Excel export failed: {e}")
        st.divider()

        tabs = st.tabs(["🔥 Apply Now", "📋 Review Today", "👀 Watch", "🔍 Manual Review", "🚫 Filtered Out"])
        BUCKETS = [
            ("APPLY NOW", tabs[0], ["Applied", "Rejected", "Watch", "Filtered Out"]),
            ("REVIEW TODAY", tabs[1], ["Apply Now", "Applied", "Watch", "Rejected", "Filtered Out"]),
            ("WATCH", tabs[2], ["Apply Now", "Review Today", "Applied", "Rejected", "Filtered Out"]),
        ]
        
        for name, tab, opts in BUCKETS:
            with tab:
                b_df = df_all[df_all["effective_bucket"] == name].copy()
                b_df = _apply_work_type_filter(b_df, work_type_filter).copy()
                b_df = _search_text_match(
                    b_df,
                    search_query,
                    ["company", "title", "location", "source", "source_lane_label", "matched_keywords", "decision_reason", "url"],
                )
                if b_df.empty: st.info(f"No jobs in {name}"); continue
                _render_apply_now_cards(b_df)
                
                # Table Rendering
                for c in DISPLAY_COLS + ["user_status", "_key", "note"]:
                    if c not in b_df.columns: b_df[c] = ""
                disp = b_df[DISPLAY_COLS + ["user_status", "_key", "note"]].copy()
                disp["Note"] = disp["note"]
                
                # Convert list types to strings for Arrow
                for col in ["matched_keywords", "decision_reason"]:
                    disp[col] = disp[col].astype(str)
                if "work_type" in disp.columns:
                    disp["work_type"] = disp["work_type"].map(_work_type_label)
                
                edited = st.data_editor(
                    disp.drop(columns=["_key"]),
                    column_config={
                        "user_status": st.column_config.SelectboxColumn("Move To", options=[""] + opts),
                        "url": st.column_config.LinkColumn("URL"),
                        "source": st.column_config.TextColumn("Source"),
                        "source_lane_label": st.column_config.TextColumn("Lane"),
                        "score": st.column_config.NumberColumn("Match Score"),
                        "fit_band": st.column_config.TextColumn("Fit Level"),
                        "age_days": st.column_config.NumberColumn("Days Old"),
                        "seen_count": st.column_config.NumberColumn("Times Seen"),
                        "open_days": st.column_config.NumberColumn("Days Posted"),
                        "velocity": st.column_config.TextColumn("Posting Status"),
                        "tier": st.column_config.NumberColumn("Priority Tier"),
                        "matched_keywords": st.column_config.TextColumn("Matched Keywords"),
                        "decision_reason": st.column_config.TextColumn("Scoring Details"),
                        "Note": st.column_config.TextColumn("Reason / Note"),
                    },
                    hide_index=True,
                    use_container_width=True,
                    key=f"ed_{name}",
                )
                
                for i, row in edited.iterrows():
                    key = disp.iloc[list(disp.index).index(i)]["_key"]
                    new_s = str(row.get("user_status") or "").strip()
                    new_note = str(row.get("Note") or "").strip()
                    old_note = str(disp.iloc[list(disp.index).index(i)]["Note"] or "").strip()
                    if new_s and new_s != disp.iloc[list(disp.index).index(i)]["user_status"]:
                        target_bucket = {
                            "Apply Now": "APPLY NOW",
                            "Review Today": "REVIEW TODAY",
                            "Watch": "WATCH",
                            "Filtered Out": "FILTERED OUT",
                        }.get(new_s)
                        db_s = {"Applied": "applied", "Rejected": "rejected", "Filtered Out": "considering"}.get(new_s, "considering")
                        conn = ats_db.get_connection(); now = datetime.now(timezone.utc).isoformat()
                        conn.execute("UPDATE applications SET status=?, updated_at=? WHERE scraper_key=?", (db_s, now, key))
                        res = conn.execute("SELECT id FROM applications WHERE scraper_key=?", (key,)).fetchone()
                        if res:
                            title = f"Moved to {new_s}"
                            if target_bucket:
                                title = f"Bucket changed to {target_bucket.title()}"
                            ats_db.add_event(conn, res["id"], db_s, now, title=title, notes=new_note or old_note)
                        ats_db.upsert_annotation(conn, key, new_note or None, target_bucket)
                        conn.commit(); conn.close(); _invalidate_data_cache(); st.rerun()
                    elif new_note != old_note:
                        conn = ats_db.get_connection()
                        try:
                            existing_tag = ann.get(key, {}).get("tag")
                            ats_db.upsert_annotation(conn, key, new_note or None, existing_tag)
                        finally:
                            conn.close()
                        _invalidate_data_cache(); st.rerun()

        with tabs[3]:
            filtered_manual_review = _apply_work_type_filter(
                pd.DataFrame(manual_review_items),
                work_type_filter,
            ) if manual_review_items else pd.DataFrame()
            if not filtered_manual_review.empty:
                filtered_manual_review = _search_text_match(
                    filtered_manual_review,
                    search_query,
                    ["company", "adapter", "note", "url"],
                )
            if filtered_manual_review.empty and not manual_review_items:
                st.info("No manual-review items are currently queued.")
            else:
                if not filtered_manual_review.empty:
                    filtered_items = filtered_manual_review.to_dict("records")
                    filtered_unresolved = [
                        item for item in filtered_items
                        if review_actions.get(item["company"], {}).get("resolution", "new") not in {"resolved", "ignored", "disabled"}
                    ]
                else:
                    filtered_items = []
                    filtered_unresolved = []
                st.caption(
                    f"Manual review queue: {len(filtered_unresolved)} unresolved of {len(filtered_items) if filtered_items else len(manual_review_items)} visible "
                    f"(registry manual-only + latest run failures + {settings.manual_review_file.name if settings.manual_review_file.exists() else 'no manual review file'})"
                )
                if not filtered_unresolved:
                    st.success("All manual-review items have been handled.")
                for item in filtered_unresolved[:50]:
                    with st.container(border=True):
                        st.markdown(
                            f"**{item['company']}**"
                            + (f"  \nSource: `{item.get('adapter')}`" if item.get("adapter") else "")
                            + (f"  \nQueue: `{item.get('queue_source')}`" if item.get("queue_source") else "")
                            + (f"  \nStatus: `{item.get('status')}`" if item.get("status") else "")
                            + (f"  \nReason: {item.get('note')}" if item.get("note") else "")
                        )
                        if item.get("url"):
                            st.markdown(f"[Open current source URL]({item['url']})")
                        
                        new_url = st.text_input("Fix URL", value=item.get("url", ""), key=f"url_edit_{item['company']}_{item.get('queue_source', 'mr')}")
                        if new_url != item.get("url"):
                            if st.button("💾 Save URL to Registry", key=f"save_url_{item['company']}_{item.get('queue_source', 'mr')}"):
                                if _update_company_url_in_registry(item["company"], new_url):
                                    st.success(f"Updated URL for {item['company']} in registry.")
                                    st.rerun()
                                else:
                                    st.error(f"Could not find {item['company']} in any registry.")

                        c1, c2, c3 = st.columns(3)
                        if c1.button("Resolve", key=f"mr_resolve_{item['company']}"):
                            conn = ats_db.get_connection()
                            try:
                                ats_db.set_manual_review_action(
                                    conn,
                                    company=item["company"],
                                    adapter=item.get("adapter"),
                                    url=item.get("url"),
                                    resolution="resolved",
                                    notes=item.get("note"),
                                )
                            finally:
                                conn.close()
                            st.rerun()
                        if c2.button("Ignore", key=f"mr_ignore_{item['company']}"):
                            conn = ats_db.get_connection()
                            try:
                                ats_db.set_manual_review_action(
                                    conn,
                                    company=item["company"],
                                    adapter=item.get("adapter"),
                                    url=item.get("url"),
                                    resolution="ignored",
                                    notes=item.get("note"),
                                )
                            finally:
                                conn.close()
                            st.rerun()
                        if c3.button("Disable Target", key=f"mr_disable_{item['company']}"):
                            if _disable_company_in_registry(item["company"]):
                                conn = ats_db.get_connection()
                                try:
                                    ats_db.set_manual_review_action(
                                        conn,
                                        company=item["company"],
                                        adapter=item.get("adapter"),
                                        url=item.get("url"),
                                        resolution="disabled",
                                        notes="Company disabled from automatic job search.",
                                    )
                                finally:
                                    conn.close()
                                st.success(f"Disabled {item['company']} in {settings.companies_yaml.name}.")
                                st.rerun()
                            else:
                                st.error(f"Could not find {item['company']} in {settings.companies_yaml.name}.")
                if len(filtered_unresolved) > 50:
                    st.info(f"Showing first 50 of {len(filtered_unresolved)} unresolved manual-review entries.")

        with tabs[4]:
            filtered_bucket_df = df_all[df_all["effective_bucket"] == "Filtered Out"].copy()
            filtered_bucket_df = _apply_work_type_filter(filtered_bucket_df, work_type_filter).copy()
            filtered_bucket_df = _search_text_match(
                filtered_bucket_df,
                search_query,
                ["company", "title", "location", "source", "source_lane_label", "matched_keywords", "decision_reason", "url"],
            )
            filtered_rejected_df = _apply_work_type_filter(rejected_df, work_type_filter).copy()
            filtered_rejected_df = _search_text_match(
                filtered_rejected_df,
                search_query,
                ["company", "title", "location", "adapter", "drop_reason", "decision_reason", "url"],
            )
            if filtered_bucket_df.empty and filtered_rejected_df.empty:
                st.info("No filtered-out jobs available.")
            else:
                if not filtered_bucket_df.empty:
                    st.caption(f"Currently saved jobs below your watch threshold: {len(filtered_bucket_df)}")
                    with st.expander("Bulk Archive Stale Low-Score Jobs"):
                        _arch_conn = ats_db.get_connection()
                        try:
                            _stale_count = _arch_conn.execute("""
                                SELECT COUNT(*) FROM applications a
                                WHERE a.status = 'considering'
                                  AND a.fit_band IN ('Filtered Out', 'Disqualified', 'Poor Match')
                                  AND a.date_added < datetime('now', '-30 days')
                                  AND NOT EXISTS (
                                      SELECT 1 FROM job_annotations ja
                                      WHERE ja.job_key = a.scraper_key
                                        AND ja.tag IS NOT NULL
                                  )
                            """).fetchone()[0]
                        finally:
                            _arch_conn.close()
                        st.write(
                            f"**{_stale_count}** stale jobs are eligible for archive — "
                            f"status 'considering', Filtered Out / Disqualified / Poor Match fit band, "
                            f"added 30+ days ago, no manual tag."
                        )
                        if _stale_count > 0 and st.button(
                            f"Archive {_stale_count} stale jobs (set to 'withdrawn')",
                            type="secondary",
                            key="btn_bulk_archive",
                        ):
                            _arch_conn2 = ats_db.get_connection()
                            try:
                                _arch_conn2.execute("""
                                    UPDATE applications SET status='withdrawn', updated_at=?
                                    WHERE status = 'considering'
                                      AND fit_band IN ('Filtered Out', 'Disqualified', 'Poor Match')
                                      AND date_added < datetime('now', '-30 days')
                                      AND NOT EXISTS (
                                          SELECT 1 FROM job_annotations ja
                                          WHERE ja.job_key = applications.scraper_key
                                            AND ja.tag IS NOT NULL
                                      )
                                """, (datetime.now(timezone.utc).isoformat(),))
                                _arch_conn2.commit()
                                _invalidate_data_cache()
                            finally:
                                _arch_conn2.close()
                            st.success(f"Archived {_stale_count} stale jobs.")
                            st.rerun()
                    promote_opts = ["Apply Now", "Review Today", "Watch", "Applied", "Rejected", "Filtered Out"]
                    for c in DISPLAY_COLS + ["user_status", "_key", "note"]:
                        if c not in filtered_bucket_df.columns:
                            filtered_bucket_df[c] = ""
                    filtered_disp = filtered_bucket_df[DISPLAY_COLS + ["user_status", "_key", "note"]].copy()
                    filtered_disp["Note"] = filtered_disp["note"]
                    if "work_type" in filtered_disp.columns:
                        filtered_disp["work_type"] = filtered_disp["work_type"].map(_work_type_label)
                    filtered_edited = st.data_editor(
                        filtered_disp.drop(columns=["_key"]),
                        column_config={
                            "user_status": st.column_config.SelectboxColumn("Move To", options=[""] + promote_opts),
                            "url": st.column_config.LinkColumn("URL"),
                            "source": st.column_config.TextColumn("Source"),
                            "source_lane_label": st.column_config.TextColumn("Lane"),
                            "score": st.column_config.NumberColumn("Match Score"),
                            "fit_band": st.column_config.TextColumn("Fit Level"),
                            "normalized_compensation_usd": st.column_config.NumberColumn("Annualized Pay (USD)", format="$%.0f"),
                            "decision_reason": st.column_config.TextColumn("Scoring Details"),
                            "Note": st.column_config.TextColumn("Reason / Note"),
                        },
                        hide_index=True,
                        use_container_width=True,
                        key="ed_filtered_saved",
                    )
                    for i, row in filtered_edited.iterrows():
                        key = filtered_disp.iloc[list(filtered_disp.index).index(i)]["_key"]
                        new_s = str(row.get("user_status") or "").strip()
                        new_note = str(row.get("Note") or "").strip()
                        old_note = str(filtered_disp.iloc[list(filtered_disp.index).index(i)]["Note"] or "").strip()
                        if new_s and new_s != filtered_disp.iloc[list(filtered_disp.index).index(i)]["user_status"]:
                            target_bucket = {
                                "Apply Now": "APPLY NOW",
                                "Review Today": "REVIEW TODAY",
                                "Watch": "WATCH",
                                "Filtered Out": "FILTERED OUT",
                            }.get(new_s)
                            db_s = {"Applied": "applied", "Rejected": "rejected", "Filtered Out": "considering"}.get(new_s, "considering")
                            conn = ats_db.get_connection()
                            try:
                                conn.execute(
                                    "UPDATE applications SET status=?, updated_at=? WHERE scraper_key=?",
                                    (db_s, datetime.now(timezone.utc).isoformat(), key),
                                )
                                conn.commit()
                                ats_db.upsert_annotation(conn, key, new_note or None, target_bucket)
                                _invalidate_data_cache()
                            finally:
                                conn.close()
                            st.rerun()
                        elif new_note != old_note:
                            conn = ats_db.get_connection()
                            try:
                                existing_tag = ann.get(key, {}).get("tag")
                                ats_db.upsert_annotation(conn, key, new_note or None, existing_tag)
                            finally:
                                conn.close()
                            _invalidate_data_cache()
                            st.rerun()
                if not filtered_rejected_df.empty:
                    st.caption(f"Filtered out by the scoring system in the latest search: {len(filtered_rejected_df)}")
                    show_cols = [c for c in ["company", "title", "score", "fit_band", "work_type", "normalized_compensation_usd", "location", "adapter", "drop_reason", "decision_reason", "url"] if c in filtered_rejected_df.columns]
                    if "work_type" in filtered_rejected_df.columns:
                        filtered_rejected_df["work_type"] = filtered_rejected_df["work_type"].map(_work_type_label)
                    st.dataframe(
                        filtered_rejected_df[show_cols],
                        column_config={
                            "url": st.column_config.LinkColumn("URL"),
                            "score": st.column_config.NumberColumn("Match Score"),
                            "fit_band": st.column_config.TextColumn("Fit Level"),
                            "normalized_compensation_usd": st.column_config.NumberColumn("Annualized Pay (USD)", format="$%.0f"),
                            "adapter": st.column_config.TextColumn("Source"),
                            "drop_reason": st.column_config.TextColumn("Reason Filtered"),
                            "decision_reason": st.column_config.TextColumn("Scoring Details"),
                        },
                        hide_index=True,
                        use_container_width=True,
                    )

    elif page == "Search Settings":
        st.title("Search Settings")
        prefs = load_yaml(settings.prefs_yaml)
        
        section_options = [
            "Compensation & Location",
            "Job Title Settings",
            "Job Description Keywords",
            "Scoring Settings",
            "Performance & Concurrency",
            "Extensions & Tools",
            "Advanced Editor",
            "App Settings",
            "Base Resume"
        ]
        section = st.selectbox("Settings Section", section_options, index=0)
        
        st.divider()

        if section == "Extensions & Tools":
            st.markdown("#### Manual Escape Hatch")
            st.write("Found a job on a niche board or Slack? Use this bookmarklet to inject it directly into your dashboard.")
            
            st.info("💡 **Requirement:** You must have an **AI Provider (Gemini or OpenAI)** configured in **App Settings** for automatic parsing and scoring to work.")
            
            # Minify and escape the JS for use in a bookmarklet
            # We use backticks (`) for internal strings to avoid quote hell
            raw_js = """(function(){
            const selection = window.getSelection().toString();
            let jobUrl = window.location.href;
            let jobText = selection || document.body.innerText.substring(0, 10000);
            let jobHtml = document.body.innerHTML.substring(0, 20000);
            let jobTitle = document.title;

            if (window.location.hostname.includes('linkedin.com')) {
            var urlParams = new URLSearchParams(window.location.search);
            var jobId = urlParams.get('currentJobId');
            if (jobId) { jobUrl = 'https://www.linkedin.com/jobs/view/' + jobId + '/'; }
            var panel = document.querySelector('#job-details') || document.querySelector('.jobs-description__content') || document.querySelector('.job-view-layout');
            if (panel && !selection) {
            jobText = panel.innerText.substring(0, 10000);
            jobHtml = panel.innerHTML.substring(0, 20000);
            }
            jobTitle = document.title.replace(' | LinkedIn', '').trim();
            }

            const jobData = {
            url: jobUrl,
            title: jobTitle,
            text: jobText,
            html: jobHtml
            };
            const div = document.createElement('div');
            div.style.position = 'fixed';
            div.style.top = '20px';
            div.style.right = '20px';
            div.style.padding = '20px';
            div.style.background = '#3b82f6';
            div.style.color = 'white';
            div.style.zIndex = '999999';
            div.style.borderRadius = '8px';
            div.style.fontFamily = 'sans-serif';
            div.style.boxShadow = '0 4px 6px rgba(0,0,0,0.3)';
            div.innerText = `🚀 Sending to Dashboard...`;
            document.body.appendChild(div);
            fetch('http://localhost:8505/inject-job', {
            method: 'POST',
            mode: 'cors',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify(jobData)
            })
            .then(function(r){
            if(!r.ok) return r.json().then(function(err){ throw new Error(`Server Error: ${err.detail || r.status}`); });
            return r.json();
            })
            .then(function(data){
            div.style.background = '#10b981';
            div.innerText = `✅ Injected: ${data.company}\\nScore: ${data.score} (${data.fit_band})`;
            setTimeout(function(){ if(div) div.remove(); }, 5000);
            })
            .catch(function(e){
            const msg = e.message || '';
            const isNetwork = msg.includes('NetworkError') || msg.includes('fetch') || msg.includes('Failed to fetch');
            if (isNetwork) {
            div.style.background = '#3b82f6';
            div.innerHTML = `<b>🛡️ CSP Blocked Connection</b><br>Greenhouse security is blocking background requests.<br><br>Attempting <b>Popup Bypass</b>...`;
            const popupUrl = 'http://localhost:8505/inject-job?url=' + encodeURIComponent(jobData.url) + '&title=' + encodeURIComponent(jobData.title);
            const win = window.open(popupUrl, 'job_injector', 'width=400,height=300');
            if (win) {
            div.style.background = '#10b981';
            div.innerText = '✅ Popup opened! Check the new tab for confirmation.';
            setTimeout(function(){ if(div) div.remove(); }, 5000);
            } else {
            div.style.background = '#ef4444';
            div.innerHTML = '<b>❌ Popup Blocked</b><br>Please allow popups for this site to use the CSP bypass.';
            }
            } else {
            div.style.background = '#ef4444';
            div.innerHTML = `<b>❌ Error: ${msg}</b><br>Check dashboard console for details.`;
            }
            div.style.cursor = 'pointer';
            div.onclick = function(){ div.remove(); };
            setTimeout(function(){ if(div) div.remove(); }, 12000);
            });
            })()"""

            # Strict cleanup and URL encode
            minified_js = " ".join([line.strip() for line in raw_js.splitlines() if line.strip()])
            bookmarklet_js = f"javascript:{urllib.parse.quote(minified_js)}"

            st.info("💡 **How to use:** Drag the button below to your browser's Bookmarks Bar. When you are on a job page, click it to send the job to your dashboard.")
            
            # Use a more standard button style that won't get caught by markdown parsers
            button_html = f'''
                <a href="{bookmarklet_js}" 
                   style="display: inline-block; padding: 12px 24px; background-color: #3b82f6; color: white; text-decoration: none; border-radius: 8px; font-weight: bold; margin: 10px 0; border: none; cursor: pointer;">
                   🚀 Send to Dashboard
                </a>
            '''
            st.markdown(button_html, unsafe_allow_html=True)
            
            with st.expander("⚠️ How to fix 'Connection Blocked' or 'NetworkError'", expanded=True):
                st.markdown("""
                Modern browsers naturally protect you by blocking **secure (HTTPS)** sites from talking to **local (HTTP)** tools like this dashboard.
                
                **To enable this for your favorite job sites:**
                
                **If you use Chrome or Edge:**
                1. Click the **Settings icon** (left of the address bar).
                2. Select **Site settings**.
                3. Find **Insecure content** and set it to **Allow**.
                
                **If you use Firefox:**
                1. Look for a small **Shield icon** 🛡️ that appears in the address bar *after* you click the bookmark.
                2. Click the shield and select **Disable protection for now**.
                
                *Once you've done this for a site (like LinkedIn or Breezy), the bookmark will work every time!*
                """)
                
            with st.expander("Manual Copy (if dragging fails)"):
                st.code(bookmarklet_js, language="javascript")

        elif section == "Compensation & Location":
            s = prefs.setdefault("search", {}); c = s.setdefault("compensation", {})
            geography = s.setdefault("geography", {})
            contractor = s.setdefault("contractor", {})
            query_tiers = s.setdefault("query_tiers", {})
            local_hybrid = s.setdefault("location_preferences", {}).setdefault("local_hybrid", {})
            remote_us = s.setdefault("location_preferences", {}).setdefault("remote_us", {})
            recency = s.setdefault("recency", {})
            
            st.markdown("#### Salary & Geography")
            f_min = st.number_input("Minimum Salary (USD)", value=int(c.get("min_salary_usd", 165000)))
            f_target = st.number_input("Target Salary (USD)", value=int(c.get("target_salary_usd", 165000)))
            f_rem = st.number_input("Remote Minimum Salary (USD)", value=int(c.get("preferred_remote_min_salary_usd", c.get("min_salary_usd", 170000))))
            policy_opts = ["remote_only", "hybrid_only", "remote_or_hybrid"]
            f_pol = st.selectbox("Work Location Policy", policy_opts, index=policy_opts.index(s.get("location_policy", "remote_only")))
            f_enforce_salary = st.checkbox("Enforce Minimum Salary", value=bool(c.get("enforce_min_salary", True)))
            f_allow_missing = st.checkbox("Show Jobs With No Salary Listed", value=bool(c.get("allow_missing_salary", True)))
            f_salary_basis = st.selectbox("Compare Posted Salary Using", ["midpoint", "low_end", "high_end"], index=["midpoint", "low_end", "high_end"].index(c.get("salary_floor_basis", "midpoint")) if c.get("salary_floor_basis", "midpoint") in ["midpoint", "low_end", "high_end"] else 0)
            f_neg_buffer = st.number_input("Negotiation Buffer (%)", min_value=0.0, max_value=1.0, value=float(c.get("negotiation_buffer_pct", 0.05)), step=0.01, format="%.2f")
            f_us_only = st.checkbox("US Only", value=bool(geography.get("us_only", True)))
            f_allow_international_remote = st.checkbox("Allow International Remote Jobs", value=bool(geography.get("allow_international_remote", False)))
            
            st.markdown("#### Hybrid & Remote Preferences")
            f_remote_enabled = st.checkbox("Include Remote US Jobs", value=bool(remote_us.get("enabled", True)))
            f_remote_bonus = st.number_input("Remote US Score Bonus", min_value=0, max_value=50, value=int(remote_us.get("bonus", 14)))
            f_hybrid_enabled = st.checkbox("Include Local Hybrid Jobs", value=bool(local_hybrid.get("enabled", True)))
            f_hybrid_bonus = st.number_input("Local Hybrid Score Bonus", min_value=0, max_value=50, value=int(local_hybrid.get("bonus", 4)))
            f_zip = st.text_input("Your ZIP Code", value=str(local_hybrid.get("primary_zip", "80504")))
            f_radius = st.number_input("Local Hybrid Radius (miles)", min_value=1, max_value=250, value=int(local_hybrid.get("radius_miles", 30)))
            f_hybrid_salary = st.number_input("Show Hybrid Jobs Only If Salary Is At Least (USD)", min_value=0, value=int(local_hybrid.get("allow_if_salary_at_least_usd", 170000)))
            f_markers = st.text_area("Cities / Areas Near You (one per line)", value="\n".join(local_hybrid.get("markers", [])))
            
            st.markdown("#### Recency & Tiers")
            f_recency_enabled = st.checkbox("Filter Out Old Job Postings", value=bool(recency.get("enforce_job_age", True)))
            f_max_age = st.number_input("Maximum Job Posting Age (days)", min_value=1, max_value=365, value=int(recency.get("max_job_age_days", 21)))
            
            st.caption("Use `2: query` or `3: query` in source search queries to mark broader searches. These limits control which tiers API aggregators and JobSpy will execute.")
            tier_col1, tier_col2 = st.columns(2)
            f_aggregator_max_tier = tier_col1.number_input("Max Query Tier For API Aggregators", min_value=1, max_value=5, value=int(query_tiers.get("aggregator_max_tier", 3) or 3))
            f_jobspy_max_tier = tier_col2.number_input("Max Query Tier For JobSpy Experimental", min_value=1, max_value=5, value=int(query_tiers.get("jobspy_max_tier", 3) or 3))
            
            st.markdown("#### Contractor Preferences")
            f_include_contract = st.checkbox("Include Contract Roles", value=bool(contractor.get("include_contract_roles", True)))
            f_allow_w2 = st.checkbox("Allow W2 Hourly Contracts", value=bool(contractor.get("allow_w2_hourly", True)))
            f_allow_1099 = st.checkbox("Allow 1099 / Corp-to-Corp Contracts", value=bool(contractor.get("allow_1099_hourly", True)))
            f_hours = st.number_input("Contract Hours Per Week", min_value=1.0, max_value=80.0, value=float(contractor.get("default_hours_per_week", 40)), step=1.0)
            f_w2_weeks = st.number_input("W2 Contract: Weeks Worked Per Year", min_value=1.0, max_value=52.0, value=float(contractor.get("default_w2_weeks_per_year", 50)), step=1.0)
            f_1099_weeks = st.number_input("1099 Contract: Weeks Worked Per Year", min_value=1.0, max_value=52.0, value=float(contractor.get("default_1099_weeks_per_year", 46)), step=1.0)
            f_benefits = st.number_input("1099: Estimated Annual Benefits Cost (USD)", min_value=0.0, value=float(contractor.get("benefits_replacement_usd", 18000)), step=1000.0)
            f_w2_gap = st.number_input("W2 Hourly: Benefits Gap vs. Salaried (USD)", min_value=0.0, value=float(contractor.get("w2_benefits_gap_usd", 6000)), step=500.0)
            f_1099_overhead = st.number_input("1099: Self-Employment Overhead Rate (%)", min_value=0.0, max_value=0.75, value=float(contractor.get("overhead_1099_pct", 0.18)), step=0.01, format="%.2f")
            
            st.markdown("#### Experience Tolerance")
            experience = s.setdefault("experience", {})
            f_years_exp = st.number_input("Your Years of Experience", min_value=0.0, max_value=60.0, value=float(experience.get("years", 0)), step=0.5)
            f_exp_gap = st.number_input("Experience Gap Tolerance (years)", min_value=0.0, max_value=20.0, value=float(experience.get("gap_tolerance", 0)), step=0.5)
            
            if st.button("Save Compensation & Location Settings"):
                c.update({
                    "min_salary_usd": int(f_min),
                    "target_salary_usd": int(f_target),
                    "preferred_remote_min_salary_usd": int(f_rem),
                    "enforce_min_salary": bool(f_enforce_salary),
                    "allow_missing_salary": bool(f_allow_missing),
                    "salary_floor_basis": f_salary_basis,
                    "negotiation_buffer_pct": float(f_neg_buffer),
                })
                geography["us_only"] = bool(f_us_only)
                geography["allow_international_remote"] = bool(f_allow_international_remote)
                remote_us.update({"enabled": bool(f_remote_enabled)})
                local_hybrid.update({
                    "enabled": bool(f_hybrid_enabled),
                    "primary_zip": f_zip.strip(),
                    "radius_miles": int(f_radius),
                    "allow_if_salary_at_least_usd": int(f_hybrid_salary),
                    "markers": [line.strip() for line in f_markers.splitlines() if line.strip()],
                })
                recency.update({"enforce_job_age": bool(f_recency_enabled), "max_job_age_days": int(f_max_age)})
                query_tiers.update({"aggregator_max_tier": int(f_aggregator_max_tier), "jobspy_max_tier": int(f_jobspy_max_tier)})
                experience.update({"years": float(f_years_exp), "gap_tolerance": float(f_exp_gap)})
                contractor.update({
                    "include_contract_roles": bool(f_include_contract),
                    "allow_w2_hourly": bool(f_allow_w2),
                    "allow_1099_hourly": bool(f_allow_1099),
                    "default_hours_per_week": float(f_hours),
                    "default_w2_weeks_per_year": float(f_w2_weeks),
                    "default_w2_hourly_weeks_per_year": float(f_w2_weeks),
                    "default_1099_weeks_per_year": float(f_1099_weeks),
                    "benefits_replacement_usd": float(f_benefits),
                    "w2_benefits_gap_usd": float(f_w2_gap),
                    "overhead_1099_pct": float(f_1099_overhead),
                })
                s["location_policy"] = f_pol
                save_yaml(settings.prefs_yaml, prefs); _invalidate_data_cache(); st.success("✅ Saved!"); st.rerun()

            with st.expander("Compensation Calculator", expanded=False):
                calc_type = st.selectbox("Calculate For", ["salary", "w2_hourly", "1099_hourly"])
                calc_amount_label = "Annual Salary USD" if calc_type == "salary" else "Hourly Rate USD"
                calc_amount = st.number_input(calc_amount_label, min_value=0.0, value=float(f_target if calc_type == "salary" else 95.0), step=5.0)
                calc_hours = st.number_input("Calculator Hours / Week", min_value=1.0, max_value=80.0, value=float(f_hours), step=1.0, disabled=(calc_type == "salary"))
                calc_weeks_default = f_w2_weeks if calc_type == "w2_hourly" else f_1099_weeks
                calc_weeks = st.number_input("Calculator Weeks / Year", min_value=1.0, max_value=52.0, value=float(52.0 if calc_type == "salary" else calc_weeks_default), step=1.0, disabled=(calc_type == "salary"))
                calc_preview = _annualized_compensation_preview(calc_type, calc_amount, calc_hours, calc_weeks, contractor)
                breakeven_w2 = (float(f_target) + float(f_w2_gap)) / max(float(f_hours) * float(f_w2_weeks), 1.0)
                breakeven_1099 = (float(f_target) + float(f_benefits)) / max((1.0 - float(f_1099_overhead)) * float(f_hours) * float(f_1099_weeks), 1.0)
                c_calc1, c_calc2, c_calc3 = st.columns(3)
                c_calc1.metric("Gross Annual Value", f"${calc_preview['gross_annual_usd']:,.0f}")
                c_calc2.metric("Take-Home Equivalent", f"${calc_preview['normalized_compensation_usd']:,.0f}")
                c_calc3.metric("Target Salary", f"${float(f_target):,.0f}")
                st.caption(f"Break-even hourly rate to match your salary target — W2: ${breakeven_w2:,.2f}/hr | 1099: ${breakeven_1099:,.2f}/hr")

        elif section == "Job Title Settings":
            t = prefs.setdefault("titles", {})
            constraints = t.setdefault("constraints", {})
            st.markdown("#### Title Keywords & Filters")
            f_keywords = st.text_area("Job Title Keywords (one per line)", value="\n".join(t.get("positive_keywords", [])), help="Job titles or keywords that indicate a good role match.")
            f_pos = st.text_area("Weighted Title Keywords (keyword: score)", value="\n".join([f"{k}: {v}" for k,v in t.get("positive_weights", {}).items()]), help="Each keyword and the score it adds to the match.")
            f_require_positive = st.checkbox("Only Show Jobs With a Matching Title Keyword", value=bool(t.get("require_one_positive_keyword", True)))
            f_fast_track = st.number_input("Fast-Track Starting Score", min_value=0, max_value=100, value=int(t.get("fast_track_base_score", 0)))
            f_fast_track_weight = st.number_input("Fast-Track Minimum Keyword Weight", min_value=0, max_value=20, value=int(t.get("fast_track_min_weight", 8)))
            
            st.markdown("#### Mandatory Modifiers")
            f_mods = st.text_area("Product Manager: Required Modifiers (one per line)", value="\n".join(constraints.get("product_manager_allowed_modifiers", [])))
            f_arch_mods = st.text_area("Architect: Required Modifiers (one per line)", value="\n".join(constraints.get("architect_allowed_modifiers", [])))
            f_ba_mods = st.text_area("Business Analyst: Required Modifiers (one per line)", value="\n".join(constraints.get("business_analyst_allowed_modifiers", [])))
            f_consult_mods = st.text_area("Consultant: Required Modifiers (one per line)", value="\n".join(constraints.get("consultant_allowed_modifiers", [])))
            
            st.markdown("#### Disqualifiers")
            f_neg = st.text_area("Job Title Disqualifiers (one per line)", value="\n".join(t.get("negative_disqualifiers", [])))
            
            if st.button("Save Job Title Settings"):
                new_w = {l.split(":")[0].strip(): int(l.split(":")[1]) for l in f_pos.splitlines() if ":" in l}
                t["positive_keywords"] = [line.strip() for line in f_keywords.splitlines() if line.strip()]
                t["positive_weights"] = new_w
                t["require_one_positive_keyword"] = bool(f_require_positive)
                t["fast_track_base_score"] = int(f_fast_track)
                t["fast_track_min_weight"] = int(f_fast_track_weight)
                constraints["product_manager_allowed_modifiers"] = [line.strip() for line in f_mods.splitlines() if line.strip()]
                constraints["architect_allowed_modifiers"] = [line.strip() for line in f_arch_mods.splitlines() if line.strip()]
                constraints["business_analyst_allowed_modifiers"] = [line.strip() for line in f_ba_mods.splitlines() if line.strip()]
                constraints["consultant_allowed_modifiers"] = [line.strip() for line in f_consult_mods.splitlines() if line.strip()]
                t["negative_disqualifiers"] = [line.strip() for line in f_neg.splitlines() if line.strip()]
                save_yaml(settings.prefs_yaml, prefs); _invalidate_data_cache(); st.success("✅ Saved!"); st.rerun()

        elif section == "Job Description Keywords":
            k = prefs.setdefault("keywords", {})
            scoring = prefs.setdefault("scoring", {})
            matching = scoring.setdefault("keyword_matching", {})
            st.markdown("#### JD Keyword Weights")
            f_bp = st.text_area("Keywords That Boost Score (keyword: score)", value="\n".join([f"{ki}: {vi}" for ki,vi in k.get("body_positive", {}).items()]))
            f_bn = st.text_area("Keywords That Reduce Score (keyword: penalty)", value="\n".join([f"{ki}: {vi}" for ki,vi in k.get("body_negative", {}).items()]))
            f_unique = st.checkbox("Count Each Keyword Only Once", value=bool(matching.get("count_unique_matches_only", True)))
            f_pos_cap = st.number_input("Maximum Score From Positive Keywords", min_value=0, max_value=200, value=int(matching.get("positive_keyword_cap", 60)))
            f_neg_cap = st.number_input("Maximum Penalty From Negative Keywords", min_value=0, max_value=200, value=int(matching.get("negative_keyword_cap", 45)))
            
            if st.button("Save Keyword Settings"):
                k["body_positive"] = {l.split(":")[0].strip(): int(l.split(":")[1]) for l in f_bp.splitlines() if ":" in l}
                k["body_negative"] = {l.split(":")[0].strip(): int(l.split(":")[1]) for l in f_bn.splitlines() if ":" in l}
                matching["count_unique_matches_only"] = bool(f_unique)
                matching["positive_keyword_cap"] = int(f_pos_cap)
                matching["negative_keyword_cap"] = int(f_neg_cap)
                save_yaml(settings.prefs_yaml, prefs); _invalidate_data_cache(); st.success("✅ Saved!"); st.rerun()

        elif section == "Scoring Settings":
            s_cfg = prefs.setdefault("scoring", {})
            title_cfg = prefs.setdefault("titles", {})
            matching = s_cfg.setdefault("keyword_matching", {})
            bucket_thresholds = prefs.get("search", {}).get("bucket_thresholds", {})

            st.caption("Scoring uses V2. Title matching and JD keyword weights drive all scores — adjust here to tune bucket placement.")

            st.markdown("#### Title Fast-Track")
            st.caption("A strong title match (weight ≥ fast-track min) grants an immediate base score, bypassing the normal keyword accumulation floor.")
            c1, c2 = st.columns(2)
            f_fast_track = c1.number_input(
                "Fast-Track Base Score", 0, 100,
                int(title_cfg.get("fast_track_base_score", 50)),
                help="Score granted immediately when a title matches a high-weight target role. **Recommended: 50.** Lower to 35–45 if borderline titles are reaching APPLY NOW."
            )
            f_fast_track_weight = c2.number_input(
                "Fast-Track Min Weight", 1, 10,
                int(title_cfg.get("fast_track_min_weight", 8)),
                help="Minimum title weight required to trigger fast-track. **Recommended: 8.** Raise to require a tighter title match."
            )

            st.markdown("#### Keyword Score Caps")
            st.caption("Anchor keywords (weight ≥ 8) and baseline keywords (weight < 8) are capped separately. Negative penalties are also capped.")
            k1, k2, k3 = st.columns(3)
            f_anchor_cap = k1.number_input(
                "Anchor Keyword Cap", 10, 200,
                int(matching.get("anchor_keyword_cap", 60)),
                help="Maximum points from high-weight anchor keywords. **Recommended: 60.**"
            )
            f_baseline_cap = k2.number_input(
                "Baseline Keyword Cap", 5, 100,
                int(matching.get("baseline_keyword_cap", 30)),
                help="Maximum points from lower-weight baseline keywords. **Recommended: 30.**"
            )
            f_negative_cap = k3.number_input(
                "Negative Penalty Cap", 0, 200,
                int(matching.get("negative_keyword_cap", 45)),
                help="Maximum penalty from negative keywords. **Recommended: 45.**"
            )

            st.markdown("#### Bucket Thresholds")
            t_apply = st.number_input(
                "APPLY NOW Threshold", 0, 200,
                int(bucket_thresholds.get("apply_now", 85)),
                help="Minimum score to reach APPLY NOW. **Recommended: 82–90.**"
            )
            t_review = st.number_input(
                "REVIEW TODAY Threshold", 0, 200,
                int(bucket_thresholds.get("review_today", 70)),
                help="Minimum score for REVIEW TODAY. **Recommended: 70–75.**"
            )
            t_watch = st.number_input(
                "WATCH Threshold", 0, 200,
                int(bucket_thresholds.get("watch", 50)),
                help="Minimum score to appear in WATCH. Jobs below this are Filtered Out. **Recommended: 45–55.**"
            )

            if st.button("Save Scoring Settings"):
                title_cfg["fast_track_base_score"] = int(f_fast_track)
                title_cfg["fast_track_min_weight"] = int(f_fast_track_weight)
                matching["anchor_keyword_cap"]   = int(f_anchor_cap)
                matching["baseline_keyword_cap"] = int(f_baseline_cap)
                matching["negative_keyword_cap"] = int(f_negative_cap)
                if "search" not in prefs: prefs["search"] = {}
                prefs["search"]["bucket_thresholds"] = {"apply_now": int(t_apply), "review_today": int(t_review), "watch": int(t_watch)}
                save_yaml(settings.prefs_yaml, prefs); _invalidate_data_cache(); st.success("✅ Saved!"); st.rerun()
            
            st.divider()
            if st.button("🔄 Re-score Saved Jobs"):
                with st.spinner("Re-scoring using current preferences..."):
                    conn = ats_db.get_connection()
                    try:
                        from jobsearch.scraper.scoring import Scorer
                        from jobsearch.scraper.scoring_v2 import score_job_v2
                        import json as _json
                        scorer = Scorer(prefs)
                        v2_cfg, v2_title_index, _v2_pw, _v2_ftbase, _v2_ftmin = _build_v2_config_from_prefs(prefs)
                        apps = ats_db.get_applications(conn)
                        updated = 0
                        now_ts = datetime.now(timezone.utc).isoformat()
                        for app in apps:
                            description = " ".join(filter(None, [
                                app["job_description"],
                                app["description_excerpt"],
                                app["jd_summary"]
                            ]))
                            job_data = {
                                "title": app["role"],
                                "description": description,
                                "location": app["location"],
                                "tier": app["tier"],
                                "is_remote": app["is_remote"],
                                "salary_min": app["salary_low"],
                                "salary_max": app["salary_high"],
                                "work_type": app["work_type"],
                                "source_lane": app["source_lane"],
                            }
                            res = scorer.score_job(job_data)
                            _new_score = res.get("score") or 0.0
                            _new_fit_band = res.get("fit_band")
                            _new_reason = res.get("decision_reason")
                            _v1_disqualified = _new_fit_band in ("Disqualified", "Filtered Out")
                            _v2r = None
                            if not _v1_disqualified:
                                try:
                                    _tpts = _v2_title_pts(app["role"] or "", v2_title_index, v2_cfg, _v2_pw, _v2_ftbase, _v2_ftmin)
                                    _v2r = score_job_v2(
                                        app["role"] or "",
                                        description,
                                        _tpts,
                                        v2_cfg,
                                        v2_title_index,
                                    )
                                    _new_score = round(_v2r.final_score, 2)
                                    _new_fit_band = _v2r.fit_band
                                    _new_reason = (
                                        f"V2: {_v2r.fit_band} | title={_v2r.canonical_title or 'unresolved'}"
                                        f" | seniority={_v2r.seniority.band}"
                                        f" | anchor={round(_v2r.keyword.anchor_score, 1)}"
                                        f" base={round(_v2r.keyword.baseline_score, 1)}"
                                        f" neg={round(_v2r.keyword.negative_score, 1)}"
                                    )
                                except Exception:
                                    pass
                            _existing_score = (app["score"] or 0.0) if "score" in app.keys() else 0.0
                            if _new_score > 0 or _existing_score == 0:
                                conn.execute(
                                    """
                                    UPDATE applications
                                    SET score=?, fit_band=?, matched_keywords=?, penalized_keywords=?, decision_reason=?,
                                        normalized_compensation_usd=?, updated_at=?
                                    WHERE id=?
                                    """,
                                    (
                                        _new_score, _new_fit_band, res["matched_keywords"], res["penalized_keywords"],
                                        _new_reason, res["normalized_compensation_usd"],
                                        now_ts, app["id"]
                                    )
                                )
                            if _v2r is not None:
                                conn.execute(
                                    """
                                    UPDATE applications
                                    SET score_v2=?, fit_band_v2=?, v2_canonical_title=?,
                                        v2_seniority_band=?, v2_anchor_score=?,
                                        v2_baseline_score=?, v2_flags=?
                                    WHERE id=?
                                    """,
                                    (
                                        round(_v2r.final_score, 2),
                                        _v2r.fit_band,
                                        _v2r.canonical_title,
                                        _v2r.seniority.band,
                                        round(_v2r.keyword.anchor_score, 2),
                                        round(_v2r.keyword.baseline_score, 2),
                                        _json.dumps(_v2r.flags),
                                        app["id"],
                                    )
                                )
                            updated += 1
                        conn.commit()
                        st.success(f"Successfully re-scored {updated} jobs (V2 primary).")
                    finally:
                        conn.close()
                    _invalidate_data_cache(); st.rerun()

        elif section == "Performance & Concurrency":
            perf = prefs.setdefault("performance", {})
            st.markdown("#### Scraper Concurrency")
            f_workday = st.number_input("Workday Concurrency", 1, 10, int(perf.get("scrape_workday_concurrency", settings.scrape_workday_concurrency)))
            f_greenhouse = st.number_input("Greenhouse Concurrency", 1, 10, int(perf.get("scrape_greenhouse_concurrency", settings.scrape_greenhouse_concurrency)))
            f_lever = st.number_input("Lever Concurrency", 1, 10, int(perf.get("scrape_lever_concurrency", settings.scrape_lever_concurrency)))
            f_jobspy = st.number_input("JobSpy Concurrency", 1, 10, int(perf.get("scrape_jobspy_concurrency", settings.scrape_jobspy_concurrency)))
            
            if st.button("Save Performance Settings"):
                perf.update({
                    "scrape_workday_concurrency": int(f_workday),
                    "scrape_greenhouse_concurrency": int(f_greenhouse),
                    "scrape_lever_concurrency": int(f_lever),
                    "scrape_jobspy_concurrency": int(f_jobspy),
                })
                save_yaml(settings.prefs_yaml, prefs); _invalidate_data_cache(); st.success("✅ Saved!"); st.rerun()

        elif section == "Advanced Editor":
            raw = settings.prefs_yaml.read_text(encoding="utf-8") if settings.prefs_yaml.exists() else ""
            st.caption("Direct YAML editor — for advanced users only.")
            new_raw = st.text_area("Raw Configuration", value=raw, height=600)
            if st.button("Save Configuration"):
                settings.prefs_yaml.write_text(new_raw, encoding="utf-8")
                _invalidate_data_cache(); st.success("✅ Saved!"); st.rerun()

        elif section == "App Settings":
            conn = ats_db.get_connection()
            try:
                weekly_goal = int(ats_db.get_setting(conn, "weekly_activity_goal", default="3") or "3")
                gmail_address = ats_db.get_setting(conn, "gmail_address", default=settings.gmail_address)
                gmail_app_password = ats_db.get_setting(conn, "gmail_app_password", default=settings.gmail_app_password)
                gmail_imap_host = ats_db.get_setting(conn, "gmail_imap_host", default=settings.gmail_imap_host)
                usajobs_api_key = ats_db.get_setting(conn, "usajobs_api_key", default=settings.usajobs_api_key)
                usajobs_user_agent = ats_db.get_setting(conn, "usajobs_user_agent", default=settings.usajobs_user_agent)
                adzuna_app_id = ats_db.get_setting(conn, "adzuna_app_id", default=settings.adzuna_app_id)
                adzuna_app_key = ats_db.get_setting(conn, "adzuna_app_key", default=settings.adzuna_app_key)
                adzuna_country = ats_db.get_setting(conn, "adzuna_country", default=settings.adzuna_country)
                jooble_api_key = ats_db.get_setting(conn, "jooble_api_key", default=settings.jooble_api_key)
                usajobs_max_requests = int(ats_db.get_setting(conn, "usajobs_max_requests_per_run", default=str(settings.usajobs_max_requests_per_run)) or settings.usajobs_max_requests_per_run)
                adzuna_max_requests = int(ats_db.get_setting(conn, "adzuna_max_requests_per_run", default=str(settings.adzuna_max_requests_per_run)) or settings.adzuna_max_requests_per_run)
                jooble_max_requests = int(ats_db.get_setting(conn, "jooble_max_requests_per_run", default=str(settings.jooble_max_requests_per_run)) or settings.jooble_max_requests_per_run)
                themuse_max_requests = int(ats_db.get_setting(conn, "themuse_max_requests_per_run", default=str(settings.themuse_max_requests_per_run)) or settings.themuse_max_requests_per_run)
                llm_provider = ats_db.get_setting(conn, "llm_provider", default="gemini") or "gemini"
                ollama_base_url = ats_db.get_setting(conn, "ollama_base_url", default="http://localhost:11434") or "http://localhost:11434"
                ollama_model = ats_db.get_setting(conn, "ollama_model", default="llama3.2") or "llama3.2"
                
                st.markdown("#### Dashboard Settings")
                app_weekly_goal = st.number_input("Weekly Activity Goal", min_value=1, max_value=50, value=weekly_goal, step=1)

                st.markdown("#### Gmail Sync")
                st.caption("Stored locally in the app settings table. Gmail app passwords are recommended over your normal account password.")
                app_gmail_address = st.text_input("Gmail Address", value=gmail_address)
                app_gmail_password = st.text_input("Gmail App Password", value=gmail_app_password, type="password")
                app_gmail_host = st.text_input("IMAP Host", value=gmail_imap_host or "imap.gmail.com")

                st.markdown("#### AI / LLM Settings")
                st.caption("Choose which AI provider to use for job analysis, preference generation, and enrichment.")
                _provider_options = ["gemini", "openai", "ollama"]
                _provider_index = _provider_options.index(llm_provider) if llm_provider in _provider_options else 0
                app_llm_provider = st.selectbox(
                    "AI Provider",
                    options=_provider_options,
                    index=_provider_index,
                    format_func=lambda x: {"gemini": "Google Gemini", "openai": "OpenAI", "ollama": "Ollama (Local)"}[x],
                    help="Gemini and OpenAI require API keys below. Ollama runs locally — no API key needed.",
                )
                app_google_api_key = st.text_input(
                    "Google API Key",
                    value=ats_db.get_setting(conn, "google_api_key", default=os.getenv("GOOGLE_API_KEY", "")),
                    type="password",
                    help="Required when using Google Gemini. Get one for free at aistudio.google.com",
                )
                app_openai_api_key = st.text_input(
                    "OpenAI API Key",
                    value=ats_db.get_setting(conn, "openai_api_key", default=os.getenv("OPENAI_API_KEY", "")),
                    type="password",
                    help="Required when using OpenAI (GPT-4o-mini). Get one at platform.openai.com",
                )
                if app_llm_provider == "ollama":
                    ollama_col1, ollama_col2 = st.columns(2)
                    app_ollama_base_url = ollama_col1.text_input(
                        "Ollama Base URL",
                        value=ollama_base_url,
                        help="URL of your running Ollama instance. Default: http://localhost:11434",
                    )
                    app_ollama_model = ollama_col2.text_input(
                        "Ollama Model",
                        value=ollama_model,
                        help="Model name as it appears in 'ollama list', e.g. llama3.2, mistral, phi3",
                    )
                else:
                    app_ollama_base_url = ollama_base_url
                    app_ollama_model = ollama_model

                st.markdown("#### Aggregator API Settings")
                st.caption("Stored locally in the app settings table. Environment variables still override these values when present.")
                app_usajobs_user_agent = st.text_input(
                    "USAJobs User-Agent / Email",
                    value=usajobs_user_agent,
                    help="USAJobs requires the registered email address in the User-Agent header.",
                )
                app_usajobs_api_key = st.text_input("USAJobs API Key", value=usajobs_api_key, type="password")
                adzuna_col1, adzuna_col2 = st.columns(2)
                app_adzuna_app_id = adzuna_col1.text_input("Adzuna App ID", value=adzuna_app_id)
                app_adzuna_app_key = adzuna_col2.text_input("Adzuna App Key", value=adzuna_app_key, type="password")
                app_adzuna_country = st.text_input(
                    "Adzuna Country",
                    value=adzuna_country or "us",
                    help="Two-letter Adzuna market code, for example us, gb, or au.",
                )
                app_jooble_api_key = st.text_input("Jooble API Key", value=jooble_api_key, type="password")
                rate_col1, rate_col2 = st.columns(2)
                app_usajobs_max_requests = rate_col1.number_input("USAJobs Max Requests / Run", min_value=1, max_value=50, value=usajobs_max_requests, step=1)
                app_adzuna_max_requests = rate_col2.number_input("Adzuna Max Requests / Run", min_value=1, max_value=50, value=adzuna_max_requests, step=1)
                rate_col3, rate_col4 = st.columns(2)
                app_jooble_max_requests = rate_col3.number_input("Jooble Max Requests / Run", min_value=1, max_value=50, value=jooble_max_requests, step=1)
                app_themuse_max_requests = rate_col4.number_input("The Muse Max Requests / Run", min_value=1, max_value=50, value=themuse_max_requests, step=1)

                if st.button("Save App Settings"):
                    ats_db.set_setting(conn, "weekly_activity_goal", str(int(app_weekly_goal)))
                    ats_db.set_setting(conn, "gmail_address", app_gmail_address.strip())
                    ats_db.set_setting(conn, "gmail_app_password", app_gmail_password.strip())
                    ats_db.set_setting(conn, "gmail_imap_host", app_gmail_host.strip() or "imap.gmail.com")
                    ats_db.set_setting(conn, "usajobs_user_agent", app_usajobs_user_agent.strip())
                    ats_db.set_setting(conn, "usajobs_api_key", app_usajobs_api_key.strip())
                    ats_db.set_setting(conn, "adzuna_app_id", app_adzuna_app_id.strip())
                    ats_db.set_setting(conn, "adzuna_app_key", app_adzuna_app_key.strip())
                    ats_db.set_setting(conn, "adzuna_country", (app_adzuna_country.strip() or "us").lower())
                    ats_db.set_setting(conn, "jooble_api_key", app_jooble_api_key.strip())
                    ats_db.set_setting(conn, "google_api_key", app_google_api_key.strip())
                    ats_db.set_setting(conn, "openai_api_key", app_openai_api_key.strip())
                    ats_db.set_setting(conn, "llm_provider", app_llm_provider)
                    ats_db.set_setting(conn, "ollama_base_url", app_ollama_base_url.strip())
                    ats_db.set_setting(conn, "ollama_model", app_ollama_model.strip())
                    ats_db.set_setting(conn, "usajobs_max_requests_per_run", str(int(app_usajobs_max_requests)))
                    ats_db.set_setting(conn, "adzuna_max_requests_per_run", str(int(app_adzuna_max_requests)))
                    ats_db.set_setting(conn, "jooble_max_requests_per_run", str(int(app_jooble_max_requests)))
                    ats_db.set_setting(conn, "themuse_max_requests_per_run", str(int(app_themuse_max_requests)))
                    st.success("✅ Saved!"); st.rerun()
            finally:
                conn.close()

        elif section == "Base Resume":
            conn = ats_db.get_connection()
            try:
                st.markdown("#### Master Resume")
                existing_name = ats_db.get_setting(conn, "base_resume_name", "Master Resume")
                existing_text = ats_db.get_setting(conn, "base_resume_text", "")
                
                f_res_name = st.text_input("Resume Name", value=existing_name)
                f_upload = st.file_uploader("Upload New Master Resume", type=["docx", "pdf", "txt"])
                if f_upload:
                    text, fmt = _extract_resume_text(f_upload)
                    if st.button("💾 Replace Resume With Uploaded File"):
                        ats_db.set_setting(conn, "base_resume_name", f_res_name)
                        ats_db.set_setting(conn, "base_resume_source_url", f_upload.name)
                        ats_db.set_setting(conn, "base_resume_text", text)
                        st.success("✅ Imported!"); st.rerun()
                
                st.divider()
                f_res_text = st.text_area("Manual Resume Text", value=existing_text, height=500)
                if st.button("Update Resume Text"):
                    ats_db.set_setting(conn, "base_resume_name", f_res_name)
                    ats_db.set_setting(conn, "base_resume_text", f_res_text)
                    st.success("✅ Updated!"); st.rerun()

                if existing_text:
                    st.divider()
                    st.markdown("#### Heuristic Alignment")
                    st.caption("Automatically update your search preferences (titles and keywords) based on your master resume using AI.")
                    if st.button("✨ Auto-Generate Search Preferences", help="Analyzes your resume to extract job titles and keywords, then merges them into your current Search Settings."):
                        from jobsearch.services.profile_service import ProfileService
                        
                        # Load current provider settings
                        llm_provider = ats_db.get_setting(conn, "llm_provider", default="gemini")
                        google_key = ats_db.get_setting(conn, "google_api_key", default=os.getenv("GOOGLE_API_KEY", ""))
                        openai_key = ats_db.get_setting(conn, "openai_api_key", default=os.getenv("OPENAI_API_KEY", ""))
                        ollama_url = ats_db.get_setting(conn, "ollama_base_url", default="http://localhost:11434")
                        ollama_model = ats_db.get_setting(conn, "ollama_model", default="llama3.2")

                        with st.spinner("Analyzing resume and updating preferences..."):
                            try:
                                profiler = ProfileService(
                                    google_api_key=google_key,
                                    openai_api_key=openai_key,
                                    preferred_provider=llm_provider,
                                    ollama_base_url=ollama_url,
                                    ollama_model=ollama_model
                                )
                                extracted = profiler.extract_preferences(existing_text)
                                if not extracted:
                                    st.error("AI could not extract preferences. Check your LLM settings.")
                                else:
                                    current_prefs = load_yaml(settings.prefs_yaml)
                                    merged = profiler.merge_preferences(current_prefs, extracted)
                                    save_yaml(settings.prefs_yaml, merged)
                                    _invalidate_data_cache()
                                    st.success("✅ Preferences updated! Go to **Search Settings** to review and then click **Re-score Saved Jobs**.")
                                    st.balloons()
                            except Exception as exc:
                                st.error(f"Failed to generate preferences: {exc}")
            finally:
                conn.close()

    elif page == "Target Companies":
        st.title("Target Companies")
        
        # Discover all registries dynamically
        discovered_regs = settings.get_company_registries()
        
        friendly_labels = {
            "job_search_companies.yaml": "Main Company List",
            "job_search_companies_contract.yaml": "Contractor Company List",
            "job_search_companies_aggregators.yaml": "Aggregator Company List",
            "job_search_companies_jobspy.yaml": "JobSpy Experimental List"
        }
        
        def get_reg_label(path: Path) -> str:
            if path.name in friendly_labels:
                return friendly_labels[path.name]
            # Convert job_search_companies_fintech.yaml -> Fintech Company List
            name = path.name.replace("job_search_companies_", "").replace(".yaml", "").replace("_", " ").title()
            if name == "job_search_companies": name = "General"
            return f"{name} Company List"

        # Create map for radio button.
        # Note: labels can collide (e.g., multiple dynamic registries mapping to the same "X Company List"),
        # and a dict comprehension would silently drop entries. Ensure labels are unique so users can
        # actually switch lists.
        registry_options: dict[str, Path] = {}
        for p in discovered_regs:
            base = get_reg_label(p)
            if base in registry_options:
                # Rename the existing entry to include its filename, then add this one similarly.
                prev_path = registry_options.pop(base)
                registry_options[f"{base} ({prev_path.name})"] = prev_path
                registry_options[f"{base} ({p.name})"] = p
            else:
                registry_options[base] = p
        
        registry_label = st.radio("Company List", list(registry_options.keys()), horizontal=True, key="target_companies_registry")
        registry_path = registry_options[registry_label]
        data = load_yaml(registry_path)
        cos = data.get("companies", [])
        t1, t2, t3, t4, t5 = st.tabs(["List", "Add / Edit", "Fix Job Listings", "Advanced Editor", "Scraper Health"])
        with t1:
            df_companies = pd.DataFrame(cos).reset_index(names="__idx")
            if not df_companies.empty:
                filter_option = st.selectbox("Show", ["All", "Active", "Inactive", "Manual Only"], index=0)
                active_series = df_companies.get("active", pd.Series([False] * len(df_companies), index=df_companies.index)).fillna(False).astype(bool)
                manual_series = df_companies.get("manual_only", pd.Series([False] * len(df_companies), index=df_companies.index)).fillna(False).astype(bool)
                if filter_option == "Active":
                    df_companies = df_companies[active_series]
                elif filter_option == "Inactive":
                    df_companies = df_companies[~active_series]
                elif filter_option == "Manual Only":
                    df_companies = df_companies[manual_series]
                total_count = len(cos)
                active_count = sum(1 for company in cos if company.get("active", True))
                manual_only_count = sum(1 for company in cos if company.get("manual_only", False))
                c1, c2, c3 = st.columns(3)
                c1.metric("Total Targets", total_count)
                c2.metric("Active Targets", active_count)
                c3.metric("Manual Only", manual_only_count)
                df_companies = df_companies.apply(lambda col: col.map(_normalize_editor_value))
            display_cols = [col for col in df_companies.columns if col != "__idx"]
            edited_df = st.data_editor(
                df_companies[display_cols],
                use_container_width=True,
                hide_index=True,
                disabled=["heal_failure_streak", "last_healed", "cooldown_until"],
                key=f"company_list_editor_{registry_path.name}",
            )
            if st.button("Save List Changes", key=f"save_company_list_{registry_path.name}"):
                filtered_indices = df_companies["__idx"].tolist()
                companies = data.get("companies", [])
                for row_position, company_idx in enumerate(filtered_indices):
                    if company_idx >= len(companies):
                        continue
                    row = edited_df.iloc[row_position].to_dict()
                    original = companies[company_idx]
                    updated = dict(original)
                    for key, value in row.items():
                        updated[key] = _coerce_company_editor_value(key, value)
                    if updated.get("manual_only"):
                        updated["status"] = "manual_only"
                    companies[company_idx] = updated
                data["companies"] = companies
                save_yaml(registry_path, data)
                st.success(f"Saved {registry_path.name} from list view.")
                st.rerun()
        with t2:
            company_names = [company.get("name", "") for company in cos if isinstance(company, dict)]
            mode = st.radio("Mode", ["Add New", "Edit Existing"], horizontal=True)
            selected_name = None
            selected_company = {}
            if mode == "Edit Existing" and company_names:
                selected_name = st.selectbox("Company", company_names)
                selected_company = next((company for company in cos if company.get("name") == selected_name), {})

            c_name = st.text_input("Company Name", value=selected_company.get("name", ""))
            c_domain = st.text_input("Website Domain", value=selected_company.get("domain", ""), help="e.g. jobs.lever.co or careers.company.com")
            c_careers = st.text_input("Careers Page URL", value=selected_company.get("careers_url", ""))
            c_adapter = st.selectbox("Job Board Type", KNOWN_ADAPTERS, index=KNOWN_ADAPTERS.index(selected_company.get("adapter", "generic")) if selected_company.get("adapter", "generic") in KNOWN_ADAPTERS else KNOWN_ADAPTERS.index("generic"), help="The job board platform this company uses (e.g. Greenhouse, Lever, Workday).")
            c_adapter_key = st.text_input("Job Board Identifier", value=selected_company.get("adapter_key", ""), help="The unique identifier for this company on their job board platform (e.g. the slug in the URL).")
            c_tier = st.number_input("Priority Tier (1 = highest)", min_value=1, max_value=4, value=int(selected_company.get("tier", 2) or 2))
            c_priority = st.selectbox("Priority", ["high", "medium", "low"], index=["high", "medium", "low"].index(selected_company.get("priority", "medium")) if selected_company.get("priority", "medium") in ["high", "medium", "low"] else 1)
            c_active = st.checkbox("Active (include in job search)", value=bool(selected_company.get("active", True)))
            c_manual_only = st.checkbox("Search Manually (skip automatic scraping)", value=bool(selected_company.get("manual_only", False)))
            status_options = ["active", "broken", "pending", "manual_only"]
            c_status = st.selectbox("Status", status_options, index=status_options.index(selected_company.get("status", "active")) if selected_company.get("status", "active") in status_options else 0)
            c_industry = st.text_input("Industry", value=_normalize_editor_value(selected_company.get("industry", "")))
            c_sub = st.text_input("Sub-Industry", value=str(selected_company.get("sub_industry", "")))
            c_search_queries = st.text_area(
                "Search Queries (one per line)",
                value="\n".join(search_query_text_lines(selected_company.get("search_queries"))),
                help="Used by aggregator and JobSpy lanes to search the source. Prefix a line with `2:` or `3:` to mark it as a broader tiered query.",
            )
            c_location_filter = st.text_input(
                "Location Filter",
                value=str(selected_company.get("location_filter", "")),
                help="Optional source-side location filter, for example United States.",
            )
            c_site_names = st.text_input(
                "Source Sites",
                value=_normalize_editor_value(selected_company.get("site_names", "")),
                help=f"Optional comma-separated site names for JobSpy. Available: {', '.join(sorted(ALLOWED_JOBSPY_SITES))}",
            )
            extra_col1, extra_col2, extra_col3, extra_col4 = st.columns(4)
            c_results_wanted = extra_col1.number_input(
                "Results Wanted",
                min_value=1,
                max_value=200,
                value=int(selected_company.get("results_wanted", selected_company.get("max_results_per_query", 20) or 20) or 20),
                step=1,
            )
            c_hours_old = extra_col2.number_input(
                "Hours Old",
                min_value=1,
                max_value=720,
                value=int(selected_company.get("hours_old", 72) or 72),
                step=1,
            )
            c_country_indeed = extra_col3.text_input(
                "Indeed Country",
                value=str(selected_company.get("country_indeed", "USA") or "USA"),
            )
            c_concurrency = extra_col4.number_input(
                "Concurrency",
                min_value=1,
                max_value=10,
                value=int(selected_company.get("concurrency", 1) or 1),
                step=1,
            )
            js_cfg1, js_cfg2, js_cfg3 = st.columns(3)
            c_max_total_results = js_cfg1.number_input(
                "Max Total Results",
                min_value=1,
                max_value=500,
                value=int(selected_company.get("max_total_results", selected_company.get("results_wanted", 20) or 20) or 20),
                step=1,
            )
            c_is_remote = js_cfg2.checkbox(
                "Remote Only",
                value=bool(selected_company.get("is_remote", False)),
            )
            c_continue_on_site_failure = js_cfg3.checkbox(
                "Continue On Site Failure",
                value=bool(selected_company.get("continue_on_site_failure", True)),
            )
            js_cfg4, js_cfg5 = st.columns(2)
            c_job_type = js_cfg4.text_input(
                "Job Type",
                value=str(selected_company.get("job_type", "")),
                help="Optional JobSpy job_type value, for example fulltime or contract.",
            )
            c_linkedin_fetch_description = js_cfg5.checkbox(
                "LinkedIn Fetch Description",
                value=bool(selected_company.get("linkedin_fetch_description", False)),
            )
            c_google_search_term_template = st.text_input(
                "Google Search Term Template",
                value=str(selected_company.get("google_search_term_template", "{query}") or "{query}"),
                help="Used only for the Google JobSpy row. Keep {query} in the template.",
            )
            c_notes = st.text_area("Notes", value=str(selected_company.get("notes", "")))

            if st.button("Save Company"):
                new_company = {
                    "name": c_name.strip() if c_name else "",
                    "domain": c_domain.strip() if c_domain else "",
                    "careers_url": c_careers.strip() if c_careers else "",
                    "adapter": c_adapter,
                    "adapter_key": c_adapter_key.strip() if c_adapter_key else "",
                    "tier": int(c_tier),
                    "priority": c_priority,
                    "active": bool(c_active),
                    "manual_only": bool(c_manual_only),
                    "status": "manual_only" if c_manual_only else c_status,
                    "industry": _parse_pipe_list(c_industry) if c_industry and "|" in c_industry else (c_industry.strip() if c_industry else ""),
                    "sub_industry": c_sub.strip() if c_sub else "",
                    "search_queries": normalize_search_queries(c_search_queries),
                    "location_filter": c_location_filter.strip() if c_location_filter else "",
                    "site_names": c_site_names.strip() if c_site_names else "",
                    "results_wanted": int(c_results_wanted),
                    "hours_old": int(c_hours_old),
                    "country_indeed": (c_country_indeed.strip() if c_country_indeed else "") or "USA",
                    "concurrency": int(c_concurrency),
                    "max_total_results": int(c_max_total_results),
                    "is_remote": bool(c_is_remote),
                    "continue_on_site_failure": bool(c_continue_on_site_failure),
                    "job_type": c_job_type.strip() if c_job_type else "",
                    "linkedin_fetch_description": bool(c_linkedin_fetch_description),
                    "google_search_term_template": (c_google_search_term_template.strip() if c_google_search_term_template else "") or "{query}",
                    "notes": c_notes.strip() if c_notes else "",
                }
                if not new_company["name"]:
                    st.error("Company name is required.")
                else:
                    if mode == "Edit Existing" and selected_name:
                        for idx, company in enumerate(cos):
                            if company.get("name") == selected_name:
                                cos[idx] = new_company
                                break
                    else:
                        cos.append(new_company)
                    data["companies"] = cos
                    save_yaml(registry_path, data)
                    st.success(f"Company saved to {registry_path.name}.")

            if mode == "Edit Existing" and selected_name and st.button("Delete Company"):
                data["companies"] = [company for company in cos if company.get("name") != selected_name]
                save_yaml(registry_path, data)
                st.success(f"Company deleted from {registry_path.name}.")
        with t3:
            st.caption(
                "Run the ATS healer against the selected company list. "
                "This will update `careers_url`, `adapter`, and `adapter_key` where possible, "
                "or mark companies `manual_only` when an ATS board is blocked/unsupported."
            )
            h_all_registries = st.checkbox(
                "Heal ALL Company Lists",
                value=False,
                help="If enabled, runs the healer across every job_search_companies*.yaml file in your config folder. Otherwise only the currently selected list is healed.",
            )
            h_ignore_cooldown = st.checkbox(
                "Ignore Cooldowns",
                value=False,
                help="Bypass healer cooldown_until/manual_only skip checks so you can re-test discovery changes without healing every active company.",
            )
            h_disable_waterfall = st.checkbox(
                "Disable Domain Waterfall",
                value=False,
                help="Skip probing jobs.<domain>/careers.<domain>/etc. Useful when DNS is flaky or you want to test search/direct ATS probes only.",
            )
            h_all = st.checkbox("Include All Companies (not just broken ones)", value=True)
            h_deep = st.checkbox("Deep Search (slower — uses browser to find hidden job boards)", value=False)
            h_force = st.checkbox("Also Re-check Already-Active Companies", value=False)
            h_workers = st.number_input("Parallel Workers", min_value=1, max_value=20, value=5, step=1)
            h_deep_timeout = st.number_input("Browser Timeout (seconds)", min_value=5, max_value=120, value=20, step=5)
            if st.button("🚀 Run Fix Job Listings"):
                cmd = [sys.executable, "-m", "jobsearch.cli", "heal"]
                if not h_all_registries:
                    cmd.extend(["--registry", str(registry_path)])
                if h_all:
                    cmd.append("--all")
                if h_ignore_cooldown:
                    cmd.append("--ignore-cooldown")
                if h_disable_waterfall:
                    cmd.append("--no-waterfall")
                if h_deep:
                    cmd.append("--deep")
                if h_force:
                    cmd.append("--force")
                cmd.extend(["--workers", str(int(h_workers))])
                cmd.extend(["--deep-timeout", str(float(h_deep_timeout))])
                proc = subprocess.Popen(
                    cmd,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.STDOUT,
                    encoding="utf-8",
                    env={**os.environ, "PYTHONPATH": "src;."},
                )
                log = st.empty()
                lines = []
                for raw in iter(proc.stdout.readline, ""):
                    lines.append(raw.rstrip())
                    log.code("\n".join(lines[-20:]))
                proc.wait()
                st.success("Healer complete.")
        with t4:
            st.caption("Direct YAML editor — for advanced users only. Use the List or Add / Edit tabs for normal changes.")
            raw = registry_path.read_text(encoding="utf-8") if registry_path.exists() else ""
            new_raw = st.text_area("Raw Company List", value=raw, height=600)
            if st.button("Save Company List"): registry_path.write_text(new_raw, encoding="utf-8"); st.success(f"Saved {registry_path.name}.")

        with t5:
            conn = ats_db.get_connection()
            try:
                # Use the new unified board_health table
                health_data = [dict(row) for row in ats_db.get_scraper_health_rows(conn)]
                health_df = pd.DataFrame(health_data)
                
                # Load latest scheduling decisions
                dec_path = settings.results_dir / "scheduling_decisions.jsonl"
                dec_df = pd.DataFrame()
                if dec_path.exists():
                    try:
                        with open(dec_path, "r", encoding="utf-8") as f:
                            all_dec = [json.loads(line) for line in f if line.strip()]
                        if all_dec:
                            # Keep only the latest decision per company
                            latest_dec = {}
                            for d in all_dec:
                                latest_dec[d["company"]] = d
                            dec_df = pd.DataFrame(list(latest_dec.values()))
                    except Exception:
                        pass
                
                # Fetch flapping data
                flap_map = {}
                try:
                    flapping_rows = ats_db.get_flapping_boards(conn, days=settings.health_flapping_window_days)
                    flap_map = {r["company"]: r["changes"] for r in flapping_rows}
                except Exception as e:
                    logger.debug("Failed to fetch flapping data: %s", e)
                
                if not dec_df.empty and not health_df.empty:
                    health_df = health_df.merge(
                        dec_df[["company", "action", "priority", "reason", "queue_healer"]], 
                        on="company", 
                        how="left"
                    )
                    # Rename columns for display
                    health_df.rename(columns={
                        "action": "planned_action",
                        "priority": "priority_score",
                        "reason": "scheduling_reason"
                    }, inplace=True)
                
                if not health_df.empty:
                    health_df["flapping_changes"] = health_df["company"].map(flap_map).fillna(0).astype(int)
                    
                    def get_severity(changes):
                        if changes >= settings.health_flapping_threshold_high: return "🔴 High"
                        if changes >= settings.health_flapping_threshold_med: return "🟠 Med"
                        if changes >= settings.health_flapping_threshold_low: return "🟡 Low"
                        return ""
                    
                    health_df["instability"] = health_df["flapping_changes"].apply(get_severity)
            finally:
                conn.close()

            if health_df.empty:
                st.info("No scraper health data recorded yet. Run the pipeline or healer first.")
            else:
                # Summary Cards
                st.subheader("Operational Summary")
                
                # Define states
                blocked_states = {"blocked_bot_protection", "blocked"}
                broken_states = {"broken_site", "broken"}
                stale_states = {"stale_url", "wrong_url"}
                
                now = datetime.now(timezone.utc)
                
                # Pre-calculate some helpful columns
                health_df["is_cooldown_active"] = False
                for idx, row in health_df.iterrows():
                    if row["cooldown_until"]:
                        try:
                            until = datetime.fromisoformat(str(row["cooldown_until"]).replace("Z", "+00:00"))
                            if until.tzinfo is None: until = until.replace(tzinfo=timezone.utc)
                            if until > now:
                                health_df.at[idx, "is_cooldown_active"] = True
                        except Exception: pass

                # Fetch Healer ROI
                conn = ats_db.get_connection()
                try:
                    roi = ats_db.get_healer_roi_metrics(conn, days=30)
                finally:
                    conn.close()

                total_count = len(health_df)
                healthy_count = len(health_df[health_df["board_state"] == "healthy"])
                blocked_count = len(health_df[health_df["board_state"].isin(blocked_states)])
                broken_count = len(health_df[health_df["board_state"].isin(broken_states)])
                stale_count = len(health_df[health_df["board_state"].isin(stale_states)])
                cooldown_count = health_df["is_cooldown_active"].sum()
                manual_count = health_df["manual_review_required"].fillna(0).astype(int).sum()
                unstable_count = (health_df["flapping_changes"] >= settings.health_flapping_threshold_low).sum()

                cols = st.columns(8)
                cols[0].metric("Healthy", f"{healthy_count}/{total_count}")
                cols[1].metric("Blocked", blocked_count)
                cols[2].metric("Broken", broken_count)
                cols[3].metric("Stale", stale_count)
                cols[4].metric("Cooldown", int(cooldown_count))
                cols[5].metric("Manual", int(manual_count))
                cols[6].metric("Unstable", int(unstable_count))
                cols[7].metric("Healer ROI", f"{roi['total_recoveries']} rev")

                st.divider()
                
                # Filters
                st.subheader("Filter Boards")
                f1, f2, f3, f4 = st.columns(4)
                
                with f1:
                    all_families = sorted(list(health_df["adapter"].dropna().unique()))
                    sel_family = st.multiselect("ATS Family", options=all_families)
                
                with f2:
                    all_states = sorted(list(health_df["board_state"].dropna().unique()))
                    sel_state = st.multiselect("Board State", options=all_states)
                
                with f3:
                    all_methods = sorted(list(health_df["last_success_method"].dropna().unique()))
                    sel_method = st.multiselect("Last Method", options=all_methods)
                    all_actions = sorted(list(health_df["planned_action"].dropna().unique())) if "planned_action" in health_df.columns else []
                    sel_action = st.multiselect("Planned Action", options=all_actions)
                
                with f4:
                    show_cooldown_only = st.checkbox("Only Active Cooldowns", value=False)
                    show_unstable_only = st.checkbox("Only Unstable (Flapping)", value=False)
                    show_manual_only = st.checkbox("Only Manual Review", value=False)
                    show_healer_only = st.checkbox("Only Healer Tasks", value=False)

                # Apply Filters
                filtered_df = health_df.copy()
                if sel_family:
                    filtered_df = filtered_df[filtered_df["adapter"].isin(sel_family)]
                if sel_state:
                    filtered_df = filtered_df[filtered_df["board_state"].isin(sel_state)]
                if sel_method:
                    filtered_df = filtered_df[filtered_df["last_success_method"].isin(sel_method)]
                if "planned_action" in filtered_df.columns and sel_action:
                    filtered_df = filtered_df[filtered_df["planned_action"].isin(sel_action)]
                if show_cooldown_only:
                    filtered_df = filtered_df[filtered_df["is_cooldown_active"]]
                if show_unstable_only:
                    filtered_df = filtered_df[filtered_df["flapping_changes"] >= settings.health_flapping_threshold_low]
                if show_manual_only:
                    filtered_df = filtered_df[filtered_df["manual_review_required"] == 1]
                if show_healer_only and "queue_healer" in filtered_df.columns:
                    filtered_df = filtered_df[filtered_df["queue_healer"] == True]

                health_query = st.text_input("Search boards", value="", placeholder="Company, adapter, status, notes…", key="scraper_health_search_v2")
                if health_query:
                    filtered_df = _search_text_match(
                        filtered_df,
                        health_query,
                        ["company", "adapter", "board_state", "notes", "careers_url"],
                    )

                # History & Trends
                st.divider()
                t_col1, t_col2 = st.columns([1, 2])
                with t_col1:
                    st.subheader("Operational Trends")
                    conn = ats_db.get_connection()
                    try:
                        counts = ats_db.get_state_transition_counts(conn)
                        if counts:
                            counts_df = pd.DataFrame([dict(r) for r in counts])
                            st.bar_chart(counts_df.set_index("to_state"))
                        else:
                            st.info("No transition history recorded yet.")
                    finally:
                        conn.close()
                
                with t_col2:
                    st.subheader("Recent Transitions")
                    conn = ats_db.get_connection()
                    try:
                        recent_events = ats_db.get_recent_board_health_events(conn, limit=10)
                        if recent_events:
                            ev_df = pd.DataFrame([dict(r) for r in recent_events])
                            st.dataframe(
                                ev_df[["timestamp", "company", "from_state", "to_state", "trigger_subsystem", "reason"]],
                                column_config={
                                    "timestamp": st.column_config.TextColumn("Time"),
                                    "company": st.column_config.TextColumn("Company"),
                                    "from_state": st.column_config.TextColumn("From"),
                                    "to_state": st.column_config.TextColumn("To"),
                                    "trigger_subsystem": st.column_config.TextColumn("Trigger"),
                                    "reason": st.column_config.TextColumn("Reason"),
                                },
                                hide_index=True,
                                use_container_width=True,
                            )
                        else:
                            st.info("No recent transitions.")
                    finally:
                        conn.close()

                st.divider()
                with st.expander("🩺 Healer ROI & Analytics (Last 30 Days)", expanded=False):
                    r1, r2, r3, r4 = st.columns(4)
                    r1.metric("Total Recoveries", roi["total_recoveries"])
                    r2.metric("Stale URLs Fixed", roi["stale_recoveries"])
                    r3.metric("Blocked/Broken Fixed", roi["blocked_recoveries"])
                    r4.metric("Manual Hours Saved", f"{roi['manual_hours_saved']:.1f}h", help="Estimated 30m saved per successful recovery.")
                    
                    if roi["by_family"]:
                        st.write("**Recovery Rate by ATS Family**")
                        roi_df = pd.DataFrame(roi["by_family"])
                        st.dataframe(
                            roi_df,
                            column_config={
                                "adapter": st.column_config.TextColumn("ATS Family"),
                                "count": st.column_config.NumberColumn("Successful Recoveries")
                            },
                            hide_index=True,
                            use_container_width=True
                        )

                st.divider()
                st.subheader("All Boards")
                if filtered_df.empty:
                    st.warning("No boards match your filters.")
                else:
                    # Detailed Table
                    # Add scheduling columns if they exist
                    cols_to_show = [
                        "company", "adapter", "board_state", "instability", "planned_action", 
                        "priority_score", "scheduling_reason", "consecutive_failures", 
                        "cooldown_until", "last_attempt_at", "last_success_at", 
                        "careers_url", "notes"
                    ]
                    # Filter to only existing columns
                    cols_to_show = [c for c in cols_to_show if c in filtered_df.columns]
                    
                    st.dataframe(
                        filtered_df[cols_to_show],
                        column_config={
                            "company": st.column_config.TextColumn("Company"),
                            "adapter": st.column_config.TextColumn("ATS Family"),
                            "board_state": st.column_config.TextColumn("State"),
                            "planned_action": st.column_config.TextColumn("Planned Action"),
                            "priority_score": st.column_config.NumberColumn("Priority"),
                            "scheduling_reason": st.column_config.TextColumn("Reason"),
                            "consecutive_failures": st.column_config.NumberColumn("Fail Streak"),
                            "cooldown_until": st.column_config.TextColumn("Paused Until"),
                            "last_attempt_at": st.column_config.TextColumn("Last Attempt"),
                            "last_success_at": st.column_config.TextColumn("Last Success"),
                            "careers_url": st.column_config.LinkColumn("Careers Page"),
                            "notes": st.column_config.TextColumn("Notes"),
                        },
                        hide_index=True,
                        use_container_width=True,
                    )
                    
                    # Actions for selected company
                    st.divider()
                    st.subheader("Actions")
                    sel_company = st.selectbox("Select Company for Action", options=[""] + sorted(list(filtered_df["company"].unique())))
                    if sel_company:
                        a1, a2 = st.columns(2)
                        with a1:
                            if st.button(f"Clear Cooldown for {sel_company}", use_container_width=True):
                                conn = ats_db.get_connection()
                                try:
                                    ats_db.update_board_health(conn, sel_company, cooldown_until=None, suppression_reason=None, trigger_subsystem="dashboard")
                                    st.success(f"Cleared cooldown for {sel_company}. It will be included in the next run.")
                                    st.rerun()
                                finally:
                                    conn.close()
                            
                            if st.button(f"Trigger Healer for {sel_company}", use_container_width=True, help="Run ATS discovery/fix immediately for this company."):
                                cmd = [sys.executable, "-m", "jobsearch.cli", "heal", "--all", "--force", "--ignore-cooldown", "--company", sel_company]
                                proc = subprocess.Popen(
                                    cmd,
                                    stdout=subprocess.PIPE,
                                    stderr=subprocess.STDOUT,
                                    encoding="utf-8",
                                    env={**os.environ, "PYTHONPATH": "src;."},
                                )
                                with st.status(f"Healing {sel_company}...", expanded=True) as status:
                                    log = st.empty()
                                    lines = []
                                    for raw in iter(proc.stdout.readline, ""):
                                        lines.append(raw.rstrip())
                                        log.code("\n".join(lines[-10:]))
                                    proc.wait()
                                    status.update(label=f"Heal complete for {sel_company}", state="complete")
                                st.rerun()

                        with a2:
                            if st.button(f"Mark Manual Review: {sel_company}", use_container_width=True):
                                conn = ats_db.get_connection()
                                try:
                                    ats_db.update_board_health(conn, sel_company, manual_review_required=1, board_state="manual_review", trigger_subsystem="dashboard")
                                    st.success(f"Marked {sel_company} for manual review.")
                                    st.rerun()
                                finally:
                                    conn.close()
                            
                            with st.expander(f"View History for {sel_company}"):
                                conn = ats_db.get_connection()
                                try:
                                    history = ats_db.get_board_health_history(conn, sel_company)
                                    if history:
                                        h_df = pd.DataFrame([dict(r) for r in history])
                                        st.dataframe(
                                            h_df[["timestamp", "from_state", "to_state", "trigger_subsystem", "reason"]],
                                            column_config={
                                                "timestamp": st.column_config.TextColumn("Time"),
                                                "from_state": st.column_config.TextColumn("From"),
                                                "to_state": st.column_config.TextColumn("To"),
                                                "trigger_subsystem": st.column_config.TextColumn("Trigger"),
                                                "reason": st.column_config.TextColumn("Reason"),
                                            },
                                            hide_index=True,
                                            use_container_width=True,
                                        )
                                    else:
                                        st.info("No history recorded for this company.")
                                finally:
                                    conn.close()
                            
                            if st.button(f"View Evidence for {sel_company}", use_container_width=True):
                                ev_path = settings.results_dir / "heal_evidence.jsonl"
                                if ev_path.exists():
                                    try:
                                        with open(ev_path, "r", encoding="utf-8") as f:
                                            all_ev = [json.loads(line) for line in f if line.strip()]
                                        comp_ev = [e for e in all_ev if str(e.get("company", "")).lower() == sel_company.lower()]
                                        if comp_ev:
                                            for ev in reversed(comp_ev[-3:]): # Show last 3 attempts
                                                with st.expander(f"Evidence from {ev.get('timestamp', 'unknown')}", expanded=True):
                                                    st.json(ev)
                                        else:
                                            st.info(f"No evidence records found for {sel_company} in {ev_path.name}")
                                    except Exception as e:
                                        st.error(f"Failed to read evidence: {e}")
                                else:
                                    st.info(f"Evidence file {ev_path.name} does not exist yet.")
                            
                            if st.button(f"Reset Stats for {sel_company}", use_container_width=True):
                                conn = ats_db.get_connection()
                                try:
                                    ats_db.update_board_health(conn, sel_company, consecutive_failures=0, board_state="healthy", trigger_subsystem="dashboard")
                                    st.success(f"Reset stats for {sel_company}.")
                                    st.rerun()
                                finally:
                                    conn.close()

    elif page == "Run Search":
        st.title("Run Search Pipeline")
        t1, t2, t3 = st.tabs(["Run Search", "Recent Run Log", "Manual Job Targets"])
        with t1:
            r_deep = st.checkbox("Deep Search (slower — uses browser to find jobs on complex sites)", value=False)
            r_full = st.checkbox("Full Refresh (ignore known jobs and re-fetch all descriptions)", value=False)
            r_test = st.checkbox("Use Test Company List (for testing only)", value=False)
            r_contract = st.checkbox("Include Contractor Sources", value=False)
            r_aggregator = st.checkbox("Include Aggregator Sources", value=False)
            r_jobspy = st.checkbox("Include JobSpy (Experimental)", value=False)
            r_all_companies = st.checkbox("Search ALL Company Lists", value=False, help="Search across every company registry file in your config folder.")
            r_workers = st.number_input("Parallel Workers", min_value=1, max_value=20, value=8, step=1)
            pref_options = [path for path in [settings.prefs_yaml, settings.config_dir / "job_search_preferences_test.yaml"] if path.exists()]
            comp_options = settings.get_company_registries()
            default_companies = settings.companies_yaml
            if r_test and (settings.config_dir / "job_search_companies_test.yaml") in comp_options:
                default_companies = settings.config_dir / "job_search_companies_test.yaml"
            default_index = comp_options.index(default_companies) if default_companies in comp_options else 0
            r_prefs = st.selectbox("Search Settings File", pref_options, format_func=lambda p: p.name)
            r_companies = st.selectbox("Company List", comp_options, index=default_index, format_func=_companies_file_label)
            selected_name = Path(r_companies).name
            if selected_name in {settings.contract_companies_yaml.name, "job_search_companies_contract_test.yaml"}:
                st.info("Contractor-only mode: this run searches contract-oriented job sources only.")
            elif selected_name == settings.aggregator_companies_yaml.name:
                st.info("Aggregator-only mode: this run searches lower-trust third-party job board sources only.")
            elif selected_name == settings.jobspy_companies_yaml.name:
                st.info("JobSpy-only mode: this run searches the experimental JobSpy discovery lane only.")
            elif r_contract and r_aggregator and r_jobspy:
                st.info("Combined mode: this run searches your main company list, contractor sources, aggregator sources, and JobSpy experimental sources.")
            elif r_contract and r_jobspy:
                st.info("Combined mode: this run searches your main company list, contractor sources, and JobSpy experimental sources.")
            elif r_aggregator and r_jobspy:
                st.info("Combined mode: this run searches your main company list, aggregator sources, and JobSpy experimental sources.")
            elif r_contract and r_aggregator:
                st.info("Combined mode: this run searches your main company list, contractor sources, and aggregator sources.")
            elif r_contract:
                st.info("Combined mode: this run searches both your main company list and contractor sources.")
            elif r_aggregator:
                st.info("Combined mode: this run searches both your main company list and aggregator sources. Aggregator jobs are supplemental and lower trust.")
            elif r_jobspy:
                st.info("Combined mode: this run searches both your main company list and JobSpy experimental sources. JobSpy results are exploratory and lower trust.")

            st.divider()

            # Auto-refresh configuration
            st.subheader("Auto-Refresh Settings")
            col1, col2 = st.columns([2, 1])
            with col1:
                enable_auto_refresh = st.checkbox("Enable Auto-Refresh", value=False, help="Run scraper automatically on a schedule without manual intervention")
            with col2:
                refresh_interval = st.number_input("Interval (hours)", min_value=1, max_value=24, value=1, step=1, disabled=not enable_auto_refresh)

            if enable_auto_refresh:
                # Get database connection for scheduler
                try:
                    conn = ats_db.connect()
                    scheduler.start_auto_refresh(conn, interval_hours=int(refresh_interval))

                    last_run = scheduler.get_last_run(conn)
                    next_run = scheduler.get_next_run(conn)

                    col1, col2, col3 = st.columns(3)
                    with col1:
                        if last_run:
                            last_dt = datetime.fromisoformat(last_run)
                            st.info(f"Last run: {last_dt.strftime('%Y-%m-%d %H:%M UTC')}")
                        else:
                            st.info("Last run: never")
                    with col2:
                        if next_run:
                            next_dt = datetime.fromisoformat(next_run)
                            st.info(f"Next run: {next_dt.strftime('%Y-%m-%d %H:%M UTC')}")
                        else:
                            st.info("Next run: pending")
                    with col3:
                        if st.button("Stop Auto-Refresh", key="stop_auto_refresh"):
                            scheduler.stop_auto_refresh()
                            st.success("Auto-refresh stopped")
                            st.rerun()
                    conn.close()
                except Exception as e:
                    st.error(f"Error configuring auto-refresh: {e}")
            else:
                # Show option to stop if one is running
                try:
                    conn = ats_db.connect()
                    next_run = scheduler.get_next_run(conn)
                    if next_run:
                        st.warning("Auto-refresh is currently running. Disable above to stop it.")
                        if st.button("Stop Auto-Refresh Now", key="stop_auto_refresh_now"):
                            scheduler.stop_auto_refresh()
                            st.success("Auto-refresh stopped")
                            st.rerun()
                    conn.close()
                except Exception:
                    pass

            st.divider()

            if st.button("🚀 Start Pipeline", type="primary"):
                cmd = [sys.executable, "-m", "jobsearch.cli", "run", "--workers", str(int(r_workers))]
                if r_deep: cmd.append("--deep-search")
                if r_full: cmd.append("--full-refresh")
                if r_test: cmd.append("--test-companies")
                if r_contract: cmd.append("--contract-sources")
                if r_aggregator: cmd.append("--aggregator-sources")
                if r_jobspy: cmd.append("--jobspy-sources")
                if r_all_companies: cmd.append("--all-companies")
                if Path(r_prefs) != settings.prefs_yaml: cmd.extend(["--prefs", str(r_prefs)])
                if not r_all_companies and Path(r_companies) != settings.companies_yaml: cmd.extend(["--companies", str(r_companies)])

                # Load jobspy concurrency setting from preferences if JobSpy is included
                env = {**os.environ, "PYTHONUNBUFFERED": "1", "PYTHONPATH": "src;."}
                if r_jobspy:
                    try:
                        prefs = load_yaml(r_prefs)
                        jobspy_conc = prefs.get("performance", {}).get("scrape_jobspy_concurrency", settings.scrape_jobspy_concurrency)
                        env["JOBSEARCH_SCRAPE_JOBSPY_CONCURRENCY"] = str(int(jobspy_conc))
                    except Exception:
                        pass  # Use default if loading fails

                log = st.empty(); lines = []
                proc = subprocess.Popen(
                    cmd,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.STDOUT,
                    encoding="utf-8",
                    env=env,
                )
                for raw in iter(proc.stdout.readline, ""):
                    lines.append(raw.rstrip())
                    log.code("\n".join(lines[-30:]))
                proc.wait(); _invalidate_data_cache(); st.success("Pipeline complete.")

                # Display token usage metrics
                try:
                    token_conn = ats_db.connect()
                    token_usage = _get_today_token_usage(token_conn)
                    token_conn.close()

                    if token_usage["total_tokens"] > 0:
                        col1, col2, col3 = st.columns(3)
                        with col1:
                            st.metric("LLM Tokens Used", f"{token_usage['total_tokens']:,}")
                        with col2:
                            st.metric("LLM Calls", token_usage["call_count"])
                        with col3:
                            budget_pct = token_usage["percent_used"]
                            if budget_pct >= 80:
                                st.warning(f"Budget: {budget_pct:.1f}%")
                            else:
                                st.metric("Budget Used", f"{budget_pct:.1f}%")
                except Exception:
                    pass

        with t2:
            log_path = settings.log_file
            if log_path.exists():
                experimental_df, jobspy_board_df = _parse_experimental_source_log(log_path)
                if not experimental_df.empty:
                    st.subheader("Experimental / API Source Results")
                    source_summary = experimental_df[experimental_df["result"] != "SUMMARY"].copy()
                    if not source_summary.empty:
                        st.dataframe(
                            source_summary[
                                [
                                    "company",
                                    "adapter",
                                    "evaluated",
                                    "persisted",
                                    "new",
                                    "dropped",
                                    "status",
                                    "scrape_ms",
                                    "process_ms",
                                    "note",
                                ]
                            ],
                            column_config={
                                "company": st.column_config.TextColumn("Source Row"),
                                "adapter": st.column_config.TextColumn("Adapter"),
                                "evaluated": st.column_config.NumberColumn("Evaluated"),
                                "persisted": st.column_config.NumberColumn("Persisted"),
                                "new": st.column_config.NumberColumn("New"),
                                "dropped": st.column_config.NumberColumn("Dropped"),
                                "status": st.column_config.TextColumn("Status"),
                                "scrape_ms": st.column_config.NumberColumn("Scrape ms"),
                                "process_ms": st.column_config.NumberColumn("Process ms"),
                                "note": st.column_config.TextColumn("Details"),
                            },
                            hide_index=True,
                            use_container_width=True,
                        )
                    totals_df = experimental_df[experimental_df["result"] == "SUMMARY"].copy()
                    if not totals_df.empty:
                        st.caption("Adapter totals from the latest run")
                        st.dataframe(
                            totals_df[["company", "evaluated", "persisted", "scrape_ms", "process_ms"]],
                            hide_index=True,
                            use_container_width=True,
                        )
                if not jobspy_board_df.empty:
                    st.subheader("JobSpy Board Metrics")
                    st.dataframe(
                        jobspy_board_df[
                            [
                                "company",
                                "board",
                                "requested",
                                "attempted",
                                "success",
                                "failed",
                                "skipped",
                                "runtime_ms",
                                "raw",
                                "normalized",
                                "deduped",
                            ]
                        ],
                        column_config={
                            "company": st.column_config.TextColumn("Source Row"),
                            "board": st.column_config.TextColumn("Board"),
                            "requested": st.column_config.NumberColumn("Requested"),
                            "attempted": st.column_config.NumberColumn("Attempted"),
                            "success": st.column_config.NumberColumn("Success"),
                            "failed": st.column_config.NumberColumn("Failed"),
                            "skipped": st.column_config.NumberColumn("Skipped"),
                            "runtime_ms": st.column_config.NumberColumn("Runtime ms"),
                            "raw": st.column_config.NumberColumn("Raw"),
                            "normalized": st.column_config.NumberColumn("Normalized"),
                            "deduped": st.column_config.NumberColumn("Cross-board Deduped"),
                        },
                        hide_index=True,
                        use_container_width=True,
                    )
                st.caption(f"Showing tail of {log_path.name}")
                st.code("\n".join(log_path.read_text(encoding="utf-8", errors="ignore").splitlines()[-60:]))
            else:
                st.info("No scrape log found yet. Run the pipeline to generate one.")

        with t3:
            manual_csv = settings.results_dir / "job_search_v6_manual_targets.csv"
            csv_value = manual_csv.read_text(encoding="utf-8") if manual_csv.exists() else "company,title,url,notes\n"
            st.caption("Add jobs here that you found manually (e.g. from LinkedIn or a company website). Format: company, title, url, notes — one per line.")
            edited_csv = st.text_area("Manual Job Targets", value=csv_value, height=320)
            if st.button("Save Manual Job Targets"):
                manual_csv.write_text(edited_csv, encoding="utf-8")
                st.success(f"Saved {manual_csv.name}.")


    else:
        view_map = {
            "Action Center": render_action_center,
            "Tailoring Studio": render_tailoring_studio,
            "Submission Review": render_submission_review,
            "Learning Loop": render_learning_loop,
            "Market Strategy": render_market_strategy,
            "Dashboard": render_home,
            "Analytics": render_analytics,
            "Company Profiles": render_company_profiles,
            "Contacts": render_contacts,
            "Question Bank": render_question_bank,
            "Message Templates": render_templates,
            "Training Tracker": render_training,
            "Journal": render_journal,
            "Pipeline": render_pipeline,
            "Weekly Activity": render_activity_report,
            }

        conn = ats_db.get_connection(); _safe_render(view_map[page], conn, page_name=page)

if __name__ == "__main__": main()
