"""app.py — Job Search v6 Dashboard"""

from __future__ import annotations

import json
import math
import os
import re
import shutil
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
import streamlit as st
import yaml

# ── ATS database layer ────────────────────────────────────────────────────────
try:
    from db.connection import get_db
    from db.schema import init_db
    from services.opportunity_service import sync_from_excel
    from services.importer import import_tracker_csv
    from pages.pipeline_page import render_pipeline
    from pages.analytics_page import render_analytics
    _ATS_AVAILABLE = True
except ImportError:
    _ATS_AVAILABLE = False

# ── Paths ─────────────────────────────────────────────────────────────────────

BASE_DIR = Path(__file__).resolve().parent
CONFIG_DIR = BASE_DIR / "config"
RESULTS_DIR = BASE_DIR / "results"

XLSX_PATH      = RESULTS_DIR / "job_search_v6_results.xlsx"
REJECTED_CSV   = RESULTS_DIR / "job_search_v6_rejected.csv"
STATUS_JSON    = RESULTS_DIR / "job_status.json"
PREFS_YAML     = CONFIG_DIR  / "job_search_preferences.yaml"
COMPANIES_YAML = CONFIG_DIR  / "job_search_companies.yaml"
COMPANIES_BAK  = CONFIG_DIR  / "job_search_companies.yaml.bak"
DB_PATH        = RESULTS_DIR / "jobsearch.db"
TRACKER_CSV    = RESULTS_DIR / "ApplicationTracker.csv"

# Initialise DB on startup (idempotent)
if _ATS_AVAILABLE:
    import sqlite3 as _sqlite3
    from db.schema import init_db as _init_db
    _init_conn = _sqlite3.connect(str(DB_PATH))
    _init_db(_init_conn)
    _init_conn.close()

# ── Page config ───────────────────────────────────────────────────────────────

st.set_page_config(page_title="Job Search Dashboard", layout="wide", page_icon="💼")

# ── Global CSS ────────────────────────────────────────────────────────────────
st.markdown("""
<style>
/* ── Metric cards: subtle border, no forced background colour ──────────────── */
[data-testid="stMetric"] {
    border: 1px solid rgba(128,128,160,0.25);
    border-radius: 10px;
    padding: 12px 16px;
}
[data-testid="stMetricLabel"] { font-size: 0.78rem !important; text-transform: uppercase; letter-spacing: 0.05em; }
[data-testid="stMetricValue"] { font-size: 1.6rem !important; font-weight: 700; }

/* ── Primary button ────────────────────────────────────────────────────────── */
[data-testid="stBaseButton-primary"] {
    background: linear-gradient(135deg, #7c3aed, #4f46e5) !important;
    border: none !important;
    font-weight: 600 !important;
    letter-spacing: 0.02em;
    color: white !important;
}
[data-testid="stBaseButton-primary"]:hover { opacity: 0.88; }

/* ── Code / log blocks ─────────────────────────────────────────────────────── */
[data-testid="stCode"] pre {
    font-size: 0.82rem;
    line-height: 1.5;
}

/* ── Rounded table/editor frames ───────────────────────────────────────────── */
[data-testid="stDataFrame"] > div,
[data-testid="stDataEditor"] > div {
    border-radius: 8px;
    overflow: hidden;
}

/* ── Alert rounding ────────────────────────────────────────────────────────── */
[data-testid="stAlert"] { border-radius: 8px; }
</style>
""", unsafe_allow_html=True)

# ── Constants ─────────────────────────────────────────────────────────────────

# Scraper bucket names (uppercase from job_search_v6.py)
SCRAPER_BUCKETS = {"APPLY NOW", "REVIEW TODAY", "WATCH", "MANUAL REVIEW", "IGNORE"}

# Statuses the user can assign; these act as bucket overrides.
USER_STATUSES = ["Applied", "Rejected", "APPLY NOW", "REVIEW TODAY", "WATCH", "MANUAL REVIEW"]

KNOWN_ADAPTERS = [
    "greenhouse", "lever", "ashby", "workday", "workday_manual",
    "custom_manual", "custom_site", "custom_blackrock", "custom_schwab", "custom_spglobal",
]

# Columns shown in job tables (subset of TRACKER_COLS from job_search_v6.py)
DISPLAY_COLS = [
    "company", "tier", "title", "score", "fit_band",
    "location", "salary_range", "age_days", "is_new",
    "matched_keywords", "decision_reason", "url",
]


# ── Helpers ───────────────────────────────────────────────────────────────────

def _job_key(company: str, title: str, url: str) -> str:
    """Stable dict key for a job; matches the fields written by the scraper."""
    return f"{str(company).strip()}||{str(title).strip()}||{str(url).strip()}"


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


# ── Data I/O ──────────────────────────────────────────────────────────────────

@st.cache_data(show_spinner=False)
def _load_xlsx() -> pd.DataFrame:
    """Read 'All Jobs' sheet from the scraper Excel. Cached until invalidated."""
    if not XLSX_PATH.exists():
        return pd.DataFrame()
    try:
        df = pd.read_excel(XLSX_PATH, sheet_name="All Jobs", dtype=str)
        for num_col in ("score", "tier", "age_days", "salary_low", "salary_high"):
            if num_col in df.columns:
                df[num_col] = pd.to_numeric(df[num_col], errors="coerce")
        df["_key"] = df.apply(
            lambda r: _job_key(r.get("company", ""), r.get("title", ""), r.get("url", "")),
            axis=1,
        )
        return df
    except Exception as exc:
        st.error(f"Could not read results file: {exc}")
        return pd.DataFrame()


@st.cache_data(show_spinner=False)
def _load_rejected_csv() -> pd.DataFrame:
    if not REJECTED_CSV.exists():
        return pd.DataFrame()
    try:
        return pd.read_csv(REJECTED_CSV, dtype=str)
    except Exception:
        return pd.DataFrame()


def _invalidate_data_cache() -> None:
    _load_xlsx.clear()
    _load_rejected_csv.clear()


def load_status_overrides() -> dict:
    """User status overrides — not cached because the UI mutates them."""
    if not STATUS_JSON.exists():
        return {}
    try:
        return json.loads(STATUS_JSON.read_text(encoding="utf-8"))
    except Exception:
        return {}


def save_status_overrides(overrides: dict) -> None:
    RESULTS_DIR.mkdir(exist_ok=True)
    STATUS_JSON.write_text(json.dumps(overrides, indent=2, ensure_ascii=False), encoding="utf-8")


def load_yaml_file(path: Path) -> dict:
    if not path.exists():
        return {}
    with path.open("r", encoding="utf-8") as fh:
        return yaml.safe_load(fh) or {}


def save_yaml_file(path: Path, data: dict) -> None:
    with path.open("w", encoding="utf-8") as fh:
        yaml.safe_dump(data, fh, sort_keys=False, allow_unicode=True)


def _scroll_hint(n_cols: int = 0, threshold: int = 6) -> None:
    """Render a right-scroll indicator when a table has many columns."""
    if n_cols < threshold:
        return
    st.markdown(
        '<div style="'
        "display:flex; align-items:center; justify-content:flex-end; gap:6px;"
        "margin:-6px 0 10px; padding:5px 10px; border-radius:6px;"
        "background:linear-gradient(90deg,transparent 0%,rgba(124,58,237,0.18) 100%);"
        "border-right:3px solid #7c3aed; font-size:0.78rem; font-weight:600;"
        "color:#a78bfa; letter-spacing:0.04em;"
        '">'
        "more columns &nbsp;&#8594;"
        "</div>",
        unsafe_allow_html=True,
    )


def _apply_overrides(df: pd.DataFrame, overrides: dict) -> pd.DataFrame:
    """
    Attach user_status and effective_bucket columns.
    effective_bucket = user_status when set, else the scraper's action_bucket (uppercased).
    """
    if df.empty:
        return df
    df = df.copy()
    df["user_status"] = df["_key"].map(lambda k: overrides.get(k, {}).get("user_status", ""))
    scraper_bucket = df.get("action_bucket", pd.Series("", index=df.index)).str.upper().fillna("")
    df["effective_bucket"] = df.apply(
        lambda r: r["user_status"] if r["user_status"] else r["_scraper_bucket"]
        if "_scraper_bucket" in r.index else scraper_bucket[r.name],
        axis=1,
    )
    # Simpler version without the nested conditional:
    df["effective_bucket"] = [
        row["user_status"] if row["user_status"] else str(row.get("action_bucket", "")).upper()
        for _, row in df.iterrows()
    ]
    return df


# ── Sidebar ───────────────────────────────────────────────────────────────────

st.sidebar.title("💼 Job Search")

_nav_options = ["Results", "Pipeline", "Analytics", "Run Pipeline", "Preferences", "Companies"]
page = st.sidebar.radio(
    "Navigate",
    _nav_options,
    label_visibility="collapsed",
)

if XLSX_PATH.exists():
    mtime = datetime.fromtimestamp(XLSX_PATH.stat().st_mtime)
    st.sidebar.caption(f"Last run: {mtime.strftime('%b %d %Y  %H:%M')}")
else:
    st.sidebar.caption("No results yet — run the pipeline.")

st.sidebar.divider()

# Quick stats in sidebar when results exist
_df_sidebar = _load_xlsx()
_ov_sidebar = load_status_overrides()
if not _df_sidebar.empty:
    _applied    = sum(1 for v in _ov_sidebar.values() if v.get("user_status") == "Applied")
    _apply_now  = int((_df_sidebar.get("action_bucket", pd.Series(dtype=str)).str.upper() == "APPLY NOW").sum())
    _review     = int((_df_sidebar.get("action_bucket", pd.Series(dtype=str)).str.upper() == "REVIEW TODAY").sum())
    st.sidebar.metric("Total Jobs", len(_df_sidebar))
    c1, c2 = st.sidebar.columns(2)
    c1.metric("Apply Now", _apply_now)
    c2.metric("Review", _review)
    st.sidebar.metric("Applied", _applied)
else:
    st.sidebar.caption("No results yet.")


# ════════════════════════════════════════════════════════════════════════════════
# PAGE: RUN PIPELINE
# ════════════════════════════════════════════════════════════════════════════════

if page == "Run Pipeline":
    st.title("Run Job Search Pipeline")
    st.markdown("Runs `run_job_search_v6.py` and refreshes results when complete.")

    col_a, col_b = st.columns([3, 1])
    with col_a:
        extra_args = st.text_input(
            "Extra arguments (optional)",
            placeholder="--company-limit 10  --company-allowlist Addepar",
        )
    with col_b:
        use_test = st.checkbox("Test companies only", help="Passes --test-companies")

    if st.button("🚀 Start Pipeline", type="primary"):
        cmd = [sys.executable, str(BASE_DIR / "run_job_search_v6.py")]
        if use_test:
            cmd.append("--test-companies")
        if extra_args.strip():
            cmd.extend(extra_args.strip().split())

        status_label = st.empty()
        progress_bar  = st.empty()
        log_box       = st.empty()

        try:
            proc = subprocess.Popen(
                [sys.executable, "-u"] + cmd[1:],  # -u = unbuffered stdout
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                encoding="utf-8",
                errors="replace",
                env={**os.environ, "PYTHONIOENCODING": "utf-8", "PYTHONUNBUFFERED": "1"},
                cwd=str(BASE_DIR),
            )

            lines_buf: list[str] = []
            total_companies = 0
            done_companies  = 0
            # Pattern: "  CompanyName   tier=N  evaluated=N ..."
            _CO_LINE = re.compile(r"^\s{1,4}\S.*tier=\d+.*evaluated=\s*\d+")
            _TOT_LINE = re.compile(r"Companies selected:\s*(\d+)\s*of\s*(\d+)")

            status_label.markdown("**Pipeline running…**")

            for raw in iter(proc.stdout.readline, ""):
                line = raw.rstrip()
                lines_buf.append(line)

                m_tot = _TOT_LINE.search(line)
                if m_tot:
                    total_companies = int(m_tot.group(2))

                if _CO_LINE.match(line):
                    done_companies += 1

                if total_companies > 0:
                    pct = min(done_companies / total_companies, 1.0)
                    progress_bar.progress(
                        pct,
                        text=f"Company {done_companies} of {total_companies}",
                    )
                else:
                    progress_bar.progress(0, text="Starting…")

                # Rolling log — last 40 lines so the page stays usable
                log_box.code("\n".join(lines_buf[-40:]), language=None)

            proc.wait()
            combined = "\n".join(lines_buf)

            _invalidate_data_cache()
            progress_bar.progress(1.0, text=f"{done_companies} of {total_companies} companies complete")

            if proc.returncode == 0:
                status_label.success("Pipeline finished successfully.")
                if _ATS_AVAILABLE and XLSX_PATH.exists():
                    try:
                        _sync_conn = get_db()
                        _sync_result = sync_from_excel(_sync_conn, XLSX_PATH, STATUS_JSON)
                        st.info(
                            f"ATS sync: {_sync_result['inserted']} new · "
                            f"{_sync_result['updated']} updated · "
                            f"{_sync_result['skipped']} skipped"
                        )
                    except Exception as _sync_exc:
                        st.warning(f"ATS sync failed: {_sync_exc}")
            else:
                status_label.error(f"Pipeline exited with code {proc.returncode}.")

            # Final full log
            log_box.code(combined[-8000:] if len(combined) > 8000 else combined, language=None)

        except Exception as exc:
            st.error(f"Failed to start pipeline: {exc}")

    # Manual sync controls (always visible)
    st.divider()
    st.markdown("### History")
    _hist_path = RESULTS_DIR / "job_search_history_v6.json"
    _hist_count = 0
    if _hist_path.exists():
        try:
            _hist_count = len(json.loads(_hist_path.read_text(encoding="utf-8")))
        except Exception:
            pass
    st.caption(
        f"The scraper skips jobs already in the history file so you only see new postings each run. "
        f"Current history: **{_hist_count}** jobs seen."
    )
    if st.button("🗑 Clear History", help="Remove all seen-job records so the next run re-evaluates everything"):
        _hist_path.write_text("{}", encoding="utf-8")
        st.success("History cleared. The next run will re-evaluate all jobs.")

    if _ATS_AVAILABLE:
        st.divider()
        st.markdown("### ATS Database")
        _sc1, _sc2 = st.columns(2)
        with _sc1:
            if st.button("↻ Sync Excel → DB", help="Import current results into the ATS database"):
                if XLSX_PATH.exists():
                    with st.spinner("Syncing…"):
                        _r = sync_from_excel(get_db(), XLSX_PATH, STATUS_JSON)
                    st.success(f"{_r['inserted']} new · {_r['updated']} updated · {_r['skipped']} skipped")
                else:
                    st.warning("No results file found. Run the pipeline first.")
        with _sc2:
            if TRACKER_CSV.exists() and st.button("⬆ Import ApplicationTracker.csv"):
                with st.spinner("Importing…"):
                    _tr = import_tracker_csv(get_db(), TRACKER_CSV)
                st.success(f"{_tr['inserted']} inserted · {_tr['updated']} updated · {_tr['errors']} errors")


# ════════════════════════════════════════════════════════════════════════════════
# PAGE: RESULTS
# ════════════════════════════════════════════════════════════════════════════════

elif page == "Pipeline":
    if _ATS_AVAILABLE:
        conn = get_db()
        render_pipeline(conn)
    else:
        st.error("ATS database modules not available. Check that db/ and services/ packages are installed.")

elif page == "Analytics":
    if _ATS_AVAILABLE:
        conn = get_db()
        render_analytics(conn)
    else:
        st.error("ATS database modules not available.")

elif page == "Results":
    st.title("Job Search Results")

    if not XLSX_PATH.exists():
        st.info("No results file found. Run the pipeline first.")
        st.stop()

    df_all = _load_xlsx()
    overrides = load_status_overrides()

    if df_all.empty:
        st.warning(
            "The pipeline ran but found no new jobs matching your filters. "
            "This usually means all matching jobs are already in your history file from a previous run. "
            "Go to **Run Pipeline → Clear History** and run again to re-evaluate all jobs."
        )
        st.stop()

    df = _apply_overrides(df_all, overrides)

    # ── Summary row ──────────────────────────────────────────────────────────
    n_total   = len(df)
    n_new     = int(df.get("is_new", pd.Series(dtype=str)).str.lower().eq("true").sum())
    n_applied = int((df["user_status"] == "Applied").sum())
    n_rej_user = int((df["user_status"] == "Rejected").sum())
    avg_score = df["score"].mean() if "score" in df.columns else 0.0

    c1, c2, c3, c4, c5 = st.columns(5)
    c1.metric("Total Kept", n_total)
    c2.metric("New This Run", n_new)
    c3.metric("Avg Score", f"{avg_score:.1f}")
    c4.metric("Applied", n_applied)
    c5.metric("User Rejected", n_rej_user)

    st.divider()

    # ── Bucket tabs ───────────────────────────────────────────────────────────
    tabs = st.tabs([
        "⚡ Apply Now",
        "📋 Review Today",
        "👁 Watch",
        "🔍 Manual Review",
        "✅ Applied",
        "❌ Rejected",
        "🚫 Filtered Out",
    ])

    # Map each tab to the effective_bucket value it should display
    BUCKET_TAB_MAP = [
        ("APPLY NOW",    tabs[0], ["Applied", "REVIEW TODAY", "WATCH", "Rejected"]),
        ("REVIEW TODAY", tabs[1], ["APPLY NOW", "Applied", "WATCH", "Rejected"]),
        ("WATCH",        tabs[2], ["APPLY NOW", "REVIEW TODAY", "Applied", "Rejected"]),
        ("MANUAL REVIEW",tabs[3], ["APPLY NOW", "Applied", "Rejected"]),
        ("Applied",      tabs[4], ["APPLY NOW", "REVIEW TODAY", "WATCH"]),
        ("Rejected",     tabs[5], ["APPLY NOW", "REVIEW TODAY", "WATCH", "Applied"]),
    ]

    all_changed_overrides: dict = {}

    for bucket_name, tab, move_options in BUCKET_TAB_MAP:
        bucket_df = df[df["effective_bucket"] == bucket_name].copy()
        bucket_df = bucket_df.sort_values("score", ascending=False, na_position="last")

        with tab:
            if bucket_df.empty:
                st.info("No jobs in this bucket.")
                continue

            vis_cols = [c for c in DISPLAY_COLS if c in bucket_df.columns]
            display_df = bucket_df[vis_cols + ["user_status", "_key"]].copy()
            display_df["user_status"] = display_df["user_status"].fillna("")

            st.caption(f"{len(bucket_df)} job(s)")
            _scroll_hint(len(vis_cols) + 1)  # +1 for Move To column

            edited_df = st.data_editor(
                display_df.drop(columns=["_key"]),
                column_config={
                    "user_status": st.column_config.SelectboxColumn(
                        "Move To",
                        options=[""] + move_options,
                        required=False,
                        width="medium",
                    ),
                    "url": st.column_config.LinkColumn("URL", width="small"),
                    "score": st.column_config.NumberColumn("Score", format="%.1f", width="small"),
                    "tier": st.column_config.NumberColumn("Tier", width="small"),
                    "age_days": st.column_config.NumberColumn("Days Old", format="%d", width="small"),
                    "company": st.column_config.TextColumn("Company", width="medium"),
                    "title": st.column_config.TextColumn("Title", width="large"),
                    "decision_reason": st.column_config.TextColumn("Reason", width="large"),
                    "is_new": st.column_config.CheckboxColumn("New", width="small"),
                },
                disabled=[c for c in vis_cols if c != "user_status"],
                hide_index=True,
                use_container_width=True,
                key=f"editor_{bucket_name}",
            )

            # Detect changed rows by comparing user_status against stored overrides
            for i, row in edited_df.iterrows():
                new_status = str(row.get("user_status") or "").strip()
                key = display_df.iloc[list(display_df.index).index(i)]["_key"]
                original_status = overrides.get(key, {}).get("user_status", "")
                if new_status and new_status != original_status:
                    entry = dict(overrides.get(key, {}))
                    entry["user_status"] = new_status
                    if new_status == "Applied" and not entry.get("applied_at"):
                        entry["applied_at"] = _now_iso()
                    all_changed_overrides[key] = entry

    # Filtered Out tab — scraper rejects, read-only
    with tabs[6]:
        df_rej = _load_rejected_csv()
        if df_rej.empty:
            st.info("No scraper-filtered jobs found.")
        else:
            st.caption(f"{len(df_rej)} job(s) filtered by scraper rules")

            # Stage breakdown summary
            if "drop_stage" in df_rej.columns:
                stage_counts = df_rej["drop_stage"].value_counts()
                sc = st.columns(min(len(stage_counts), 5))
                for i, (stage, cnt) in enumerate(stage_counts.items()):
                    sc[i % len(sc)].metric(stage or "Unknown", cnt)
                st.divider()

            # Build a readable rejection summary column
            def _fmt_rejection(row: pd.Series) -> str:
                stage = str(row.get("drop_stage") or "").strip()
                reason = str(row.get("drop_reason") or "").strip()
                if not stage and not reason:
                    return "—"
                STAGE_LABELS = {
                    "Title Gate":      "Title didn't pass gate",
                    "Salary Floor":    "Salary below minimum",
                    "Location Policy": "Location not eligible",
                    "Score Threshold": "Score too low",
                    "Duplicate":       "Already seen",
                }
                label = STAGE_LABELS.get(stage, stage)
                # Make certain raw codes more readable
                detail = reason
                if reason.startswith("title_fail:"):
                    code = reason.replace("title_fail:", "")
                    detail = {
                        "no_positive_keyword": "No required title keyword matched",
                        "hard_disqualifier":   "Matched a disqualifying title keyword",
                        "modifier_required":   "Title keyword requires a qualifying modifier",
                    }.get(code, code)
                elif " < " in reason and stage == "Score Threshold":
                    parts = reason.split(" < ")
                    detail = f"Scored {parts[0]} (need {parts[1]})"
                return f"{label}: {detail}" if detail else label

            df_display = df_rej.copy()
            df_display["Rejection Reason"] = df_display.apply(_fmt_rejection, axis=1)

            rej_vis = [c for c in ["company", "title", "Rejection Reason", "location", "url"] if c in df_display.columns]
            if "score" in df_display.columns:
                df_display["score"] = pd.to_numeric(df_display["score"], errors="coerce")
                rej_vis.insert(2, "score")

            # Optional stage filter
            if "drop_stage" in df_rej.columns:
                stage_filter = st.multiselect(
                    "Filter by stage",
                    options=sorted(df_rej["drop_stage"].dropna().unique()),
                    default=[],
                    placeholder="All stages",
                )
                if stage_filter:
                    df_display = df_display[df_display["drop_stage"].isin(stage_filter)]

            _rej_cols = [c for c in rej_vis if c in df_display.columns]
            _scroll_hint(len(_rej_cols))
            st.dataframe(
                df_display[_rej_cols],
                use_container_width=True,
                hide_index=True,
                column_config={
                    "url": st.column_config.LinkColumn("URL", width="small"),
                    "score": st.column_config.NumberColumn("Score", format="%.1f", width="small"),
                    "Rejection Reason": st.column_config.TextColumn("Rejection Reason", width="large"),
                },
            )

    # Persist any changes made this render
    if all_changed_overrides:
        merged = {**overrides, **all_changed_overrides}
        save_status_overrides(merged)
        st.toast(f"Saved {len(all_changed_overrides)} status update(s).", icon="✅")
        st.rerun()


# ════════════════════════════════════════════════════════════════════════════════
# PAGE: PREFERENCES
# ════════════════════════════════════════════════════════════════════════════════

elif page == "Preferences":
    st.title("Job Search Preferences")

    prefs = load_yaml_file(PREFS_YAML)
    raw_yaml = PREFS_YAML.read_text(encoding="utf-8") if PREFS_YAML.exists() else ""

    if not prefs:
        st.warning("No preferences file found at config/job_search_preferences.yaml")
        st.stop()

    comp     = prefs.get("search", {}).get("compensation", {})
    search   = prefs.get("search", {})
    scoring  = prefs.get("scoring", {})
    titles   = prefs.get("titles", {})
    keywords = prefs.get("keywords", {})

    # ── Shared widget: editable keyword→weight table ───────────────────────────
    def _kw_weight_editor(data_dict: dict, editor_key: str, note: str = "") -> dict:
        """Render a keyword→weight mapping as an editable table; return updated dict."""
        rows = [{"keyword": k, "weight": int(v)} for k, v in sorted(data_dict.items(), key=lambda x: -x[1])]
        df_kw = pd.DataFrame(rows) if rows else pd.DataFrame(columns=["keyword", "weight"])
        if note:
            st.caption(note)
        edited = st.data_editor(
            df_kw,
            column_config={
                "keyword": st.column_config.TextColumn("Keyword / Phrase", width="large"),
                "weight": st.column_config.NumberColumn("Points", min_value=1, max_value=50, step=1, width="small"),
            },
            num_rows="dynamic",
            hide_index=True,
            use_container_width=True,
            key=editor_key,
        )
        result: dict = {}
        for _, row in edited.iterrows():
            kw = str(row.get("keyword") or "").strip()
            wt = row.get("weight")
            if kw and wt is not None and not (isinstance(wt, float) and math.isnan(wt)):
                try:
                    result[kw] = int(wt)
                except (ValueError, TypeError):
                    pass
        return result

    tab_comp, tab_title, tab_jd, tab_scoring, tab_raw = st.tabs([
        "Compensation & Location",
        "Title Keywords",
        "JD Keywords",
        "Scoring",
        "Full YAML Editor",
    ])

    # ── Compensation & Location ───────────────────────────────────────────────
    with tab_comp:
        st.subheader("Compensation")
        fc1, fc2, fc3 = st.columns(3)
        with fc1:
            f_min_sal = st.number_input(
                "Min Salary (USD)", value=int(comp.get("min_salary_usd", 170000)), step=5000,
            )
        with fc2:
            f_enforce = st.checkbox("Enforce Min Salary", value=bool(comp.get("enforce_min_salary", True)))
            f_allow_missing = st.checkbox("Allow Missing Salary", value=bool(comp.get("allow_missing_salary", True)))
        with fc3:
            basis_opts = ["midpoint", "low_end", "high_end"]
            f_basis = st.selectbox(
                "Salary Floor Basis", basis_opts,
                index=basis_opts.index(comp.get("salary_floor_basis", "midpoint")),
            )

        st.subheader("Location")
        loc_opts = ["remote_only", "remote_or_hybrid", "any"]
        f_loc_policy = st.selectbox(
            "Location Policy", loc_opts,
            index=loc_opts.index(search.get("location_policy", "remote_only")),
        )

        if st.button("Save Compensation & Location", type="primary", key="save_comp"):
            try:
                prefs.setdefault("search", {}).setdefault("compensation", {}).update({
                    "min_salary_usd": f_min_sal,
                    "target_salary_usd": f_min_sal,
                    "preferred_remote_min_salary_usd": f_min_sal,
                    "enforce_min_salary": f_enforce,
                    "allow_missing_salary": f_allow_missing,
                    "salary_floor_basis": f_basis,
                })
                prefs["search"]["location_policy"] = f_loc_policy
                save_yaml_file(PREFS_YAML, prefs)
                st.success("Saved.")
            except Exception as exc:
                st.error(f"Save failed: {exc}")

    # ── Title Keywords ─────────────────────────────────────────────────────────
    with tab_title:
        fast_track_min = int(titles.get("fast_track_min_weight", 8))
        st.info(
            f"**How title scoring works:** A title must match at least one positive keyword "
            f"to pass the gate. Weighted keywords then determine the score boost — weight "
            f"≥ **{fast_track_min}** triggers a Fast-Track base score of 50. "
            f"Disqualifiers are hard rejects applied before any scoring."
        )

        col_pos, col_neg = st.columns(2)
        with col_pos:
            st.subheader("Positive Keywords (gate — unweighted)")
            st.caption("Title must match at least one of these to proceed past the title gate.")
            pos_current = titles.get("positive_keywords", [])
            f_pos_kws = st.text_area(
                "pos_kws", label_visibility="collapsed",
                value="\n".join(str(k) for k in pos_current),
                height=300, key="pos_kws_area",
            )

        with col_neg:
            st.subheader("Negative Disqualifiers (hard reject)")
            st.caption("Any match in the title immediately drops the job — no scoring attempted.")
            dq_current = titles.get("negative_disqualifiers", [])
            f_disqualifiers = st.text_area(
                "neg_dq", label_visibility="collapsed",
                value="\n".join(str(k) for k in dq_current),
                height=300, key="neg_dq_area",
            )

        st.subheader("Title Weights (scoring)")
        f_title_weights = _kw_weight_editor(
            titles.get("positive_weights", {}),
            "title_weights_editor",
            note=(
                f"Weight ≥ {fast_track_min} → Fast-Track (base score 50, JD points halved). "
                f"Lower weights add to score incrementally."
            ),
        )

        if st.button("Save Title Keywords", type="primary", key="save_titles"):
            try:
                prefs.setdefault("titles", {}).update({
                    "positive_keywords": [ln.strip() for ln in f_pos_kws.splitlines() if ln.strip()],
                    "negative_disqualifiers": [ln.strip() for ln in f_disqualifiers.splitlines() if ln.strip()],
                    "positive_weights": f_title_weights,
                })
                save_yaml_file(PREFS_YAML, prefs)
                st.success("Saved.")
            except Exception as exc:
                st.error(f"Save failed: {exc}")

    # ── JD Keywords ───────────────────────────────────────────────────────────
    with tab_jd:
        st.info(
            "**How JD scoring works:** Positive keywords in the job description add points "
            "(capped, then halved for Fast-Track titles). Negative keywords subtract points "
            "— a few high-weight negatives can push a job below the keep threshold entirely."
        )

        col_jd_pos, col_jd_neg = st.columns(2)
        with col_jd_pos:
            st.subheader("Positive JD Keywords")
            f_body_pos = _kw_weight_editor(
                keywords.get("body_positive", {}),
                "body_pos_editor",
                note="Points added per match. Domain-specific signals (e.g. 'aladdin', 'IBOR') score highest.",
            )

        with col_jd_neg:
            st.subheader("Negative JD Keywords")
            f_body_neg = _kw_weight_editor(
                keywords.get("body_negative", {}),
                "body_neg_editor",
                note="Points subtracted per match. High values (20+) reliably push bad-fit roles below threshold.",
            )

        if st.button("Save JD Keywords", type="primary", key="save_jd"):
            try:
                prefs.setdefault("keywords", {}).update({
                    "body_positive": f_body_pos,
                    "body_negative": f_body_neg,
                })
                save_yaml_file(PREFS_YAML, prefs)
                st.success("Saved.")
            except Exception as exc:
                st.error(f"Save failed: {exc}")

    # ── Scoring ───────────────────────────────────────────────────────────────
    with tab_scoring:
        st.subheader("Score Threshold")
        f_min_score = st.number_input(
            "Min Score to Keep", value=int(scoring.get("minimum_score_to_keep", 35)),
            min_value=0, max_value=100,
        )

        st.subheader("Keyword Matching")
        kw_cfg = scoring.get("keyword_matching", {})
        sc1, sc2, sc3 = st.columns(3)
        with sc1:
            f_pos_cap = st.number_input(
                "Positive Keyword Cap", value=int(kw_cfg.get("positive_keyword_cap", 40)),
                min_value=1, max_value=100,
            )
        with sc2:
            f_neg_cap = st.number_input(
                "Negative Keyword Cap", value=int(kw_cfg.get("negative_keyword_cap", 45)),
                min_value=1, max_value=100,
            )
        with sc3:
            f_unique_only = st.checkbox(
                "Count Unique Matches Only",
                value=bool(kw_cfg.get("count_unique_matches_only", True)),
            )

        st.subheader("Salary Score Adjustments")
        adj = scoring.get("adjustments", {})
        ac1, ac2 = st.columns(2)
        with ac1:
            f_missing_sal_pen = st.number_input(
                "Missing Salary Penalty", value=int(adj.get("missing_salary_penalty", 6)), min_value=0, max_value=30,
            )
            f_sal_above_bonus = st.number_input(
                "Salary ≥ Target Bonus", value=int(adj.get("salary_at_or_above_target_bonus", 6)), min_value=0, max_value=30,
            )
        with ac2:
            f_sal_floor_bonus = st.number_input(
                "Salary Meets Floor Bonus", value=int(adj.get("salary_meets_floor_bonus", 2)), min_value=0, max_value=30,
            )
            f_sal_below_pen = st.number_input(
                "Salary Below Target Penalty", value=int(adj.get("salary_below_target_penalty", 12)), min_value=0, max_value=30,
            )

        if st.button("Save Scoring", type="primary", key="save_scoring"):
            try:
                prefs.setdefault("scoring", {}).update({
                    "minimum_score_to_keep": f_min_score,
                    "keyword_matching": {
                        "count_unique_matches_only": f_unique_only,
                        "positive_keyword_cap": f_pos_cap,
                        "negative_keyword_cap": f_neg_cap,
                    },
                    "adjustments": {
                        "missing_salary_penalty": f_missing_sal_pen,
                        "salary_at_or_above_target_bonus": f_sal_above_bonus,
                        "salary_meets_floor_bonus": f_sal_floor_bonus,
                        "salary_below_target_penalty": f_sal_below_pen,
                    },
                })
                save_yaml_file(PREFS_YAML, prefs)
                st.success("Saved.")
            except Exception as exc:
                st.error(f"Save failed: {exc}")

    # ── Full YAML editor ───────────────────────────────────────────────────────
    with tab_raw:
        st.caption("Edit the full YAML directly. Indentation must be consistent.")
        f_raw_yaml = st.text_area(
            "preferences YAML", value=raw_yaml, height=650, label_visibility="collapsed",
        )
        if st.button("Save Raw YAML", type="primary", key="save_prefs_raw"):
            try:
                parsed = yaml.safe_load(f_raw_yaml)
                if not isinstance(parsed, dict):
                    st.error("Invalid YAML: top-level must be a mapping.")
                else:
                    PREFS_YAML.write_text(f_raw_yaml, encoding="utf-8")
                    st.success("Preferences saved.")
            except yaml.YAMLError as exc:
                st.error(f"YAML parse error: {exc}")


# ════════════════════════════════════════════════════════════════════════════════
# PAGE: COMPANIES
# ════════════════════════════════════════════════════════════════════════════════

elif page == "Companies":
    st.title("Company Registry")

    data = load_yaml_file(COMPANIES_YAML)
    companies: list = data.get("companies", [])

    # Status summary badges above tabs
    STATUS_COLORS = {"active": "🟢", "new": "🔵", "changed": "🟡", "broken": "🔴"}
    if companies:
        from collections import Counter
        sc = Counter(c.get("status") or "unscanned" for c in companies)
        cols_s = st.columns(len(sc) + 1)
        cols_s[0].metric("Total", len(companies))
        for i, (s, cnt) in enumerate(sc.most_common(), 1):
            icon = STATUS_COLORS.get(s, "⚪")
            cols_s[i].metric(f"{icon} {s.title()}", cnt)
        st.divider()

    tab_list, tab_edit, tab_heal, tab_raw = st.tabs(["Company List", "Add / Edit Company", "Heal ATS", "Raw YAML Editor"])

    # ── Company list (editable table) ─────────────────────────────────────────
    with tab_list:
        if not companies:
            st.info("No companies in registry.")
        else:
            df_co = pd.DataFrame(companies)
            # Normalize mixed-type columns so PyArrow can build the Arrow table.
            # 'industry' is often a list in YAML but must be a plain string for data_editor.
            for col in df_co.columns:
                if df_co[col].apply(lambda v: isinstance(v, list)).any():
                    df_co[col] = df_co[col].apply(
                        lambda v: ", ".join(str(i) for i in v) if isinstance(v, list) else (str(v) if v is not None else "")
                    )
            # Reorder so useful columns come first; ensure status column exists
            if "status" not in df_co.columns:
                df_co["status"] = ""
            front = ["name", "status", "tier", "priority", "adapter", "adapter_key", "domain", "careers_url", "active"]
            rest  = [c for c in df_co.columns if c not in front]
            df_co = df_co[[c for c in front if c in df_co.columns] + rest]

            # Filter controls
            fc1, fc2 = st.columns([3, 1])
            with fc1:
                q = st.text_input("Filter", placeholder="Search by name, adapter, or tier…")
            with fc2:
                status_filter = st.selectbox(
                    "Status", ["All", "active", "new", "changed", "broken", "unscanned"],
                    label_visibility="collapsed",
                )
            if q:
                mask = df_co.apply(
                    lambda row: row.astype(str).str.contains(q, case=False, na=False).any(), axis=1
                )
                df_co = df_co[mask]
            if status_filter != "All":
                if status_filter == "unscanned":
                    df_co = df_co[df_co["status"].fillna("").eq("")]
                else:
                    df_co = df_co[df_co["status"].fillna("").eq(status_filter)]

            st.caption(f"{len(df_co)} {'companies' if len(df_co) != 1 else 'company'}")

            edited_co = st.data_editor(
                df_co,
                column_config={
                    "careers_url": st.column_config.LinkColumn("Careers URL"),
                    "tier": st.column_config.NumberColumn("Tier", min_value=1, max_value=4, step=1, width="small"),
                    "active": st.column_config.CheckboxColumn("Active", width="small"),
                    "adapter": st.column_config.SelectboxColumn("Adapter", options=KNOWN_ADAPTERS),
                    "priority": st.column_config.SelectboxColumn("Priority", options=["high", "medium", "low"]),
                    "status": st.column_config.SelectboxColumn(
                        "Status", options=["", "active", "new", "changed", "broken"], width="small",
                    ),
                },
                hide_index=True,
                use_container_width=True,
                num_rows="dynamic",
                key="co_table_editor",
            )

            if st.button("💾 Save Company List", type="primary"):
                try:
                    shutil.copy2(COMPANIES_YAML, COMPANIES_BAK)
                    records = edited_co.to_dict(orient="records")
                    # Build lookup of original records to detect ATS-relevant changes
                    orig_by_name = {c.get("name"): c for c in companies}
                    ATS_FIELDS = {"adapter", "adapter_key", "careers_url", "domain"}
                    cleaned = []
                    for rec in records:
                        clean_rec = {}
                        for k, v in rec.items():
                            if isinstance(v, float) and math.isnan(v):
                                clean_rec[k] = None
                            elif k == "industry" and isinstance(v, str):
                                clean_rec[k] = [i.strip() for i in v.split(",") if i.strip()] or None
                            else:
                                clean_rec[k] = v
                        # Auto-flag status when ATS fields change via the table editor
                        orig = orig_by_name.get(clean_rec.get("name"))
                        if orig is None:
                            clean_rec.setdefault("status", "new")
                        elif clean_rec.get("status") == "active":
                            ats_changed = any(
                                str(clean_rec.get(f) or "") != str(orig.get(f) or "")
                                for f in ATS_FIELDS
                            )
                            if ats_changed:
                                clean_rec["status"] = "changed"
                        cleaned.append(clean_rec)
                    data["companies"] = cleaned
                    save_yaml_file(COMPANIES_YAML, data)
                    st.success(f"Saved {len(cleaned)} companies. Backup → {COMPANIES_BAK.name}")
                except Exception as exc:
                    st.error(f"Save failed: {exc}")

    # ── Add / Edit form ───────────────────────────────────────────────────────
    with tab_edit:
        company_names = ["— New Company —"] + [c.get("name", "") for c in companies]
        selected_name = st.selectbox("Select company to edit, or choose 'New Company'", company_names)

        existing = {}
        if selected_name != "— New Company —":
            existing = next((c for c in companies if c.get("name") == selected_name), {})

        with st.form("company_form", clear_on_submit=False):
            rc1, rc2 = st.columns(2)
            with rc1:
                f_name     = st.text_input("Name *", value=existing.get("name", ""))
                f_tier     = st.number_input("Tier (1=highest)", value=int(existing.get("tier", 4)), min_value=1, max_value=4)
                f_priority = st.selectbox(
                    "Priority", ["high", "medium", "low"],
                    index=["high", "medium", "low"].index(existing.get("priority", "medium")),
                )
                f_active   = st.checkbox("Active", value=bool(existing.get("active", True)))
            with rc2:
                _current_adapter = existing.get("adapter", "custom_manual")
                _adapter_idx = KNOWN_ADAPTERS.index(_current_adapter) if _current_adapter in KNOWN_ADAPTERS else KNOWN_ADAPTERS.index("custom_manual")
                f_adapter     = st.selectbox("Adapter", KNOWN_ADAPTERS, index=_adapter_idx)
                f_adapter_key = st.text_input("Adapter Key", value=existing.get("adapter_key") or "")
                f_careers_url = st.text_input("Careers URL", value=existing.get("careers_url") or "")
                f_domain      = st.text_input("Domain", value=existing.get("domain") or "")

            _industry_val = existing.get("industry", [])
            if isinstance(_industry_val, list):
                _industry_str = ", ".join(str(i) for i in _industry_val)
            else:
                _industry_str = str(_industry_val)
            f_industry = st.text_input("Industry (comma-separated)", value=_industry_str)
            f_notes    = st.text_area("Notes", value=existing.get("notes") or "", height=80)

            col_save, col_delete = st.columns([3, 1])
            submitted = col_save.form_submit_button("Save Company", type="primary")
            delete_clicked = col_delete.form_submit_button(
                "🗑 Delete", help="Remove this company from the registry",
                disabled=(selected_name == "— New Company —"),
            )

        if submitted:
            if not f_name.strip():
                st.error("Name is required.")
            else:
                try:
                    shutil.copy2(COMPANIES_YAML, COMPANIES_BAK)
                    ATS_FIELDS = {"adapter", "adapter_key", "careers_url", "domain"}
                    rec = {
                        "name":        f_name.strip(),
                        "tier":        int(f_tier),
                        "priority":    f_priority,
                        "adapter":     f_adapter,
                        "active":      f_active,
                        "careers_url": f_careers_url.strip(),
                        "domain":      f_domain.strip(),
                        "industry":    [i.strip() for i in f_industry.split(",") if i.strip()],
                        "notes":       f_notes.strip(),
                    }
                    if f_adapter_key.strip():
                        rec["adapter_key"] = f_adapter_key.strip()

                    idx = next(
                        (i for i, c in enumerate(companies) if c.get("name") == selected_name),
                        None,
                    )
                    if idx is not None:
                        # Flag as changed if any ATS field was edited
                        ats_changed = any(
                            str(rec.get(f) or "") != str(existing.get(f) or "")
                            for f in ATS_FIELDS
                        )
                        rec["status"] = "changed" if ats_changed else existing.get("status", "")
                        companies[idx] = rec
                        st.success(f"Updated '{f_name}'.")
                    else:
                        rec["status"] = "new"
                        companies.append(rec)
                        st.success(f"Added '{f_name}'.")

                    data["companies"] = companies
                    save_yaml_file(COMPANIES_YAML, data)
                except Exception as exc:
                    st.error(f"Save failed: {exc}")

        if delete_clicked and selected_name != "— New Company —":
            try:
                shutil.copy2(COMPANIES_YAML, COMPANIES_BAK)
                data["companies"] = [c for c in companies if c.get("name") != selected_name]
                save_yaml_file(COMPANIES_YAML, data)
                st.success(f"Deleted '{selected_name}'. Backup → {COMPANIES_BAK.name}")
                st.rerun()
            except Exception as exc:
                st.error(f"Delete failed: {exc}")

    # ── Heal ATS ──────────────────────────────────────────────────────────────
    with tab_heal:
        from collections import Counter as _Counter
        _sc = _Counter(c.get("status") or "" for c in companies if c.get("active") is not False)
        _needs = _sc.get("new", 0) + _sc.get("changed", 0) + _sc.get("broken", 0) + _sc.get("", 0)
        _active = _sc.get("active", 0)

        st.markdown(
            "Runs `heal_ats_yaml.py` to probe ATS boards and update company records. "
            "By default only **new**, **changed**, **broken**, and **unscanned** companies are checked — "
            "this is much faster than scanning all 485 entries."
        )

        hc1, hc2, hc3, hc4 = st.columns(4)
        hc1.metric("🔵 New", _sc.get("new", 0))
        hc2.metric("🟡 Changed", _sc.get("changed", 0))
        hc3.metric("🔴 Broken", _sc.get("broken", 0))
        hc4.metric("⚪ Unscanned", _sc.get("", 0))
        st.caption(f"🟢 {_active} already active (will be skipped unless 'Heal All' is checked)")

        st.divider()

        heal_all_flag = st.checkbox(
            "Heal All (re-scan every active company too)",
            help="Unchecked = only new/changed/broken/unscanned. Checked = every active company is re-probed.",
        )

        if _needs == 0 and not heal_all_flag:
            st.success("Nothing to heal — all companies are active.")
        else:
            target_count = len(companies) if heal_all_flag else _needs
            st.info(f"Will scan **{target_count}** {'companies' if target_count != 1 else 'company'}.")

        if st.button("🔧 Run Heal ATS", type="primary", key="run_heal"):
            cmd = [sys.executable, str(BASE_DIR / "heal_ats_yaml.py")]
            if heal_all_flag:
                cmd.append("--all")
            heal_status  = st.empty()
            heal_progress = st.empty()
            log_box      = st.empty()
            try:
                proc = subprocess.Popen(
                    [sys.executable, "-u"] + cmd[1:],
                    stdout=subprocess.PIPE,
                    stderr=subprocess.STDOUT,
                    encoding="utf-8",
                    errors="replace",
                    env={**os.environ, "PYTHONIOENCODING": "utf-8", "PYTHONUNBUFFERED": "1"},
                    cwd=str(BASE_DIR),
                )
                lines_buf: list[str] = []
                done_co = 0
                # heal lines look like "[  3/485] OK  CompanyName  ..."
                _HEAL_LINE = re.compile(r"^\[\s*(\d+)/\s*(\d+)\]")
                heal_status.markdown("**Healing…**")
                for raw in iter(proc.stdout.readline, ""):
                    line = raw.rstrip()
                    lines_buf.append(line)
                    m = _HEAL_LINE.match(line)
                    if m:
                        done_co   = int(m.group(1))
                        total_co  = int(m.group(2))
                        heal_progress.progress(
                            done_co / total_co,
                            text=f"Company {done_co} of {total_co}",
                        )
                    log_box.code("\n".join(lines_buf[-40:]), language=None)
                proc.wait()
                combined = "\n".join(lines_buf)
                if proc.returncode == 0:
                    heal_status.success("Heal complete.")
                else:
                    heal_status.error(f"Healer exited with code {proc.returncode}.")
                log_box.code(combined[-8000:] if len(combined) > 8000 else combined, language=None)
            except Exception as exc:
                st.error(f"Failed to start healer: {exc}")

        # Show last heal report if it exists
        heal_report = RESULTS_DIR / "heal_ats_yaml_report.csv"
        if heal_report.exists():
            st.divider()
            st.subheader("Last Heal Report")
            try:
                df_heal = pd.read_csv(heal_report, dtype=str)
                st.caption(f"{len(df_heal)} companies scanned in last run")
                status_col = "heal_status" if "heal_status" in df_heal.columns else "status"
                if status_col in df_heal.columns:
                    st.dataframe(
                        df_heal,
                        use_container_width=True,
                        hide_index=True,
                        column_config={
                            "new_url": st.column_config.LinkColumn("New URL"),
                            "old_url": st.column_config.LinkColumn("Old URL"),
                        },
                    )
            except Exception as exc:
                st.warning(f"Could not read heal report: {exc}")

    # ── Raw YAML editor ───────────────────────────────────────────────────────
    with tab_raw:
        raw_co = COMPANIES_YAML.read_text(encoding="utf-8") if COMPANIES_YAML.exists() else ""
        st.caption("Direct YAML edit — changes here are independent of the table editor.")
        f_raw_co = st.text_area(
            "companies YAML", value=raw_co, height=650, label_visibility="collapsed",
        )
        if st.button("💾 Save Raw YAML", type="primary", key="save_co_raw"):
            try:
                parsed = yaml.safe_load(f_raw_co)
                if not isinstance(parsed, dict) or "companies" not in parsed:
                    st.error("Invalid YAML: must have a top-level 'companies' key.")
                else:
                    shutil.copy2(COMPANIES_YAML, COMPANIES_BAK)
                    COMPANIES_YAML.write_text(f_raw_co, encoding="utf-8")
                    st.success(f"Saved. Backup → {COMPANIES_BAK.name}")
            except yaml.YAMLError as exc:
                st.error(f"YAML parse error: {exc}")
