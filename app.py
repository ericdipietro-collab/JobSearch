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
    import ats_db
    from views.tracker_page           import render_tracker
    from views.report_page            import render_activity_report
    from views.training_page          import render_training
    from views.home_page              import render_home
    from views.templates_page         import render_templates
    from views.journal_page           import render_journal
    from views.contacts_page          import render_contacts
    from views.question_bank_page     import render_question_bank
    from views.company_profiles_page  import render_company_profiles
    from views.pipeline_page          import render_pipeline
    from views.analytics_page         import render_analytics
    _ATS_AVAILABLE = True
    _TRACKER_AVAILABLE = True
except ImportError:
    _ATS_AVAILABLE = False
    _TRACKER_AVAILABLE = False

def _safe_render(fn, *args, page_name: str = "", **kwargs):
    """Wrap a render function so a crash shows a friendly error instead of breaking the app.
    Streamlit's internal RerunException / StopException are always re-raised so that
    st.rerun() and st.stop() still work correctly inside rendered pages.
    """
    try:
        fn(*args, **kwargs)
    except Exception as _render_exc:
        # Re-raise Streamlit control-flow exceptions — swallowing them breaks reruns/stops.
        _exc_name = type(_render_exc).__name__
        if "Rerun" in _exc_name or "Stop" in _exc_name:
            raise
        import traceback
        st.error(f"An error occurred rendering this page: **{_render_exc}**")
        with st.expander("Show traceback"):
            st.code(traceback.format_exc(), language="python")
        st.caption(
            "If this keeps happening, check the log at `results/job_search_v6.log` "
            "or restart the app."
        )

# ── Paths ─────────────────────────────────────────────────────────────────────

BASE_DIR = Path(__file__).resolve().parent
CONFIG_DIR = BASE_DIR / "config"
RESULTS_DIR = BASE_DIR / "results"

XLSX_PATH          = RESULTS_DIR / "job_search_v6_results.xlsx"
MANUAL_TARGETS_CSV = RESULTS_DIR / "job_search_v6_manual_targets.csv"
REJECTED_CSV       = RESULTS_DIR / "job_search_v6_rejected.csv"
STATUS_JSON    = RESULTS_DIR / "job_status.json"
STORE_JSON     = RESULTS_DIR / "job_search_store.json"   # cumulative job store
PREFS_YAML         = CONFIG_DIR  / "job_search_preferences.yaml"
EXAMPLE_PREFS_YAML = CONFIG_DIR  / "job_search_preferences.example.yaml"
COMPANIES_YAML     = CONFIG_DIR  / "job_search_companies.yaml"
COMPANIES_BAK  = CONFIG_DIR  / "job_search_companies.yaml.bak"
DB_PATH        = RESULTS_DIR / "jobsearch.db"
TRACKER_CSV    = RESULTS_DIR / "ApplicationTracker.csv"

# Initialise DB on startup (idempotent)
if _ATS_AVAILABLE:
    import ats_db as _db
    _init_conn = _db.get_connection()
    _db.init_db(_init_conn)
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
    "company", "url", "title", "score", "fit_band",
    "location", "salary_range", "age_days", "tier", "is_new",
    "matched_keywords", "decision_reason",
]


# ── Helpers ───────────────────────────────────────────────────────────────────

def _job_key(company: str, title: str, url: str) -> str:
    """Stable dict key for a job; matches the fields written by the scraper."""
    return f"{str(company).strip()}||{str(title).strip()}||{str(url).strip()}"


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


# ── Data I/O ──────────────────────────────────────────────────────────────────

def _load_store() -> dict:
    """Load the cumulative job store from disk."""
    if not STORE_JSON.exists():
        return {}
    try:
        return json.loads(STORE_JSON.read_text(encoding="utf-8"))
    except Exception:
        return {}


def _save_store(store: dict) -> None:
    RESULTS_DIR.mkdir(exist_ok=True)
    STORE_JSON.write_text(
        json.dumps(store, ensure_ascii=False, default=str, indent=None),
        encoding="utf-8",
    )


def _merge_run_into_store(new_df: pd.DataFrame) -> dict:
    """
    Merge the latest pipeline run's kept jobs into the persistent store.
    Scraper fields (score, salary, etc.) are always refreshed.
    Jobs never leave the store — Applied jobs are preserved forever.
    Returns {"added": N, "updated": N, "total": N}.
    """
    store = _load_store()
    overrides = load_status_overrides()
    added = updated = 0

    for _, row in new_df.iterrows():
        # Sanitise NaN so JSON serialisation doesn't choke
        row_dict = {
            k: (None if (isinstance(v, float) and math.isnan(v)) else v)
            for k, v in row.to_dict().items()
        }
        key = _job_key(
            str(row_dict.get("company") or ""),
            str(row_dict.get("title") or ""),
            str(row_dict.get("url") or ""),
        )
        if key in store:
            store[key].update(row_dict)
            updated += 1
        else:
            store[key] = row_dict
            added += 1

    _save_store(store)
    _load_jobs.clear()
    return {"added": added, "updated": updated, "total": len(store)}


@st.cache_data(show_spinner=False)
def _load_jobs() -> pd.DataFrame:
    """
    Load jobs from the unified database.
    Maps database fields to the expected DataFrame structure for the UI.
    """
    if not _TRACKER_AVAILABLE:
        return pd.DataFrame()
        
    conn = ats_db.get_connection()
    try:
        # Load all applications that are in discovery/match status
        query = "SELECT * FROM applications"
        df = pd.read_sql_query(query, conn)
        
        if df.empty:
            return df
            
        # Map DB columns to UI expected names
        column_map = {
            "role": "title",
            "job_url": "url",
            "scraper_key": "_key",
        }
        df = df.rename(columns=column_map)
        
        # Derive age_days from date_discovered
        now = datetime.now()
        def get_age(d):
            if not d: return 0
            try:
                dt = datetime.fromisoformat(d.split('T')[0])
                return (now - dt).days
            except: return 0
        df["age_days"] = df["date_discovered"].apply(get_age)

        # Derive buckets from status and score
        def get_bucket(row):
            status = str(row.get("status", "")).lower()
            if status in ("applied", "interviewing", "screening", "offer", "accepted"):
                return "Applied"
            if status == "rejected":
                return "Rejected"
            if status == "withdrawn":
                return "Filtered Out"
                
            # If still considering, use score to bucket
            score = float(row.get("score", 0))
            if score >= 85: return "APPLY NOW"
            if score >= 70: return "REVIEW TODAY"
            if score >= 50: return "WATCH"
            return "MANUAL REVIEW"

        df["effective_bucket"] = df.apply(get_bucket, axis=1)
        df["action_bucket"] = df["effective_bucket"] # for UI legacy
        df["user_status"] = df["status"].map(lambda s: s.capitalize())
        df["is_new"] = False # Default for now
        
        # Ensure numeric types
        for col in ("score", "user_priority", "salary_low", "salary_high", "tier"):
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce")
                
        return df
    finally:
        conn.close()


@st.cache_data(show_spinner=False)
def _load_rejected_csv() -> pd.DataFrame:
    if not REJECTED_CSV.exists():
        return pd.DataFrame()
    try:
        return pd.read_csv(REJECTED_CSV, dtype=str)
    except Exception:
        return pd.DataFrame()


def _invalidate_data_cache() -> None:
    _load_jobs.clear()
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


def _render_apply_now_cards(bucket_df, overrides: dict, store: dict) -> None:
    """Quick-action cards for the Apply Now bucket: open job link + one-click Apply & Track."""
    if bucket_df.empty:
        return
        
    for _, row in bucket_df.iterrows():
        key     = row["_key"]
        company = str(row.get("company") or "")
        title   = str(row.get("title")   or "")
        score   = row.get("score")
        url     = str(row.get("url") or row.get("canonical_url") or "")

        c1, c2, c3 = st.columns([5, 2, 2])
        score_html = (
            f' <span style="color:#9ca3af;font-size:.8rem">score {score:.0f}</span>'
            if score and str(score) not in ("nan", "None") else ""
        )
        c1.markdown(f"**{company}** — {title}{score_html}", unsafe_allow_html=True)
        if url:
            c2.markdown(
                f'<a href="{url}" target="_blank" style="text-decoration:none">🔗 Open Job</a>',
                unsafe_allow_html=True,
            )
        if c3.button("✅ Apply & Track", key=f"apply_track_{key}"):
            if _TRACKER_AVAILABLE:
                try:
                    conn = ats_db.get_connection()
                    now = _now_iso()
                    today = now[:10]
                    
                    # 1. Update status
                    conn.execute(
                        "UPDATE applications SET status = 'applied', date_applied = ?, updated_at = ? "
                        "WHERE scraper_key = ?",
                        (today, now, key)
                    )
                    
                    # 2. Add event
                    res = conn.execute("SELECT id FROM applications WHERE scraper_key = ?", (key,)).fetchone()
                    if res:
                        ats_db.add_event(conn, res["id"], "applied", now, title=f"Applied to {company}")
                    
                    conn.commit()
                    conn.close()
                    _invalidate_data_cache()
                    st.toast(f"Marked '{company}' as Applied!", icon="✅")
                    st.rerun()
                except Exception as _exc:
                    st.error(f"Failed to update application: {_exc}")
            else:
                st.error("Tracker not available for updates.")
                
        st.markdown('<hr style="margin:2px 0;border-color:#374151">', unsafe_allow_html=True)
    st.divider()


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

APP_VERSION = "1.5"

st.sidebar.title("💼 Job Search")

_nav_options = ["Home", "Job Matches", "My Applications", "Journal", "Contacts", "Company Profiles", "Training", "Question Bank", "Weekly Report", "Templates", "Pipeline", "Analytics", "Run Job Search", "Search Settings", "Target Companies"]
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
_df_sidebar = _load_jobs()
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

# Follow-up reminders counter
if _TRACKER_AVAILABLE:
    try:
        _sb_tc = ats_db.get_connection()
        ats_db.init_db(_sb_tc)
        _fu_overdue  = len(ats_db.follow_up_due(_sb_tc))
        _fu_upcoming = len(ats_db.follow_up_upcoming(_sb_tc, days=3))
        if _fu_overdue or _fu_upcoming:
            st.sidebar.divider()
            if _fu_overdue:
                st.sidebar.error(
                    f"🔔 {_fu_overdue} follow-up{'s' if _fu_overdue != 1 else ''} overdue"
                )
            if _fu_upcoming:
                st.sidebar.info(
                    f"🗓 {_fu_upcoming} follow-up{'s' if _fu_upcoming != 1 else ''} due soon"
                )
    except Exception:
        pass

st.sidebar.divider()
st.sidebar.markdown(
    "[![Buy Me A Coffee](https://img.shields.io/badge/Buy%20Me%20A%20Coffee-Donate-orange?style=flat-square&logo=buy-me-a-coffee)](https://www.buymeacoffee.com/ericdipietro)"
)
st.sidebar.caption(
    f"v{APP_VERSION} · "
    "[Report a bug](https://github.com/ericdipietro-collab/JobSearch/issues/new)"
)


# ════════════════════════════════════════════════════════════════════════════════
# PAGE: RUN PIPELINE
# ════════════════════════════════════════════════════════════════════════════════

if page == "Run Job Search":
    st.title("Run Job Search")
    st.markdown("Runs the job search pipeline and refreshes results when complete.")

    # --- Deep Search availability check ---
    try:
        from deep_search import playwright_adapter as _dsa
        _ds_installed = _dsa.is_available()
    except ImportError:
        _ds_installed = False

    with st.expander("Deep Search (add-on)", expanded=not _ds_installed):
        if _ds_installed:
            st.success(
                "Deep Search is installed. Enable it below to scrape JavaScript-heavy "
                "careers pages (BlackRock, Schwab, S&P Global, and sites with `render_required`)."
            )
        else:
            st.info(
                "**Deep Search is not installed.** 300+ companies blocked by JavaScript or "
                "iframes are skipped by default. Install the add-on to unlock them.\n\n"
                "Run `deep_search/install_deep_search.bat` (Windows) or "
                "`deep_search/install_deep_search.sh` (Mac/Linux), then restart the dashboard."
            )
        use_deep_search = st.checkbox(
            "Enable Deep Search",
            value=False,
            disabled=not _ds_installed,
            help=(
                "Uses a headless Chromium browser to scrape JS-rendered pages. "
                "Slower than standard scraping — allow extra time per run."
            ),
        )

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
        if use_deep_search and _ds_installed:
            cmd.append("--deep-search")
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
                # Merge scored jobs into the persistent store
                if XLSX_PATH.exists():
                    try:
                        _new_df = pd.read_excel(XLSX_PATH, sheet_name="All Jobs", dtype=str)
                        _merge_stats = _merge_run_into_store(_new_df)
                        st.info(
                            f"Results: **{_merge_stats['added']}** new jobs added · "
                            f"**{_merge_stats['updated']}** existing updated · "
                            f"**{_merge_stats['total']}** total in library"
                        )
                    except Exception as _merge_exc:
                        st.warning(f"Could not merge results: {_merge_exc}")
                # Also merge manual targets so they appear in the MANUAL REVIEW tab
                if MANUAL_TARGETS_CSV.exists():
                    try:
                        _mt_df = pd.read_csv(MANUAL_TARGETS_CSV, dtype=str).fillna("")
                        _mt_stats = _merge_run_into_store(_mt_df)
                        st.info(
                            f"Manual targets: **{_mt_stats['added']}** new · "
                            f"**{_mt_stats['updated']}** updated — check the **Manual Review** tab in Job Matches"
                        )
                    except Exception as _mt_exc:
                        st.warning(f"Could not merge manual targets: {_mt_exc}")
                if _ATS_AVAILABLE and XLSX_PATH.exists():
                    try:
                        _sync_conn = ats_db.get_connection()
                        sync_from_excel(_sync_conn, XLSX_PATH, STATUS_JSON)
                    except Exception:
                        pass
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
            if st.button("↻ Sync Excel → Store", help="Merge the last pipeline Excel output into the persistent job library"):
                if XLSX_PATH.exists():
                    with st.spinner("Merging…"):
                        _sd = pd.read_excel(XLSX_PATH, sheet_name="All Jobs", dtype=str)
                        _sr = _merge_run_into_store(_sd)
                        if _ATS_AVAILABLE:
                            sync_from_excel(ats_db.get_connection(), XLSX_PATH, STATUS_JSON)
                    st.success(f"{_sr['added']} added · {_sr['updated']} updated · {_sr['total']} total")
                else:
                    st.warning("No results file found. Run the pipeline first.")
        with _sc2:
            if TRACKER_CSV.exists() and st.button("⬆ Import ApplicationTracker.csv"):
                with st.spinner("Importing…"):
                    _tr = import_tracker_csv(ats_db.get_connection(), TRACKER_CSV)
                st.success(f"{_tr['inserted']} inserted · {_tr['updated']} updated · {_tr['errors']} errors")

    # ── Re-score with current preferences ─────────────────────────────────────
    st.divider()
    st.markdown("### Re-score Pipeline")
    st.caption(
        "Apply your current keyword weights and scoring settings to jobs already in the library, "
        "without re-running the scraper. Use this after updating preferences."
    )
    if st.button("♻ Re-score with Current Preferences", key="rescore_btn"):
        _rescore_script = BASE_DIR / "rescore_pipeline.py"
        if not _rescore_script.exists():
            st.error("rescore_pipeline.py not found — make sure the file is in the repo root.")
        else:
            _rs_log = st.empty()
            _rs_status = st.empty()
            try:
                _rs_proc = subprocess.Popen(
                    [sys.executable, "-u", str(_rescore_script)],
                    stdout=subprocess.PIPE,
                    stderr=subprocess.STDOUT,
                    encoding="utf-8",
                    errors="replace",
                    cwd=str(BASE_DIR),
                )
                _rs_lines: list[str] = []
                for _rs_raw in iter(_rs_proc.stdout.readline, ""):
                    _rs_lines.append(_rs_raw.rstrip())
                    _rs_log.code("\n".join(_rs_lines[-20:]), language=None)
                _rs_proc.wait()
                if _rs_proc.returncode == 0:
                    _invalidate_data_cache()
                    _rs_status.success("Re-score complete. Job Matches now reflects your updated preferences.")
                else:
                    _rs_status.error(f"Re-score exited with code {_rs_proc.returncode}.")
            except Exception as _rs_exc:
                st.error(f"Could not start re-scorer: {_rs_exc}")

    # ── Manual job entry ───────────────────────────────────────────────────────
    st.divider()
    st.markdown("### Manual Job Entry")
    st.caption(
        "Add jobs you found elsewhere (LinkedIn, Indeed, company website, referral) "
        "without using the scraper. They'll be scored against your current preferences "
        "and appear in Job Matches."
    )

    _template_path = CONFIG_DIR / "manual_jobs_template.csv"
    if _template_path.exists():
        st.download_button(
            "⬇ Download CSV Template",
            data=_template_path.read_bytes(),
            file_name="manual_jobs_template.csv",
            mime="text/csv",
            key="dl_manual_template",
        )

    _manual_upload = st.file_uploader(
        "Upload filled-in CSV",
        type=["csv"],
        key="manual_jobs_upload",
        help="Fill in the template and upload it here. Required columns: company, title, location, url.",
    )

    if _manual_upload is not None:
        try:
            _manual_df = pd.read_csv(_manual_upload, dtype=str).fillna("")
            _required_cols = {"company", "title", "location", "url"}
            _missing = _required_cols - set(_manual_df.columns.str.lower())
            if _missing:
                st.error(f"CSV is missing required columns: {', '.join(sorted(_missing))}")
            else:
                _manual_df.columns = _manual_df.columns.str.lower()
                _manual_jobs_added = 0
                _store = _load_store()
                for _, _mrow in _manual_df.iterrows():
                    _mkey = _job_key(
                        str(_mrow.get("company") or ""),
                        str(_mrow.get("title") or ""),
                        str(_mrow.get("url") or ""),
                    )
                    if _mkey not in _store:
                        _store[_mkey] = {
                            "company":             str(_mrow.get("company") or ""),
                            "title":               str(_mrow.get("title") or ""),
                            "location":            str(_mrow.get("location") or ""),
                            "url":                 str(_mrow.get("url") or ""),
                            "salary_range":        str(_mrow.get("salary_range") or ""),
                            "description_excerpt": str(_mrow.get("description") or "")[:600],
                            "tier":                2,
                            "score":               0,
                            "fit_band":            "?",
                            "action_bucket":       "MANUAL REVIEW",
                            "source":              "manual_upload",
                            "is_new":              True,
                            "seen_first_at":       _now_iso(),
                            "seen_last_at":        _now_iso(),
                        }
                        _manual_jobs_added += 1
                _save_store(_store)
                _invalidate_data_cache()
                st.success(
                    f"Added {_manual_jobs_added} job(s) to your library. "
                    "Click **Re-score with Current Preferences** above to score them now."
                )
                if _manual_df.shape[0] - _manual_jobs_added > 0:
                    st.caption(f"{_manual_df.shape[0] - _manual_jobs_added} duplicate(s) skipped (already in library).")
        except Exception as _mex:
            st.error(f"Could not process CSV: {_mex}")

    # ── Scraper run log ────────────────────────────────────────────────────────
    _log_path = RESULTS_DIR / "job_search_v6.log"
    if _log_path.exists():
        st.divider()
        with st.expander("📋 Scraper Run Log", expanded=False):
            _log_lines = _log_path.read_text(encoding="utf-8", errors="replace").splitlines()
            # Show last 300 lines — full log can be thousands of lines
            _log_tail = "\n".join(_log_lines[-300:])
            lc1, lc2 = st.columns([4, 1])
            lc1.caption(f"Showing last {min(300, len(_log_lines))} of {len(_log_lines)} lines · `{_log_path}`")
            if lc2.button("Clear Log", key="clear_scraper_log"):
                _log_path.write_text("", encoding="utf-8")
                st.success("Log cleared.")
                st.rerun()
            st.code(_log_tail, language=None)


# ════════════════════════════════════════════════════════════════════════════════
# PAGE: HOME
# ════════════════════════════════════════════════════════════════════════════════

elif page == "Home":
    st.title("Job Search Dashboard")
    if _TRACKER_AVAILABLE:
        _home_conn = ats_db.get_connection()
        _safe_render(render_home, _home_conn, page_name="Home")
    else:
        st.error("Tracker modules not available — check that ats_db.py is present.")

# ════════════════════════════════════════════════════════════════════════════════
# PAGE: RESULTS
# ════════════════════════════════════════════════════════════════════════════════

elif page == "My Applications":
    st.title("My Applications")
    if _TRACKER_AVAILABLE:
        _tracker_conn = ats_db.get_connection()
        _safe_render(render_tracker, _tracker_conn, page_name="My Applications")
    else:
        st.error("Tracker modules not available — check that ats_db.py and views/tracker_page.py are present.")

elif page == "Training":
    st.title("Skills Training")
    if _TRACKER_AVAILABLE:
        _training_conn = ats_db.get_connection()
        _safe_render(render_training, _training_conn, page_name="Training")
    else:
        st.error("Tracker modules not available — check that ats_db.py is present.")

elif page == "Weekly Report":
    st.title("Weekly Activity Report")
    if _TRACKER_AVAILABLE:
        _report_conn = ats_db.get_connection()
        _safe_render(render_activity_report, _report_conn, page_name="Weekly Report")
    else:
        st.error("Tracker modules not available — check that ats_db.py is present.")

elif page == "Templates":
    st.title("Email Templates")
    if _TRACKER_AVAILABLE:
        _tmpl_conn = ats_db.get_connection()
        _safe_render(render_templates, _tmpl_conn, page_name="Templates")
    else:
        st.error("Tracker modules not available — check that ats_db.py is present.")

elif page == "Journal":
    st.title("Job Search Journal")
    if _TRACKER_AVAILABLE:
        _journal_conn = ats_db.get_connection()
        _safe_render(render_journal, _journal_conn, page_name="Journal")
    else:
        st.error("Tracker modules not available.")

elif page == "Contacts":
    st.title("Networking Contacts")
    if _TRACKER_AVAILABLE:
        _contacts_conn = ats_db.get_connection()
        _safe_render(render_contacts, _contacts_conn, page_name="Contacts")
    else:
        st.error("Tracker modules not available.")

elif page == "Question Bank":
    st.title("Interview Question Bank")
    if _TRACKER_AVAILABLE:
        _qb_conn = ats_db.get_connection()
        _safe_render(render_question_bank, _qb_conn, page_name="Question Bank")
    else:
        st.error("Tracker modules not available.")

elif page == "Company Profiles":
    st.title("Company Research Profiles")
    if _TRACKER_AVAILABLE:
        _cp_conn = ats_db.get_connection()
        _safe_render(render_company_profiles, _cp_conn, page_name="Company Profiles")
    else:
        st.error("Tracker modules not available.")

elif page == "Pipeline":
    if _ATS_AVAILABLE:
        conn = ats_db.get_connection()
        _safe_render(render_pipeline, conn, page_name="Pipeline")
    else:
        st.error("ATS database modules not available. Check that db/ and services/ packages are installed.")

elif page == "Analytics":
    if _ATS_AVAILABLE:
        conn = ats_db.get_connection()
        _safe_render(render_analytics, conn, page_name="Analytics")
    else:
        st.error("ATS database modules not available.")

elif page == "Job Matches":
    st.title("Job Matches")

    if not XLSX_PATH.exists():
        st.info("No results file found. Run the pipeline first.")
        st.stop()

    df_all = _load_jobs()
    overrides = load_status_overrides()

    # Load annotations from DB
    _ann_dict: dict = {}
    if _TRACKER_AVAILABLE:
        _ann_conn = ats_db.get_connection()
        ats_db.init_db(_ann_conn)
        _ann_dict = {r["job_key"]: {"note": r["note"] or "", "tag": r["tag"] or ""}
                     for r in ats_db.get_all_annotations(_ann_conn)}

    if df_all.empty:
        st.info(
            "No jobs yet. Run the pipeline from **Run Pipeline** to fetch results. "
            "If you've already run it and see nothing, use **Clear History** there and run again."
        )
        st.stop()

    df = _apply_overrides(df_all, overrides)

    # ── Summary row ──────────────────────────────────────────────────────────
    n_total   = len(df)
    # Check is_new - handle both boolean and string representations
    if "is_new" in df.columns:
        n_new = int(df["is_new"].apply(lambda x: str(x).lower() == "true").sum())
    else:
        n_new = 0
        
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

            # Apply Now: show per-row quick-action cards above the data editor
            if bucket_name == "APPLY NOW":
                _store_at = _load_store()
                _render_apply_now_cards(bucket_df, overrides, _store_at)

            vis_cols = [c for c in DISPLAY_COLS if c in bucket_df.columns]
            display_df = bucket_df[vis_cols + ["user_status", "_key"]].copy()
            display_df["user_status"] = display_df["user_status"].fillna("")
            # Merge in annotations
            display_df["Note"] = display_df["_key"].map(
                lambda k: _ann_dict.get(k, {}).get("note", "")
            )
            display_df["Tag"] = display_df["_key"].map(
                lambda k: _ann_dict.get(k, {}).get("tag", "")
            )

            st.caption(f"{len(bucket_df)} job(s)")
            _scroll_hint(len(vis_cols) + 3)  # +Move To +Note +Tag

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
                    "Note": st.column_config.TextColumn("Note", width="medium"),
                    "Tag": st.column_config.SelectboxColumn(
                        "Tag",
                        options=["", "skip", "watch", "research needed", "need referral", "applied elsewhere"],
                        required=False,
                        width="small",
                    ),
                },
                disabled=[c for c in vis_cols if c != "user_status"],
                hide_index=True,
                use_container_width=True,
                key=f"editor_{bucket_name}",
            )

            # Detect changed rows — status overrides + annotations
            for i, row in edited_df.iterrows():
                key = display_df.iloc[list(display_df.index).index(i)]["_key"]

                new_status_label = str(row.get("user_status") or "").strip()
                original_status_label = display_df.iloc[list(display_df.index).index(i)]["user_status"]
                
                if new_status_label and new_status_label != original_status_label:
                    # Map UI label back to DB status
                    status_map = {
                        "Applied": "applied",
                        "Rejected": "rejected",
                        "APPLY NOW": "considering",
                        "REVIEW TODAY": "considering",
                        "WATCH": "considering",
                        "MANUAL REVIEW": "considering"
                    }
                    db_status = status_map.get(new_status_label, "considering")
                    
                    try:
                        conn = ats_db.get_connection()
                        now = _now_iso()
                        
                        # 1. Update status
                        update_query = "UPDATE applications SET status = ?, updated_at = ?"
                        params = [db_status, now]
                        
                        if db_status == "applied":
                            update_query += ", date_applied = ?"
                            params.append(now[:10])
                        
                        update_query += " WHERE scraper_key = ?"
                        params.append(key)
                        
                        conn.execute(update_query, tuple(params))
                        
                        # 2. Add event
                        res = conn.execute("SELECT id, company FROM applications WHERE scraper_key = ?", (key,)).fetchone()
                        if res:
                            ats_db.add_event(conn, res["id"], db_status, now, 
                                             title=f"Moved to {new_status_label}")
                        
                        conn.commit()
                        conn.close()
                        _invalidate_data_cache()
                        st.toast(f"Moved to {new_status_label}", icon="✅")
                        st.rerun()
                    except Exception as _exc:
                        st.error(f"Failed to update status: {_exc}")

                # Annotations
                if _TRACKER_AVAILABLE:
                    new_note = str(row.get("Note") or "").strip()
                    new_tag  = str(row.get("Tag")  or "").strip()
                    old_note = _ann_dict.get(key, {}).get("note", "")
                    old_tag  = _ann_dict.get(key, {}).get("tag", "")
                    if new_note != old_note or new_tag != old_tag:
                        ats_db.upsert_annotation(_ann_conn, key,
                                                  note=new_note or None,
                                                  tag=new_tag or None)

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

        # Bridge: newly-Applied jobs → Application Tracker
        if _TRACKER_AVAILABLE:
            try:
                _tc = ats_db.get_connection()
                ats_db.init_db(_tc)
                store = _load_store()
                for _key, _entry in all_changed_overrides.items():
                    if _entry.get("user_status") != "Applied":
                        continue
                    _job = store.get(_key, {})
                    if not _job:
                        continue
                    _company = str(_job.get("company") or "")
                    _role    = str(_job.get("title")   or "")
                    if not _company or not _role:
                        continue
                    # Skip if already tracked (matched on company + role)
                    _exists = _tc.execute(
                        "SELECT id FROM applications WHERE lower(company)=lower(?) AND lower(role)=lower(?)",
                        (_company, _role),
                    ).fetchone()
                    if _exists:
                        continue
                    _sal_low  = _job.get("salary_low")
                    _sal_high = _job.get("salary_high")
                    _sal_rng  = _job.get("salary_range") or (
                        f"${int(_sal_low):,}–${int(_sal_high):,}"
                        if _sal_low and _sal_high else None
                    )
                    _applied_at = (_entry.get("applied_at") or _now_iso())[:10]
                    _app_id = ats_db.add_application(
                        _tc,
                        company      = _company,
                        role         = _role,
                        job_url      = str(_job.get("url") or _job.get("canonical_url") or ""),
                        source       = "scraper",
                        scraper_key  = _key,
                        status       = "applied",
                        salary_low   = int(_sal_low)  if _sal_low  else None,
                        salary_high  = int(_sal_high) if _sal_high else None,
                        salary_range = _sal_rng,
                        jd_summary   = str(_job.get("description_excerpt") or "")[:500] or None,
                        date_applied = _applied_at,
                    )
                    ats_db.add_event(_tc, _app_id, "applied", _applied_at,
                                     title=f"Applied to {_company}")
            except Exception as _exc:
                st.toast(f"Tracker sync warning: {_exc}", icon="⚠️")

        st.toast(f"Saved {len(all_changed_overrides)} status update(s).", icon="✅")
        st.rerun()


# ════════════════════════════════════════════════════════════════════════════════
# PAGE: PREFERENCES
# ════════════════════════════════════════════════════════════════════════════════

elif page == "Search Settings":
    st.title("Search Settings")

    # Auto-initialize from example on first install if the file is missing
    if not PREFS_YAML.exists() and EXAMPLE_PREFS_YAML.exists():
        shutil.copy(EXAMPLE_PREFS_YAML, PREFS_YAML)

    prefs = load_yaml_file(PREFS_YAML)
    raw_yaml = PREFS_YAML.read_text(encoding="utf-8") if PREFS_YAML.exists() else ""

    if not prefs:
        st.error(
            "Preferences file could not be loaded. "
            "If `config/job_search_preferences.yaml` is missing, re-run the installer or "
            "copy `config/job_search_preferences.example.yaml` to that name manually."
        )
        st.stop()

    # Banner when placeholder values are still present
    _pref_placeholders = ["YOUR_ZIP", "YOUR_CITY", "NEARBY_CITY"]
    if any(ph in raw_yaml for ph in _pref_placeholders):
        st.info(
            "👋 **Welcome!** Your preferences were auto-initialized from the template. "
            "Fill in your **salary floor** and **location** details below, then click **Save**.",
            icon=None,
        )

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

    tab_comp, tab_title, tab_jd, tab_scoring, tab_raw, tab_app, tab_backup = st.tabs([
        "Compensation & Location",
        "Title Keywords",
        "JD Keywords",
        "Scoring",
        "Full YAML Editor",
        "App Settings",
        "Backup & Restore",
    ])

    # ── Compensation & Location ───────────────────────────────────────────────
    with tab_comp:
        loc_prefs   = search.get("location_preferences", {})
        remote_cfg  = loc_prefs.get("remote_us", {})
        hybrid_cfg  = loc_prefs.get("local_hybrid", {})

        st.subheader("Compensation")
        fc1, fc2, fc3 = st.columns(3)
        with fc1:
            f_min_sal = st.number_input(
                "Min Salary (USD)", value=int(comp.get("min_salary_usd", 170000)), step=5000,
                help="Jobs with salary below this floor are filtered out (subject to Enforce Min Salary).",
            )
        with fc2:
            f_enforce = st.checkbox("Enforce Min Salary", value=bool(comp.get("enforce_min_salary", True)),
                                    help="Uncheck to score all jobs regardless of salary.")
            f_allow_missing = st.checkbox("Allow Missing Salary", value=bool(comp.get("allow_missing_salary", True)),
                                          help="Include jobs that don't list a salary range.")
        with fc3:
            basis_opts = ["midpoint", "low_end", "high_end"]
            f_basis = st.selectbox(
                "Salary Floor Basis", basis_opts,
                index=basis_opts.index(comp.get("salary_floor_basis", "midpoint")),
                help="Which part of a posted range to compare against your floor.",
            )

        st.divider()
        st.subheader("Location Policy")
        loc_opts        = ["remote_only", "remote_or_hybrid", "in_office", "any"]
        loc_opt_labels  = {
            "remote_only":      "Remote only (hybrid allowed if local + high salary)",
            "remote_or_hybrid": "Remote or Hybrid (must be within commute range)",
            "in_office":        "In-office (remote excluded; hybrid allowed if local)",
            "any":              "Any (no location filtering)",
        }
        current_policy = search.get("location_policy", "remote_only")
        if current_policy not in loc_opts:
            current_policy = "remote_only"
        f_loc_policy = st.selectbox(
            "Location Policy",
            loc_opts,
            index=loc_opts.index(current_policy),
            format_func=lambda x: loc_opt_labels[x],
            help="Controls which work-arrangement types are kept vs. filtered out.",
        )

        # ── Remote settings ───────────────────────────────────────────────────
        st.markdown("**Remote**")
        rc1, rc2 = st.columns(2)
        f_remote_enabled = rc1.checkbox(
            "Include remote jobs", value=bool(remote_cfg.get("enabled", True)),
            help="Uncheck to exclude remote roles entirely.",
        )
        f_remote_bonus = rc2.number_input(
            "Remote score bonus (pts)", value=int(remote_cfg.get("bonus", 14)),
            min_value=0, max_value=30, step=1,
            help="Extra score points added to confirmed-remote roles.",
        )

        # ── Hybrid / Local settings ───────────────────────────────────────────
        st.markdown("**Hybrid & Local (commute range)**")
        hc1, hc2 = st.columns(2)
        f_hybrid_enabled = hc1.checkbox(
            "Include hybrid jobs", value=bool(hybrid_cfg.get("enabled", True)),
            help="Uncheck to exclude hybrid roles entirely.",
        )
        f_hybrid_bonus = hc2.number_input(
            "Hybrid score bonus (pts)", value=int(hybrid_cfg.get("bonus", 4)),
            min_value=0, max_value=30, step=1,
            help="Extra score points added to local-hybrid roles.",
        )

        lc1, lc2, lc3 = st.columns(3)
        f_zip = lc1.text_input(
            "Primary ZIP code", value=str(hybrid_cfg.get("primary_zip", "")),
            placeholder="e.g. 80504",
            help="Your ZIP code — used to calculate whether a hybrid role is within commute range.",
        )
        f_radius = lc2.number_input(
            "Radius (miles)", value=int(hybrid_cfg.get("radius_miles", 30)),
            min_value=5, max_value=150, step=5,
            help="How far you're willing to commute each way.",
        )
        f_hybrid_min_sal = lc3.number_input(
            "Min salary for hybrid ($)", value=int(hybrid_cfg.get("allow_if_salary_at_least_usd", 0)),
            min_value=0, step=5000,
            help="Hybrid roles are only kept if their salary meets this floor (0 = no extra floor).",
        )

        st.caption("Location markers — city/town names that appear in job listings near you (one per line):")
        current_markers = hybrid_cfg.get("markers", [])
        f_markers = st.text_area(
            "markers", label_visibility="collapsed",
            value="\n".join(str(m) for m in current_markers),
            height=160,
            placeholder="firestone\nfrederick\nlongmont\nboulder\n...",
            help="If any of these strings appear in the job location field, the role is treated as commutable.",
        )

        if st.button("Save Compensation & Location", type="primary", key="save_comp"):
            try:
                parsed_markers = [ln.strip() for ln in f_markers.splitlines() if ln.strip()]
                prefs.setdefault("search", {}).setdefault("compensation", {}).update({
                    "min_salary_usd": f_min_sal,
                    "target_salary_usd": f_min_sal,
                    "preferred_remote_min_salary_usd": f_min_sal,
                    "enforce_min_salary": f_enforce,
                    "allow_missing_salary": f_allow_missing,
                    "salary_floor_basis": f_basis,
                })
                prefs["search"]["location_policy"] = f_loc_policy
                prefs["search"].setdefault("location_preferences", {}).update({
                    "remote_us": {
                        "enabled": f_remote_enabled,
                        "bonus":   f_remote_bonus,
                    },
                    "local_hybrid": {
                        "enabled":                    f_hybrid_enabled,
                        "primary_zip":                f_zip.strip() or hybrid_cfg.get("primary_zip", ""),
                        "radius_miles":               f_radius,
                        "markers":                    parsed_markers,
                        "bonus":                      f_hybrid_bonus,
                        "allow_if_salary_at_least_usd": f_hybrid_min_sal,
                    },
                })
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

    with tab_app:
        st.subheader("App Settings", anchor=False)
        st.caption("These settings apply to the dashboard and activity tracking.")

        if _TRACKER_AVAILABLE:
            _settings_conn = ats_db.get_connection()
            ats_db.init_db(_settings_conn)

            current_goal = int(ats_db.get_setting(_settings_conn, "weekly_activity_goal", default="3"))
            new_goal = st.number_input(
                "Weekly activity goal",
                min_value=1,
                max_value=20,
                value=current_goal,
                step=1,
                help=(
                    "The number of job search activities you aim to complete each week. "
                    "Shown as a progress bar on the Home page and in the Weekly Report. "
                    "Set this to match your state's unemployment requirements, or your own personal target."
                ),
            )

            st.markdown("**Theme**")
            _STREAMLIT_CONFIG = BASE_DIR / ".streamlit" / "config.toml"

            def _read_current_theme() -> str:
                if not _STREAMLIT_CONFIG.exists():
                    return "light"
                content = _STREAMLIT_CONFIG.read_text(encoding="utf-8")
                if 'base = "dark"' in content:
                    return "dark"
                return "light"

            current_theme = _read_current_theme()
            theme_choice  = st.radio(
                "Color theme",
                ["light", "dark"],
                index=0 if current_theme == "light" else 1,
                horizontal=True,
            )

            if st.button("Save App Settings", type="primary", key="save_app_settings"):
                ats_db.set_setting(_settings_conn, "weekly_activity_goal", str(new_goal))
                st.success(f"Weekly activity goal set to {new_goal}.")

                # Write theme to .streamlit/config.toml
                _STREAMLIT_CONFIG.parent.mkdir(exist_ok=True)
                _STREAMLIT_CONFIG.write_text(
                    f'[theme]\nbase = "{theme_choice}"\n', encoding="utf-8"
                )
                if theme_choice != current_theme:
                    st.info("Theme change saved. Reload the page (F5) to apply the new theme.")
        else:
            st.error("Tracker modules not available.")

    # ── Backup & Restore ──────────────────────────────────────────────────────
    with tab_backup:
        import zipfile
        import tempfile

        st.subheader("Backup", anchor=False)
        st.caption(
            "Creates a ZIP archive containing your database and config files. "
            "Store it somewhere safe — it captures everything needed to restore your job search."
        )

        _BACKUP_FILES = [
            RESULTS_DIR / "jobsearch.db",
            RESULTS_DIR / "job_applications.db",
            RESULTS_DIR / "job_search_store.json",
            RESULTS_DIR / "job_status.json",
            CONFIG_DIR  / "job_search_preferences.yaml",
            CONFIG_DIR  / "job_search_companies.yaml",
        ]

        if st.button("📦 Create Backup", type="primary", key="create_backup"):
            try:
                _tmp = tempfile.NamedTemporaryFile(suffix=".zip", delete=False)
                _tmp.close()
                _zip_path = Path(_tmp.name)
                with zipfile.ZipFile(_zip_path, "w", compression=zipfile.ZIP_DEFLATED) as zf:
                    for _fp in _BACKUP_FILES:
                        if _fp.exists():
                            zf.write(_fp, arcname=_fp.name)
                _zip_bytes = _zip_path.read_bytes()
                _zip_path.unlink(missing_ok=True)
                from datetime import datetime as _dt
                _fname = f"jobsearch_backup_{_dt.now().strftime('%Y%m%d_%H%M%S')}.zip"
                st.download_button(
                    label=f"⬇ Download {_fname}",
                    data=_zip_bytes,
                    file_name=_fname,
                    mime="application/zip",
                    key="download_backup_zip",
                )
                st.success(f"Backup ready — click the button above to download.")
            except Exception as _bup_exc:
                st.error(f"Backup failed: {_bup_exc}")

        st.divider()
        st.subheader("Restore", anchor=False)
        st.caption(
            "Upload a backup ZIP to restore your files. "
            "Existing files with the same names will be **overwritten**."
        )
        _restore_file = st.file_uploader(
            "Upload backup ZIP", type=["zip"], key="restore_zip_upload",
            label_visibility="collapsed",
        )
        if _restore_file is not None:
            _restore_names = []
            try:
                with zipfile.ZipFile(_restore_file) as _zf:
                    _restore_names = _zf.namelist()
                st.info(f"Archive contains: {', '.join(_restore_names)}")
            except Exception as _re:
                st.error(f"Could not read ZIP: {_re}")
                _restore_names = []

            if _restore_names and st.button("♻ Restore Files", type="primary", key="do_restore"):
                try:
                    _restore_file.seek(0)
                    with zipfile.ZipFile(_restore_file) as _zf:
                        for _name in _restore_names:
                            _dest = None
                            if (RESULTS_DIR / _name).parent == RESULTS_DIR or _name.endswith(".db") or _name.endswith(".json"):
                                _dest = RESULTS_DIR / _name
                            elif _name.endswith(".yaml"):
                                _dest = CONFIG_DIR / _name
                            if _dest:
                                _dest.parent.mkdir(exist_ok=True)
                                _dest.write_bytes(_zf.read(_name))
                    st.success(f"Restored {len(_restore_names)} file(s). Reload the page (F5) to see updated data.")
                except Exception as _re2:
                    st.error(f"Restore failed: {_re2}")


# ════════════════════════════════════════════════════════════════════════════════
# PAGE: COMPANIES
# ════════════════════════════════════════════════════════════════════════════════

elif page == "Target Companies":
    st.title("Target Companies")

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

    tab_list, tab_edit, tab_bulk, tab_heal, tab_raw, tab_manual = st.tabs(["Company List", "Add / Edit Company", "Bulk URL Fix", "Heal ATS", "Raw YAML Editor", "🔍 Manual Check"])

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
                f_active     = st.checkbox("Active", value=bool(existing.get("active", True)))
                f_heal_skip  = st.checkbox(
                    "Skip Healer (heal_skip)",
                    value=bool(existing.get("heal_skip", False)),
                    help="When checked, the ATS healer will never probe or modify this entry. "
                         "Use for sites that block automated checks (e.g. Cloudflare-protected).",
                )
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
                    if f_heal_skip:
                        rec["heal_skip"] = True
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

        # --- Deep Heal availability ---
        try:
            from heal_ats_yaml import deep_heal_available as _dha
            _deep_heal_ok = _dha()
        except Exception:
            _deep_heal_ok = False

        with st.expander("Deep Heal (add-on)", expanded=not _deep_heal_ok):
            if _deep_heal_ok:
                st.success(
                    "Deep Heal is installed. Enable it below to use Playwright network "
                    "interception for companies the static scanner can't identify "
                    "(NOT_FOUND, FALLBACK, or unresolved custom sites). "
                    "Adds ~10–30 s per unresolved company."
                )
            else:
                st.info(
                    "**Deep Heal is not installed.** The ATS Healer uses only static HTTP "
                    "requests and may misidentify ~20% of companies whose career pages rely "
                    "on JavaScript. Install the add-on to unlock Playwright-based ATS "
                    "fingerprinting via network interception.\n\n"
                    "Run `deep_search/install_deep_search.bat` (Windows) or "
                    "`deep_search/install_deep_search.sh` (Mac/Linux), then restart."
                )
            use_deep_heal = st.checkbox(
                "Enable Deep Heal",
                value=False,
                disabled=not _deep_heal_ok,
                help=(
                    "Uses a headless browser to intercept ATS API calls and detect "
                    "JS-constructed iframes — catches providers invisible to static HTML parsing. "
                    "Only fires for companies that couldn't be resolved by static discovery."
                ),
            )

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
            if use_deep_heal and _deep_heal_ok:
                cmd.append("--deep-heal")
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

    # ── Bulk URL fix ──────────────────────────────────────────────────────────
    with tab_bulk:
        if not companies:
            st.info("No companies in registry.")
        else:
            st.caption(
                "Edit careers URLs and adapters in bulk. "
                "Filter to broken/new companies to focus on what needs fixing. "
                "Click **Save Changes** when done."
            )

            # Filter controls
            bc1, bc2 = st.columns([2, 3])
            with bc1:
                bulk_filter = st.selectbox(
                    "Show",
                    ["Broken / New only", "All companies"],
                    key="bulk_filter",
                )
            with bc2:
                bulk_search = st.text_input(
                    "Search by name", placeholder="Type to filter…", key="bulk_search"
                )

            # Build the editable slice
            bulk_rows = []
            for c in companies:
                status = (c.get("status") or "").lower()
                if bulk_filter == "Broken / New only" and status not in ("broken", "new", "changed", "", None):
                    continue
                name = c.get("name", "")
                if bulk_search and bulk_search.lower() not in name.lower():
                    continue
                bulk_rows.append({
                    "name":         name,
                    "adapter":      c.get("adapter") or "",
                    "careers_url":  c.get("careers_url") or "",
                    "status":       c.get("status") or "",
                    "heal_skip":    bool(c.get("heal_skip", False)),
                })

            if not bulk_rows:
                st.info("No companies match the current filter.")
            else:
                st.caption(f"{len(bulk_rows)} companies shown.")
                bulk_df = pd.DataFrame(bulk_rows)
                edited_bulk = st.data_editor(
                    bulk_df,
                    use_container_width=True,
                    num_rows="fixed",
                    column_config={
                        "name":        st.column_config.TextColumn("Company", disabled=True, width="medium"),
                        "adapter":     st.column_config.SelectboxColumn("Adapter", options=KNOWN_ADAPTERS, width="small"),
                        "careers_url": st.column_config.TextColumn("Careers URL", width="large"),
                        "status":      st.column_config.SelectboxColumn(
                            "Status",
                            options=["active", "broken", "new", "changed"],
                            width="small",
                        ),
                        "heal_skip":   st.column_config.CheckboxColumn("Skip Healer", width="small"),
                    },
                    key="bulk_editor",
                )

                if st.button("💾 Save Changes", type="primary", key="bulk_save"):
                    try:
                        shutil.copy2(COMPANIES_YAML, COMPANIES_BAK)
                        # Build lookup of edits by name
                        edits = {
                            row["name"]: row
                            for row in edited_bulk.to_dict("records")
                        }
                        ATS_FIELDS = {"adapter", "adapter_key", "careers_url", "domain"}
                        changed_count = 0
                        for company in companies:
                            name = company.get("name", "")
                            if name not in edits:
                                continue
                            edit = edits[name]
                            ats_changed = any(
                                str(edit.get(f) or "") != str(company.get(f) or "")
                                for f in ATS_FIELDS if f in edit
                            )
                            updated = False
                            for field in ("adapter", "careers_url", "status"):
                                new_val = edit.get(field, "")
                                if str(company.get(field) or "") != str(new_val):
                                    company[field] = new_val
                                    updated = True
                            if edit.get("heal_skip"):
                                if not company.get("heal_skip"):
                                    company["heal_skip"] = True
                                    updated = True
                            elif "heal_skip" in company:
                                del company["heal_skip"]
                                updated = True
                            if ats_changed and edit.get("status") not in ("broken", "new"):
                                company["status"] = "changed"
                            if updated:
                                changed_count += 1

                        with COMPANIES_YAML.open("w", encoding="utf-8") as f:
                            yaml.safe_dump({"companies": companies}, f, sort_keys=False, allow_unicode=True)
                        st.success(f"Saved {changed_count} updated companies.")
                        st.rerun()
                    except Exception as exc:
                        st.error(f"Save failed: {exc}")

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

    # ── Manual Check ──────────────────────────────────────────────────────────
    with tab_manual:
        st.caption(
            "Companies the scraper couldn't auto-fetch jobs from. "
            "Visit each career page manually and check for open roles."
        )
        if not MANUAL_TARGETS_CSV.exists():
            st.info("No manual targets yet — run the pipeline first.")
        else:
            try:
                _mt = pd.read_csv(MANUAL_TARGETS_CSV, dtype=str).fillna("")
                # Keep only the columns we need and rename for display
                _mt_cols = ["company", "tier", "priority", "url", "company_notes"]
                _mt_disp = _mt[[c for c in _mt_cols if c in _mt.columns]].copy()
                _mt_disp.columns = [c.replace("company_notes", "notes").replace("url", "career_page") for c in _mt_disp.columns]
                # Sort by tier (numeric), then company name
                if "tier" in _mt_disp.columns:
                    _mt_disp["tier"] = pd.to_numeric(_mt_disp["tier"], errors="coerce").fillna(9).astype(int)
                    _mt_disp = _mt_disp.sort_values(["tier", "company"], ignore_index=True)
                # Filter control
                _mt_q = st.text_input("Filter", placeholder="Search company or notes…", key="mt_filter")
                if _mt_q:
                    _mt_mask = _mt_disp.apply(
                        lambda r: r.astype(str).str.contains(_mt_q, case=False, na=False).any(), axis=1
                    )
                    _mt_disp = _mt_disp[_mt_mask]
                st.caption(f"{len(_mt_disp)} companies need manual checks")
                st.dataframe(
                    _mt_disp,
                    use_container_width=True,
                    hide_index=True,
                    column_config={
                        "tier":        st.column_config.NumberColumn("Tier", width="small"),
                        "priority":    st.column_config.TextColumn("Priority", width="small"),
                        "company":     st.column_config.TextColumn("Company", width="medium"),
                        "career_page": st.column_config.LinkColumn("Career Page", width="large"),
                        "notes":       st.column_config.TextColumn("Notes", width="large"),
                    },
                )
            except Exception as _mt_exc:
                st.error(f"Could not load manual targets: {_mt_exc}")
