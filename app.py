"""app.py — Job Search v6 Dashboard"""

from __future__ import annotations

import json
import math
import shutil
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
import streamlit as st
import yaml

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

# ── Page config ───────────────────────────────────────────────────────────────

st.set_page_config(page_title="Job Search Dashboard", layout="wide", page_icon="💼")

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

page = st.sidebar.radio(
    "Navigate",
    ["Results", "Run Pipeline", "Preferences", "Companies"],
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
    _applied = sum(1 for v in _ov_sidebar.values() if v.get("user_status") == "Applied")
    st.sidebar.metric("Total Jobs", len(_df_sidebar))
    st.sidebar.metric("Applied", _applied)


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

        log_box = st.empty()
        with st.spinner("Pipeline running… this may take several minutes."):
            try:
                proc = subprocess.run(
                    cmd,
                    capture_output=True,
                    text=True,
                    cwd=str(BASE_DIR),
                    timeout=3600,
                )
                _invalidate_data_cache()
                combined = (proc.stdout or "") + (proc.stderr or "")
                if proc.returncode == 0:
                    st.success("Pipeline finished successfully.")
                else:
                    st.error(f"Pipeline exited with code {proc.returncode}.")
                # Show last 8 000 chars so the log box doesn't get enormous
                log_box.code(combined[-8000:] if len(combined) > 8000 else combined, language=None)
            except subprocess.TimeoutExpired:
                st.error("Pipeline timed out after 60 minutes.")
            except Exception as exc:
                st.error(f"Failed to start pipeline: {exc}")


# ════════════════════════════════════════════════════════════════════════════════
# PAGE: RESULTS
# ════════════════════════════════════════════════════════════════════════════════

elif page == "Results":
    st.title("Job Search Results")

    if not XLSX_PATH.exists():
        st.info("No results file found. Run the pipeline first.")
        st.stop()

    df_all = _load_xlsx()
    overrides = load_status_overrides()

    if df_all.empty:
        st.info("Results file is empty. Run the pipeline first.")
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

            st.dataframe(
                df_display[[c for c in rej_vis if c in df_display.columns]],
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

    tab_form, tab_raw = st.tabs(["Key Settings", "Full YAML Editor"])

    # ── Structured form ───────────────────────────────────────────────────────
    with tab_form:
        if not prefs:
            st.warning("No preferences file found at config/job_search_preferences.yaml")
            st.stop()

        comp = prefs.get("search", {}).get("compensation", {})
        search = prefs.get("search", {})
        scoring = prefs.get("scoring", {})
        titles = prefs.get("titles", {})

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

        st.subheader("Scoring")
        f_min_score = st.number_input(
            "Min Score to Keep", value=int(scoring.get("minimum_score_to_keep", 35)),
            min_value=0, max_value=100,
        )

        st.subheader("Title — Negative Disqualifiers")
        dq_current = titles.get("negative_disqualifiers", [])
        f_disqualifiers = st.text_area(
            "One keyword per line — jobs matching any of these are immediately rejected",
            value="\n".join(str(k) for k in dq_current),
            height=220,
        )

        st.subheader("Title — Positive Keywords")
        pos_current = titles.get("positive_keywords", [])
        f_pos_kws = st.text_area(
            "One keyword per line — job title must match at least one",
            value="\n".join(str(k) for k in pos_current),
            height=200,
            key="pos_kws_area",
        )

        if st.button("Save Key Settings", type="primary"):
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
                prefs.setdefault("scoring", {})["minimum_score_to_keep"] = f_min_score
                prefs.setdefault("titles", {})["negative_disqualifiers"] = [
                    ln.strip() for ln in f_disqualifiers.splitlines() if ln.strip()
                ]
                prefs["titles"]["positive_keywords"] = [
                    ln.strip() for ln in f_pos_kws.splitlines() if ln.strip()
                ]
                save_yaml_file(PREFS_YAML, prefs)
                st.success("Preferences saved.")
            except Exception as exc:
                st.error(f"Save failed: {exc}")

    # ── Raw YAML editor ───────────────────────────────────────────────────────
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

    tab_list, tab_edit, tab_raw = st.tabs(["Company List", "Add / Edit Company", "Raw YAML Editor"])

    # ── Company list (editable table) ─────────────────────────────────────────
    with tab_list:
        if not companies:
            st.info("No companies in registry.")
        else:
            df_co = pd.DataFrame(companies)
            # Reorder so useful columns come first
            front = ["name", "tier", "priority", "adapter", "adapter_key", "domain", "careers_url", "active"]
            rest  = [c for c in df_co.columns if c not in front]
            df_co = df_co[[c for c in front if c in df_co.columns] + rest]

            q = st.text_input("Filter", placeholder="Search by name, adapter, or tier…")
            if q:
                mask = df_co.apply(
                    lambda row: row.astype(str).str.contains(q, case=False, na=False).any(), axis=1
                )
                df_co = df_co[mask]

            st.caption(f"{len(df_co)} {'companies' if len(df_co) != 1 else 'company'}")

            edited_co = st.data_editor(
                df_co,
                column_config={
                    "careers_url": st.column_config.LinkColumn("Careers URL"),
                    "tier": st.column_config.NumberColumn("Tier", min_value=1, max_value=4, step=1, width="small"),
                    "active": st.column_config.CheckboxColumn("Active", width="small"),
                    "adapter": st.column_config.SelectboxColumn("Adapter", options=KNOWN_ADAPTERS),
                    "priority": st.column_config.SelectboxColumn("Priority", options=["high", "medium", "low"]),
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
                    # Replace float NaN with None so YAML serialises cleanly
                    import math
                    cleaned = [
                        {k: (None if isinstance(v, float) and math.isnan(v) else v)
                         for k, v in rec.items()}
                        for rec in records
                    ]
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
                        companies[idx] = rec
                        st.success(f"Updated '{f_name}'.")
                    else:
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
