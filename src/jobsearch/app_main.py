"""src/jobsearch/app_main.py — Full Feature Dashboard with Structural Reinforcements."""

from __future__ import annotations
import os, re, json, subprocess, sys, io, zipfile
from datetime import datetime, timezone
from pathlib import Path
from xml.etree import ElementTree as ET
import pandas as pd
import streamlit as st
import yaml

from jobsearch.config.settings import settings
from jobsearch import ats_db

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

# ── Constants ──
KNOWN_ADAPTERS = ["greenhouse", "lever", "ashby", "workday", "rippling", "smartrecruiters", "custom_manual", "generic"]
DISPLAY_COLS = [
    "company",
    "url",
    "title",
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
    "contract": "Contract",
    "unknown": "Unknown",
}

def _safe_render(fn, *args, page_name: str = "", **kwargs):
    try: fn(*args, **kwargs)
    except Exception as e:
        if "Rerun" in type(e).__name__ or "Stop" in type(e).__name__: raise
        st.error(f"Error in {page_name}: {e}")
        import traceback
        st.code(traceback.format_exc())

def _invalidate_data_cache(): 
    _load_jobs_df.clear()


def _sidebar_metrics_for_df(df: pd.DataFrame) -> dict[str, int]:
    if df.empty or "status" not in df.columns:
        return {"scraped_leads": 0, "tracked": 0, "active": 0}
    statuses = df["status"].fillna("").astype(str).str.lower()
    return {
        "scraped_leads": int((statuses == "considering").sum()),
        "tracked": int((statuses != "considering").sum()),
        "active": int(statuses.isin(["applied", "screening", "interviewing", "offer"]).sum()),
    }


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

@st.cache_data(show_spinner=False)
def _load_jobs_df() -> pd.DataFrame:
    conn = ats_db.get_connection()
    try:
        rows = ats_db.get_applications(conn)
        df = pd.DataFrame([dict(r) for r in rows])
        if df.empty: return df
        
        df = df.rename(columns={"role": "title", "job_url": "url", "scraper_key": "_key"})
        df = _decorate_role_velocity(df)
        
        now = datetime.now()
        df["age_days"] = df["date_discovered"].apply(
            lambda d: (now - datetime.fromisoformat(str(d).split('T')[0])).days if d and str(d).strip() else 0
        )
        
        def bucket(row):
            s = str(row.get("status", "")).lower()
            if s in ("applied", "screening", "interviewing", "offer", "accepted"): return "Applied"
            if s == "rejected": return "Rejected"
            if s == "withdrawn": return "Filtered Out"
            sc = float(row.get("score", 0) or 0)
            if sc >= 85: return "APPLY NOW"
            if sc >= 70: return "REVIEW TODAY"
            if sc >= 50: return "WATCH"
            return "MANUAL REVIEW"
            
        df["effective_bucket"] = df.apply(bucket, axis=1)
        df["user_status"] = df["status"].map(lambda s: str(s).capitalize() if s else "Considering")
        
        for c in ("score", "fit_stars", "salary_low", "salary_high", "tier"):
            if c in df.columns: df[c] = pd.to_numeric(df[c], errors="coerce")
            
        return df
    finally: conn.close()

@st.cache_data(show_spinner=False)
def _load_rejected_jobs_df() -> pd.DataFrame:
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

def _render_apply_now_cards(df):
    for _, r in df.iterrows():
        key, co, ti, sc, url = r["_key"], str(r.get("company","")), str(r.get("title","")), r.get("score"), str(r.get("url",""))
        c1, c2, c3 = st.columns([5, 2, 2])
        c1.markdown(f"**{co}** — {ti} <small>score {sc:.0f}</small>", unsafe_allow_html=True)
        if url: c2.markdown(f"[🔗 Open Job]({url})")
        if c3.button("✅ Apply & Track", key=f"at_{key}"):
            conn = ats_db.get_connection(); now = datetime.now(timezone.utc).isoformat()
            conn.execute("UPDATE applications SET status='applied', date_applied=?, updated_at=? WHERE scraper_key=?", (now[:10], now, key))
            res = conn.execute("SELECT id FROM applications WHERE scraper_key=?", (key,)).fetchone()
            if res: ats_db.add_event(conn, res["id"], "applied", now, title=f"Applied to {co}")
            conn.commit(); conn.close(); _invalidate_data_cache(); st.rerun()
        st.markdown("<hr style='margin:2px 0;border-color:#374151'>", unsafe_allow_html=True)

def load_yaml(p): return yaml.safe_load(p.read_text(encoding="utf-8")) if p.exists() else {}
def save_yaml(p, d): p.write_text(yaml.safe_dump(d, sort_keys=False, allow_unicode=True), encoding="utf-8")

def _normalize_editor_value(value):
    if isinstance(value, list):
        return " | ".join(str(item) for item in value if item is not None)
    if isinstance(value, dict):
        return json.dumps(value, ensure_ascii=False)
    return value

def _parse_pipe_list(value: str):
    return [item.strip() for item in str(value or "").split("|") if item.strip()]


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
        return f"{name} (primary ATS lane)"
    if name == "job_search_companies_test.yaml":
        return f"{name} (test ATS lane)"
    if name == settings.contract_companies_yaml.name:
        return f"{name} (contractor lane)"
    if name == "job_search_companies_contract_test.yaml":
        return f"{name} (legacy contractor test lane)"
    return name


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
    st.set_page_config(page_title="Job Search", page_icon="💼", layout="wide")
    
    st.sidebar.title("💼 Job Search")
    nav = ["Home", "Job Matches", "My Applications", "Journal", "Contacts", "Company Profiles", "Training", "Question Bank", "Weekly Report", "Templates", "Pipeline", "Analytics", "Run Job Search", "Search Settings", "Target Companies"]
    page = st.sidebar.radio("Navigate", nav, label_visibility="collapsed")
    
    df_all = _load_jobs_df()
    if not df_all.empty:
        sidebar_metrics = _sidebar_metrics_for_df(df_all)
        st.sidebar.metric("Scraped Leads", sidebar_metrics["scraped_leads"])
        st.sidebar.metric("Tracked", sidebar_metrics["tracked"])
        st.sidebar.metric("Active", sidebar_metrics["active"])
    
    if page == "Home":
        conn = ats_db.get_connection(); _safe_render(render_home, conn, page_name="Home")
    
    elif page == "My Applications":
        conn = ats_db.get_connection(); _safe_render(render_tracker, conn, page_name="My Applications")

    elif page == "Job Matches":
        st.title("Job Matches")
        rejected_df = _load_rejected_jobs_df()
        manual_review_lines = _load_manual_review_lines()
        manual_review_items = _parse_manual_review_lines(manual_review_lines)
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
        if df_all.empty and rejected_df.empty and not manual_review_lines:
            st.info("No jobs yet. Run the pipeline.")
            return
        velocity_summary = _role_velocity_summary(df_all)
        vm1, vm2, vm3 = st.columns(3)
        vm1.metric("Stale Roles", velocity_summary["stale"])
        vm2.metric("Reposted Roles", velocity_summary["reposted"])
        vm3.metric("Dormant Roles", velocity_summary["dormant"])
        work_type_series = df_all.get("work_type", pd.Series(["unknown"] * len(df_all), index=df_all.index)).map(_normalize_work_type)
        contractor_count = int(work_type_series.isin({"w2_contract", "1099_contract", "c2c_contract", "contract"}).sum())
        fte_count = int((work_type_series == "fte").sum())
        wt1, wt2, wt3 = st.columns([1, 1, 2])
        wt1.metric("Contract Roles", contractor_count)
        wt2.metric("Full-time Roles", fte_count)
        work_type_filter = wt3.selectbox(
            "Work Type Filter",
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
        ann = {r["job_key"]: dict(r) for r in ats_db.get_all_annotations(ats_db.get_connection())}
        tabs = st.tabs(["🔥 Apply Now", "📋 Review Today", "👀 Watch", "🔍 Manual Review", "🚫 Filtered Out"])
        BUCKETS = [("APPLY NOW", tabs[0], ["Applied", "Rejected"]), ("REVIEW TODAY", tabs[1], ["APPLY NOW", "Applied", "WATCH", "Rejected"]), ("WATCH", tabs[2], ["APPLY NOW", "REVIEW TODAY", "Applied", "Rejected"]), ("MANUAL REVIEW", tabs[3], ["APPLY NOW", "Applied", "Rejected"])]
        
        for name, tab, opts in BUCKETS:
            with tab:
                b_df = df_all[df_all["effective_bucket"] == name].copy()
                b_df = _apply_work_type_filter(b_df, work_type_filter).copy()
                if b_df.empty: st.info(f"No jobs in {name}"); continue
                if name == "APPLY NOW": _render_apply_now_cards(b_df)
                
                # Table Rendering
                for c in DISPLAY_COLS + ["user_status", "_key"]:
                    if c not in b_df.columns: b_df[c] = ""
                disp = b_df[DISPLAY_COLS + ["user_status", "_key"]].copy()
                disp["Note"] = disp["_key"].map(lambda k: ann.get(k, {}).get("note", ""))
                
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
                    },
                    hide_index=True,
                    use_container_width=True,
                    key=f"ed_{name}",
                )
                
                for i, row in edited.iterrows():
                    key = disp.iloc[list(disp.index).index(i)]["_key"]
                    new_s = str(row.get("user_status") or "").strip()
                    if new_s and new_s != disp.iloc[list(disp.index).index(i)]["user_status"]:
                        db_s = {"Applied": "applied", "Rejected": "rejected"}.get(new_s, "considering")
                        conn = ats_db.get_connection(); now = datetime.now(timezone.utc).isoformat()
                        conn.execute("UPDATE applications SET status=?, updated_at=? WHERE scraper_key=?", (db_s, now, key))
                        res = conn.execute("SELECT id FROM applications WHERE scraper_key=?", (key,)).fetchone()
                        if res: ats_db.add_event(conn, res["id"], db_s, now, title=f"Moved to {new_s}")
                        conn.commit(); conn.close(); _invalidate_data_cache(); st.rerun()

        with tabs[3]:
            filtered_manual_review = _apply_work_type_filter(
                pd.DataFrame(manual_review_items),
                work_type_filter,
            ) if manual_review_items else pd.DataFrame()
            if filtered_manual_review.empty and not manual_review_items:
                st.info("No manual-review items recorded for the latest run.")
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
                    f"({settings.manual_review_file.name})"
                )
                if not filtered_unresolved:
                    st.success("All manual-review items have been handled.")
                for item in filtered_unresolved[:50]:
                    with st.container(border=True):
                        st.markdown(
                            f"**{item['company']}**"
                            + (f"  \nAdapter: `{item.get('adapter')}`" if item.get("adapter") else "")
                            + (f"  \nReason: {item.get('note')}" if item.get("note") else "")
                        )
                        if item.get("url"):
                            st.markdown(f"[Open source URL]({item['url']})")
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
                                        notes="Disabled in primary ATS registry from manual review queue.",
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
            filtered_rejected_df = _apply_work_type_filter(rejected_df, work_type_filter).copy()
            if filtered_rejected_df.empty:
                st.info("No filtered-out jobs recorded for the latest pipeline run.")
            else:
                st.caption(f"Filtered out by scoring in latest run: {len(filtered_rejected_df)}")
                show_cols = [c for c in ["company", "title", "score", "fit_band", "work_type", "normalized_compensation_usd", "location", "adapter", "drop_reason", "decision_reason", "url"] if c in filtered_rejected_df.columns]
                if "work_type" in filtered_rejected_df.columns:
                    filtered_rejected_df["work_type"] = filtered_rejected_df["work_type"].map(_work_type_label)
                st.dataframe(
                    filtered_rejected_df[show_cols],
                    column_config={"url": st.column_config.LinkColumn("URL")},
                    hide_index=True,
                    use_container_width=True,
                )

    elif page == "Search Settings":
        st.title("Search Settings")
        prefs = load_yaml(settings.prefs_yaml)
        t1, t2, t3, t4, t5, t6, t7 = st.tabs(["Compensation & Location", "Title Evaluation", "JD Evaluation", "Scoring & Rescue", "Full YAML Editor", "App Settings", "Base Resume"])
        
        with t1:
            s = prefs.setdefault("search", {}); c = s.setdefault("compensation", {})
            geography = s.setdefault("geography", {})
            contractor = s.setdefault("contractor", {})
            local_hybrid = s.setdefault("location_preferences", {}).setdefault("local_hybrid", {})
            remote_us = s.setdefault("location_preferences", {}).setdefault("remote_us", {})
            recency = s.setdefault("recency", {})
            f_min = st.number_input("Global Min", value=int(c.get("min_salary_usd", 165000)))
            f_target = st.number_input("Target Salary", value=int(c.get("target_salary_usd", 165000)))
            f_rem = st.number_input("Remote Min", value=int(c.get("preferred_remote_min_salary_usd", c.get("min_salary_usd", 170000))))
            policy_opts = ["remote_only", "hybrid_only", "remote_or_hybrid"]
            f_pol = st.selectbox("Policy", policy_opts, index=policy_opts.index(s.get("location_policy", "remote_only")))
            f_enforce_salary = st.checkbox("Enforce Min Salary", value=bool(c.get("enforce_min_salary", True)))
            f_allow_missing = st.checkbox("Allow Missing Salary", value=bool(c.get("allow_missing_salary", True)))
            f_salary_basis = st.selectbox("Salary Floor Basis", ["midpoint", "low_end", "high_end"], index=["midpoint", "low_end", "high_end"].index(c.get("salary_floor_basis", "midpoint")) if c.get("salary_floor_basis", "midpoint") in ["midpoint", "low_end", "high_end"] else 0)
            f_neg_buffer = st.number_input("Negotiation Buffer %", min_value=0.0, max_value=1.0, value=float(c.get("negotiation_buffer_pct", 0.05)), step=0.01, format="%.2f")
            f_us_only = st.checkbox("US Only", value=bool(geography.get("us_only", True)))
            f_allow_international_remote = st.checkbox("Allow International Remote", value=bool(geography.get("allow_international_remote", False)))
            f_remote_enabled = st.checkbox("Remote US Enabled", value=bool(remote_us.get("enabled", True)))
            f_remote_bonus = st.number_input("Remote Bonus", min_value=0, max_value=50, value=int(remote_us.get("bonus", 14)))
            f_hybrid_enabled = st.checkbox("Local Hybrid Enabled", value=bool(local_hybrid.get("enabled", True)))
            f_zip = st.text_input("Primary ZIP", value=str(local_hybrid.get("primary_zip", "80504")))
            f_radius = st.number_input("Hybrid Radius Miles", min_value=1, max_value=250, value=int(local_hybrid.get("radius_miles", 30)))
            f_hybrid_bonus = st.number_input("Hybrid Bonus", min_value=0, max_value=50, value=int(local_hybrid.get("bonus", 4)))
            f_hybrid_salary = st.number_input("Hybrid Allow If Salary At Least", min_value=0, value=int(local_hybrid.get("allow_if_salary_at_least_usd", 170000)))
            f_markers = st.text_area("Hybrid Location Markers", value="\n".join(local_hybrid.get("markers", [])))
            f_recency_enabled = st.checkbox("Enforce Job Age", value=bool(recency.get("enforce_job_age", True)))
            f_max_age = st.number_input("Max Job Age Days", min_value=1, max_value=365, value=int(recency.get("max_job_age_days", 21)))
            st.markdown("#### Contractor Preferences")
            f_include_contract = st.checkbox("Include Contract Roles", value=bool(contractor.get("include_contract_roles", True)))
            f_allow_w2 = st.checkbox("Allow W2 Hourly", value=bool(contractor.get("allow_w2_hourly", True)))
            f_allow_1099 = st.checkbox("Allow 1099 / C2C", value=bool(contractor.get("allow_1099_hourly", True)))
            f_hours = st.number_input("Default Hours / Week", min_value=1.0, max_value=80.0, value=float(contractor.get("default_hours_per_week", 40)), step=1.0)
            f_w2_weeks = st.number_input("Default W2 Weeks / Year", min_value=1.0, max_value=52.0, value=float(contractor.get("default_w2_weeks_per_year", 50)), step=1.0)
            f_1099_weeks = st.number_input("Default 1099 Weeks / Year", min_value=1.0, max_value=52.0, value=float(contractor.get("default_1099_weeks_per_year", 46)), step=1.0)
            f_benefits = st.number_input("1099 Benefits Replacement USD", min_value=0.0, value=float(contractor.get("benefits_replacement_usd", 18000)), step=1000.0)
            f_w2_gap = st.number_input("W2 Hourly Benefits Gap USD", min_value=0.0, value=float(contractor.get("w2_benefits_gap_usd", 6000)), step=500.0)
            f_1099_overhead = st.number_input("1099 Overhead %", min_value=0.0, max_value=0.75, value=float(contractor.get("overhead_1099_pct", 0.18)), step=0.01, format="%.2f")
            if st.button("Save Comp"):
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
                remote_us.update({"enabled": bool(f_remote_enabled), "bonus": int(f_remote_bonus)})
                local_hybrid.update({
                    "enabled": bool(f_hybrid_enabled),
                    "primary_zip": f_zip.strip(),
                    "radius_miles": int(f_radius),
                    "bonus": int(f_hybrid_bonus),
                    "allow_if_salary_at_least_usd": int(f_hybrid_salary),
                    "markers": [line.strip() for line in f_markers.splitlines() if line.strip()],
                })
                recency.update({"enforce_job_age": bool(f_recency_enabled), "max_job_age_days": int(f_max_age)})
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
                save_yaml(settings.prefs_yaml, prefs); st.success("Saved.")

            with st.expander("Compensation Calculator", expanded=False):
                calc_type = st.selectbox("Comp Type", ["salary", "w2_hourly", "1099_hourly"])
                calc_amount_label = "Annual Salary USD" if calc_type == "salary" else "Hourly Rate USD"
                calc_amount = st.number_input(calc_amount_label, min_value=0.0, value=float(f_target if calc_type == "salary" else 95.0), step=5.0)
                calc_hours = st.number_input(
                    "Calculator Hours / Week",
                    min_value=1.0,
                    max_value=80.0,
                    value=float(f_hours),
                    step=1.0,
                    disabled=(calc_type == "salary"),
                )
                calc_weeks_default = f_w2_weeks if calc_type == "w2_hourly" else f_1099_weeks
                calc_weeks = st.number_input(
                    "Calculator Weeks / Year",
                    min_value=1.0,
                    max_value=52.0,
                    value=float(52.0 if calc_type == "salary" else calc_weeks_default),
                    step=1.0,
                    disabled=(calc_type == "salary"),
                )
                calc_preview = _annualized_compensation_preview(
                    calc_type,
                    calc_amount,
                    calc_hours,
                    calc_weeks,
                    contractor,
                )
                breakeven_w2 = (float(f_target) + float(f_w2_gap)) / max(float(f_hours) * float(f_w2_weeks), 1.0)
                breakeven_1099 = (float(f_target) + float(f_benefits)) / max((1.0 - float(f_1099_overhead)) * float(f_hours) * float(f_1099_weeks), 1.0)
                c_calc1, c_calc2, c_calc3 = st.columns(3)
                c_calc1.metric("Gross Annualized", f"${calc_preview['gross_annual_usd']:,.0f}")
                c_calc2.metric("Normalized Equivalent", f"${calc_preview['normalized_compensation_usd']:,.0f}")
                c_calc3.metric("Target Salary", f"${float(f_target):,.0f}")
                st.caption(
                    f"Break-even hourly vs target salary: W2 ${breakeven_w2:,.2f}/hr | "
                    f"1099 ${breakeven_1099:,.2f}/hr"
                )
        
        with t2:
            t = prefs.setdefault("titles", {})
            constraints = t.setdefault("constraints", {})
            f_keywords = st.text_area("Positive Keywords", value="\n".join(t.get("positive_keywords", [])))
            f_pos = st.text_area("Weights (phrase: weight)", value="\n".join([f"{k}: {v}" for k,v in t.get("positive_weights", {}).items()]))
            f_require_positive = st.checkbox("Require One Positive Keyword", value=bool(t.get("require_one_positive_keyword", True)))
            f_fast_track = st.number_input("Fast Track Base Score", min_value=0, max_value=100, value=int(t.get("fast_track_base_score", 50)))
            f_fast_track_weight = st.number_input("Fast Track Min Weight", min_value=0, max_value=20, value=int(t.get("fast_track_min_weight", 8)))
            f_mods = st.text_area("Product Manager Allowed Modifiers", value="\n".join(constraints.get("product_manager_allowed_modifiers", [])))
            f_arch_mods = st.text_area("Architect Allowed Modifiers", value="\n".join(constraints.get("architect_allowed_modifiers", [])))
            f_ba_mods = st.text_area("Business Analyst Allowed Modifiers", value="\n".join(constraints.get("business_analyst_allowed_modifiers", [])))
            f_consult_mods = st.text_area("Consultant Allowed Modifiers", value="\n".join(constraints.get("consultant_allowed_modifiers", [])))
            f_neg = st.text_area("Hard Disqualifiers", value="\n".join(t.get("negative_disqualifiers", [])))
            if st.button("Save Titles"):
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
                save_yaml(settings.prefs_yaml, prefs); st.success("Saved.")

        with t3:
            k = prefs.setdefault("keywords", {})
            scoring = prefs.setdefault("scoring", {})
            matching = scoring.setdefault("keyword_matching", {})
            f_bp = st.text_area("Body Positive", value="\n".join([f"{ki}: {vi}" for ki,vi in k.get("body_positive", {}).items()]))
            f_bn = st.text_area("Body Negative", value="\n".join([f"{ki}: {vi}" for ki,vi in k.get("body_negative", {}).items()]))
            f_unique = st.checkbox("Count Unique Matches Only", value=bool(matching.get("count_unique_matches_only", True)))
            f_pos_cap = st.number_input("Positive Keyword Cap", min_value=0, max_value=200, value=int(matching.get("positive_keyword_cap", 60)))
            f_neg_cap = st.number_input("Negative Keyword Cap", min_value=0, max_value=200, value=int(matching.get("negative_keyword_cap", 45)))
            if st.button("Save Keywords"):
                k["body_positive"] = {l.split(":")[0].strip(): int(l.split(":")[1]) for l in f_bp.splitlines() if ":" in l}
                k["body_negative"] = {l.split(":")[0].strip(): int(l.split(":")[1]) for l in f_bn.splitlines() if ":" in l}
                matching["count_unique_matches_only"] = bool(f_unique)
                matching["positive_keyword_cap"] = int(f_pos_cap)
                matching["negative_keyword_cap"] = int(f_neg_cap)
                save_yaml(settings.prefs_yaml, prefs); st.success("Saved.")

        with t4:
            s_cfg = prefs.setdefault("scoring", {})
            rescue = prefs.setdefault("policy", {}).setdefault("title_rescue", {})
            adjustments = s_cfg.setdefault("adjustments", {})
            apply_now = s_cfg.setdefault("apply_now", {})
            f_min_keep = st.number_input("Minimum Score To Keep", min_value=0, max_value=100, value=int(s_cfg.get("minimum_score_to_keep", 35)))
            f_missing_salary = st.number_input("Missing Salary Penalty", min_value=0, max_value=50, value=int(adjustments.get("missing_salary_penalty", 6)))
            f_salary_target_bonus = st.number_input("Salary At/Above Target Bonus", min_value=0, max_value=50, value=int(adjustments.get("salary_at_or_above_target_bonus", 6)))
            f_salary_floor_bonus = st.number_input("Salary Meets Floor Bonus", min_value=0, max_value=50, value=int(adjustments.get("salary_meets_floor_bonus", 2)))
            f_salary_below_penalty = st.number_input("Salary Below Target Penalty", min_value=0, max_value=50, value=int(adjustments.get("salary_below_target_penalty", 12)))
            f_require_strong_title = st.checkbox("Apply Now Requires Strong Title", value=bool(apply_now.get("require_strong_title", True)))
            f_min_role_alignment = st.number_input("Apply Now Min Role Alignment", min_value=0.0, max_value=20.0, value=float(apply_now.get("min_role_alignment", 6.0)), step=0.5)
            f_direct_title_markers = st.text_area("Apply Now Direct Title Markers", value="\n".join(apply_now.get("direct_title_markers", [])))
            f_adj_bonus = st.number_input("Adjacent Title Bonus", min_value=0, max_value=50, value=int(rescue.get("adjacent_title_bonus", 8)))
            f_adj_domain_bonus = st.number_input("Adjacent Strong Domain Bonus", min_value=0, max_value=50, value=int(rescue.get("adjacent_title_strong_domain_bonus", 6)))
            f_adj_min = st.number_input("Adjacent Title Min Score To Keep", min_value=0, max_value=100, value=int(rescue.get("adjacent_title_min_score_to_keep", 26)))
            f_strong_body_markers = st.text_area("Strong Body Domain Markers", value="\n".join(rescue.get("strong_body_domain_markers", [])))
            f_adj_markers = st.text_area("Adjacent Title Markers", value="\n".join(rescue.get("adjacent_title_markers", [])))
            f_analyst_markers = st.text_area("Analyst Variant Markers", value="\n".join(rescue.get("analyst_variant_markers", [])))
            if st.button("Save Scoring"):
                s_cfg["minimum_score_to_keep"] = int(f_min_keep)
                adjustments["missing_salary_penalty"] = int(f_missing_salary)
                adjustments["salary_at_or_above_target_bonus"] = int(f_salary_target_bonus)
                adjustments["salary_meets_floor_bonus"] = int(f_salary_floor_bonus)
                adjustments["salary_below_target_penalty"] = int(f_salary_below_penalty)
                apply_now["require_strong_title"] = bool(f_require_strong_title)
                apply_now["min_role_alignment"] = float(f_min_role_alignment)
                apply_now["direct_title_markers"] = [line.strip() for line in f_direct_title_markers.splitlines() if line.strip()]
                rescue["adjacent_title_bonus"] = int(f_adj_bonus)
                rescue["adjacent_title_strong_domain_bonus"] = int(f_adj_domain_bonus)
                rescue["adjacent_title_min_score_to_keep"] = int(f_adj_min)
                rescue["strong_body_domain_markers"] = [line.strip() for line in f_strong_body_markers.splitlines() if line.strip()]
                rescue["adjacent_title_markers"] = [line.strip() for line in f_adj_markers.splitlines() if line.strip()]
                rescue["analyst_variant_markers"] = [line.strip() for line in f_analyst_markers.splitlines() if line.strip()]
                save_yaml(settings.prefs_yaml, prefs); st.success("Saved.")

        with t5:
            raw = settings.prefs_yaml.read_text(encoding="utf-8") if settings.prefs_yaml.exists() else ""
            new_raw = st.text_area("YAML", value=raw, height=600)
            if st.button("Save YAML"): settings.prefs_yaml.write_text(new_raw, encoding="utf-8"); st.success("Saved.")

        with t6:
            conn = ats_db.get_connection()
            try:
                weekly_goal = int(ats_db.get_setting(conn, "weekly_activity_goal", default="3") or "3")
                gmail_address = ats_db.get_setting(conn, "gmail_address", default=settings.gmail_address)
                gmail_app_password = ats_db.get_setting(conn, "gmail_app_password", default=settings.gmail_app_password)
                gmail_imap_host = ats_db.get_setting(conn, "gmail_imap_host", default=settings.gmail_imap_host)

                st.markdown("#### Dashboard Settings")
                app_weekly_goal = st.number_input("Weekly Activity Goal", min_value=1, max_value=50, value=weekly_goal, step=1)

                st.markdown("#### Gmail Sync")
                st.caption("Stored locally in the app settings table. Gmail app passwords are recommended over your normal account password.")
                with st.expander("How to set up a Gmail App Password", expanded=False):
                    st.markdown(
                        "\n".join(
                            [
                                "1. Turn on Google 2-Step Verification for the account you want to sync.",
                                "2. Open `https://myaccount.google.com/apppasswords` while signed into that account.",
                                "3. Create a new App Password with a label like `JobSearch Dashboard`.",
                                "4. Paste the generated 16-character password into `Gmail App Password` below.",
                                "5. Save settings, then use `Sync Gmail Inbox` from `My Applications`.",
                            ]
                        )
                    )
                    st.info(
                        "Use a Google App Password, not your regular Gmail password. "
                        "If `App passwords` is missing, the account may be managed by an organization "
                        "or protected by a policy that disables IMAP app passwords."
                    )
                app_gmail_address = st.text_input("Gmail Address", value=gmail_address)
                app_gmail_password = st.text_input("Gmail App Password", value=gmail_app_password, type="password")
                app_gmail_host = st.text_input("IMAP Host", value=gmail_imap_host or "imap.gmail.com")

                if st.button("Save App Settings"):
                    ats_db.set_setting(conn, "weekly_activity_goal", str(int(app_weekly_goal)))
                    ats_db.set_setting(conn, "gmail_address", app_gmail_address.strip())
                    ats_db.set_setting(conn, "gmail_app_password", app_gmail_password.strip())
                    ats_db.set_setting(conn, "gmail_imap_host", app_gmail_host.strip() or "imap.gmail.com")
                    st.success("App settings saved.")
            finally:
                conn.close()

        with t7:
            conn = ats_db.get_connection()
            try:
                resume_name = ats_db.get_setting(conn, "base_resume_name", default="Master Resume")
                resume_source_url = ats_db.get_setting(conn, "base_resume_source_url", default="")
                resume_text = ats_db.get_setting(conn, "base_resume_text", default="")
                resume_notes = ats_db.get_setting(conn, "base_resume_notes", default="")
                keyword_focus = ats_db.get_setting(conn, "base_resume_keyword_focus", default="")
                keyword_ignore = ats_db.get_setting(conn, "base_resume_keyword_ignore", default="")

                st.session_state.setdefault("base_resume_name_input", resume_name)
                st.session_state.setdefault("base_resume_source_input", resume_source_url)
                st.session_state.setdefault("base_resume_notes_input", resume_notes)
                st.session_state.setdefault("base_resume_text_input", resume_text)
                st.session_state.setdefault("base_resume_focus_input", keyword_focus)
                st.session_state.setdefault("base_resume_ignore_input", keyword_ignore)

                st.caption(
                    "Store the canonical resume text here, then use it as the source-of-truth for tailored resumes "
                    "and keyword-gap analysis."
                )
                uploaded_resume = st.file_uploader(
                    "Upload Base Resume (.txt, .docx, .pdf)",
                    type=["txt", "docx", "pdf"],
                    help="The extracted text populates the editable base resume field below. You can review and edit it before saving.",
                )
                if uploaded_resume is not None and st.button("Load Uploaded Resume"):
                    try:
                        extracted_text, source_type = _extract_resume_text(uploaded_resume)
                        if not extracted_text.strip():
                            st.warning("The uploaded file was read, but no extractable text was found.")
                        else:
                            st.session_state["base_resume_text_input"] = extracted_text
                            st.session_state["base_resume_source_input"] = uploaded_resume.name
                            if not str(st.session_state.get("base_resume_name_input", "")).strip():
                                st.session_state["base_resume_name_input"] = Path(uploaded_resume.name).stem
                            st.success(f"Loaded {uploaded_resume.name} ({source_type.upper()}). Review and save when ready.")
                            st.rerun()
                    except Exception as exc:
                        st.error(f"Could not extract resume text: {exc}")
                br1, br2 = st.columns(2)
                base_resume_name = br1.text_input("Resume Name", key="base_resume_name_input")
                base_resume_source = br2.text_input("Source URL", key="base_resume_source_input", placeholder="Optional link to the living master resume")
                base_resume_notes = st.text_area(
                    "Resume Notes",
                    key="base_resume_notes_input",
                    height=80,
                    placeholder="Role focus, intended audience, or editing notes.",
                )
                base_resume_text = st.text_area(
                    "Base Resume Text",
                    key="base_resume_text_input",
                    height=360,
                    placeholder="Paste the full canonical resume text here.",
                )
                kf1, kf2 = st.columns(2)
                base_keyword_focus = kf1.text_area(
                    "Priority Keywords / Phrases",
                    key="base_resume_focus_input",
                    height=120,
                    placeholder="One per line. Optional emphasis list for tailoring.",
                )
                base_keyword_ignore = kf2.text_area(
                    "Ignore Keywords / Phrases",
                    key="base_resume_ignore_input",
                    height=120,
                    placeholder="One per line. Terms to ignore in gap analysis.",
                )

                stats1, stats2 = st.columns(2)
                stats1.metric("Characters", len(base_resume_text or ""))
                stats2.metric("Lines", len([line for line in str(base_resume_text or "").splitlines() if line.strip()]))

                if st.button("Save Base Resume"):
                    ats_db.set_setting(conn, "base_resume_name", base_resume_name.strip() or "Master Resume")
                    ats_db.set_setting(conn, "base_resume_source_url", base_resume_source.strip())
                    ats_db.set_setting(conn, "base_resume_text", base_resume_text)
                    ats_db.set_setting(conn, "base_resume_notes", base_resume_notes.strip())
                    ats_db.set_setting(conn, "base_resume_keyword_focus", base_keyword_focus.strip())
                    ats_db.set_setting(conn, "base_resume_keyword_ignore", base_keyword_ignore.strip())
                    st.success("Base resume saved.")
            finally:
                conn.close()

    elif page == "Target Companies":
        st.title("Target Companies")
        registry_options = {
            "Primary ATS Registry": settings.companies_yaml,
            "Contractor Registry": settings.contract_companies_yaml,
        }
        registry_label = st.radio("Registry", list(registry_options.keys()), horizontal=True)
        registry_path = registry_options[registry_label]
        data = load_yaml(registry_path)
        cos = data.get("companies", [])
        t1, t2, t3, t4 = st.tabs(["List", "Add / Edit", "Heal ATS", "YAML Editor"])
        with t1:
            df_companies = pd.DataFrame(cos)
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
            st.data_editor(df_companies, use_container_width=True, hide_index=True)
        with t2:
            company_names = [company.get("name", "") for company in cos if isinstance(company, dict)]
            mode = st.radio("Mode", ["Add New", "Edit Existing"], horizontal=True)
            selected_name = None
            selected_company = {}
            if mode == "Edit Existing" and company_names:
                selected_name = st.selectbox("Company", company_names)
                selected_company = next((company for company in cos if company.get("name") == selected_name), {})

            c_name = st.text_input("Name", value=selected_company.get("name", ""))
            c_domain = st.text_input("Domain", value=selected_company.get("domain", ""))
            c_careers = st.text_input("Careers URL", value=selected_company.get("careers_url", ""))
            c_adapter = st.selectbox("Adapter", KNOWN_ADAPTERS, index=KNOWN_ADAPTERS.index(selected_company.get("adapter", "generic")) if selected_company.get("adapter", "generic") in KNOWN_ADAPTERS else KNOWN_ADAPTERS.index("generic"))
            c_adapter_key = st.text_input("Adapter Key", value=selected_company.get("adapter_key", ""))
            c_tier = st.number_input("Tier", min_value=1, max_value=4, value=int(selected_company.get("tier", 2)))
            c_priority = st.selectbox("Priority", ["high", "medium", "low"], index=["high", "medium", "low"].index(selected_company.get("priority", "medium")) if selected_company.get("priority", "medium") in ["high", "medium", "low"] else 1)
            c_active = st.checkbox("Active", value=bool(selected_company.get("active", True)))
            c_manual_only = st.checkbox("Manual Only", value=bool(selected_company.get("manual_only", False)))
            status_options = ["active", "broken", "pending", "manual_only"]
            c_status = st.selectbox("Status", status_options, index=status_options.index(selected_company.get("status", "active")) if selected_company.get("status", "active") in status_options else 0)
            c_industry = st.text_input("Industry", value=_normalize_editor_value(selected_company.get("industry", "")))
            c_sub = st.text_input("Sub Industry", value=str(selected_company.get("sub_industry", "")))
            c_notes = st.text_area("Notes", value=str(selected_company.get("notes", "")))

            if st.button("Save Company"):
                new_company = {
                    "name": c_name.strip(),
                    "domain": c_domain.strip(),
                    "careers_url": c_careers.strip(),
                    "adapter": c_adapter,
                    "adapter_key": c_adapter_key.strip(),
                    "tier": int(c_tier),
                    "priority": c_priority,
                    "active": bool(c_active),
                    "manual_only": bool(c_manual_only),
                    "status": "manual_only" if c_manual_only else c_status,
                    "industry": _parse_pipe_list(c_industry) if "|" in c_industry else c_industry.strip(),
                    "sub_industry": c_sub.strip(),
                    "notes": c_notes.strip(),
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
            if registry_path != settings.companies_yaml:
                st.info("ATS healing is only supported for the primary ATS registry. Use Add / Edit or YAML Editor for contractor sources.")
            else:
                h_all = st.checkbox("All", value=True)
                h_deep = st.checkbox("Deep", value=False)
                h_force = st.checkbox("Process Active Too", value=False)
                h_workers = st.number_input("Workers", min_value=1, max_value=20, value=5, step=1)
                h_deep_timeout = st.number_input("Deep Timeout (sec)", min_value=5, max_value=120, value=20, step=5)
                if st.button("🚀 Run Healer"):
                    cmd = [sys.executable, "-m", "jobsearch.cli", "heal"]
                    if h_all: cmd.append("--all")
                    if h_deep: cmd.append("--deep")
                    if h_force: cmd.append("--force")
                    cmd.extend(["--workers", str(int(h_workers))])
                    cmd.extend(["--deep-timeout", str(float(h_deep_timeout))])
                    proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, encoding="utf-8", env={**os.environ, "PYTHONPATH": "src;."})
                    log = st.empty(); lines = []
                    for raw in iter(proc.stdout.readline, ""): lines.append(raw.rstrip()); log.code("\n".join(lines[-20:]))
                    proc.wait(); st.success("Healer complete.")
        with t4:
            raw = registry_path.read_text(encoding="utf-8") if registry_path.exists() else ""
            new_raw = st.text_area("YAML", value=raw, height=600)
            if st.button("Save Companies"): registry_path.write_text(new_raw, encoding="utf-8"); st.success(f"Saved {registry_path.name}.")

    elif page == "Run Job Search":
        st.title("Run Search Pipeline")
        t1, t2, t3 = st.tabs(["Pipeline Run", "Recent Logs", "Manual CSV Entry"])
        with t1:
            r_deep = st.checkbox("Deep Search", value=False)
            r_test = st.checkbox("Use Test Companies", value=False)
            r_contract = st.checkbox("Use Contractor Sources", value=False)
            r_workers = st.number_input("Workers", min_value=1, max_value=20, value=8, step=1)
            pref_options = [path for path in [settings.prefs_yaml, settings.config_dir / "job_search_preferences_test.yaml"] if path.exists()]
            comp_options = [
                path
                for path in [
                    settings.companies_yaml,
                    settings.config_dir / "job_search_companies_test.yaml",
                    settings.contract_companies_yaml,
                    settings.config_dir / "job_search_companies_contract_test.yaml",
                ]
                if path.exists()
            ]
            default_companies = settings.companies_yaml
            if r_contract and settings.contract_companies_yaml in comp_options:
                default_companies = settings.contract_companies_yaml
            elif r_test and (settings.config_dir / "job_search_companies_test.yaml") in comp_options:
                default_companies = settings.config_dir / "job_search_companies_test.yaml"
            default_index = comp_options.index(default_companies) if default_companies in comp_options else 0
            r_prefs = st.selectbox("Preferences File", pref_options, format_func=lambda p: p.name)
            r_companies = st.selectbox("Companies File", comp_options, index=default_index, format_func=_companies_file_label)
            selected_name = Path(r_companies).name
            if selected_name in {settings.contract_companies_yaml.name, "job_search_companies_contract_test.yaml"}:
                st.info("Contractor-only lane enabled: this run uses external contract-oriented sources only.")
            elif r_contract:
                st.info("Combined lane enabled: this run merges the selected ATS company file with the contractor source registry.")

            if st.button("🚀 Start Pipeline", type="primary"):
                cmd = [sys.executable, "-m", "jobsearch.cli", "run", "--workers", str(int(r_workers))]
                if r_deep: cmd.append("--deep-search")
                if r_test: cmd.append("--test-companies")
                if r_contract: cmd.append("--contract-sources")
                if Path(r_prefs) != settings.prefs_yaml: cmd.extend(["--prefs", str(r_prefs)])
                if Path(r_companies) != settings.companies_yaml: cmd.extend(["--companies", str(r_companies)])

                log = st.empty(); lines = []
                proc = subprocess.Popen(
                    cmd,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.STDOUT,
                    encoding="utf-8",
                    env={**os.environ, "PYTHONUNBUFFERED": "1", "PYTHONPATH": "src;."},
                )
                for raw in iter(proc.stdout.readline, ""):
                    lines.append(raw.rstrip())
                    log.code("\n".join(lines[-30:]))
                proc.wait(); _invalidate_data_cache(); st.success("Pipeline complete.")

        with t2:
            log_path = settings.log_file
            if log_path.exists():
                st.caption(f"Showing tail of {log_path.name}")
                st.code("\n".join(log_path.read_text(encoding="utf-8", errors="ignore").splitlines()[-60:]))
            else:
                st.info("No scrape log found yet. Run the pipeline to generate one.")

        with t3:
            manual_csv = settings.results_dir / "job_search_v6_manual_targets.csv"
            csv_value = manual_csv.read_text(encoding="utf-8") if manual_csv.exists() else "company,title,url,notes\n"
            edited_csv = st.text_area("Manual Targets CSV", value=csv_value, height=320)
            if st.button("Save Manual CSV"):
                manual_csv.write_text(edited_csv, encoding="utf-8")
                st.success(f"Saved {manual_csv.name}.")

    else:
        view_map = {"Pipeline": render_pipeline, "Analytics": render_analytics, "Training": render_training, "Journal": render_journal, "Contacts": render_contacts, "Company Profiles": render_company_profiles, "Weekly Report": render_activity_report, "Templates": render_templates, "Question Bank": render_question_bank}
        conn = ats_db.get_connection(); _safe_render(view_map[page], conn, page_name=page)

if __name__ == "__main__": main()
