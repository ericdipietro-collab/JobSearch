"""src/jobsearch/app_main.py — Full Feature Dashboard with Structural Reinforcements."""

from __future__ import annotations
import os, re, json, subprocess, sys
from datetime import datetime, timezone
from pathlib import Path
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
DISPLAY_COLS = ["company", "url", "title", "score", "fit_band", "location", "age_days", "tier", "matched_keywords", "decision_reason"]

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

@st.cache_data(show_spinner=False)
def _load_jobs_df() -> pd.DataFrame:
    conn = ats_db.get_connection()
    try:
        rows = ats_db.get_applications(conn)
        df = pd.DataFrame([dict(r) for r in rows])
        if df.empty: return df
        
        df = df.rename(columns={"role": "title", "job_url": "url", "scraper_key": "_key"})
        
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
        if df_all.empty and rejected_df.empty and not manual_review_lines:
            st.info("No jobs yet. Run the pipeline.")
            return
        ann = {r["job_key"]: dict(r) for r in ats_db.get_all_annotations(ats_db.get_connection())}
        tabs = st.tabs(["🔥 Apply Now", "📋 Review Today", "👀 Watch", "🔍 Manual Review", "🚫 Filtered Out"])
        BUCKETS = [("APPLY NOW", tabs[0], ["Applied", "Rejected"]), ("REVIEW TODAY", tabs[1], ["APPLY NOW", "Applied", "WATCH", "Rejected"]), ("WATCH", tabs[2], ["APPLY NOW", "REVIEW TODAY", "Applied", "Rejected"]), ("MANUAL REVIEW", tabs[3], ["APPLY NOW", "Applied", "Rejected"])]
        
        for name, tab, opts in BUCKETS:
            with tab:
                b_df = df_all[df_all["effective_bucket"] == name].copy()
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
            if manual_review_lines:
                st.caption(f"Blocked or protected sites requiring manual review: {len(manual_review_lines)}")
                st.code("\n".join(manual_review_lines[:50]))
                if len(manual_review_lines) > 50:
                    st.info(f"Showing first 50 of {len(manual_review_lines)} manual-review entries. Full list: {settings.manual_review_file.name}")

        with tabs[4]:
            if rejected_df.empty:
                st.info("No filtered-out jobs recorded for the latest pipeline run.")
            else:
                st.caption(f"Filtered out by scoring in latest run: {len(rejected_df)}")
                show_cols = [c for c in ["company", "title", "score", "fit_band", "location", "adapter", "drop_reason", "decision_reason", "url"] if c in rejected_df.columns]
                st.dataframe(
                    rejected_df[show_cols],
                    column_config={"url": st.column_config.LinkColumn("URL")},
                    hide_index=True,
                    use_container_width=True,
                )

    elif page == "Search Settings":
        st.title("Search Settings")
        prefs = load_yaml(settings.prefs_yaml)
        t1, t2, t3, t4, t5 = st.tabs(["Compensation & Location", "Title Evaluation", "JD Evaluation", "Scoring & Rescue", "Full YAML Editor"])
        
        with t1:
            s = prefs.setdefault("search", {}); c = s.setdefault("compensation", {})
            geography = s.setdefault("geography", {})
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
                s["location_policy"] = f_pol
                save_yaml(settings.prefs_yaml, prefs); st.success("Saved.")
        
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

    elif page == "Target Companies":
        st.title("Target Companies")
        data = load_yaml(settings.companies_yaml); cos = data.get("companies", [])
        t1, t2, t3, t4 = st.tabs(["List", "Add / Edit", "Heal ATS", "YAML Editor"])
        with t1:
            df_companies = pd.DataFrame(cos)
            if not df_companies.empty:
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
            c_status = st.selectbox("Status", ["active", "broken", "pending"], index=["active", "broken", "pending"].index(selected_company.get("status", "active")) if selected_company.get("status", "active") in ["active", "broken", "pending"] else 0)
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
                    "status": c_status,
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
                    save_yaml(settings.companies_yaml, data)
                    st.success("Company saved.")

            if mode == "Edit Existing" and selected_name and st.button("Delete Company"):
                data["companies"] = [company for company in cos if company.get("name") != selected_name]
                save_yaml(settings.companies_yaml, data)
                st.success("Company deleted.")
        with t3:
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
            raw = settings.companies_yaml.read_text(encoding="utf-8") if settings.companies_yaml.exists() else ""
            new_raw = st.text_area("YAML", value=raw, height=600)
            if st.button("Save Companies"): settings.companies_yaml.write_text(new_raw, encoding="utf-8"); st.success("Saved.")

    elif page == "Run Job Search":
        st.title("Run Search Pipeline")
        t1, t2, t3 = st.tabs(["Pipeline Run", "Recent Logs", "Manual CSV Entry"])
        with t1:
            r_deep = st.checkbox("Deep Search", value=False)
            r_test = st.checkbox("Use Test Companies", value=False)
            r_workers = st.number_input("Workers", min_value=1, max_value=20, value=8, step=1)
            pref_options = [path for path in [settings.prefs_yaml, settings.config_dir / "job_search_preferences_test.yaml"] if path.exists()]
            comp_options = [path for path in [settings.companies_yaml, settings.config_dir / "job_search_companies_test.yaml"] if path.exists()]
            r_prefs = st.selectbox("Preferences File", pref_options, format_func=lambda p: p.name)
            r_companies = st.selectbox("Companies File", comp_options, format_func=lambda p: p.name)

            if st.button("🚀 Start Pipeline", type="primary"):
                cmd = [sys.executable, "-m", "jobsearch.cli", "run", "--workers", str(int(r_workers))]
                if r_deep: cmd.append("--deep-search")
                if r_test: cmd.append("--test-companies")
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
