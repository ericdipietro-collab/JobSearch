"""views/pipeline_page.py — ATS Pipeline Kanban/Table page for the Streamlit dashboard (Unified DB)."""

from __future__ import annotations
import sqlite3
from datetime import datetime
from typing import List
import pandas as pd
import streamlit as st

from jobsearch import ats_db
from jobsearch.services.analytics_service import time_in_stage

STATUS_OPTIONS = [
    "Applied",
    "Screening",
    "Interviewing",
    "Offer",
    "Accepted",
    "Rejected",
    "Withdrawn",
]

STATUS_TO_DB = {
    "Applied": "applied",
    "Screening": "screening",
    "Interviewing": "interviewing",
    "Offer": "offer",
    "Accepted": "accepted",
    "Rejected": "rejected",
    "Withdrawn": "withdrawn",
}

# Active stages for default view
ACTIVE_STAGES = [
    "Applied",
    "Screening",
    "Interviewing",
    "Offer",
]

_SEL_KEY = "pipeline_selected_app_id"

def _init_session() -> None:
    if _SEL_KEY not in st.session_state:
        st.session_state[_SEL_KEY] = None

def _truncate(text: str, max_len: int = 60) -> str:
    return text if len(text) <= max_len else text[: max_len - 1] + "…"

def _build_pipeline_df(conn: sqlite3.Connection, stages: List[str], search: str) -> pd.DataFrame:
    if not stages:
        return pd.DataFrame()

    placeholders = ",".join("?" * len(stages))
    status_params = [STATUS_TO_DB[s] for s in stages]
    
    rows = conn.execute(
        f"""
        SELECT id, company, role AS title, score, fit_band, status AS current_stage, 
               location, salary_text, job_url AS url, user_priority, notes
        FROM applications
        WHERE lower(status) IN ({placeholders})
        ORDER BY score DESC
        """,
        status_params,
    ).fetchall()

    if not rows:
        return pd.DataFrame()

    df = pd.DataFrame([dict(r) for r in rows])

    # Merge days_in_stage from analytics helper
    tis = time_in_stage(conn)[["application_id", "days_in_stage"]]
    df = df.merge(tis, left_on="id", right_on="application_id", how="left").drop(
        columns=["application_id"], errors="ignore"
    )

    # Apply search filter
    if search.strip():
        q = search.strip().lower()
        mask = (
            df["company"].str.lower().str.contains(q, na=False)
            | df["title"].str.lower().str.contains(q, na=False)
            | df["location"].str.lower().str.contains(q, na=False)
        )
        df = df[mask]

    return df

def _stage_metric_row(conn: sqlite3.Connection) -> None:
    rows = conn.execute(
        "SELECT status, COUNT(*) AS n FROM applications GROUP BY status"
    ).fetchall()
    counts = {
        label: next((r["n"] for r in rows if r["status"] == db_status), 0)
        for label, db_status in STATUS_TO_DB.items()
    }

    visible = [s for s in ACTIVE_STAGES if counts.get(s, 0) > 0]
    if not visible:
        visible = ACTIVE_STAGES[:4]

    cols = st.columns(len(visible))
    for col, stage in zip(cols, visible):
        col.metric(stage, counts.get(stage, 0))

def _render_detail_panel(conn: sqlite3.Connection, app_id: int) -> None:
    app = ats_db.get_application(conn, app_id)
    if app is None:
        st.warning("Application not found.")
        return

    st.divider()
    st.subheader(f"{app['company']} — {app['role']}")

    col1, col2, col3, col4 = st.columns(4)
    col1.metric("Score", f"{app['score']:.1f}")
    col2.metric("Fit Level", app['fit_band'] or "—")
    col3.metric("Status", str(app['status']).capitalize())
    col4.metric("Priority", app['user_priority'])

    st.write(f"**Location:** {app['location'] or '—'} | **Salary:** {app['salary_text'] or '—'}")
    if app['job_url']:
        st.markdown(f"[Open job posting]({app['job_url']})")

    # Activities/Events
    events = ats_db.get_events(conn, app_id)
    if events:
        with st.expander("Activity History", expanded=True):
            ev_data = [
                {
                    "Type": e["event_type"],
                    "Date": e["event_date"][:10],
                    "Note": e["notes"],
                }
                for e in events
            ]
            st.dataframe(pd.DataFrame(ev_data), hide_index=True, use_container_width=True)

    # Simple Notes Editor
    st.markdown("**Notes**")
    new_notes = st.text_area("Notes", value=app['notes'] or "", height=120, key=f"notes_{app_id}", label_visibility="collapsed")
    if st.button("Save Notes", key=f"save_notes_{app_id}"):
        conn.execute("UPDATE applications SET notes = ?, updated_at = ? WHERE id = ?", (new_notes, datetime.now().isoformat(), app_id))
        conn.commit()
        st.success("Notes saved.")

def render_pipeline(conn: sqlite3.Connection) -> None:
    _init_session()
    st.title("ATS Pipeline")
    _stage_metric_row(conn)
    st.divider()

    col_filter, col_search = st.columns([3, 2])
    with col_filter:
        selected_stages = st.multiselect("Filter by stage", options=STATUS_OPTIONS, default=ACTIVE_STAGES)
    with col_search:
        search_query = st.text_input("Search", placeholder="Company, title, or location...", label_visibility="collapsed")

    pipeline_df = _build_pipeline_df(conn, selected_stages, search_query)
    if pipeline_df.empty:
        st.info("No applications match the current filters.")
        return

    display_cols = ["score", "company", "url", "title", "current_stage", "location", "days_in_stage"]
    rename_map = {"current_stage": "Stage", "days_in_stage": "Days"}
    display_df = pipeline_df[display_cols].copy().rename(columns=rename_map)
    display_df["title"] = display_df["title"].apply(lambda t: _truncate(str(t), 50))

    event = st.dataframe(
        display_df,
        column_config={
            "score": st.column_config.NumberColumn("Score", format="%.1f", width="small"),
            "url": st.column_config.LinkColumn("URL", width="small"),
        },
        hide_index=True,
        use_container_width=True,
        selection_mode="single-row",
        on_select="rerun",
        key="pipeline_table_new",
    )

    if event and event.selection and event.selection.get("rows"):
        row_idx = event.selection["rows"][0]
        st.session_state[_SEL_KEY] = pipeline_df.iloc[row_idx]["id"]

    sel_id = st.session_state.get(_SEL_KEY)
    if sel_id:
        _render_detail_panel(conn, sel_id)
