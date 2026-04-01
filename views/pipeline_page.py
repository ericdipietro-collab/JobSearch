"""pages/pipeline_page.py — ATS Pipeline Kanban/Table page for the Streamlit dashboard."""

from __future__ import annotations

import sqlite3
from typing import List, Optional

import pandas as pd
import streamlit as st

from db.models import Activity
from services.analytics_service import time_in_stage
from services.opportunity_service import (
    get_opportunity,
    list_opportunities,
    update_notes,
    update_priority,
)
from services.pipeline_service import (
    ACTIVE_STAGES,
    STAGES,
    VALID_TRANSITIONS,
    bulk_transition,
    get_stage_history,
    transition_stage,
)

# ── Session state keys ────────────────────────────────────────────────────────

_SEL_KEY = "pipeline_selected_opp_id"
_NOTES_SAVED_KEY = "pipeline_notes_saved"


def _init_session() -> None:
    if _SEL_KEY not in st.session_state:
        st.session_state[_SEL_KEY] = None
    if _NOTES_SAVED_KEY not in st.session_state:
        st.session_state[_NOTES_SAVED_KEY] = False


# ── Helpers ───────────────────────────────────────────────────────────────────

def _truncate(text: str, max_len: int = 60) -> str:
    return text if len(text) <= max_len else text[: max_len - 1] + "…"


def _stage_badge_color(stage: str) -> str:
    """Return a CSS color string for the stage label (used in markdown)."""
    palette = {
        "New": "#aaa",
        "Scored": "#6ca0dc",
        "Shortlisted": "#2196F3",
        "Applied": "#4CAF50",
        "Recruiter Screen": "#FF9800",
        "Hiring Manager": "#FF5722",
        "Panel": "#9C27B0",
        "Final Round": "#E91E63",
        "Offer": "#009688",
        "Rejected": "#F44336",
        "Archived": "#757575",
    }
    return palette.get(stage, "#888")


def _build_pipeline_df(conn: sqlite3.Connection, stages: List[str], search: str) -> pd.DataFrame:
    """
    Pull all opportunities for the requested stages, merge in days_in_stage,
    and apply optional search filter.
    """
    if not stages:
        return pd.DataFrame()

    placeholders = ",".join("?" * len(stages))
    rows = conn.execute(
        f"""
        SELECT id, company, role_title_raw AS title, role_title_normalized,
               score, fit_band, current_stage, location,
               salary_text, url, user_priority, notes
        FROM opportunities
        WHERE current_stage IN ({placeholders})
        ORDER BY score DESC
        """,
        stages,
    ).fetchall()

    if not rows:
        return pd.DataFrame()

    df = pd.DataFrame([dict(r) for r in rows])

    # Merge days_in_stage from analytics helper
    tis = time_in_stage(conn)[["opportunity_id", "days_in_stage"]]
    df = df.merge(tis, left_on="id", right_on="opportunity_id", how="left").drop(
        columns=["opportunity_id"], errors="ignore"
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
    """Render a compact metric row showing counts per active stage."""
    rows = conn.execute(
        "SELECT current_stage, COUNT(*) AS n FROM opportunities GROUP BY current_stage"
    ).fetchall()
    counts = {r["current_stage"]: r["n"] for r in rows}

    # Display only ACTIVE_STAGES that have data (plus any that have 0)
    visible = [s for s in ACTIVE_STAGES if counts.get(s, 0) > 0]
    if not visible:
        visible = ACTIVE_STAGES[:5]  # show first 5 even if empty

    cols = st.columns(len(visible))
    for col, stage in zip(cols, visible):
        col.metric(stage, counts.get(stage, 0))


# ── Activity form ─────────────────────────────────────────────────────────────

def _render_activity_form(conn: sqlite3.Connection, opp_id: str) -> None:
    """Render the 'Log Activity' expander form."""
    with st.expander("Log Activity"):
        with st.form(key=f"activity_form_{opp_id}"):
            a_type = st.selectbox(
                "Type",
                ["screen", "hm", "panel", "final", "offer", "rejection", "note"],
                format_func=lambda x: {
                    "screen": "Recruiter Screen",
                    "hm": "Hiring Manager",
                    "panel": "Panel Interview",
                    "final": "Final Round",
                    "offer": "Offer",
                    "rejection": "Rejection",
                    "note": "Note",
                }.get(x, x),
            )
            col1, col2 = st.columns(2)
            with col1:
                sched_date = st.date_input("Scheduled Date", value=None)
            with col2:
                comp_date = st.date_input("Completed Date", value=None)
            interviewer = st.text_input("Interviewer / Contact")
            outcome = st.text_input("Outcome")
            act_notes = st.text_area("Notes", height=80)
            submitted = st.form_submit_button("Add Activity")

        if submitted:
            conn.execute(
                """
                INSERT INTO activities
                    (opportunity_id, activity_type, scheduled_date, completed_date,
                     outcome, interviewer, notes)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    opp_id,
                    a_type,
                    sched_date.isoformat() if sched_date else None,
                    comp_date.isoformat() if comp_date else None,
                    outcome,
                    interviewer,
                    act_notes,
                ),
            )
            conn.commit()
            st.success("Activity logged.")
            st.rerun()


# ── Opportunity detail panel ──────────────────────────────────────────────────

def _render_detail_panel(conn: sqlite3.Connection, opp_id: str) -> None:
    """Render the detail panel for a selected opportunity."""
    opp = get_opportunity(conn, opp_id)
    if opp is None:
        st.warning("Opportunity not found — it may have been deleted.")
        st.session_state[_SEL_KEY] = None
        return

    st.divider()
    st.subheader(f"{opp.company} — {opp.role_title_raw}")

    # Core metadata
    col1, col2, col3, col4 = st.columns(4)
    col1.metric("Score", f"{opp.score:.1f}")
    col2.metric("Fit Band", opp.fit_band or "—")
    col3.metric("Stage", opp.current_stage)
    col4.metric("Priority", opp.user_priority)

    col_a, col_b, col_c = st.columns(3)
    col_a.write(f"**Location:** {opp.location or '—'}")
    col_b.write(f"**Salary:** {opp.salary_text or '—'}")
    remote_label = "Remote" if opp.is_remote else "On-site/Hybrid"
    col_c.write(f"**Work type:** {remote_label}")

    if opp.url:
        st.markdown(f"[Open job posting]({opp.url})")

    # Description excerpt
    if opp.description_excerpt:
        with st.expander("Description excerpt"):
            st.write(opp.description_excerpt)

    # Stage history
    history = get_stage_history(conn, opp_id)
    if history:
        with st.expander("Stage history", expanded=False):
            hist_data = [
                {
                    "From": h.from_stage or "(created)",
                    "To": h.to_stage,
                    "When": h.timestamp[:16].replace("T", " "),
                    "Note": h.note,
                }
                for h in history
            ]
            st.dataframe(
                pd.DataFrame(hist_data),
                hide_index=True,
                use_container_width=True,
            )

    # Activities
    act_rows = conn.execute(
        """
        SELECT activity_type, scheduled_date, completed_date, outcome, interviewer, notes
        FROM activities WHERE opportunity_id = ? ORDER BY scheduled_date ASC
        """,
        (opp_id,),
    ).fetchall()

    if act_rows:
        with st.expander("Activities", expanded=False):
            act_data = [
                {
                    "Type": r["activity_type"],
                    "Scheduled": r["scheduled_date"] or "",
                    "Completed": r["completed_date"] or "",
                    "Outcome": r["outcome"],
                    "Interviewer": r["interviewer"],
                    "Notes": r["notes"],
                }
                for r in act_rows
            ]
            st.dataframe(pd.DataFrame(act_data), hide_index=True, use_container_width=True)

    # Notes editor
    st.markdown("**Notes**")
    new_notes = st.text_area(
        "Notes",
        value=opp.notes,
        height=120,
        key=f"notes_{opp_id}",
        label_visibility="collapsed",
    )
    col_save, col_prio = st.columns([1, 3])
    with col_save:
        if st.button("Save notes", key=f"save_notes_{opp_id}"):
            update_notes(conn, opp_id, new_notes)
            st.success("Notes saved.")

    with col_prio:
        new_priority = st.slider(
            "Priority (0 = normal, 5 = urgent)",
            min_value=0,
            max_value=5,
            value=opp.user_priority,
            key=f"priority_{opp_id}",
        )
        if new_priority != opp.user_priority:
            if st.button("Update priority", key=f"save_prio_{opp_id}"):
                update_priority(conn, opp_id, new_priority)
                st.success("Priority updated.")
                st.rerun()

    # Manual stage transition
    allowed_next = VALID_TRANSITIONS.get(opp.current_stage, [])
    if allowed_next:
        st.markdown("**Move stage**")
        col_ts, col_tb = st.columns([2, 1])
        with col_ts:
            target_stage = st.selectbox(
                "Move to",
                allowed_next,
                key=f"move_stage_{opp_id}",
                label_visibility="collapsed",
            )
        with col_tb:
            if st.button("Move", key=f"do_move_{opp_id}"):
                ok = transition_stage(conn, opp_id, target_stage, note="Manual via UI")
                if ok:
                    st.success(f"Moved to {target_stage}.")
                    st.rerun()
                else:
                    st.error("Transition not allowed.")

    _render_activity_form(conn, opp_id)


# ── Main page renderer ────────────────────────────────────────────────────────

def render_pipeline(conn: sqlite3.Connection) -> None:
    """Entry point called from app.py."""
    _init_session()

    st.title("ATS Pipeline")

    # Stage count metrics
    _stage_metric_row(conn)
    st.divider()

    # ── Filters ───────────────────────────────────────────────────────────────
    col_filter, col_search = st.columns([3, 2])
    with col_filter:
        selected_stages: List[str] = st.multiselect(
            "Filter by stage",
            options=STAGES,
            default=ACTIVE_STAGES,
            placeholder="Select stages…",
        )
    with col_search:
        search_query = st.text_input(
            "Search",
            placeholder="Company, title, or location…",
            label_visibility="collapsed",
        )

    # ── Build table ───────────────────────────────────────────────────────────
    pipeline_df = _build_pipeline_df(conn, selected_stages, search_query)

    if pipeline_df.empty:
        st.info("No opportunities match the current filters.")
        return

    # Columns to show in the table
    display_cols = [
        c for c in [
            "score", "company", "url", "title", "current_stage", "location",
            "salary_text", "days_in_stage",
        ]
        if c in pipeline_df.columns
    ]

    # Rename for display
    rename_map = {
        "current_stage": "Stage",
        "salary_text": "Salary",
        "days_in_stage": "Days in Stage",
    }
    display_df = pipeline_df[display_cols].copy().rename(columns=rename_map)
    display_df["title"] = display_df["title"].apply(lambda t: _truncate(str(t), 60))

    st.caption(f"{len(display_df)} opportunit{'y' if len(display_df) == 1 else 'ies'}")

    col_config = {
        "score": st.column_config.NumberColumn("Score", format="%.1f", width="small"),
        "company": st.column_config.TextColumn("Company", width="medium"),
        "title": st.column_config.TextColumn("Title", width="large"),
        "Stage": st.column_config.TextColumn("Stage", width="medium"),
        "location": st.column_config.TextColumn("Location", width="medium"),
        "Salary": st.column_config.TextColumn("Salary", width="small"),
        "Days in Stage": st.column_config.NumberColumn("Days in Stage", format="%d", width="small"),
        "url": st.column_config.LinkColumn("URL", width="small"),
    }

    event = st.dataframe(
        display_df,
        column_config=col_config,
        hide_index=True,
        use_container_width=True,
        selection_mode="single-row",
        on_select="rerun",
        key="pipeline_table",
    )

    # Resolve selected row → opp_id
    selected_rows = event.selection.get("rows", []) if event and event.selection else []
    if selected_rows:
        row_idx = selected_rows[0]
        if row_idx < len(pipeline_df):
            st.session_state[_SEL_KEY] = pipeline_df.iloc[row_idx]["id"]
    else:
        # Don't clear selection on redraw — keep the panel open
        pass

    # ── Bulk transition ───────────────────────────────────────────────────────
    st.markdown("**Bulk move selected**")
    col_bulk_s, col_bulk_b, col_bulk_n = st.columns([2, 1, 3])
    with col_bulk_s:
        bulk_target = st.selectbox(
            "Move selected to",
            options=STAGES,
            key="bulk_target_stage",
            label_visibility="collapsed",
        )
    with col_bulk_b:
        if st.button("Apply", key="bulk_apply"):
            if selected_rows:
                ids = [pipeline_df.iloc[r]["id"] for r in selected_rows if r < len(pipeline_df)]
                moved = bulk_transition(conn, ids, bulk_target, note="Bulk move via UI")
                st.success(f"Moved {moved} record(s) to {bulk_target}.")
                st.rerun()
            else:
                st.warning("Select at least one row first.")
    with col_bulk_n:
        st.caption("Select a row in the table, then choose a target stage and click Apply.")

    # ── Detail panel ──────────────────────────────────────────────────────────
    selected_opp_id: Optional[str] = st.session_state.get(_SEL_KEY)
    if selected_opp_id:
        _render_detail_panel(conn, selected_opp_id)
