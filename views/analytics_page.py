"""pages/analytics_page.py — Analytics & reporting page for the Streamlit dashboard."""

from __future__ import annotations

import sqlite3

import pandas as pd
import streamlit as st

from services.analytics_service import (
    avg_score_by_stage,
    company_pipeline,
    conversion_rates,
    funnel_counts,
    high_score_not_applied,
    score_vs_outcome,
    time_in_stage,
)

# Days-in-stage threshold for "stuck" highlight
_STUCK_THRESHOLD_DAYS = 7


def _metric_or_dash(value, fmt: str = "{}") -> str:
    """Format a value or return '—' for None/empty."""
    if value is None:
        return "—"
    try:
        return fmt.format(value)
    except Exception:
        return str(value)


# ── Section renderers ─────────────────────────────────────────────────────────

def _render_funnel_overview(conn: sqlite3.Connection) -> None:
    st.subheader("Funnel Overview")

    df = funnel_counts(conn)
    rates = conversion_rates(conn)

    # Conversion rate metrics
    col1, col2, col3, col4 = st.columns(4)
    col1.metric("Discovered", rates["total_discovered"])
    col2.metric(
        "Applied",
        rates["total_applied"],
        delta=f"{rates['applied_rate']}% of discovered",
        delta_color="normal",
    )
    col3.metric(
        "Interviewed",
        rates["total_interviewed"],
        delta=f"{rates['interview_rate']}% of applied",
        delta_color="normal",
    )
    col4.metric(
        "Offers",
        rates["total_offer"],
        delta=f"{rates['offer_rate']}% of interviewed",
        delta_color="normal",
    )

    st.divider()

    if df.empty or df["count"].sum() == 0:
        st.info("No data available yet.")
        return

    # Bar chart of stage counts
    st.bar_chart(
        df.set_index("stage")["count"],
        use_container_width=True,
        color="#2196F3",
    )

    # Raw table toggle
    with st.expander("Raw counts table"):
        st.dataframe(df, hide_index=True, use_container_width=True)


def _render_score_analysis(conn: sqlite3.Connection) -> None:
    st.subheader("Score Analysis")

    df = avg_score_by_stage(conn)
    if df.empty:
        st.info("No score data available.")
        return

    col_table, col_chart = st.columns([1, 2])
    with col_table:
        st.dataframe(
            df,
            hide_index=True,
            use_container_width=True,
            column_config={
                "stage": st.column_config.TextColumn("Stage"),
                "avg_score": st.column_config.NumberColumn("Avg Score", format="%.1f"),
                "count": st.column_config.NumberColumn("Count"),
            },
        )
    with col_chart:
        if not df.empty:
            st.bar_chart(
                df.set_index("stage")["avg_score"],
                use_container_width=True,
                color="#FF9800",
            )


def _render_pipeline_health(conn: sqlite3.Connection) -> None:
    st.subheader("Pipeline Health")

    tis_df = time_in_stage(conn)

    # --- Stuck jobs ---
    st.markdown(f"**Opportunities stuck in stage > {_STUCK_THRESHOLD_DAYS} days**")
    if tis_df.empty:
        st.info("No stage-duration data available.")
    else:
        stuck = tis_df[
            tis_df["days_in_stage"].notna()
            & (tis_df["days_in_stage"] > _STUCK_THRESHOLD_DAYS)
        ].sort_values("days_in_stage", ascending=False)

        if stuck.empty:
            st.success(f"No opportunities stuck longer than {_STUCK_THRESHOLD_DAYS} days.")
        else:
            st.dataframe(
                stuck[["company", "title", "current_stage", "days_in_stage"]],
                hide_index=True,
                use_container_width=True,
                column_config={
                    "days_in_stage": st.column_config.NumberColumn("Days in Stage", format="%d"),
                },
            )

    st.divider()

    # --- High-score, not applied ---
    st.markdown("**High-score opportunities not yet applied to (score ≥ 70)**")
    hs_df = high_score_not_applied(conn, min_score=70)
    if hs_df.empty:
        st.info("No high-score opportunities waiting for action.")
    else:
        vis_cols = [c for c in ["company", "title", "score", "fit_band", "current_stage", "location", "url"] if c in hs_df.columns]
        st.dataframe(
            hs_df[vis_cols],
            hide_index=True,
            use_container_width=True,
            column_config={
                "score": st.column_config.NumberColumn("Score", format="%.1f"),
                "url": st.column_config.LinkColumn("URL"),
            },
        )


def _render_company_pipeline(conn: sqlite3.Connection) -> None:
    st.subheader("Company Pipeline")

    df = company_pipeline(conn)
    if df.empty:
        st.info("No data available.")
        return

    st.dataframe(
        df,
        hide_index=True,
        use_container_width=True,
        column_config={
            "company": st.column_config.TextColumn("Company", width="medium"),
            "total": st.column_config.NumberColumn("Total", width="small"),
            "applied": st.column_config.NumberColumn("Applied", width="small"),
            "interviewed": st.column_config.NumberColumn("Interviewed", width="small"),
            "offer": st.column_config.NumberColumn("Offer", width="small"),
        },
    )


def _render_score_vs_outcome(conn: sqlite3.Connection) -> None:
    st.subheader("Score vs Outcome")

    df = score_vs_outcome(conn)
    if df.empty:
        st.info("Not enough data for outcome analysis. Apply to some jobs first.")
        return

    col_table, col_chart = st.columns([1, 2])
    with col_table:
        st.dataframe(
            df,
            hide_index=True,
            use_container_width=True,
            column_config={
                "stage": st.column_config.TextColumn("Stage"),
                "avg_score": st.column_config.NumberColumn("Avg Score", format="%.1f"),
                "count": st.column_config.NumberColumn("Count"),
            },
        )
    with col_chart:
        st.bar_chart(
            df.set_index("stage")["avg_score"],
            use_container_width=True,
            color="#9C27B0",
        )


# ── Main page renderer ────────────────────────────────────────────────────────

def render_analytics(conn: sqlite3.Connection) -> None:
    """Entry point called from app.py."""
    st.title("Analytics")

    tab_funnel, tab_score, tab_health, tab_company, tab_outcome = st.tabs([
        "Funnel",
        "Score Analysis",
        "Pipeline Health",
        "By Company",
        "Score vs Outcome",
    ])

    with tab_funnel:
        _render_funnel_overview(conn)

    with tab_score:
        _render_score_analysis(conn)

    with tab_health:
        _render_pipeline_health(conn)

    with tab_company:
        _render_company_pipeline(conn)

    with tab_outcome:
        _render_score_vs_outcome(conn)
