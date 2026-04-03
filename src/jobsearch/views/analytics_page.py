"""pages/analytics_page.py — Analytics & reporting page for the Streamlit dashboard."""

from __future__ import annotations

import ast
import sqlite3
from collections import Counter

import altair as alt
import pandas as pd
import streamlit as st

from jobsearch.config.settings import settings
from jobsearch.services.analytics_service import (
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


def _load_rejected_jobs() -> pd.DataFrame:
    path = settings.rejected_csv
    if not path.exists():
        return pd.DataFrame()
    try:
        return pd.read_csv(path)
    except Exception:
        return pd.DataFrame()


def _parse_keyword_blob(value) -> list[str]:
    if value is None:
        return []
    if isinstance(value, list):
        return [str(item).strip() for item in value if str(item).strip()]
    text = str(value).strip()
    if not text or text.lower() in {"nan", "none", "[]"}:
        return []
    try:
        parsed = ast.literal_eval(text)
        if isinstance(parsed, list):
            return [str(item).strip() for item in parsed if str(item).strip()]
    except Exception:
        pass
    return [item.strip() for item in text.split(",") if item.strip()]


def _title_family(title: str) -> str:
    text = str(title or "").lower()
    checks = [
        ("Architect", ["architect"]),
        ("Product", ["product manager", "product owner", "product lead", "product"]),
        ("Analyst", ["analyst", "business systems"]),
        ("Consulting", ["consultant", "solutions engineer", "solution engineer"]),
        ("Program/Project", ["program manager", "project manager", "pmo"]),
        ("Leadership", ["director", "vp", "head of", "chief"]),
        ("Engineering", ["engineer", "developer", "devops", "sre"]),
    ]
    for family, needles in checks:
        if any(needle in text for needle in needles):
            return family
    return "Other"


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
    st.altair_chart(
        alt.Chart(df).mark_bar(color="#2196F3").encode(
            x=alt.X("stage:N", sort=None, axis=alt.Axis(labelAngle=-30, title=None)),
            y=alt.Y("count:Q", axis=alt.Axis(tickMinStep=1, title="Count")),
            tooltip=["stage", "count"],
        ).properties(height=240),
        use_container_width=True,
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
            st.altair_chart(
                alt.Chart(df).mark_bar(color="#FF9800").encode(
                    x=alt.X("stage:N", sort=None, axis=alt.Axis(labelAngle=-30, title=None)),
                    y=alt.Y("avg_score:Q", axis=alt.Axis(title="Avg Score")),
                    tooltip=["stage", alt.Tooltip("avg_score:Q", format=".1f")],
                ).properties(height=240),
                use_container_width=True,
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
        st.altair_chart(
            alt.Chart(df).mark_bar(color="#9C27B0").encode(
                x=alt.X("stage:N", sort=None, axis=alt.Axis(labelAngle=-30, title=None)),
                y=alt.Y("avg_score:Q", axis=alt.Axis(title="Avg Score")),
                tooltip=["stage", alt.Tooltip("avg_score:Q", format=".1f")],
            ).properties(height=240),
            use_container_width=True,
        )


def _render_rejection_patterns(conn: sqlite3.Connection) -> None:
    st.subheader("Rejection Pattern Analysis")

    rejected_df = _load_rejected_jobs()
    if rejected_df.empty:
        st.info("No rejected-job file available yet. Run the pipeline first.")
        return

    apps_df = pd.DataFrame([dict(r) for r in conn.execute("SELECT company, role, status, score FROM applications").fetchall()])
    applied_df = apps_df[apps_df["status"].isin(["applied", "screening", "interviewing", "offer", "accepted"])] if not apps_df.empty else pd.DataFrame()

    rejected_df = rejected_df.copy()
    rejected_df["title_family"] = rejected_df["title"].map(_title_family)
    rejected_df["score"] = pd.to_numeric(rejected_df.get("score"), errors="coerce")
    if not applied_df.empty:
        applied_df = applied_df.copy()
        applied_df["title_family"] = applied_df["role"].map(_title_family)

    col1, col2, col3 = st.columns(3)
    col1.metric("Rejected in Latest Run", len(rejected_df))
    col2.metric("Rejected Companies", rejected_df["company"].nunique())
    col3.metric("Rejected Title Families", rejected_df["title_family"].nunique())

    company_counts = (
        rejected_df.groupby("company")
        .agg(rejected_count=("company", "size"), avg_score=("score", "mean"))
        .reset_index()
        .sort_values(["rejected_count", "avg_score"], ascending=[False, False])
    )
    family_counts = (
        rejected_df.groupby("title_family")
        .size()
        .reset_index(name="rejected_count")
        .sort_values("rejected_count", ascending=False)
    )

    keyword_counter = Counter()
    for value in rejected_df.get("penalized_keywords", pd.Series(dtype=object)):
        keyword_counter.update(_parse_keyword_blob(value))
    keyword_df = pd.DataFrame(
        [{"keyword": keyword, "count": count} for keyword, count in keyword_counter.most_common(10)]
    )

    top_left, top_right = st.columns(2)
    with top_left:
        st.markdown("**Top Rejected Companies**")
        st.dataframe(company_counts.head(10), hide_index=True, use_container_width=True)
    with top_right:
        st.markdown("**Rejected Title Families**")
        st.dataframe(family_counts, hide_index=True, use_container_width=True)

    if not keyword_df.empty:
        st.markdown("**Most Common Penalty Signals**")
        st.altair_chart(
            alt.Chart(keyword_df).mark_bar(color="#E57373").encode(
                x=alt.X("count:Q", title="Rejected Jobs"),
                y=alt.Y("keyword:N", sort="-x", title=None),
                tooltip=["keyword", "count"],
            ).properties(height=260),
            use_container_width=True,
        )

    if not applied_df.empty:
        applied_family = (
            applied_df.groupby("title_family").size().reset_index(name="applied_count")
        )
        family_compare = family_counts.merge(applied_family, on="title_family", how="outer").fillna(0)
        family_compare["gap"] = family_compare["rejected_count"] - family_compare["applied_count"]
        st.markdown("**Title Family Blind Spots**")
        st.dataframe(
            family_compare.sort_values("gap", ascending=False),
            hide_index=True,
            use_container_width=True,
        )

    with st.expander("Inspect latest rejected rows"):
        show_cols = [c for c in ["company", "title", "score", "title_family", "drop_reason", "decision_reason", "penalized_keywords"] if c in rejected_df.columns]
        st.dataframe(rejected_df[show_cols], hide_index=True, use_container_width=True)


# ── Main page renderer ────────────────────────────────────────────────────────

def render_analytics(conn: sqlite3.Connection) -> None:
    """Entry point called from app.py."""
    st.title("Analytics")

    tab_funnel, tab_score, tab_health, tab_company, tab_outcome, tab_reject = st.tabs([
        "Funnel",
        "Score Analysis",
        "Pipeline Health",
        "By Company",
        "Score vs Outcome",
        "Rejection Patterns",
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

    with tab_reject:
        _render_rejection_patterns(conn)
