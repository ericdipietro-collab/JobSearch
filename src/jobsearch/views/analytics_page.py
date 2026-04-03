"""pages/analytics_page.py — Analytics & reporting page for the Streamlit dashboard."""

from __future__ import annotations

import ast
import sqlite3
from collections import Counter
import re

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


def _normalize_gap_text(value: str) -> str:
    return re.sub(r"[^a-z0-9+/#.\s-]+", " ", str(value or "").lower()).strip()


def _resume_contains_keyword(resume_text: str, keyword: str) -> bool:
    normalized_resume = f" {_normalize_gap_text(resume_text)} "
    normalized_keyword = _normalize_gap_text(keyword)
    if not normalized_keyword:
        return True
    return f" {normalized_keyword} " in normalized_resume


def _resume_gap_rows(
    df: pd.DataFrame,
    resume_text: str,
    minimum_score: float = 60.0,
    ignored_keywords: list[str] | None = None,
) -> pd.DataFrame:
    if df.empty or not resume_text.strip():
        return pd.DataFrame()
    ignored = {_normalize_gap_text(keyword) for keyword in (ignored_keywords or []) if _normalize_gap_text(keyword)}

    work_df = df.copy()
    if "score" in work_df.columns:
        work_df["score"] = pd.to_numeric(work_df["score"], errors="coerce").fillna(0.0)
        work_df = work_df[work_df["score"] >= float(minimum_score)]
    if work_df.empty:
        return pd.DataFrame()

    keyword_rows = []
    for _, row in work_df.iterrows():
        keywords = _parse_keyword_blob(row.get("matched_keywords"))
        unique_keywords = []
        seen = set()
        for keyword in keywords:
            normalized = _normalize_gap_text(keyword)
            if normalized and normalized not in seen:
                unique_keywords.append(keyword)
                seen.add(normalized)
        for keyword in unique_keywords:
            if _normalize_gap_text(keyword) in ignored:
                continue
            if _resume_contains_keyword(resume_text, keyword):
                continue
            keyword_rows.append(
                {
                    "keyword": keyword,
                    "company": row.get("company", ""),
                    "title": row.get("title") or row.get("role") or "",
                    "score": float(row.get("score", 0.0) or 0.0),
                }
            )

    if not keyword_rows:
        return pd.DataFrame()

    keyword_df = pd.DataFrame(keyword_rows)
    total_roles = max(len(work_df), 1)
    gap_df = (
        keyword_df.groupby("keyword")
        .agg(
            roles=("keyword", "size"),
            avg_score=("score", "mean"),
            companies=("company", lambda s: ", ".join(sorted({str(v) for v in s if str(v).strip()})[:3])),
            sample_titles=("title", lambda s: " | ".join(list(dict.fromkeys([str(v) for v in s if str(v).strip()]))[:3])),
        )
        .reset_index()
        .sort_values(["roles", "avg_score", "keyword"], ascending=[False, False, True])
    )
    gap_df["coverage_pct"] = gap_df["roles"].map(lambda count: round((count / total_roles) * 100, 1))
    return gap_df


def _render_resume_gap_analysis(conn: sqlite3.Connection) -> None:
    st.subheader("Resume Keyword Gap Analysis")

    resume_name = (conn.execute("SELECT value FROM settings WHERE key = 'base_resume_name'").fetchone() or [None])[0]
    resume_text = (conn.execute("SELECT value FROM settings WHERE key = 'base_resume_text'").fetchone() or [None])[0] or ""
    ignored_keywords = (
        (conn.execute("SELECT value FROM settings WHERE key = 'base_resume_keyword_ignore'").fetchone() or [None])[0]
        or ""
    )
    if not str(resume_text).strip():
        st.info("Add your base resume in Search Settings → Base Resume to enable keyword gap analysis.")
        return

    apps_df = pd.DataFrame(
        [
            dict(row)
            for row in conn.execute(
                "SELECT company, role AS title, score, matched_keywords, status FROM applications"
            ).fetchall()
        ]
    )
    if apps_df.empty:
        st.info("No scraped role data available yet.")
        return

    minimum_score = st.slider("Minimum role score to include", min_value=35, max_value=95, value=60, step=5)
    ignored = [line.strip() for line in str(ignored_keywords).splitlines() if line.strip()]
    gap_df = _resume_gap_rows(apps_df, resume_text, minimum_score=minimum_score, ignored_keywords=ignored)
    col1, col2, col3 = st.columns(3)
    col1.metric("Base Resume", resume_name or "Stored")
    col2.metric("Roles Scanned", int((pd.to_numeric(apps_df.get("score"), errors="coerce").fillna(0) >= minimum_score).sum()))
    col3.metric("Gap Keywords", len(gap_df))

    if gap_df.empty:
        st.success("No obvious keyword gaps found at the selected score threshold.")
        return

    st.caption("Terms below are appearing repeatedly in stronger roles but were not found in the stored base resume.")
    st.dataframe(
        gap_df[["keyword", "roles", "coverage_pct", "avg_score", "companies", "sample_titles"]],
        hide_index=True,
        use_container_width=True,
        column_config={
            "roles": st.column_config.NumberColumn("Roles"),
            "coverage_pct": st.column_config.NumberColumn("Coverage %", format="%.1f"),
            "avg_score": st.column_config.NumberColumn("Avg Score", format="%.1f"),
        },
    )


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

    recommendations: list[str] = []
    if not keyword_df.empty:
        top_keyword = keyword_df.iloc[0]
        recommendations.append(
            f"Penalty hotspot: `{top_keyword['keyword']}` appears in {int(top_keyword['count'])} rejected roles. "
            "Review whether this should stay a hard negative in preferences or be softened for adjacent roles."
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
        if not family_compare.empty:
            top_gap = family_compare.sort_values("gap", ascending=False).iloc[0]
            if int(top_gap["gap"]) > 0:
                recommendations.append(
                    f"Blind spot: `{top_gap['title_family']}` roles are being rejected more often than you advance them. "
                    "Compare recent rejects in this family against your stronger applications and tighten title/keyword targeting."
                )

    if not company_counts.empty:
        top_company = company_counts.iloc[0]
        if int(top_company["rejected_count"]) >= 3:
            recommendations.append(
                f"Company pattern: `{top_company['company']}` produced {int(top_company['rejected_count'])} rejects in the latest run. "
                "Check if this target is systematically outside your fit or if the registry needs a cleaner source."
            )

    if recommendations:
        st.markdown("**Recommended Actions**")
        for line in recommendations:
            st.markdown(f"- {line}")

    with st.expander("Inspect latest rejected rows"):
        show_cols = [c for c in ["company", "title", "score", "title_family", "drop_reason", "decision_reason", "penalized_keywords"] if c in rejected_df.columns]
        st.dataframe(rejected_df[show_cols], hide_index=True, use_container_width=True)


def _render_interview_signal_analysis(conn: sqlite3.Connection) -> None:
    st.subheader("Interview Debrief + Outcome Correlation")
    rows = [dict(row) for row in conn.execute(
        """
        SELECT i.*, a.company, a.role, a.status
        FROM interviews i
        JOIN applications a ON a.id = i.application_id
        """
    ).fetchall()]
    if not rows:
        st.info("No interview data available yet.")
        return

    df = pd.DataFrame(rows)
    if df.empty:
        st.info("No interview data available yet.")
        return

    df["outcome"] = df["outcome"].fillna("pending").astype(str)
    scored_cols = ["rapport_score", "role_clarity_score", "interviewer_engaged_score", "confidence_score"]
    for col in scored_cols:
        df[col] = pd.to_numeric(df.get(col), errors="coerce")
    bool_cols = ["next_steps_clear", "timeline_mentioned", "compensation_discussed", "availability_discussed"]
    for col in bool_cols:
        df[col] = pd.to_numeric(df.get(col), errors="coerce").fillna(0).astype(int)

    reviewed_df = df[
        df[scored_cols + bool_cols].notna().any(axis=1)
    ].copy()
    c1, c2, c3 = st.columns(3)
    c1.metric("Interview Rows", len(df))
    c2.metric("Debriefs Completed", len(reviewed_df))
    c3.metric("Passed Rounds", int((df["outcome"] == "passed").sum()))

    if reviewed_df.empty:
        st.info("Start filling out Interview Debrief on interview rounds to unlock signal correlation.")
        return

    avg_df = (
        reviewed_df.groupby("outcome")[scored_cols]
        .mean()
        .reset_index()
        .sort_values("outcome")
    )
    st.markdown("**Average Debrief Scores by Outcome**")
    st.dataframe(avg_df, hide_index=True, use_container_width=True)

    signal_rows = []
    for col in bool_cols:
        yes_df = reviewed_df[reviewed_df[col] == 1]
        total = len(yes_df)
        passed = int((yes_df["outcome"] == "passed").sum())
        failed = int((yes_df["outcome"] == "failed").sum())
        signal_rows.append(
            {
                "signal": col.replace("_", " ").title(),
                "count": total,
                "passed": passed,
                "failed": failed,
                "pass_rate_pct": round((passed / total) * 100, 1) if total else 0.0,
            }
        )
    signal_df = pd.DataFrame(signal_rows).sort_values(["pass_rate_pct", "count"], ascending=[False, False])
    st.markdown("**Signal Win Rates**")
    st.dataframe(signal_df, hide_index=True, use_container_width=True)

    with st.expander("Latest interview debrief rows"):
        show_cols = [
            "company",
            "role",
            "scheduled_at",
            "outcome",
            "rapport_score",
            "role_clarity_score",
            "interviewer_engaged_score",
            "confidence_score",
            "next_steps_clear",
            "timeline_mentioned",
            "compensation_discussed",
            "availability_discussed",
            "debrief_notes",
        ]
        st.dataframe(reviewed_df[[c for c in show_cols if c in reviewed_df.columns]], hide_index=True, use_container_width=True)


# ── Main page renderer ────────────────────────────────────────────────────────

def render_analytics(conn: sqlite3.Connection) -> None:
    """Entry point called from app.py."""
    st.title("Analytics")

    tab_funnel, tab_score, tab_health, tab_company, tab_outcome, tab_reject, tab_resume, tab_interview = st.tabs([
        "Funnel",
        "Score Analysis",
        "Pipeline Health",
        "By Company",
        "Score vs Outcome",
        "Rejection Patterns",
        "Resume Gaps",
        "Interview Signals",
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

    with tab_resume:
        _render_resume_gap_analysis(conn)

    with tab_interview:
        _render_interview_signal_analysis(conn)
