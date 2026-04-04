"""
Home dashboard — overview of your job search at a glance.
"""
from __future__ import annotations

from datetime import date, datetime, timedelta

import altair as alt
import pandas as pd
import streamlit as st

from jobsearch import ats_db as db
from jobsearch.views.setup_wizard_page import render_setup_checklist
from jobsearch.views.style_utils import feed_item, empty_state

EVENT_ICONS = {
    "applied":             "📨",
    "conversation":        "💬",
    "networking_call":     "🤝",
    "recruiter_outreach":  "📞",
    "screening_scheduled": "📅",
    "screening_complete":  "✅",
    "interview_scheduled": "🗓️",
    "interview_complete":  "🎙️",
    "offer_received":      "🎉",
    "offer_negotiating":   "🤝",
    "offer_accepted":      "✔️",
    "offer_declined":      "❌",
    "rejected":            "👎",
    "withdrawn":           "🚪",
    "follow_up_sent":      "📧",
    "note":                "📝",
}


def render_home(conn) -> None:
    db.init_db(conn)

    render_setup_checklist(conn)

    st.title("Welcome Back")
    st.markdown("<p style='color: #64748b; font-size: 1.1rem; margin-top: -1rem;'>Here is your job search at a glance.</p>", unsafe_allow_html=True)
    st.markdown("<div style='margin-bottom: 2rem;'></div>", unsafe_allow_html=True)

    # ── Top KPI row ───────────────────────────────────────────────────────────
    today = date.today()
    week_start = today - timedelta(days=today.weekday())
    week_end   = week_start + timedelta(days=6)

    overdue_count   = len(db.follow_up_due(conn))
    interviews_week = db.upcoming_interviews_this_week(conn)
    weekly_count    = db.weekly_activity_count(conn, week_start.isoformat(), week_end.isoformat())
    weekly_goal     = int(db.get_setting(conn, "weekly_activity_goal", default="3"))
    active_count    = db.apply_now_count(conn)

    k1, k2, k3, k4 = st.columns(4)
    with k1:
        st.metric("Active Apps", active_count)
    with k2:
        st.metric(
            "Weekly Goal",
            f"{weekly_count} / {weekly_goal}",
            delta="On Track" if weekly_count >= weekly_goal else f"-{weekly_goal - weekly_count}",
            delta_color="normal" if weekly_count >= weekly_goal else "inverse",
        )
    with k3:
        st.metric("Overdue", overdue_count, delta_color="inverse" if overdue_count else "off")
    with k4:
        st.metric("Interviews", len(interviews_week))

    # ── Weekly activity progress bar ─────────────────────────────────────────
    st.markdown("<div style='margin-bottom: 2rem;'></div>", unsafe_allow_html=True)
    
    with st.container(border=True):
        st.subheader("Weekly Progress", anchor=False)
        pct = min(weekly_count / weekly_goal, 1.0) if weekly_goal else 0.0
        if weekly_count >= weekly_goal:
            st.success(f"Goal met: {weekly_count} / {weekly_goal} activities")
        else:
            st.progress(pct, text=f"{weekly_count} / {weekly_goal} activities this week")

        if interviews_week:
            st.markdown("<div style='margin-top: 1rem;'></div>", unsafe_allow_html=True)
            for iv in interviews_week:
                sched = iv["scheduled_at"] or ""
                try:
                    sched_fmt = datetime.fromisoformat(sched).strftime("%a %b %d %I:%M %p")
                except Exception:
                    sched_fmt = sched[:16]
                st.info(
                    f"🗓️ **{iv['company']}** — {iv['role']}  "
                    f"| {(iv['interview_type'] or 'interview').replace('_',' ').title()}  "
                    f"| {sched_fmt}"
                )

    # ── Pipeline snapshot ─────────────────────────────────────────────────────
    st.markdown("<div style='margin-bottom: 2.5rem;'></div>", unsafe_allow_html=True)
    st.subheader("Application Pipeline", anchor=False)

    overdue_followups = db.follow_up_due(conn)
    upcoming_followups = db.follow_up_upcoming(conn, days=3)
    ghosted_apps = db.potentially_ghosted_applications(conn, days_without_response=14, limit=8)
    if overdue_followups or upcoming_followups:
        st.caption("**Follow-up queue**")
        q1, q2 = st.columns(2)
        with q1:
            if overdue_followups:
                st.warning("Overdue")
                for app in overdue_followups[:5]:
                    st.markdown(f"- **{app['company']}** â€” {app['role']}  \nDue {app['follow_up_date']}")
            else:
                st.caption("No overdue follow-ups.")
        with q2:
            if upcoming_followups:
                st.info("Due in next 3 days")
                for app in upcoming_followups[:5]:
                    st.markdown(f"- **{app['company']}** â€” {app['role']}  \nDue {app['follow_up_date']}")
            else:
                st.caption("No upcoming follow-ups.")

    if ghosted_apps:
        st.warning(
            f"Potentially ghosted: {len(ghosted_apps)} application{'s' if len(ghosted_apps) != 1 else ''} "
            f"have had no response for 14+ days.",
            icon="👻",
        )
        for app in ghosted_apps[:5]:
            st.markdown(
                f"- **{app['company']}** — {app['role']}  \nApplied {app['date_applied']}"
            )

    snapshot = db.pipeline_snapshot(conn)
    active_statuses = ["applied", "screening", "interviewing", "offer"]
    closed_statuses = ["accepted", "rejected", "withdrawn"]

    if not snapshot:
        st.info("No applications tracked yet. Go to **My Applications** to add your first entry.")
    else:
        # Active pipeline
        act_cols = st.columns(len(active_statuses))
        for col, status in zip(act_cols, active_statuses):
            count = snapshot.get(status, 0)
            color = db.STATUS_COLORS.get(status, "#6b7280")
            col.markdown(
                f'<div style="border-left:4px solid {color};padding:8px 12px;border-radius:4px">'
                f'<div style="font-size:.78rem;text-transform:uppercase;letter-spacing:.05em;color:{color}">'
                f'{status.title()}</div>'
                f'<div style="font-size:1.8rem;font-weight:700">{count}</div>'
                f'</div>',
                unsafe_allow_html=True,
            )

        st.caption("")
        cl_cols = st.columns(len(closed_statuses) + 1)
        for col, status in zip(cl_cols, closed_statuses):
            count = snapshot.get(status, 0)
            color = db.STATUS_COLORS.get(status, "#6b7280")
            col.markdown(
                f'<div style="border-left:4px solid {color};padding:6px 10px;border-radius:4px;opacity:.75">'
                f'<div style="font-size:.75rem;text-transform:uppercase;color:{color}">{status.title()}</div>'
                f'<div style="font-size:1.4rem;font-weight:600">{count}</div>'
                f'</div>',
                unsafe_allow_html=True,
            )
        # Exploring / considering
        other_count = snapshot.get("exploring", 0) + snapshot.get("considering", 0)
        cl_cols[-1].markdown(
            f'<div style="border-left:4px solid #7c3aed;padding:6px 10px;border-radius:4px;opacity:.75">'
            f'<div style="font-size:.75rem;text-transform:uppercase;color:#7c3aed">Exploring / Considering</div>'
            f'<div style="font-size:1.4rem;font-weight:600">{other_count}</div>'
            f'</div>',
            unsafe_allow_html=True,
        )

    # ── Charts ────────────────────────────────────────────────────────────────
    st.divider()
    st.subheader("Activity Trends", anchor=False)

    ch1, ch2 = st.columns(2)

    with ch1:
        st.caption("Applications submitted — last 8 weeks")
        weekly_data = db.applications_by_week(conn, weeks=8)
        if weekly_data:
            wdf = pd.DataFrame(weekly_data)
            chart = (
                alt.Chart(wdf)
                .mark_bar(color="#4f46e5")
                .encode(
                    x=alt.X("week:N", sort=None, axis=alt.Axis(labelAngle=-45, title=None)),
                    y=alt.Y("count:Q", axis=alt.Axis(title="Applications", tickMinStep=1)),
                    tooltip=["week", "count"],
                )
                .properties(height=220)
            )
            st.altair_chart(chart, use_container_width=True)
        else:
            st.caption("No application events logged yet.")

    with ch2:
        st.caption("Pipeline by status")
        funnel_statuses = ["applied", "screening", "interviewing", "offer", "accepted"]
        funnel_data = {"status": funnel_statuses, "count": [snapshot.get(s, 0) for s in funnel_statuses]}
        fdf = pd.DataFrame(funnel_data)
        if fdf["count"].sum() > 0:
            status_colors = {s: db.STATUS_COLORS.get(s, "#6b7280") for s in funnel_statuses}
            chart = (
                alt.Chart(fdf)
                .mark_bar()
                .encode(
                    x=alt.X("status:N", sort=funnel_statuses, axis=alt.Axis(labelAngle=0, title=None)),
                    y=alt.Y("count:Q", axis=alt.Axis(title="Count", tickMinStep=1)),
                    color=alt.Color("status:N", scale=alt.Scale(
                        domain=funnel_statuses,
                        range=[status_colors[s] for s in funnel_statuses],
                    ), legend=None),
                    tooltip=["status", "count"],
                )
                .properties(height=220)
            )
            st.altair_chart(chart, use_container_width=True)
        else:
            st.caption("No active applications yet.")

    # ── Recent activity feed ──────────────────────────────────────────────────
    st.markdown("<div style='margin-bottom: 2rem;'></div>", unsafe_allow_html=True)
    st.subheader("Recent Activity", anchor=False)

    recent = db.get_recent_events(conn, limit=10)
    if not recent:
        empty_state("📝", "No activity yet", "Your recent application events and networking calls will show up here.")
    else:
        with st.container(border=True):
            for ev in recent:
                icon  = EVENT_ICONS.get(ev["event_type"], "•")
                label = db.EVENT_LABELS.get(ev["event_type"], ev["event_type"].replace("_", " ").title())
                d_str = ev["event_date"][:10] if ev["event_date"] else ""
                try:
                    d_fmt = date.fromisoformat(d_str).strftime("%b %d")
                except Exception:
                    d_fmt = d_str
                
                feed_item(
                    icon=icon,
                    title=ev["company"],
                    subtitle=f"{ev['role'] or ''} • {label}",
                    date_text=d_fmt
                )
