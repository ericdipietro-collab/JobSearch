"""
Weekly Activity Report — generates a job-search activity log for any date range.
Designed to make filling out unemployment benefit forms straightforward.
"""
from __future__ import annotations

from datetime import date, timedelta
from typing import List

import pandas as pd
import streamlit as st

import ats_db as db

# ── Human-friendly method labels ───────────────────────────────────────────────

_METHOD = {
    "applied":             "Applied online",
    "conversation":        "Phone / video call",
    "networking_call":     "Phone / video call",
    "recruiter_outreach":  "Phone / email",
    "screening_scheduled": "Phone / video call",
    "screening_complete":  "Phone / video call",
    "interview_scheduled": "In-person / video interview",
    "interview_complete":  "In-person / video interview",
    "follow_up_sent":      "Email follow-up",
}

_RESULT = {
    "applied":             "Submitted application",
    "conversation":        "Networking conversation",
    "networking_call":     "Networking call",
    "recruiter_outreach":  "Recruiter reached out",
    "screening_scheduled": "Phone screen scheduled",
    "screening_complete":  "Phone screen completed",
    "interview_scheduled": "Interview scheduled",
    "interview_complete":  "Interview completed",
    "follow_up_sent":      "Follow-up sent",
}


def _week_bounds(offset_weeks: int = 0):
    """Return (monday, sunday) for a given week offset (0 = current week)."""
    today = date.today()
    monday = today - timedelta(days=today.weekday()) - timedelta(weeks=offset_weeks)
    sunday = monday + timedelta(days=6)
    return monday, sunday


def _activity_minimum_banner(n_activities: int, n_training: int, period_is_single_week: bool, minimum: int = 3) -> None:
    """Show a compliance indicator when viewing a single-week period."""
    if not period_is_single_week:
        return
    total = n_activities + (1 if n_training else 0)   # training counts as 1 activity
    if total >= minimum:
        st.success(
            f"✅ **{total} / {minimum} required activities** this week — "
            f"you meet the minimum for your weekly certification.",
            icon=None,
        )
    elif total == minimum - 1:
        st.warning(
            f"⚠️ **{total} / {minimum} required activities** — "
            f"you need {minimum - total} more to meet the weekly minimum.",
            icon=None,
        )
    else:
        st.error(
            f"🔴 **{total} / {minimum} required activities** — "
            f"you need {minimum - total} more before submitting your weekly certification.",
            icon=None,
        )
    st.caption(
        f"Weekly goal: {minimum} activities. "
        "Adjust this in Search Settings → App Settings."
    )


def render_activity_report(conn) -> None:
    db.init_db(conn)

    st.markdown(
        "Use this page to review your job search activity and copy it into your "
        "unemployment weekly certification form."
    )

    # ── Date range controls ───────────────────────────────────────────────────
    st.subheader("Date Range", anchor=False)
    rc1, rc2, rc3 = st.columns([2, 2, 3])

    with rc3:
        preset = st.selectbox(
            "Quick select",
            ["This week", "Last week", "Last 2 weeks", "Last 30 days", "Custom"],
            key="report_preset",
            label_visibility="collapsed",
        )

    if preset == "This week":
        default_start, default_end = _week_bounds(0)
    elif preset == "Last week":
        default_start, default_end = _week_bounds(1)
    elif preset == "Last 2 weeks":
        default_start, _ = _week_bounds(1)
        _, default_end   = _week_bounds(0)
    elif preset == "Last 30 days":
        default_end   = date.today()
        default_start = default_end - timedelta(days=30)
    else:
        default_start, default_end = _week_bounds(1)  # last week as sane default

    with rc1:
        start_d = st.date_input("From", value=default_start, key="report_start")
    with rc2:
        end_d   = st.date_input("To",   value=default_end,   key="report_end")

    if start_d > end_d:
        st.error("Start date must be before end date.")
        return

    start_str = start_d.isoformat()
    end_str   = end_d.isoformat()

    # ── Fetch data ────────────────────────────────────────────────────────────
    rows = db.get_activity_report(conn, start_str, end_str)

    # Also fetch training active this period
    training_rows = db.get_training_for_report(conn, start_str, end_str)

    _weekly_goal = int(db.get_setting(conn, "weekly_activity_goal", default="3"))

    # Is this a single-calendar-week view? (Mon–Sun, exactly 7 days)
    _is_single_week = (
        (end_d - start_d).days == 6
        and start_d.weekday() == 0   # Monday
        and preset in ("This week", "Last week")
    )

    if not rows and not training_rows:
        _activity_minimum_banner(0, 0, _is_single_week, minimum=_weekly_goal)
        st.info(f"No reportable activity between {start_d.strftime('%b %d')} and {end_d.strftime('%b %d, %Y')}.")
        return

    # ── Summary metrics ───────────────────────────────────────────────────────
    st.divider()
    from collections import Counter
    type_counts = Counter(r["event_type"] for r in rows)

    apps_submitted   = type_counts.get("applied", 0)
    job_fairs        = sum(1 for r in rows if r["entry_type"] == "job_fair")
    networking       = type_counts.get("conversation", 0) + type_counts.get("networking_call", 0) - job_fairs
    recruiter_calls  = type_counts.get("recruiter_outreach", 0)
    screens          = type_counts.get("screening_complete", 0) + type_counts.get("screening_scheduled", 0)
    interviews       = type_counts.get("interview_complete", 0) + type_counts.get("interview_scheduled", 0)
    follow_ups       = type_counts.get("follow_up_sent", 0)
    training_count   = len(training_rows)

    m1, m2, m3, m4, m5, m6, m7 = st.columns(7)
    m1.metric("Applications",  apps_submitted)
    m2.metric("Job Fairs",     job_fairs)
    m3.metric("Networking",    max(networking, 0))
    m4.metric("Recruiter",     recruiter_calls)
    m5.metric("Screenings",    screens)
    m6.metric("Interviews",    interviews)
    m7.metric("Training",      training_count)

    # Weekly minimum compliance banner (single-week views only)
    _n_job_activities = len(rows)
    _activity_minimum_banner(_n_job_activities, training_count, _is_single_week, minimum=_weekly_goal)

    # ── Activity table ────────────────────────────────────────────────────────
    st.subheader("Activity Log", anchor=False)

    table_rows = []
    for r in rows:
        d_str = r["event_date"][:10] if r["event_date"] else ""
        try:
            d_fmt = date.fromisoformat(d_str).strftime("%b %d")
        except Exception:
            d_fmt = d_str
        label  = db.EVENT_LABELS.get(r["event_type"], r["event_type"].replace("_", " ").title())
        detail = r["event_title"] or ""
        if r["event_notes"]:
            detail = (detail + " — " + r["event_notes"]).strip(" —")
        contacts = r["contact_names"] or ""
        etype = r["entry_type"] or "application"
        type_label = {"application": "📋 Application",
                      "opportunity": "🤝 Opportunity",
                      "job_fair":    "🎪 Job Fair"}.get(etype, "📋 Application")
        table_rows.append({
            "Date":     d_fmt,
            "Company":  r["company"],
            "Role":     r["role"] or "—",
            "Type":     type_label,
            "Activity": label,
            "Contact":  contacts,
            "Detail":   detail,
        })

    for t in training_rows:
        status_label = "In Progress" if t["status"] == "in_progress" else "Completed"
        comp_str = f" (completed {t['completion_date'][:10]})" if t["completion_date"] else ""
        table_rows.append({
            "Date":     start_d.strftime("%b %d") + "–" + end_d.strftime("%b %d"),
            "Company":  t["provider"] or "—",
            "Role":     t["name"],
            "Type":     "📚 Training",
            "Activity": f"Job skills training — {status_label}",
            "Contact":  "",
            "Detail":   f"{t['category'] or ''}{comp_str}".strip(),
        })

    df_act = pd.DataFrame(table_rows)
    st.dataframe(
        df_act,
        hide_index=True,
        use_container_width=True,
        column_config={
            "Date":     st.column_config.TextColumn("Date",     width="small"),
            "Company":  st.column_config.TextColumn("Company",  width="medium"),
            "Role":     st.column_config.TextColumn("Role",     width="large"),
            "Type":     st.column_config.TextColumn("Type",     width="medium"),
            "Activity": st.column_config.TextColumn("Activity", width="medium"),
            "Contact":  st.column_config.TextColumn("Contact",  width="medium"),
            "Detail":   st.column_config.TextColumn("Detail",   width="large"),
        },
    )

    # ── Copyable unemployment report ──────────────────────────────────────────
    st.subheader("Unemployment Certification Report", anchor=False)
    st.caption(
        "Copy the text below and paste it into your weekly certification form "
        "(Colorado MyUI+ or similar). Each line is one activity entry."
    )

    lines: List[str] = []
    lines.append(
        f"Job Search Activity — {start_d.strftime('%B %d')} to {end_d.strftime('%B %d, %Y')}"
    )
    lines.append("=" * 60)
    summary_parts = [f"Applications submitted: {apps_submitted}"]
    if job_fairs:
        summary_parts.append(f"Job fairs attended: {job_fairs}")
    if max(networking, 0):
        summary_parts.append(f"Networking calls: {max(networking, 0)}")
    if recruiter_calls:
        summary_parts.append(f"Recruiter contacts: {recruiter_calls}")
    if screens + interviews:
        summary_parts.append(f"Interviews/screens: {screens + interviews}")
    if follow_ups:
        summary_parts.append(f"Follow-ups sent: {follow_ups}")
    if training_count:
        summary_parts.append(f"Training courses active: {training_count}")
    lines.append("  |  ".join(summary_parts))
    lines.append("")

    # Group job/opportunity activity by date
    by_date: dict = {}
    for r in rows:
        d_key = r["event_date"][:10] if r["event_date"] else "unknown"
        by_date.setdefault(d_key, []).append(r)

    for d_key in sorted(by_date.keys(), reverse=True):
        try:
            d_label = date.fromisoformat(d_key).strftime("%A, %B %d")
        except Exception:
            d_label = d_key
        lines.append(d_label)
        lines.append("-" * 40)
        for r in by_date[d_key]:
            etype = r["entry_type"] or "application"
            if etype == "job_fair":
                method = "In person / job fair"
                result = "Attended job fair"
            else:
                method = _METHOD.get(r["event_type"], "Online / email")
                result = _RESULT.get(r["event_type"], db.EVENT_LABELS.get(r["event_type"], r["event_type"]))
            contact = f" (contact: {r['contact_names']})" if r["contact_names"] else ""
            note    = f" — {r['event_title']}" if r["event_title"] else ""
            lines.append(
                f"  Employer: {r['company']}"
                + (f"  |  Position: {r['role']}" if r["role"] else "")
            )
            lines.append(f"  Method: {method}  |  Result: {result}{contact}{note}")
            lines.append("")
        lines.append("")

    # Training section
    if training_rows:
        lines.append("Job Skills Training")
        lines.append("-" * 40)
        for t in training_rows:
            status_str = "Completed" if t["status"] == "completed" else "In Progress"
            comp_note  = f" — completed {t['completion_date'][:10]}" if t["completion_date"] else ""
            lines.append(
                f"  Course: {t['name']}  |  Provider: {t['provider'] or 'N/A'}"
                f"  |  Category: {t['category'] or 'N/A'}  |  Status: {status_str}{comp_note}"
            )
        lines.append("")

    report_text = "\n".join(lines)
    st.text_area(
        "Report text",
        value=report_text,
        height=400,
        label_visibility="collapsed",
        key="report_text_area",
    )
    st.caption("Select all (Ctrl+A) and copy, or use the icon in the top-right corner of the text box.")
