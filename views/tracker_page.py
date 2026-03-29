"""
Application Tracker — mini CRM view.
Renders inside the main app.py navigation via render_tracker(conn).
"""
from __future__ import annotations

from datetime import date, datetime
from typing import Optional

import pandas as pd
import streamlit as st

import ats_db as db

# ── Helpers ────────────────────────────────────────────────────────────────────

def _status_badge(status: str) -> str:
    color = db.STATUS_COLORS.get(status, "#6b7280")
    return (
        f'<span style="background:{color};color:#fff;padding:2px 10px;'
        f'border-radius:999px;font-size:0.75rem;font-weight:600;'
        f'text-transform:uppercase;letter-spacing:.04em">{status}</span>'
    )


def _stars(n: Optional[int]) -> str:
    if not n:
        return "—"
    return "⭐" * int(n)


def _fmt_dt(iso: Optional[str]) -> str:
    if not iso:
        return "—"
    try:
        dt = datetime.fromisoformat(iso)
        if "T" in iso:
            return dt.strftime("%m/%d/%y %I:%M %p").lstrip("0")
        return dt.strftime("%m/%d/%y").lstrip("0")
    except Exception:
        return iso[:10]


def _days_until(iso: Optional[str]) -> Optional[int]:
    if not iso:
        return None
    try:
        target = date.fromisoformat(iso[:10])
        return (target - date.today()).days
    except Exception:
        return None


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


# ── Main entry point ───────────────────────────────────────────────────────────

def render_tracker(conn) -> None:
    db.init_db(conn)

    # Seed from CSV on first load
    csv_path = db._BASE_DIR / "results" / "ApplicationTracker.csv"  # type: ignore[attr-defined]
    if csv_path.exists() and not st.session_state.get("tracker_csv_seeded"):
        n = db.migrate_from_csv(conn, csv_path)
        if n:
            st.toast(f"Imported {n} applications from ApplicationTracker.csv", icon="📂")
        st.session_state["tracker_csv_seeded"] = True

    _render_followup_banner(conn)
    _render_summary_bar(conn)
    st.divider()

    # ── Filters + Add button ──────────────────────────────────────────────────
    fc1, fc2, fc3, fc4 = st.columns([2, 2, 2, 2])
    with fc1:
        status_opts = ["All statuses"] + db.STATUSES
        sel_status  = st.selectbox("Status", status_opts, key="tracker_status_filter",
                                   label_visibility="collapsed")
    with fc2:
        type_opts  = ["All types", "Applications", "Opportunities", "Job Fairs"]
        sel_type   = st.selectbox("Type", type_opts, key="tracker_type_filter",
                                  label_visibility="collapsed")
    with fc3:
        search = st.text_input("Search", placeholder="company or role…",
                               key="tracker_search", label_visibility="collapsed")
    with fc4:
        if st.button("➕ Add", key="tracker_add_btn", use_container_width=True):
            toggled = not st.session_state.get("tracker_show_add_form", False)
            st.session_state["tracker_show_add_form"] = toggled
            if toggled:
                st.session_state["tracker_selected_id"] = None

    if st.session_state.get("tracker_show_add_form"):
        _render_add_form(conn)
        st.divider()

    # ── Load + filter ─────────────────────────────────────────────────────────
    _entry_filter = {
        "Applications": "application",
        "Opportunities": "opportunity",
        "Job Fairs": "job_fair",
    }.get(sel_type)
    apps = db.get_applications(
        conn,
        status=None if sel_status == "All statuses" else sel_status,
        entry_type=_entry_filter,
    )
    if search:
        q = search.lower()
        apps = [a for a in apps if q in a["company"].lower() or q in a["role"].lower()]

    if not apps:
        st.info("No applications match the current filter.")
        return

    # ── Sortable table ────────────────────────────────────────────────────────
    rows = []
    for a in apps:
        fu_date   = a["follow_up_date"] or ""
        days_left = _days_until(fu_date) if fu_date else None
        if fu_date and days_left is not None and days_left < 0:
            fu_label = f"⚠ {fu_date}"
        else:
            fu_label = fu_date
        entry = a["entry_type"] if a["entry_type"] else "application"
        type_label = {"application": "📋 Application",
                      "opportunity": "🤝 Opportunity",
                      "job_fair":    "🎪 Job Fair"}.get(entry, "📋 Application")
        rows.append({
            "_id":       a["id"],
            "Type":      type_label,
            "Company":   a["company"],
            "Role":      a["role"],
            "Status":    a["status"].title(),
            "Applied":   a["date_applied"] or "",
            "Fit":       "⭐" * int(a["fit_stars"]) if a["fit_stars"] else "—",
            "Follow-up": fu_label,
        })
    df_tbl = pd.DataFrame(rows)

    event = st.dataframe(
        df_tbl.drop(columns=["_id"]),
        on_select="rerun",
        selection_mode="single-row",
        hide_index=True,
        use_container_width=True,
        column_config={
            "Type":      st.column_config.TextColumn("Type",      width="small"),
            "Company":   st.column_config.TextColumn("Company",   width="medium"),
            "Role":      st.column_config.TextColumn("Role",      width="large"),
            "Status":    st.column_config.TextColumn("Status",    width="medium"),
            "Applied":   st.column_config.TextColumn("Applied",   width="small"),
            "Fit":       st.column_config.TextColumn("Fit",       width="small"),
            "Follow-up": st.column_config.TextColumn("Follow-up", width="medium"),
        },
    )

    # Sync table row click → session state
    if event.selection.rows:
        sel_id = int(df_tbl.iloc[event.selection.rows[0]]["_id"])
        st.session_state["tracker_selected_id"] = sel_id
        st.session_state["tracker_show_add_form"] = False

    # ── Inline detail panel ───────────────────────────────────────────────────
    sel_id = st.session_state.get("tracker_selected_id")
    if sel_id:
        st.divider()
        hdr1, hdr2 = st.columns([10, 1])
        hdr1.subheader("Application Detail", anchor=False)
        if hdr2.button("✕ Close", key="close_detail"):
            st.session_state["tracker_selected_id"] = None
            st.rerun()
        _render_detail(conn, sel_id)


# ── Follow-up banner ──────────────────────────────────────────────────────────

def _render_followup_banner(conn) -> None:
    overdue  = db.follow_up_due(conn)
    upcoming = db.follow_up_upcoming(conn, days=3)

    if not overdue and not upcoming:
        return

    if overdue:
        with st.container():
            st.error(f"**{len(overdue)} follow-up{'s' if len(overdue)>1 else ''} overdue**", icon="🔔")
            for app in overdue:
                days_ago = abs(_days_until(app["follow_up_date"]) or 0)
                label = f"**{app['company']}** — {app['role']}"
                note  = app["follow_up_notes"] or ""
                contacts = app["contact_summary"] or ""
                col1, col2 = st.columns([5, 1])
                col1.markdown(
                    f"{label}  \n"
                    f"<span style='color:#f87171'>Due {app['follow_up_date']}"
                    f" ({days_ago} day{'s' if days_ago!=1 else ''} ago)</span>"
                    + (f"  \n👤 {contacts}" if contacts else "")
                    + (f"  \n_{note}_" if note else ""),
                    unsafe_allow_html=True,
                )
                if col2.button("View", key=f"fu_view_{app['id']}"):
                    st.session_state["tracker_selected_id"] = app["id"]
                    st.session_state["tracker_show_add_form"] = False
                    st.rerun()

    if upcoming:
        with st.expander(f"🗓 {len(upcoming)} follow-up{'s' if len(upcoming)>1 else ''} due in the next 3 days"):
            for app in upcoming:
                days_left = _days_until(app["follow_up_date"]) or 0
                contacts  = app["contact_summary"] or ""
                st.markdown(
                    f"**{app['company']}** — {app['role']}  \n"
                    f"<span style='color:#fbbf24'>Due {app['follow_up_date']}"
                    f" (in {days_left} day{'s' if days_left!=1 else ''})</span>"
                    + (f"  \n👤 {contacts}" if contacts else ""),
                    unsafe_allow_html=True,
                )


# ── Summary bar ────────────────────────────────────────────────────────────────

def _render_summary_bar(conn) -> None:
    counts = db.status_counts(conn)
    total  = sum(counts.values())
    active = sum(counts.get(s, 0) for s in ("applied", "screening", "interviewing", "offer"))

    cols = st.columns(6)
    metrics = [
        ("Total",        total,                          None),
        ("Active",        active,                        None),
        ("Interviewing",  counts.get("interviewing", 0), None),
        ("Offers",        counts.get("offer", 0),        None),
        ("Accepted",      counts.get("accepted", 0),     None),
        ("Rejected",      counts.get("rejected", 0),     None),
    ]
    for col, (label, val, delta) in zip(cols, metrics):
        col.metric(label, val, delta)

    # Upcoming interviews
    upcoming = db.upcoming_interviews(conn, limit=3)
    if upcoming:
        st.caption("**Upcoming interviews**")
        for iv in upcoming:
            st.info(
                f"🗓 **{iv['company']}** — {iv['role']}  "
                f"| {iv['interview_type'] or 'interview'}  "
                f"| {_fmt_dt(iv['scheduled_at'])}"
            )




# ── Add application form ───────────────────────────────────────────────────────

def _render_add_form(conn) -> None:
    with st.form("add_app_form", clear_on_submit=True):
        # Entry type toggle
        ta1, ta2 = st.columns(2)
        entry_type = ta1.radio(
            "Type",
            ["Application", "Opportunity", "Job Fair"],
            horizontal=True,
            key="add_form_entry_type",
            help="Application = formal job posting.  "
                 "Opportunity = network contact / informal conversation.  "
                 "Job Fair = attended a job fair or recruiting event.",
        )
        is_opp      = entry_type == "Opportunity"
        is_job_fair = entry_type == "Job Fair"

        st.markdown(f"**New {entry_type}**")
        r1c1, r1c2 = st.columns(2)
        company  = r1c1.text_input("Company *")
        role     = r1c2.text_input(
            "Role / Description *",
            placeholder="e.g. Product Lead" if is_opp else "e.g. Senior Product Manager",
        )
        r2c1, r2c2, r2c3 = st.columns(3)
        # Status defaults: application→applied, opportunity/job_fair→exploring
        default_status_idx = (
            db.STATUSES.index("applied") if not is_opp and not is_job_fair
            else db.STATUSES.index("exploring")
        )
        status   = r2c1.selectbox("Status", db.STATUSES, index=default_status_idx)
        fit      = r2c2.selectbox("Fit", ["—", "1", "2", "3", "4", "5"])
        date_app = r2c3.date_input(
            "Event Date" if is_job_fair else ("Date of first contact" if is_opp else "Date Applied"),
            value=date.today(),
        )

        if not is_opp and not is_job_fair:
            job_url = st.text_input("Job URL")
        else:
            job_url = ""

        sal_low, sal_high = st.columns(2)
        s_low  = sal_low.number_input("Salary Low ($)", value=0, step=5000)
        s_high = sal_high.number_input("Salary High ($)", value=0, step=5000)
        referral   = st.text_input(
            "Referred by" if (is_opp or is_job_fair) else "Referral",
            placeholder="e.g. John Smith (former manager)" if is_opp else
                        "e.g. Hired by Design networking event" if is_job_fair else "",
        )
        jd_summary = st.text_area(
            "Event / Role Description" if is_job_fair else ("Notes on the role" if is_opp else "JD Summary"),
            height=80,
            placeholder="e.g. Denver Tech Job Fair — talked to Acme Corp, TechCo, StartupX" if is_job_fair else "",
        )
        notes = st.text_area("Notes", height=60)

        st.markdown("**Follow-up**")
        rf1, rf2 = st.columns(2)
        follow_up_date  = rf1.date_input("Follow-up date", value=None)
        follow_up_notes = rf2.text_input(
            "Who / what to say",
            placeholder="e.g. Follow up with John after call",
        )

        if not is_opp and not is_job_fair:
            st.markdown("**Documents**")
            resume_version     = st.text_input("Resume version", placeholder="e.g. PM resume v3 – fintech tailored")
            cover_letter_notes = st.text_input("Cover letter notes", placeholder="e.g. Tailored intro, emphasized API PM exp")
        else:
            resume_version = cover_letter_notes = ""

        if st.form_submit_button("Save", type="primary"):
            if not company.strip() or not role.strip():
                st.error("Company and Role are required.")
            else:
                app_id = db.add_application(
                    conn,
                    company            = company.strip(),
                    role               = role.strip(),
                    job_url            = job_url.strip() or None,
                    source             = "manual",
                    entry_type         = "job_fair" if is_job_fair else ("opportunity" if is_opp else "application"),
                    status             = status,
                    fit_stars          = int(fit) if fit != "—" else None,
                    salary_low         = s_low  or None,
                    salary_high        = s_high or None,
                    salary_range       = f"${s_low:,}–${s_high:,}" if s_low and s_high else None,
                    referral           = referral.strip() or None,
                    jd_summary         = jd_summary.strip() or None,
                    notes              = notes.strip() or None,
                    date_applied       = date_app.isoformat(),
                    follow_up_date     = follow_up_date.isoformat() if follow_up_date else None,
                    follow_up_notes    = follow_up_notes.strip() or None,
                    resume_version     = resume_version.strip() or None,
                    cover_letter_notes = cover_letter_notes.strip() or None,
                )
                # First event based on type
                if is_job_fair:
                    first_event = "conversation"
                    first_title = f"Attended job fair — {company.strip()}"
                elif is_opp:
                    first_event = "conversation"
                    first_title = f"Initial conversation — {company.strip()}"
                else:
                    first_event = "applied"
                    first_title = f"Applied to {company.strip()}"
                db.add_event(conn, app_id, first_event, date_app.isoformat(), title=first_title)
                st.session_state["tracker_selected_id"]  = app_id
                st.session_state["tracker_show_add_form"] = False
                st.rerun()


# ── Application detail ─────────────────────────────────────────────────────────

def _render_detail(conn, app_id: int) -> None:
    app = db.get_application(conn, app_id)
    if not app:
        st.warning("Application not found.")
        st.session_state["tracker_selected_id"] = None
        return

    # Header
    color = db.STATUS_COLORS.get(app["status"], "#6b7280")
    st.markdown(
        f"### {app['company']}\n{app['role']}",
    )
    hc1, hc2, hc3, hc4 = st.columns(4)
    hc1.markdown(_status_badge(app["status"]), unsafe_allow_html=True)
    hc2.caption(f"Applied {app['date_applied'] or '—'}")
    hc3.caption(f"Fit {_stars(app['fit_stars'])}")
    hc4.caption(f"💰 {app['salary_range'] or '—'}")
    if app["job_url"]:
        st.markdown(f"[Job Posting ↗]({app['job_url']})")

    tab_tl, tab_iv, tab_co, tab_ed = st.tabs(["Timeline", "Interviews", "Contacts", "Edit"])

    with tab_tl:
        _render_timeline(conn, app)

    with tab_iv:
        _render_interviews(conn, app)

    with tab_co:
        _render_contacts(conn, app)

    with tab_ed:
        _render_edit_form(conn, app)


# ── Timeline tab ───────────────────────────────────────────────────────────────

def _render_timeline(conn, app) -> None:
    events = db.get_events(conn, app["id"])

    # Add event form
    with st.expander("➕ Add event", expanded=False):
        with st.form(f"add_event_{app['id']}"):
            ec1, ec2 = st.columns(2)
            etype    = ec1.selectbox("Type", db.EVENT_TYPES)
            edate    = ec2.date_input("Date", value=date.today())
            etitle   = st.text_input("Title (optional)")
            enotes   = st.text_area("Notes", height=70)
            if st.form_submit_button("Add", type="primary"):
                db.add_event(conn, app["id"], etype, edate.isoformat(),
                             title=etitle.strip() or None,
                             notes=enotes.strip() or None)
                # Auto-advance status
                _maybe_advance_status(conn, app, etype)
                st.rerun()

    st.divider()

    if not events:
        st.caption("No events yet.")
        return

    for ev in reversed(events):   # newest first
        icon  = EVENT_ICONS.get(ev["event_type"], "•")
        label = (ev["title"] or ev["event_type"].replace("_", " ").title())
        st.markdown(
            f'{icon} **{label}** <span style="color:#9ca3af;font-size:.8rem">'
            f'{_fmt_dt(ev["event_date"])}</span>',
            unsafe_allow_html=True,
        )
        if ev["notes"]:
            st.caption(ev["notes"])
        dc1, _ = st.columns([1, 8])
        if dc1.button("🗑", key=f"del_ev_{ev['id']}", help="Delete event"):
            db.delete_event(conn, ev["id"])
            st.rerun()
        st.markdown('<hr style="margin:4px 0;border-color:#374151">', unsafe_allow_html=True)


def _maybe_advance_status(conn, app, event_type: str) -> None:
    """Automatically advance application status based on the event logged."""
    mapping = {
        "applied":             "applied",
        "screening_scheduled": "screening",
        "screening_complete":  "screening",
        "interview_scheduled": "interviewing",
        "interview_complete":  "interviewing",
        "offer_received":      "offer",
        "offer_negotiating":   "offer",
        "offer_accepted":      "accepted",
        "offer_declined":      "withdrawn",
        "rejected":            "rejected",
        "withdrawn":           "withdrawn",
    }
    new_status = mapping.get(event_type)
    if new_status and new_status != app["status"]:
        db.update_application(conn, app["id"], status=new_status)


# ── Interviews tab ─────────────────────────────────────────────────────────────

def _render_interviews(conn, app) -> None:
    interviews = db.get_interviews(conn, app["id"])
    next_round = max((iv["round_number"] or 0 for iv in interviews), default=0) + 1

    with st.expander("➕ Schedule interview", expanded=False):
        with st.form(f"add_iv_{app['id']}"):
            ic1, ic2, ic3 = st.columns(3)
            round_n  = ic1.number_input("Round", value=next_round, min_value=1, step=1)
            iv_type  = ic2.selectbox("Type", db.INTERVIEW_TYPES)
            iv_fmt   = ic3.selectbox("Format", db.INTERVIEW_FORMATS)
            id1, id2 = st.columns(2)
            sched_d  = id1.date_input("Date", value=date.today())
            sched_t  = id2.time_input("Time")
            dur      = st.number_input("Duration (mins)", value=45, step=15)
            interviewers = st.text_input("Interviewers (comma-separated)")
            location = st.text_input("Location / Video link")
            prep     = st.text_area("Prep notes", height=70)
            if st.form_submit_button("Schedule", type="primary"):
                sched_dt = datetime.combine(sched_d, sched_t).isoformat()
                db.add_interview(
                    conn, app["id"],
                    round_number      = int(round_n),
                    interview_type    = iv_type,
                    format            = iv_fmt,
                    scheduled_at      = sched_dt,
                    duration_mins     = int(dur),
                    interviewer_names = interviewers.strip() or None,
                    location          = location.strip() or None,
                    prep_notes        = prep.strip() or None,
                )
                db.add_event(conn, app["id"], "interview_scheduled", sched_d.isoformat(),
                             title=f"Round {round_n} — {iv_type.replace('_',' ').title()} scheduled")
                if app["status"] not in ("interviewing", "offer", "accepted"):
                    db.update_application(conn, app["id"], status="interviewing")
                st.rerun()

    st.divider()

    if not interviews:
        st.caption("No interviews scheduled yet.")
        return

    for iv in interviews:
        outcome_color = {"pending": "#f59e0b", "passed": "#10b981", "failed": "#ef4444"}.get(
            iv["outcome"] or "pending", "#6b7280"
        )
        st.markdown(
            f"**Round {iv['round_number'] or '?'}** — "
            f"{(iv['interview_type'] or '').replace('_',' ').title()}  "
            f"<span style='color:{outcome_color};font-weight:600'>{iv['outcome'] or 'pending'}</span>  "
            f"<span style='color:#9ca3af;font-size:.8rem'>{_fmt_dt(iv['scheduled_at'])}</span>",
            unsafe_allow_html=True,
        )
        if iv["interviewer_names"]:
            st.caption(f"With: {iv['interviewer_names']}")
        if iv["location"]:
            st.caption(f"📍 {iv['location']}")

        # Quick outcome update
        oc1, oc2, _ = st.columns([2, 2, 4])
        new_outcome = oc1.selectbox(
            "Outcome", db.OUTCOME_OPTIONS,
            index=db.OUTCOME_OPTIONS.index(iv["outcome"] or "pending"),
            key=f"iv_out_{iv['id']}",
            label_visibility="collapsed",
        )
        if oc2.button("Update", key=f"iv_upd_{iv['id']}"):
            db.update_interview(conn, iv["id"], outcome=new_outcome)
            if new_outcome == "passed":
                db.add_event(conn, app["id"], "interview_complete",
                             date.today().isoformat(),
                             title=f"Round {iv['round_number']} passed")
            elif new_outcome == "failed":
                db.add_event(conn, app["id"], "interview_complete",
                             date.today().isoformat(),
                             title=f"Round {iv['round_number']} — did not advance")
            st.rerun()

        dc1, _ = st.columns([1, 8])
        if dc1.button("🗑", key=f"del_iv_{iv['id']}", help="Delete"):
            db.delete_interview(conn, iv["id"])
            st.rerun()
        st.markdown('<hr style="margin:6px 0;border-color:#374151">', unsafe_allow_html=True)


# ── Contacts tab ───────────────────────────────────────────────────────────────

def _render_contacts(conn, app) -> None:
    contacts = db.get_contacts(conn, app["id"])

    with st.expander("➕ Add contact", expanded=False):
        with st.form(f"add_co_{app['id']}"):
            cc1, cc2 = st.columns(2)
            cname    = cc1.text_input("Name *")
            ctitle   = cc2.text_input("Title")
            cc3, cc4 = st.columns(2)
            cemail   = cc3.text_input("Email")
            cphone   = cc4.text_input("Phone")
            cc5, cc6 = st.columns(2)
            crole    = cc5.selectbox("Role in Process", db.CONTACT_ROLES)
            clinkedin = cc6.text_input("LinkedIn URL")
            cnotes   = st.text_area("Notes", height=60)
            if st.form_submit_button("Add Contact", type="primary"):
                if not cname.strip():
                    st.error("Name is required.")
                else:
                    db.add_contact(
                        conn, app["id"],
                        name            = cname.strip(),
                        title           = ctitle.strip() or None,
                        email           = cemail.strip() or None,
                        phone           = cphone.strip() or None,
                        role_in_process = crole,
                        linkedin_url    = clinkedin.strip() or None,
                        notes           = cnotes.strip() or None,
                    )
                    st.rerun()

    st.divider()

    if not contacts:
        st.caption("No contacts yet.")
        return

    for co in contacts:
        role_color = {"recruiter": "#3b82f6", "hiring_manager": "#f59e0b",
                      "interviewer": "#8b5cf6", "referral": "#10b981"}.get(
            co["role_in_process"] or "", "#6b7280"
        )
        st.markdown(
            f"**{co['name']}**"
            + (f" — {co['title']}" if co["title"] else "")
            + f' <span style="color:{role_color};font-size:.8rem">{co["role_in_process"] or ""}</span>',
            unsafe_allow_html=True,
        )
        links = []
        if co["email"]:
            links.append(f"✉️ {co['email']}")
        if co["phone"]:
            links.append(f"📞 {co['phone']}")
        if co["linkedin_url"]:
            links.append(f"[LinkedIn ↗]({co['linkedin_url']})")
        if links:
            st.caption("  |  ".join(links))
        if co["notes"]:
            st.caption(co["notes"])
        dc1, _ = st.columns([1, 8])
        if dc1.button("🗑", key=f"del_co_{co['id']}", help="Delete"):
            db.delete_contact(conn, co["id"])
            st.rerun()
        st.markdown('<hr style="margin:4px 0;border-color:#374151">', unsafe_allow_html=True)


# ── Edit form ──────────────────────────────────────────────────────────────────

def _render_edit_form(conn, app) -> None:
    # Parse stored follow_up_date ISO string → date object for date_input
    _fu_date = None
    if app["follow_up_date"]:
        try:
            _fu_date = date.fromisoformat(app["follow_up_date"][:10])
        except Exception:
            pass

    _current_entry_type = app["entry_type"] if app["entry_type"] else "application"

    with st.form(f"edit_app_{app['id']}"):
        et1, et2 = st.columns(2)
        _type_options = ["Application", "Opportunity", "Job Fair"]
        _type_idx     = {"application": 0, "opportunity": 1, "job_fair": 2}.get(_current_entry_type, 0)
        entry_type_label = et1.radio(
            "Type",
            _type_options,
            index=_type_idx,
            horizontal=True,
            key=f"edit_entry_type_{app['id']}",
        )

        ec1, ec2 = st.columns(2)
        company  = ec1.text_input("Company", value=app["company"] or "")
        role     = ec2.text_input("Role",    value=app["role"]    or "")

        ea1, ea2, ea3 = st.columns(3)
        status_idx = db.STATUSES.index(app["status"]) if app["status"] in db.STATUSES else 0
        status   = ea1.selectbox("Status", db.STATUSES, index=status_idx)
        fit_opts = ["—", "1", "2", "3", "4", "5"]
        fit_idx  = fit_opts.index(str(app["fit_stars"])) if str(app["fit_stars"]) in fit_opts else 0
        fit      = ea2.selectbox("Fit", fit_opts, index=fit_idx)
        job_url  = ea3.text_input("Job URL", value=app["job_url"] or "")

        eb1, eb2 = st.columns(2)
        sal_low  = eb1.number_input("Salary Low ($)",  value=int(app["salary_low"]  or 0), step=5000)
        sal_high = eb2.number_input("Salary High ($)", value=int(app["salary_high"] or 0), step=5000)

        referral   = st.text_input("Referral", value=app["referral"] or "")
        jd_summary = st.text_area("JD Summary", value=app["jd_summary"] or "", height=100)
        notes      = st.text_area("Notes", value=app["notes"] or "", height=80)

        st.markdown("**Follow-up**")
        rf1, rf2 = st.columns(2)
        follow_up_date  = rf1.date_input("Follow-up date", value=_fu_date)
        follow_up_notes = rf2.text_input("Who / what to say",
                                         value=app["follow_up_notes"] or "",
                                         placeholder="e.g. Email Sarah the recruiter")

        st.markdown("**Documents**")
        resume_version     = st.text_input("Resume version",
                                            value=app["resume_version"] or "",
                                            placeholder="e.g. PM resume v3 – fintech tailored")
        cover_letter_notes = st.text_input("Cover letter notes",
                                            value=app["cover_letter_notes"] or "",
                                            placeholder="e.g. Tailored intro, emphasized API PM exp")

        sc1, sc2 = st.columns([2, 1])
        saved   = sc1.form_submit_button("💾 Save", type="primary")
        deleted = sc2.form_submit_button("🗑 Delete Application")

    if saved:
        db.update_application(
            conn, app["id"],
            entry_type         = {"Opportunity": "opportunity", "Job Fair": "job_fair"}.get(entry_type_label, "application"),
            company            = company.strip(),
            role               = role.strip(),
            status             = status,
            fit_stars          = int(fit) if fit != "—" else None,
            job_url            = job_url.strip() or None,
            salary_low         = sal_low  or None,
            salary_high        = sal_high or None,
            salary_range       = f"${sal_low:,}–${sal_high:,}" if sal_low and sal_high else None,
            referral           = referral.strip() or None,
            jd_summary         = jd_summary.strip() or None,
            notes              = notes.strip() or None,
            follow_up_date     = follow_up_date.isoformat() if follow_up_date else None,
            follow_up_notes    = follow_up_notes.strip() or None,
            resume_version     = resume_version.strip() or None,
            cover_letter_notes = cover_letter_notes.strip() or None,
        )
        st.success("Saved.")
        st.rerun()

    if deleted:
        db.delete_application(conn, app["id"])
        st.session_state["tracker_selected_id"] = None
        st.rerun()
