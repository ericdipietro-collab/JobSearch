"""
Application Tracker — mini CRM view.
Renders inside the main app.py navigation via render_tracker(conn).
"""
from __future__ import annotations

from datetime import date, datetime
from typing import Optional

import pandas as pd
import streamlit as st

from jobsearch import ats_db as db

FORMAL_TRACKER_EXCLUDED_STATUSES = {"considering"}

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


def _formal_tracker_rows(rows):
    return [
        row
        for row in rows
        if str(row["status"]).lower() not in FORMAL_TRACKER_EXCLUDED_STATUSES
    ]


def _summary_metrics_for_rows(rows):
    counts = {status: 0 for status in db.STATUSES}
    for row in rows:
        status = str(row["status"]).lower()
        counts[status] = counts.get(status, 0) + 1
    total = len(rows)
    active = sum(counts.get(status, 0) for status in ("applied", "screening", "interviewing", "offer"))
    return {
        "total": total,
        "active": active,
        "interviewing": counts.get("interviewing", 0),
        "offers": counts.get("offer", 0),
        "accepted": counts.get("accepted", 0),
        "rejected": counts.get("rejected", 0),
    }


# ── LinkedIn CSV import ─────────────────────────────────────────────────────────

_LI_COLUMN_MAP = {
    # LinkedIn "Applied Jobs" export column names → our field names
    "Company Name":   "company",
    "Company":        "company",
    "Job Title":      "role",
    "Title":          "role",
    "Position":       "role",
    "Job URL":        "job_url",
    "URL":            "job_url",
    "Applied At":     "date_applied",
    "Date Applied":   "date_applied",
    "Application Date": "date_applied",
    "Status":         "li_status",
}


def _render_linkedin_import(conn) -> None:
    with st.expander("📥 Import from LinkedIn CSV", expanded=False):
        st.caption(
            "Export your Applied Jobs from LinkedIn: go to **My Jobs → Applied** → "
            "click the export/download icon. Upload the CSV here."
        )
        uploaded = st.file_uploader("Choose LinkedIn CSV", type="csv",
                                     key="li_import_uploader", label_visibility="collapsed")
        if not uploaded:
            return

        try:
            li_df = pd.read_csv(uploaded, dtype=str).fillna("")
        except Exception as e:
            st.error(f"Could not read CSV: {e}")
            return

        # Map column names
        col_rename = {}
        for orig, mapped in _LI_COLUMN_MAP.items():
            if orig in li_df.columns:
                col_rename[orig] = mapped
        li_df = li_df.rename(columns=col_rename)

        if "company" not in li_df.columns or "role" not in li_df.columns:
            st.error(
                "Could not find Company and Job Title columns. "
                f"Found columns: {list(li_df.columns)}"
            )
            return

        # Deduplicate against existing applications
        existing = {
            (r["company"].lower(), r["role"].lower())
            for r in conn.execute("SELECT company, role FROM applications").fetchall()
        }

        new_rows = []
        for _, row in li_df.iterrows():
            co = str(row.get("company") or "").strip()
            ro = str(row.get("role") or "").strip()
            if co and ro and (co.lower(), ro.lower()) not in existing:
                new_rows.append(row)

        if not new_rows:
            st.info("All entries in this CSV are already in your tracker.")
            return

        preview_df = pd.DataFrame(new_rows)[
            [c for c in ["company", "role", "date_applied", "job_url", "li_status"]
             if c in pd.DataFrame(new_rows).columns]
        ]
        st.markdown(f"**{len(new_rows)} new application(s) to import:**")
        st.dataframe(preview_df, hide_index=True, use_container_width=True)

        if st.button(f"Import {len(new_rows)} application(s)", type="primary",
                     key="li_import_confirm"):
            imported = 0
            for row in new_rows:
                co = str(row.get("company") or "").strip()
                ro = str(row.get("role") or "").strip()
                url = str(row.get("job_url") or "").strip() or None
                raw_date = str(row.get("date_applied") or "").strip()

                # Best-effort date parse — strip any time component first
                app_date = None
                from datetime import datetime as _dt
                _d = raw_date.split("T")[0].strip() if "T" in raw_date else raw_date.strip()
                for fmt in ("%Y-%m-%d", "%m/%d/%Y", "%B %d, %Y", "%b %d, %Y"):
                    try:
                        app_date = _dt.strptime(_d, fmt).date().isoformat()
                        break
                    except Exception:
                        continue

                app_id = db.add_application(
                    conn,
                    company      = co,
                    role         = ro,
                    job_url      = url,
                    source       = "linkedin_import",
                    status       = "applied",
                    entry_type   = "application",
                    date_applied = app_date,
                )
                if app_date:
                    db.add_event(conn, app_id, "applied", app_date,
                                 title=f"Applied to {co} (LinkedIn import)")
                imported += 1

            st.success(f"Imported {imported} application(s) from LinkedIn.")
            st.rerun()


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
    _render_linkedin_import(conn)

    offer_apps = db.get_applications_with_offers(conn)
    if len(offer_apps) >= 2:
        with st.expander(f"⚖️ Compare {len(offer_apps)} Offers", expanded=False):
            _render_offer_comparison(offer_apps)

    st.divider()

    # ── Filters + Add button ──────────────────────────────────────────────────
    fc1, fc2, fc3, fc4, fc5 = st.columns([2, 2, 2, 1, 1])
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
    with fc5:
        bulk_mode = st.toggle("Bulk", key="tracker_bulk_mode",
                               help="Switch to multi-select mode for bulk status changes or deletes")
        if not bulk_mode:
            st.session_state["tracker_bulk_selected_ids"] = []

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
    # Filter out scraper-only 'considering' matches from the formal tracker.
    apps = _formal_tracker_rows(apps)
    if search:
        q = search.lower()
        apps = [a for a in apps if q in a["company"].lower() or q in a["role"].lower()]

    if not apps:
        st.info("No applications match the current filter.")
        return

    # ── Build table DataFrame ─────────────────────────────────────────────────
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

    # Export CSV (always visible, reflects current filter)
    csv_bytes = df_tbl.drop(columns=["_id"]).to_csv(index=False).encode("utf-8")
    st.download_button(
        label="⬇️ Export CSV",
        data=csv_bytes,
        file_name=f"applications_{date.today().isoformat()}.csv",
        mime="text/csv",
        key="export_csv_btn",
    )

    _col_config = {
        "Type":      st.column_config.TextColumn("Type",      width="small"),
        "Company":   st.column_config.TextColumn("Company",   width="medium"),
        "Role":      st.column_config.TextColumn("Role",      width="large"),
        "Status":    st.column_config.TextColumn("Status",    width="medium"),
        "Applied":   st.column_config.TextColumn("Applied",   width="small"),
        "Fit":       st.column_config.TextColumn("Fit",       width="small"),
        "Follow-up": st.column_config.TextColumn("Follow-up", width="medium"),
    }

    bulk_mode = st.session_state.get("tracker_bulk_mode", False)

    if bulk_mode:
        # ── Multi-row bulk select ─────────────────────────────────────────────
        event = st.dataframe(
            df_tbl.drop(columns=["_id"]),
            on_select="rerun",
            selection_mode="multi-row",
            hide_index=True,
            use_container_width=True,
            column_config=_col_config,
        )
        selected_ids = [int(df_tbl.iloc[i]["_id"]) for i in event.selection.rows]
        st.session_state["tracker_bulk_selected_ids"] = selected_ids

        if selected_ids:
            st.caption(f"{len(selected_ids)} row{'s' if len(selected_ids) != 1 else ''} selected")
            ba1, ba2, ba3, _ = st.columns([3, 2, 2, 3])
            new_status = ba1.selectbox("Change status to", db.STATUSES,
                                        key="bulk_status_pick", label_visibility="collapsed")
            if ba2.button(f"Update {len(selected_ids)}", type="primary", key="bulk_update_btn"):
                db.bulk_update_status(conn, selected_ids, new_status)
                st.toast(f"Updated {len(selected_ids)} application(s) to '{new_status}'")
                st.rerun()
            if ba3.button("🗑 Delete selected", key="bulk_delete_btn"):
                db.bulk_delete_applications(conn, selected_ids)
                st.session_state["tracker_bulk_selected_ids"] = []
                st.toast(f"Deleted {len(selected_ids)} application(s)", icon="🗑️")
                st.rerun()
    else:
        # ── Single-row click → detail ─────────────────────────────────────────
        event = st.dataframe(
            df_tbl.drop(columns=["_id"]),
            on_select="rerun",
            selection_mode="single-row",
            hide_index=True,
            use_container_width=True,
            column_config=_col_config,
        )
        if event.selection.rows:
            sel_id = int(df_tbl.iloc[event.selection.rows[0]]["_id"])
            st.session_state["tracker_selected_id"] = sel_id
            st.session_state["tracker_show_add_form"] = False

    # ── Inline detail panel (single-row mode only) ────────────────────────────
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

def _contact_action_links(app) -> str:
    """Return HTML links for email and LinkedIn from the first contact on an app row."""
    parts = []
    email = app["first_contact_email"] if "first_contact_email" in app.keys() else None
    linkedin = app["first_contact_linkedin"] if "first_contact_linkedin" in app.keys() else None
    if email:
        parts.append(f'<a href="mailto:{email}">✉️ Email</a>')
    if linkedin:
        parts.append(f'<a href="{linkedin}" target="_blank">🔗 LinkedIn</a>')
    return "  |  ".join(parts)


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
                contact_links = _contact_action_links(app)
                col1, col2 = st.columns([5, 1])
                col1.markdown(
                    f"{label}  \n"
                    f"<span style='color:#f87171'>Due {app['follow_up_date']}"
                    f" ({days_ago} day{'s' if days_ago!=1 else ''} ago)</span>"
                    + (f"  \n👤 {contacts}" if contacts else "")
                    + (f"  \n{contact_links}" if contact_links else "")
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
                contact_links = _contact_action_links(app)
                st.markdown(
                    f"**{app['company']}** — {app['role']}  \n"
                    f"<span style='color:#fbbf24'>Due {app['follow_up_date']}"
                    f" (in {days_left} day{'s' if days_left!=1 else ''})</span>"
                    + (f"  \n👤 {contacts}" if contacts else "")
                    + (f"  \n{contact_links}" if contact_links else ""),
                    unsafe_allow_html=True,
                )


# ── Summary bar ────────────────────────────────────────────────────────────────

def _render_summary_bar(conn) -> None:
    metrics = _summary_metrics_for_rows(_formal_tracker_rows(db.get_applications(conn)))

    cols = st.columns(6)
    cards = [
        ("Total", metrics["total"], None),
        ("Active", metrics["active"], None),
        ("Interviewing", metrics["interviewing"], None),
        ("Offers", metrics["offers"], None),
        ("Accepted", metrics["accepted"], None),
        ("Rejected", metrics["rejected"], None),
    ]
    for col, (label, val, delta) in zip(cols, cards):
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
        status   = r2c1.selectbox(
            "Status", db.STATUSES, index=default_status_idx,
            help="Pipeline stage: exploring → applied → screening → interviewing → offer → accepted/rejected/withdrawn",
        )
        fit      = r2c2.selectbox(
            "Fit", ["—", "1", "2", "3", "4", "5"],
            help="Your personal excitement/fit score: 1 = low interest, 5 = dream role",
        )
        date_app = r2c3.date_input(
            "Event Date" if is_job_fair else ("Date of first contact" if is_opp else "Date Applied"),
            value=date.today(),
        )

        if not is_opp and not is_job_fair:
            job_url = st.text_input("Job URL")
        else:
            job_url = ""

        sal_low, sal_high = st.columns(2)
        s_low  = sal_low.number_input("Salary Low ($)", value=0, step=5000,
                                       help="Bottom of the posted salary range (0 = not posted)")
        s_high = sal_high.number_input("Salary High ($)", value=0, step=5000,
                                        help="Top of the posted salary range (0 = not posted)")
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
            da1, da2 = st.columns(2)
            resume_url       = da1.text_input("Resume URL", placeholder="e.g. link to Google Doc")
            cover_letter_url = da2.text_input("Cover Letter URL", placeholder="e.g. link to Google Doc")
        else:
            resume_version = cover_letter_notes = ""
            resume_url = cover_letter_url = ""

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
                    resume_url         = resume_url.strip() or None,
                    cover_letter_url   = cover_letter_url.strip() or None,
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
    # Quick links row
    link_parts = []
    if app["job_url"]:
        link_parts.append(f"[Job Posting ↗]({app['job_url']})")
    if app["resume_url"]:
        link_parts.append(f"[Resume ↗]({app['resume_url']})")
    if app["cover_letter_url"]:
        link_parts.append(f"[Cover Letter ↗]({app['cover_letter_url']})")
    if link_parts:
        st.markdown("  |  ".join(link_parts))

    if app["job_description"]:
        with st.expander("📄 Full Job Description"):
            st.markdown(app["job_description"])

    tab_tl, tab_iv, tab_co, tab_pr, tab_ng, tab_ed = st.tabs(
        ["Timeline", "Interviews", "Contacts", "Prep", "Negotiate", "Edit"]
    )

    with tab_tl:
        _render_timeline(conn, app)

    with tab_iv:
        _render_interviews(conn, app)

    with tab_co:
        _render_contacts(conn, app)

    with tab_pr:
        _render_prep_tab(conn, app)

    with tab_ng:
        _render_negotiate_tab(conn, app)

    with tab_ed:
        _render_edit_form(conn, app)

    # ── Company profile (below tabs) ──────────────────────────────────────────
    _render_inline_company_profile(conn, app["company"])


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


# ── Prep tab ───────────────────────────────────────────────────────────────────

def _render_prep_tab(conn, app) -> None:
    st.caption("Use this tab to prep for interviews. Notes are saved per application.")
    with st.form(f"prep_form_{app['id']}"):
        prep_company = st.text_area(
            "Company Research",
            value=app["prep_company"] or "",
            height=100,
            placeholder="Products, business model, recent news, competitors, culture signals…",
        )
        prep_why = st.text_area(
            "Why This Role / Why This Company",
            value=app["prep_why"] or "",
            height=80,
            placeholder="What excites you about this role specifically?",
        )
        prep_tyabt = st.text_area(
            "Tell Me About Yourself (TMAY)",
            value=app["prep_tyabt"] or "",
            height=100,
            placeholder="Your tailored 90-second pitch for this role…",
        )
        prep_questions = st.text_area(
            "Questions to Ask",
            value=app["prep_questions"] or "",
            height=80,
            placeholder="What does success look like in 90 days?\nWhat's the biggest challenge the team is facing?",
        )
        prep_notes = st.text_area(
            "Other Prep Notes",
            value=app["prep_notes"] or "",
            height=80,
            placeholder="STAR stories, key talking points, things to avoid…",
        )
        if st.form_submit_button("💾 Save Prep Notes", type="primary"):
            db.update_application(
                conn, app["id"],
                prep_company   = prep_company.strip() or None,
                prep_why       = prep_why.strip() or None,
                prep_tyabt     = prep_tyabt.strip() or None,
                prep_questions = prep_questions.strip() or None,
                prep_notes     = prep_notes.strip() or None,
            )
            st.success("Prep notes saved.")
            st.rerun()


# ── Negotiate tab ──────────────────────────────────────────────────────────────

def _render_negotiate_tab(conn, app) -> None:
    offer_base = app["offer_base"] or 0
    st.caption("Use this worksheet to plan your counter-offer before negotiating.")

    with st.form(f"nego_form_{app['id']}"):
        nc1, nc2 = st.columns(2)
        target_base   = nc1.number_input("Your Target Base ($)", step=5000,
                                          value=int(app["nego_target_base"] or 0),
                                          help="The salary you'll ask for")
        walkaway_base = nc2.number_input("Walk-Away Base ($)", step=5000,
                                          value=int(app["nego_walkaway_base"] or 0),
                                          help="The lowest you'll accept")
        nm1, nm2 = st.columns(2)
        market_low  = nm1.number_input("Market Low ($)",  step=5000,
                                        value=int(app["nego_market_low"] or 0),
                                        help="From Levels.fyi, Glassdoor, LinkedIn Salary, etc.")
        market_high = nm2.number_input("Market High ($)", step=5000,
                                        value=int(app["nego_market_high"] or 0))
        nego_notes  = st.text_area("Talking Points / Notes",
                                    value=app["nego_notes"] or "", height=100,
                                    placeholder="BATNA, competing offers, reasons you deserve more…")
        if st.form_submit_button("💾 Save Worksheet", type="primary"):
            db.update_application(
                conn, app["id"],
                nego_target_base   = target_base or None,
                nego_walkaway_base = walkaway_base or None,
                nego_market_low    = market_low or None,
                nego_market_high   = market_high or None,
                nego_notes         = nego_notes.strip() or None,
            )
            st.success("Saved.")
            st.rerun()

    # ── Live calculation ──────────────────────────────────────────────────────
    if offer_base or target_base:
        st.divider()
        st.markdown("**Quick math**")
        rows = []
        if offer_base:
            rows.append(("Current offer", f"${offer_base:,}"))
        if target_base and offer_base:
            diff = target_base - offer_base
            pct  = diff / offer_base * 100
            rows.append(("Counter-offer ask", f"${target_base:,}  (+${diff:,} / +{pct:.1f}%)"))
        if walkaway_base and offer_base:
            diff = walkaway_base - offer_base
            pct  = diff / offer_base * 100
            sign = "+" if diff >= 0 else ""
            rows.append(("Walk-away floor", f"${walkaway_base:,}  ({sign}${diff:,} / {sign}{pct:.1f}%)"))
        if market_low and market_high:
            rows.append(("Market range", f"${market_low:,} – ${market_high:,}"))
        if offer_base and market_low and market_high:
            mid = (market_low + market_high) // 2
            vs_mid = offer_base - mid
            sign = "+" if vs_mid >= 0 else ""
            rows.append(("Offer vs market midpoint", f"{sign}${vs_mid:,}"))
        for label, val in rows:
            c1, c2 = st.columns([2, 3])
            c1.caption(label)
            c2.markdown(val)


# ── Inline company profile ──────────────────────────────────────────────────────

def _render_inline_company_profile(conn, company_name: str) -> None:
    """Show a collapsible company profile panel below the application detail tabs."""
    profile = db.get_company_profile(conn, company_name)
    label   = f"🏢 {company_name} — Company Profile" + (" ✏️" if profile else " (no profile yet)")
    with st.expander(label, expanded=False):
        from jobsearch.views.company_profiles_page import _render_profile_form
        _render_profile_form(conn, profile=profile if profile else None)
        if not profile:
            st.caption(f"This will create a new profile for **{company_name}** shared across all applications to this company.")


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
        du1, du2 = st.columns(2)
        resume_url       = du1.text_input("Resume URL",
                                          value=app["resume_url"] or "",
                                          placeholder="e.g. link to Google Doc or Dropbox")
        cover_letter_url = du2.text_input("Cover Letter URL",
                                          value=app["cover_letter_url"] or "",
                                          placeholder="e.g. link to Google Doc")
        job_description  = st.text_area("Full Job Description",
                                         value=app["job_description"] or "",
                                         height=120,
                                         placeholder="Paste the full JD here for reference and future search")

        st.markdown("**Offer Details**")
        of1, of2, of3 = st.columns(3)
        offer_base      = of1.number_input("Base Salary ($)", value=int(app["offer_base"] or 0), step=5000)
        offer_bonus_pct = of2.number_input("Bonus (%)", value=int(app["offer_bonus_pct"] or 0), step=5)
        offer_signing   = of3.number_input("Signing Bonus ($)", value=int(app["offer_signing"] or 0), step=1000)
        of4, of5, of6 = st.columns(3)
        offer_pto_days    = of4.number_input("PTO Days", value=int(app["offer_pto_days"] or 0), step=1)
        offer_k401_match  = of5.text_input("401k Match", value=app["offer_k401_match"] or "", placeholder="e.g. 4% match")
        offer_equity      = of6.text_input("Equity", value=app["offer_equity"] or "", placeholder="e.g. $50k RSU over 4yr")
        remote_opts = ["", "Remote", "Hybrid", "Onsite"]
        remote_idx  = remote_opts.index(app["offer_remote_policy"]) if app["offer_remote_policy"] in remote_opts else 0
        offer_remote_policy = st.selectbox("Remote Policy", remote_opts, index=remote_idx)

        _offer_start_date  = None
        _offer_expiry_date = None
        if app["offer_start_date"]:
            try:
                _offer_start_date = date.fromisoformat(app["offer_start_date"][:10])
            except Exception:
                pass
        if app["offer_expiry_date"]:
            try:
                _offer_expiry_date = date.fromisoformat(app["offer_expiry_date"][:10])
            except Exception:
                pass
        od1, od2 = st.columns(2)
        offer_start_date  = od1.date_input("Start Date",  value=_offer_start_date)
        offer_expiry_date = od2.date_input("Offer Expires", value=_offer_expiry_date)
        offer_notes = st.text_area("Offer Notes", value=app["offer_notes"] or "", height=60,
                                   placeholder="Benefits details, equity vesting schedule, negotiation notes…")

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
            resume_url         = resume_url.strip() or None,
            cover_letter_url   = cover_letter_url.strip() or None,
            job_description    = job_description.strip() or None,
            offer_base         = offer_base or None,
            offer_bonus_pct    = offer_bonus_pct or None,
            offer_signing      = offer_signing or None,
            offer_pto_days     = offer_pto_days or None,
            offer_k401_match   = offer_k401_match.strip() or None,
            offer_equity       = offer_equity.strip() or None,
            offer_remote_policy = offer_remote_policy or None,
            offer_start_date   = offer_start_date.isoformat() if offer_start_date else None,
            offer_expiry_date  = offer_expiry_date.isoformat() if offer_expiry_date else None,
            offer_notes        = offer_notes.strip() or None,
        )
        st.success("Saved.")
        st.rerun()

    if deleted:
        db.delete_application(conn, app["id"])
        st.session_state["tracker_selected_id"] = None
        st.rerun()


# ── Offer comparison ────────────────────────────────────────────────────────────

def _render_offer_comparison(offer_apps) -> None:
    """Side-by-side comparison of offers."""
    if len(offer_apps) < 2:
        return

    rows = []
    for a in offer_apps:
        base = a["offer_base"] or a["salary_low"] or 0
        bonus = a["offer_bonus_pct"] or 0
        total = int(base * (1 + bonus / 100)) if base else 0
        rows.append({
            "Company":        a["company"],
            "Role":           a["role"],
            "Base ($)":       f"${base:,}" if base else "—",
            "Bonus":          f"{bonus}%" if bonus else "—",
            "Total ($)":      f"${total:,}" if total else "—",
            "Signing ($)":    f"${a['offer_signing']:,}" if a["offer_signing"] else "—",
            "Equity":         a["offer_equity"] or "—",
            "PTO Days":       str(a["offer_pto_days"]) if a["offer_pto_days"] else "—",
            "401k":           a["offer_k401_match"] or "—",
            "Remote Policy":  a["offer_remote_policy"] or "—",
            "Start Date":     a["offer_start_date"] or "—",
            "Expires":        a["offer_expiry_date"] or "—",
            "Notes":          a["offer_notes"] or "",
        })

    df = pd.DataFrame(rows).set_index("Company")
    st.dataframe(df.T, use_container_width=True)
