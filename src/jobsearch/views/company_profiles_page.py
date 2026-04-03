"""
Company Research Profiles — persistent research notes per company,
shared across all applications to that company.
"""
from __future__ import annotations

from datetime import date

import pandas as pd
import streamlit as st

from jobsearch import ats_db as db


def render_company_profiles(conn) -> None:
    db.init_db(conn)

    st.markdown(
        "Research notes that persist across all applications to the same company. "
        "Add a profile here once — it will appear automatically when you open any application to that company."
    )

    network_map = db.get_company_network_map(conn)
    if network_map:
        st.markdown("**Network map**")
        map_df = pd.DataFrame(
            [
                {
                    "Company": row["company"],
                    "Contacts": row["contacts"],
                    "Reached Out": row["reached_out"],
                    "Referrals": row["referrals"],
                    "Due": row["follow_up_due"],
                    "Next Follow-up": row["next_follow_up"] or "—",
                }
                for row in network_map
            ]
        )
        st.dataframe(map_df, hide_index=True, use_container_width=True)
        st.caption("Use this to see which target companies have warm contacts, referrals, and overdue networking follow-ups.")
        st.divider()

    # ── Search + Add ───────────────────────────────────────────────────────────
    sc1, sc2 = st.columns([3, 1])
    search = sc1.text_input("Search companies", placeholder="Company name…",
                             label_visibility="collapsed", key="cp_search")
    if sc2.button("➕ New Profile", use_container_width=True):
        st.session_state["cp_show_add"] = not st.session_state.get("cp_show_add", False)
        st.session_state["cp_selected_name"] = None

    if st.session_state.get("cp_show_add"):
        st.divider()
        st.markdown("**New Company Profile**")
        _render_profile_form(conn, profile=None)
        st.divider()

    # ── Profile list ──────────────────────────────────────────────────────────
    profiles = db.get_all_company_profiles(conn, search=search or None)

    if not profiles:
        if search:
            st.info(f'No profiles found for "{search}".')
        else:
            st.info("No company profiles yet. Add one above.")
        return

    for p in profiles:
        # Completeness indicator
        filled = sum(1 for f in ("about", "culture_notes", "interview_process", "red_flags")
                     if p[f])
        badge = "🟢" if filled >= 3 else ("🟡" if filled >= 1 else "⚪")
        label = f"{badge} {p['name']}"
        if p["glassdoor_url"] or p["linkedin_url"] or p["website_url"]:
            label += "  🔗"

        with st.expander(label, expanded=False):
            _render_profile_detail(conn, p)


def _render_profile_detail(conn, profile) -> None:
    # Quick links
    links = []
    if profile["website_url"]:
        links.append(f"[Website ↗]({profile['website_url']})")
    if profile["linkedin_url"]:
        links.append(f"[LinkedIn ↗]({profile['linkedin_url']})")
    if profile["glassdoor_url"]:
        links.append(f"[Glassdoor ↗]({profile['glassdoor_url']})")
    if links:
        st.markdown("  |  ".join(links))

    summary = db.get_network_summary_for_company(conn, profile["name"])
    mc1, mc2, mc3, mc4 = st.columns(4)
    mc1.metric("Contacts", summary["contacts"])
    mc2.metric("Reached Out", summary["reached_out"])
    mc3.metric("Referrals", summary["referrals"])
    mc4.metric("Follow-up Due", summary["follow_up_due"])

    related_contacts = db.get_network_contacts_for_company(conn, profile["name"])
    if related_contacts:
        with st.expander(f"🤝 {len(related_contacts)} linked contact(s)", expanded=False):
            for contact in related_contacts:
                st.markdown(
                    f"**{contact['name']}**"
                    + (f" — {contact['title']}" if contact["title"] else "")
                    + (f"  \n_{contact['relationship']}_" if contact["relationship"] else "")
                    + (f"  \nLast contact: {contact['last_contact_date']}" if contact["last_contact_date"] else "")
                    + (f"  \nFollow-up: {contact['follow_up_date']}" if contact["follow_up_date"] else ""),
                )
                if contact["notes"]:
                    st.caption(contact["notes"])
                st.markdown('<hr style="margin:4px 0;border-color:#374151">', unsafe_allow_html=True)
    else:
        st.caption("No networking contacts linked to this company yet.")

    with st.expander("➕ Add linked networking contact", expanded=False):
        _render_company_contact_form(conn, profile["name"])

    _render_profile_form(conn, profile=profile)


def _render_profile_form(conn, profile) -> None:
    is_edit = profile is not None
    form_key = f"cp_form_{'edit_' + str(profile['id']) if is_edit else 'add'}"

    with st.form(form_key):
        if not is_edit:
            name = st.text_input("Company Name *")
        else:
            name = profile["name"]
            st.markdown(f"**{name}**")

        url1, url2, url3 = st.columns(3)
        website  = url1.text_input("Website", value=profile["website_url"] if is_edit else "")
        linkedin = url2.text_input("LinkedIn URL", value=profile["linkedin_url"] if is_edit else "")
        glassdoor = url3.text_input("Glassdoor URL", value=profile["glassdoor_url"] if is_edit else "")

        about = st.text_area(
            "About the Company",
            value=profile["about"] if is_edit else "",
            height=90,
            placeholder="Products, business model, size, funding stage, recent news…",
            help="General company background — pulled from their website, LinkedIn, or Crunchbase.",
        )
        culture = st.text_area(
            "Culture & Values",
            value=profile["culture_notes"] if is_edit else "",
            height=80,
            placeholder="Glassdoor themes, employee reviews, known culture signals…",
            help="What it's actually like to work there — Glassdoor reviews, LinkedIn posts by employees, interview feedback.",
        )
        interview_process = st.text_area(
            "Interview Process",
            value=profile["interview_process"] if is_edit else "",
            height=80,
            placeholder="Number of rounds, formats, timelines, what interviewers care about…",
            help="Rounds, formats (behavioral / technical / take-home), typical timeline. Check Glassdoor interviews or ask your recruiter.",
        )
        red_flags = st.text_area(
            "Red Flags / Watch Out For",
            value=profile["red_flags"] if is_edit else "",
            height=70,
            placeholder="High churn, leadership issues, slow hiring, unrealistic expectations…",
            help="Anything that makes you cautious — high turnover, Glassdoor warns, slow offer process, bait-and-switch titles.",
        )

        if is_edit:
            sc1, sc2 = st.columns([2, 1])
            saved   = sc1.form_submit_button("💾 Save", type="primary")
            deleted = sc2.form_submit_button("🗑 Delete Profile")
        else:
            saved   = st.form_submit_button("Save Profile", type="primary")
            deleted = False

    if saved:
        if not is_edit and not name.strip():
            st.error("Company name is required.")
        else:
            db.upsert_company_profile(
                conn,
                name         = name.strip() if not is_edit else profile["name"],
                website_url  = website.strip() or None,
                linkedin_url = linkedin.strip() or None,
                glassdoor_url = glassdoor.strip() or None,
                about        = about.strip() or None,
                culture_notes = culture.strip() or None,
                interview_process = interview_process.strip() or None,
                red_flags    = red_flags.strip() or None,
            )
            if not is_edit:
                st.session_state["cp_show_add"] = False
            st.success("Saved.")
            st.rerun()

    if deleted:
        db.delete_company_profile(conn, profile["id"])
        st.rerun()


def _render_company_contact_form(conn, company_name: str) -> None:
    with st.form(f"company_contact_{company_name}"):
        c1, c2 = st.columns(2)
        name = c1.text_input("Name *")
        title = c2.text_input("Title")
        c3, c4 = st.columns(2)
        relationship = c3.selectbox("Relationship", [""] + db.NETWORK_RELATIONSHIPS)
        email = c4.text_input("Email")
        c5, c6 = st.columns(2)
        linkedin = c5.text_input("LinkedIn URL")
        follow_up_date = c6.date_input("Follow-up date", value=None)
        notes = st.text_area(
            "Notes",
            height=70,
            placeholder=f"Why this person matters at {company_name}, what you plan to ask, referral context…",
        )
        if st.form_submit_button("Add Contact", type="primary"):
            if not name.strip():
                st.error("Name is required.")
            else:
                db.add_network_contact(
                    conn,
                    name=name.strip(),
                    company=company_name,
                    title=title.strip() or None,
                    email=email.strip() or None,
                    linkedin_url=linkedin.strip() or None,
                    relationship=relationship or None,
                    follow_up_date=follow_up_date.isoformat() if follow_up_date else None,
                    last_contact_date=date.today().isoformat() if relationship == "referral" else None,
                    notes=notes.strip() or None,
                )
                st.success(f"Added contact for {company_name}.")
                st.rerun()
