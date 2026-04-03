"""
Networking Contacts — standalone contact book for your broader network.
Not tied to a specific application.
"""
from __future__ import annotations

from datetime import date

import pandas as pd
import streamlit as st

from jobsearch import ats_db as db

_REL_LABELS = {
    "former_colleague": "Former Colleague",
    "recruiter":        "Recruiter",
    "mentor":           "Mentor",
    "referral":         "Referral",
    "friend":           "Friend",
    "other":            "Other",
}


def render_contacts(conn) -> None:
    db.init_db(conn)

    # ── Follow-up reminders ────────────────────────────────────────────────────
    overdue = db.network_contacts_follow_up_due(conn)
    if overdue:
        with st.container():
            st.warning(f"**{len(overdue)} networking follow-up{'s' if len(overdue)>1 else ''} overdue**", icon="🔔")
            for c in overdue:
                try:
                    d_fmt = date.fromisoformat(c["follow_up_date"]).strftime("%b %d")
                except Exception:
                    d_fmt = c["follow_up_date"]
                links = []
                if c["email"]:
                    links.append(f'<a href="mailto:{c["email"]}">✉️ Email</a>')
                if c["linkedin_url"]:
                    links.append(f'<a href="{c["linkedin_url"]}" target="_blank">🔗 LinkedIn</a>')
                link_str = "  |  ".join(links)
                st.markdown(
                    f"**{c['name']}**"
                    + (f" — {c['company']}" if c["company"] else "")
                    + f"  \n<span style='color:#fbbf24'>Follow up by {d_fmt}</span>"
                    + (f"  \n{link_str}" if link_str else ""),
                    unsafe_allow_html=True,
                )
        st.divider()

    # ── Filters + Add button ───────────────────────────────────────────────────
    ff1, ff2, ff3, ff4 = st.columns([2, 2, 2, 2])
    with ff1:
        rel_opts = ["All relationships"] + db.NETWORK_RELATIONSHIPS
        sel_rel  = ff1.selectbox("Relationship", rel_opts,
                                  label_visibility="collapsed", key="contacts_rel_filter")
    with ff2:
        search = ff2.text_input("Search", placeholder="name or company…",
                                 label_visibility="collapsed", key="contacts_search")
    with ff3:
        companies = sorted({row["company"] for row in db.get_network_contacts(conn) if row["company"]})
        company_opts = ["All companies"] + companies
        sel_company = ff3.selectbox("Company", company_opts, label_visibility="collapsed", key="contacts_company_filter")
    with ff4:
        if st.button("➕ Add Contact", key="contacts_add_btn", use_container_width=True):
            st.session_state["contacts_show_add"] = not st.session_state.get("contacts_show_add", False)
            st.session_state["contacts_selected_id"] = None

    if st.session_state.get("contacts_show_add"):
        _render_contact_form(conn, existing=None)
        st.divider()

    # ── Load + filter ──────────────────────────────────────────────────────────
    contacts = db.get_network_contacts(
        conn,
        relationship = None if sel_rel == "All relationships" else sel_rel,
        search       = search or None,
    )
    if sel_company != "All companies":
        contacts = [contact for contact in contacts if (contact["company"] or "") == sel_company]

    if not contacts:
        st.info("No contacts yet. Add your first one above.")
        return

    # ── Table ──────────────────────────────────────────────────────────────────
    rows = []
    for c in contacts:
        fu = c["follow_up_date"] or ""
        try:
            days_left = (date.fromisoformat(fu) - date.today()).days if fu else None
        except Exception:
            days_left = None
        fu_label = (f"⚠ {fu}" if fu and days_left is not None and days_left < 0
                    else fu)
        rows.append({
            "_id":         c["id"],
            "Name":        c["name"],
            "Company":     c["company"] or "—",
            "Title":       c["title"] or "—",
            "Relationship": _REL_LABELS.get(c["relationship"] or "", c["relationship"] or "—"),
            "Reached Out": "Yes" if c["last_contact_date"] else "No",
            "Last Contact": c["last_contact_date"] or "—",
            "Follow-up":   fu_label or "—",
        })

    df = pd.DataFrame(rows)
    event = st.dataframe(
        df.drop(columns=["_id"]),
        on_select="rerun",
        selection_mode="single-row",
        hide_index=True,
        use_container_width=True,
        column_config={
            "Name":         st.column_config.TextColumn("Name",         width="medium"),
            "Company":      st.column_config.TextColumn("Company",      width="medium"),
            "Title":        st.column_config.TextColumn("Title",        width="medium"),
            "Relationship": st.column_config.TextColumn("Relationship", width="medium"),
            "Reached Out":  st.column_config.TextColumn("Reached Out",  width="small"),
            "Last Contact": st.column_config.TextColumn("Last Contact", width="small"),
            "Follow-up":    st.column_config.TextColumn("Follow-up",    width="small"),
        },
    )

    if event.selection.rows:
        sel_id = int(df.iloc[event.selection.rows[0]]["_id"])
        st.session_state["contacts_selected_id"] = sel_id
        st.session_state["contacts_show_add"] = False

    # ── Detail / edit panel ────────────────────────────────────────────────────
    sel_id = st.session_state.get("contacts_selected_id")
    if sel_id:
        contact = db.get_network_contact(conn, sel_id)
        if contact:
            st.divider()
            hdr1, hdr2 = st.columns([10, 1])
            hdr1.subheader(contact["name"], anchor=False)
            if hdr2.button("✕ Close", key="close_contact_detail"):
                st.session_state["contacts_selected_id"] = None
                st.rerun()
            _render_contact_form(conn, existing=contact)


def _render_contact_form(conn, existing) -> None:
    is_edit = existing is not None
    form_key = f"contact_form_{'edit_' + str(existing['id']) if is_edit else 'add'}"

    _fu_date = None
    _lc_date = None
    if is_edit:
        if existing["follow_up_date"]:
            try:
                _fu_date = date.fromisoformat(existing["follow_up_date"][:10])
            except Exception:
                pass
        if existing["last_contact_date"]:
            try:
                _lc_date = date.fromisoformat(existing["last_contact_date"][:10])
            except Exception:
                pass

    with st.form(form_key):
        fc1, fc2 = st.columns(2)
        name    = fc1.text_input("Name *", value=existing["name"] if is_edit else "")
        company = fc2.text_input("Company", value=existing["company"] if is_edit else "")

        fc3, fc4 = st.columns(2)
        title   = fc3.text_input("Title", value=existing["title"] if is_edit else "")
        rel_opts = [""] + db.NETWORK_RELATIONSHIPS
        rel_idx  = rel_opts.index(existing["relationship"]) if (is_edit and existing["relationship"] in rel_opts) else 0
        rel      = fc4.selectbox("Relationship", rel_opts, index=rel_idx,
                                  format_func=lambda x: _REL_LABELS.get(x, x) if x else "—")

        fc5, fc6 = st.columns(2)
        email    = fc5.text_input("Email", value=existing["email"] if is_edit else "")
        phone    = fc6.text_input("Phone", value=existing["phone"] if is_edit else "")
        linkedin = st.text_input("LinkedIn URL", value=existing["linkedin_url"] if is_edit else "")

        fc7, fc8 = st.columns(2)
        last_contact = fc7.date_input("Last Contact Date", value=_lc_date)
        follow_up    = fc8.date_input("Follow-up Date", value=_fu_date)

        notes = st.text_area("Notes", value=existing["notes"] if is_edit else "", height=80,
                              placeholder="How you know them, context, what to ask next time…")

        if is_edit:
            sc1, sc2 = st.columns([2, 1])
            saved   = sc1.form_submit_button("💾 Save", type="primary")
            deleted = sc2.form_submit_button("🗑 Delete")
        else:
            saved   = st.form_submit_button("Save Contact", type="primary")
            deleted = False

    if saved:
        if not name.strip():
            st.error("Name is required.")
        else:
            kwargs = dict(
                name              = name.strip(),
                company           = company.strip() or None,
                title             = title.strip() or None,
                email             = email.strip() or None,
                phone             = phone.strip() or None,
                linkedin_url      = linkedin.strip() or None,
                relationship      = rel or None,
                last_contact_date = last_contact.isoformat() if last_contact else None,
                follow_up_date    = follow_up.isoformat() if follow_up else None,
                notes             = notes.strip() or None,
            )
            if is_edit:
                db.update_network_contact(conn, existing["id"], **kwargs)
            else:
                db.add_network_contact(conn, **kwargs)
                st.session_state["contacts_show_add"] = False
            st.rerun()

    if deleted:
        db.delete_network_contact(conn, existing["id"])
        st.session_state["contacts_selected_id"] = None
        st.rerun()
