"""
Email Templates — library of reusable message templates with variable substitution.
"""
from __future__ import annotations

import streamlit as st

import ats_db as db

_TYPE_LABELS = {
    "follow_up":  "Follow-Up",
    "thank_you":  "Thank You",
    "networking": "Networking",
    "recruiter":  "Recruiter Response",
    "offer":      "Offer",
    "withdrawal": "Withdrawal",
    "other":      "Other",
}


def _render_variable(text: str, company: str, role: str, contact_name: str, my_name: str) -> str:
    return (
        text
        .replace("{company}", company or "{company}")
        .replace("{role}", role or "{role}")
        .replace("{contact_name}", contact_name or "{contact_name}")
        .replace("{my_name}", my_name or "{my_name}")
    )


def render_templates(conn) -> None:
    db.init_db(conn)

    st.markdown(
        "Reusable email and message templates. Use `{company}`, `{role}`, `{contact_name}`, "
        "and `{my_name}` as placeholders — they'll be replaced when you render a template."
    )

    templates = db.get_all_templates(conn)

    # ── Add new template ──────────────────────────────────────────────────────
    with st.expander("➕ Add Template", expanded=False):
        with st.form("add_template_form", clear_on_submit=True):
            nt1, nt2 = st.columns(2)
            new_name    = nt1.text_input("Template Name *")
            type_opts   = list(_TYPE_LABELS.keys())
            new_type    = nt2.selectbox("Type", type_opts,
                                        format_func=lambda x: _TYPE_LABELS[x])
            new_subject = st.text_input("Subject line", placeholder="e.g. Following up — {role} at {company}")
            new_body    = st.text_area("Body *", height=200,
                                       placeholder="Use {company}, {role}, {contact_name}, {my_name}")
            if st.form_submit_button("Save Template", type="primary"):
                if not new_name.strip() or not new_body.strip():
                    st.error("Name and Body are required.")
                else:
                    db.add_template(
                        conn,
                        name          = new_name.strip(),
                        template_type = new_type,
                        subject       = new_subject.strip() or None,
                        body          = new_body.strip(),
                    )
                    st.success("Template saved.")
                    st.rerun()

    st.divider()

    if not templates:
        st.info("No templates yet. Add one above or re-initialize the database to load defaults.")
        return

    # Group by type
    by_type: dict = {}
    for t in templates:
        ttype = t["template_type"] or "other"
        by_type.setdefault(ttype, []).append(t)

    for ttype, group in by_type.items():
        st.subheader(_TYPE_LABELS.get(ttype, ttype.title()), anchor=False)
        for tmpl in group:
            with st.expander(tmpl["name"], expanded=False):
                _render_template_card(conn, tmpl)
        st.markdown("")


def _render_template_card(conn, tmpl) -> None:
    t_tab, e_tab = st.tabs(["Use", "Edit"])

    with t_tab:
        # Variable substitution inputs
        st.caption("Fill in the blanks to render your message:")
        rc1, rc2, rc3, rc4 = st.columns(4)
        company      = rc1.text_input("Company",      key=f"rv_co_{tmpl['id']}")
        role         = rc2.text_input("Role",         key=f"rv_ro_{tmpl['id']}")
        contact_name = rc3.text_input("Contact name", key=f"rv_cn_{tmpl['id']}")
        my_name      = rc4.text_input("Your name",    key=f"rv_mn_{tmpl['id']}",
                                      value=st.session_state.get("my_name_default", ""))

        # Persist my_name across cards
        if my_name:
            st.session_state["my_name_default"] = my_name

        if tmpl["subject"]:
            rendered_subject = _render_variable(tmpl["subject"], company, role, contact_name, my_name)
            st.markdown(f"**Subject:** `{rendered_subject}`")

        rendered_body = _render_variable(tmpl["body"], company, role, contact_name, my_name)
        st.text_area(
            "Rendered message",
            value=rendered_body,
            height=250,
            key=f"rendered_{tmpl['id']}",
            label_visibility="collapsed",
            disabled=True,
            help="Read-only preview — select all (Ctrl+A) and copy to use this message.",
        )

    with e_tab:
        with st.form(f"edit_tmpl_{tmpl['id']}"):
            type_opts = list(_TYPE_LABELS.keys())
            cur_type_idx = type_opts.index(tmpl["template_type"]) if tmpl["template_type"] in type_opts else 0
            et1, et2 = st.columns(2)
            upd_name    = et1.text_input("Name", value=tmpl["name"] or "")
            upd_type    = et2.selectbox("Type", type_opts, index=cur_type_idx,
                                        format_func=lambda x: _TYPE_LABELS[x])
            upd_subject = st.text_input("Subject", value=tmpl["subject"] or "")
            upd_body    = st.text_area("Body", value=tmpl["body"] or "", height=200)
            ec1, ec2 = st.columns([2, 1])
            save_it   = ec1.form_submit_button("💾 Save", type="primary")
            delete_it = ec2.form_submit_button("🗑 Delete")

        if save_it:
            if not upd_name.strip() or not upd_body.strip():
                st.error("Name and Body are required.")
            else:
                db.update_template(
                    conn, tmpl["id"],
                    name          = upd_name.strip(),
                    template_type = upd_type,
                    subject       = upd_subject.strip() or None,
                    body          = upd_body.strip(),
                )
                st.success("Saved.")
                st.rerun()

        if delete_it:
            db.delete_template(conn, tmpl["id"])
            st.rerun()
