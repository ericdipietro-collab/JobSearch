"""
Interview Question Bank — shared STAR story library for behavioral interview prep.
"""
from __future__ import annotations

import streamlit as st

from jobsearch import ats_db as db

_CAT_LABELS = {
    "behavioral":   "Behavioral",
    "situational":  "Situational",
    "leadership":   "Leadership",
    "role-specific":"Role-Specific",
    "technical":    "Technical",
    "other":        "Other",
}

_STAR_FIELDS = [
    ("star_situation", "Situation", "Set the scene. What was the context?"),
    ("star_task",      "Task",      "What was your responsibility?"),
    ("star_action",    "Action",    "What did YOU specifically do? (use 'I', not 'we')"),
    ("star_result",    "Result",    "What was the outcome? Quantify where possible."),
]


def render_question_bank(conn) -> None:
    db.init_db(conn)
    apps = db.get_applications(conn)
    app_options = {f"{a['company']} — {a['role']}": a["id"] for a in apps}
    company_options = sorted({str(a["company"]) for a in apps if a["company"]})

    st.markdown(
        "Build your answer library here. Write STAR stories once, reuse them across any interview. "
        "The Prep tab on each application links to these same categories."
    )

    # ── Add question form ──────────────────────────────────────────────────────
    with st.expander("➕ Add Question", expanded=False):
        with st.form("add_question_form", clear_on_submit=True):
            aq1, aq2 = st.columns([3, 1])
            new_q    = aq1.text_input("Question *")
            cat_opts = db.QUESTION_CATEGORIES
            new_cat  = aq2.selectbox("Category", cat_opts,
                                      format_func=lambda x: _CAT_LABELS.get(x, x))
            l1, l2 = st.columns(2)
            new_company = l1.selectbox("Linked Company", ["Shared"] + company_options)
            new_app_label = l2.selectbox("Linked Application", ["None"] + list(app_options.keys()))
            new_tags = st.text_input("Tags (comma-separated)", placeholder="e.g. conflict, leadership, data")
            if st.form_submit_button("Add Question", type="primary"):
                if not new_q.strip():
                    st.error("Question text is required.")
                else:
                    db.add_question(
                        conn,
                        new_q.strip(),
                        new_cat,
                        company=None if new_company == "Shared" else new_company,
                        application_id=app_options.get(new_app_label),
                        tags=new_tags.strip() or None,
                    )
                    st.success("Question added.")
                    st.rerun()

    st.divider()

    # ── Filter ─────────────────────────────────────────────────────────────────
    cat_filter = st.selectbox(
        "Filter by category",
        ["All"] + db.QUESTION_CATEGORIES,
        format_func=lambda x: "All Categories" if x == "All" else _CAT_LABELS.get(x, x),
        key="qb_cat_filter",
        label_visibility="collapsed",
    )
    company_filter = st.selectbox(
        "Filter by company",
        ["All Companies", "Shared"] + company_options,
        key="qb_company_filter",
        label_visibility="collapsed",
    )

    filter_company = None
    if company_filter == "Shared":
        filter_company = "__shared__"
    elif company_filter != "All Companies":
        filter_company = company_filter

    questions = db.get_questions(conn, category=None if cat_filter == "All" else cat_filter)
    if filter_company == "__shared__":
        questions = [q for q in questions if not q["company"] and not q["application_id"]]
    elif filter_company:
        questions = [q for q in questions if (q["company"] or "").lower() == filter_company.lower() or q["application_id"]]

    if not questions:
        st.info("No questions yet. Add one above.")
        return

    # ── Group by category ──────────────────────────────────────────────────────
    by_cat: dict = {}
    for q in questions:
        by_cat.setdefault(q["category"], []).append(q)

    for cat, group in by_cat.items():
        st.subheader(_CAT_LABELS.get(cat, cat.title()), anchor=False)
        for q in group:
            _render_question_card(conn, q)
        st.markdown("")


def _render_question_card(conn, q) -> None:
    # Indicate whether STAR story is filled in
    has_story = any(q[f] for f in ("star_situation", "star_task", "star_action", "star_result"))
    badge = "✅" if has_story else "○"
    tags  = f"  ·  _{q['tags']}_" if q["tags"] else ""
    scope_bits = []
    if q["company"]:
        scope_bits.append(f"Company: {q['company']}")
    if q["application_id"]:
        scope_bits.append(f"App #{q['application_id']}")
    scope = f"  ·  {' | '.join(scope_bits)}" if scope_bits else ""

    with st.expander(f"{badge} {q['question']}{tags}{scope}", expanded=False):
        # STAR fields — individual Save buttons (not inside a form) so each saves independently
        new_vals = {}
        for field, label, hint in _STAR_FIELDS:
            new_vals[field] = st.text_area(
                label,
                value=q[field] or "",
                height=90,
                placeholder=hint,
                key=f"{field}_{q['id']}",
            )

        upd_tags = st.text_input("Tags", value=q["tags"] or "", key=f"tags_{q['id']}")
        linked_company = st.text_input("Linked Company", value=q["company"] or "", key=f"company_{q['id']}")
        linked_app = st.text_input("Linked Application ID", value=str(q["application_id"] or ""), key=f"app_{q['id']}")

        col_save, col_del, _ = st.columns([1, 1, 4])
        if col_save.button("💾 Save", key=f"save_q_{q['id']}", type="primary"):
            db.update_question(
                conn, q["id"],
                star_situation = new_vals["star_situation"].strip() or None,
                star_task      = new_vals["star_task"].strip() or None,
                star_action    = new_vals["star_action"].strip() or None,
                star_result    = new_vals["star_result"].strip() or None,
                tags           = upd_tags.strip() or None,
                company        = linked_company.strip() or None,
                application_id = int(linked_app) if linked_app.strip().isdigit() else None,
            )
            st.toast("Saved.")
            st.rerun()

        if col_del.button("🗑", key=f"del_q_{q['id']}", help="Delete question"):
            db.delete_question(conn, q["id"])
            st.rerun()
