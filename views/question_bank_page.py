"""
Interview Question Bank — shared STAR story library for behavioral interview prep.
"""
from __future__ import annotations

import streamlit as st

import ats_db as db

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
            new_tags = st.text_input("Tags (comma-separated)", placeholder="e.g. conflict, leadership, data")
            if st.form_submit_button("Add Question", type="primary"):
                if not new_q.strip():
                    st.error("Question text is required.")
                else:
                    db.add_question(conn, new_q.strip(), new_cat,
                                    tags=new_tags.strip() or None)
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

    questions = db.get_questions(conn, category=None if cat_filter == "All" else cat_filter)

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

    with st.expander(f"{badge} {q['question']}{tags}", expanded=False):
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

        col_save, col_del, _ = st.columns([1, 1, 4])
        if col_save.button("💾 Save", key=f"save_q_{q['id']}", type="primary"):
            db.update_question(
                conn, q["id"],
                star_situation = new_vals["star_situation"].strip() or None,
                star_task      = new_vals["star_task"].strip() or None,
                star_action    = new_vals["star_action"].strip() or None,
                star_result    = new_vals["star_result"].strip() or None,
                tags           = upd_tags.strip() or None,
            )
            st.toast("Saved.")
            st.rerun()

        if col_del.button("🗑", key=f"del_q_{q['id']}", help="Delete question"):
            db.delete_question(conn, q["id"])
            st.rerun()
