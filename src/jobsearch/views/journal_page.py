"""
Job Search Journal — daily notes for tracking thoughts, decisions, and morale.
"""
from __future__ import annotations

from datetime import date

import streamlit as st

from jobsearch import ats_db as db

_MOODS = ["", "💪 Energized", "😊 Good", "😐 Neutral", "😔 Low", "😤 Frustrated", "🤔 Uncertain"]


def render_journal(conn) -> None:
    db.init_db(conn)

    st.markdown(
        "A private log for daily reflections — decisions made, how you're feeling, "
        "things to remember. Not tied to any specific application."
    )

    # ── Add entry form ─────────────────────────────────────────────────────────
    with st.form("journal_add_form", clear_on_submit=True):
        jc1, jc2 = st.columns([2, 2])
        entry_date = jc1.date_input("Date", value=date.today())
        mood       = jc2.selectbox("Mood", _MOODS, label_visibility="visible")
        content    = st.text_area("Entry *", height=130,
                                   placeholder="What happened today? How are you feeling about the search?")
        if st.form_submit_button("➕ Add Entry", type="primary"):
            if not content.strip():
                st.error("Entry text is required.")
            else:
                db.add_journal_entry(
                    conn,
                    entry_date = entry_date.isoformat(),
                    content    = content.strip(),
                    mood       = mood if mood else None,
                )
                st.rerun()

    st.divider()

    # ── Entry list ─────────────────────────────────────────────────────────────
    entries = db.get_journal_entries(conn)

    if not entries:
        st.info("No journal entries yet. Write your first one above.")
        return

    for e in entries:
        d_str = e["entry_date"][:10] if e["entry_date"] else ""
        try:
            d_fmt = date.fromisoformat(d_str).strftime("%A, %B %d, %Y")
        except Exception:
            d_fmt = d_str
        mood_prefix = (e["mood"] + " — ") if e["mood"] else ""
        label = f"{mood_prefix}{d_fmt}"

        with st.expander(label, expanded=False):
            # Inline edit form
            with st.form(f"edit_entry_{e['id']}"):
                upd_mood    = st.selectbox("Mood", _MOODS,
                                            index=_MOODS.index(e["mood"]) if e["mood"] in _MOODS else 0,
                                            key=f"mood_edit_{e['id']}")
                upd_content = st.text_area("Entry", value=e["content"] or "", height=120,
                                            key=f"content_edit_{e['id']}")
                ec1, ec2 = st.columns([2, 1])
                save_it   = ec1.form_submit_button("💾 Save", type="primary")
                delete_it = ec2.form_submit_button("🗑 Delete")

            if save_it:
                if not upd_content.strip():
                    st.error("Entry cannot be empty.")
                else:
                    db.update_journal_entry(
                        conn, e["id"],
                        mood    = upd_mood if upd_mood else None,
                        content = upd_content.strip(),
                    )
                    st.rerun()

            if delete_it:
                db.delete_journal_entry(conn, e["id"])
                st.rerun()
