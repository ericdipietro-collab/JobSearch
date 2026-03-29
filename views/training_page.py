"""
Skills Training tracker — plan and track courses, certifications, and training programs.
"""
from __future__ import annotations

from datetime import date
from typing import Optional

import pandas as pd
import streamlit as st

import ats_db as db


# ── Helpers ────────────────────────────────────────────────────────────────────

def _status_pill(status: str) -> str:
    color = db.TRAINING_STATUS_COLORS.get(status, "#6b7280")
    label = status.replace("_", " ").title()
    return (
        f'<span style="background:{color};color:#fff;padding:2px 10px;'
        f'border-radius:999px;font-size:0.75rem;font-weight:600;">{label}</span>'
    )


def _days_left(iso: Optional[str]) -> Optional[int]:
    if not iso:
        return None
    try:
        return (date.fromisoformat(iso[:10]) - date.today()).days
    except Exception:
        return None


def _fmt_date(iso: Optional[str]) -> str:
    if not iso:
        return "—"
    try:
        return date.fromisoformat(iso[:10]).strftime("%b %d, %Y")
    except Exception:
        return iso[:10]


# ── Main entry point ───────────────────────────────────────────────────────────

def render_training(conn) -> None:
    db.init_db(conn)

    counts = db.training_status_counts(conn)
    total  = sum(counts.values())

    # ── Summary bar ───────────────────────────────────────────────────────────
    sc = st.columns(5)
    sc[0].metric("Total",       total)
    sc[1].metric("In Progress", counts.get("in_progress", 0))
    sc[2].metric("Planned",     counts.get("planned",     0))
    sc[3].metric("Completed",   counts.get("completed",   0))
    sc[4].metric("Paused",      counts.get("paused",      0))

    st.divider()

    # ── Filters + Add ────────────────────────────────────────────────────────
    fc1, fc2, fc3 = st.columns([2, 3, 2])
    with fc1:
        status_opts = ["All"] + db.TRAINING_STATUSES
        sel_status  = st.selectbox("Status", status_opts, key="training_status_filter",
                                   label_visibility="collapsed")
    with fc2:
        search = st.text_input("Search", placeholder="course or provider…",
                               key="training_search", label_visibility="collapsed")
    with fc3:
        if st.button("➕ Add Course", key="training_add_btn", use_container_width=True):
            toggled = not st.session_state.get("training_show_add_form", False)
            st.session_state["training_show_add_form"] = toggled
            if toggled:
                st.session_state["training_selected_id"] = None

    if st.session_state.get("training_show_add_form"):
        _render_add_form(conn)
        st.divider()

    # ── Load + filter ────────────────────────────────────────────────────────
    courses = db.get_all_training(
        conn, status=None if sel_status == "All" else sel_status
    )
    if search:
        q = search.lower()
        courses = [c for c in courses
                   if q in (c["name"] or "").lower()
                   or q in (c["provider"] or "").lower()
                   or q in (c["category"] or "").lower()]

    if not courses:
        st.info("No courses found. Add one with the button above.")
        return

    # ── Sortable table ───────────────────────────────────────────────────────
    rows = []
    for c in courses:
        days = _days_left(c["target_date"])
        target_label = ""
        if c["target_date"]:
            if c["status"] == "completed":
                target_label = "✓ Done"
            elif days is not None and days < 0:
                target_label = f"⚠ {c['target_date'][:10]}"
            elif days == 0:
                target_label = "Due today"
            else:
                target_label = c["target_date"][:10]

        rows.append({
            "_id":      c["id"],
            "Course":   c["name"],
            "Provider": c["provider"] or "—",
            "Category": c["category"] or "—",
            "Status":   c["status"].replace("_", " ").title(),
            "Started":  c["start_date"][:10] if c["start_date"] else "—",
            "Target":   target_label,
            "Hrs/wk":   str(c["weekly_hours"]) if c["weekly_hours"] else "—",
        })
    df_tbl = pd.DataFrame(rows)

    event = st.dataframe(
        df_tbl.drop(columns=["_id"]),
        on_select="rerun",
        selection_mode="single-row",
        hide_index=True,
        use_container_width=True,
        column_config={
            "Course":   st.column_config.TextColumn("Course",   width="large"),
            "Provider": st.column_config.TextColumn("Provider", width="medium"),
            "Category": st.column_config.TextColumn("Category", width="medium"),
            "Status":   st.column_config.TextColumn("Status",   width="medium"),
            "Started":  st.column_config.TextColumn("Started",  width="small"),
            "Target":   st.column_config.TextColumn("Target",   width="small"),
            "Hrs/wk":   st.column_config.TextColumn("Hrs/wk",  width="small"),
        },
    )

    # Sync row selection
    if event.selection.rows:
        sel_id = int(df_tbl.iloc[event.selection.rows[0]]["_id"])
        st.session_state["training_selected_id"] = sel_id
        st.session_state["training_show_add_form"] = False

    # ── Inline detail / edit panel ────────────────────────────────────────────
    sel_id = st.session_state.get("training_selected_id")
    if sel_id:
        course = db.get_training(conn, sel_id)
        if course:
            st.divider()
            h1, h2 = st.columns([10, 1])
            h1.subheader(course["name"], anchor=False)
            if h2.button("✕ Close", key="training_close"):
                st.session_state["training_selected_id"] = None
                st.rerun()
            _render_edit_form(conn, course)


# ── Add form ───────────────────────────────────────────────────────────────────

def _render_add_form(conn) -> None:
    with st.form("add_training_form", clear_on_submit=True):
        st.markdown("**New Course / Training**")

        r1c1, r1c2 = st.columns(2)
        name     = r1c1.text_input("Course / Training Name *")
        provider_choice = r1c2.selectbox("Provider", db.TRAINING_PROVIDERS)

        r2c1, r2c2, r2c3 = st.columns(3)
        category = r2c1.selectbox("Category", db.TRAINING_CATEGORIES)
        status   = r2c2.selectbox("Status", db.TRAINING_STATUSES,
                                  index=db.TRAINING_STATUSES.index("planned"))
        weekly_h = r2c3.number_input("Hours / week", value=0, min_value=0, step=1)

        url = st.text_input("Course URL", placeholder="https://…")

        r3c1, r3c2 = st.columns(2)
        start_d  = r3c1.date_input("Start date", value=None, key="add_start")
        target_d = r3c2.date_input("Target completion", value=None, key="add_target")

        r4c1, r4c2 = st.columns(2)
        est_hrs  = r4c1.number_input("Estimated total hours", value=0, min_value=0, step=5)
        comp_d   = r4c2.date_input("Completion date", value=None, key="add_comp",
                                   help="Fill in when you finish")

        cert_url = st.text_input("Certificate URL (once earned)")
        notes    = st.text_area("Notes", height=70)

        if st.form_submit_button("Save", type="primary"):
            if not name.strip():
                st.error("Course name is required.")
            else:
                # Auto-set start_date to today when marking in_progress
                effective_start = start_d.isoformat() if start_d else (
                    date.today().isoformat() if status == "in_progress" else None
                )
                db.add_training(
                    conn,
                    name            = name.strip(),
                    provider        = provider_choice,
                    category        = category,
                    status          = status,
                    url             = url.strip() or None,
                    start_date      = effective_start,
                    target_date     = target_d.isoformat() if target_d else None,
                    completion_date = comp_d.isoformat() if comp_d else None,
                    estimated_hours = est_hrs or None,
                    weekly_hours    = weekly_h or None,
                    certificate_url = cert_url.strip() or None,
                    notes           = notes.strip() or None,
                )
                st.session_state["training_show_add_form"] = False
                st.rerun()


# ── Edit / detail form ─────────────────────────────────────────────────────────

def _render_edit_form(conn, course) -> None:
    # Parse stored dates
    def _to_date(iso):
        if not iso:
            return None
        try:
            return date.fromisoformat(iso[:10])
        except Exception:
            return None

    # Quick info strip
    ic1, ic2, ic3, ic4 = st.columns(4)
    ic1.markdown(_status_pill(course["status"]), unsafe_allow_html=True)
    ic2.caption(f"Provider: {course['provider'] or '—'}")
    ic3.caption(f"Category: {course['category'] or '—'}")
    if course["url"]:
        ic4.markdown(f"[Course Link ↗]({course['url']})")

    if course["notes"]:
        st.caption(course["notes"])

    st.markdown("---")

    with st.form(f"edit_training_{course['id']}"):
        ec1, ec2 = st.columns(2)
        name     = ec1.text_input("Course Name", value=course["name"] or "")
        prov_idx = db.TRAINING_PROVIDERS.index(course["provider"]) \
                   if course["provider"] in db.TRAINING_PROVIDERS else len(db.TRAINING_PROVIDERS) - 1
        provider = ec2.selectbox("Provider", db.TRAINING_PROVIDERS, index=prov_idx)

        ea1, ea2, ea3 = st.columns(3)
        cat_idx  = db.TRAINING_CATEGORIES.index(course["category"]) \
                   if course["category"] in db.TRAINING_CATEGORIES else len(db.TRAINING_CATEGORIES) - 1
        category = ea1.selectbox("Category", db.TRAINING_CATEGORIES, index=cat_idx)
        stat_idx = db.TRAINING_STATUSES.index(course["status"]) \
                   if course["status"] in db.TRAINING_STATUSES else 0
        status   = ea2.selectbox("Status", db.TRAINING_STATUSES, index=stat_idx)
        weekly_h = ea3.number_input("Hours / week", value=int(course["weekly_hours"] or 0),
                                    min_value=0, step=1)

        url = st.text_input("Course URL", value=course["url"] or "")

        eb1, eb2, eb3 = st.columns(3)
        start_d  = eb1.date_input("Start date",          value=_to_date(course["start_date"]))
        target_d = eb2.date_input("Target completion",   value=_to_date(course["target_date"]))
        comp_d   = eb3.date_input("Completion date",     value=_to_date(course["completion_date"]),
                                  help="Fill in when you finish")

        ec3, ec4 = st.columns(2)
        est_hrs  = ec3.number_input("Estimated total hours",
                                    value=int(course["estimated_hours"] or 0),
                                    min_value=0, step=5)
        cert_url = ec4.text_input("Certificate URL", value=course["certificate_url"] or "")

        notes = st.text_area("Notes", value=course["notes"] or "", height=80)

        sc1, sc2 = st.columns([2, 1])
        saved   = sc1.form_submit_button("💾 Save", type="primary")
        deleted = sc2.form_submit_button("🗑 Delete")

    if saved:
        # Auto-set start date when transitioning to in_progress
        effective_start = start_d.isoformat() if start_d else (
            date.today().isoformat()
            if status == "in_progress" and not course["start_date"] else None
        )
        # Auto-set completion date when transitioning to completed
        effective_comp = comp_d.isoformat() if comp_d else (
            date.today().isoformat()
            if status == "completed" and not course["completion_date"] else None
        )
        db.update_training(
            conn, course["id"],
            name            = name.strip(),
            provider        = provider,
            category        = category,
            status          = status,
            url             = url.strip() or None,
            start_date      = effective_start or course["start_date"],
            target_date     = target_d.isoformat() if target_d else None,
            completion_date = effective_comp or course["completion_date"],
            estimated_hours = est_hrs or None,
            weekly_hours    = weekly_h or None,
            certificate_url = cert_url.strip() or None,
            notes           = notes.strip() or None,
        )
        st.success("Saved.")
        st.rerun()

    if deleted:
        db.delete_training(conn, course["id"])
        st.session_state["training_selected_id"] = None
        st.rerun()
