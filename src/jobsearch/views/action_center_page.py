import streamlit as st
import pandas as pd
from datetime import datetime
from typing import List

from jobsearch.services.action_center_service import ActionCenterService, ActionRecommendation
from jobsearch.services.readiness_service import ReadinessService
from jobsearch.services.submission_review_service import SubmissionReviewService
from jobsearch.views.export_components import render_quick_export
from jobsearch import ats_db

def render_action_center(conn):
    st.header("🎯 Action Center")
    st.write("Your prioritized queue for today. Focus on these high-impact tasks to accelerate your search.")

    readiness_service = ReadinessService(conn)
    submission_service = SubmissionReviewService(conn)
    service = ActionCenterService(conn, readiness_service, submission_service)
    recommendations = service.get_recommendations()

    if not recommendations:
        st.success("🎉 You're all caught up! No urgent actions recommended right now.")
        st.info("💡 Tip: Try running a new scrape or adding more target companies to generate new opportunities.")
        return

    # Summary Metrics
    total = len(recommendations)
    high_urgency = len([r for r in recommendations if r.urgency_score >= 80])
    
    m1, m2, m3 = st.columns(3)
    m1.metric("Pending Actions", total)
    m2.metric("High Urgency", high_urgency)
    m3.metric("Goal", "3 Actions/Day")

    st.divider()

    # Filters
    with st.expander("🔍 Filter Actions", expanded=False):
        f1, f2 = st.columns(2)
        action_types = sorted(list(set(r.action_label for r in recommendations)))
        selected_types = f1.multiselect("Action Type", options=action_types, default=action_types)
        
        companies = sorted(list(set(r.company for r in recommendations)))
        selected_companies = f2.multiselect("Companies", options=companies, default=companies)

    # Filtered List
    filtered_recs = [
        r for r in recommendations 
        if r.action_label in selected_types and r.company in selected_companies
    ]

    if not filtered_recs:
        st.warning("No actions match your filters.")
        return

    # Grouping logic
    groups = {
        "🔥 High Priority": [r for r in filtered_recs if r.total_score >= 80],
        "⚡ Needs Attention": [r for r in filtered_recs if 50 <= r.total_score < 80],
        "📅 Routine Maintenance": [r for r in filtered_recs if r.total_score < 50],
    }

    for group_name, group_recs in groups.items():
        if not group_recs:
            continue
            
        st.subheader(group_name)
        for rec in group_recs:
            with st.container(border=True):
                c1, c2 = st.columns([4, 1])
                
                with c1:
                    st.markdown(f"**{rec.action_label}**: {rec.role_title} @ {rec.company}")
                    st.caption(f"💡 {rec.reason}")
                    
                    if rec.entity_type in ('job', 'application'):
                        render_quick_export(conn, int(rec.entity_id))

                    # Metadata badges
                    badge_html = f"""
                    <span style="background-color: #f0f2f6; padding: 2px 8px; border-radius: 10px; font-size: 0.8rem; margin-right: 5px;">Urgency: {int(rec.urgency_score)}</span>
                    <span style="background-color: #f0f2f6; padding: 2px 8px; border-radius: 10px; font-size: 0.8rem; margin-right: 5px;">Impact: {int(rec.impact_score)}</span>
                    """
                    st.markdown(badge_html, unsafe_allow_html=True)

                with c2:
                    # Action Buttons
                    action_id = f"{rec.entity_type}_{rec.entity_id}_{rec.action_key}"
                    
                    if st.button("Done", key=f"done_{action_id}", help="Mark this action as completed"):
                        service.complete_action(rec)
                        st.rerun()
                    
                    # Simple popover for more actions
                    with st.popover("...", help="More actions"):
                        if st.button("Snooze (3d)", key=f"snooze_{action_id}"):
                            service.snooze_action(rec, days=3)
                            st.rerun()
                        if st.button("Dismiss", key=f"dismiss_{action_id}", help="Permanently hide this recommendation"):
                            service.dismiss_action(rec)
                            st.rerun()
                        
                        # Link to detail page logic
                        if rec.entity_type == 'job' or rec.entity_type == 'application':
                            st.button("View Detail", key=f"view_{action_id}", on_click=lambda: _goto_detail(rec))

def _goto_detail(rec):
    # This logic would depend on how the main app handles navigation
    # For now we'll just store it in session state if the main app supports it
    st.session_state.action_center_target = {
        "entity_type": rec.entity_type,
        "entity_id": rec.entity_id
    }
    st.info(f"Navigate to Detail Page for {rec.company} (ID: {rec.entity_id})")
