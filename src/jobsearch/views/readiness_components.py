import streamlit as st
from jobsearch.services.readiness_service import ReadinessState

def render_readiness_badge(state: ReadinessState, show_label: bool = True):
    """Returns an HTML badge for table/UI display."""
    colors = {
        'ready': '#10b981',   # Green
        'draft': '#3b82f6',   # Blue
        'blocked': '#ef4444', # Red
    }
    
    color = colors.get(state.status, '#6b7280')
    label = state.status.upper() if show_label else ""
    
    # Use a tooltip for the reason
    reason = state.reason
    if state.critical_issues:
        reason += " | " + " | ".join(state.critical_issues)
        
    badge_html = f"""
    <span title="{reason}" style="
        background-color: {color};
        color: white;
        padding: 2px 8px;
        border-radius: 10px;
        font-size: 0.7rem;
        font-weight: 700;
        cursor: help;
    ">{label}</span>
    """
    return badge_html
