import streamlit as st

# Semantic Theme Colors
THEME = {
    "primary": "#3b82f6",
    "success": "#10b981",
    "warning": "#f59e0b",
    "danger": "#ef4444",
    "info": "#6366f1",
    "gray": "#64748b",
    "background": "#f8fafc",
    "card_bg": "#ffffff",
    "border": "#e2e8f0",
    "text_main": "#0f172a",
    "text_muted": "#64748b",
}

def set_custom_style():
    """Injects custom CSS to modernize the Streamlit UI."""
    st.markdown("""
        <style>
        /* Modern font and background */
        @import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700&display=swap');
        
        html, body, [class*="css"] {
            font-family: 'Inter', sans-serif;
        }

        /* Keep Streamlit header/menu visible so users can access settings and theme controls */
        footer {visibility: hidden;}

        /* Sidebar styling */
        section[data-testid="stSidebar"] {
            background-color: #f8fafc;
            border-right: 1px solid #e2e8f0;
        }
        
        section[data-testid="stSidebar"] .stRadio > label {
            display: none;
        }
        
        /* Sidebar Navigation Items */
        section[data-testid="stSidebar"] [data-testid="stWidgetLabel"] {
            font-weight: 600;
            color: #475569;
        }

        /* Card-like containers */
        .st-emotion-cache-12w0qpk {
            padding: 1.5rem;
            border-radius: 0.75rem;
            border: 1px solid #e2e8f0;
            background-color: white;
            box-shadow: 0 1px 3px 0 rgb(0 0 0 / 0.1), 0 1px 2px -1 sm rgb(0 0 0 / 0.1);
        }

        /* Metrics styling */
        [data-testid="stMetricValue"] {
            font-size: 1.875rem;
            font-weight: 700;
            color: #1e293b;
        }
        
        [data-testid="stMetricLabel"] {
            font-size: 0.875rem;
            font-weight: 500;
            color: #64748b;
            text-transform: uppercase;
            letter-spacing: 0.025em;
        }

        /* Custom Card Helper Class */
        .job-card {
            background-color: white;
            padding: 1.25rem;
            border-radius: 0.75rem;
            border: 1px solid #e2e8f0;
            margin-bottom: 1rem;
            transition: all 0.2s ease;
        }
        .job-card:hover {
            border-color: #3b82f6;
            box-shadow: 0 4px 6px -1px rgb(0 0 0 / 0.1), 0 2px 4px -2px rgb(0 0 0 / 0.1);
        }

        /* Status Badges */
        .badge {
            display: inline-flex;
            align-items: center;
            padding: 0.125rem 0.625rem;
            border-radius: 9999px;
            font-size: 0.75rem;
            font-weight: 600;
            line-height: 1.25rem;
        }
        
        .badge-blue { background-color: #dbeafe; color: #1e40af; }
        .badge-green { background-color: #dcfce7; color: #166534; }
        .badge-yellow { background-color: #fef9c3; color: #854d0e; }
        .badge-red { background-color: #fee2e2; color: #991b1b; }
        .badge-purple { background-color: #f3e8ff; color: #6b21a8; }
        .badge-gray { background-color: #f1f5f9; color: #334155; }

        /* Titles and Headers */
        h1, h2, h3 {
            color: #0f172a !important;
            font-weight: 700 !important;
        }
        
        .stButton > button {
            border-radius: 0.5rem;
            font-weight: 500;
            transition: all 0.2s;
        }
        
        .stButton > button:hover {
            border-color: #3b82f6;
            color: #3b82f6;
        }

        .stButton > button[kind="primary"] {
            background-color: #3b82f6;
            color: white;
            border: none;
        }
        
        .stButton > button[kind="primary"]:hover {
            background-color: #2563eb;
            color: white;
        }

        /* Make scrollbars more visible and modern */
        ::-webkit-scrollbar {
            width: 8px;
            height: 8px;
        }
        ::-webkit-scrollbar-track {
            background: #f1f5f9;
            border-radius: 4px;
        }
        ::-webkit-scrollbar-thumb {
            background: #cbd5e1;
            border-radius: 4px;
            border: 2px solid #f1f5f9;
        }
        ::-webkit-scrollbar-thumb:hover {
            background: #94a3b8;
        }
        
        /* Specific fix for Streamlit's data grid container if possible */
        [data-testid="stTable"] {
            border-radius: 0.75rem;
            overflow: hidden;
            border: 1px solid #e2e8f0;
        }
        
        /* Improve spacing in the main app container */
        .block-container {
            padding-top: 2rem;
            padding-bottom: 2rem;
            max-width: 1200px;
        }

        </style>
    """, unsafe_allow_html=True)

def card(title, content, footer=None):
    """Renders a card with optional title and footer."""
    html = f"""
    <div class="job-card">
        <div style="font-weight: 600; font-size: 1.1rem; color: #1e293b; margin-bottom: 0.5rem;">{title}</div>
        <div style="color: #475569; font-size: 0.95rem;">{content}</div>
        {f'<div style="margin-top: 1rem; padding-top: 0.75rem; border-top: 1px solid #f1f5f9; font-size: 0.85rem; color: #64748b;">{footer}</div>' if footer else ''}
    </div>
    """
    st.markdown(html, unsafe_allow_html=True)

def badge(text, color="blue"):
    """Returns a status badge HTML string."""
    colors = {
        "blue": "badge-blue",
        "green": "badge-green",
        "yellow": "badge-yellow",
        "red": "badge-red",
        "purple": "badge-purple",
        "gray": "badge-gray"
    }
    cls = colors.get(color, "badge-blue")
    return f'<span class="badge {cls}">{text}</span>'

def feed_item(icon, title, subtitle, date_text):
    """Renders a modern feed item."""
    html = f"""
    <div style="display: flex; align-items: flex-start; gap: 1rem; padding: 0.75rem 0; border-bottom: 1px solid #f1f5f9;">
        <div style="font-size: 1.25rem; background: #f1f5f9; padding: 0.5rem; border-radius: 0.5rem; line-height: 1;">{icon}</div>
        <div style="flex: 1;">
            <div style="font-weight: 600; color: #1e293b; font-size: 0.95rem;">{title}</div>
            <div style="color: #64748b; font-size: 0.85rem;">{subtitle}</div>
        </div>
        <div style="color: #94a3b8; font-size: 0.75rem; white-space: nowrap;">{date_text}</div>
    </div>
    """
    st.markdown(html, unsafe_allow_html=True)

def empty_state(icon, title, message):
    """Renders an empty state component."""
    html = f"""
    <div style="text-align: center; padding: 3rem 1rem; background: #f8fafc; border-radius: 0.75rem; border: 2px dashed #e2e8f0; margin: 1rem 0;">
        <div style="font-size: 2.5rem; margin-bottom: 1rem;">{icon}</div>
        <div style="font-weight: 600; color: #1e293b; font-size: 1.1rem; margin-bottom: 0.5rem;">{title}</div>
        <div style="color: #64748b; font-size: 0.95rem; max-width: 300px; margin: 0 auto;">{message}</div>
    </div>
    """
    st.markdown(html, unsafe_allow_html=True)
