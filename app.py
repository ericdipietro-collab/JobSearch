import streamlit as st
import pandas as pd
import yaml
from pathlib import Path

# --- CONFIG & LOADING ---
st.set_page_config(page_title="Job Search v6 Dashboard", layout="wide")

@st.cache_data # This keeps the UI fast by not reloading data on every click
def load_results():
    # Load your results (adjust filename if needed)
    return pd.read_csv("job_search_v6_results.csv")

@st.cache_data
def load_rejected():
    return pd.read_csv("job_search_v6_rejected.csv")

# --- SIDEBAR: Gating & Preferences ---
st.sidebar.header("🎯 Preferences & Gating")
salary_floor = st.sidebar.number_input("Min Salary ($)", value=170000, step=5000)
remote_only = st.sidebar.checkbox("Remote Only", value=True)

# Add a "Run Scraper" button
if st.sidebar.button("🚀 Run Job Search Pipeline"):
    with st.spinner("Scraping companies... this may take minutes."):
        # This calls your existing script logic
        import job_search_v6
        job_search_v6.main()
        st.success("Pipeline Complete!")

# --- MAIN UI ---
st.title("💼 Job Search Healer & Scraper")
st.write(f"Showing results for **Eric DiPietro** | Location: **Longmont, CO**")

# Metrics row
df_kept = load_results()
df_rejected = load_rejected()

col1, col2, col3 = st.columns(3)
col1.metric("Total Kept", len(df_kept))
col2.metric("Filtered Out", len(df_rejected))
col3.metric("Avg Score", f"{df_kept['score'].mean():.1f}")

# Tabs for Organization
tab1, tab2, tab3 = st.tabs(["🔥 Top Matches", "📂 Manual Review", "🚫 Rejected (Audit)"])

with tab1:
    st.subheader("Jobs passing all Hard Gates")
    # Interactive Table
    st.dataframe(df_kept.sort_values("score", ascending=False), use_container_width=True)

with tab2:
    st.subheader("Flags for human eyes")
    manual_jobs = df_kept[df_kept['action_bucket'] == "MANUAL REVIEW"]
    st.write(manual_jobs[['company', 'title', 'url']])

with tab3:
    st.subheader("Audit Log: Why jobs were dropped")
    # This is where your "Soft Disqualification" logic shines
    st.dataframe(df_rejected[['company', 'title', 'drop_stage', 'drop_reason']])