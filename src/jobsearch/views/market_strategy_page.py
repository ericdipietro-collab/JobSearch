import streamlit as st
import pandas as pd
import plotly.express as px
from jobsearch.services.role_clustering_service import RoleClusteringService
from jobsearch.services.job_canonicalization_service import JobCanonicalizationService
from jobsearch.services.resume_market_gap_service import ResumeMarketGapService
from jobsearch import ats_db

def render_market_strategy(conn):
    st.header("🌎 Market Intelligence & Strategy")
    st.write("Understand the broader market landscape, consolidate duplicates, and identify strategic resume gaps.")

    cluster_service = RoleClusteringService(conn)
    dedupe_service = JobCanonicalizationService(conn)
    gap_service = ResumeMarketGapService(conn)

    tab1, tab2, tab3 = st.tabs(["📊 Role Clusters & Market Map", "🎯 Resume-to-Market Gaps", "🔗 Data Canonicalization"])

    with tab1:
        st.subheader("Market Map by Role Family")
        # Ensure clustering is run
        if st.button("🚀 Run / Refresh Role Clustering", use_container_width=True):
            with st.spinner("Clustering jobs..."):
                cluster_service.run_batch_clustering()
                st.success("Clustering complete.")
                st.rerun()

        map_df = cluster_service.get_market_map()
        if not map_df.empty:
            c1, c2 = st.columns([2, 1])
            with c1:
                fig = px.pie(map_df, values='total_jobs', names='cluster', title='Market Distribution by Cluster')
                st.plotly_chart(fig, use_container_width=True)
            with c2:
                st.write("**Cluster Performance**")
                st.dataframe(
                    map_df[['cluster', 'total_jobs', 'interview_rate']],
                    column_config={
                        "cluster": "Cluster",
                        "total_jobs": "Jobs",
                        "interview_rate": st.column_config.ProgressColumn("Conv %", format="%.1f%%", min_value=0, max_value=100)
                    },
                    hide_index=True
                )
        else:
            st.info("Run clustering to see your market map.")

    with tab2:
        st.subheader("Resume Gap Analysis (Strategic)")
        analysis = gap_service.analyze_gaps()
        
        col1, col2 = st.columns(2)
        with col1:
            st.write("**✅ Top Market Alignment**")
            st.write("Keywords you already cover that are high-demand:")
            for item in analysis['aligned']:
                st.write(f"- {item['keyword']} (found in {item['market_frequency']} jobs)")
        
        with col2:
            st.write("**❌ Top Strategic Gaps**")
            st.write("Demand exists but your resume is silent:")
            for item in analysis['gaps']:
                st.write(f"- **{item['keyword']}** (found in {item['market_frequency']} jobs)")

        st.divider()
        st.write("**Theme-Level Exposure**")
        if analysis['theme_gaps']:
            theme_df = pd.DataFrame([{"theme": k, "missing_keywords": v} for k, v in analysis['theme_gaps'].items()])
            fig_t = px.bar(theme_df, x='theme', y='missing_keywords', title='Missing Market Signals by Theme')
            st.plotly_chart(fig_t, use_container_width=True)

    with tab3:
        st.subheader("Job Posting De-duplication")
        st.write("Consolidate identical jobs from different sources (ATS vs Aggregators).")
        
        if st.button("🔍 Find and Merge Duplicates", use_container_width=True):
            with st.spinner("Analyzing similarities..."):
                dedupe_service.find_duplicates()
                st.success("Deduplication complete.")
                st.rerun()
        
        # Stats
        total = conn.execute("SELECT COUNT(*) FROM applications").fetchone()[0]
        canon = conn.execute("SELECT COUNT(*) FROM applications WHERE is_canonical = 1").fetchone()[0]
        dupes = total - canon
        
        st.metric("Total Records", total, delta=f"-{dupes} duplicates" if dupes > 0 else None)
        
        if dupes > 0:
            st.info(f"The system has identified {dupes} duplicate postings and hidden them from the main matches view.")
