import streamlit as st
import pandas as pd
import plotly.express as px
from jobsearch.services.learning_loop_service import LearningLoopService
from jobsearch import ats_db

def render_learning_loop(conn):
    st.header("🔄 Learning Loop & Calibration")
    st.write("Analyze search outcomes to optimize your targeting and scoring strategy.")

    service = LearningLoopService(conn)
    
    # 1. Insights & Alerts
    insights = service.get_calibration_insights()
    if insights:
        st.subheader("💡 System Insights")
        for ins in insights:
            if ins['type'] == 'warning':
                st.warning(f"**{ins['title']}**  \n{ins['reason']}")
            else:
                st.success(f"**{ins['title']}**  \n{ins['reason']}")
    
    st.divider()
    
    # 2. Score Calibration
    st.subheader("📊 Score vs. Outcome Correlation")
    score_df = service.get_score_vs_outcome()
    if score_df.empty:
        st.info("Not enough application data to show correlations yet.")
    else:
        c1, c2 = st.columns([2, 1])
        with c1:
            # Bar chart for interest rate by score band
            fig = px.bar(
                score_df, 
                x="score_band", 
                y="interest_rate",
                title="Interview Rate (%) by Score Band",
                labels={"score_band": "Match Score Band", "interest_rate": "Interview Rate %"},
                color="interest_rate",
                color_continuous_scale="Viridis"
            )
            st.plotly_chart(fig, use_container_width=True)
        with c2:
            st.write("**Outcome Breakdown**")
            st.dataframe(
                score_df,
                column_config={
                    "score_band": "Score Band",
                    "total": "Applied",
                    "interested": "Interviews",
                    "interest_rate": st.column_config.ProgressColumn("Rate", format="%.1f%%", min_value=0, max_value=100)
                },
                hide_index=True
            )

    st.divider()
    
    # 3. Dimensions & Friction
    st.subheader("🏗️ Conversion & Friction")
    tab1, tab2, tab3 = st.tabs(["Top Companies", "Title Families", "Submission Friction"])
    
    with tab1:
        co_df = service.get_dimension_conversion("company")
        if not co_df.empty:
            st.dataframe(
                co_df,
                column_config={
                    "dimension": "Company",
                    "applied": "Applied",
                    "interviews": "Interviews",
                    "conversion_rate": st.column_config.ProgressColumn("Conv. Rate", format="%.1f%%", min_value=0, max_value=100)
                },
                hide_index=True,
                use_container_width=True
            )
        else:
            st.info("Add more application outcomes to see company conversion rates.")
            
    with tab2:
        title_df = service.get_dimension_conversion("title")
        if not title_df.empty:
            st.dataframe(
                title_df,
                column_config={
                    "dimension": "Title Family",
                    "applied": "Applied",
                    "interviews": "Interviews",
                    "conversion_rate": st.column_config.ProgressColumn("Conv. Rate", format="%.1f%%", min_value=0, max_value=100)
                },
                hide_index=True,
                use_container_width=True
            )
        else:
            st.info("Add more application outcomes to see title family conversion rates.")

    with tab3:
        f_col1, f_col2 = st.columns([1, 1])
        with f_col1:
            st.write("**Final Mile Conversion**")
            funnel = service.get_final_mile_funnel()
            st.metric("Ever Prepared", funnel["ever_prepared"])
            st.metric("Total Submitted", funnel["total_submitted"])
            if funnel["ever_prepared"] > 0:
                rate = (funnel["total_submitted"] / funnel["ever_prepared"]) * 100
                st.write(f"**Completion Rate:** {rate:.1f}%")
        
        with f_col2:
            st.write("**Top Submission Blockers**")
            friction_df = service.get_submission_friction_stats()
            if not friction_df.empty:
                fig_f = px.bar(friction_df, x="reason", y="count", title="Reasons for Non-Submission")
                st.plotly_chart(fig_f, use_container_width=True)
            else:
                st.info("No submission blockers recorded yet.")

        st.write("**ATS Friction Report**")
        ats_f = service.get_ats_friction_report()
        if not ats_f.empty:
            st.dataframe(ats_f, hide_index=True, use_container_width=True)

    st.divider()
    
    # 4. Keywords
    st.subheader("🔑 Keyword ROI")
    kw_df = service.get_keyword_correlation()
    if not kw_df.empty:
        st.write("Keywords that appear most frequently in roles where you landed an interview.")
        st.dataframe(
            kw_df,
            column_config={
                "keyword": "Keyword",
                "applied": "Total Found",
                "success": "In Interviews",
                "rate": st.column_config.ProgressColumn("Success Rate", format="%.1f%%", min_value=0, max_value=100)
            },
            hide_index=True,
            use_container_width=True
        )
    else:
        st.info("Not enough keyword data yet. Keep tracking application outcomes.")
