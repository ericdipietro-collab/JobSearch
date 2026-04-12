import streamlit as st
import asyncio
from datetime import datetime
from jobsearch.services.submission_review_service import SubmissionReviewService, SubmissionQueueItem
from jobsearch.services.package_service import ApplicationPackageService
from jobsearch.views.readiness_components import render_readiness_badge

def render_submission_review(conn):
    st.header("🛫 Submission Review")
    st.write("The final cockpit for manual applications. Review, export, and confirm your submissions.")

    service = SubmissionReviewService(conn)
    package_service = ApplicationPackageService(conn)
    
    queue = service.get_queue()
    
    if not queue:
        st.success("🎉 Your submission queue is empty! Great work.")
        st.info("💡 Hint: Use the Tailoring Studio to prepare more applications or run a new scrape.")
        return

    # 1. Queue Sidebar / Navigation
    st.sidebar.subheader("Queue")
    
    # Summary Metrics in Sidebar
    ready_count = len([item for item in queue if item.status == 'considering' and item.readiness.status == 'ready'])
    prepared_count = len([item for item in queue if item.status == 'prepared'])
    
    st.sidebar.markdown(f"✅ **{ready_count}** Ready to Review")
    st.sidebar.markdown(f"🟣 **{prepared_count}** Prepared")
    
    options = {f"{item.company} - {item.role}": i for i, item in enumerate(queue)}
    selected_idx = st.sidebar.radio("Select Application", options=list(options.keys()), index=0)
    
    item = queue[options[selected_idx]]
    
    # 2. Main Review Pane
    st.divider()
    c1, c2 = st.columns([3, 1])
    with c1:
        st.subheader(f"{item.company}")
        st.markdown(f"**{item.role}**")
        st.caption(f"Current Status: {item.status.upper()}")
    with c2:
        badge_html = render_readiness_badge(item.readiness)
        st.markdown(badge_html, unsafe_allow_html=True)

    # 3. Checklist & Materials
    st.write("---")
    l_col, r_col = st.columns([1, 1])
    
    with l_col:
        st.write("**Pre-flight Checklist**")
        all_passed = True
        
        # Freshness Check
        freshness = service.evaluate_freshness(item.application_id)
        if freshness["status"] == "fresh":
            st.write("✅ Package Freshness")
        elif freshness["status"] == "missing_export":
            st.write("❌ Package Freshness")
            st.caption("_No export generated yet._")
            all_passed = False
        else:
            st.write("⚠️ Package Freshness")
            st.warning(freshness["reason"])
            if st.button("🔄 Re-export Now", key=f"re_exp_{item.application_id}"):
                # This is handled by the Export button below, 
                # but we can trigger a rerun to refresh state
                st.rerun()

        for check in item.checklist:
            if check.status:
                st.write(f"✅ {check.label}")
            else:
                st.write(f"❌ {check.label}")
                if check.is_required: all_passed = False
            st.caption(f"_{check.description}_")

    with r_col:
        st.write("**Application Materials**")
        if item.apply_url:
            st.link_button("🔗 Open Apply Portal", item.apply_url, use_container_width=True)
        else:
            st.warning("Missing Apply URL")
            
        if st.button("📦 Export Package (ZIP)", use_container_width=True, help="Download all tailored artifacts for submission"):
            with st.spinner("Generating..."):
                try:
                    zip_bytes, filename = asyncio.run(package_service.create_package(item.application_id))
                    st.download_button(
                        label="📥 Download ZIP",
                        data=zip_bytes,
                        file_name=filename,
                        mime="application/zip",
                        use_container_width=True,
                        on_click=lambda: service.record_export(item.application_id)
                    )
                    st.success("Package ready.")
                except Exception as e:
                    st.error(f"Export failed: {e}")
        
        if item.last_exported_at:
            st.caption(f"Last exported: {item.last_exported_at}")

    # 4. Confirmation Workflow
    st.divider()
    st.write("**Final Confirmation**")
    
    if item.status == 'considering':
        if st.button("🟣 Mark as Prepared", use_container_width=True, help="Move to prepared list after reviewing materials."):
            service.mark_prepared(item.application_id)
            st.success("Moved to Prepared.")
            st.rerun()

    conf_c1, conf_c2, conf_c3 = st.columns(3)
    
    with conf_c1:
        if st.button("✔️ Mark as Submitted", type="primary", use_container_width=True):
            service.mark_submitted(item.application_id)
            st.success("Submission tracked!")
            st.rerun()
            
    with conf_c2:
        with st.popover("❌ Blocked / Closed", use_container_width=True):
            reason = st.selectbox("Reason", options=list(service.BLOCKER_REASONS.keys()), format_func=lambda x: service.BLOCKER_REASONS[x])
            b_notes = st.text_area("Notes")
            if st.button("Confirm Blocker"):
                service.log_blocker(item.application_id, reason, b_notes)
                st.rerun()

    with conf_c3:
        if st.button("📝 Needs Revision", use_container_width=True):
            service.log_blocker(item.application_id, "revision_needed", "User requested revision from review cockpit.")
            st.info("Moved back to Considering.")
            st.rerun()
