import streamlit as st
import asyncio
from typing import Any
from jobsearch.services.readiness_service import ReadinessService, ReadinessState
from jobsearch.services.package_service import ApplicationPackageService

def render_quick_export(conn: Any, application_id: int):
    """Reusable UI component for fast package export with readiness validation."""
    
    readiness_service = ReadinessService(conn)
    package_service = ApplicationPackageService(conn)
    
    state = readiness_service.evaluate(application_id)
    
    # 1. Header & Status
    c1, c2 = st.columns([3, 1])
    with c1:
        if state.status == 'ready':
            st.success(f"✅ **{state.reason}**")
        elif state.status == 'draft':
            st.info(f"📝 **{state.reason}**")
        else:
            st.error(f"🛑 **{state.status.capitalize()}**: {state.reason}")
            
    with c2:
        if state.status != 'blocked' or not st.session_state.get('settings', {}).get('tailoring_block_on_critical', True):
            if st.button("🚀 Quick Export (ZIP)", use_container_width=True, key=f"quick_exp_{application_id}"):
                with st.spinner("Bundling package..."):
                    try:
                        zip_bytes, filename = asyncio.run(package_service.create_package(application_id))
                        st.download_button(
                            label="📥 Download Now",
                            data=zip_bytes,
                            file_name=filename,
                            mime="application/zip",
                            use_container_width=True,
                            key=f"dl_btn_{application_id}"
                        )
                    except Exception as e:
                        st.error(f"Export failed: {e}")
        else:
            st.button("🚀 Export Locked", disabled=True, use_container_width=True, 
                      help="Fix critical issues to unlock export.", 
                      key=f"export_locked_{application_id}")

    # 2. Detailed Breakdown
    with st.expander("Submission Readiness Details"):
        st.write("**Artifact Checklist:**")
        st.write(f"{'✅' if state.has_resume else '❌'} Tailored Resume")
        st.write(f"{'✅' if state.has_cover_letter else '❌'} Cover Letter")
        st.write(f"{'✅' if state.has_outreach else '❌'} Recruiter Outreach Note")
        
        if state.critical_issues:
            st.write("**Critical Issues:**")
            for issue in state.critical_issues:
                st.write(f"- 🔴 {issue}")
        
        st.caption("Tip: Use the Tailoring Studio to fix missing items or critical errors.")
