import json
import asyncio
from pathlib import Path
import streamlit as st
import pandas as pd
from datetime import datetime
from typing import List, Dict, Any, Optional

from jobsearch.services.tailoring_service import TailoringService, KeywordAnalysis
from jobsearch.services.resume_renderer import AndyWarthogRenderer, ResumeContent, ResumeHeader, ExperienceItem, ProjectItem, EducationItem, CompetencyGroup
from jobsearch.services.package_service import ApplicationPackageService
from jobsearch.views.resume_form_editor import render_resume_form
from jobsearch import ats_db

def render_tailoring_studio(conn):
    st.header("🎨 Tailoring Studio")
    st.write("Turn a promising job into a high-quality tailored application.")

    # 1. Application Selector
    apps = ats_db.get_applications(conn)
    # Filter to active stages
    active_apps = [a for r in [apps] for a in r if a['status'] in ('considering', 'applied', 'interviewing')]

    if not active_apps:
        st.warning("No active applications found. Add or discover some jobs first!")
        return

    options = {f"{a['company']} - {a['role']} ({a['status']})": a['id'] for a in active_apps}
    selected_label = st.selectbox("Select Application to Tailor", options=list(options.keys()))
    app_id = options[selected_label]

    app = ats_db.get_application(conn, app_id)
    service = TailoringService(conn)
    renderer = AndyWarthogRenderer()
    package_service = ApplicationPackageService(conn)

    tab_drafting, tab_templates, tab_package = st.tabs(["✍️ Artifact Drafting", "🖼️ Resume Templates", "📦 Package & Export"])

    with tab_drafting:
        col1, col2 = st.columns([1, 1])

        with col1:
            st.subheader("📋 JD & Context")
            with st.expander("View JD Excerpt", expanded=False):
                st.write(app['description_excerpt'] or "No description available.")

            st.write(f"**Company:** {app['company']}")
            st.write(f"**Current Score:** {app['score']:.1f} ({app['fit_band']})")

            analysis = service.analyze_keywords(app_id)
            st.subheader("🔑 Keyword Analysis")

            k_tabs = st.tabs(["Matched", "Missing / Gaps"])
            with k_tabs[0]:
                if analysis.matched:
                    st.write(", ".join([f"✅ {k}" for k in analysis.matched]))
                else:
                    st.info("No matches detected.")

            with k_tabs[1]:
                if analysis.missing:
                    st.write(", ".join([f"❌ {k}" for k in analysis.missing]))
                    st.caption("💡 Try to incorporate these keywords into your tailored resume or cover letter.")
                else:
                    st.success("Your resume covers all major JD keywords!")

        with col2:
            st.subheader("✍️ AI Drafting")
            artifact_types = {
                "resume_summary": "Resume Summary",
                "resume_bullets": "Refined Bullets",
                "cover_letter": "Cover Letter",
                "outreach_note": "Recruiter Note",
                "why_company": "Why this Company?",
                "why_role": "Why this Role?"
            }

            selected_type = st.selectbox("Choose Artifact to Draft", options=list(artifact_types.keys()), format_func=lambda x: artifact_types[x])

            if st.button(f"🪄 Generate Draft", key="gen_draft_btn", use_container_width=True):
                with st.spinner("Drafting with AI..."):
                    try:
                        draft = service.generate_draft(app_id, selected_type)
                        st.session_state[f"draft_{selected_type}"] = draft
                    except Exception as e:
                        st.error(f"Drafting failed: {e}")

            current_draft = st.session_state.get(f"draft_{selected_type}", "")
            draft_content = st.text_area("Draft Content", value=current_draft, height=300, key=f"editor_{selected_type}")

            d_col1, d_col2 = st.columns(2)
            if d_col1.button("💾 Save to Application", key="save_art_btn", use_container_width=True):
                service.save_artifact(app_id, selected_type, draft_content)
                st.success(f"Saved {artifact_types[selected_type]}!")

            if d_col2.button("📋 Copy to Clipboard", key="copy_art_btn", use_container_width=True):
                st.info("Select all text and press Ctrl+C to copy.")

    with tab_templates:
        st.subheader("Andy Warthog - Template Studio")

        # State management for current working content
        if "working_content" not in st.session_state:
            # Try to load latest saved JSON for this app
            saved = service.get_artifacts(app_id)
            json_art = next((a for a in saved if a['artifact_type'] == "resume_json_warthog"), None)
            if json_art:
                st.session_state["working_content"] = json.loads(json_art['content'])
            else:
                st.session_state["working_content"] = {}

        t_col1, t_col2 = st.columns([1, 1])

        with t_col1:
            m_col1, m_col2 = st.columns([2, 1])
            m_col1.write("Edit your resume content below.")
            advanced_mode = m_col2.toggle("Advanced JSON Mode", value=False)

            if st.button("🪄 AI Tailor Full Resume (Initial Draft)", use_container_width=True):
                with st.spinner("Analyzing JD and tailoring content..."):
                    json_res = service.generate_resume_json(app_id)
                    try:
                        st.session_state["working_content"] = json.loads(json_res)
                    except:
                        st.error("AI returned invalid JSON. Try again.")

            # Map raw dict to ResumeContent for form/preview
            current_data = st.session_state["working_content"]
            try:
                working_obj = _map_dict_to_obj(current_data)
            except:
                working_obj = ResumeContent(header=ResumeHeader(name="Set Name"))

            if advanced_mode:
                edited_json = st.text_area("Resume JSON", value=json.dumps(current_data, indent=2), height=600)
                try:
                    st.session_state["working_content"] = json.loads(edited_json)
                except:
                    st.error("Invalid JSON syntax")
            else:
                # STRUCTURED FORM EDITOR
                updated_obj = render_resume_form(working_obj)
                st.session_state["working_content"] = _map_obj_to_dict(updated_obj)

            if st.button("💾 Save as New Version", use_container_width=True):
                service.save_artifact(app_id, "resume_json_warthog", json.dumps(st.session_state["working_content"]))
                st.success("Version saved!")

        with t_col2:
            st.write("**Live Preview & Validation**")

            # Use current working content
            try:
                # Validation & Normalization Feedback
                norm_obj, warnings = renderer.normalize(_map_dict_to_obj(st.session_state["working_content"]))

                if warnings:
                    with st.expander(f"⚠️ Layout Warnings ({len(warnings)})", expanded=True):
                        for w in warnings:
                            st.write(w)
                else:
                    st.success("✅ Content fits the template perfectly.")

                # Styled preview
                html = renderer.to_html(norm_obj)
                st.components.v1.html(html, height=700, scrolling=True)

                st.divider()
                st.write("**Quick Export**")
                ex_col1, ex_col2, ex_col3 = st.columns(3)
                if ex_col1.button("📄 Styled PDF"):
                    _export_pdf(renderer, html, app, "Warthog")
                if ex_col2.button("📜 ATS-Safe PDF"):
                    ats_html = renderer.to_html(norm_obj, ats_safe=True)
                    _export_pdf(renderer, ats_html, app, "ATS")
                if ex_col3.button("📝 DOCX Resume"):
                    _export_docx(renderer, norm_obj, app)

            except Exception as e:
                st.error(f"Render failed: {e}")

    with tab_package:
        st.subheader("📦 Application Package")
        st.write("Bundle all tailored materials into a single ZIP file for submission.")

        saved_artifacts = service.get_artifacts(app_id)
        if not saved_artifacts:
            st.info("No tailored artifacts found for this application. Draft some materials first!")
        else:
            st.write("**Contents to be included:**")
            for art in saved_artifacts:
                st.write(f"- {art['artifact_type']} (Last updated: {art['updated_at']})")

            if st.button("🚀 Generate Application Package (ZIP)", use_container_width=True):
                with st.spinner("Rendering PDFs and bundling artifacts..."):
                    try:
                        zip_bytes, zip_filename = asyncio.run(package_service.create_package(app_id))
                        st.download_button(
                            label="📥 Download ZIP Package",
                            data=zip_bytes,
                            file_name=zip_filename,
                            mime="application/zip",
                            use_container_width=True
                        )
                        st.success(f"Package '{zip_filename}' generated successfully!")
                    except Exception as e:
                        st.error(f"Package generation failed: {e}")

    st.divider()
    st.subheader("📚 Saved Artifacts History")
    artifacts = service.get_artifacts(app_id)
    if not artifacts:
        st.info("No tailored artifacts saved yet.")
    else:
        for art in artifacts:
            label = art['artifact_type']
            if art['artifact_type'] == "resume_json_warthog":
                label = "Andy Warthog JSON"

            with st.expander(f"{label} - {art['updated_at']}", expanded=False):
                if art['artifact_type'] == "resume_json_warthog":
                    if st.button("📝 Load into Editor", key=f"load_{art['id']}"):
                        st.session_state["working_content"] = json.loads(art['content'])
                        st.rerun()
                st.code(art['content'])
                if st.button("🗑️ Delete", key=f"del_{art['id']}"):
                    ats_db.delete_tailored_artifact(conn, art['id'])
                    st.rerun()

def _map_dict_to_obj(data: Dict[str, Any]) -> ResumeContent:
    header = ResumeHeader(**data.get("header", {}))
    comps = [CompetencyGroup(**g) for g in data.get("core_competencies", [])]
    exps = [ExperienceItem(**e) for e in data.get("experience", [])]
    projs = [ProjectItem(**p) for p in data.get("projects", [])]
    edus = [EducationItem(**ed) for ed in data.get("education", [])]
    return ResumeContent(
        header=header,
        summary=data.get("summary", ""),
        core_competencies=comps,
        experience=exps,
        projects=projs,
        education=edus,
        awards=data.get("awards", []),
        template_name=data.get("template_name", "Andy Warthog")
    )

def _map_obj_to_dict(obj: ResumeContent) -> Dict[str, Any]:
    from dataclasses import asdict
    return asdict(obj)

def _export_pdf(renderer, html, app, label):
    import asyncio
    out_dir = Path("results/resumes")
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / f"Tailored_Resume_{app['company']}_{label}.pdf"
    asyncio.run(renderer.render_pdf(html, out_path))
    st.success(f"PDF saved to {out_path}")

def _export_docx(renderer, content, app):
    out_dir = Path("results/resumes")
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / f"Tailored_Resume_{app['company']}_Warthog.docx"
    renderer.render_docx(content, out_path)
    st.success(f"DOCX saved to {out_path}")
