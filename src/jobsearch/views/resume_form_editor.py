import streamlit as st
from typing import List, Dict, Any, Tuple
from jobsearch.services.resume_renderer import ResumeContent, ResumeHeader, ExperienceItem, ProjectItem, EducationItem, CompetencyGroup

def render_resume_form(content: ResumeContent) -> ResumeContent:
    """Provides a structured form for editing ResumeContent."""
    
    st.subheader("👤 Header & Contact")
    c1, c2 = st.columns(2)
    content.header.name = c1.text_input("Name", value=content.header.name)
    content.header.headline = c2.text_input("Headline / Value Prop", value=content.header.headline)
    
    c3, c4 = st.columns(2)
    content.header.email = c3.text_input("Email", value=content.header.email)
    content.header.phone = c4.text_input("Phone", value=content.header.phone)
    
    c5, c6 = st.columns(2)
    content.header.portfolio_url = c5.text_input("Portfolio URL", value=content.header.portfolio_url)
    content.header.linkedin_url = c6.text_input("LinkedIn URL", value=content.header.linkedin_url)

    st.divider()
    st.subheader("📝 Professional Summary")
    content.summary = st.text_area("Summary", value=content.summary, height=100, help="Max 450 characters for ideal layout.")

    st.divider()
    st.subheader("🛠 Core Competencies")
    # Competency Groups (Fixed at 3 for Andy Warthog layout)
    if not content.core_competencies:
        content.core_competencies = [
            CompetencyGroup("Technical Stack", []),
            CompetencyGroup("Domain Expertise", []),
            CompetencyGroup("Leadership", [])
        ]
    
    for i, group in enumerate(content.core_competencies[:3]):
        st.write(f"**Group {i+1}: {group.label}**")
        group.label = st.text_input(f"Label", value=group.label, key=f"comp_label_{i}")
        items_str = st.text_input(f"Items (comma separated)", value=", ".join(group.items), key=f"comp_items_{i}")
        group.items = [item.strip() for item in items_str.split(",") if item.strip()]

    st.divider()
    st.subheader("💼 Professional Experience")
    
    # Simple list editor for Experience
    new_experience = []
    for i, exp in enumerate(content.experience):
        with st.expander(f"Experience {i+1}: {exp.role} at {exp.company}", expanded=(i==0)):
            role = st.text_input("Role", value=exp.role, key=f"exp_role_{i}")
            company = st.text_input("Company", value=exp.company, key=f"exp_co_{i}")
            dates = st.text_input("Dates", value=exp.dates, key=f"exp_dates_{i}")
            loc = st.text_input("Location", value=exp.location, key=f"exp_loc_{i}")
            bullets_str = st.text_area("Bullets (one per line)", value="\n".join(exp.bullets), key=f"exp_bullets_{i}", height=100)
            bullets = [b.strip() for b in bullets_str.split("\n") if b.strip()]
            
            if st.button(f"🗑 Remove Role", key=f"rem_exp_{i}"):
                continue # Skip adding to new_experience
            new_experience.append(ExperienceItem(role, company, loc, dates, bullets))
            
    if st.button("➕ Add Experience Role"):
        new_experience.append(ExperienceItem("New Role", "Company", "City, ST", "Start - End", []))
    content.experience = new_experience

    st.divider()
    st.subheader("🚀 Featured Projects")
    new_projects = []
    for i, proj in enumerate(content.projects):
        with st.expander(f"Project {i+1}: {proj.title}", expanded=True):
            title = st.text_input("Title", value=proj.title, key=f"proj_title_{i}")
            desc = st.text_input("Description", value=proj.description, key=f"proj_desc_{i}")
            link = st.text_input("Link", value=proj.link or "", key=f"proj_link_{i}")
            
            if st.button(f"🗑 Remove Project", key=f"rem_proj_{i}"):
                continue
            new_projects.append(ProjectItem(title, desc, link))
            
    if len(new_projects) < 2:
        if st.button("➕ Add Project"):
            new_projects.append(ProjectItem("New Project", "One-line description"))
    content.projects = new_projects

    st.divider()
    st.subheader("🎓 Education")
    new_edu = []
    for i, edu in enumerate(content.education):
        with st.expander(f"Education {i+1}: {edu.degree}", expanded=False):
            degree = st.text_input("Degree", value=edu.degree, key=f"edu_deg_{i}")
            school = st.text_input("School", value=edu.school, key=f"edu_sch_{i}")
            year = st.text_input("Year", value=edu.year, key=f"edu_yr_{i}")
            
            if st.button(f"🗑 Remove Education", key=f"rem_edu_{i}"):
                continue
            new_edu.append(EducationItem(degree, school, year))
            
    if st.button("➕ Add Education"):
        new_edu.append(EducationItem("Bachelor of...", "University", "YYYY"))
    content.education = new_edu

    st.divider()
    st.subheader("🏆 Awards & Recognition")
    awards_str = st.text_area("Awards (one per line)", value="\n".join(content.awards), height=100)
    content.awards = [a.strip() for a in awards_str.split("\n") if a.strip()]

    return content
