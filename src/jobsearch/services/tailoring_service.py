from __future__ import annotations
import logging
import json
import sqlite3
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import List, Dict, Any, Optional

from jobsearch import ats_db
from jobsearch.services.llm_client import LLMClient
from jobsearch.services.resume_renderer import ResumeContent, ResumeHeader, ExperienceItem, ProjectItem, EducationItem, CompetencyGroup

logger = logging.getLogger(__name__)

@dataclass
class KeywordAnalysis:
    matched: List[str]
    missing: List[str]
    gaps: List[str]  # Critical missing skills/keywords

class TailoringService:
    """Service for identifying resume gaps and generating tailored application artifacts."""

    def __init__(self, conn: sqlite3.Connection, llm_client: Optional[LLMClient] = None):
        self.conn = conn
        self.llm_client = llm_client or LLMClient()

    def get_base_resume(self) -> Dict[str, str]:
        """Load the user's base resume from settings."""
        return {
            "name": ats_db.get_setting(self.conn, "base_resume_name", "Master Resume"),
            "text": ats_db.get_setting(self.conn, "base_resume_text", ""),
            "notes": ats_db.get_setting(self.conn, "base_resume_notes", ""),
        }

    def analyze_keywords(self, application_id: int) -> KeywordAnalysis:
        """Analyze JD keywords against the base resume."""
        app_row = ats_db.get_application(self.conn, application_id)
        if not app_row:
            return KeywordAnalysis([], [], [])
        
        app = dict(app_row)
        resume = self.get_base_resume()
        resume_text = resume["text"].lower()
        
        # Pull keywords from scoring/v2 columns if available
        jd_keywords = set()
        if app.get("matched_keywords"):
            jd_keywords.update(str(app["matched_keywords"]).lower().split("|"))
        if app.get("penalized_keywords"):
            jd_keywords.update(str(app["penalized_keywords"]).lower().split("|"))
            
        # If no keywords stored, we'd need to re-extract
        
        matched = []
        missing = []
        for kw in jd_keywords:
            kw = kw.strip()
            if not kw: continue
            if kw in resume_text:
                matched.append(kw)
            else:
                missing.append(kw)
                
        return KeywordAnalysis(
            matched=sorted(matched),
            missing=sorted(missing),
            gaps=sorted(missing[:10]) # Simplistic gap selection
        )

    def generate_draft(self, application_id: int, artifact_type: str) -> str:
        """Use LLM to generate a tailored artifact draft."""
        app_row = ats_db.get_application(self.conn, application_id)
        if not app_row:
            return ""
            
        app = dict(app_row)
        resume = self.get_base_resume()
        analysis = self.analyze_keywords(application_id)
        
        company = app.get("company", "the company")
        role = app.get("role", "the role")
        jd = app.get("description_excerpt", "")
        
        prompts = {
            "resume_summary": f"Rewrite my resume professional summary to target the {role} role at {company}. Emphasize these keywords: {', '.join(analysis.matched)}. Address these gaps if possible: {', '.join(analysis.gaps)}.",
            "cover_letter": f"Write a brief, punchy cover letter for {role} at {company}. Use my resume as context and explain why my background in {', '.join(analysis.matched[:3])} makes me a great fit.",
            "outreach_note": f"Write a 2-sentence LinkedIn outreach note to a recruiter for {role} at {company}.",
            "why_company": f"Based on the company {company} and the JD {jd[:500]}, give me 3 specific points on why I want to work here.",
            "why_role": f"Based on my resume and the JD {jd[:500]}, give me 3 specific points on why I am the best fit for this specific role.",
            "resume_bullets": f"Suggest 3 tailored resume bullets based on my experience that emphasize {', '.join(analysis.gaps[:3])} which are required by the JD."
        }
        
        instruction = prompts.get(artifact_type, "Draft a tailored note for this application.")
        
        prompt = f"""
        You are an expert career coach and resume strategist.
        
        Context:
        - Target Role: {role}
        - Target Company: {company}
        - JD Excerpt: {jd[:1000]}
        - My Base Resume: {resume['text'][:2000]}
        
        Instruction: {instruction}
        
        Output only the copy-ready draft text. No preamble.
        """
        
        return self.llm_client.chat(prompt)

    def generate_resume_json(self, application_id: int) -> str:
        """Generate structured JSON for the Andy Warthog resume template."""
        app_row = ats_db.get_application(self.conn, application_id)
        if not app_row:
            return ""
            
        app = dict(app_row)
        resume = self.get_base_resume()
        analysis = self.analyze_keywords(application_id)
        
        prompt = f"""
        You are an expert career strategist specializing in high-end resume tailoring.
        
        Generate resume content for the Andy Warthog style template.
        Context:
        - Role: {app.get('role')} @ {app.get('company')}
        - JD: {app.get('description_excerpt', '')[:1000]}
        - My Base Resume: {resume['text'][:3000]}
        - Matched Keywords: {', '.join(analysis.matched)}
        - Gaps to Address: {', '.join(analysis.gaps)}
        
        Output MUST be a single valid JSON object following this schema:
        {{
          "header": {{
            "name": "User Name",
            "headline": "Role Title | Key Value Prop",
            "phone": "User Phone",
            "email": "User Email",
            "portfolio_url": "Portfolio Link",
            "linkedin_url": "LinkedIn Link"
          }},
          "summary": "Tailored 3-4 line professional summary.",
          "core_competencies": [
            {{ "label": "Technical Stack", "items": ["Skill 1", "Skill 2"] }},
            {{ "label": "Domain Expertise", "items": ["Skill 3", "Skill 4"] }},
            {{ "label": "Leadership", "items": ["Skill 5", "Skill 6"] }}
          ],
          "experience": [
            {{
              "role": "Job Title",
              "company": "Company Name",
              "location": "City, ST",
              "dates": "Start - End",
              "bullets": ["Result-oriented bullet 1", "Result-oriented bullet 2"]
            }}
          ],
          "projects": [
            {{ "title": "Project name", "description": "1-line description", "link": "url" }}
          ],
          "education": [
            {{ "degree": "Degree name", "school": "School name", "year": "YYYY" }}
          ],
          "awards": ["Award 1", "Award 2"]
        }}
        
        Constraints:
        - DO NOT invent layout, styling, or formatting.
        - Summary max 4 lines.
        - Max 3 bullets per role.
        - Max 2 projects.
        - Return JSON only. No preamble or postscript.
        """
        
        res = self.llm_client.chat(prompt)
        # Cleanup potential markdown blocks
        if "```json" in res:
            res = res.split("```json")[1].split("```")[0].strip()
        elif "```" in res:
            res = res.split("```")[1].split("```")[0].strip()
        return res

    def save_artifact(self, application_id: int, artifact_type: str, content: str, notes: Optional[str] = None):
        """Persist a tailored artifact."""
        return ats_db.add_tailored_artifact(self.conn, application_id, artifact_type, content, notes)

    def get_artifacts(self, application_id: int) -> List[Dict[str, Any]]:
        """Retrieve all tailored artifacts for an application."""
        rows = ats_db.get_tailored_artifacts_for_application(self.conn, application_id)
        return [dict(row) for row in rows]
