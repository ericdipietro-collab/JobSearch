from __future__ import annotations
import logging
import json
import sqlite3
from dataclasses import dataclass
from typing import List, Dict, Any, Optional

from jobsearch import ats_db
from jobsearch.services.resume_renderer import AndyWarthogRenderer, ResumeContent, ResumeHeader, ExperienceItem, ProjectItem, EducationItem, CompetencyGroup

logger = logging.getLogger(__name__)

@dataclass
class ReadinessState:
    status: str  # 'blocked', 'draft', 'ready'
    reason: str
    has_resume: bool = False
    has_cover_letter: bool = False
    has_outreach: bool = False
    critical_issues: List[str] = None

class ReadinessService:
    """Evaluates if an application package is ready for export/submission."""

    def __init__(self, conn: sqlite3.Connection):
        self.conn = conn
        self.renderer = AndyWarthogRenderer()

    def evaluate(self, application_id: int) -> ReadinessState:
        artifacts = ats_db.get_tailored_artifacts_for_application(self.conn, application_id)
        
        has_resume = any(a['artifact_type'] == 'resume_json_warthog' for a in artifacts)
        has_cl = any(a['artifact_type'] == 'cover_letter' for a in artifacts)
        has_outreach = any(a['artifact_type'] == 'outreach_note' for a in artifacts)
        
        # 1. Check for basic existence
        if not has_resume:
            return ReadinessState('blocked', "Missing tailored resume JSON.", has_resume, has_cl, has_outreach)

        # 2. Deep validation of the resume content
        resume_art = next(a for a in artifacts if a['artifact_type'] == 'resume_json_warthog')
        try:
            data = json.loads(resume_art['content'])
            obj = self._map_dict_to_obj(data)
            _, issues = self.renderer.normalize(obj)
            
            critical = [i.message for i in issues if i.severity == 'critical']
            if critical:
                return ReadinessState('blocked', f"Critical issues: {critical[0]}", has_resume, has_cl, has_outreach, critical)
            
            warnings = [i.message for i in issues if i.severity == 'warning']
            
            if has_cl and has_outreach:
                return ReadinessState('ready', "Complete: Resume, Cover Letter, and Outreach ready.", True, True, True)
            
            if has_cl or has_outreach:
                return ReadinessState('ready', "Partial: Resume + some artifacts ready.", True, has_cl, has_outreach)
                
            return ReadinessState('draft', "Draft: Resume valid but package is incomplete.", True, False, False)

        except Exception as e:
            return ReadinessState('blocked', f"Failed to parse resume: {str(e)}", has_resume, has_cl, has_outreach)

    def evaluate_batch(self, application_ids: List[int]) -> Dict[int, ReadinessState]:
        """Evaluates readiness for multiple IDs efficiently."""
        if not application_ids:
            return {}
            
        placeholders = ",".join(["?"] * len(application_ids))
        rows = self.conn.execute(
            f"SELECT * FROM tailored_artifacts WHERE application_id IN ({placeholders})",
            application_ids
        ).fetchall()
        
        # Group by app_id
        by_app = {}
        for row in rows:
            aid = row['application_id']
            if aid not in by_app: by_app[aid] = []
            by_app[aid].append(dict(row))
            
        results = {}
        for aid in application_ids:
            artifacts = by_app.get(aid, [])
            has_resume = any(a['artifact_type'] == 'resume_json_warthog' for a in artifacts)
            has_cl = any(a['artifact_type'] == 'cover_letter' for a in artifacts)
            has_outreach = any(a['artifact_type'] == 'outreach_note' for a in artifacts)
            
            if not has_resume:
                results[aid] = ReadinessState('blocked', "Missing resume", has_resume, has_cl, has_outreach)
                continue
                
            try:
                resume_art = next(a for a in artifacts if a['artifact_type'] == 'resume_json_warthog')
                data = json.loads(resume_art['content'])
                obj = self._map_dict_to_obj(data)
                _, issues = self.renderer.normalize(obj)
                critical = [i.message for i in issues if i.severity == 'critical']
                
                if critical:
                    results[aid] = ReadinessState('blocked', critical[0], True, has_cl, has_outreach, critical)
                elif has_cl and has_outreach:
                    results[aid] = ReadinessState('ready', "Ready", True, True, True)
                elif has_cl or has_outreach:
                    results[aid] = ReadinessState('ready', "Partial", True, has_cl, has_outreach)
                else:
                    results[aid] = ReadinessState('draft', "Draft", True, False, False)
            except:
                results[aid] = ReadinessState('blocked', "Error parsing", has_resume, has_cl, has_outreach)
                
        return results

    def _map_dict_to_obj(self, data: Dict[str, Any]) -> ResumeContent:
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
