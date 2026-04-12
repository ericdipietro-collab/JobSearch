from __future__ import annotations
import logging
import json
import io
import zipfile
import asyncio
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, Any, List, Optional

from jobsearch import ats_db
from jobsearch.services.resume_renderer import AndyWarthogRenderer, ResumeContent, ResumeHeader, ExperienceItem, ProjectItem, EducationItem, CompetencyGroup

logger = logging.getLogger(__name__)

class ApplicationPackageService:
    """Service for bundling all tailored artifacts for an application into a single ZIP."""

    def __init__(self, conn: Any):
        self.conn = conn
        self.renderer = AndyWarthogRenderer()

    async def create_package(self, application_id: int) -> tuple[bytes, str]:
        """
        Gathers all artifacts, renders PDFs, and returns (zip_bytes, filename).
        """
        app = ats_db.get_application(self.conn, application_id)
        if not app:
            raise ValueError(f"Application {application_id} not found.")

        artifacts = ats_db.get_tailored_artifacts_for_application(self.conn, application_id)
        
        zip_buffer = io.BytesIO()
        timestamp = datetime.now().strftime("%Y%m%d_%H%M")
        safe_company = "".join([c if c.isalnum() else "_" for c in app['company']])
        filename = f"Application_Package_{safe_company}_{timestamp}.zip"

        manifest = {
            "company": app['company'],
            "role": app['role'],
            "package_date": datetime.now(timezone.utc).isoformat(),
            "artifacts": []
        }

        with zipfile.ZipFile(zip_buffer, "w", zipfile.ZIP_DEFLATED) as zip_file:
            for art in artifacts:
                art_type = art['artifact_type']
                content = art['content']
                
                if art_type == "resume_json_warthog":
                    # 1. Render Styled PDF
                    try:
                        data = json.loads(content)
                        resume_content = self._map_to_resume_content(data)
                        
                        # Styled HTML
                        html_styled = self.renderer.to_html(resume_content)
                        # We render PDF to a temporary buffer if possible, or a real temp file
                        # For simplicity in this env, we'll use a local temp path
                        temp_pdf = Path("results/temp_styled.pdf")
                        temp_pdf.parent.mkdir(parents=True, exist_ok=True)
                        await self.renderer.render_pdf(html_styled, temp_pdf)
                        zip_file.write(temp_pdf, "resume_styled.pdf")
                        manifest["artifacts"].append({"file": "resume_styled.pdf", "type": "Styled Resume"})
                        
                        # 2. Render ATS-Safe PDF
                        html_ats = self.renderer.to_html(resume_content, ats_safe=True)
                        temp_ats = Path("results/temp_ats.pdf")
                        await self.renderer.render_pdf(html_ats, temp_ats)
                        zip_file.write(temp_ats, "resume_ats_safe.pdf")
                        manifest["artifacts"].append({"file": "resume_ats_safe.pdf", "type": "ATS-Safe Resume"})
                        
                        # 3. Render DOCX
                        temp_docx = Path("results/temp_resume.docx")
                        self.renderer.render_docx(resume_content, temp_docx)
                        zip_file.write(temp_docx, "resume_tailored.docx")
                        manifest["artifacts"].append({"file": "resume_tailored.docx", "type": "Tailored DOCX Resume"})

                        # Cleanup
                        if temp_pdf.exists(): temp_pdf.unlink()
                        if temp_ats.exists(): temp_ats.unlink()
                        if temp_docx.exists(): temp_docx.unlink()
                    except Exception as e:
                        logger.error("Failed to include PDF in package: %s", e)

                elif art_type == "cover_letter":
                    zip_file.writestr("cover_letter.txt", content)
                    manifest["artifacts"].append({"file": "cover_letter.txt", "type": "Cover Letter"})
                
                elif art_type == "outreach_note":
                    zip_file.writestr("recruiter_outreach.txt", content)
                    manifest["artifacts"].append({"file": "recruiter_outreach.txt", "type": "Outreach Note"})
                
                elif art_type in ("why_company", "why_role"):
                    zip_file.writestr(f"{art_type}.txt", content)
                    manifest["artifacts"].append({"file": f"{art_type}.txt", "type": art_type})

            # Add Manifest
            zip_file.writestr("artifact_manifest.json", json.dumps(manifest, indent=2))

        return zip_buffer.getvalue(), filename

    async def create_bulk_package(self, application_ids: List[int]) -> tuple[bytes, str, Dict[str, Any]]:
        """
        Creates a master ZIP containing packages for all READY applications.
        Returns (zip_bytes, filename, report).
        """
        from jobsearch.services.readiness_service import ReadinessService
        readiness_service = ReadinessService(self.conn)
        readiness_map = readiness_service.evaluate_batch(application_ids)
        
        ready_ids = [aid for aid, state in readiness_map.items() if state.status == 'ready']
        skipped = {aid: state.reason for aid, state in readiness_map.items() if state.status != 'ready'}
        
        master_buffer = io.BytesIO()
        timestamp = datetime.now().strftime("%Y%m%d_%H%M")
        filename = f"Bulk_Export_{timestamp}.zip"
        
        report = {
            "total_requested": len(application_ids),
            "exported_count": len(ready_ids),
            "skipped_count": len(skipped),
            "skipped_details": []
        }

        with zipfile.ZipFile(master_buffer, "w", zipfile.ZIP_DEFLATED) as master_zip:
            for aid in ready_ids:
                try:
                    # Reuse existing single-package logic
                    zip_bytes, sub_filename = await self.create_package(aid)
                    # Add the sub-zip to the master zip
                    master_zip.writestr(sub_filename, zip_bytes)
                except Exception as e:
                    logger.error(f"Bulk export failed for ID {aid}: {e}")
                    report["skipped_count"] += 1
                    report["exported_count"] -= 1
            
            # Add a summary report to the master zip
            for aid, reason in skipped.items():
                app = ats_db.get_application(self.conn, aid)
                co_name = app['company'] if app else f"ID {aid}"
                report["skipped_details"].append({"company": co_name, "reason": reason})
            
            master_zip.writestr("export_summary.json", json.dumps(report, indent=2))

        return master_buffer.getvalue(), filename, report

    def _map_to_resume_content(self, data: Dict[str, Any]) -> ResumeContent:
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
