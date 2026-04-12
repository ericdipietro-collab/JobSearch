import unittest
import sqlite3
import json
import asyncio
from pathlib import Path
from unittest.mock import MagicMock, patch
from jobsearch.db.schema import init_db
from jobsearch import ats_db
from jobsearch.services.resume_renderer import AndyWarthogRenderer, ResumeContent, ResumeHeader
from jobsearch.services.package_service import ApplicationPackageService
from jobsearch.views.tailoring_studio_page import _map_dict_to_obj, _map_obj_to_dict

class TestTailoringV2(unittest.TestCase):
    def setUp(self):
        self.conn = sqlite3.connect(":memory:")
        self.conn.row_factory = sqlite3.Row
        init_db(self.conn)
        ats_db.init_db(self.conn)
        
        # Setup mock application
        self.conn.execute(
            "INSERT INTO applications (id, scraper_key, company, role, status, created_at, updated_at) "
            "VALUES (1, 'key1', 'TestCo', 'Engineer', 'applied', '2026-01-01', '2026-01-01')"
        )
        self.conn.commit()

    def test_normalization_warnings(self):
        renderer = AndyWarthogRenderer()
        content = ResumeContent(
            header=ResumeHeader(name=""), # Missing name
            summary="A" * 1000, # Too long
            experience=[], projects=[], education=[], awards=[]
        )
        norm_obj, warnings = renderer.normalize(content)
        
        # Should have warnings for missing name and truncated summary
        self.assertTrue(any("Name is missing" in w for w in warnings))
        self.assertTrue(any("Summary exceeded" in w for w in warnings))
        self.assertEqual(len(norm_obj.summary), 450)

    def test_mapping_roundtrip(self):
        data = {
            "header": {"name": "Alice", "headline": "Dev", "email": "a@b.com", "phone": "123", "portfolio_url": "", "linkedin_url": ""},
            "summary": "Summary text",
            "core_competencies": [{"label": "L1", "items": ["I1"]}],
            "experience": [{"role": "R", "company": "C", "location": "L", "dates": "D", "bullets": ["B"]}],
            "projects": [],
            "education": [],
            "awards": [],
            "template_name": "Andy Warthog"
        }
        
        obj = _map_dict_to_obj(data)
        self.assertEqual(obj.header.name, "Alice")
        self.assertEqual(obj.experience[0].role, "R")
        
        data_back = _map_obj_to_dict(obj)
        self.assertEqual(data_back["header"]["name"], "Alice")

    @patch("jobsearch.services.resume_renderer.AndyWarthogRenderer.render_pdf")
    def test_package_export(self, mock_render):
        # Mock the async render_pdf call to actually create dummy files
        def side_effect(html, path):
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text("dummy pdf content")
            return asyncio.Future()
        
        # We need to return a future that resolves
        mock_render.side_effect = lambda html, path: asyncio.ensure_future(asyncio.sleep(0))
        
        # Manually create the files since we mocked the renderer
        Path("results/temp_styled.pdf").parent.mkdir(parents=True, exist_ok=True)
        Path("results/temp_styled.pdf").write_text("dummy")
        Path("results/temp_ats.pdf").write_text("dummy")
        Path("results/temp_resume.docx").write_text("dummy")
        
        # Add some artifacts
        ats_db.add_tailored_artifact(self.conn, 1, "cover_letter", "CL content")
        ats_db.add_tailored_artifact(self.conn, 1, "resume_json_warthog", json.dumps({"header": {"name": "Alice"}}))
        
        service = ApplicationPackageService(self.conn)
        zip_bytes, filename = asyncio.run(service.create_package(1))
        
        self.assertIn("Application_Package_TestCo", filename)
        self.assertTrue(len(zip_bytes) > 0)
        
        import zipfile
        import io
        with zipfile.ZipFile(io.BytesIO(zip_bytes)) as z:
            names = z.namelist()
            self.assertIn("cover_letter.txt", names)
            self.assertIn("resume_styled.pdf", names)
            self.assertIn("resume_tailored.docx", names)
            self.assertIn("artifact_manifest.json", names)

if __name__ == "__main__":
    unittest.main()
