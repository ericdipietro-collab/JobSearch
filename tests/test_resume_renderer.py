import unittest
import json
from pathlib import Path
from jobsearch.services.resume_renderer import (
    AndyWarthogRenderer, ResumeContent, ResumeHeader, 
    ExperienceItem, CompetencyGroup, ProjectItem
)

class TestResumeRenderer(unittest.TestCase):
    def setUp(self):
        self.renderer = AndyWarthogRenderer()
        self.sample_data = ResumeContent(
            header=ResumeHeader(name="Test User", headline="Developer"),
            summary="This is a short test summary.",
            core_competencies=[
                CompetencyGroup("Tech", ["Python", "AWS"])
            ],
            experience=[
                ExperienceItem(
                    role="Dev", company="Co", location="NY", dates="2020",
                    bullets=["Bullet 1", "Bullet 2", "Bullet 3", "Bullet 4"]
                )
            ],
            projects=[
                ProjectItem("P1", "Desc 1"),
                ProjectItem("P2", "Desc 2"),
                ProjectItem("P3", "Desc 3")
            ]
        )

    def test_normalization_bullets(self):
        # Bullet count should be capped at 3
        normalized, warnings = self.renderer.normalize(self.sample_data)
        self.assertEqual(len(normalized.experience[0].bullets), 3)
        self.assertTrue(any("more than 3 bullets" in w for w in warnings))

    def test_normalization_projects(self):
        # Projects should be capped at 2
        normalized, warnings = self.renderer.normalize(self.sample_data)
        self.assertEqual(len(normalized.projects), 2)
        self.assertTrue(any("2 featured projects" in w for w in warnings))

    def test_normalization_summary(self):
        long_summary = "A" * 1000
        self.sample_data.summary = long_summary
        normalized, warnings = self.renderer.normalize(self.sample_data)
        self.assertTrue(len(normalized.summary) <= 450)
        self.assertTrue(normalized.summary.endswith("..."))
        self.assertTrue(any("Summary exceeded" in w for w in warnings))

    def test_html_rendering(self):
        html = self.renderer.to_html(self.sample_data)
        self.assertIn("Test User", html)
        self.assertIn("Teal", html) # Style check
        self.assertIn("container", html)

    def test_ats_safe_rendering(self):
        html = self.renderer.to_html(self.sample_data, ats_safe=True)
        # Should not contain the complex CSS but should have content
        self.assertIn("Test User", html)
        self.assertNotIn("grid-template-columns", html)

    def test_docx_rendering(self):
        out_path = Path("results/test_resume.docx")
        out_path.parent.mkdir(parents=True, exist_ok=True)
        try:
            self.renderer.render_docx(self.sample_data, out_path)
            self.assertTrue(out_path.exists())
            self.assertTrue(out_path.stat().st_size > 0)
        finally:
            if out_path.exists():
                out_path.unlink()

if __name__ == "__main__":
    unittest.main()
