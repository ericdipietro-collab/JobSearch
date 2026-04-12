import unittest
import sqlite3
import pandas as pd
from unittest.mock import MagicMock, patch
from jobsearch.db.schema import init_db
from jobsearch import ats_db
from jobsearch.services.tailoring_service import TailoringService
from jobsearch.services.learning_loop_service import LearningLoopService

class TestTailoringAndLearning(unittest.TestCase):
    def setUp(self):
        self.conn = sqlite3.connect(":memory:")
        self.conn.row_factory = sqlite3.Row
        init_db(self.conn)
        ats_db.init_db(self.conn)
        
        # Set up a base resume in settings
        ats_db.set_setting(self.conn, "base_resume_text", "Experienced Python Developer with expertise in AWS and SQL.")
        
        # Set up an application with keywords
        self.conn.execute(
            """
            INSERT INTO applications (id, scraper_key, company, role, status, score, fit_band, matched_keywords, description_excerpt, created_at, updated_at)
            VALUES (1, 'key1', 'CloudCo', 'Backend Engineer', 'applied', 85, 'GOOD MATCH', 'Python|AWS|Docker|Kubernetes', 'JD: Needs Python, AWS, Docker, and Kubernetes.', '2026-01-01', '2026-01-01')
            """
        )
        self.conn.commit()

    def test_keyword_gap_detection(self):
        service = TailoringService(self.conn)
        analysis = service.analyze_keywords(1)
        
        # Matched: Python, AWS (in resume)
        # Missing: Docker, Kubernetes (not in resume)
        self.assertIn("python", analysis.matched)
        self.assertIn("aws", analysis.matched)
        self.assertIn("docker", analysis.missing)
        self.assertIn("kubernetes", analysis.missing)

    def test_artifact_persistence(self):
        service = TailoringService(self.conn)
        art_id = service.save_artifact(1, "cover_letter", "Dear Hiring Manager...")
        
        artifacts = service.get_artifacts(1)
        self.assertEqual(len(artifacts), 1)
        self.assertEqual(artifacts[0]["artifact_type"], "cover_letter")
        self.assertEqual(artifacts[0]["content"], "Dear Hiring Manager...")

    def test_learning_loop_score_correlation(self):
        # Add another application that led to an interview
        self.conn.execute(
            "INSERT INTO applications (scraper_key, company, role, status, score, created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?)",
            ('key2', 'TechCorp', 'Python Dev', 'interviewing', 92, '2026-01-01', '2026-01-01')
        )
        self.conn.commit()
        
        service = LearningLoopService(self.conn)
        df = service.get_score_vs_outcome()
        
        # Should have 90-100 band and 80-89 band
        self.assertIn('90-100', df['score_band'].values)
        self.assertIn('80-89', df['score_band'].values)
        
        # Interest rate for 90-100 should be 100% (TechCorp)
        row_90 = df[df['score_band'] == '90-100'].iloc[0]
        self.assertEqual(row_90['interest_rate'], 100.0)

    def test_calibration_insights(self):
        # Create a "score inversion" scenario
        # 1. High score application (95) -> Rejected
        self.conn.execute(
            "INSERT INTO applications (scraper_key, company, role, status, score, created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?)",
            ('key_high', 'HighFit', 'Lead', 'rejected', 95, '2026-01-01', '2026-01-01')
        )
        # 2. Low score application (65) -> Interviewing
        self.conn.execute(
            "INSERT INTO applications (scraper_key, company, role, status, score, created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?)",
            ('key_low', 'LowFit', 'Junior', 'interviewing', 65, '2026-01-01', '2026-01-01')
        )
        self.conn.commit()
        
        service = LearningLoopService(self.conn)
        insights = service.get_calibration_insights()
        
        # Look for the inversion warning
        warning = next((i for i in insights if "Inversion" in i["title"]), None)
        self.assertIsNotNone(warning)

if __name__ == "__main__":
    unittest.main()
