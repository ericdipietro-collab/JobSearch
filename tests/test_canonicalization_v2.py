import unittest
import sqlite3
import json
from jobsearch import ats_db
from jobsearch.scraper.models import Job
from jobsearch.services.opportunity_service import upsert_job
from jobsearch.services.job_canonicalization_service import JobCanonicalizationService
from jobsearch.scraper.normalization import SourceLaneRegistry

class TestCanonicalizationV2(unittest.TestCase):
    def setUp(self):
        self.conn = sqlite3.connect(":memory:")
        self.conn.row_factory = sqlite3.Row
        ats_db.init_db(self.conn)

    def test_exact_req_id_merge(self):
        # 1. Insert an aggregator job
        job1 = Job(
            id="job1",
            company="TestCo", role_title_raw="Software Engineer", url="url1",
            source="aggregator", source_lane=SourceLaneRegistry.LANE_AGGREGATOR,
            req_id="REQ-123", location="Remote"
        )
        inserted1, id1 = upsert_job(self.conn, job1)
        
        # 2. Insert same job from Employer ATS (higher trust)
        job2 = Job(
            id="job2",
            company="TestCo", role_title_raw="Software Engineer", url="url2",
            source="greenhouse", source_lane=SourceLaneRegistry.LANE_EMPLOYER_ATS,
            req_id="REQ-123", location="Remote"
        )
        inserted2, id2 = upsert_job(self.conn, job2)
        
        # Verify job2 superseded job1
        row1 = self.conn.execute("SELECT is_canonical, canonical_merge_rationale FROM applications WHERE id = ?", (id1,)).fetchone()
        row2 = self.conn.execute("SELECT is_canonical, canonical_merge_rationale FROM applications WHERE id = ?", (id2,)).fetchone()
        
        self.assertEqual(row1['is_canonical'], 0)
        self.assertEqual(row2['is_canonical'], 1)
        self.assertIn("Superseded existing canonical", row2['canonical_merge_rationale'])

    def test_identity_merge_with_conflicts(self):
        # 1. Insert primary
        job1 = Job(
            id="job3",
            company="ConflictCo", role_title_raw="Product Manager", url="url1",
            source="greenhouse", source_lane=SourceLaneRegistry.LANE_EMPLOYER_ATS,
            location="New York", salary_max=200000
        )
        upsert_job(self.conn, job1)
        
        # 2. Insert secondary with conflicting salary (more than 20% diff)
        job2 = Job(
            id="job4",
            company="ConflictCo", role_title_raw="Product Manager", url="url2",
            source="dice", source_lane=SourceLaneRegistry.LANE_SPECIALTY_BOARD,
            location="New York", salary_max=150000
        )
        inserted2, id2 = upsert_job(self.conn, job2)
        
        # Verify job2 is NOT canonical but is merged
        row2 = self.conn.execute("SELECT is_canonical, notes FROM applications WHERE id = ?", (id2,)).fetchone()
        self.assertEqual(row2['is_canonical'], 0)
        self.assertIn("Salary mismatch", row2['notes'])

    def test_no_merge_different_locations(self):
        # 1. New York
        job1 = Job(
            id="job5",
            company="TestCo", role_title_raw="Engineer", url="url1",
            source="greenhouse", source_lane=SourceLaneRegistry.LANE_EMPLOYER_ATS,
            location="New York"
        )
        upsert_job(self.conn, job1)
        
        # 2. London
        job2 = Job(
            id="job6",
            company="TestCo", role_title_raw="Engineer", url="url2",
            source="greenhouse", source_lane=SourceLaneRegistry.LANE_EMPLOYER_ATS,
            location="London"
        )
        inserted2, id2 = upsert_job(self.conn, job2)
        
        # Both should be canonical
        row2 = self.conn.execute("SELECT is_canonical FROM applications WHERE id = ?", (id2,)).fetchone()
        self.assertEqual(row2['is_canonical'], 1)

if __name__ == "__main__":
    unittest.main()
