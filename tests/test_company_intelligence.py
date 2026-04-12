import unittest
import sqlite3
import json
from datetime import datetime, timezone
from jobsearch.db.schema import init_db
from jobsearch import ats_db
from jobsearch.services.company_intelligence_service import CompanyIntelligenceService

class TestCompanyIntelligence(unittest.TestCase):
    def setUp(self):
        self.conn = sqlite3.connect(":memory:")
        self.conn.row_factory = sqlite3.Row
        init_db(self.conn)
        ats_db.init_db(self.conn)
        self.service = CompanyIntelligenceService(self.conn)

    def test_apply_cold_strategy_new_company(self):
        playbook = self.service.get_company_playbook("NewCo")
        self.assertEqual(playbook.recommended_strategy, "apply_cold")
        self.assertIn("No history yet", playbook.strategy_rationale)

    def test_ask_for_referral_strategy(self):
        # 1. Add warm contact
        ats_db.add_network_contact(
            self.conn, name="Alice", company="WarmCo", relationship="former_colleague"
        )
        
        # 2. Add failing cold apps
        for i in range(2):
            self.conn.execute(
                "INSERT INTO applications (scraper_key, company, role, status, created_at, updated_at) "
                "VALUES (?, 'WarmCo', 'Role', 'rejected', '2026-01-01', '2026-01-01')",
                (f"key{i}",)
            )
        self.conn.commit()
        
        playbook = self.service.get_company_playbook("WarmCo")
        self.assertEqual(playbook.recommended_strategy, "ask_for_referral")
        self.assertIn("Warm contacts available", playbook.strategy_rationale)
        self.assertEqual(len(playbook.contact_strategy), 1)
        self.assertEqual(playbook.contact_strategy[0].next_action, "Ask for Referral")

    def test_role_family_performance(self):
        # 1. Successful architecture role
        self.conn.execute(
            "INSERT INTO applications (scraper_key, company, role, v2_canonical_title, status, created_at, updated_at) "
            "VALUES ('k1', 'MultiRole', 'Arch', 'Solution Architect', 'interviewing', '2026-01-01', '2026-01-01')"
        )
        # 2. Failing product roles
        for i in range(2):
            self.conn.execute(
                "INSERT INTO applications (scraper_key, company, role, v2_canonical_title, status, created_at, updated_at) "
                "VALUES (?, 'MultiRole', 'PM', 'Product Manager', 'rejected', '2026-01-01', '2026-01-01')",
                (f"kp{i}",)
            )
        self.conn.commit()
        
        playbook = self.service.get_company_playbook("MultiRole")
        self.assertIn("Solution Architect", playbook.top_role_families)
        self.assertIn("Product Manager", playbook.underperforming_families)

if __name__ == "__main__":
    unittest.main()
