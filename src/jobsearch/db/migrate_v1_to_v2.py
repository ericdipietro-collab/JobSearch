"""Migration script from v1 (split DBs) to v2 (unified DB)."""

import sqlite3
import shutil
from pathlib import Path
from datetime import datetime, timezone

BASE_DIR = Path(__file__).resolve().parent.parent.parent.parent
OLD_SCRAPER_DB = BASE_DIR / "results" / "jobsearch.db"
OLD_TRACKER_DB = BASE_DIR / "results" / "job_applications.db"
NEW_DB = BASE_DIR / "results" / "jobsearch_v2.db"

def migrate():
    print(f"🚀 Starting migration to {NEW_DB}...")
    
    if NEW_DB.exists():
        print(f"⚠️  Removing existing {NEW_DB}")
        NEW_DB.unlink()

    # Initialize New DB
    from src.jobsearch.db.connection import get_connection
    new_conn = get_connection(NEW_DB)
    
    # 1. Migrate Tracker Data (job_applications.db)
    if OLD_TRACKER_DB.exists():
        print(f"📂 Migrating tracker data from {OLD_TRACKER_DB}...")
        old_tracker_conn = sqlite3.connect(str(OLD_TRACKER_DB))
        old_tracker_conn.row_factory = sqlite3.Row
        
        # --- Applications ---
        apps = old_tracker_conn.execute("SELECT * FROM applications").fetchall()
        print(f"  - Found {len(apps)} applications")
        for app in apps:
            d = dict(app)
            # Map old fields to new schema
            new_conn.execute("""
                INSERT INTO applications (
                    id, company, role, job_url, source, scraper_key, status,
                    salary_text, salary_low, salary_high, referral, jd_summary,
                    notes, date_added, date_applied, date_closed, follow_up_date,
                    follow_up_notes, resume_version, cover_letter_notes, entry_type,
                    created_at, updated_at
                ) VALUES (
                    :id, :company, :role, :job_url, :source, :scraper_key, :status,
                    :salary_range, :salary_low, :salary_high, :referral, :jd_summary,
                    :notes, :date_added, :date_applied, :date_closed, :follow_up_date,
                    :follow_up_notes, :resume_version, :cover_letter_notes, :entry_type,
                    :created_at, :updated_at
                )
            """, d)
            
        # --- Copy other tracker tables as-is ---
        for table in ["events", "contacts", "training", "interviews", "settings", 
                     "email_templates", "journal_entries", "network_contacts", 
                     "question_bank", "company_profiles"]:
            rows = old_tracker_conn.execute(f"SELECT * FROM {table}").fetchall()
            if rows:
                print(f"  - Migrating {len(rows)} rows from {table}")
                cols = rows[0].keys()
                placeholders = ", ".join(["?"] * len(cols))
                new_conn.executemany(
                    f"INSERT INTO {table} ({', '.join(cols)}) VALUES ({placeholders})",
                    [tuple(r) for r in rows]
                )
        old_tracker_conn.close()

    # 2. Migrate Scraper Data (jobsearch.db)
    if OLD_SCRAPER_DB.exists():
        print(f"📂 Migrating scraper results from {OLD_SCRAPER_DB}...")
        old_scraper_conn = sqlite3.connect(str(OLD_SCRAPER_DB))
        old_scraper_conn.row_factory = sqlite3.Row
        
        opps = old_scraper_conn.execute("SELECT * FROM opportunities").fetchall()
        print(f"  - Found {len(opps)} scraper opportunities")
        
        for opp in opps:
            d = dict(opp)
            # Check if this app already exists (from tracker)
            existing = new_conn.execute(
                "SELECT id FROM applications WHERE scraper_key = ?", (d["id"],)
            ).fetchone()
            
            if existing:
                # Update existing record with scraper metadata
                new_conn.execute("""
                    UPDATE applications SET
                        role_normalized = :role_title_normalized,
                        score = :score,
                        fit_band = :fit_band,
                        matched_keywords = :matched_keywords,
                        penalized_keywords = :penalized_keywords,
                        decision_reason = :decision_reason,
                        description_excerpt = :description_excerpt,
                        is_remote = :is_remote,
                        location = :location,
                        user_priority = :user_priority,
                        date_discovered = :date_discovered
                    WHERE scraper_key = :id
                """, d)
            else:
                # Insert new scraper record
                new_conn.execute("""
                    INSERT INTO applications (
                        company, role, role_normalized, job_url, source, scraper_key,
                        status, score, fit_band, matched_keywords, penalized_keywords,
                        decision_reason, description_excerpt, location, is_remote,
                        salary_text, salary_low, salary_high, date_discovered,
                        user_priority, notes, created_at, updated_at
                    ) VALUES (
                        :company, :role_title_raw, :role_title_normalized, :url, :source, :id,
                        'considering', :score, :fit_band, :matched_keywords, :penalized_keywords,
                        :decision_reason, :description_excerpt, :location, :is_remote,
                        :salary_text, :salary_min, :salary_max, :date_discovered,
                        :user_priority, :notes, :last_updated, :last_updated
                    )
                """, d)
        old_scraper_conn.close()

    new_conn.commit()
    new_conn.close()
    print("✅ Migration complete!")

if __name__ == "__main__":
    migrate()
