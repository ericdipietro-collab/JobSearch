import sqlite3
import os
import yaml
from pathlib import Path
from datetime import datetime, timezone
from jobsearch.scraper.scoring import Scorer

# Setup paths
app_data = Path(os.environ['LOCALAPPDATA']) / 'JobSearchDashboardData'
db_path = app_data / 'results' / 'jobsearch.db'
prefs_path = app_data / 'config' / 'job_search_preferences.yaml'

with open(prefs_path, 'r', encoding='utf-8') as f:
    prefs = yaml.safe_load(f)

scorer = Scorer(prefs)
conn = sqlite3.connect(str(db_path))
conn.row_factory = sqlite3.Row

rows = conn.execute("SELECT * FROM applications WHERE status = 'considering'").fetchall()
print(f"Re-scoring {len(rows)} jobs...")

updated = 0
now = datetime.now(timezone.utc).isoformat()

for row in rows:
    job_data = {
        "title": row["role"],
        "description": " ".join(filter(None, [
            row["job_description"],
            row["description_excerpt"],
            row["jd_summary"]
        ])),
        "location": row["location"],
        "tier": row["tier"],
        "is_remote": row["is_remote"],
        "salary_min": row["salary_low"],
        "salary_max": row["salary_high"],
        "work_type": row["work_type"],
        "source_lane": row["source_lane"],
    }
    
    res = scorer.score_job(job_data)
    
    conn.execute(
        "UPDATE applications SET score=?, fit_band=?, apply_now_eligible=?, matched_keywords=?, penalized_keywords=?, decision_reason=?, updated_at=? WHERE id=?",
        (res["score"], res["fit_band"], int(res.get("apply_now_eligible", True)), res["matched_keywords"], res["penalized_keywords"], res["decision_reason"], now, row["id"])
    )
    updated += 1

conn.commit()
print(f"Successfully re-scored {updated} jobs.")

# Check Juniper Square specifically
print("\nJuniper Square Check:")
js = conn.execute("SELECT company, role, score, fit_band, decision_reason FROM applications WHERE company='Juniper Square' AND role LIKE '%Fund Accounting%'").fetchone()
if js:
    print(dict(js))

conn.close()
