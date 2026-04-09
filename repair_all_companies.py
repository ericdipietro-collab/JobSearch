import os
import yaml
import sqlite3
from pathlib import Path
from jobsearch.services.healer_service import ATSHealer
from jobsearch.config.settings import settings

# 1. Setup paths to target the active AppData directory
app_data = Path(os.environ['LOCALAPPDATA']) / 'JobSearchDashboardData'
db_path = app_data / 'results' / 'jobsearch.db'
registries = [
    app_data / 'config' / 'job_search_companies.yaml',
    app_data / 'config' / 'job_search_companies_aggregators.yaml',
    app_data / 'config' / 'job_search_companies_jobspy.yaml',
]

healer = ATSHealer()

def get_low_signal_companies():
    """Find companies that are failing or returning 0 results using the split health tables."""
    if not db_path.exists(): return set()
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    # Combine results from both health tables
    query = """
        SELECT company FROM workday_target_health 
        WHERE last_status IN ('empty', 'blocked', 'low_signal', 'error')
        UNION
        SELECT company FROM generic_target_health
        WHERE last_status IN ('empty', 'blocked', 'low_signal', 'error')
    """
    rows = conn.execute(query).fetchall()
    conn.close()
    return {row['company'] for row in rows}

print("============================================================")
print(" Global Registry Repair & ATS Discovery")
print("============================================================")

low_signal_targets = get_low_signal_companies()
print(f"Detected {len(low_signal_targets)} companies with health issues.")

for reg_path in registries:
    if not reg_path.exists(): continue
    print(f"\nProcessing registry: {reg_path.name}")
    with open(reg_path, 'r', encoding='utf-8') as f:
        data = yaml.safe_load(f)
    
    companies = data.get("companies", [])
    changed = False
    
    for i, comp in enumerate(companies, 1):
        name = comp.get("name", "Unknown")
        # Fix if it's in our low-signal list OR if it's currently 'generic' (worth a check)
        is_low_signal = name in low_signal_targets
        is_generic = comp.get("adapter") == "generic"
        
        if is_low_signal or is_generic:
            print(f"[{i}/{len(companies)}] INVESTIGATING: {name}...")
            try:
                # Run the full parallel discovery pipeline
                res = healer.discover(comp, force=True)
                
                if res.status in ("FOUND", "VALID") and (res.careers_url != comp.get("careers_url") or res.adapter != comp.get("adapter")):
                    print(f"    ✅ IMPROVED: {comp.get('adapter')} -> {res.adapter}")
                    print(f"    🔗 URL: {res.careers_url}")
                    comp["careers_url"] = res.careers_url
                    comp["adapter"] = res.adapter
                    comp["adapter_key"] = res.adapter_key
                    changed = True
                else:
                    print(f"    ━ No better source found.")
            except Exception as e:
                print(f"    ❌ Error: {e}")

    if changed:
        print(f"\nSaving improvements to {reg_path.name}...")
        with open(reg_path, 'w', encoding='utf-8') as f:
            yaml.dump(data, f, sort_keys=False, allow_unicode=True)
    else:
        print(f"\nNo improvements found for {reg_path.name}.")

print("\nGlobal Repair Complete.")
