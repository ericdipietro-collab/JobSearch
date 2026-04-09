import os
import yaml
import re
from pathlib import Path
from jobsearch.services.healer_service import ATSHealer

# Target the AppData directory
app_data = Path(os.environ['LOCALAPPDATA']) / 'JobSearchDashboardData'
registries = [
    app_data / 'config' / 'job_search_companies.yaml',
    app_data / 'config' / 'job_search_companies_aggregators.yaml',
    app_data / 'config' / 'job_search_companies_jobspy.yaml',
]

healer = ATSHealer()

def is_incomplete_workday(url: str) -> bool:
    if not url or "myworkdayjobs.com" not in url:
        return False
    path = re.sub(r"https?://[^/]+", "", url).strip("/")
    if not path: return True
    if re.fullmatch(r"[a-z]{2}(?:-[A-Z]{2})?", path): return True
    return False

print("============================================================")
print(" Workday URL Aggressive Path-Finder (v3)")
print("============================================================")

for reg_path in registries:
    if not reg_path.exists(): continue
    print(f"\nProcessing registry: {reg_path.name}")
    with open(reg_path, 'r', encoding='utf-8') as f:
        data = yaml.safe_load(f)
    
    companies = data.get("companies", [])
    changed = False
    workday_candidates = [c for c in companies if "myworkdayjobs.com" in str(c.get("careers_url", ""))]
    
    for i, comp in enumerate(workday_candidates, 1):
        name = comp.get("name", "Unknown")
        current_url = comp.get("careers_url", "")
        
        if is_incomplete_workday(current_url):
            print(f"[{i}/{len(workday_candidates)}] PROBING: {name}...")
            
            try:
                # Use the FULL discovery pipeline (parallel search, redirects, etc.)
                res = healer.discover(comp, force=True)
                
                if res.status in ("FOUND", "VALID") and res.careers_url != current_url:
                    print(f"    ✅ FIXED: {res.careers_url}")
                    comp["careers_url"] = res.careers_url
                    comp["adapter"] = res.adapter
                    comp["adapter_key"] = res.adapter_key
                    changed = True
                else:
                    print(f"    ━ No better URL found.")
            except Exception as e:
                print(f"    ❌ Error: {e}")

    if changed:
        print(f"\nSaving improvements to {reg_path.name}...")
        with open(reg_path, 'w', encoding='utf-8') as f:
            yaml.dump(data, f, sort_keys=False, allow_unicode=True)
    else:
        print(f"\nNo deep paths discovered for {reg_path.name}.")

print("\nRepair Complete.")
