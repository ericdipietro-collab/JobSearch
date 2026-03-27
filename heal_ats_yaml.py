import csv
import re
import shutil
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Optional, Tuple
from urllib.parse import urljoin, urlparse

import requests
import yaml
from bs4 import BeautifulSoup

# --- Configuration ---
BASE_DIR = Path(__file__).resolve().parent
YAML_FILE = BASE_DIR / "config" / "job_search_companies.yaml"
BACKUP_FILE = BASE_DIR / "config" / "job_search_companies.yaml.bak"
REPORT_FILE = BASE_DIR / "results" / "heal_ats_yaml_report.csv"

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
}
REQUEST_TIMEOUT = 10
SLEEP_BETWEEN_COMPANIES = 0.5 

@dataclass
class DiscoveryResult:
    adapter: Optional[str]
    adapter_key: Optional[str]
    careers_url: Optional[str]
    domain: Optional[str]  # Added to track the YAML domain field
    status: str
    detail: str


# Updated Stealth Configuration
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
    "Accept-Language": "en-US,en;q=0.9",
    "Accept-Encoding": "gzip, deflate, br",
    "DNT": "1",
    "Connection": "keep-alive",
    "Upgrade-Insecure-Requests": "1",
    "Sec-Fetch-Dest": "document",
    "Sec-Fetch-Mode": "navigate",
    "Sec-Fetch-Site": "none",
    "Sec-Fetch-User": "?1",
}

def make_session() -> requests.Session:
    session = requests.Session()
    session.headers.update(HEADERS)
    # Greenhouse often triggers on "too fast" connection reuse
    # This force-closes the connection to rotate the underlying socket
    adapter = requests.adapters.HTTPAdapter(pool_connections=1, pool_maxsize=1)
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    return session

ATS_PATTERNS = {
    "greenhouse": [
        re.compile(r"https?://(?:job-boards\.)?greenhouse\.io/([\w.-]+)", re.I),
        re.compile(r"https?://boards\.greenhouse\.io/([\w.-]+)", re.I),
    ],
    "lever": [
        re.compile(r"https?://jobs\.lever\.co/([\w.-]+)", re.I),
    ],
    "ashby": [
        re.compile(r"https?://jobs\.ashbyhq\.com/([\w.-]+)", re.I),
    ],
    "workday": [
        re.compile(r"https?://([\w.-]+\.myworkdayjobs\.com/[^\"'\s<>]+)", re.I),
    ],
}

def extract_ats_key(text: str, url: str, adapter: str) -> Optional[str]:
    patterns = ATS_PATTERNS.get(adapter, [])
    for p in patterns:
        for source in [url, text]:
            m = p.search(source)
            if m: return m.group(1).split("?")[0].strip("/")
    return None

def verify_page_ownership(html: str, name: str) -> bool:
    """Check if the company name appears in the page title or main headers."""
    soup = BeautifulSoup(html, "html.parser")
    page_title = (soup.title.string or "").lower() if soup.title else ""
    
    # Clean the name for flexible matching (e.g. "Access Softek" -> "access softek")
    clean_name = name.lower()
    
    # Logic: Does the title contain the company name? 
    # Or is there an <h1> containing the name?
    if clean_name in page_title:
        return True
    
    h1 = soup.find("h1")
    if h1 and clean_name in h1.get_text().lower():
        return True
        
    return False

def discover_for_company(session: requests.Session, company: dict) -> DiscoveryResult:
    name = company.get("name", "Unknown")
    slug = re.sub(r'[^a-zA-Z0-9]', '', name).lower()
    # Use the official domain for the waterfall search
    comp_domain = company.get("domain", "").strip().lower()
    if any(x in comp_domain for x in ["greenhouse.io", "ashbyhq.com", "lever.co", "workdayjobs.com"]):
        # If the domain is already an ATS, try to find the real one from company name
        comp_domain = re.sub(r'[^a-zA-Z0-9.]', '', name).lower() + ".com"

    slug_candidates = [slug, f"{slug}1", f"{slug}inc"]
    best_fallback_url = None

    # --- PHASE 1: GREENHOUSE (Priority) ---
    # ... previous code ...
    for s in slug_candidates:
        url = f"https://boards.greenhouse.io/{s}"
        try:
            resp = session.get(url, timeout=5, allow_redirects=True)
            
            if resp.status_code == 406:
                print(f" !!! BLOCKED by Greenhouse (406) - Sleeping 10s...")
                time.sleep(10)
                return DiscoveryResult(None, None, None, "custom_site", "BLOCKED", "Greenhouse WAF 406")
            
            if resp.status_code == 200 and "greenhouse.io" in resp.url.lower():
                if verify_page_ownership(resp.text, name):
                    return DiscoveryResult("greenhouse", s, resp.url, "boards.greenhouse.io", "FOUND", "Greenhouse Verified")
        except: continue

    # --- PHASE 2: ASHBY (Priority - with STRICT Verification) ---
    for s in slug_candidates:
        url = f"https://jobs.ashbyhq.com/{s}"
        try:
            resp = session.get(url, timeout=5, allow_redirects=True)
            # CRITICAL: We only accept Ashby if the company name is in the title
            if resp.status_code == 200 and "ashbyhq.com" in resp.url.lower():
                if verify_page_ownership(resp.text, name):
                    return DiscoveryResult("ashby", s, resp.url, "jobs.ashbyhq.com", "FOUND", "Ashby Verified")
        except: continue

    # --- PHASE 3: THE WATERFALL (Handles Informatica/Salesforce) ---
    search_bases = [f"https://www.{comp_domain}", f"https://careers.{comp_domain}", f"https://{comp_domain}"]
    for base in search_bases:
        for path in ["", "/careers", "/jobs", "/about-us/careers.html"]:
            try:
                url = urljoin(base, path)
                resp = session.get(url, timeout=REQUEST_TIMEOUT, allow_redirects=True)
                if resp.status_code != 200: continue
                
                f_url = resp.url.lower()
                content = resp.text.lower()

                # Informatica Corner Case: Detect Salesforce/Workday from Redirects
                if "salesforce.com" in f_url or "myworkdayjobs.com" in f_url:
                    return DiscoveryResult("manual", None, resp.url, "custom_site", "FOUND", f"Redirected to {urlparse(f_url).netloc}")

                if not best_fallback_url and any(x in f_url for x in ["career", "job", "opening"]):
                    best_fallback_url = resp.url

                # Content-based detection
                for ats, marker in [("lever", "lever.co"), ("workday", "myworkdayjobs.com")]:
                    if marker in content or marker in f_url:
                        return DiscoveryResult(ats, None, resp.url, f"jobs.{marker}" if ats=="lever" else marker, "FOUND", f"{ats} detected")
            except: continue

    # --- PHASE 4: FALLBACK ---
    if best_fallback_url:
        return DiscoveryResult("manual", None, best_fallback_url, "custom_site", "FALLBACK", "Generic Career Page")

    return DiscoveryResult(None, None, None, "custom_site", "NOT_FOUND", "No board detected")

def update_company_record(company: dict, result: DiscoveryResult) -> bool:
    changed = False
    
    # Update careers_url
    if result.careers_url and company.get("careers_url") != result.careers_url:
        company["careers_url"] = result.careers_url
        changed = True
    
    # Update adapter
    if result.adapter and company.get("adapter") != result.adapter:
        company["adapter"] = result.adapter
        changed = True

    # Update adapter_key
    if result.adapter_key is not None and company.get("adapter_key") != result.adapter_key:
        company["adapter_key"] = result.adapter_key
        changed = True

    # CRITICAL: Update the domain field as requested
    if result.domain and company.get("domain") != result.domain:
        company["domain"] = result.domain
        changed = True
        
    return changed

def heal_registry() -> None:
    if not YAML_FILE.exists():
        print(f"Error: {YAML_FILE} not found.")
        return

    with YAML_FILE.open("r", encoding="utf-8") as f:
        data = yaml.safe_load(f) or {}

    companies = data.get("companies", [])
    print(f"Scanning {len(companies)} companies...")

    shutil.copy2(YAML_FILE, BACKUP_FILE)
    session = make_session()
    updated_count, report_rows = 0, []

    for idx, company in enumerate(companies, start=1):
        name = company.get("name", "<unknown>")
        if company.get("active") is False: continue

        old_adapter, old_url = company.get("adapter", ""), company.get("careers_url", "")
        
        result = discover_for_company(session, company)
        changed = update_company_record(company, result)
        if changed: updated_count += 1

        report_rows.append({
            "name": name, "status": result.status, "detail": result.detail,
            "old_adapter": old_adapter, "new_adapter": company.get("adapter", ""),
            "old_url": old_url, "new_url": company.get("careers_url", ""),
        })

        marker = "UPDATED" if changed else "OK"
        print(f"[{idx:>4}/{len(companies)}] {marker:<7} {name:<35} {result.status} (Domain: {company.get('domain')})")

    with YAML_FILE.open("w", encoding="utf-8") as f:
        yaml.safe_dump(data, f, sort_keys=False, allow_unicode=True)

    REPORT_FILE.parent.mkdir(parents=True, exist_ok=True)
    with REPORT_FILE.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=["name", "status", "detail", "old_adapter", "new_adapter", "old_url", "new_url"])
        writer.writeheader()
        writer.writerows(report_rows)

    print(f"\nDone. Updated {updated_count} companies.")

if __name__ == "__main__":
    heal_registry()