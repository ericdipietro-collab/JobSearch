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

# --- Updated Configuration Section ---
BASE_DIR = Path(__file__).resolve().parent
YAML_FILE = BASE_DIR / "config" / "job_search_companies.yaml"
BACKUP_FILE = BASE_DIR / "config" / "job_search_companies.yaml.bak"
REPORT_FILE = BASE_DIR / "results" / "heal_ats_yaml_report.csv"

# ... inside heal_registry() ...
yaml_path = Path(YAML_FILE)
# Then use the variables directly:
shutil.copy2(yaml_path, BACKUP_FILE) 
report_path = REPORT_FILE
report_path.parent.mkdir(parents=True, exist_ok=True)
HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/135.0.0.0 Safari/537.36"
    )
}
REQUEST_TIMEOUT = 8
SLEEP_BETWEEN_COMPANIES = 0.2
CAREER_PATHS = [
    "/careers",
    "/jobs",
    "/company/careers",
    "/about/careers",
    "/careers/",
    "/jobs/",
    "/careers/openings",
    "/careers/jobs",
]
ATS_PRIORITY = ["greenhouse", "lever", "ashby", "workday"]

# Add these to your configuration section
SUBDOMAINS = ["careers", "jobs", "work-with-us"]
# Keep your existing CAREER_PATHS

@dataclass
class DiscoveryResult:
    adapter: Optional[str]
    adapter_key: Optional[str]
    careers_url: Optional[str]
    status: str
    detail: str


def make_session() -> requests.Session:
    session = requests.Session()
    session.headers.update(HEADERS)
    return session


def normalize_domain(domain: str) -> str:
    d = (domain or "").strip().lower()
    d = re.sub(r"^https?://", "", d)
    d = d.split("/")[0]
    if d.startswith("www."):
        d = d[4:]
    return d


def normalize_url(url: Optional[str]) -> Optional[str]:
    if not url:
        return None
    u = url.strip()
    if not u:
        return None
    if not re.match(r"^https?://", u, flags=re.I):
        u = "https://" + u.lstrip("/")
    return u


def fetch(session: requests.Session, url: str) -> Optional[requests.Response]:
    try:
        response = session.get(url, timeout=REQUEST_TIMEOUT, allow_redirects=True)
        return response
    except requests.RequestException:
        return None


ATS_PATTERNS = {
    "greenhouse": [
        re.compile(r"https?://(?:job-boards\.)?greenhouse\.io/([\w.-]+)", re.I),
        re.compile(r"https?://boards\.greenhouse\.io/([\w.-]+)", re.I),
    ],
    "lever": [
        re.compile(r"https?://jobs\.lever\.co/([\w.-]+)", re.I),
        re.compile(r"https?://api\.lever\.co/v0/postings/([\w.-]+)", re.I),
    ],
    "ashby": [
        re.compile(r"https?://jobs\.ashbyhq\.com/([\w.-]+)", re.I),
    ],
    "workday": [
        re.compile(r"https?://([\w.-]+\.myworkdayjobs\.com/[^\"'\s<>]+)", re.I),
        re.compile(r"https?://([\w.-]+\.wd\d+\.myworkdayjobs\.com/[^\"'\s<>]+)", re.I),
    ],
}


def extract_ats_from_text(text: str) -> Optional[Tuple[str, str, str]]:
    for adapter in ATS_PRIORITY:
        for pattern in ATS_PATTERNS[adapter]:
            match = pattern.search(text)
            if not match:
                continue
            if adapter == "workday":
                full_path = "https://" + match.group(1).rstrip("/")
                return adapter, "", full_path
            key = match.group(1).split("?")[0].strip("/")
            if key:
                if adapter == "greenhouse":
                    careers_url = f"https://job-boards.greenhouse.io/{key}"
                elif adapter == "lever":
                    careers_url = f"https://jobs.lever.co/{key}"
                elif adapter == "ashby":
                    careers_url = f"https://jobs.ashbyhq.com/{key}"
                else:
                    careers_url = None
                return adapter, key, careers_url or ""
    return None


def find_career_links(base_url: str, html: str) -> list[str]:
    soup = BeautifulSoup(html, "html.parser")
    links: list[str] = []
    for a in soup.find_all("a", href=True):
        href = a.get("href", "").strip()
        label = " ".join(a.get_text(" ", strip=True).lower().split())
        href_l = href.lower()
        if any(word in href_l for word in ["career", "job", "join-us", "work-with-us", "openings"]):
            links.append(urljoin(base_url, href))
        elif any(word in label for word in ["career", "jobs", "open positions", "openings", "join our team"]):
            links.append(urljoin(base_url, href))
    deduped = []
    seen = set()
    for link in links:
        if link not in seen:
            seen.add(link)
            deduped.append(link)
    return deduped[:15]


def discover_from_url(session: requests.Session, url: str) -> DiscoveryResult:
    response = fetch(session, url)
    if response is None:
        return DiscoveryResult(None, None, None, "fetch_failed", f"request failed for {url}")
    if response.status_code >= 400:
        return DiscoveryResult(None, None, None, "http_error", f"{response.status_code} for {url}")

    final_url = response.url
    html = response.text or ""

    found = extract_ats_from_text(html)
    if found:
        adapter, key, careers_url = found
        return DiscoveryResult(adapter, key, careers_url or final_url, "ats_found", f"found on {final_url}")

    soup = BeautifulSoup(html, "html.parser")
    for iframe in soup.find_all("iframe", src=True):
        src = urljoin(final_url, iframe["src"])
        found = extract_ats_from_text(src)
        if found:
            adapter, key, careers_url = found
            return DiscoveryResult(adapter, key, careers_url or src, "ats_found_iframe", f"iframe on {final_url}")

    for script in soup.find_all("script"):
        script_text = script.get_text(" ", strip=True)
        if not script_text:
            continue
        found = extract_ats_from_text(script_text)
        if found:
            adapter, key, careers_url = found
            return DiscoveryResult(adapter, key, careers_url or final_url, "ats_found_script", f"script on {final_url}")

    follow_links = find_career_links(final_url, html)
    for link in follow_links:
        sub = fetch(session, link)
        if sub is None or sub.status_code >= 400:
            continue
        sub_html = sub.text or ""
        found = extract_ats_from_text(sub_html)
        if found:
            adapter, key, careers_url = found
            return DiscoveryResult(adapter, key, careers_url or sub.url, "ats_found_link", f"via link {link}")
        found = extract_ats_from_text(sub.url)
        if found:
            adapter, key, careers_url = found
            return DiscoveryResult(adapter, key, careers_url or sub.url, "ats_found_redirect", f"redirect via {link}")

    return DiscoveryResult("custom_site", None, final_url, "career_page_only", f"no ATS found on {final_url}")


def try_current_link(session: requests.Session, company: dict) -> DiscoveryResult:
    careers_url = normalize_url(company.get("careers_url"))
    adapter = (company.get("adapter") or "").strip()
    adapter_key = (company.get("adapter_key") or "").strip()

    if not careers_url:
        return DiscoveryResult(None, None, None, "missing_url", "no careers_url present")

    response = fetch(session, careers_url)
    if response is None:
        return DiscoveryResult(None, None, None, "fetch_failed", f"request failed for current url {careers_url}")
    if response.status_code >= 400:
        return DiscoveryResult(None, None, None, "http_error", f"{response.status_code} for current url {careers_url}")

    text = response.text or ""
    final_url = response.url
    found = extract_ats_from_text(final_url) or extract_ats_from_text(text)

    if adapter in {"greenhouse", "lever", "ashby", "workday"}:
        return DiscoveryResult(adapter, adapter_key or None, careers_url, "current_ok", f"current adapter/url responded 200 at {final_url}")

    if found:
        detected_adapter, detected_key, detected_url = found
        return DiscoveryResult(detected_adapter, detected_key or None, detected_url or final_url, "current_upgraded", f"found ATS on current url {final_url}")

    return DiscoveryResult("custom_site", None, final_url, "current_ok_custom", f"current careers page responded 200 at {final_url}")


def discover_for_company(session: requests.Session, company: dict) -> DiscoveryResult:
    name = company.get("name", "Unknown")
    # Use the company name as a potential ATS slug (common for Greenhouse/Lever)
    slug = re.sub(r'[^a-zA-Z0-9]', '', name).lower()
    domain = normalize_domain(company.get("domain", ""))
    
    if not domain:
        return DiscoveryResult(None, None, None, "ERROR", "No domain")

    # --- PHASE 1: DIRECT ATS GUESSING (The "Hail Mary") ---
    # This is fast and avoids hitting the company's main website.
    ats_guesses = [
        ("greenhouse", f"https://boards.greenhouse.io/{slug}"),
        ("lever", f"https://jobs.lever.co/{slug}"),
        ("ashby", f"https://jobs.ashbyhq.com/{slug}"),
    ]
    
    for adapter, url in ats_guesses:
        try:
            resp = session.get(url, timeout=5, allow_redirects=True)
            if resp.status_code == 200 and adapter in resp.url:
                return DiscoveryResult(adapter, slug, resp.url, "FOUND", f"Direct {adapter} match")
        except: continue

    # --- PHASE 2: REDUCED WATERFALL PROBING ---
    # We include 'www.' and only check the most likely career paths
    base_candidates = [f"https://{domain}", f"https://www.{domain}", f"https://careers.{domain}"]
    best_fallback = None

    for base in base_candidates:
        # Check only the top 3 most common paths to reduce noise
        for path in ["", "/careers", "/jobs"]:
            url = urljoin(base, path)
            try:
                resp = session.get(url, timeout=REQUEST_TIMEOUT, allow_redirects=True)
                
                # If we get a 403, we are being blocked. Log it!
                if resp.status_code == 403:
                    return DiscoveryResult(None, None, None, "BLOCKED", "Anti-bot (403)")
                
                if resp.status_code != 200:
                    continue

                content = resp.text.lower()
                final_url = resp.url
                
                # Capture fallback URL if it looks like a career page
                if not best_fallback and any(x in final_url for x in ["job", "career"]):
                    best_fallback = final_url

                # Check for ATS markers in the page content
                for ats in ["greenhouse", "lever", "ashby", "workday"]:
                    if f"{ats}.io" in content or f"{ats}.co" in content or f"{ats}." in final_url:
                        key = extract_ats_key(content, final_url, ats) if ats != "workday" else None
                        return DiscoveryResult(ats, key, final_url, "FOUND", f"{ats} detected")

            except Exception:
                continue

    if best_fallback:
        return DiscoveryResult("manual", None, best_fallback, "FALLBACK", "Generic career page")

    return DiscoveryResult(None, None, None, "NOT_FOUND", "No ATS or Career page found")

def update_company_record(company: dict, result: DiscoveryResult) -> bool:
    changed = False
    
    # If we found something, update the careers_url
    if result.careers_url and company.get("careers_url") != result.careers_url:
        company["careers_url"] = result.careers_url
        changed = True
    
    # Update the adapter
    if result.adapter and company.get("adapter") != result.adapter:
        company["adapter"] = result.adapter
        changed = True

    # Update the key if found
    if result.adapter_key and company.get("adapter_key") != result.adapter_key:
        company["adapter_key"] = result.adapter_key
        changed = True
        
    return changed


def heal_registry(yaml_file: str = YAML_FILE) -> None:
    yaml_path = Path(yaml_file)
    if not yaml_path.exists():
        print(f"Error: {yaml_file} not found in this folder.")
        return

    with yaml_path.open("r", encoding="utf-8") as f:
        data = yaml.safe_load(f) or {}

    companies = data.get("companies", [])
    print(f"Scanning {len(companies)} companies...")

    # Updated backup logic
    shutil.copy2(yaml_path, Path(BACKUP_FILE))

    session = make_session()
    updated_count = 0
    report_rows = []

    for idx, company in enumerate(companies, start=1):
        name = company.get("name", "<unknown>")
        active = company.get("active", True)
        if active is False:
            report_rows.append({
                "name": name,
                "status": "inactive_skipped",
                "detail": "inactive company",
                "old_adapter": company.get("adapter", ""),
                "new_adapter": company.get("adapter", ""),
                "old_url": company.get("careers_url", ""),
                "new_url": company.get("careers_url", ""),
            })
            continue

        old_adapter = company.get("adapter", "")
        old_url = company.get("careers_url", "")
        result = discover_for_company(session, company)
        changed = update_company_record(company, result)
        if changed:
            updated_count += 1

        report_rows.append({
            "name": name,
            "status": result.status,
            "detail": result.detail,
            "old_adapter": old_adapter,
            "new_adapter": company.get("adapter", ""),
            "old_url": old_url,
            "new_url": company.get("careers_url", ""),
        })

        marker = "UPDATED" if changed else "OK"
        print(f"[{idx:>4}/{len(companies)}] {marker:<7} {name:<35} {result.status}")
        time.sleep(SLEEP_BETWEEN_COMPANIES)

    with yaml_path.open("w", encoding="utf-8") as f:
        yaml.safe_dump(data, f, sort_keys=False, allow_unicode=True)

    report_path = Path(REPORT_FILE)
# Add this line to ensure the 'results' folder exists
    report_path.parent.mkdir(parents=True, exist_ok=True)
    with report_path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=["name", "status", "detail", "old_adapter", "new_adapter", "old_url", "new_url"],
        )
        writer.writeheader()
        writer.writerows(report_rows)

    print("\nDone.")
    print(f"Updated companies: {updated_count}")
    print(f"Backup written to: {BACKUP_FILE}")
    print(f"Report written to: {report_path}")


if __name__ == "__main__":
    heal_registry()
