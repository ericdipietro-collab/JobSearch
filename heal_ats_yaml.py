import csv
import re
import shutil
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Optional
from urllib.parse import urljoin, urlparse

import requests
import yaml
from bs4 import BeautifulSoup

BASE_DIR = Path(__file__).resolve().parent
YAML_FILE = BASE_DIR / "config" / "job_search_companies.yaml"
BACKUP_FILE = BASE_DIR / "config" / "job_search_companies.yaml.bak"
REPORT_FILE = BASE_DIR / "results" / "heal_ats_yaml_report.csv"

REQUEST_TIMEOUT = 12
SLEEP_BETWEEN_COMPANIES = 0.35
SLEEP_BETWEEN_REQUESTS = 0.20
MAX_INTERNAL_LINKS_PER_PAGE = 12

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/135.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
    "DNT": "1",
    "Upgrade-Insecure-Requests": "1",
}

COMMON_CAREER_PATHS = [
    "",
    "/careers",
    "/careers/",
    "/jobs",
    "/jobs/",
    "/about/careers",
    "/about-us/careers",
    "/about-us/careers/",
    "/company/careers",
    "/company/careers/",
    "/careers/job-openings",
    "/careers/open-positions",
    "/join-us",
    "/work-with-us",
]

COMMON_SUBDOMAINS = ["", "www", "careers", "jobs"]

CAREER_LINK_HINTS = [
    "career",
    "careers",
    "job",
    "jobs",
    "open positions",
    "openings",
    "join our team",
    "join-us",
    "work with us",
    "work-with-us",
]

INVALID_PAGE_MARKERS = [
    "page not found",
    "the page you requested was not found",
    "the job board you were viewing is no longer active",
    "job board you were viewing is no longer active",
    "job board is no longer active",
    "this page doesn't exist",
    "this page does not exist",
    "the page you were looking for doesn't exist",
    "the page you were looking for does not exist",
    "requested page could not be found",
]

ATS_HOST_MARKERS = {
    "greenhouse": ["greenhouse.io", "boards.greenhouse.io", "job-boards.greenhouse.io"],
    "lever": ["jobs.lever.co", "lever.co"],
    "ashby": ["jobs.ashbyhq.com", "ashbyhq.com"],
    "workday": ["myworkdayjobs.com"],
}

ATS_PATTERNS = {
    "greenhouse": [
        re.compile(r"https?://(?:job-boards\\.)?greenhouse\\.io/([\\w.-]+)", re.I),
        re.compile(r"https?://boards\\.greenhouse\\.io/([\\w.-]+)", re.I),
    ],
    "lever": [
        re.compile(r"https?://jobs\\.lever\\.co/([\\w.-]+)", re.I),
    ],
    "ashby": [
        re.compile(r"https?://jobs\\.ashbyhq\\.com/([\\w.-]+)", re.I),
    ],
    "workday": [
        re.compile(r'https?://([\\w.-]+\\.myworkdayjobs\\.com/[^\\s"\\'<>]+)', re.I),
        re.compile(r'https?://([\\w.-]+\\.wd\\d+\\.myworkdayjobs\\.com/[^\\s"\\'<>]+)', re.I),
    ],
}


@dataclass
class DiscoveryResult:
    adapter: Optional[str]
    adapter_key: Optional[str]
    careers_url: Optional[str]
    domain: Optional[str]
    status: str
    detail: str


def make_session() -> requests.Session:
    session = requests.Session()
    session.headers.update(HEADERS)
    adapter = requests.adapters.HTTPAdapter(pool_connections=2, pool_maxsize=2, max_retries=0)
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    return session


def normalize_domain(domain: Optional[str]) -> str:
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


def base_company_tokens(name: str) -> list[str]:
    tokens = re.findall(r"[a-z0-9]+", (name or "").lower())
    stop = {
        "the", "inc", "llc", "ltd", "corp", "corporation", "company", "co", "group",
        "financial", "bank", "holdings", "technologies",
    }
    out = [t for t in tokens if len(t) > 2 and t not in stop]
    return out or [re.sub(r"[^a-z0-9]+", "", (name or "").lower())]


def pause(seconds: float = SLEEP_BETWEEN_REQUESTS) -> None:
    time.sleep(seconds)


def get_page_text(soup: BeautifulSoup, limit: int = 4000) -> str:
    return " ".join(" ".join(soup.stripped_strings).lower().split())[:limit]


def page_has_invalid_markers(url: str, html: str) -> bool:
    soup = BeautifulSoup(html, "html.parser")
    title = (soup.title.string or "").strip().lower() if soup.title else ""
    text = get_page_text(soup)
    combined = f"{title} {text}".strip()
    host = normalize_domain(urlparse(url).netloc)

    if any(marker in combined for marker in INVALID_PAGE_MARKERS):
        return True

    if any(h in host for h in ATS_HOST_MARKERS["greenhouse"]):
        if "greenhouse recruiting" in combined and "page not found" in combined:
            return True
        if "job board" in combined and "no longer active" in combined:
            return True

    if any(h in host for h in ATS_HOST_MARKERS["ashby"]):
        if "powered by ashby" in combined and "page not found" in combined:
            return True

    return False


def company_name_matches_page(company_name: str, url: str, html: str) -> bool:
    soup = BeautifulSoup(html, "html.parser")
    pieces = []

    title = (soup.title.string or "") if soup.title else ""
    pieces.append(title)

    for tag_name in ["h1", "h2"]:
        tag = soup.find(tag_name)
        if tag:
            pieces.append(tag.get_text(" ", strip=True))

    for key in ["og:site_name", "application-name", "twitter:title"]:
        node = soup.find("meta", attrs={"property": key}) or soup.find("meta", attrs={"name": key})
        if node and node.get("content"):
            pieces.append(node["content"])

    combined = " ".join(pieces).lower()
    host = normalize_domain(urlparse(url).netloc)
    tokens = base_company_tokens(company_name)

    if len(tokens) == 1:
        tok = tokens[0]
        return tok in combined or tok in host

    full_norm = " ".join(tokens)
    if full_norm in combined:
        return True

    matched = sum(1 for tok in tokens if tok in combined or tok in host)
    return matched >= max(1, len(tokens) - 1)


def page_has_career_hints(url: str, html: str) -> bool:
    soup = BeautifulSoup(html, "html.parser")
    title = (soup.title.string or "").lower() if soup.title else ""
    text = get_page_text(soup)
    combined = f"{title} {url.lower()} {text}"
    return any(hint in combined for hint in CAREER_LINK_HINTS)


def extract_ats_from_source(source: str) -> Optional[tuple[str, Optional[str], str]]:
    for adapter, patterns in ATS_PATTERNS.items():
        for pattern in patterns:
            match = pattern.search(source or "")
            if not match:
                continue
            if adapter == "workday":
                full_url = "https://" + match.group(1).rstrip("/")
                return adapter, None, full_url
            key = match.group(1).split("?")[0].strip("/")
            if adapter == "greenhouse":
                careers_url = f"https://job-boards.greenhouse.io/{key}"
            elif adapter == "lever":
                careers_url = f"https://jobs.lever.co/{key}"
            else:
                careers_url = f"https://jobs.ashbyhq.com/{key}"
            return adapter, key, careers_url
    return None


def detect_embedded_ats(url: str, html: str) -> Optional[tuple[str, Optional[str], str]]:
    direct = extract_ats_from_source(url) or extract_ats_from_source(html)
    if direct:
        return direct

    soup = BeautifulSoup(html, "html.parser")

    for tag_name, attr in [("a", "href"), ("iframe", "src"), ("script", "src")]:
        for tag in soup.find_all(tag_name):
            raw = (tag.get(attr) or "").strip()
            if not raw:
                continue
            absolute = urljoin(url, raw)
            found = extract_ats_from_source(absolute)
            if found:
                return found

    for script in soup.find_all("script"):
        script_text = script.get_text(" ", strip=True)
        found = extract_ats_from_source(script_text)
        if found:
            return found

    return None


def fetch(session: requests.Session, url: str) -> Optional[requests.Response]:
    try:
        resp = session.get(url, timeout=REQUEST_TIMEOUT, allow_redirects=True)
        pause()
        return resp
    except requests.RequestException:
        return None


def validate_candidate_page(company: dict, response: requests.Response) -> tuple[bool, str]:
    if response.status_code >= 400:
        return False, f"http_{response.status_code}"

    final_url = response.url
    html = response.text or ""
    if page_has_invalid_markers(final_url, html):
        return False, "soft_404"

    host = normalize_domain(urlparse(final_url).netloc)
    embedded = detect_embedded_ats(final_url, html)
    company_name = company.get("name", "")

    if any(marker in host for markers in ATS_HOST_MARKERS.values() for marker in markers):
        if company_name_matches_page(company_name, final_url, html):
            return True, "valid_ats"
        return False, "ats_wrong_company"

    if company_name_matches_page(company_name, final_url, html) or page_has_career_hints(final_url, html):
        return True, "valid_career_page"

    if embedded:
        return True, "embedded_ats"

    return False, "not_career_related"


def company_site_candidates(company: dict) -> list[str]:
    domains = []
    raw_domain = normalize_domain(company.get("domain"))
    current_url = normalize_url(company.get("careers_url"))
    current_host = normalize_domain(urlparse(current_url).netloc) if current_url else ""

    for d in [raw_domain, current_host]:
        if not d:
            continue
        if any(marker in d for markers in ATS_HOST_MARKERS.values() for marker in markers):
            continue
        if d not in domains:
            domains.append(d)

    urls = []
    for domain in domains:
        for sub in COMMON_SUBDOMAINS:
            host = f"{sub}.{domain}" if sub else domain
            base = f"https://{host}"
            if base not in urls:
                urls.append(base)
    return urls


def career_links_from_page(base_url: str, html: str) -> list[str]:
    soup = BeautifulSoup(html, "html.parser")
    found = []

    for a in soup.find_all("a", href=True):
        href = (a.get("href") or "").strip()
        if not href or href.startswith("mailto:") or href.startswith("tel:"):
            continue
        label = " ".join(a.get_text(" ", strip=True).lower().split())
        href_l = href.lower()
        if any(hint in href_l for hint in CAREER_LINK_HINTS) or any(hint in label for hint in CAREER_LINK_HINTS):
            found.append(urljoin(base_url, href))

    deduped = []
    seen = set()
    for link in found:
        if link not in seen:
            seen.add(link)
            deduped.append(link)
    return deduped[:MAX_INTERNAL_LINKS_PER_PAGE]


def try_current_careers_url(session: requests.Session, company: dict) -> Optional[DiscoveryResult]:
    current_url = normalize_url(company.get("careers_url"))
    if not current_url:
        return None

    response = fetch(session, current_url)
    if response is None:
        return None

    valid, reason = validate_candidate_page(company, response)
    if not valid:
        return None

    final_url = response.url
    html = response.text or ""
    embedded = detect_embedded_ats(final_url, html)

    if embedded:
        adapter, adapter_key, ats_url = embedded
        return DiscoveryResult(
            adapter=adapter,
            adapter_key=adapter_key,
            careers_url=ats_url,
            domain=normalize_domain(urlparse(ats_url).netloc),
            status="CURRENT_VALID",
            detail=f"current url validated and exposes {adapter}",
        )

    adapter = company.get("adapter")
    adapter_key = company.get("adapter_key")
    if adapter in {"greenhouse", "lever", "ashby", "workday"}:
        return DiscoveryResult(
            adapter=adapter,
            adapter_key=adapter_key,
            careers_url=final_url,
            domain=normalize_domain(urlparse(final_url).netloc),
            status="CURRENT_VALID",
            detail=f"current careers_url validated ({reason})",
        )

    return DiscoveryResult(
        adapter="manual",
        adapter_key=None,
        careers_url=final_url,
        domain=normalize_domain(urlparse(final_url).netloc),
        status="CURRENT_VALID",
        detail=f"current career page validated ({reason})",
    )


def search_company_site(session: requests.Session, company: dict) -> Optional[DiscoveryResult]:
    fallback_url = None

    for base in company_site_candidates(company):
        for path in COMMON_CAREER_PATHS:
            candidate = urljoin(base, path)
            response = fetch(session, candidate)
            if response is None:
                continue

            valid, reason = validate_candidate_page(company, response)
            if not valid:
                continue

            final_url = response.url
            html = response.text or ""
            embedded = detect_embedded_ats(final_url, html)
            if embedded:
                adapter, adapter_key, ats_url = embedded
                return DiscoveryResult(
                    adapter=adapter,
                    adapter_key=adapter_key,
                    careers_url=ats_url,
                    domain=normalize_domain(urlparse(ats_url).netloc),
                    status="FOUND",
                    detail=f"embedded {adapter} found from {candidate}",
                )

            if fallback_url is None and page_has_career_hints(final_url, html):
                fallback_url = final_url

            for link in career_links_from_page(final_url, html):
                sub_resp = fetch(session, link)
                if sub_resp is None:
                    continue
                sub_valid, _ = validate_candidate_page(company, sub_resp)
                if not sub_valid:
                    continue
                sub_url = sub_resp.url
                sub_html = sub_resp.text or ""

                embedded = detect_embedded_ats(sub_url, sub_html)
                if embedded:
                    adapter, adapter_key, ats_url = embedded
                    return DiscoveryResult(
                        adapter=adapter,
                        adapter_key=adapter_key,
                        careers_url=ats_url,
                        domain=normalize_domain(urlparse(ats_url).netloc),
                        status="FOUND",
                        detail=f"embedded {adapter} found via {link}",
                    )

                if fallback_url is None and page_has_career_hints(sub_url, sub_html):
                    fallback_url = sub_url

    if fallback_url:
        return DiscoveryResult(
            adapter="manual",
            adapter_key=None,
            careers_url=fallback_url,
            domain=normalize_domain(urlparse(fallback_url).netloc),
            status="FALLBACK",
            detail="validated company career page",
        )

    return None


def slug_candidates(company: dict) -> list[str]:
    name = company.get("name", "")
    base_slug = re.sub(r"[^a-zA-Z0-9]", "", name).lower()
    adapter_key = re.sub(r"[^a-zA-Z0-9]", "", str(company.get("adapter_key") or "")).lower()
    candidates = [c for c in [adapter_key, base_slug, f"{base_slug}inc", f"{base_slug}1"] if c]
    out = []
    seen = set()
    for c in candidates:
        if c not in seen:
            seen.add(c)
            out.append(c)
    return out


def direct_ats_guess(session: requests.Session, company: dict) -> Optional[DiscoveryResult]:
    company_name = company.get("name", "")
    for slug in slug_candidates(company):
        for adapter, template in [
            ("greenhouse", "https://job-boards.greenhouse.io/{slug}"),
            ("greenhouse", "https://boards.greenhouse.io/{slug}"),
            ("lever", "https://jobs.lever.co/{slug}"),
            ("ashby", "https://jobs.ashbyhq.com/{slug}"),
        ]:
            url = template.format(slug=slug)
            response = fetch(session, url)
            if response is None:
                continue
            final_url = response.url
            html = response.text or ""
            if response.status_code >= 400 or page_has_invalid_markers(final_url, html):
                continue
            if not company_name_matches_page(company_name, final_url, html):
                continue

            detected = detect_embedded_ats(final_url, html) or extract_ats_from_source(final_url)
            if detected:
                det_adapter, adapter_key, ats_url = detected
                return DiscoveryResult(
                    adapter=det_adapter,
                    adapter_key=adapter_key,
                    careers_url=ats_url,
                    domain=normalize_domain(urlparse(ats_url).netloc),
                    status="FOUND",
                    detail=f"direct {det_adapter} guess validated",
                )

            return DiscoveryResult(
                adapter=adapter,
                adapter_key=slug if adapter != "workday" else None,
                careers_url=final_url,
                domain=normalize_domain(urlparse(final_url).netloc),
                status="FOUND",
                detail=f"direct {adapter} guess validated",
            )
    return None


def discover_for_company(session: requests.Session, company: dict) -> DiscoveryResult:
    current = try_current_careers_url(session, company)
    if current:
        return current

    site_result = search_company_site(session, company)
    if site_result:
        return site_result

    ats_result = direct_ats_guess(session, company)
    if ats_result:
        return ats_result

    return DiscoveryResult(
        adapter=None,
        adapter_key=None,
        careers_url=normalize_url(company.get("careers_url")),
        domain=normalize_domain(company.get("domain")),
        status="NOT_FOUND",
        detail="no validated ATS or career page found",
    )


def update_company_record(company: dict, result: DiscoveryResult) -> bool:
    changed = False

    if result.careers_url and company.get("careers_url") != result.careers_url:
        company["careers_url"] = result.careers_url
        changed = True

    if result.adapter is not None and company.get("adapter") != result.adapter:
        company["adapter"] = result.adapter
        changed = True

    if result.adapter_key is not None:
        if company.get("adapter_key") != result.adapter_key:
            company["adapter_key"] = result.adapter_key
            changed = True
    elif result.adapter in {"manual", None} and company.get("adapter_key") not in (None, ""):
        company["adapter_key"] = None
        changed = True

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
    updated_count = 0
    report_rows = []

    for idx, company in enumerate(companies, start=1):
        name = company.get("name", "<unknown>")
        if company.get("active") is False:
            continue

        old_adapter = company.get("adapter", "")
        old_url = company.get("careers_url", "")
        old_domain = company.get("domain", "")

        result = discover_for_company(session, company)
        changed = update_company_record(company, result)
        if changed:
            updated_count += 1

        report_rows.append(
            {
                "name": name,
                "status": result.status,
                "detail": result.detail,
                "old_adapter": old_adapter,
                "new_adapter": company.get("adapter", ""),
                "old_domain": old_domain,
                "new_domain": company.get("domain", ""),
                "old_url": old_url,
                "new_url": company.get("careers_url", ""),
            }
        )

        marker = "UPDATED" if changed else "OK"
        print(
            f"[{idx:>4}/{len(companies)}] {marker:<7} "
            f"{name:<35} {result.status:<12} {result.detail}"
        )
        time.sleep(SLEEP_BETWEEN_COMPANIES)

    with YAML_FILE.open("w", encoding="utf-8") as f:
        yaml.safe_dump(data, f, sort_keys=False, allow_unicode=True)

    REPORT_FILE.parent.mkdir(parents=True, exist_ok=True)
    with REPORT_FILE.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=[
                "name",
                "status",
                "detail",
                "old_adapter",
                "new_adapter",
                "old_domain",
                "new_domain",
                "old_url",
                "new_url",
            ],
        )
        writer.writeheader()
        writer.writerows(report_rows)

    print(f"\nDone. Updated {updated_count} companies.")
    print(f"Backup written to: {BACKUP_FILE}")
    print(f"Report written to: {REPORT_FILE}")


if __name__ == "__main__":
    heal_registry()
