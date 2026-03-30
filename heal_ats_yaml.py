import argparse
import csv
import random
import re
import shutil
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from pathlib import Path
from typing import List, Optional
from urllib.parse import urljoin, urlparse

import requests
import yaml
from bs4 import BeautifulSoup

# --- Configuration ---
BASE_DIR = Path(__file__).resolve().parent
YAML_FILE = BASE_DIR / "config" / "job_search_companies.yaml"
BACKUP_FILE = BASE_DIR / "config" / "job_search_companies.yaml.bak"
REPORT_FILE = BASE_DIR / "results" / "heal_ats_yaml_report.csv"
REPORT_FILE.parent.mkdir(exist_ok=True)

REQUEST_TIMEOUT = 7    # reduced: 12 → 7s (parallel workers tolerate tighter timeouts)
MIN_SLEEP = 1.5        # minimum seconds between companies (sequential mode only)
MAX_SLEEP = 4.0        # maximum seconds between companies (sequential mode only)
RATE_LIMIT_SLEEP = 45  # sleep when we hit a 429/503
MAX_WORKERS = 8        # parallel threads for discovery

# Rotate User-Agents per company to reduce bot-detection fingerprinting.
USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:123.0) Gecko/20100101 Firefox/123.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.3 Safari/605.1.15",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
]

# Base headers without User-Agent — UA is rotated per company in the main loop.
HEADERS = {
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


@dataclass
class DiscoveryResult:
    adapter: Optional[str]
    adapter_key: Optional[str]
    careers_url: Optional[str]
    # ATS host for reporting (e.g. "boards.greenhouse.io"); not written to YAML domain field.
    ats_host: Optional[str]
    status: str
    detail: str


def make_session() -> requests.Session:
    session = requests.Session()
    session.headers.update(HEADERS)
    session.headers["User-Agent"] = random.choice(USER_AGENTS)
    # Force-close connections after each request to avoid sticky sessions that
    # some bot-detection systems use to fingerprint clients.
    http_adapter = requests.adapters.HTTPAdapter(pool_connections=1, pool_maxsize=1)
    session.mount("https://", http_adapter)
    session.mount("http://", http_adapter)
    return session


def _jitter(min_s: float = MIN_SLEEP, max_s: float = MAX_SLEEP) -> None:
    """Sleep a random duration to avoid predictable request patterns."""
    time.sleep(random.uniform(min_s, max_s))


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
        # Stop at &, \, {, } in addition to quotes/whitespace/angle-brackets so that
        # HTML-encoded quotes (&#34;) and JSON remnants don't bleed into the captured path.
        re.compile(r"https?://([\w.-]+\.myworkdayjobs\.com/[^\"'&\s<>\\{}]+)", re.I),
    ],
}


# --- Career link scoring for homepage waterfall ---
# Weights for keywords found in anchor href or visible text.
CAREER_LINK_KEYWORDS: List[tuple] = [
    ("careers", 5),
    ("career", 5),
    ("jobs", 4),
    ("join us", 4),
    ("join-us", 4),
    ("work with us", 4),
    ("work-with-us", 4),
    ("openings", 3),
    ("open roles", 3),
    ("open-roles", 3),
    ("hiring", 3),
    ("opportunities", 2),
    ("positions", 2),
    ("talent", 2),
    ("job", 2),
    ("work", 1),
    ("team", 1),
]

# Penalise links that are clearly not job boards.
CAREER_LINK_PENALTIES: List[str] = [
    "privacy", "cookie", "legal", "terms", "accessibility",
    "login", "logout", "signin", "signup", "press", "news",
    "blog", "investor", "media", "about", "contact", "support",
    "linkedin.com", "twitter.com", "x.com", "facebook.com",
    "instagram.com", "youtube.com", "glassdoor.com", "indeed.com",
    "mailto:", "tel:", "#",
]


def score_career_link(href: str, text: str) -> int:
    """Score an anchor tag by career-page keyword relevance. Higher = more likely a careers page."""
    combined = (href + " " + text).lower()
    score = 0
    for keyword, weight in CAREER_LINK_KEYWORDS:
        if keyword in combined:
            score += weight
    for penalty in CAREER_LINK_PENALTIES:
        if penalty in combined:
            score -= 6
    return score


def _homepage_career_links(session: requests.Session, domain: str) -> List[str]:
    """
    Fetch the company homepage and return up to 5 internal links scored as likely career pages,
    ranked highest-score first.  These are prepended to the Phase 4 waterfall so we follow
    real site navigation rather than purely guessing paths.
    """
    candidates: List[tuple] = []  # (score, url)
    for root_url in (f"https://www.{domain}", f"https://{domain}"):
        try:
            resp = session.get(root_url, timeout=REQUEST_TIMEOUT, allow_redirects=True)
            if resp.status_code != 200:
                continue
            soup = BeautifulSoup(resp.text, "html.parser")
            base = f"https://{urlparse(resp.url).netloc}"
            for a in soup.find_all("a", href=True):
                href = a["href"].strip()
                text = a.get_text(" ", strip=True)
                abs_href = urljoin(base, href)
                # Only keep internal links (same domain family)
                parsed = urlparse(abs_href)
                if domain not in parsed.netloc:
                    continue
                sc = score_career_link(abs_href, text)
                if sc > 0:
                    candidates.append((sc, abs_href))
            break  # got a 200 — no need to try the other variant
        except Exception:
            continue

    # Deduplicate by URL, keep highest score per URL, sort descending
    seen: dict = {}
    for sc, url in candidates:
        if url not in seen or sc > seen[url]:
            seen[url] = sc
    ranked = sorted(seen.items(), key=lambda x: x[1], reverse=True)
    return [url for url, _ in ranked[:5]]


def _ddg_career_search(session: requests.Session, name: str, domain: str) -> Optional[str]:
    """
    Search DuckDuckGo Lite for '{company} careers site:{domain}' and return the first
    result URL that looks like a careers page, or None.  Uses lite.duckduckgo.com which
    requires no API key and returns plain HTML.
    """
    query = f"{name} careers site:{domain}"
    try:
        resp = session.get(
            "https://lite.duckduckgo.com/lite/",
            params={"q": query},
            timeout=REQUEST_TIMEOUT,
            allow_redirects=True,
        )
        if resp.status_code != 200:
            return None
        soup = BeautifulSoup(resp.text, "html.parser")
        for a in soup.find_all("a", href=True):
            href = a["href"].strip()
            # DDG Lite wraps result URLs in a redirect; extract the real URL from uddg= param
            if "uddg=" in href:
                from urllib.parse import parse_qs, urlparse as _up
                qs = parse_qs(_up(href).query)
                real = qs.get("uddg", [None])[0]
                if real:
                    href = real
            if not href.startswith("http"):
                continue
            sc = score_career_link(href, a.get_text(" ", strip=True))
            if sc > 0:
                return href
    except Exception:
        pass
    return None


_WORKDAY_AUTH_PATHS = re.compile(r"/(login|logout|auth|register|resetpassword)(/.*)?$", re.I)

def extract_ats_key(text: str, url: str, adapter: str) -> Optional[str]:
    patterns = ATS_PATTERNS.get(adapter, [])
    for p in patterns:
        for source in [url, text]:
            m = p.search(source)
            if m:
                key = m.group(1).split("?")[0].strip("/\\ ")
                if adapter == "workday":
                    key = _WORKDAY_AUTH_PATHS.sub("", key).strip("/\\ ")
                return key or None
    return None


def _is_ats_not_found(html: str) -> bool:
    """Detect ATS false-200 'Page not found' responses (Greenhouse, Ashby, Lever, etc.)."""
    soup = BeautifulSoup(html, "html.parser")
    title = (soup.title.string or "").lower() if soup.title else ""
    if "not found" in title or "page not found" in title:
        return True
    h1 = soup.find("h1")
    if h1 and "not found" in h1.get_text().lower():
        return True
    return False


def _is_rate_limited(status_code: int) -> bool:
    return status_code in (429, 503, 429)


def verify_page_ownership(html: str, name: str) -> bool:
    """
    Check if the company name appears anywhere in the page's prominent text.
    Checks title, h1/h2, and og:site_name meta tag.
    Uses a first-word heuristic for multi-word names (e.g. "Charles Schwab" → "charles")
    to handle pages that abbreviate or shorten the name.
    """
    soup = BeautifulSoup(html, "html.parser")
    clean_name = name.lower().strip()
    # First significant word of the company name (skips generic "the", "a" prefixes)
    name_words = [w for w in clean_name.split() if len(w) > 2]
    first_word = name_words[0] if name_words else clean_name

    candidates = []

    # <title>
    if soup.title and soup.title.string:
        candidates.append(soup.title.string.lower())

    # <meta property="og:site_name"> and <meta name="application-name">
    for meta in soup.find_all("meta", attrs={"property": "og:site_name"}):
        candidates.append((meta.get("content") or "").lower())
    for meta in soup.find_all("meta", attrs={"name": "application-name"}):
        candidates.append((meta.get("content") or "").lower())

    # <h1> and <h2>
    for tag in soup.find_all(["h1", "h2"]):
        candidates.append(tag.get_text().lower())

    for text in candidates:
        if clean_name in text or first_word in text:
            return True
    return False


def _validate_existing_url(session: requests.Session, url: str, company_name: str, adapter: str) -> bool:
    """
    Returns True if the existing careers_url is still live and valid.
    For known ATS adapters (greenhouse/ashby/lever), also verifies company ownership
    to guard against false-200 'Page not found' responses.
    """
    if not url:
        return False
    # Implementation/staging Workday URLs (impl-wd*) are never production — force re-discovery.
    if re.search(r"\.impl-wd\d+\.myworkdayjobs\.com", url, re.I):
        return False
    try:
        resp = session.get(url, timeout=REQUEST_TIMEOUT, allow_redirects=True)
        if resp.status_code != 200:
            return False
        if _is_ats_not_found(resp.text):
            return False
        if adapter in ("greenhouse", "ashby", "lever"):
            return verify_page_ownership(resp.text, company_name)
        return True
    except Exception:
        return False


def discover_for_company(session: requests.Session, company: dict) -> DiscoveryResult:
    name = company.get("name", "Unknown")
    slug = re.sub(r"[^a-zA-Z0-9]", "", name).lower()
    existing_url = company.get("careers_url", "")
    existing_adapter = company.get("adapter", "")

    # --- MANUAL OVERRIDE: heal_skip=true means full trust — no network check ---
    # The user has manually verified this entry; skip all discovery phases.
    if company.get("heal_skip"):
        return DiscoveryResult(None, None, existing_url, None, "VALID", "User-verified (heal_skip)")

    # --- PRE-CHECK: if the existing URL is still live, skip full discovery ---
    if _validate_existing_url(session, existing_url, name, existing_adapter):
        return DiscoveryResult(None, None, existing_url, None, "VALID", "Existing URL confirmed")

    # Derive a fallback company domain for the waterfall phase.
    # If domain is currently an ATS hostname, reconstruct from name.
    comp_domain = company.get("domain", "").strip().lower()
    if any(x in comp_domain for x in ["greenhouse.io", "ashbyhq.com", "lever.co", "workdayjobs.com"]):
        # If the company name already looks like a domain (e.g. "Bill.com", "Lemonade.com"),
        # use it directly — stripping special chars preserves the dot before the TLD.
        # Otherwise strip all non-alphanumeric chars and append ".com".
        if re.search(r"\.(com|io|net|org|co|ai|app|tech|finance)$", name.lower()):
            comp_domain = re.sub(r"[^a-zA-Z0-9.]", "", name).lower()
        else:
            comp_domain = re.sub(r"[^a-zA-Z0-9]", "", name).lower() + ".com"

    slug_candidates = [slug, f"{slug}1", f"{slug}inc"]
    best_fallback_url = None

    # --- PHASE 1: GREENHOUSE (Direct Probe) ---
    # Try both the legacy boards.greenhouse.io and the newer job-boards.greenhouse.io hosts.
    for s in slug_candidates:
        for gh_host in ("job-boards.greenhouse.io", "boards.greenhouse.io"):
            url = f"https://{gh_host}/{s}"
            try:
                resp = session.get(url, timeout=8, allow_redirects=True)
                if resp.status_code == 406:
                    print(f"  !!! BLOCKED by Greenhouse (406) — sleeping {RATE_LIMIT_SLEEP}s")
                    time.sleep(2)  # brief back-off; parallel workers don't need the full 45s stall
                    return DiscoveryResult(None, None, None, None, "BLOCKED", "Greenhouse WAF 406")
                if _is_rate_limited(resp.status_code):
                    print(f"  Rate limited ({resp.status_code}) on Greenhouse — sleeping {RATE_LIMIT_SLEEP}s")
                    time.sleep(2)  # brief back-off; parallel workers don't need the full 45s stall
                    return DiscoveryResult(None, None, None, None, "RATE_LIMITED", f"Greenhouse HTTP {resp.status_code}")
                if resp.status_code == 200 and "greenhouse.io" in resp.url.lower():
                    if not _is_ats_not_found(resp.text) and verify_page_ownership(resp.text, name):
                        final_host = urlparse(resp.url).netloc
                        return DiscoveryResult("greenhouse", s, resp.url, final_host, "FOUND", f"Greenhouse Verified ({final_host})")
            except Exception:
                pass
            _jitter(0.4, 1.0)  # delay between slug probes on the same ATS host

    # --- PHASE 2: ASHBY (Direct Probe with false-200 guard) ---
    for s in slug_candidates:
        url = f"https://jobs.ashbyhq.com/{s}"
        try:
            resp = session.get(url, timeout=8, allow_redirects=True)
            if _is_rate_limited(resp.status_code):
                print(f"  Rate limited ({resp.status_code}) on Ashby — sleeping {RATE_LIMIT_SLEEP}s")
                time.sleep(2)  # brief back-off; parallel workers don't need the full 45s stall
                return DiscoveryResult(None, None, None, None, "RATE_LIMITED", f"Ashby HTTP {resp.status_code}")
            if resp.status_code == 200 and "ashbyhq.com" in resp.url.lower():
                if not _is_ats_not_found(resp.text) and verify_page_ownership(resp.text, name):
                    return DiscoveryResult("ashby", s, resp.url, "jobs.ashbyhq.com", "FOUND", "Ashby Verified")
        except Exception:
            pass
        _jitter(0.4, 1.0)

    # --- PHASE 3: LEVER (Direct Probe) ---
    for s in slug_candidates:
        url = f"https://jobs.lever.co/{s}"
        try:
            resp = session.get(url, timeout=8, allow_redirects=True)
            if _is_rate_limited(resp.status_code):
                print(f"  Rate limited ({resp.status_code}) on Lever — sleeping {RATE_LIMIT_SLEEP}s")
                time.sleep(2)  # brief back-off; parallel workers don't need the full 45s stall
                return DiscoveryResult(None, None, None, None, "RATE_LIMITED", f"Lever HTTP {resp.status_code}")
            if resp.status_code == 200 and "lever.co" in resp.url.lower():
                if not _is_ats_not_found(resp.text) and verify_page_ownership(resp.text, name):
                    return DiscoveryResult("lever", s, resp.url, "jobs.lever.co", "FOUND", "Lever Verified")
        except Exception:
            pass
        _jitter(0.4, 1.0)

    # --- PHASE 3.5: WORKDAY (probe wd1..wd25 when company is known to use Workday) ---
    # Triggered when the existing adapter is "workday" or the existing URL is a
    # myworkdayjobs.com URL that failed validation.  We extract the tenant slug from
    # the existing URL (most reliable) or fall back to the name-derived slug.
    workday_slug: Optional[str] = None
    if "myworkdayjobs.com" in (existing_url or "").lower():
        m = re.match(r"https?://([\w-]+)\.wd\d+\.myworkdayjobs\.com", existing_url or "", re.I)
        if m:
            workday_slug = m.group(1)
    if workday_slug is None and existing_adapter == "workday":
        workday_slug = slug

    if workday_slug:
        # Extract the site name (careers portal slug) from the existing URL so we probe
        # the full path rather than the bare hostname.  Bare Workday hostnames return 406
        # regardless of whether the wd number is correct; the site path returns 200 on the
        # right number and 500 on the wrong one, making it a reliable discriminator.
        # e.g. "https://capitalone.wd1.myworkdayjobs.com/Capital_One" → site_name = "Capital_One"
        workday_site: Optional[str] = None
        if existing_url:
            _parsed_wd = urlparse(existing_url)
            _path_parts = [p for p in _parsed_wd.path.split("/") if p and p.lower() not in ("en-us", "en_us")]
            if _path_parts:
                workday_site = _path_parts[0]

        for wd_n in range(1, 26):
            if workday_site:
                wd_url = f"https://{workday_slug}.wd{wd_n}.myworkdayjobs.com/{workday_site}"
            else:
                wd_url = f"https://{workday_slug}.wd{wd_n}.myworkdayjobs.com"
            try:
                resp = session.get(wd_url, timeout=7, allow_redirects=True)
                if _is_rate_limited(resp.status_code):
                    print(f"  Rate limited ({resp.status_code}) on Workday wd{wd_n} — sleeping {RATE_LIMIT_SLEEP}s")
                    time.sleep(2)
                    return DiscoveryResult(None, None, None, None, "RATE_LIMITED", f"Workday HTTP {resp.status_code}")
                if resp.status_code == 200 and not _is_ats_not_found(resp.text):
                    final_url = resp.url
                    wd_key = extract_ats_key(resp.text, final_url, "workday")
                    ats_url = f"https://{wd_key}" if wd_key else final_url
                    return DiscoveryResult(
                        "workday", wd_key,
                        ats_url, urlparse(ats_url).netloc,
                        "FOUND", f"Workday wd{wd_n} probe",
                    )
            except Exception:
                pass
            _jitter(0.3, 0.7)

    # --- PHASE 4: WATERFALL (company careers page — scan for embedded ATS links) ---
    # Fetch the homepage first and extract scored career-link candidates to try before
    # falling through to generic guessed paths.  This way we follow real site navigation.
    homepage_candidates = _homepage_career_links(session, comp_domain)

    # Ordered by likelihood: dedicated careers/jobs subdomains first, then www, then root.
    search_bases = [
        f"https://careers.{comp_domain}",
        f"https://jobs.{comp_domain}",
        f"https://hiring.{comp_domain}",
        f"https://www.{comp_domain}",
        f"https://{comp_domain}",
    ]
    # Build the full URL list: homepage-scored candidates first, then guessed paths.
    waterfall_urls: List[str] = list(homepage_candidates)
    for base in search_bases:
        for path in ["", "/careers", "/jobs", "/en/jobs", "/about-us/careers.html", "/company/careers"]:
            candidate = urljoin(base, path)
            if candidate not in waterfall_urls:
                waterfall_urls.append(candidate)

    for url in waterfall_urls:
        try:
            resp = session.get(url, timeout=REQUEST_TIMEOUT, allow_redirects=True)
            if _is_rate_limited(resp.status_code):
                print(f"  Rate limited ({resp.status_code}) on {url} — sleeping {RATE_LIMIT_SLEEP}s")
                time.sleep(2)  # brief back-off; parallel workers don't need the full 45s stall
                continue
            if resp.status_code != 200:
                continue

            f_url = resp.url.lower()
            content = resp.text.lower()

            # Detect redirect straight to a known ATS/HR platform
            _non_parseable_hr = ("salesforce.com", "icims.com", "taleo.net",
                                 "successfactors.com", "smartrecruiters.com",
                                 "jobvite.com", "brassring.com")
            if any(h in f_url for h in _non_parseable_hr):
                return DiscoveryResult("custom_manual", None, resp.url, urlparse(f_url).netloc, "FOUND", f"Redirected to {urlparse(f_url).netloc}")
            # Detect redirect to a parseable ATS (greenhouse, ashby, lever, workday)
            for ats, marker in [("greenhouse", "greenhouse.io"), ("ashby", "ashbyhq.com"),
                                 ("lever", "lever.co"), ("workday", "myworkdayjobs.com")]:
                if marker in f_url:
                    key = extract_ats_key(resp.text, resp.url, ats)
                    if ats == "greenhouse" and key:
                        ats_url = f"https://job-boards.greenhouse.io/{key}"
                    elif ats == "ashby" and key:
                        ats_url = f"https://jobs.ashbyhq.com/{key}"
                    elif ats == "lever" and key:
                        ats_url = f"https://jobs.lever.co/{key}"
                    elif ats == "workday" and key:
                        ats_url = f"https://{key}"
                    else:
                        ats_url = resp.url
                    return DiscoveryResult(ats, key, ats_url, marker, "FOUND", f"Redirected to {ats}")

            # Reject stale or aggregator URLs as fallback candidates.
            _stale_markers = ("/old/", "/archive/", "/legacy/", "/deprecated/")
            _aggregator_domains = ("dejobs.org", "jobs2careers.com", "jora.com", "neuvoo.com")
            url_is_stale = any(m in f_url for m in _stale_markers)
            url_is_aggregator = any(d in f_url for d in _aggregator_domains)

            if not best_fallback_url and not url_is_stale and not url_is_aggregator and any(x in f_url for x in ["career", "job", "opening"]):
                best_fallback_url = resp.url

            # Content-based ATS link detection — extract adapter key when possible
            _ats_markers = [
                ("greenhouse", "greenhouse.io"),
                ("ashby",      "ashbyhq.com"),
                ("lever",      "lever.co"),
                ("workday",    "myworkdayjobs.com"),
            ]
            for ats, marker in _ats_markers:
                if marker in content or marker in f_url:
                    key = extract_ats_key(resp.text, resp.url, ats)
                    if ats == "greenhouse" and key:
                        ats_url = f"https://job-boards.greenhouse.io/{key}"
                    elif ats == "ashby" and key:
                        ats_url = f"https://jobs.ashbyhq.com/{key}"
                    elif ats == "lever" and key:
                        ats_url = f"https://jobs.lever.co/{key}"
                    elif ats == "workday" and key:
                        ats_url = f"https://{key}"
                    else:
                        ats_url = resp.url
                    return DiscoveryResult(ats, key, ats_url, marker, "FOUND", f"{ats} detected in content")

            _jitter(0.5, 1.2)
        except Exception:
            continue

    # --- PHASE 4.5: DUCKDUCKGO LITE SEARCH ---
    # If the waterfall found nothing, try a targeted web search for the company's careers page.
    # Uses DuckDuckGo Lite (no API key required) to find a plausible URL, then re-runs the
    # ATS content scan on whatever page it returns.
    ddg_url = _ddg_career_search(session, name, comp_domain)
    if ddg_url:
        try:
            resp = session.get(ddg_url, timeout=REQUEST_TIMEOUT, allow_redirects=True)
            if resp.status_code == 200:
                f_url = resp.url.lower()
                content = resp.text.lower()
                # Check for ATS redirect
                for ats, marker in [("greenhouse", "greenhouse.io"), ("ashby", "ashbyhq.com"),
                                     ("lever", "lever.co"), ("workday", "myworkdayjobs.com")]:
                    if marker in f_url or marker in content:
                        key = extract_ats_key(resp.text, resp.url, ats)
                        if ats == "greenhouse" and key:
                            ats_url = f"https://job-boards.greenhouse.io/{key}"
                        elif ats == "ashby" and key:
                            ats_url = f"https://jobs.ashbyhq.com/{key}"
                        elif ats == "lever" and key:
                            ats_url = f"https://jobs.lever.co/{key}"
                        elif ats == "workday" and key:
                            ats_url = f"https://{key}"
                        else:
                            ats_url = resp.url
                        return DiscoveryResult(ats, key, ats_url, marker, "FOUND", f"DDG search → {ats}")
                # No ATS found but page looks like a careers page — use as fallback
                if not best_fallback_url and any(x in f_url for x in ["career", "job", "opening"]):
                    best_fallback_url = resp.url
        except Exception:
            pass

    # --- PHASE 5: FALLBACK ---
    if best_fallback_url:
        return DiscoveryResult("custom_manual", None, best_fallback_url, None, "FALLBACK", "Generic Career Page")

    return DiscoveryResult(None, None, None, None, "NOT_FOUND", "No board detected")


def update_company_record(company: dict, result: DiscoveryResult) -> bool:
    """
    Apply discovered values back to the company dict.
    domain is synced to the careers_url hostname when it is currently empty,
    mirroring the auto-derive logic in job_search_v6.py (_normalize_company_record).
    """
    changed = False

    # If the existing URL was confirmed valid, just mark active and fill missing domain.
    if result.status == "VALID":
        if result.careers_url and not company.get("domain"):
            new_domain = urlparse(result.careers_url).netloc.lower()
            if new_domain:
                company["domain"] = new_domain
                changed = True
        if company.get("status") != "active":
            company["status"] = "active"
            changed = True
        return changed

    # Non-actionable outcomes: don't touch URL/adapter, but mark broken on NOT_FOUND.
    if result.status == "NOT_FOUND":
        if company.get("status") != "broken":
            company["status"] = "broken"
            return True
        return False
    if result.status in ("BLOCKED", "RATE_LIMITED"):
        return False

    # Update careers_url
    url_changed = result.careers_url and company.get("careers_url") != result.careers_url
    if url_changed:
        company["careers_url"] = result.careers_url
        changed = True

    # Sync domain whenever careers_url changes (e.g. company switches from Greenhouse
    # to Ashby) or when domain is simply missing.  This keeps the domain aligned with
    # the actual ATS host so job_search_v6.py's source_trust_profile and
    # allowed-hosts logic see the correct hostname.
    if result.careers_url and (url_changed or not company.get("domain")):
        new_domain = urlparse(result.careers_url).netloc.lower()
        if new_domain and company.get("domain") != new_domain:
            company["domain"] = new_domain
            changed = True

    # Update adapter
    if result.adapter and company.get("adapter") != result.adapter:
        company["adapter"] = result.adapter
        changed = True

    # Update adapter_key
    if result.adapter_key is not None and company.get("adapter_key") != result.adapter_key:
        company["adapter_key"] = result.adapter_key
        changed = True

    # Mark confirmed/discovered entries as active.
    if company.get("status") != "active":
        company["status"] = "active"
        changed = True

    return changed


def heal_registry(heal_all: bool = False) -> None:
    if not YAML_FILE.exists():
        print(f"Error: {YAML_FILE} not found.")
        return

    with YAML_FILE.open("r", encoding="utf-8") as f:
        data = yaml.safe_load(f) or {}

    companies = data.get("companies", [])

    # Determine which companies need healing.
    # heal_skip=true entries are always excluded — user has manually verified them.
    # "active" companies are skipped unless --all is passed.
    NEEDS_HEAL = {"new", "changed", "broken", "", None}
    skipped_trust = [c for c in companies if c.get("heal_skip")]
    eligible = [c for c in companies if not c.get("heal_skip") and c.get("active") is not False]
    to_heal = [c for c in eligible if heal_all or c.get("status") in NEEDS_HEAL]
    skipped = len(companies) - len(to_heal)

    mode = "all active" if heal_all else "new / changed / broken"
    print(f"Scanning {len(to_heal)} companies ({mode}) — skipping {skipped} (active/inactive) + {len(skipped_trust)} user-verified (heal_skip).")
    if not to_heal:
        print("Nothing to heal.")
        return

    shutil.copy2(YAML_FILE, BACKUP_FILE)
    updated_count, report_rows = 0, []
    print_lock = threading.Lock()

    def _heal_one(idx: int, company: dict):
        """Worker: create its own session, discover, update in-place, return report row."""
        session = make_session()
        name = company.get("name", "<unknown>")
        old_adapter = company.get("adapter", "")
        old_url = company.get("careers_url", "")

        result = discover_for_company(session, company)
        changed = update_company_record(company, result)

        row = {
            "idx": idx,
            "name": name,
            "heal_status": result.status,
            "detail": result.detail,
            "company_status": company.get("status", ""),
            "old_adapter": old_adapter,
            "new_adapter": company.get("adapter", ""),
            "old_url": old_url,
            "new_url": company.get("careers_url", ""),
            "changed": changed,
        }
        marker = "UPDATED" if changed else "OK"
        with print_lock:
            print(f"[{idx:>4}/{len(to_heal)}] {marker:<7} {name:<35} {result.status} → status:{company.get('status','?')}")
        return row

    # Worker timeout: max seconds we'll wait for a single company before giving up.
    # Covers stuck TCP connections and rate-limit sleeps that outlast everything else.
    WORKER_TIMEOUT = 120

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as pool:
        futures = {
            pool.submit(_heal_one, idx, company): (idx, company.get("name", "?"))
            for idx, company in enumerate(to_heal, start=1)
        }
        for future in as_completed(futures):
            idx, name = futures[future]
            try:
                row = future.result(timeout=WORKER_TIMEOUT)
                if row["changed"]:
                    updated_count += 1
                report_rows.append(row)
            except TimeoutError:
                with print_lock:
                    print(f"[{idx:>4}/{len(to_heal)}] TIMEOUT  {name:<35} worker exceeded {WORKER_TIMEOUT}s — skipped")
            except Exception as exc:
                with print_lock:
                    print(f"[{idx:>4}/{len(to_heal)}] ERROR    {name:<35} {exc}")

    # Sort report by original order for the CSV
    report_rows.sort(key=lambda r: r["idx"])

    with YAML_FILE.open("w", encoding="utf-8") as f:
        yaml.safe_dump(data, f, sort_keys=False, allow_unicode=True)

    REPORT_FILE.parent.mkdir(parents=True, exist_ok=True)
    with REPORT_FILE.open("w", newline="", encoding="utf-8") as f:
        csv_fields = ["name", "heal_status", "detail", "company_status", "old_adapter", "new_adapter", "old_url", "new_url"]
        writer = csv.DictWriter(f, fieldnames=csv_fields, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(report_rows)

    print(f"\nDone. Updated {updated_count} of {len(to_heal)} companies scanned.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Heal and verify ATS URLs in the company registry.")
    parser.add_argument(
        "--all",
        action="store_true",
        dest="heal_all",
        help="Scan all active companies, not just those flagged new/changed/broken.",
    )
    args = parser.parse_args()
    heal_registry(heal_all=args.heal_all)
