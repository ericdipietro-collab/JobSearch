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

# --- Optional deep search add-on (playwright-based) ---
try:
    from deep_search import playwright_adapter as _deep_search_mod
    _DEEP_SEARCH_INSTALLED = True
except ImportError:
    _deep_search_mod = None  # type: ignore[assignment]
    _DEEP_SEARCH_INSTALLED = False


def deep_heal_available() -> bool:
    """Return True if the Playwright add-on is installed and usable."""
    return _DEEP_SEARCH_INSTALLED and _deep_search_mod is not None and _deep_search_mod.is_available()

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


# Matches individual job-listing URL path segments — these get a high base score
# because a sitemap containing /jobs/senior-pm-1234 is strong evidence of a real
# job board at that URL, even without fetching the page.
_JOB_DETAIL_URL_RE = re.compile(
    r"/(job|jobs|careers?|openings?|vacanc(?:y|ies)|req(?:uisition)?|"
    r"position(?:s)?|posting(?:s)?)/[^/\s?#]",
    re.I,
)


def _score_sitemap_loc(url: str) -> int:
    """
    Score a sitemap <loc> URL for career relevance.

    Job detail URLs (containing /jobs/slug, /careers/title-id, /req/1234, etc.)
    score highest because they confirm an active job board.  General career
    landing pages score via the existing career-keyword scorer.  Everything
    else scores 0 and is ignored.
    """
    if _JOB_DETAIL_URL_RE.search(url):
        # Extra bump if the URL also contains a known ATS domain
        ats_bonus = 5 if any(
            m in url.lower() for m in ("greenhouse.io", "ashbyhq.com", "lever.co", "myworkdayjobs.com")
        ) else 0
        return 15 + ats_bonus
    # Fall back to the general career-link scorer for landing pages
    return score_career_link(url, "")


def _sitemap_career_urls(session: requests.Session, domain: str) -> List[str]:
    """
    Fetch the company sitemap and return URLs that look like careers/jobs pages.

    Checks in order:
      1. robots.txt  — picks up Sitemap: directives
      2. /sitemap_index.xml — index of sub-sitemaps; follows each sub-sitemap
      3. /sitemap.xml — flat list

    Returns up to 10 candidate URLs scored by career-keyword relevance,
    highest-score first.
    """
    import xml.etree.ElementTree as ET

    sitemap_urls: List[str] = []

    # 1. Check robots.txt for declared Sitemap: lines
    for root_url in (f"https://www.{domain}", f"https://{domain}"):
        try:
            r = session.get(f"{root_url}/robots.txt", timeout=REQUEST_TIMEOUT, allow_redirects=True)
            if r.status_code == 200:
                for line in r.text.splitlines():
                    if line.strip().lower().startswith("sitemap:"):
                        sm = line.split(":", 1)[1].strip()
                        if sm and sm not in sitemap_urls:
                            sitemap_urls.append(sm)
                break
        except Exception:
            continue

    # 2. Always also try the two standard paths
    for root_url in (f"https://www.{domain}", f"https://{domain}"):
        for path in ("/sitemap_index.xml", "/sitemap.xml"):
            candidate = root_url + path
            if candidate not in sitemap_urls:
                sitemap_urls.append(candidate)

    def _parse_sitemap(xml_text: str) -> List[str]:
        """Extract all <loc> values from a sitemap or sitemap index."""
        locs: List[str] = []
        try:
            root = ET.fromstring(xml_text)
            # Strip namespace
            ns = re.match(r"\{[^}]+\}", root.tag)
            prefix = ns.group(0) if ns else ""
            for elem in root.iter(f"{prefix}loc"):
                if elem.text:
                    locs.append(elem.text.strip())
        except Exception:
            pass
        return locs

    visited: set = set()
    career_candidates: List[str] = []

    for sm_url in sitemap_urls[:6]:   # cap to avoid infinite sitemap chains
        if sm_url in visited:
            continue
        visited.add(sm_url)
        try:
            r = session.get(sm_url, timeout=REQUEST_TIMEOUT, allow_redirects=True)
            if r.status_code != 200:
                continue
            locs = _parse_sitemap(r.text)
            # If this is an index (contains sub-sitemap URLs), recurse one level
            is_index = any(
                "sitemap" in loc.lower() and loc.lower().endswith(".xml")
                for loc in locs
            )
            if is_index:
                for sub in locs[:10]:   # limit sub-sitemaps followed
                    if sub in visited:
                        continue
                    visited.add(sub)
                    try:
                        sr = session.get(sub, timeout=REQUEST_TIMEOUT, allow_redirects=True)
                        if sr.status_code == 200:
                            locs.extend(_parse_sitemap(sr.text))
                    except Exception:
                        continue

            for loc in locs:
                sc = _score_sitemap_loc(loc)
                if sc > 0:
                    career_candidates.append((sc, loc))
        except Exception:
            continue

    # Deduplicate, sort by score descending, return top 10
    seen: dict = {}
    for sc, url in career_candidates:
        if url not in seen or sc > seen[url]:
            seen[url] = sc
    ranked = sorted(seen.items(), key=lambda x: x[1], reverse=True)
    return [url for url, _ in ranked[:10]]


def _web_career_search(session: requests.Session, name: str, domain: str) -> Optional[str]:
    """
    Search for '{company} careers' using Yahoo search (returns static HTML, no JS required)
    and a few other engine fallbacks.  Returns the first result URL that looks like a
    careers page, or None.  No API key required.
    """
    from urllib.parse import parse_qs, urlparse as _up

    query = f"{name} careers site:{domain}"
    query_open = f"{name} careers jobs"  # broader fallback without site: restriction

    def _extract_yahoo_links(html: str, target_domain: str) -> List[str]:
        """Extract external result links from Yahoo search HTML."""
        soup = BeautifulSoup(html, "html.parser")
        results: List[str] = []
        # Yahoo wraps results in <a> with class containing "ac-algo" or rel="noopener"
        # and the real URL is in href directly (not behind a redirect param).
        for a in soup.find_all("a", href=True):
            href = a["href"].strip()
            # Skip Yahoo-internal navigation
            if not href.startswith("http") or "yahoo.com" in href:
                continue
            # Yahoo sometimes wraps via /rd/... — extract RU= param
            if "/rd/" in href and "RU=" in href:
                qs = parse_qs(_up(href).query)
                real = qs.get("RU", [None])[0]
                if real:
                    href = real
            if target_domain and target_domain not in href:
                continue
            results.append(href)
        return results

    def _try_yahoo(q: str, target_domain: str) -> Optional[str]:
        try:
            resp = session.get(
                "https://search.yahoo.com/search",
                params={"p": q, "n": "10"},
                headers={"Referer": "https://search.yahoo.com/"},
                timeout=REQUEST_TIMEOUT,
                allow_redirects=True,
            )
            if resp.status_code == 200:
                for href in _extract_yahoo_links(resp.text, target_domain):
                    sc = score_career_link(href, "")
                    if sc > 0:
                        return href
        except Exception:
            pass
        return None

    def _try_mojeek(q: str, target_domain: str) -> Optional[str]:
        """Mojeek is an independent engine that returns plain HTML results."""
        try:
            resp = session.get(
                "https://www.mojeek.com/search",
                params={"q": q},
                headers={"Referer": "https://www.mojeek.com/"},
                timeout=REQUEST_TIMEOUT,
                allow_redirects=True,
            )
            if resp.status_code == 200:
                soup = BeautifulSoup(resp.text, "html.parser")
                for a in soup.find_all("a", href=True):
                    href = a["href"].strip()
                    if not href.startswith("http") or "mojeek.com" in href:
                        continue
                    if target_domain and target_domain not in href:
                        continue
                    sc = score_career_link(href, a.get_text(" ", strip=True))
                    if sc > 0:
                        return href
        except Exception:
            pass
        return None

    # 1. Yahoo with site: restriction
    result = _try_yahoo(query, domain)
    if result:
        return result
    _jitter(1.0, 2.0)

    # 2. Yahoo with open query — useful when the company's ATS is on a different domain
    result = _try_yahoo(query_open, "")
    if result:
        # Only accept if it's on a known ATS or the company domain
        sc = score_career_link(result, "")
        if sc > 0 and (domain in result or any(
            m in result for m in ("greenhouse.io", "ashbyhq.com", "lever.co", "myworkdayjobs.com")
        )):
            return result
    _jitter(1.0, 2.0)

    # 3. Mojeek fallback
    result = _try_mojeek(query, domain)
    if result:
        return result

    return None


_WORKDAY_AUTH_PATHS = re.compile(r"/(login|logout|auth|register|resetpassword)(/.*)?$", re.I)
# Strip job-detail path segments so we store the board root, not an individual listing.
# e.g. "bmo.wd3.myworkdayjobs.com/External/job/Frisco-CO/..." → "bmo.wd3.myworkdayjobs.com/External"
_WORKDAY_JOB_DETAIL_PATH = re.compile(r"/job(?:s)?/.*$", re.I)

def extract_ats_key(text: str, url: str, adapter: str) -> Optional[str]:
    patterns = ATS_PATTERNS.get(adapter, [])
    for p in patterns:
        for source in [url, text]:
            m = p.search(source)
            if m:
                key = m.group(1).split("?")[0].strip("/\\ ")
                if adapter == "workday":
                    key = _WORKDAY_AUTH_PATHS.sub("", key).strip("/\\ ")
                    key = _WORKDAY_JOB_DETAIL_PATH.sub("", key).strip("/\\ ")
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


_CAREER_PAGE_SIGNALS = [
    "apply", "job", "career", "position", "opening", "opportunity",
    "hiring", "vacancy", "vacancies", "requisition",
]
# Stronger signals that appear on real job-board pages but rarely on generic homepages.
_STRONG_JOB_SIGNALS = [
    "open positions", "open roles", "view all jobs", "view jobs", "see all jobs",
    "apply now", "apply today", "submit application", "job description",
    "job requirements", "qualifications", "we're hiring", "we are hiring",
    "join our team", "current openings", "job openings", "job listings",
    "no open positions", "no current openings",  # "no openings" pages still count
]

# Path suffixes that indicate a company homepage or non-careers page even though
# the URL passed validation.  If the final URL ends with one of these after
# stripping the domain, the page is almost certainly not a dedicated job board.
_NON_CAREER_PATH_SUFFIXES = (
    "/", "", "/index", "/index.html", "/home", "/about", "/about-us",
    "/company", "/team", "/contact", "/products", "/solutions",
)


def _validate_existing_url(session: requests.Session, url: str, company_name: str, adapter: str) -> bool:
    """
    Returns True if the existing careers_url is still live and valid.

    Validation strictness scales with adapter type:
    - greenhouse / ashby / lever : HTTP 200 + not-found check + ownership verification
    - workday                    : HTTP 200 + final URL must remain on myworkdayjobs.com
    - custom_manual / others     : HTTP 200 + URL has career path segment AND content
                                   has either an ATS embed or strong job-listing signals
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

        if adapter == "workday":
            # If the URL redirected away from Workday entirely, the tenant is gone.
            return "myworkdayjobs.com" in resp.url.lower()

        # custom_manual / smartrecruiters / icims / taleo / etc.
        # Require two independent signals:
        #  1. The final URL path looks like a careers page (not the bare homepage)
        #  2. The page content has either an ATS embed OR strong job-listing vocabulary
        f_url = resp.url.lower()
        content = resp.text.lower()

        # Reject if the final URL looks like a plain homepage/about/contact page
        parsed_path = urlparse(resp.url).path.rstrip("/").lower() or "/"
        if parsed_path in _NON_CAREER_PATH_SUFFIXES:
            return False

        # Signal 1: URL path must contain a career/job path segment
        url_has_career_path = any(
            x in f_url for x in ["career", "job", "opening", "hiring", "talent", "work-with-us", "join-us"]
        )
        if not url_has_career_path:
            return False

        # Signal 2: content must have an ATS embed OR strong job-listing vocabulary
        has_ats = any(m in content for m in ["greenhouse.io", "ashbyhq.com", "lever.co", "myworkdayjobs.com",
                                              "smartrecruiters.com", "icims.com", "taleo.net",
                                              "successfactors.com", "jobvite.com", "brassring.com",
                                              "ultipro.com", "ukg.com"])
        if has_ats:
            return True

        # Strong signal: at least one phrase that only appears on real job-listing pages
        has_strong = any(s in content for s in _STRONG_JOB_SIGNALS)
        if has_strong:
            return True

        # Fallback: require 5+ general career-word hits (raised from 3 to reduce false positives)
        hits = sum(1 for s in _CAREER_PAGE_SIGNALS if s in content)
        return hits >= 5

    except Exception:
        return False


def discover_for_company(session: requests.Session, company: dict, force_rediscover: bool = False) -> DiscoveryResult:
    name = company.get("name", "Unknown")
    slug = re.sub(r"[^a-zA-Z0-9]", "", name).lower()
    existing_url = company.get("careers_url", "")
    existing_adapter = company.get("adapter", "")

    # --- MANUAL OVERRIDE: heal_skip=true means full trust — no network check ---
    # The user has manually verified this entry; skip all discovery phases.
    if company.get("heal_skip"):
        return DiscoveryResult(None, None, existing_url, None, "VALID", "User-verified (heal_skip)")

    # --- PRE-CHECK: if the existing URL is still live, skip full discovery ---
    # Skipped when --force is passed so every active company gets full rediscovery.
    if not force_rediscover and _validate_existing_url(session, existing_url, name, existing_adapter):
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

    # --- PHASE 3.5: WORKDAY (probe wd1..wd25) ---
    # Triggered when:
    #  (a) the existing URL is a myworkdayjobs.com URL that failed validation, or
    #  (b) the existing adapter is "workday", or
    #  (c) any broken company — companies sometimes migrate TO Workday from other platforms.
    #      For non-Workday companies we use a lightweight fast probe (just wd1..wd5,
    #      "External" site name only) before the full waterfall to avoid excessive delay.
    workday_slug: Optional[str] = None
    _workday_was_adapter = existing_adapter == "workday" or "myworkdayjobs.com" in (existing_url or "").lower()
    if "myworkdayjobs.com" in (existing_url or "").lower():
        m = re.match(r"https?://([\w-]+)\.wd\d+\.myworkdayjobs\.com", existing_url or "", re.I)
        if m:
            workday_slug = m.group(1)
    if workday_slug is None and _workday_was_adapter:
        workday_slug = slug
    # Cross-platform: probe Workday with the name-derived slug even if the company
    # was previously on a different ATS.  Restrict to wd1–5 for speed.
    if workday_slug is None:
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

        # Build candidate site names to try.  Many companies rename their Workday portal
        # (e.g. "Capital_One" → "Careers") so we probe the original name first, then
        # the most common Workday site name conventions.
        slug_title = slug.capitalize()
        workday_site_candidates: List[Optional[str]] = []
        if workday_site:
            workday_site_candidates.append(workday_site)
        # Common Workday site path names — try these when the original fails
        _common_wd_sites = [
            "External", "Careers", "Jobs", "Career", "Job",
            slug, slug_title,
            f"{slug}_EXT", f"{slug_title}_EXT",
            f"{slug}_External", f"{slug_title}_External",
            "US_External", "GlobalExternal", "Global_External",
            None,  # bare hostname (wd-number probe without a path)
        ]
        for s in _common_wd_sites:
            if s not in workday_site_candidates:
                workday_site_candidates.append(s)

        # For companies that were NOT previously on Workday, do a quick cross-platform
        # probe: "External" site name only, wd1–5.  Avoids spending 5+ minutes probing
        # wd1–25 × 15 site names for every broken non-Workday company.
        if not _workday_was_adapter:
            workday_site_candidates = ["External", "Careers", slug, slug_title]
        wd_max = 25 if _workday_was_adapter else 5

        # If the existing URL has a specific wd number (which may be > 25, e.g. wd103,
        # wd108, wd503), extract it and always try that number first — it's the most
        # likely to still be correct even if the site name changed.
        _wd_n_match = re.search(r"\.wd(\d+)\.myworkdayjobs\.com", existing_url or "", re.I)
        existing_wd_n = int(_wd_n_match.group(1)) if _wd_n_match else None
        if existing_wd_n:
            wd_sweep = [existing_wd_n] + [n for n in range(1, wd_max + 1) if n != existing_wd_n]
        else:
            wd_sweep = list(range(1, wd_max + 1))

        for wd_site_name in workday_site_candidates:
            for wd_n in wd_sweep:
                if wd_site_name:
                    wd_url = f"https://{workday_slug}.wd{wd_n}.myworkdayjobs.com/{wd_site_name}"
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
                        site_label = wd_site_name or "(bare)"
                        return DiscoveryResult(
                            "workday", wd_key,
                            ats_url, urlparse(ats_url).netloc,
                            "FOUND", f"Workday wd{wd_n}/{site_label} probe",
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
                                 "jobvite.com", "brassring.com",
                                 "ultipro.com", "ukg.com")
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
                ("greenhouse",   "greenhouse.io"),
                ("ashby",        "ashbyhq.com"),
                ("lever",        "lever.co"),
                ("workday",      "myworkdayjobs.com"),
                ("custom_manual", "ultipro.com"),
                ("custom_manual", "ukg.com"),
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

                # Explicit iframe scan — catches ATS boards embedded via <iframe src=...>
                for iframe in BeautifulSoup(resp.text, "html.parser").find_all("iframe", src=True):
                    isrc = iframe["src"].strip()
                    if not isrc:
                        continue
                    isrc_l = isrc.lower()
                    for ats, marker in [("greenhouse", "greenhouse.io"), ("ashby", "ashbyhq.com"),
                                        ("lever", "lever.co"), ("workday", "myworkdayjobs.com")]:
                        if marker in isrc_l:
                            key = extract_ats_key("", isrc, ats)
                            if ats == "greenhouse" and key:
                                ats_url = f"https://job-boards.greenhouse.io/{key}"
                            elif ats == "ashby" and key:
                                ats_url = f"https://jobs.ashbyhq.com/{key}"
                            elif ats == "lever" and key:
                                ats_url = f"https://jobs.lever.co/{key}"
                            elif ats == "workday" and key:
                                ats_url = f"https://{key}"
                            else:
                                ats_url = isrc
                            return DiscoveryResult(ats, key, ats_url, marker, "FOUND", f"iframe embed → {ats}")

            _jitter(0.5, 1.2)
        except Exception:
            continue

    # --- PHASE 4.5: WEB SEARCH (Yahoo / Mojeek fallbacks) ---
    # If the waterfall found nothing, try a targeted web search for the company's careers page.
    # Tries Yahoo search (static HTML) then Mojeek.  No API key required.
    ddg_url = _web_career_search(session, name, comp_domain)
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

    # --- PHASE 5: SITEMAP DISCOVERY ---
    # Parse the company's XML sitemap(s) for career/job URLs, then run the
    # standard ATS content scan on each candidate.  Sitemaps are static XML
    # so this works on JS-heavy sites that resist HTML scraping.
    #
    # Two URL types come back from _sitemap_career_urls:
    #  - Job detail URLs  (/jobs/title-id, /req/1234)  — confirmed job board;
    #    derive the base careers URL and scan it for ATS markers rather than
    #    fetching the individual listing (which may be JS-rendered).
    #  - Career landing pages (/careers, /jobs) — fetch and scan normally.
    for sitemap_url in _sitemap_career_urls(session, comp_domain):
        try:
            is_detail = bool(_JOB_DETAIL_URL_RE.search(sitemap_url))

            # For job detail URLs: check if the URL itself reveals the ATS,
            # then derive and fetch the careers base URL instead of the listing.
            if is_detail:
                for ats, marker in [("greenhouse", "greenhouse.io"), ("ashby", "ashbyhq.com"),
                                     ("lever", "lever.co"), ("workday", "myworkdayjobs.com")]:
                    if marker in sitemap_url.lower():
                        key = extract_ats_key("", sitemap_url, ats)
                        if ats == "greenhouse" and key:
                            ats_url = f"https://job-boards.greenhouse.io/{key}"
                        elif ats == "ashby" and key:
                            ats_url = f"https://jobs.ashbyhq.com/{key}"
                        elif ats == "lever" and key:
                            ats_url = f"https://jobs.lever.co/{key}"
                        elif ats == "workday" and key:
                            ats_url = f"https://{key}"
                        else:
                            ats_url = sitemap_url
                        return DiscoveryResult(ats, key, ats_url, marker, "FOUND", f"Sitemap job URL → {ats}")
                # No ATS in the URL itself — derive base careers URL by stripping
                # everything from the job-detail path segment onward.
                m = _JOB_DETAIL_URL_RE.search(sitemap_url)
                base_careers = sitemap_url[:m.start()] if m else sitemap_url
                if not best_fallback_url and base_careers:
                    best_fallback_url = base_careers
                # Fall through to fetch the base URL below
                fetch_url = base_careers or sitemap_url
            else:
                fetch_url = sitemap_url

            resp = session.get(fetch_url, timeout=REQUEST_TIMEOUT, allow_redirects=True)
            if resp.status_code != 200:
                continue
            f_url = resp.url.lower()
            content = resp.text.lower()
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
                    return DiscoveryResult(ats, key, ats_url, marker, "FOUND", f"Sitemap → {ats}")
            # No ATS found — store as fallback if URL looks like a careers page
            if not best_fallback_url and any(x in f_url for x in ["career", "job", "opening"]):
                best_fallback_url = resp.url
        except Exception:
            continue

    # --- PHASE 6: FALLBACK ---
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


def heal_registry(heal_all: bool = False, heal_broken: bool = False, force_rediscover: bool = False, deep_heal: bool = False) -> None:
    if not YAML_FILE.exists():
        print(f"Error: {YAML_FILE} not found.")
        return

    with YAML_FILE.open("r", encoding="utf-8") as f:
        data = yaml.safe_load(f) or {}

    companies = data.get("companies", [])

    # Determine which companies need healing.
    # heal_skip=true entries are always excluded — user has manually verified them.
    # --broken : only companies with status=broken
    # --all    : all non-skip companies (active included), validate existing URLs
    # --force  : all non-skip companies, skip pre-check → full rediscovery every time
    # (default): new / changed / broken
    NEEDS_HEAL = {"new", "changed", "broken", "", None}
    skipped_trust = [c for c in companies if c.get("heal_skip")]
    eligible = [c for c in companies if not c.get("heal_skip") and c.get("active") is not False]

    if heal_broken:
        to_heal = [c for c in eligible if c.get("status") == "broken"]
        mode = "broken only"
    elif force_rediscover or heal_all:
        to_heal = list(eligible)
        mode = "ALL (force rediscover)" if force_rediscover else "all active"
    else:
        to_heal = [c for c in eligible if c.get("status") in NEEDS_HEAL]
        mode = "new / changed / broken"

    skipped = len(companies) - len(to_heal)

    deep_heal_flag = deep_heal and deep_heal_available()
    if deep_heal and not deep_heal_available():
        print("WARNING: --deep-heal requested but playwright is not installed. Run deep_search/install_deep_search.bat first.")
    deep_label = " + deep heal (playwright)" if deep_heal_flag else ""
    print(f"Scanning {len(to_heal)} companies ({mode}{deep_label}) — skipping {skipped} (active/inactive) + {len(skipped_trust)} user-verified (heal_skip).")
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

        result = discover_for_company(session, company, force_rediscover=force_rediscover)

        # Deep heal: fire when static discovery couldn't pin the ATS.
        # Triggers on: NOT_FOUND, FALLBACK, or FOUND-but-custom_manual.
        if deep_heal_flag and result.status not in ("VALID", "BLOCKED", "RATE_LIMITED"):
            needs_deep = (
                result.status in ("NOT_FOUND", "FALLBACK")
                or (result.status == "FOUND" and result.adapter == "custom_manual")
            )
            if needs_deep:
                with print_lock:
                    print(f"  deep heal → {name}")
                try:
                    raw = _deep_search_mod.deep_heal_company(
                        result.careers_url or company.get("careers_url") or "",
                        name,
                        comp_domain=company.get("domain", ""),
                    )
                    if raw:
                        result = DiscoveryResult(
                            adapter=raw["adapter"],
                            adapter_key=raw.get("adapter_key"),
                            careers_url=raw["careers_url"],
                            ats_host=None,
                            status="FOUND",
                            detail=f"deep heal: {raw.get('detail', '')}",
                        )
                except Exception as exc:
                    with print_lock:
                        print(f"  deep heal error {name}: {exc}")

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
    mode_group = parser.add_mutually_exclusive_group()
    mode_group.add_argument(
        "--all",
        action="store_true",
        dest="heal_all",
        help="Scan all active companies (validates existing URLs, skips if still valid).",
    )
    mode_group.add_argument(
        "--broken",
        action="store_true",
        dest="heal_broken",
        help="Only scan companies with status=broken.",
    )
    mode_group.add_argument(
        "--force",
        action="store_true",
        dest="force_rediscover",
        help=(
            "Force full rediscovery for every company — skips the pre-check that "
            "would otherwise accept an existing active URL as still valid. "
            "Use this when active companies are pointing at wrong pages."
        ),
    )
    parser.add_argument(
        "--deep-heal",
        action="store_true",
        dest="deep_heal",
        help=(
            "Enable deep heal mode: use Playwright to identify ATS providers for companies "
            "that static discovery couldn't resolve (NOT_FOUND, FALLBACK, or custom_manual). "
            "Requires the deep search add-on — run deep_search/install_deep_search.bat first."
        ),
    )
    args = parser.parse_args()
    heal_registry(
        heal_all=args.heal_all,
        heal_broken=args.heal_broken,
        force_rediscover=args.force_rediscover,
        deep_heal=args.deep_heal,
    )
