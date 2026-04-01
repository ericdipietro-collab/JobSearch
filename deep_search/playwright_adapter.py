"""
deep_search/playwright_adapter.py

Playwright-based scrapers for JavaScript-heavy careers pages.
Used as a fallback when static HTTP scraping returns no results.

Requires:
    pip install playwright
    playwright install chromium

This module is intentionally self-contained — it returns plain dicts so that
job_search_v6.py can convert them to Job objects via make_job().

Each scrape function returns List[Dict] with keys:
    title, location, url, description, posted_at, source
"""

from __future__ import annotations

import json
import logging
import re
import threading
from typing import Any, Dict, List, Optional
from urllib.parse import urljoin

logger = logging.getLogger("deep_search")

# ---------------------------------------------------------------------------
# Availability
# ---------------------------------------------------------------------------

def is_available() -> bool:
    """Return True if playwright is installed (browsers may still need installing)."""
    try:
        import playwright  # noqa: F401
        return True
    except ImportError:
        return False


def check_browser_ready() -> bool:
    """Return True if playwright AND the chromium browser are ready to use."""
    if not is_available():
        return False
    try:
        from playwright.sync_api import sync_playwright
        with sync_playwright() as pw:
            browser = pw.chromium.launch(headless=True)
            browser.close()
        return True
    except Exception:
        return False


# ---------------------------------------------------------------------------
# Browser setup
# ---------------------------------------------------------------------------

_LAUNCH_ARGS = [
    "--no-sandbox",
    "--disable-blink-features=AutomationControlled",
    "--disable-dev-shm-usage",
    "--disable-gpu",
]

_USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/122.0.0.0 Safari/537.36"
)


def _new_context(playwright_instance):
    """Launch a browser and return (browser, context)."""
    browser = playwright_instance.chromium.launch(headless=True, args=_LAUNCH_ARGS)
    context = browser.new_context(
        user_agent=_USER_AGENT,
        viewport={"width": 1280, "height": 900},
        locale="en-US",
        extra_http_headers={
            "Accept-Language": "en-US,en;q=0.9",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        },
    )
    # Hide automation fingerprint
    context.add_init_script(
        "Object.defineProperty(navigator, 'webdriver', {get: () => undefined})"
    )
    return browser, context


# ---------------------------------------------------------------------------
# Extraction helpers
# ---------------------------------------------------------------------------

def _clean(text: str) -> str:
    return re.sub(r"\s+", " ", text or "").strip()


def _abs_url(base: str, href: str) -> str:
    if not href:
        return base
    if href.startswith("http"):
        return href
    return urljoin(base, href)


def _extract_jsonld(html: str) -> List[Dict]:
    """Extract JobPosting objects from JSON-LD script tags."""
    results = []
    for m in re.finditer(
        r'<script[^>]+type=["\']application/ld\+json["\'][^>]*>(.*?)</script>',
        html,
        re.DOTALL | re.IGNORECASE,
    ):
        try:
            obj = json.loads(m.group(1))
        except (json.JSONDecodeError, ValueError):
            continue
        if isinstance(obj, list):
            items = obj
        elif isinstance(obj, dict) and obj.get("@graph"):
            items = obj["@graph"]
        else:
            items = [obj]
        for item in items:
            t = item.get("@type") or ""
            if isinstance(t, list):
                t = " ".join(t)
            if "JobPosting" in t:
                results.append(item)
    return results


def _jsonld_location(obj: Dict) -> str:
    loc = obj.get("jobLocation") or {}
    if isinstance(loc, list):
        loc = loc[0] if loc else {}
    addr = loc.get("address") or {}
    if isinstance(addr, str):
        return addr
    parts = [
        addr.get("addressLocality", ""),
        addr.get("addressRegion", ""),
        addr.get("addressCountry", ""),
    ]
    return ", ".join(p for p in parts if p).strip(", ")


def _description_text(obj: Dict) -> str:
    raw = obj.get("description") or obj.get("descriptionHtml") or ""
    return _clean(re.sub(r"<[^>]+>", " ", raw))


def _extract_next_data(html: str) -> Optional[Dict]:
    """Extract Next.js __NEXT_DATA__ JSON embedded in the page."""
    m = re.search(
        r'<script[^>]+id=["\']__NEXT_DATA__["\'][^>]*>([^<]+)</script>',
        html,
        re.IGNORECASE,
    )
    if m:
        try:
            return json.loads(m.group(1))
        except (json.JSONDecodeError, ValueError):
            pass
    return None


def _jobs_from_next_data(data: Any, base_url: str, _depth: int = 0) -> List[Dict]:
    """Recursively search Next.js state for job-like objects."""
    if _depth > 8:
        return []
    jobs: List[Dict] = []
    if isinstance(data, list):
        for item in data:
            jobs.extend(_jobs_from_next_data(item, base_url, _depth + 1))
    elif isinstance(data, dict):
        keys_lower = {k.lower() for k in data}
        has_title = bool(keys_lower & {"title", "jobtitle", "job_title", "name"})
        has_url = bool(keys_lower & {"url", "applyurl", "externalpath", "link", "hostedurl"})
        has_location = "location" in keys_lower or "locationname" in keys_lower
        if has_title and (has_url or has_location):
            title = _clean(str(
                data.get("title") or data.get("jobTitle") or
                data.get("job_title") or data.get("name") or ""
            ))
            url = _abs_url(base_url, str(
                data.get("url") or data.get("applyUrl") or data.get("hostedUrl") or
                data.get("externalPath") or data.get("link") or base_url
            ))
            loc = data.get("location") or data.get("locationName") or data.get("office") or ""
            if isinstance(loc, dict):
                loc = loc.get("name") or loc.get("city") or ""
            if title and len(title) > 3:
                jobs.append({
                    "title": title,
                    "location": _clean(str(loc)),
                    "url": url,
                    "description": _clean(re.sub(r"<[^>]+>", " ", str(data.get("description") or ""))),
                    "posted_at": str(data.get("datePosted") or data.get("publicationDate") or ""),
                })
        else:
            for v in data.values():
                jobs.extend(_jobs_from_next_data(v, base_url, _depth + 1))
    return jobs


# CSS selectors tried in order to find job listing cards in the rendered DOM
_CARD_SELECTORS = [
    "[data-automation*='job']",
    "[data-qa*='job']",
    "[data-testid*='job']",
    "[class*='job-card']",
    "[class*='JobCard']",
    "[class*='job-item']",
    "[class*='JobItem']",
    "[class*='job-listing']",
    "[class*='JobListing']",
    "[class*='job-result']",
    "[class*='JobResult']",
    "[class*='job-tile']",
    "[class*='JobTile']",
    "[class*='position-item']",
    "[class*='position-card']",
    "[class*='opening-item']",
    "[class*='opportunity-card']",
    ".posting",
    "li.job",
    "tr.job",
]

_TITLE_SELECTORS = [
    "[class*='title']", "[class*='Title']", "[class*='name']",
    "h1", "h2", "h3", "h4", "strong", "a",
]

_LOCATION_SELECTORS = [
    "[class*='location']", "[class*='Location']",
    "[aria-label*='location']", "[class*='city']", "[class*='region']",
    "span", "p",
]


def _extract_cards(page, base_url: str, source_label: str, seen: set) -> List[Dict]:
    """Try common CSS selectors to find job cards in a rendered Playwright page."""
    jobs: List[Dict] = []
    for selector in _CARD_SELECTORS:
        try:
            cards = page.query_selector_all(selector)
        except Exception:
            continue
        if not cards:
            continue
        for card in cards[:150]:
            try:
                link_el = card.query_selector("a[href]")
                href = link_el.get_attribute("href") if link_el else ""
                url = _abs_url(base_url, href) if href else base_url

                title = ""
                for tsel in _TITLE_SELECTORS:
                    el = card.query_selector(tsel)
                    if el:
                        t = _clean(el.inner_text())
                        if t and 3 < len(t) < 200:
                            title = t
                            break

                if not title and link_el:
                    title = _clean(link_el.inner_text())

                location = ""
                for lsel in _LOCATION_SELECTORS:
                    el = card.query_selector(lsel)
                    if el:
                        t = _clean(el.inner_text())
                        if t and t != title and len(t) < 120:
                            location = t
                            break

                if title and url not in seen:
                    seen.add(url)
                    jobs.append({
                        "title": title,
                        "location": location,
                        "url": url,
                        "description": "",
                        "posted_at": "",
                        "source": source_label,
                    })
            except Exception:
                continue
        if jobs:
            break
    return jobs


def _job_from_detail_html(html: str, url: str, source_label: str) -> Optional[Dict]:
    """Extract a single job from a rendered detail page."""
    # Prefer JSON-LD
    for obj in _extract_jsonld(html):
        title = _clean(obj.get("title") or obj.get("name") or "")
        if title:
            return {
                "title": title,
                "location": _jsonld_location(obj),
                "url": url,
                "description": _description_text(obj),
                "posted_at": obj.get("datePosted") or "",
                "source": source_label,
            }
    # Fall back to h1
    m = re.search(r"<h1[^>]*>(.*?)</h1>", html, re.IGNORECASE | re.DOTALL)
    title = _clean(re.sub(r"<[^>]+>", " ", m.group(1))) if m else ""
    loc_m = re.search(r'(?:location|city|office)[^>]*>\s*([^<]{3,80})<', html, re.IGNORECASE)
    location = _clean(loc_m.group(1)) if loc_m else ""
    description = _clean(re.sub(r"<[^>]+>", " ", html))[:3000]
    if title:
        return {
            "title": title,
            "location": location,
            "url": url,
            "description": description,
            "posted_at": "",
            "source": source_label,
        }
    return None


# Pattern for URLs that look like job detail pages
_JOB_URL_PATTERN = re.compile(
    r"/(job|jobs|opening|openings|position|positions|career|careers|req|requisition|posting)/",
    re.IGNORECASE,
)


# ---------------------------------------------------------------------------
# Generic scraper (works for most JS careers pages)
# ---------------------------------------------------------------------------

def scrape_jobs_generic(
    careers_url: str,
    company_name: str,
    *,
    source_label: str = "Deep Search",
    nav_timeout: int = 35_000,
    settle_ms: int = 3_000,
    max_detail_pages: int = 40,
    scroll_count: int = 3,
) -> List[Dict]:
    """
    Generic deep scraper for any JS-heavy careers page.

    Tries four strategies in order:
      1. JSON-LD extracted from the fully rendered HTML
      2. Next.js __NEXT_DATA__ embedded in the page
      3. DOM card extraction via common CSS selectors
      4. Follow job-like links from the rendered page to detail pages

    Returns List[Dict] with keys: title, location, url, description, posted_at, source
    """
    from playwright.sync_api import sync_playwright

    jobs: List[Dict] = []
    seen: set = set()

    with sync_playwright() as pw:
        browser, ctx = _new_context(pw)
        page = ctx.new_page()
        try:
            logger.info("deep_search navigating to %s (%s)", careers_url, company_name)
            try:
                page.goto(careers_url, wait_until="networkidle", timeout=nav_timeout)
            except Exception:
                # networkidle can time out on heavy SPAs — fall back to domcontentloaded
                try:
                    page.goto(careers_url, wait_until="domcontentloaded", timeout=nav_timeout)
                except Exception as e:
                    logger.warning("deep_search nav failed company=%s err=%s", company_name, e)
                    browser.close()
                    return []
            page.wait_for_timeout(settle_ms)

            # Scroll to trigger lazy-loaded content
            for _ in range(scroll_count):
                page.keyboard.press("End")
                page.wait_for_timeout(700)

            html = page.content()

            # --- Strategy 1: JSON-LD ---
            for obj in _extract_jsonld(html):
                title = _clean(obj.get("title") or obj.get("name") or "")
                url = _abs_url(careers_url, obj.get("url") or "")
                if not title or url in seen:
                    continue
                seen.add(url)
                jobs.append({
                    "title": title,
                    "location": _jsonld_location(obj),
                    "url": url,
                    "description": _description_text(obj),
                    "posted_at": obj.get("datePosted") or "",
                    "source": source_label,
                })
            if jobs:
                logger.info("deep_search JSON-LD found %d jobs (%s)", len(jobs), company_name)
                browser.close()
                return jobs

            # --- Strategy 2: __NEXT_DATA__ ---
            next_data = _extract_next_data(html)
            if next_data:
                for job in _jobs_from_next_data(next_data, careers_url):
                    if job["url"] not in seen:
                        seen.add(job["url"])
                        job["source"] = source_label
                        jobs.append(job)
            if jobs:
                logger.info("deep_search NextData found %d jobs (%s)", len(jobs), company_name)
                browser.close()
                return jobs

            # --- Strategy 3: DOM card extraction ---
            card_jobs = _extract_cards(page, careers_url, source_label, seen)
            jobs.extend(card_jobs)
            if jobs:
                logger.info("deep_search DOM cards found %d jobs (%s)", len(jobs), company_name)
                browser.close()
                return jobs

            # --- Strategy 4: Follow job-detail links ---
            try:
                all_links = page.eval_on_selector_all(
                    "a[href]",
                    "els => els.map(e => e.href).filter(h => h && !h.startsWith('javascript'))",
                )
            except Exception:
                all_links = []

            job_links = list(dict.fromkeys(
                lnk for lnk in all_links
                if _JOB_URL_PATTERN.search(lnk) and lnk not in seen
            ))[:max_detail_pages]

            logger.info(
                "deep_search following %d job links (%s)", len(job_links), company_name
            )
            for href in job_links:
                if href in seen:
                    continue
                seen.add(href)
                try:
                    page.goto(href, wait_until="networkidle", timeout=20_000)
                    page.wait_for_timeout(1_500)
                    job = _job_from_detail_html(page.content(), href, source_label)
                    if job and job["title"]:
                        jobs.append(job)
                except Exception as e:
                    logger.debug("deep_search detail failed url=%s err=%s", href, e)

        except Exception as e:
            logger.error("deep_search scrape failed company=%s err=%s", company_name, e)
        finally:
            try:
                browser.close()
            except Exception:
                pass

    logger.info("deep_search total %d jobs (%s)", len(jobs), company_name)
    return jobs


# ---------------------------------------------------------------------------
# Company-specific scrapers
# ---------------------------------------------------------------------------

def scrape_jobs_blackrock(careers_url: str, company_name: str = "BlackRock") -> List[Dict]:
    """
    BlackRock uses a Phenom People / custom Angular app.
    After rendering, look for Phenom-specific card selectors, then fall back to generic.
    """
    from playwright.sync_api import sync_playwright

    jobs: List[Dict] = []
    seen: set = set()

    phenom_card_selectors = [
        "[class*='job-tile']",
        "[class*='phenom']",
        "ppc-job-card",
        "[data-ph-at-id*='job']",
        "[class*='search-result']",
        "[class*='SearchResult']",
        ".job-list .card",
        "[class*='job-list-item']",
    ]

    with sync_playwright() as pw:
        browser, ctx = _new_context(pw)
        page = ctx.new_page()
        try:
            page.goto(careers_url, wait_until="networkidle", timeout=40_000)
            page.wait_for_timeout(4_000)
            # Scroll to trigger lazy loading
            for _ in range(6):
                page.keyboard.press("End")
                page.wait_for_timeout(600)

            # Try Phenom-specific selectors
            for selector in phenom_card_selectors:
                try:
                    cards = page.query_selector_all(selector)
                    if not cards:
                        continue
                    for card in cards[:150]:
                        try:
                            link_el = card.query_selector("a[href]")
                            href = link_el.get_attribute("href") if link_el else ""
                            url = _abs_url(careers_url, href) if href else careers_url

                            title = ""
                            for tsel in ["h3", "h2", "[class*='title']", "[class*='name']", "a"]:
                                el = card.query_selector(tsel)
                                if el:
                                    t = _clean(el.inner_text())
                                    if t and 3 < len(t) < 200:
                                        title = t
                                        break

                            loc = ""
                            for lsel in ["[class*='location']", "[class*='city']", "span", "p"]:
                                el = card.query_selector(lsel)
                                if el:
                                    t = _clean(el.inner_text())
                                    if t and t != title and len(t) < 120:
                                        loc = t
                                        break

                            if title and url not in seen:
                                seen.add(url)
                                jobs.append({
                                    "title": title,
                                    "location": loc,
                                    "url": url,
                                    "description": "",
                                    "posted_at": "",
                                    "source": "BlackRock Careers (Deep Search)",
                                })
                        except Exception:
                            continue
                    if jobs:
                        break
                except Exception:
                    continue

            # Fall back to JSON-LD from rendered page
            if not jobs:
                html = page.content()
                for obj in _extract_jsonld(html):
                    title = _clean(obj.get("title") or "")
                    url = _abs_url(careers_url, obj.get("url") or "")
                    if title and url not in seen:
                        seen.add(url)
                        jobs.append({
                            "title": title,
                            "location": _jsonld_location(obj),
                            "url": url,
                            "description": _description_text(obj),
                            "posted_at": obj.get("datePosted") or "",
                            "source": "BlackRock Careers (Deep Search)",
                        })

        except Exception as e:
            logger.warning("blackrock deep scrape error err=%s", e)
        finally:
            try:
                browser.close()
            except Exception:
                pass

    # Final fallback: generic scraper
    if not jobs:
        logger.info("blackrock Phenom selectors found nothing — falling back to generic")
        jobs = scrape_jobs_generic(
            careers_url, company_name,
            source_label="BlackRock Careers (Deep Search)",
            settle_ms=4_000,
        )

    logger.info("blackrock deep search found %d jobs", len(jobs))
    return jobs


def scrape_jobs_schwab(careers_url: str, company_name: str = "Charles Schwab") -> List[Dict]:
    """Charles Schwab careers deep scraper."""
    return scrape_jobs_generic(
        careers_url, company_name,
        source_label="Schwab Careers (Deep Search)",
        settle_ms=4_000,
        max_detail_pages=50,
        scroll_count=4,
    )


def scrape_jobs_spglobal(careers_url: str, company_name: str = "S&P Global") -> List[Dict]:
    """S&P Global careers deep scraper."""
    return scrape_jobs_generic(
        careers_url, company_name,
        source_label="S&P Global Careers (Deep Search)",
        settle_ms=3_000,
        max_detail_pages=60,
        scroll_count=4,
    )


# ---------------------------------------------------------------------------
# ATS Healer integration — deep_heal_company()
# ---------------------------------------------------------------------------

# Network request URL patterns that uniquely fingerprint each ATS backend.
# Each entry: (adapter_name, compiled_regex, url_fn(match), key_fn(match))
_ATS_NET_PATTERNS = [
    (
        "greenhouse",
        re.compile(r"boards-api\.greenhouse\.io/v\d+/boards/([^/?#]+)/jobs", re.I),
        lambda m: f"https://job-boards.greenhouse.io/{m.group(1)}",
        lambda m: m.group(1),
    ),
    (
        "lever",
        re.compile(r"api\.lever\.co/v\d+/postings/([^/?#]+)", re.I),
        lambda m: f"https://jobs.lever.co/{m.group(1)}",
        lambda m: m.group(1),
    ),
    (
        "ashby",
        re.compile(r"api\.ashbyhq\.com/posting-api/job-board/([^/?#]+)", re.I),
        lambda m: f"https://jobs.ashbyhq.com/{m.group(1)}",
        lambda m: m.group(1),
    ),
    (
        "workday",
        re.compile(r"([\w-]+\.wd\d+\.myworkdayjobs\.com)/wday/cxs/", re.I),
        lambda m: f"https://{m.group(1)}",
        lambda m: m.group(1),
    ),
    (
        "smartrecruiters",
        re.compile(r"api\.smartrecruiters\.com/v\d+/companies/([^/?#]+)/postings", re.I),
        lambda m: f"https://careers.smartrecruiters.com/{m.group(1)}",
        lambda m: m.group(1),
    ),
    (
        "icims",
        re.compile(r"([\w-]+\.icims\.com)/jobs/\d+/", re.I),
        lambda m: f"https://{m.group(1)}/jobs/search",
        lambda m: None,
    ),
]

# ATS URL patterns to match against frame URLs and rendered page source.
# Greenhouse embed iframes use ?for=SLUG — must be checked BEFORE the generic
# path pattern or it would extract "embed" as the slug.
_ATS_URL_PATTERNS = [
    ("greenhouse",    re.compile(r"(?:job-boards|boards)\.greenhouse\.io/embed/job_board\?for=([^&\s\"'#]+)", re.I),
     lambda m: f"https://job-boards.greenhouse.io/{m.group(1)}", lambda m: m.group(1)),
    ("greenhouse",    re.compile(r"(?:job-boards|boards)\.greenhouse\.io/(?!embed/)([^/?#\s\"']+)", re.I),
     lambda m: f"https://job-boards.greenhouse.io/{m.group(1)}", lambda m: m.group(1)),
    ("lever",         re.compile(r"jobs\.lever\.co/([^/?#\s\"']+)", re.I),
     lambda m: f"https://jobs.lever.co/{m.group(1)}", lambda m: m.group(1)),
    ("ashby",         re.compile(r"jobs\.ashbyhq\.com/([^/?#\s\"']+)", re.I),
     lambda m: f"https://jobs.ashbyhq.com/{m.group(1)}", lambda m: m.group(1)),
    ("workday",       re.compile(r"([\w-]+\.wd\d+\.myworkdayjobs\.com)", re.I),
     lambda m: f"https://{m.group(1)}", lambda m: m.group(1)),
    ("smartrecruiters", re.compile(r"careers\.smartrecruiters\.com/([^/?#\s\"']+)", re.I),
     lambda m: f"https://careers.smartrecruiters.com/{m.group(1)}", lambda m: m.group(1)),
]

_CAREER_NAV_KEYWORDS = frozenset(
    ["careers", "jobs", "join us", "join-us", "work with us", "we're hiring", "openings", "hiring"]
)


def _match_ats_url(url: str) -> Optional[Dict]:
    """Return ATS hit dict if *url* matches any known ATS network pattern."""
    for adapter, pattern, url_fn, key_fn in _ATS_NET_PATTERNS:
        m = pattern.search(url)
        if m:
            try:
                return {
                    "adapter": adapter,
                    "adapter_key": key_fn(m),
                    "careers_url": url_fn(m),
                }
            except Exception:
                continue
    return None


def _match_ats_in_text(text: str) -> Optional[Dict]:
    """Return ATS hit dict if *text* (HTML/JS source) contains a recognisable ATS URL."""
    for adapter, pattern, url_fn, key_fn in _ATS_URL_PATTERNS:
        m = pattern.search(text)
        if m:
            try:
                return {
                    "adapter": adapter,
                    "adapter_key": key_fn(m),
                    "careers_url": url_fn(m),
                }
            except Exception:
                continue
    return None


def deep_heal_company(
    careers_url: str,
    company_name: str,
    comp_domain: str = "",
    *,
    nav_timeout: int = 25_000,
    settle_ms: int = 3_000,
) -> Optional[Dict]:
    """
    Deep-scan a careers page with Playwright to identify the ATS provider.

    Targets companies where static HTML scraping failed to detect the ATS —
    typically because the ATS board is loaded via JavaScript (dynamic iframe
    src, AJAX-mounted React widget, etc.).

    Three strategies run in order for each probe URL:

      1. Network interception — listens to every outgoing HTTP request during
         page load and matches against known ATS API endpoint patterns.  This
         catches Greenhouse, Lever, Ashby, Workday, SmartRecruiters, iCIMS
         with certainty because the API slug appears directly in the URL.

      2. Frame inspection — after the page settles, iterates all loaded frames
         (including those whose src= was set via JavaScript) and checks each
         frame URL against ATS host patterns.

      3. Rendered source scan — searches the fully rendered HTML for ATS URLs
         embedded in script tags, data attributes, or inline JSON.

    If a careers URL is not known, probes the company homepage and follows any
    rendered navigation link that looks like a careers page.

    Returns a dict with keys: adapter, adapter_key, careers_url, detail
    or None if no ATS could be identified.
    """
    from playwright.sync_api import sync_playwright

    result: Optional[Dict] = None
    result_lock = threading.Lock()

    def _on_request(request) -> None:
        nonlocal result
        if result:
            return
        hit = _match_ats_url(request.url)
        if hit:
            with result_lock:
                if not result:
                    result = {**hit, "detail": f"network request → {hit['adapter']} ({request.url[:100]})"}

    # Build probe list: known careers URL first, then homepage variants
    probe_urls: List[str] = []
    if careers_url and careers_url.startswith("http"):
        probe_urls.append(careers_url)
    if comp_domain:
        for base in (
            f"https://careers.{comp_domain}",
            f"https://www.{comp_domain}",
            f"https://{comp_domain}",
        ):
            if base not in probe_urls:
                probe_urls.append(base)

    if not probe_urls:
        logger.warning("deep_heal no probe URLs for %s", company_name)
        return None

    with sync_playwright() as pw:
        browser, ctx = _new_context(pw)
        page = ctx.new_page()
        page.on("request", _on_request)

        try:
            i = 0
            while i < len(probe_urls):
                url = probe_urls[i]
                i += 1

                if result:
                    break

                logger.info("deep_heal probing %s (%s)", url, company_name)
                try:
                    page.goto(url, wait_until="networkidle", timeout=nav_timeout)
                except Exception:
                    try:
                        page.goto(url, wait_until="domcontentloaded", timeout=nav_timeout)
                    except Exception as e:
                        logger.debug("deep_heal nav failed url=%s err=%s", url, e)
                        continue
                page.wait_for_timeout(settle_ms)

                if result:
                    break

                # Strategy 2: frame inspection (JS-constructed iframe src values)
                for frame in page.frames:
                    frame_url = frame.url
                    if not frame_url or frame_url in ("about:blank", url, ""):
                        continue
                    hit = _match_ats_url(frame_url) or _match_ats_in_text(frame_url)
                    if hit:
                        result = {**hit, "detail": f"JS iframe → {hit['adapter']} ({frame_url[:100]})"}
                        break

                if result:
                    break

                # Strategy 3: rendered source scan
                try:
                    html = page.content()
                    hit = _match_ats_in_text(html)
                    if hit:
                        result = {**hit, "detail": f"rendered source → {hit['adapter']}"}
                except Exception:
                    html = ""

                if result:
                    break

                # If this was a homepage probe and no ATS found yet, look for a
                # rendered career nav link to add as the next probe URL.
                parsed_host = url.split("//")[-1].split("/")[0]
                if comp_domain and comp_domain in parsed_host and "/careers" not in url and "/jobs" not in url:
                    try:
                        links = page.eval_on_selector_all(
                            "a[href]",
                            "els => els.map(e => ({href: e.href, text: (e.innerText||'').trim().toLowerCase()}))",
                        )
                        for lnk in links:
                            txt = lnk.get("text", "")
                            href = lnk.get("href", "")
                            if (
                                any(kw in txt for kw in _CAREER_NAV_KEYWORDS)
                                and href.startswith("http")
                                and href not in probe_urls
                            ):
                                logger.info("deep_heal found career nav link: %s", href)
                                probe_urls.insert(i, href)
                                break
                    except Exception:
                        pass

        except Exception as e:
            logger.error("deep_heal session error company=%s err=%s", company_name, e)
        finally:
            try:
                browser.close()
            except Exception:
                pass

    if result:
        logger.info(
            "deep_heal identified %s for %s: %s",
            result.get("adapter"), company_name, result.get("detail"),
        )
    else:
        logger.info("deep_heal found nothing for %s", company_name)

    return result
