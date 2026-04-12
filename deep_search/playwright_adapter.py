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
import os
import re
import sys
import threading
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional
from urllib.parse import urljoin, urlparse

from jobsearch.config.settings import settings
from jobsearch.scraper.ats_routing import fingerprint_ats
from jobsearch.scraper.jsonld_extractor import extract_jobposting_objects

logger = logging.getLogger("deep_search")
_THREAD_STATE = threading.local()

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
    context_kwargs = {
        "user_agent": _USER_AGENT,
        "viewport": {"width": 1280, "height": 900},
        "locale": "en-US",
        "extra_http_headers": {
            "Accept-Language": "en-US,en;q=0.9",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        },
    }
    context_kwargs["service_workers"] = "block"
    try:
        context = browser.new_context(**context_kwargs)
    except TypeError:
        context_kwargs.pop("service_workers", None)
        context = browser.new_context(**context_kwargs)
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
    for item in extract_jobposting_objects(html):
        if isinstance(item, dict):
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


def _set_last_run_evidence(evidence: Dict[str, Any]) -> None:
    _THREAD_STATE.last_run_evidence = evidence


def get_last_run_evidence() -> Dict[str, Any]:
    return dict(getattr(_THREAD_STATE, "last_run_evidence", {}) or {})


def _artifact_paths(company_name: str) -> Dict[str, str]:
    if not settings.scrape_debug_artifacts:
        return {"screenshot_path": "", "html_snapshot_path": ""}
    slug = re.sub(r"[^a-z0-9]+", "-", str(company_name or "company").lower()).strip("-") or "company"
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    directory = settings.debug_artifacts_dir / slug
    directory.mkdir(parents=True, exist_ok=True)
    return {
        "screenshot_path": str(directory / f"{stamp}.png"),
        "html_snapshot_path": str(directory / f"{stamp}.html"),
    }


def _collect_jobs_from_api_payload(data: Any, base_url: str, source_label: str, _depth: int = 0) -> List[Dict]:
    if _depth > 8:
        return []
    jobs: List[Dict] = []
    if isinstance(data, list):
        for item in data:
            jobs.extend(_collect_jobs_from_api_payload(item, base_url, source_label, _depth + 1))
        return jobs
    if not isinstance(data, dict):
        return jobs

    lower_keys = {str(key).lower() for key in data.keys()}
    
    # Standard field mapping
    has_title = bool(lower_keys & {"title", "name", "jobtitle", "job_title", "postingtitle"})
    has_url = bool(lower_keys & {"url", "applyurl", "externalpath", "hostedurl", "joburl", "absolute_url"})
    has_location = bool(lower_keys & {"location", "locationname", "locationstext", "joblocation"})
    
    # Eightfold compact mapping (t=title, l=location, u=url, jd=description)
    is_eightfold_item = "t" in lower_keys and "u" in lower_keys and len(lower_keys) < 15
    
    if (has_title and (has_url or has_location)) or is_eightfold_item:
        title = _clean(
            data.get("title")
            or data.get("name")
            or data.get("jobTitle")
            or data.get("job_title")
            or data.get("postingTitle")
            or data.get("t") # Eightfold
            or ""
        )
        url = _abs_url(
            base_url,
            str(
                data.get("url")
                or data.get("applyUrl")
                or data.get("hostedUrl")
                or data.get("jobUrl")
                or data.get("absolute_url")
                or data.get("externalPath")
                or data.get("u") # Eightfold
                or ""
            ),
        )
        location = (
            data.get("location") 
            or data.get("locationName") 
            or data.get("locationsText") 
            or data.get("jobLocation") 
            or data.get("l") # Eightfold
            or ""
        )
        if isinstance(location, dict):
            location = (
                location.get("name")
                or location.get("city")
                or ", ".join(part for part in [location.get("city"), location.get("region")] if part)
            )
        if isinstance(location, list):
            location = ", ".join(str(item) for item in location if item)
            
        description = (
            data.get("description") 
            or data.get("content") 
            or data.get("descriptionPlain") 
            or data.get("jd") # Eightfold
            or ""
        )
        if title and len(title) > 3 and (url or location):
            jobs.append(
                {
                    "title": title,
                    "location": _clean(str(location)),
                    "url": url or base_url,
                    "description": _clean(re.sub(r"<[^>]+>", " ", str(description))),
                    "posted_at": str(data.get("datePosted") or data.get("publicationDate") or ""),
                    "source": source_label,
                }
            )

    for value in data.values():
        if isinstance(value, (dict, list)):
            jobs.extend(_collect_jobs_from_api_payload(value, base_url, source_label, _depth + 1))
    return jobs


def _parse_jobs_from_api_response(
    url: str,
    content_type: str,
    body_text: str,
    *,
    base_url: str,
    source_label: str,
) -> List[Dict]:
    if not body_text:
        return []
    content_l = str(content_type or "").lower()
    url_l = str(url or "").lower()
    likely_endpoint = any(token in url_l for token in ("/jobs", "/job", "/postings", "/positions", "/openings", "/search"))
    likely_json = "json" in content_l or body_text.lstrip().startswith(("{", "["))
    if not (likely_endpoint or likely_json):
        return []
    try:
        payload = json.loads(body_text)
    except (json.JSONDecodeError, ValueError, TypeError):
        return []
    jobs = _collect_jobs_from_api_payload(payload, base_url, f"{source_label} (Intercepted API)")
    unique: List[Dict] = []
    seen: set[str] = set()
    for job in jobs:
        key = str(job.get("url") or "") + "|" + str(job.get("title") or "")
        if key in seen:
            continue
        seen.add(key)
        unique.append(job)
    return unique


def _is_probable_job_title_text(text: str) -> bool:
    text_c = _clean(text)
    if not text_c:
        return False
    text_l = text_c.lower()
    blacklist = (
        "search jobs",
        "view all jobs",
        "join our talent",
        "sign up",
        "learn more",
        "read more",
        "benefits",
        "culture",
        "about us",
        "our team",
        "login",
        "sign in",
        "create profile",
    )
    if any(token in text_l for token in blacklist):
        return False
    words = re.findall(r"[a-zA-Z][a-zA-Z+&/-]*", text_c)
    if len(words) < 2 or len(words) > 14:
        return False
    strong_words = {
        "engineer", "developer", "architect", "manager", "scientist", "analyst",
        "designer", "specialist", "recruiter", "consultant", "administrator",
        "director", "lead", "principal", "staff", "product", "software",
        "executive", "representative", "commercial", "account", "sales",
    }
    return any(word.lower() in strong_words for word in words)


def _looks_like_job_context(text: str) -> bool:
    text_c = _clean(text)
    if not text_c:
        return False
    text_l = text_c.lower()
    if "result" in text_l and re.search(r"\b\d+\s+results?\b", text_l):
        return False
    if re.search(r"\breq[-\s]?[a-z0-9]+\b", text_l, re.I):
        return True
    if re.search(r"\b(remote|united states|canada|germany|france|netherlands|australia)\b", text_l, re.I):
        return True
    if "|" in text_c and len(text_c.split("|")) >= 2:
        return True
    return False


def _extract_anchor_jobs_from_entries(
    entries: List[Dict[str, str]],
    *,
    base_url: str,
    source_label: str,
    seen: set,
) -> List[Dict]:
    jobs: List[Dict] = []
    for entry in entries:
        href = str(entry.get("href") or "")
        if not href:
            continue
        url = _abs_url(base_url, href)
        url_l = url.lower()
        if "search-results" in url_l or "search-jobs" in url_l:
            continue
        title = _clean(entry.get("text") or "")
        context = _clean(entry.get("context") or "")
        if not _is_probable_job_title_text(title):
            continue
        same_host = (urlparse(url).netloc or "").lower() == (urlparse(base_url).netloc or "").lower()
        has_jobish_url = bool(_JOB_URL_PATTERN.search(url_l))
        if not has_jobish_url and not (same_host and _looks_like_job_context(context)):
            continue
        if url in seen:
            continue
        seen.add(url)
        location = ""
        if re.search(r"\bremote\b", context, re.I):
            location = "Remote"
        else:
            city_state = re.search(r"\b([A-Z][a-z]+(?:\s+[A-Z][a-z]+)*,\s*[A-Z]{2})\b", context)
            if city_state:
                location = city_state.group(1)
        jobs.append(
            {
                "title": title,
                "location": location,
                "url": url,
                "description": "",
                "posted_at": "",
                "source": source_label,
            }
        )
    return jobs


def _extract_anchor_jobs(page, base_url: str, source_label: str, seen: set) -> List[Dict]:
    try:
        entries = page.eval_on_selector_all(
            "a[href]",
            """
            els => els.map(e => ({
              href: e.href || "",
              text: (e.innerText || e.textContent || "").trim(),
              context: ((e.closest('li, article, tr, div, section') || e.parentElement || e).innerText || "").trim()
            }))
            """,
        )
    except Exception:
        return []
    if not isinstance(entries, list):
        return []
    return _extract_anchor_jobs_from_entries(entries, base_url=base_url, source_label=source_label, seen=seen)


def _listing_links_from_page(page, base_url: str, limit: int = 5) -> List[str]:
    try:
        all_links = page.eval_on_selector_all(
            "a[href]",
            "els => els.map(e => e.href).filter(h => h && !h.startsWith('javascript'))",
        )
    except Exception:
        return []
    base_host = (urljoin(base_url, "/").split("/")[2] if "://" in base_url else "")
    listing_markers = (
        "search-results",
        "search-jobs",
        "/jobs/search",
        "/openings",
        "/positions",
        "/job-search",
    )
    links: List[str] = []
    for href in all_links:
        href_s = str(href or "")
        href_l = href_s.lower()
        if not any(marker in href_l for marker in listing_markers):
            continue
        if base_host and "://" in href_s and href_s.split("/")[2] != base_host:
            continue
        links.append(href_s)
    return list(dict.fromkeys(links))[: max(1, limit)]


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
      1. Intercepted API calls (XHR/fetch)
      2. JSON-LD extracted from the fully rendered HTML
      3. Next.js __NEXT_DATA__ embedded in the page
      4. DOM card extraction via common CSS selectors
      5. Follow job-like links from the rendered page to detail pages

    Returns List[Dict] with keys: title, location, url, description, posted_at, source
    """
    from playwright.sync_api import sync_playwright

    jobs: List[Dict] = []
    seen: set = set()
    evidence = {
        "network_response_urls": [],
        "network_events": [],
        "hidden_api_detected": False,
        "ats_family": fingerprint_ats(careers_url),
        "extraction_method": "",
        "screenshot_path": "",
        "html_snapshot_path": "",
        "status_code": 0,
        "timing": {},
    }
    artifacts = _artifact_paths(company_name)
    evidence.update(artifacts)
    start_t = time.perf_counter()

    def _capture_response(response) -> None:
        try:
            req = response.request
            if req.resource_type not in {"xhr", "fetch"}:
                return
            url = response.url
            content_type = response.headers.get("content-type", "")
            body_text = ""
            parsed_jobs: List[Dict] = []
            
            # Identify likely jobs endpoints based on URL patterns
            is_job_endpoint = any(token in url.lower() for token in ("/jobs", "/postings", "/positions", "/openings", "/search", "graphql", "query"))
            
            try:
                if "json" in content_type.lower() or body_text.lstrip().startswith(("{", "[")):
                    body_text = response.text()[:10000] # Increased for better parsing
                    parsed_jobs = _parse_jobs_from_api_response(
                        url,
                        content_type,
                        body_text,
                        base_url=careers_url,
                        source_label=source_label,
                    )
                else:
                    body_text = response.text()[:800]
            except Exception:
                body_text = ""

            record = {
                "url": url,
                "status": int(response.status),
                "content_type": content_type[:120],
                "body_preview": body_text[:500],
                "parsed_job_count": len(parsed_jobs),
                "is_job_endpoint": is_job_endpoint,
            }
            evidence["network_events"].append(record)
            if url not in evidence["network_response_urls"]:
                evidence["network_response_urls"].append(url)
            
            if parsed_jobs:
                evidence["hidden_api_detected"] = True
                detected_family = fingerprint_ats(careers_url, response_urls=[url])
                if detected_family != "unknown":
                    evidence["ats_family"] = detected_family
                
                for job in parsed_jobs:
                    key = str(job.get("url") or "") + "|" + str(job.get("title") or "")
                    if key in seen:
                        continue
                    seen.add(key)
                    jobs.append(job)
        except Exception:
            return

    with sync_playwright() as pw:
        browser, ctx = _new_context(pw)
        page = ctx.new_page()
        page.on("response", _capture_response)
        try:
            logger.info("deep_search navigating to %s (%s)", careers_url, company_name)
            try:
                resp = page.goto(careers_url, wait_until="networkidle", timeout=nav_timeout)
                if resp:
                    evidence["status_code"] = resp.status
            except Exception:
                # networkidle can time out on heavy SPAs — fall back to domcontentloaded
                try:
                    resp = page.goto(careers_url, wait_until="domcontentloaded", timeout=nav_timeout)
                    if resp:
                        evidence["status_code"] = resp.status
                except Exception as e:
                    logger.warning("deep_search nav failed company=%s err=%s", company_name, e)
                    evidence["failure_reason"] = str(e)
                    # If we have a response object even after timeout, use its status
                    # Note: Playwright sometimes throws but still has a partial response
                    browser.close()
                    _set_last_run_evidence(evidence)
                    return []
            
            evidence["timing"]["nav_ms"] = round((time.perf_counter() - start_t) * 1000, 1)
            page.wait_for_timeout(settle_ms)

            # Scroll to trigger lazy-loaded content
            for _ in range(scroll_count):
                page.keyboard.press("End")
                page.wait_for_timeout(700)

            html = page.content()
            if artifacts.get("html_snapshot_path"):
                try:
                    Path(artifacts["html_snapshot_path"]).write_text(html, encoding="utf-8")
                except Exception:
                    pass
            if artifacts.get("screenshot_path"):
                try:
                    page.screenshot(path=artifacts["screenshot_path"], full_page=True)
                except Exception:
                    pass

            if jobs:
                evidence["extraction_method"] = "intercepted_api"
                _set_last_run_evidence(evidence)
                logger.info("deep_search intercepted API found %d jobs (%s)", len(jobs), company_name)
                browser.close()
                return jobs

            # --- Strategy 1: JSON-LD ---
            jsonld_objs = _extract_jsonld(html)
            for obj in jsonld_objs:
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
                evidence["extraction_method"] = "jsonld"
                _set_last_run_evidence(evidence)
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
                evidence["extraction_method"] = "next_data"
                _set_last_run_evidence(evidence)
                logger.info("deep_search NextData found %d jobs (%s)", len(jobs), company_name)
                browser.close()
                return jobs

            # --- Strategy 3: DOM card extraction ---
            card_jobs = _extract_cards(page, careers_url, source_label, seen)
            jobs.extend(card_jobs)
            if jobs:
                evidence["extraction_method"] = "dom_render"
                _set_last_run_evidence(evidence)
                logger.info("deep_search DOM cards found %d jobs (%s)", len(jobs), company_name)
                browser.close()
                return jobs

            anchor_jobs = _extract_anchor_jobs(page, careers_url, source_label, seen)
            jobs.extend(anchor_jobs)
            if jobs:
                evidence["extraction_method"] = "dom_render"
                _set_last_run_evidence(evidence)
                logger.info("deep_search anchor extraction found %d jobs (%s)", len(jobs), company_name)
                browser.close()
                return jobs

            # --- Strategy 3b: Follow same-site listing pages (e.g. /search-results) ---
            listing_links = _listing_links_from_page(page, careers_url, limit=4)
            for listing_url in listing_links:
                try:
                    page.goto(listing_url, wait_until="networkidle", timeout=20_000)
                except Exception:
                    try:
                        page.goto(listing_url, wait_until="domcontentloaded", timeout=20_000)
                    except Exception:
                        continue
                page.wait_for_timeout(1_500)
                listing_html = page.content()

                for obj in _extract_jsonld(listing_html):
                    title = _clean(obj.get("title") or obj.get("name") or "")
                    url = _abs_url(listing_url, obj.get("url") or "")
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
                    evidence["extraction_method"] = "jsonld"
                    _set_last_run_evidence(evidence)
                    logger.info("deep_search listing page JSON-LD found %d jobs (%s)", len(jobs), company_name)
                    browser.close()
                    return jobs

                next_data = _extract_next_data(listing_html)
                if next_data:
                    for job in _jobs_from_next_data(next_data, listing_url):
                        if job["url"] not in seen:
                            seen.add(job["url"])
                            job["source"] = source_label
                            jobs.append(job)
                if jobs:
                    evidence["extraction_method"] = "next_data"
                    _set_last_run_evidence(evidence)
                    logger.info("deep_search listing page NextData found %d jobs (%s)", len(jobs), company_name)
                    browser.close()
                    return jobs

                card_jobs = _extract_cards(page, listing_url, source_label, seen)
                jobs.extend(card_jobs)
                if jobs:
                    evidence["extraction_method"] = "dom_render"
                    _set_last_run_evidence(evidence)
                    logger.info("deep_search listing page DOM cards found %d jobs (%s)", len(jobs), company_name)
                    browser.close()
                    return jobs

                anchor_jobs = _extract_anchor_jobs(page, listing_url, source_label, seen)
                jobs.extend(anchor_jobs)
                if jobs:
                    evidence["extraction_method"] = "dom_render"
                    _set_last_run_evidence(evidence)
                    logger.info("deep_search listing page anchor extraction found %d jobs (%s)", len(jobs), company_name)
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
            evidence["failure_reason"] = str(e)
        finally:
            try:
                browser.close()
            except Exception:
                pass

    if jobs:
        evidence["extraction_method"] = evidence["extraction_method"] or "dom_render"
    
    evidence["timing"]["total_ms"] = round((time.perf_counter() - start_t) * 1000, 1)
    _set_last_run_evidence(evidence)
    logger.info("deep_search complete for %s: jobs=%d status=%s failure=%s method=%s", 
                company_name, len(jobs), evidence.get("status_code"), 
                evidence.get("failure_reason", "none"), evidence.get("extraction_method", "none"))
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
        "workday",
        re.compile(r"wday/cxs/([^/]+)/([^/]+)/jobs", re.I),
        lambda m: f"https://{urlparse(careers_url).netloc}/wday/cxs/{m.group(1)}/{m.group(2)}/jobs",
        lambda m: f"{m.group(1)}/{m.group(2)}",
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
    (
        "workable",
        re.compile(r"apply\.workable\.com/api/v\d+/accounts/([^/?#]+)/jobs", re.I),
        lambda m: f"https://apply.workable.com/{m.group(1)}",
        lambda m: m.group(1),
    ),
    (
        "jobvite",
        re.compile(r"jobs\.jobvite\.com/([^/?#\s]+)/job/", re.I),
        lambda m: f"https://jobs.jobvite.com/{m.group(1)}/jobs",
        lambda m: m.group(1),
    ),
    (
        "eightfold",
        re.compile(r"([\w-]+\.eightfold\.ai)/api/v1/career_site/search", re.I),
        lambda m: f"https://{m.group(1)}/careers",
        lambda m: m.group(1),
    ),
    (
        "phenom",
        re.compile(r"/api/v\d+/jobs(?:\?|/)", re.I),
        lambda m: None,
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
    ("workable",       re.compile(r"apply\.workable\.com/([^/?#\s\"']+)", re.I),
     lambda m: f"https://apply.workable.com/{m.group(1)}", lambda m: m.group(1)),
    ("breezy",         re.compile(r"([\w-]+)\.breezy\.hr", re.I),
     lambda m: f"https://{m.group(1)}.breezy.hr", lambda m: m.group(1)),
    ("bamboohr",       re.compile(r"([\w-]+)\.bamboohr\.com", re.I),
     lambda m: f"https://{m.group(1)}.bamboohr.com/jobs/", lambda m: m.group(1)),
    ("jobvite",        re.compile(r"jobs\.jobvite\.com/([^/?#\s\"']+)", re.I),
     lambda m: f"https://jobs.jobvite.com/{m.group(1)}/jobs", lambda m: m.group(1)),
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
    network_evidence: List[Dict] = []
    evidence_lock = threading.Lock()

    def _on_request(request) -> None:
        nonlocal result
        if result:
            return
        hit = _match_ats_url(request.url)
        if hit:
            with result_lock:
                if not result:
                    result = {**hit, "detail": f"network request → {hit['adapter']} ({request.url[:100]})"}

    def _on_response(response) -> None:
        """Capture response metadata for URLs matching ATS patterns."""
        url = response.url
        hit = _match_ats_url(url)
        if not hit:
            return
        status = response.status
        content_type = ""
        body_excerpt = ""
        try:
            content_type = response.headers.get("content-type", "")
            if "json" in content_type or "javascript" in content_type:
                try:
                    body_raw = response.body()
                    body_excerpt = body_raw[:500].decode("utf-8", errors="replace") if isinstance(body_raw, bytes) else str(body_raw)[:500]
                except Exception:
                    pass
        except Exception:
            pass
        record = {
            "url": url[:200],
            "status": status,
            "content_type": content_type[:80],
            "body_excerpt": body_excerpt,
            "adapter": hit.get("adapter"),
        }
        with evidence_lock:
            network_evidence.append(record)
        logger.debug("deep_heal response captured: %s %s %s", status, content_type[:40], url[:80])

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
        page.on("response", _on_response)

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
        result["_network_evidence"] = network_evidence
    else:
        logger.info("deep_heal found nothing for %s", company_name)
        if network_evidence:
            logger.debug("deep_heal %s: %d ATS response(s) captured but no match", company_name, len(network_evidence))

    return result
