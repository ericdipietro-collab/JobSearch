"""Healer Service: Modular ATS discovery and URL repair."""

from __future__ import annotations
import re
import time
import random
import logging
import requests
from datetime import datetime, timezone
from pathlib import Path
from typing import List, Dict, Any, Optional, Tuple
from dataclasses import dataclass
from urllib.parse import urljoin, urlparse, parse_qs
from bs4 import BeautifulSoup
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from concurrent.futures import ThreadPoolExecutor, as_completed, TimeoutError
from jobsearch.config.settings import get_headers, get_shared_session, settings
from jobsearch import ats_db as db

logger = logging.getLogger(__name__)

@dataclass
class DiscoveryResult:
    adapter: Optional[str]
    adapter_key: Optional[str]
    careers_url: Optional[str]
    status: str # VALID, FOUND, FALLBACK, NOT_FOUND, BLOCKED
    detail: str

class ATSHealer:
    CACHE_TTL_S = 600.0
    MAX_HTML_PARSE_CHARS = 500_000
    DIRECT_PROBE_TIMEOUTS = {
        "ashby": 5,
        "lever": 5,
        "greenhouse": 4,
        "smartrecruiters": 5,
        "workday": 5,
    }

    # Common patterns for direct ATS host detection
    ATS_PATTERNS = {
        "greenhouse": [
            re.compile(r"(?:job-boards|boards)\.greenhouse\.io/([\w.-]+)", re.I),
            re.compile(r"board=([\w.-]+)", re.I),
            re.compile(r"for=([\w.-]+)", re.I),
            re.compile(r"js\?for=([\w.-]+)", re.I), # Catch Greenhouse widget embeds
            re.compile(r"gh_src=([\w.-]+)", re.I),
        ],
        "lever": [
            re.compile(r"jobs\.lever\.co/([\w.-]+)", re.I),
            re.compile(r"postings/([\w.-]+)", re.I),
        ],
        "ashby": [
            re.compile(r"jobs\.ashbyhq\.com/([\w.-]+)", re.I),
        ],
        "workday": [
            re.compile(r"([\w.-]+\.myworkdayjobs\.com/[^\"'&\s<>\\{}]+)", re.I),
        ],
        "rippling": [
            re.compile(r"rippling\.com/([\w.-]+)", re.I),
        ],
        "smartrecruiters": [
            re.compile(r"smartrecruiters\.com/([\w.-]+)", re.I),
        ],
    }

    BLACKLISTED_SUBDOMAINS = ["support.", "help.", "docs.", "developers.", "api.", "community.", "hc."]

    # ATS domains that require an account / API key or use an unsupported vendor.
    # Companies whose careers_url resolves to one of these are routed to manual_only
    # immediately rather than burning the waterfall budget.
    KNOWN_UNSUPPORTED_ATS_MARKERS = [
        "eightfold.ai",
        "icims.com",
        "bamboohr.com",
        "taleo.net",
        "successfactors.",
        "myworkday.com/wday/authgwy",  # auth-gated Workday
        "workday.com/en-us/applications",
    ]

    # Generic ATS homepages that are not company-specific job boards.
    # A careers_url matching one of these is stale and should trigger re-discovery.
    GENERIC_ATS_HOMEPAGES = {
        "www.greenhouse.com/careers",
        "www.greenhouse.io",
        "greenhouse.io",
        "lever.co",
        "jobs.ashbyhq.com",
        "boards.greenhouse.io",
        "job-boards.greenhouse.io",
        "app.ashbyhq.com",
    }

    def _is_blacklisted(self, url: str) -> bool:
        return any(b in url.lower() for b in self.BLACKLISTED_SUBDOMAINS)

    def _url_has_ats_signal(self, url: str) -> bool:
        """Return True if the URL contains a path that suggests an ATS-specific endpoint
        rather than just a company homepage or generic marketing page."""
        url_l = str(url or "").lower()
        ats_indicators = [
            "greenhouse", "lever", "ashby", "workday", "myworkdayjobs",
            "rippling", "smartrecruiters", "icims", "eightfold",
            "/jobs", "/careers/", "/open-positions", "/job-search",
        ]
        return any(ind in url_l for ind in ats_indicators)

    def _url_is_generic_ats_homepage(self, url: str) -> bool:
        """Return True if careers_url points to the generic ATS vendor homepage (not a company board)."""
        try:
            parsed = urlparse(url)
            canonical = f"{parsed.netloc}{parsed.path}".rstrip("/").lower()
            if canonical in {h.lower() for h in self.GENERIC_ATS_HOMEPAGES}:
                return True
        except Exception:
            pass
        return False

    def _url_is_unsupported_ats(self, url: str) -> bool:
        """Return True if the URL belongs to an ATS vendor we cannot scrape."""
        url_l = str(url or "").lower()
        return any(marker in url_l for marker in self.KNOWN_UNSUPPORTED_ATS_MARKERS)

    # Enhanced detection markers for embedded boards
    EMBED_MARKERS = [
        ("greenhouse", "greenhouse.io"),
        ("greenhouse", "grnh.js"),
        ("lever", "lever.co"),
        ("ashby", "ashbyhq.com"),
        ("workday", "myworkdayjobs.com"),
        ("rippling", "rippling.com"),
        ("smartrecruiters", "smartrecruiters.com"),
        ("icims", "icims.com"),
        ("bamboohr", "bamboohr.com"),
        ("generic", "happydance.com"),
        ("generic", "onetrust.com"),
    ]
    GENERIC_POSITIVE_URL_MARKERS = [
        "/careers",
        "/career",
        "/jobs",
        "/job-search",
        "/join-us",
        "/openings",
        "/positions",
        "/work-with-us",
    ]
    GENERIC_NEGATIVE_URL_MARKERS = [
        "/about",
        "/leadership",
        "/company",
        "/solutions",
        "/solution",
        "/platform",
        "/products",
        "/product",
        "/integrations",
        "/integration",
        "/investor",
        "/news",
        "/blog",
        "/press",
        "/customers",
        "/contact",
    ]
    GENERIC_POSITIVE_TEXT_MARKERS = [
        "career opportunities",
        "current openings",
        "current opportunities",
        "join our team",
        "open positions",
        "open roles",
        "search jobs",
        "view open roles",
        "we're hiring",
        "work with us",
    ]
    GENERIC_NEGATIVE_TEXT_MARKERS = [
        "leadership team",
        "product overview",
        "platform overview",
        "customer stories",
        "investor relations",
        "our solutions",
        "request a demo",
        "contact sales",
    ]

    def __init__(self, session: Optional[requests.Session] = None, deep_timeout_s: float = 20.0):
        self.session = session or get_shared_session()
        self.deep_timeout_s = deep_timeout_s
        self.discovery_budget_ms = settings.heal_discovery_budget_ms
        self._validate_url_cache: Dict[str, Tuple[float, bool]] = {}
        self._discovery_validation_cache: Dict[Tuple[str, str, str], Tuple[float, bool]] = {}
        # Optional Deep Heal integration
        try:
            from deep_search import playwright_adapter
            self.playwright = playwright_adapter if playwright_adapter.is_available() else None
        except ImportError:
            self.playwright = None

    def _get_headers(self, referer: Optional[str] = None) -> Dict[str, str]:
        return get_headers(referer)

    def _jitter(self):
        min_jitter = min(settings.heal_jitter_min_ms, settings.heal_jitter_max_ms) / 1000.0
        max_jitter = max(settings.heal_jitter_min_ms, settings.heal_jitter_max_ms) / 1000.0
        time.sleep(random.uniform(min_jitter, max_jitter))

    def _probe_timeout(self, adapter: Optional[str]) -> int:
        return self.DIRECT_PROBE_TIMEOUTS.get(adapter or "", 5)

    def _remaining_budget_s(self, deadline: Optional[float]) -> float:
        if deadline is None:
            return float(self.discovery_budget_ms) / 1000.0
        return max(0.0, deadline - time.perf_counter())

    def _timed_out(self, deadline: Optional[float]) -> bool:
        return self._remaining_budget_s(deadline) <= 0.0

    def _request_timeout(self, adapter: Optional[str], deadline: Optional[float], default: float | None = None) -> float:
        preferred = float(default if default is not None else self._probe_timeout(adapter))
        remaining = self._remaining_budget_s(deadline)
        if remaining <= 0.0:
            return 0.1
        return max(0.5, min(preferred, remaining))

    def _cache_get(self, cache: Dict[Any, Tuple[float, bool]], key: Any) -> Optional[bool]:
        entry = cache.get(key)
        if not entry:
            return None
        cached_at, value = entry
        if (time.perf_counter() - cached_at) > self.CACHE_TTL_S:
            cache.pop(key, None)
            return None
        return value

    def _cache_set(self, cache: Dict[Any, Tuple[float, bool]], key: Any, value: bool) -> None:
        cache[key] = (time.perf_counter(), value)

    def _html_for_parse(self, html: str) -> str:
        return str(html or "")[: self.MAX_HTML_PARSE_CHARS]

    def _signal_words(self, name: str) -> List[str]:
        name_l = str(name or "").lower()
        words = [word for word in re.findall(r"[a-z0-9]+", name_l) if len(word) >= 2]
        if words:
            return words
        compact = re.sub(r"[^a-z0-9]+", "", name_l)
        return [compact] if compact else []

    def _phase_deadline(self, deadline: Optional[float], budget_ms: int) -> Optional[float]:
        phase = time.perf_counter() + max(0.0, float(budget_ms) / 1000.0)
        if deadline is None:
            return phase
        return min(deadline, phase)

    def _domain_suggested_adapter(self, domain: str) -> Optional[str]:
        domain_l = str(domain or "").lower()
        for adapter, markers in self.EMBED_MARKERS:
            if adapter == "generic":
                continue
            if any(marker in domain_l for marker in [markers] if marker):
                return adapter
        return None

    def _url_suggested_adapter(self, url: str) -> Optional[str]:
        url_l = str(url or "").lower()
        for adapter, marker in self.EMBED_MARKERS:
            if adapter == "generic":
                continue
            if marker in url_l:
                return adapter
        return None

    def _existing_assignment_suspicious(self, company: Dict[str, Any]) -> bool:
        adapter = str(company.get("adapter", "") or "").lower()
        if adapter not in {"ashby", "greenhouse", "lever", "rippling", "smartrecruiters", "workday"}:
            return False

        domain_hint = self._domain_suggested_adapter(company.get("domain", ""))
        if domain_hint and domain_hint != adapter:
            return True

        url_hint = self._url_suggested_adapter(company.get("careers_url", ""))
        if url_hint and url_hint != adapter:
            return True

        adapter_key = str(company.get("adapter_key", "") or "").strip().lower()
        if adapter == "smartrecruiters" and adapter_key in {"fantastic", "_next"}:
            logger.info("Suspicious smartrecruiters adapter_key for %s: %s", company.get("name", "Unknown"), adapter_key)
            return True
        if adapter == "rippling" and adapter_key in {"_next", "fantastic"}:
            logger.info("Suspicious rippling adapter_key for %s: %s", company.get("name", "Unknown"), adapter_key)
            return True

        name = str(company.get("name", "") or "")
        if adapter == "ashby" and re.fullmatch(r"[A-Z0-9&. ]{2,8}", name):
            return True

        if company.get("heal_skip") and adapter != "generic":
            return True

        return False

    def _persistent_health_skip_reason(self, company: Dict[str, Any]) -> Optional[str]:
        company_name = str(company.get("name", "") or "").strip()
        if not company_name:
            return None
        adapter = str(company.get("adapter", "") or "").lower()
        conn = db.get_connection()
        try:
            row = None
            source = ""
            if adapter == "workday":
                row = db.get_workday_target_health(conn, company_name)
                source = "workday"
            elif adapter == "generic":
                row = db.get_generic_target_health(conn, company_name)
                source = "generic"
            if not row or not row["cooldown_until"]:
                return None
            cooldown_until = datetime.fromisoformat(str(row["cooldown_until"]).replace("Z", "+00:00"))
            if cooldown_until.tzinfo is None:
                cooldown_until = cooldown_until.replace(tzinfo=timezone.utc)
            cooldown_until = cooldown_until.astimezone(timezone.utc)
            if cooldown_until > datetime.now(timezone.utc):
                last_status = str(row["last_status"] or "").strip()
                detail = f"Skipped due to {source} scrape cooldown until {cooldown_until.isoformat()}"
                if last_status:
                    detail += f" ({last_status})"
                return detail
            return None
        except Exception:
            return None
        finally:
            conn.close()

    def discover(self, company: Dict[str, Any], force: bool = False, deep: bool = False) -> DiscoveryResult:
        name = company.get("name", "Unknown")
        existing_url = company.get("careers_url", "")
        deadline = time.perf_counter() + (float(self.discovery_budget_ms) / 1000.0)
        
        # 0. Manual Override check
        if company.get("heal_skip") and not force:
            return DiscoveryResult(None, None, existing_url, "VALID", "User-verified (heal_skip)")

        if not force:
            health_skip_reason = self._persistent_health_skip_reason(company)
            if health_skip_reason:
                return DiscoveryResult(
                    company.get("adapter"),
                    company.get("adapter_key"),
                    existing_url,
                    "VALID",
                    health_skip_reason,
                )

        # 1a. Unsupported ATS fast-exit — mark manual_only immediately, no waterfall waste.
        if existing_url and self._url_is_unsupported_ats(existing_url):
            logger.info("%s: careers_url uses unsupported ATS (%s) — marking manual_only", name, existing_url)
            return DiscoveryResult(
                company.get("adapter"), company.get("adapter_key"),
                existing_url, "BLOCKED",
                f"Unsupported ATS vendor: {existing_url}",
            )

        # 1b. Adapter mismatch auto-correct — if the URL clearly belongs to a different ATS,
        # update the adapter field before validating so the right scraper is used.
        if existing_url:
            url_adapter = self._url_suggested_adapter(existing_url)
            current_adapter = str(company.get("adapter", "") or "").lower()
            if url_adapter and current_adapter and url_adapter != current_adapter:
                logger.info(
                    "%s: adapter mismatch — stored '%s' but URL suggests '%s'; correcting",
                    name, current_adapter, url_adapter,
                )
                company = {**company, "adapter": url_adapter}

        # 1c. Validation check
        if existing_url and not self._existing_assignment_suspicious(company) and self._validate_existing_assignment(company):
            detail = "Existing URL confirmed"
            if force:
                detail = "Existing URL confirmed (forced check)"
            return DiscoveryResult(
                company.get("adapter"), company.get("adapter_key"),
                existing_url, "VALID", detail,
            )

        # Create slug candidates
        slug = re.sub(r"[^a-zA-Z0-9]", "", name).lower()
        slug_candidates = [slug]
        if " " in name:
            slug_full = name.replace(" ", "").lower()
            if slug_full != slug: slug_candidates.append(slug_full)
        
        domain = company.get("domain", "")
        if not domain:
            domain = slug + ".com"
        domain_hint = self._domain_suggested_adapter(domain)

        # 2. "Company First" - Waterfall Discovery (Find official page)
        # Skip straight to search if the existing URL has no ATS signal — crawling a
        # marketing homepage wastes the entire waterfall budget for nothing.
        existing_url_has_signal = existing_url and self._url_has_ats_signal(existing_url)
        if not existing_url_has_signal and not existing_url:
            # No URL at all — waterfall may find something, run it.
            res = self._waterfall_discovery(domain, name, deadline)
        elif not existing_url_has_signal:
            # URL exists but it's a marketing page — skip to search.
            logger.debug("%s: existing URL has no ATS signal, skipping waterfall → search", name)
            res = None
        else:
            res = self._waterfall_discovery(domain, name, deadline)

        # 3. Web Search
        if not res or res.status == "FALLBACK":
            search_res = self._search_discovery(name, domain, deadline)
            if search_res and search_res.status == "FOUND":
                res = search_res

        if res and res.status == "FOUND":
            return res

        # 5. "Guessing" - Direct Slug Probes (Greenhouse, Lever, Ashby)
        direct_probe_order = ["greenhouse", "lever", "ashby"]
        if domain_hint in direct_probe_order:
            direct_probe_order = [domain_hint] + [adapter for adapter in direct_probe_order if adapter != domain_hint]

        for s in slug_candidates:
            if self._timed_out(deadline):
                break
            for adapter_name in direct_probe_order:
                if self._timed_out(deadline):
                    break
                if adapter_name == "greenhouse":
                    for gh_domain in ["job-boards.greenhouse.io", "boards.greenhouse.io"]:
                        if self._timed_out(deadline):
                            break
                        probe = self._probe_direct(f"https://{gh_domain}/{s}", "greenhouse", s, name, deadline)
                        if probe:
                            return probe
                elif adapter_name == "lever":
                    probe = self._probe_direct(f"https://jobs.lever.co/{s}", "lever", s, name, deadline)
                    if not probe:
                        probe = self._probe_direct(f"https://api.lever.co/v0/postings/{s}?mode=json", "lever", s, name, deadline)
                    if probe:
                        return probe
                elif adapter_name == "ashby":
                    probe = self._probe_direct(f"https://jobs.ashbyhq.com/{s}", "ashby", s, name, deadline)
                    if not probe:
                        probe = self._probe_direct(f"https://api.ashbyhq.com/posting-api/job-board/{s}", "ashby", s, name, deadline)
                    if probe:
                        return probe

        # 6. Workday Sweep (Guessing wd1..wd25 + outliers)
        wd_res = self._probe_workday(slug, name, deadline, adapter_key=str(company.get("adapter_key") or ""))
        if wd_res: return wd_res

        # 7. Final fallback
        if res:
            if deep and not self._timed_out(deadline) and res.status in {"FALLBACK", "BLOCKED"} and self.playwright:
                deep_res = self._deep_heal(res.careers_url or existing_url, name, domain, deadline)
                if deep_res: return deep_res
            return res

        # 8. Deep Heal (Final Attempt)
        if deep and not self._timed_out(deadline) and self.playwright:
            target_url = existing_url or f"https://{domain}"
            deep_res = self._deep_heal(target_url, name, domain, deadline)
            if deep_res: return deep_res

        if self._timed_out(deadline):
            return DiscoveryResult(None, None, None, "NOT_FOUND", "Discovery budget exhausted")
        return DiscoveryResult(None, None, None, "NOT_FOUND", "No board detected")

    def _deep_heal(self, url: str, name: str, domain: str, deadline: Optional[float] = None) -> Optional[DiscoveryResult]:
        """Call the playwright adapter for deep discovery."""
        if not self.playwright: return None
        try:
            remaining = self._remaining_budget_s(deadline)
            if remaining <= 0.0:
                return None
            with ThreadPoolExecutor(max_workers=1) as executor:
                future = executor.submit(self.playwright.deep_heal_company, url, name, comp_domain=domain)
                raw = future.result(timeout=min(self.deep_timeout_s, remaining))
            if raw:
                return DiscoveryResult(
                    adapter=raw["adapter"],
                    adapter_key=raw.get("adapter_key"),
                    careers_url=raw["careers_url"],
                    status="FOUND",
                    detail=f"Deep heal: {raw.get('detail', '')}"
                )
        except TimeoutError:
            logger.warning("Deep heal timed out for %s after %.1fs", name, self.deep_timeout_s)
        except Exception as e:
            logger.error(f"Deep heal error for {name}: {e}")
        return None

    def _probe_workday(self, slug: str, name: str, deadline: Optional[float] = None, adapter_key: str = "") -> Optional[DiscoveryResult]:
        """Sweep wd1..wd25 (plus outlier numbers) for Workday tenants in priority batches.

        Tries the most common wd-numbers first (wd1-5) with the two most common
        site names before falling back to the full sweep, saving ~80% of probes
        when the tenant is found in the first batch.

        If the existing adapter_key or careers_url contains a specific wd-number
        (e.g. ``wd108``, ``wd501``), that number is probed first so companies that
        already used an out-of-range tenant are recovered without burning the full sweep.
        """
        from concurrent.futures import ThreadPoolExecutor, as_completed, TimeoutError as FuturesTimeout

        common_sites = ["External", "Careers"]
        all_sites = ["External", "Careers", "Jobs", slug.capitalize()]

        # Extract any hint number from the existing adapter_key (e.g. "slug.wd501.myworkdayjobs.com/External")
        hint_nums: List[int] = []
        hint_match = re.search(r"\.wd(\d+)\.", str(adapter_key or ""), re.I)
        if hint_match:
            hint_n = int(hint_match.group(1))
            if hint_n not in range(1, 6):  # already in priority batch
                hint_nums = [hint_n]

        # Known outlier numbers beyond wd25 seen in the wild
        outlier_nums = [50, 100, 108, 200, 201, 500, 501, 503]

        # Priority batch: hint numbers first, then wd1-5 × common sites.
        hint_candidates = [(n, s) for n in hint_nums for s in all_sites]
        priority_candidates = hint_candidates + [(n, s) for n in range(1, 6) for s in common_sites]
        # Full sweep: wd6-25 × all sites + outlier numbers × common sites, submitted only if batch misses.
        remaining_candidates = (
            [(n, s) for n in ([10, 12, 25] + [n for n in range(6, 26) if n not in (10, 12, 25)]) for s in all_sites]
            + [(n, s) for n in outlier_nums if n not in hint_nums for s in common_sites]
        )

        def check_one(n, site):
            if self._timed_out(deadline):
                return None
            url = f"https://{slug}.wd{n}.myworkdayjobs.com/{site}"
            try:
                resp = self.session.get(
                    url,
                    headers=self._get_headers(),
                    timeout=self._request_timeout("workday", deadline),
                    allow_redirects=True,
                )
                if resp.status_code == 200 and "myworkdayjobs.com" in resp.url:
                    parsed = urlparse(resp.url)
                    clean_path = re.sub(r"/[a-z]{2}-[a-z]{2}/?", "/", parsed.path, flags=re.I).rstrip("/")
                    key = f"{parsed.netloc}{clean_path}"
                    res = DiscoveryResult("workday", key, resp.url, "FOUND", f"Workday wd{n} probe ({site})")
                    if self._validate_discovery(res, name):
                        return res
            except Exception:
                logger.debug("Workday probe failed for %s wd%d/%s", slug, n, site)
            return None

        def _run_batch(candidates):
            remaining = max(1.0, self._remaining_budget_s(deadline))
            with ThreadPoolExecutor(max_workers=3) as executor:
                futures = [executor.submit(check_one, n, site) for n, site in candidates]
                try:
                    for future in as_completed(futures, timeout=remaining):
                        res = future.result()
                        if res:
                            executor.shutdown(wait=False, cancel_futures=True)
                            return res
                except FuturesTimeout:
                    executor.shutdown(wait=False, cancel_futures=True)
            return None

        result = _run_batch(priority_candidates)
        if result:
            return result
        if not self._timed_out(deadline):
            result = _run_batch(remaining_candidates)
        return result

    def _validate_url(self, url: str, name: str) -> bool:
        if self._is_blacklisted(url): return False
        cache_key = f"{name.lower()}|{url}"
        cached = self._cache_get(self._validate_url_cache, cache_key)
        if cached is not None:
            return cached

        try:
            resp = self.session.get(url, headers=self._get_headers(), timeout=8, allow_redirects=True)
            if resp.status_code != 200:
                self._cache_set(self._validate_url_cache, cache_key, False)
                return False

            is_valid = self._is_probable_careers_page(resp.text, resp.url, name) or self._looks_like_known_ats_url(resp.url)
            self._cache_set(self._validate_url_cache, cache_key, is_valid)
            return is_valid
        except Exception:
            self._cache_set(self._validate_url_cache, cache_key, False)
            return False

    def _validate_existing_assignment(self, company: Dict[str, Any]) -> bool:
        name = str(company.get("name", "Unknown"))
        existing_url = str(company.get("careers_url", "") or "")
        if not existing_url:
            return False

        # Reject generic ATS vendor homepages — they are stale and should be re-discovered.
        if self._url_is_generic_ats_homepage(existing_url):
            logger.info("%s: careers_url is a generic ATS homepage (%s) — forcing re-discovery", name, existing_url)
            return False

        # Reject root-domain / no-ATS-signal URLs immediately so the waterfall can skip
        # straight to search discovery instead of crawling a marketing homepage.
        if not self._url_has_ats_signal(existing_url):
            parsed = urlparse(existing_url)
            path = parsed.path.strip("/")
            if not path or path in {"careers", "jobs"}:
                logger.debug("%s: careers_url has no ATS signal and no specific path (%s) — skipping validation", name, existing_url)
                return False

        adapter = str(company.get("adapter", "") or "").lower() or None
        adapter_key = str(company.get("adapter_key", "") or "").strip() or None

        # For Ashby, probe the posting API directly — a 404 means the slug is dead.
        if adapter == "ashby" and adapter_key:
            try:
                api_url = f"https://api.ashbyhq.com/posting-api/job-board/{adapter_key}"
                resp = self.session.get(
                    api_url,
                    headers=self._get_headers(),
                    timeout=5,
                    allow_redirects=False,
                )
                if resp.status_code == 404:
                    logger.info("%s: Ashby slug '%s' returned 404 — forcing re-discovery", name, adapter_key)
                    return False
                if resp.status_code == 200:
                    return True
            except Exception as exc:
                logger.debug("%s: Ashby API probe failed for slug '%s': %s", name, adapter_key, exc)
            # Fall through to normal URL validation on unexpected status codes

        if adapter and self._looks_like_known_ats_url(existing_url):
            result = DiscoveryResult(adapter, adapter_key, existing_url, "VALID", "Stored ATS assignment")
            return self._validate_discovery(result, name)

        return self._validate_url(existing_url, name)

    def _probe_direct(self, url: str, adapter: str, key: str, name: str, deadline: Optional[float] = None) -> Optional[DiscoveryResult]:
        if self._timed_out(deadline):
            return None
        try:
            resp = self.session.get(
                url,
                headers=self._get_headers(),
                timeout=self._request_timeout(adapter, deadline),
                allow_redirects=True,
            )
            
            if resp.status_code in (200, 403):
                res = DiscoveryResult(adapter, key, resp.url, "FOUND", f"Direct {adapter} probe")
                if self._validate_discovery_response(res, name, resp):
                    return res
            elif resp.status_code == 429:
                time.sleep(1)
        except Exception:
            pass
        return None

    def _waterfall_discovery(self, domain: str, name: str, deadline: Optional[float] = None) -> Optional[DiscoveryResult]:
        from concurrent.futures import ThreadPoolExecutor, as_completed, TimeoutError as FuturesTimeout

        bases = [f"jobs.{domain}", f"careers.{domain}", f"www.{domain}", domain]
        paths = [
            "", "/careers", "/jobs", "/positions", "/open-positions", "/all-jobs",
            "/en/jobs", "/en-us/jobs", "/about/careers", "/join-us", "/search-jobs",
            "/careers/search", "/careers/positions", "/careers/openings",
            "/careers/", "/jobs/", "/en/jobs/", "/en-us/jobs/"
        ]

        checked_urls = set()
        candidates = []
        for b in bases:
            for p in paths:
                url = urljoin(f"https://{b}", p)
                if url not in checked_urls:
                    candidates.append((b, url))
                    checked_urls.add(url)

        def check_url(base, url):
            if self._timed_out(deadline):
                return None
            try:
                resp = self.session.get(
                    url,
                    headers=self._get_headers(),
                    timeout=self._request_timeout(None, deadline, default=5),
                    allow_redirects=True,
                )
                if resp.status_code == 200:
                    res = self._scan_content(resp.text, resp.url, name)
                    if res:
                        return res
                    if base == domain or base == f"www.{domain}" or "careers." in base:
                        follow_deadline = self._phase_deadline(deadline, settings.heal_waterfall_follow_budget_ms)
                        for link in self._find_careers_links(resp.text, resp.url):
                            if self._timed_out(follow_deadline):
                                return None
                            try:
                                sub_resp = self.session.get(
                                    link,
                                    headers=self._get_headers(),
                                    timeout=self._request_timeout(None, follow_deadline, default=5),
                                    allow_redirects=True,
                                )
                                if sub_resp.status_code == 200:
                                    sub_res = self._scan_content(sub_resp.text, sub_resp.url, name)
                                    if sub_res:
                                        return sub_res
                            except Exception:
                                continue
                elif resp.status_code == 403 and any(x in url.lower() for x in ["jobs", "careers", "positions", "openings"]):
                    return DiscoveryResult("generic", None, url, "FALLBACK", "Potential blocked career page")
            except Exception:
                logger.debug("Waterfall probe failed for %s at %s", name, url)
            return None

        # Reduced inner pool (4) so 5 outer workers → max 20 concurrent connections
        remaining = max(1.0, self._remaining_budget_s(deadline))
        with ThreadPoolExecutor(max_workers=4) as executor:
            futures = [executor.submit(check_url, b, u) for b, u in candidates]
            try:
                for future in as_completed(futures, timeout=remaining):
                    res = future.result()
                    if res:
                        executor.shutdown(wait=False, cancel_futures=True)
                        return res
            except FuturesTimeout:
                executor.shutdown(wait=False, cancel_futures=True)

        if self._timed_out(deadline):
            return None
        return self._search_discovery(name, domain, deadline)

    def _find_careers_links(self, html: str, base_url: str) -> List[str]:
        """Find non-standard career links on a page."""
        if not self._looks_like_html(html):
            return []
        soup = BeautifulSoup(html, "html.parser")
        candidates = []
        for a in soup.find_all("a", href=True):
            text = a.get_text().lower()
            href = a["href"]
            # Look for listing-specific signal words
            if any(x in text for x in ["career", "job", "open position", "join us", "work with", "positions", "openings"]):
                # Filter out obvious noise
                if any(x in href.lower() for x in ["linkedin.com", "twitter.com", "facebook.com", "glassdoor.com", "instagram.com"]):
                    continue
                candidates.append(urljoin(base_url, href))
        return list(set(candidates))[:8] # Top 8 candidates

    def _search_links_duckduckgo(self, name: str, domain: str, deadline: Optional[float]) -> List[str]:
        """Fetch candidate career page URLs from DuckDuckGo HTML search (no API key required)."""
        from urllib.parse import quote, unquote
        query = quote(f"{name} careers jobs")
        url = f"https://html.duckduckgo.com/html/?q={query}&kl=us-en"
        try:
            resp = self.session.get(
                url,
                headers={**self._get_headers(), "Accept": "text/html,application/xhtml+xml"},
                timeout=self._request_timeout(None, deadline, default=10),
                allow_redirects=True,
            )
            if resp.status_code != 200:
                return []
            soup = BeautifulSoup(resp.text, "html.parser")
            links = []
            for a in soup.find_all("a", href=True):
                href = a["href"]
                # DDG wraps result links in /l/?uddg=<encoded-url> redirects
                if "uddg=" in href:
                    m = re.search(r"uddg=([^&]+)", href)
                    if m:
                        href = unquote(m.group(1))
                if not href.startswith("http") or "duckduckgo.com" in href:
                    continue
                if domain in href or any(marker in href for _, marker in self.EMBED_MARKERS):
                    links.append(href)
            return list(dict.fromkeys(links))[:10]
        except Exception:
            logger.debug("DuckDuckGo search failed for %s", name)
            return []

    def _search_links_yahoo(self, name: str, domain: str, deadline: Optional[float]) -> List[str]:
        """Fetch candidate career page URLs from Yahoo web search."""
        from urllib.parse import quote, unquote
        query = quote(f"{name} careers")
        url = f"https://search.yahoo.com/search?p={query}"
        try:
            resp = self.session.get(
                url,
                headers=self._get_headers(),
                timeout=self._request_timeout(None, deadline, default=10),
                allow_redirects=True,
            )
            if resp.status_code != 200:
                return []
            soup = BeautifulSoup(resp.text, "html.parser")
            links = []
            for a in soup.find_all("a", href=True):
                href = a["href"]
                if "/RU=" in href:
                    m = re.search(r"RU=([^/]+)", href)
                    if m:
                        href = unquote(m.group(1))
                if not href.startswith("http") or "yahoo.com" in href:
                    continue
                if domain in href or any(marker in href for _, marker in self.EMBED_MARKERS):
                    links.append(href)
            return list(dict.fromkeys(links))[:10]
        except Exception:
            logger.debug("Yahoo search failed for %s", name)
            return []

    def _search_discovery(self, name: str, domain: str, deadline: Optional[float] = None) -> Optional[DiscoveryResult]:
        """Search for the careers page using DDG (primary) then Yahoo (fallback).

        Verifies all candidate links concurrently with a deadline-aware timeout
        on as_completed() so a hung verification never blocks the heal thread.
        """
        from concurrent.futures import ThreadPoolExecutor, as_completed, TimeoutError as FuturesTimeout

        if self._timed_out(deadline):
            return None

        links = self._search_links_duckduckgo(name, domain, deadline)
        if not links and not self._timed_out(deadline):
            links = self._search_links_yahoo(name, domain, deadline)
        if not links:
            return None

        def verify_link(url):
            if self._timed_out(deadline):
                return None
            try:
                v_resp = self.session.get(
                    url,
                    headers=self._get_headers(),
                    timeout=self._request_timeout(None, deadline, default=6),
                    allow_redirects=True,
                )
                if v_resp.status_code == 200:
                    return self._scan_content(v_resp.text, v_resp.url, name)
            except Exception:
                logger.debug("Search verification failed for %s: %s", name, url)
            return None

        remaining = max(1.0, self._remaining_budget_s(deadline))
        with ThreadPoolExecutor(max_workers=3) as executor:
            futures = [executor.submit(verify_link, l) for l in links]
            try:
                for future in as_completed(futures, timeout=remaining):
                    res = future.result()
                    if res:
                        executor.shutdown(wait=False, cancel_futures=True)
                        return res
            except FuturesTimeout:
                executor.shutdown(wait=False, cancel_futures=True)
        return None

    def _scan_content(self, html: str, url: str, name: str) -> Optional[DiscoveryResult]:
        if not self._looks_like_html(html):
            return None
        soup = BeautifulSoup(html, "html.parser")
        
        # 1. Search for script tags or data attributes commonly used by embeds
        scripts = soup.find_all("script", src=True)
        for s in scripts:
            src = s["src"]
            if self._is_blacklisted(src): continue
            
            for adapter, marker in self.EMBED_MARKERS:
                if marker in src:
                    key = self._extract_key(src, adapter)
                    if key:
                        # Verify the key is actually a company board
                        res = DiscoveryResult(adapter, key, url, "FOUND", f"Embedded {adapter} found in scripts")
                        if self._validate_discovery(res, name):
                            return res

        # 2. Check all links for ATS domains
        links = soup.find_all("a", href=True)
        for a in links:
            href = a["href"]
            if self._is_blacklisted(href): continue
            
            for adapter, marker in self.EMBED_MARKERS:
                if marker in href:
                    key = self._extract_key(href, adapter)
                    if key:
                        res = DiscoveryResult(adapter, key, href, "FOUND", f"Link to {adapter} board found")
                        if self._validate_discovery(res, name):
                            return res

        # 3. Fallback to generic if page seems valid
        if self._is_probable_careers_page(html, url, name):
            return DiscoveryResult("generic", None, url, "FALLBACK", "Generic career page discovered")
            
        return None

    def _validate_discovery(self, result: DiscoveryResult, name: str) -> bool:
        """Strict verification step for discovered ATS links."""
        if not result.careers_url: return False
        cache_key = (name.lower(), result.careers_url, result.adapter or "")
        cached = self._cache_get(self._discovery_validation_cache, cache_key)
        if cached is not None:
            return cached

        try:
            resp = self.session.get(
                result.careers_url,
                headers=self._get_headers(),
                timeout=self._probe_timeout(result.adapter),
                allow_redirects=True,
            )
            is_valid = self._validate_discovery_response(result, name, resp)
            self._cache_set(self._discovery_validation_cache, cache_key, is_valid)
            return is_valid
        except Exception:
            self._cache_set(self._discovery_validation_cache, cache_key, False)
            return False

    def _validate_discovery_response(self, result: DiscoveryResult, name: str, resp: requests.Response) -> bool:
        sig_words = self._signal_words(name)
        if not sig_words:
            return False

        final_url = resp.url or result.careers_url or ""
        status_code = resp.status_code

        if status_code == 200:
            soup = BeautifulSoup(self._html_for_parse(resp.text), "html.parser")
            title_text = (soup.title.string or "").lower() if soup.title else ""
            body_text = soup.get_text().lower()
            evidence_matches = sum(1 for w in sig_words if w in title_text or w in body_text)

            if self._looks_like_known_ats_url(final_url):
                return evidence_matches >= 1

            if result.adapter == "generic":
                return self._is_probable_careers_page(resp.text, final_url, name)

            if any(w in title_text for w in sig_words):
                return True

            if self._is_probable_careers_page(resp.text, final_url, name):
                return True

            matches = sum(1 for w in sig_words if w in body_text)
            return matches >= min(len(sig_words), 2)

        if status_code == 403:
            if self._looks_like_known_ats_url(final_url):
                return True
            if result.adapter_key and sig_words[0] in result.adapter_key.lower():
                return True

        return False

    def _looks_like_known_ats_url(self, url: str) -> bool:
        url_l = url.lower()
        return any(
            marker in url_l
            for _, marker in self.EMBED_MARKERS
            if marker not in {"onetrust.com", "happydance.com"}
        )

    def _looks_like_html(self, text: str) -> bool:
        if not text:
            return False
        sample = text.lstrip()[:256].lower()
        if sample.startswith(("http://", "https://", "www.")):
            return False
        return "<html" in sample or "<body" in sample or "<script" in sample or "<a " in sample or "<!doctype" in sample

    def _is_probable_careers_page(self, html: str, url: str, name: str) -> bool:
        if not self._looks_like_html(html):
            return False
        text = BeautifulSoup(self._html_for_parse(html), "html.parser").get_text(" ", strip=True).lower()
        url_l = url.lower()
        sig_words = self._signal_words(name)
        company_matches = sum(1 for word in sig_words if word in text)
        careers_signals = ["career", "careers", "job", "jobs", "opening", "openings", "join us", "working at"]

        if self._looks_like_known_ats_url(url):
            return True

        if any(marker in url_l for marker in self.GENERIC_NEGATIVE_URL_MARKERS):
            return False
        if any(marker in text for marker in self.GENERIC_NEGATIVE_TEXT_MARKERS):
            return False

        if any(marker in url_l for marker in self.GENERIC_POSITIVE_URL_MARKERS):
            return any(signal in text for signal in careers_signals) or company_matches >= 1

        if any(marker in text for marker in self.GENERIC_POSITIVE_TEXT_MARKERS):
            return company_matches >= 1

        if company_matches >= min(len(sig_words), 2) and any(signal in text for signal in careers_signals):
            return True

        if any(signal in url_l for signal in ["/careers", "/jobs", "/positions", "/openings", "/join-us"]):
            return company_matches >= 1 or any(signal in text for signal in careers_signals)

        return False

    def _extract_key(self, text: str, adapter: str) -> Optional[str]:
        patterns = self.ATS_PATTERNS.get(adapter, [])
        for p in patterns:
            m = p.search(text)
            if m:
                return m.group(1).split("?")[0].strip("/")
        return None
