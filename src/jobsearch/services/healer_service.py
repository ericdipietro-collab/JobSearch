"""Healer Service: Modular ATS discovery and URL repair."""

from __future__ import annotations
import re
import time
import random
import logging
import requests
from pathlib import Path
from typing import List, Dict, Any, Optional, Tuple
from dataclasses import dataclass
from urllib.parse import urljoin, urlparse, parse_qs
from bs4 import BeautifulSoup
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from concurrent.futures import ThreadPoolExecutor, as_completed, TimeoutError
from jobsearch.config.settings import get_headers, get_shared_session, settings

logger = logging.getLogger(__name__)

@dataclass
class DiscoveryResult:
    adapter: Optional[str]
    adapter_key: Optional[str]
    careers_url: Optional[str]
    status: str # VALID, FOUND, FALLBACK, NOT_FOUND, BLOCKED
    detail: str

class ATSHealer:
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

    def _is_blacklisted(self, url: str) -> bool:
        return any(b in url.lower() for b in self.BLACKLISTED_SUBDOMAINS)

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

    def __init__(self, session: Optional[requests.Session] = None, deep_timeout_s: float = 20.0):
        self.session = session or get_shared_session()
        self.deep_timeout_s = deep_timeout_s
        self._validate_url_cache: Dict[str, bool] = {}
        self._discovery_validation_cache: Dict[Tuple[str, str, str], bool] = {}
        # Optional Deep Heal integration
        try:
            from deep_search import playwright_adapter
            self.playwright = playwright_adapter if playwright_adapter.is_available() else None
        except ImportError:
            self.playwright = None

    def _get_headers(self, referer: Optional[str] = None) -> Dict[str, str]:
        return get_headers(referer)

    def _jitter(self):
        # Reduced jitter for speed
        time.sleep(random.uniform(0.5, 1.5))

    def _probe_timeout(self, adapter: Optional[str]) -> int:
        return self.DIRECT_PROBE_TIMEOUTS.get(adapter or "", 5)

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
            return True
        if adapter == "rippling" and adapter_key in {"_next", "fantastic"}:
            return True

        name = str(company.get("name", "") or "")
        if adapter == "ashby" and re.fullmatch(r"[A-Z0-9&. ]{2,8}", name):
            return True

        if company.get("heal_skip") and adapter != "generic":
            return True

        return False

    def discover(self, company: Dict[str, Any], force: bool = False, deep: bool = False) -> DiscoveryResult:
        name = company.get("name", "Unknown")
        existing_url = company.get("careers_url", "")
        
        # 0. Manual Override check
        if company.get("heal_skip") and not force:
            return DiscoveryResult(None, None, existing_url, "VALID", "User-verified (heal_skip)")

        # 1. Validation check
        if existing_url and not self._existing_assignment_suspicious(company) and self._validate_existing_assignment(company):
            detail = "Existing URL confirmed"
            if force:
                detail = "Existing URL confirmed (forced check)"
            return DiscoveryResult(None, None, existing_url, "VALID", detail)

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
        res = self._waterfall_discovery(domain, name)
        
        # 3. Web Search
        if not res or res.status == "FALLBACK":
            search_res = self._search_discovery(name, domain)
            if search_res and search_res.status == "FOUND":
                res = search_res

        if res and res.status == "FOUND":
            return res

        # 5. "Guessing" - Direct Slug Probes (Greenhouse, Lever, Ashby)
        direct_probe_order = ["greenhouse", "lever", "ashby"]
        if domain_hint in direct_probe_order:
            direct_probe_order = [domain_hint] + [adapter for adapter in direct_probe_order if adapter != domain_hint]

        for s in slug_candidates:
            for adapter_name in direct_probe_order:
                if adapter_name == "greenhouse":
                    for gh_domain in ["job-boards.greenhouse.io", "boards.greenhouse.io"]:
                        probe = self._probe_direct(f"https://{gh_domain}/{s}", "greenhouse", s, name)
                        if probe:
                            return probe
                elif adapter_name == "lever":
                    probe = self._probe_direct(f"https://jobs.lever.co/{s}", "lever", s, name)
                    if not probe:
                        probe = self._probe_direct(f"https://api.lever.co/v0/postings/{s}?mode=json", "lever", s, name)
                    if probe:
                        return probe
                elif adapter_name == "ashby":
                    probe = self._probe_direct(f"https://jobs.ashbyhq.com/{s}", "ashby", s, name)
                    if not probe:
                        probe = self._probe_direct(f"https://api.ashbyhq.com/posting-api/job-board/{s}", "ashby", s, name)
                    if probe:
                        return probe

        # 6. Workday Sweep (Guessing wd1..wd25)
        wd_res = self._probe_workday(slug, name)
        if wd_res: return wd_res

        # 7. Final fallback
        if res:
            if deep and res.status in {"FALLBACK", "BLOCKED"} and self.playwright:
                deep_res = self._deep_heal(res.careers_url or existing_url, name, domain)
                if deep_res: return deep_res
            return res

        # 8. Deep Heal (Final Attempt)
        if deep and self.playwright:
            target_url = existing_url or f"https://{domain}"
            deep_res = self._deep_heal(target_url, name, domain)
            if deep_res: return deep_res

        return DiscoveryResult(None, None, None, "NOT_FOUND", "No board detected")

    def _deep_heal(self, url: str, name: str, domain: str) -> Optional[DiscoveryResult]:
        """Call the playwright adapter for deep discovery."""
        if not self.playwright: return None
        try:
            with ThreadPoolExecutor(max_workers=1) as executor:
                future = executor.submit(self.playwright.deep_heal_company, url, name, comp_domain=domain)
                raw = future.result(timeout=self.deep_timeout_s)
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

    def _probe_workday(self, slug: str, name: str) -> Optional[DiscoveryResult]:
        """Sweep wd1..wd25 for Workday tenants concurrently."""
        from concurrent.futures import ThreadPoolExecutor, as_completed
        
        sites = ["External", "Careers", "Jobs", slug.capitalize()]
        # Common WD numbers to try
        wd_numbers = [1, 2, 3, 4, 5, 10, 12, 25] + [n for n in range(1, 26) if n not in [1, 2, 3, 4, 5, 10, 12, 25]]
        
        candidates = []
        for n in wd_numbers:
            for site in sites:
                candidates.append((n, site))

        def check_one(n, site):
            url = f"https://{slug}.wd{n}.myworkdayjobs.com/{site}"
            try:
                # Use isolated headers per request
                resp = self.session.get(url, headers=self._get_headers(), timeout=5, allow_redirects=True)
                if resp.status_code == 200 and "myworkdayjobs.com" in resp.url:
                    parsed = urlparse(resp.url)
                    # Key normalization: remove lang codes like /en-US
                    clean_path = re.sub(r"/[a-z]{2}-[A-Z]{2}/?", "/", parsed.path).rstrip("/")
                    key = f"{parsed.netloc}{clean_path}"
                    
                    res = DiscoveryResult("workday", key, resp.url, "FOUND", f"Workday wd{n} probe ({site})")
                    if self._validate_discovery(res, name):
                        return res
            except:
                pass
            return None

        # Use a small pool for internal company probes to avoid getting blocked by WD
        with ThreadPoolExecutor(max_workers=5) as executor:
            futures = [executor.submit(check_one, n, site) for n, site in candidates]
            for future in as_completed(futures):
                res = future.result()
                if res:
                    executor.shutdown(wait=False, cancel_futures=True)
                    return res
        return None

    def _validate_url(self, url: str, name: str) -> bool:
        if self._is_blacklisted(url): return False
        cache_key = f"{name.lower()}|{url}"
        if cache_key in self._validate_url_cache:
            return self._validate_url_cache[cache_key]

        try:
            resp = self.session.get(url, headers=self._get_headers(), timeout=8, allow_redirects=True)
            if resp.status_code != 200:
                self._validate_url_cache[cache_key] = False
                return False

            is_valid = self._is_probable_careers_page(resp.text, resp.url, name) or self._looks_like_known_ats_url(resp.url)
            self._validate_url_cache[cache_key] = is_valid
            return is_valid
        except Exception:
            self._validate_url_cache[cache_key] = False
            return False

    def _validate_existing_assignment(self, company: Dict[str, Any]) -> bool:
        name = str(company.get("name", "Unknown"))
        existing_url = str(company.get("careers_url", "") or "")
        if not existing_url:
            return False

        adapter = str(company.get("adapter", "") or "").lower() or None
        adapter_key = str(company.get("adapter_key", "") or "").strip() or None
        if adapter and self._looks_like_known_ats_url(existing_url):
            result = DiscoveryResult(adapter, adapter_key, existing_url, "VALID", "Stored ATS assignment")
            return self._validate_discovery(result, name)

        return self._validate_url(existing_url, name)

    def _probe_direct(self, url: str, adapter: str, key: str, name: str) -> Optional[DiscoveryResult]:
        try:
            resp = self.session.get(
                url,
                headers=self._get_headers(),
                timeout=self._probe_timeout(adapter),
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

    def _waterfall_discovery(self, domain: str, name: str) -> Optional[DiscoveryResult]:
        from concurrent.futures import ThreadPoolExecutor, as_completed
        
        bases = [f"jobs.{domain}", f"careers.{domain}", f"www.{domain}", domain]
        # Expanded paths to catch sites like Coinbase (/positions)
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
            try:
                resp = self.session.get(url, headers=self._get_headers(), timeout=5, allow_redirects=True)
                if resp.status_code == 200:
                    res = self._scan_content(resp.text, resp.url, name)
                    if res: return res
                    
                    # Heuristic follow-up for main domains
                    if base == domain or base == f"www.{domain}" or 'careers.' in base:
                        found_links = self._find_careers_links(resp.text, resp.url)
                        for link in found_links:
                            try:
                                sub_resp = self.session.get(link, headers=self._get_headers(), timeout=5, allow_redirects=True)
                                if sub_resp.status_code == 200:
                                    sub_res = self._scan_content(sub_resp.text, sub_resp.url, name)
                                    if sub_res: return sub_res
                            except: continue
                elif resp.status_code == 403 and any(x in url.lower() for x in ["jobs", "careers", "positions", "openings"]):
                    return DiscoveryResult("generic", None, url, "FALLBACK", "Potential blocked career page")
            except:
                pass
            return None

        with ThreadPoolExecutor(max_workers=8) as executor:
            futures = [executor.submit(check_url, b, u) for b, u in candidates]
            for future in as_completed(futures):
                res = future.result()
                if res:
                    executor.shutdown(wait=False, cancel_futures=True)
                    return res
        
        # 3.5 Web Search Fallback (Yahoo)
        return self._search_discovery(name, domain)

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

    def _search_discovery(self, name: str, domain: str) -> Optional[DiscoveryResult]:
        """Search for the careers page and verify all results concurrently."""
        from concurrent.futures import ThreadPoolExecutor, as_completed
        
        query = f"{name} careers"
        search_url = f"https://search.yahoo.com/search?p={query}"
        try:
            resp = self.session.get(search_url, headers=self._get_headers(), timeout=10)
            if resp.status_code != 200: return None
            
            soup = BeautifulSoup(resp.text, "html.parser")
            search_links = []
            for a in soup.find_all("a", href=True):
                href = a["href"]
                if "/RU=" in href:
                    from urllib.parse import unquote
                    m = re.search(r"RU=([^/]+)", href)
                    if m: href = unquote(m.group(1))
                
                if not href.startswith("http") or "yahoo.com" in href:
                    continue
                if domain in href or any(m in href for _, m in self.EMBED_MARKERS):
                    search_links.append(href)

            def verify_link(url):
                try:
                    v_resp = self.session.get(url, headers=self._get_headers(), timeout=6, allow_redirects=True)
                    if v_resp.status_code == 200:
                        return self._scan_content(v_resp.text, v_resp.url, name)
                except:
                    pass
                return None

            with ThreadPoolExecutor(max_workers=5) as executor:
                futures = [executor.submit(verify_link, l) for l in list(set(search_links))[:10]]
                for future in as_completed(futures):
                    res = future.result()
                    if res:
                        executor.shutdown(wait=False, cancel_futures=True)
                        return res
        except:
            pass
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
        if cache_key in self._discovery_validation_cache:
            return self._discovery_validation_cache[cache_key]

        try:
            resp = self.session.get(
                result.careers_url,
                headers=self._get_headers(),
                timeout=self._probe_timeout(result.adapter),
                allow_redirects=True,
            )
            is_valid = self._validate_discovery_response(result, name, resp)
            self._discovery_validation_cache[cache_key] = is_valid
            return is_valid
        except Exception:
            self._discovery_validation_cache[cache_key] = False
            return False

    def _validate_discovery_response(self, result: DiscoveryResult, name: str, resp: requests.Response) -> bool:
        name_l = name.lower()
        sig_words = [w for w in name_l.split() if len(w) > 2]
        if not sig_words:
            sig_words = [name_l]

        final_url = resp.url or result.careers_url or ""
        status_code = resp.status_code

        if status_code == 200:
            soup = BeautifulSoup(resp.text, "html.parser")
            title_text = (soup.title.string or "").lower() if soup.title else ""
            body_text = soup.get_text().lower()
            evidence_matches = sum(1 for w in sig_words if w in title_text or w in body_text)

            if self._looks_like_known_ats_url(final_url):
                return evidence_matches >= 1

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
        text = BeautifulSoup(html, "html.parser").get_text(" ", strip=True).lower()
        url_l = url.lower()
        name_l = name.lower()
        sig_words = [word for word in re.findall(r"[a-z0-9]+", name_l) if len(word) > 2]
        company_matches = sum(1 for word in sig_words if word in text)
        careers_signals = ["career", "careers", "job", "jobs", "opening", "openings", "join us", "working at"]

        if self._looks_like_known_ats_url(url):
            return True

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
