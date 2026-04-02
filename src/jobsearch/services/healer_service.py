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

logger = logging.getLogger(__name__)

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:124.0) Gecko/20100101 Firefox/124.0",
]

# Global shared session for connection pooling across threads
_SHARED_SESSION: Optional[requests.Session] = None

def get_shared_session() -> requests.Session:
    global _SHARED_SESSION
    if _SHARED_SESSION is None:
        _SHARED_SESSION = requests.Session()
        retries = Retry(total=2, backoff_factor=0.1, status_forcelist=[500, 502, 503, 504])
        # Scaled pool size for high concurrency
        adapter = HTTPAdapter(max_retries=retries, pool_connections=50, pool_maxsize=50)
        _SHARED_SESSION.mount("https://", adapter)
        _SHARED_SESSION.mount("http://", adapter)
    return _SHARED_SESSION

@dataclass
class DiscoveryResult:
    adapter: Optional[str]
    adapter_key: Optional[str]
    careers_url: Optional[str]
    status: str # VALID, FOUND, FALLBACK, NOT_FOUND, BLOCKED
    detail: str

class ATSHealer:
    # Common patterns for direct ATS host detection
    ATS_PATTERNS = {
        "greenhouse": [
            re.compile(r"(?:job-boards|boards)\.greenhouse\.io/([\w.-]+)", re.I),
            re.compile(r"board=([\w.-]+)", re.I), # Embed param
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

    def __init__(self, session: Optional[requests.Session] = None):
        self.session = session or get_shared_session()
        self.ua = random.choice(USER_AGENTS)
        
        # Optional Deep Heal integration
        try:
            from deep_search import playwright_adapter
            self.playwright = playwright_adapter if playwright_adapter.is_available() else None
        except ImportError:
            self.playwright = None

    def _get_headers(self, referer: Optional[str] = None) -> Dict[str, str]:
        headers = {
            "User-Agent": self.ua,
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.5",
            "DNT": "1",
            "Connection": "keep-alive",
            "Upgrade-Insecure-Requests": "1",
        }
        if referer:
            headers["Referer"] = referer
        return headers

    def _jitter(self):
        time.sleep(random.uniform(0.5, 1.5))

    def discover(self, company: Dict[str, Any], force: bool = False, deep: bool = False) -> DiscoveryResult:
        name = company.get("name", "Unknown")
        existing_url = company.get("careers_url", "")
        
        # 0. Manual Override check
        if company.get("heal_skip") and not force:
            return DiscoveryResult(None, None, existing_url, "VALID", "User-verified (heal_skip)")

        # 1. Validation check
        if not force and existing_url:
            if self._validate_url(existing_url, name):
                return DiscoveryResult(None, None, existing_url, "VALID", "Existing URL confirmed")

        # Create slug candidates
        slug = re.sub(r"[^a-zA-Z0-9]", "", name).lower()
        slug_candidates = [slug]
        if " " in name:
            slug_full = name.replace(" ", "").lower()
            if slug_full != slug: slug_candidates.append(slug_full)
        
        domain = company.get("domain", "")
        if not domain:
            domain = slug + ".com"

        # 2. "Company First" - Waterfall Discovery (Find official page)
        res = self._waterfall_discovery(domain, name)
        
        # 3. Web Search (If Waterfall failed or only found a generic page)
        if not res or res.status == "FALLBACK":
            search_res = self._search_discovery(name, domain)
            if search_res and search_res.status == "FOUND":
                res = search_res # Found a real ATS via search

        # 4. If we found a real ATS via official channels, we are done!
        if res and res.status == "FOUND":
            return res

        # 5. "Guessing" - Direct Slug Probes (Greenhouse, Lever, Ashby)
        for s in slug_candidates:
            # Greenhouse
            probe = self._probe_direct(f"https://boards.greenhouse.io/{s}", "greenhouse", s, name)
            if probe: return probe
            
            # Lever
            probe = self._probe_direct(f"https://api.lever.co/v0/postings/{s}?mode=json", "lever", s, name)
            if probe: return probe
            
            # Ashby
            probe = self._probe_direct(f"https://api.ashbyhq.com/posting-api/job-board/{s}", "ashby", s, name)
            if probe: return probe

        # 6. Workday Sweep (Guessing wd1..wd25)
        wd_res = self._probe_workday(slug, name)
        if wd_res: return wd_res

        # 7. Final fallback to whatever Waterfall/Search found (likely "generic")
        if res:
            if deep and res.status == "FALLBACK" and self.playwright:
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
            raw = self.playwright.deep_heal_company(url, name, comp_domain=domain)
            if raw:
                return DiscoveryResult(
                    adapter=raw["adapter"],
                    adapter_key=raw.get("adapter_key"),
                    careers_url=raw["careers_url"],
                    status="FOUND",
                    detail=f"Deep heal: {raw.get('detail', '')}"
                )
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
                resp = self.session.get(url, headers=self._get_headers(), timeout=5, allow_redirects=True)
                if resp.status_code == 200 and "myworkdayjobs.com" in resp.url:
                    parsed = urlparse(resp.url)
                    # Key normalization: remove lang codes like /en-US
                    clean_path = re.sub(r"/[a-z]{2}-[A-Z]{2}/?", "/", parsed.path).rstrip("/")
                    key = f"{parsed.netloc}{clean_path}"
                    
                    res = DiscoveryResult("workday", key, resp.url, "FOUND", f"Workday wd{n} probe")
                    if self._validate_discovery(res, name):
                        return res
            except:
                pass
            return None

        # Use a small pool for internal company probes to avoid getting blocked
        with ThreadPoolExecutor(max_workers=5) as executor:
            futures = [executor.submit(check_one, n, site) for n, site in candidates]
            for future in as_completed(futures):
                res = future.result()
                if res:
                    executor.shutdown(wait=False, cancel_futures=True)
                    return res
        return None

    def _validate_url(self, url: str, name: str) -> bool:
        try:
            resp = self.session.get(url, headers=self._get_headers(), timeout=12, allow_redirects=True)
            if resp.status_code != 200: return False
            
            content = resp.text.lower()
            name_l = name.lower()
            # Heuristic: Name or first word of name must be in content
            name_words = [w for w in name_l.split() if len(w) > 2]
            first_word = name_words[0] if name_words else name_l
            return name_l in content or first_word in content
        except:
            return False

    def _probe_direct(self, url: str, adapter: str, key: str, name: str) -> Optional[DiscoveryResult]:
        try:
            resp = self.session.get(url, headers=self._get_headers(), timeout=5)
            if resp.status_code == 200:
                # Basic verification that the page belongs to the company
                if name.lower() in resp.text.lower() or adapter == "lever": # Lever API is key-based
                    return DiscoveryResult(adapter, key, resp.url, "FOUND", f"Direct {adapter} probe")
        except:
            pass
        return None

    def _waterfall_discovery(self, domain: str, name: str) -> Optional[DiscoveryResult]:
        bases = [f"jobs.{domain}", f"careers.{domain}", f"www.{domain}", domain]
        # Paths with and without trailing slashes
        paths = [
            "", "/careers", "/jobs", "/en/jobs", "/en-us/jobs", 
            "/about/careers", "/join-us", "/search-jobs", "/careers/search",
            "/careers/", "/jobs/", "/en/jobs/", "/en-us/jobs/"
        ]
        
        checked_urls = set()
        
        for base in bases:
            for path in paths:
                url = urljoin(f"https://{base}", path)
                if url in checked_urls: continue
                checked_urls.add(url)
                
                try:
                    resp = self.session.get(url, headers=self._get_headers(), timeout=8, allow_redirects=True)
                    if resp.status_code == 200:
                        res = self._scan_content(resp.text, resp.url, name)
                        if res: return res
                        
                        # 2. Heuristic: If we hit the main domain, find any "Careers" links and follow them
                        if base == domain or base == f"www.{domain}" or 'careers.' in base:
                            found_links = self._find_careers_links(resp.text, resp.url)
                            for link in found_links:
                                if link in checked_urls: continue
                                checked_urls.add(link)
                                try:
                                    sub_resp = self.session.get(link, headers=self._get_headers(), timeout=8, allow_redirects=True)
                                    if sub_resp.status_code == 200:
                                        res = self._scan_content(sub_resp.text, sub_resp.url, name)
                                        if res: return res
                                except: continue
                                
                    elif resp.status_code == 403 and "jobs." in base:
                        # If jobs subdomain returns 403, it's a high-signal candidate for deep heal
                        return DiscoveryResult("generic", None, url, "FALLBACK", "Potential blocked career page")
                except:
                    continue
        
        # 3.5 Web Search Fallback (Yahoo)
        return self._search_discovery(name, domain)

    def _find_careers_links(self, html: str, base_url: str) -> List[str]:
        """Find non-standard career links on a page."""
        soup = BeautifulSoup(html, "html.parser")
        candidates = []
        for a in soup.find_all("a", href=True):
            text = a.get_text().lower()
            href = a["href"]
            if any(x in text for x in ["career", "job", "open position", "join us", "work with"]):
                # Filter out obvious noise
                if any(x in href.lower() for x in ["linkedin.com", "twitter.com", "facebook.com", "glassdoor.com", "instagram.com"]):
                    continue
                candidates.append(urljoin(base_url, href))
        return list(set(candidates))[:5] # Top 5 candidates only

    def _search_discovery(self, name: str, domain: str) -> Optional[DiscoveryResult]:
        """Search for the careers page using a public search engine."""
        query = f"{name} careers"
        search_url = f"https://search.yahoo.com/search?p={query}"
        try:
            resp = self.session.get(search_url, headers=self._get_headers(), timeout=10)
            if resp.status_code == 200:
                soup = BeautifulSoup(resp.text, "html.parser")
                # Extract results
                for a in soup.find_all("a", href=True):
                    href = a["href"]
                    # Clean Yahoo redirects if present
                    if "/RU=" in href:
                        from urllib.parse import unquote
                        m = re.search(r"RU=([^/]+)", href)
                        if m: href = unquote(m.group(1))
                    
                    if not href.startswith("http") or "yahoo.com" in href:
                        continue
                        
                    # If result is on the company domain or a known ATS
                    if domain in href or any(m in href for _, m in self.EMBED_MARKERS):
                        # Verify the result
                        try:
                            v_resp = self.session.get(href, headers=self._get_headers(), timeout=8, allow_redirects=True)
                            if v_resp.status_code == 200:
                                res = self._scan_content(v_resp.text, v_resp.url, name)
                                if res: return res
                        except:
                            continue
        except:
            pass
        return None

    def _scan_content(self, html: str, url: str, name: str) -> Optional[DiscoveryResult]:
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
        if name.lower() in html.lower() and ("career" in html.lower() or "job" in html.lower()):
            return DiscoveryResult("generic", None, url, "FALLBACK", "Generic career page discovered")
            
        return None

    def _validate_discovery(self, result: DiscoveryResult, name: str) -> bool:
        """Strict verification step for discovered ATS links."""
        if not result.careers_url: return False
        try:
            name_l = name.lower()
            # Find significant words (longer than 2 chars)
            sig_words = [w for w in name_l.split() if len(w) > 2]
            if not sig_words: sig_words = [name_l]
            
            resp = self.session.get(result.careers_url, headers=self._get_headers(), timeout=10, allow_redirects=True)
            if resp.status_code != 200:
                if resp.status_code == 403 and result.adapter_key:
                    # If blocked but slug matches exactly, it's likely correct
                    return sig_words[0] in result.adapter_key.lower()
                return False
            
            soup = BeautifulSoup(resp.text, "html.parser")
            
            # 1. Check <title> - highest signal
            title_text = (soup.title.string or "").lower() if soup.title else ""
            if any(w in title_text for w in sig_words):
                return True
                
            # 2. Check body text - fallback
            body_text = soup.get_text().lower()
            # Require at least 2 significant words if possible to reduce false positives
            matches = sum(1 for w in sig_words if w in body_text)
            return matches >= min(len(sig_words), 2)
        except:
            return False

    def _extract_key(self, text: str, adapter: str) -> Optional[str]:
        patterns = self.ATS_PATTERNS.get(adapter, [])
        for p in patterns:
            m = p.search(text)
            if m:
                return m.group(1).split("?")[0].strip("/")
        return None
