"""Healer Service: Modular ATS discovery and URL repair."""

from __future__ import annotations
import re
import time
import random
import logging
import requests
import threading
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

    # Limit concurrency against public search endpoints (DDG/Yahoo) to reduce dropped connections / throttling.
    # This is intentionally low because heal runs already parallelize across companies.
    _SEARCH_ENGINE_SEMAPHORE = threading.BoundedSemaphore(2)

    # Circuit breaker for public search providers (DDG/Yahoo). When a provider is consistently failing due to
    # DNS/proxy/throttling/connection drops, stop spending the entire heal budget on retries.
    SEARCH_BREAKER_FAIL_THRESHOLD = 3      # consecutive failures within window to open breaker
    SEARCH_BREAKER_WINDOW_S = 60.0         # window for counting failures
    SEARCH_BREAKER_OPEN_S = 90.0           # how long to keep breaker open once tripped

    # Common patterns for direct ATS host detection
    ATS_PATTERNS = {
        "greenhouse": [
            re.compile(r"js\?for=([\w.-]+)", re.I), # Catch Greenhouse widget embeds
            re.compile(r"(?:\\?|&)for=([\w.-]+)", re.I),
            re.compile(r"board=([\w.-]+)", re.I),
            re.compile(r"gh_src=([\w.-]+)", re.I),
            re.compile(r"(?:job-boards|boards)\.greenhouse\.io/([\w.-]+)", re.I),
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
    SEARCH_NOISE_HOST_MARKERS = [
        "linkedin.com",
        "indeed.com",
        "glassdoor.com",
        "ziprecruiter.com",
        "monster.com",
        "simplyhired.com",
        "greenhouse.io",  # handled as ATS host separately
        "lever.co",       # handled as ATS host separately
    ]

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

    def _url_domain(self, url: str) -> str:
        try:
            return (urlparse(str(url or "")).netloc or "").lower()
        except Exception:
            return ""

    def _normalize_company_domain(self, domain: str) -> str:
        """Normalize a company 'domain' field to a registrable-ish host (no scheme/path, no leading www.)."""
        raw = str(domain or "").strip()
        if not raw:
            return ""
        try:
            # If user stored a full URL, extract host.
            if "://" in raw:
                raw = urlparse(raw).netloc or raw
            # Strip path fragments if present.
            raw = raw.split("/")[0]
            raw = raw.strip().lower().strip(".")
            if raw.startswith("www."):
                raw = raw[4:]
            return raw
        except Exception:
            return str(domain or "").strip().lower()

    def _domain_slug_candidates(self, domain: str) -> List[str]:
        """Heuristic slug candidates derived from a company domain (often better than name-based slugging)."""
        host = self._normalize_company_domain(domain)
        if not host:
            return []
        labels = [p for p in host.split(".") if p]
        if not labels:
            return []
        ignore = {"www", "careers", "career", "jobs", "job", "apply", "ats"}
        candidates: List[str] = []
        for idx, label in enumerate(labels[:3]):
            if label in ignore:
                continue
            # Prefer the earliest non-noise label.
            candidates.append(re.sub(r"[^a-z0-9]+", "", label.lower()))
            break
        # If the first label was noise, try the next one.
        if not candidates and len(labels) >= 2:
            for label in labels[1:3]:
                if label in ignore:
                    continue
                candidates.append(re.sub(r"[^a-z0-9]+", "", label.lower()))
                break
        return [c for c in candidates if c]

    def _matches_domain_suffix(self, host: str, domain: str) -> bool:
        host = str(host or "").lower().strip(".")
        domain = str(domain or "").lower().strip(".")
        if not host or not domain:
            return False
        return host == domain or host.endswith("." + domain)

    def _classify_known_ats_host(self, url: str) -> Optional[str]:
        """Return suggested adapter if URL is on a known ATS host; otherwise None."""
        host = self._url_domain(url)
        if not host:
            return None
        for adapter, dom in self.KNOWN_ATS_HOSTS:
            if self._matches_domain_suffix(host, dom):
                return adapter
        # Also treat SEARCH_DOMAINS as ATS hosts even if not in KNOWN_ATS_HOSTS.
        for dom in self.SEARCH_DOMAINS:
            if self._matches_domain_suffix(host, dom):
                return "generic"
        return None

    def _url_has_ats_signal(self, url: str) -> bool:
        """Return True if the URL contains a path that suggests an ATS-specific endpoint
        rather than just a company homepage or generic marketing page."""
        url_l = str(url or "").lower()
        ats_indicators = [
            "greenhouse", "lever", "ashby", "workday", "myworkdayjobs",
            "rippling", "smartrecruiters", "icims", "eightfold",
            "/jobs", "/jobs/", "/careers", "/careers/", "/open-positions", "/job-search",
        ]
        return any(ind in url_l for ind in ats_indicators)

    def _looks_like_careersish_url(self, url: str) -> bool:
        """Loose heuristic for careers pages when we don't know the company domain yet."""
        try:
            p = urlparse(str(url or ""))
            host = (p.netloc or "").lower()
            if not host or any(m in host for m in self.SEARCH_NOISE_HOST_MARKERS):
                return False
            path = (p.path or "").lower()
            if any(marker in path for marker in self.GENERIC_POSITIVE_URL_MARKERS):
                return True
            # Common variants that don't exactly match our markers.
            if "/careers" in path or "/jobs" in path or "/openings" in path:
                return True
            return False
        except Exception:
            return False

    def _should_try_workday_sweep(self, company: Dict[str, Any], existing_url: str, domain_hint: Optional[str]) -> bool:
        adapter = str(company.get("adapter", "") or "").lower()
        adapter_key = str(company.get("adapter_key", "") or "").lower()
        if "myworkdayjobs.com" in str(existing_url or "").lower():
            return True
        if adapter in {"workday", "workday_manual"}:
            return True
        if domain_hint == "workday":
            return True
        # Some registries store Workday-ish adapter_keys (e.g., wdN hints or tenant paths).
        if "wd" in adapter_key or "workday" in adapter_key:
            return True
        return False

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
        ("greenhouse", "boards.greenhouse.io"),
        ("greenhouse", "job-boards.greenhouse.io"),
        ("greenhouse", "grnh.js"),
        ("lever", "lever.co"),
        ("ashby", "ashbyhq.com"),
        ("workday", "myworkdayjobs.com"),
        ("rippling", "rippling.com"),
        ("smartrecruiters", "smartrecruiters.com"),
        ("icims", "icims.com"),
        ("bamboohr", "bamboohr.com"),
        ("workable", "workable.com"),
        ("breezy", "breezy.hr"),
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

    # Comprehensive list of job board domains for targeted search discovery
    SEARCH_DOMAINS = [
        "greenhouse.io",
        "lever.co",
        "ashbyhq.com",
        "myworkdayjobs.com",
        "rippling.com",
        "smartrecruiters.com",
        "bamboohr.com",
        "icims.com",
        "jobvite.com",
        "taleo.net",
        "eightfold.ai",
        "workable.com",
        "breezy.hr",
        "personio.com",
        "recruitee.com",
        "freshteam.com",
        "applytojob.com", # JazzHR
        "oraclecloud.com",
        "successfactors.com",
        "paylocity.com",
    ]

    # Domains we can treat as "ATS-like" during discovery. Used to:
    # - accept search results from known ATS hosts (even if not embedded on the company site)
    # - fast-classify unsupported vendors to BLOCKED
    # - improve coverage when a job board is hosted off-domain
    KNOWN_ATS_HOSTS = [
        # Supported adapters
        ("greenhouse", "boards.greenhouse.io"),
        ("greenhouse", "job-boards.greenhouse.io"),
        ("greenhouse", "greenhouse.io"),
        ("lever", "jobs.lever.co"),
        ("lever", "lever.co"),
        ("ashby", "jobs.ashbyhq.com"),
        ("ashby", "ashbyhq.com"),
        ("workday", "myworkdayjobs.com"),
        ("rippling", "rippling.com"),
        ("smartrecruiters", "smartrecruiters.com"),

        # Known ATS / recruiting platforms with dedicated adapters
        ("workable", "workable.com"),
        ("breezy", "breezy.hr"),
        ("generic", "personio.com"),
        ("generic", "recruitee.com"),
        ("generic", "freshteam.com"),
        ("generic", "applytojob.com"),  # JazzHR
        ("generic", "jobvite.com"),
        ("generic", "paylocity.com"),

        # Usually unsupported/blocked for automated scraping in this project
        ("generic", "icims.com"),
        ("generic", "bamboohr.com"),
        ("generic", "taleo.net"),
        ("generic", "successfactors.com"),
        ("generic", "oraclecloud.com"),
        ("generic", "eightfold.ai"),
    ]

    def __init__(self, session: Optional[requests.Session] = None, deep_timeout_s: float = 20.0):
        # Use a dedicated, no-retry session by default. The shared session in settings.py is tuned for scraping
        # and will retry transient network errors, which is counterproductive for healer discovery (slow + noisy).
        self.session = session or self._build_healer_session()
        self.deep_timeout_s = deep_timeout_s
        self.discovery_budget_ms = settings.heal_discovery_budget_ms
        self._validate_url_cache: Dict[str, Tuple[float, bool]] = {}
        self._discovery_validation_cache: Dict[Tuple[str, str, str], Tuple[float, bool]] = {}
        self._search_breaker_lock = threading.Lock()
        # provider -> (opened_until_perf, first_failure_perf, failure_count)
        self._search_breakers: Dict[str, Tuple[float, float, int]] = {}
        # Optional Deep Heal integration
        try:
            from deep_search import playwright_adapter
            self.playwright = playwright_adapter if playwright_adapter.is_available() else None
        except ImportError:
            self.playwright = None

    def _build_healer_session(self) -> requests.Session:
        session = requests.Session()
        retry_strategy = Retry(total=0, backoff_factor=0)
        adapter = HTTPAdapter(pool_connections=20, pool_maxsize=20, max_retries=retry_strategy)
        session.mount("https://", adapter)
        session.mount("http://", adapter)
        return session

    def _search_provider_allowed(self, provider: str, deadline: Optional[float]) -> bool:
        if self._timed_out(deadline):
            return False
        now = time.perf_counter()
        with self._search_breaker_lock:
            state = self._search_breakers.get(provider)
            if not state:
                return True
            opened_until, _, _ = state
            if opened_until > now:
                return False
            # When opened_until == 0, the breaker isn't open; keep failure counters.
            if opened_until == 0.0:
                return True
            # Breaker expired
            self._search_breakers.pop(provider, None)
            return True

    def _search_record_success(self, provider: str) -> None:
        with self._search_breaker_lock:
            # Any success closes the breaker and resets counters for that provider.
            self._search_breakers.pop(provider, None)

    def _search_record_failure(self, provider: str) -> None:
        now = time.perf_counter()
        with self._search_breaker_lock:
            opened_until, first_fail, count = self._search_breakers.get(provider, (0.0, now, 0))
            if opened_until > now:
                # Already open; don't extend it repeatedly.
                return
            if (now - first_fail) > float(self.SEARCH_BREAKER_WINDOW_S):
                first_fail = now
                count = 0
            count += 1
            if count >= int(self.SEARCH_BREAKER_FAIL_THRESHOLD):
                opened_until = now + float(self.SEARCH_BREAKER_OPEN_S)
                logger.warning("[healer] search circuit breaker opened for %s for %.0fs", provider, float(self.SEARCH_BREAKER_OPEN_S))
                self._search_breakers[provider] = (opened_until, first_fail, count)
            else:
                self._search_breakers[provider] = (0.0, first_fail, count)

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

    def _scan_existing_careers_url(self, url: str, name: str, deadline: Optional[float]) -> Optional[DiscoveryResult]:
        """Fetch an existing careers URL and scan HTML for embedded ATS signals (fast-path)."""
        if not url or self._timed_out(deadline) or self._is_blacklisted(url):
            return None
        try:
            resp = self.session.get(
                url,
                headers=self._get_headers(),
                timeout=self._request_timeout(None, deadline, default=6),
                allow_redirects=True,
            )
            if resp.status_code == 200:
                return self._scan_content(resp.text, resp.url, name)
            if resp.status_code == 403 and self._looks_like_careersish_url(resp.url or url):
                return DiscoveryResult("generic", None, resp.url or url, "FALLBACK", "Potential blocked career page")
        except Exception:
            return None
        return None

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

    def discover(
        self,
        company: Dict[str, Any],
        force: bool = False,
        deep: bool = False,
        ignore_cooldown: bool = False,
        disable_waterfall: bool = False,
    ) -> DiscoveryResult:
        name = company.get("name", "Unknown")
        existing_url = company.get("careers_url", "")
        deadline = time.perf_counter() + (float(self.discovery_budget_ms) / 1000.0)
        res = None
        
        # 0. Manual Override check
        if company.get("heal_skip") and not force:
            return DiscoveryResult(None, None, existing_url, "VALID", "User-verified (heal_skip)")

        if not force and not ignore_cooldown:
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
         
        # Do not guess a domain (e.g. slug + ".com") as it causes lots of DNS failures
        # and wastes the discovery budget. If domain is missing, rely on search + embed
        # extraction to find a real careers URL first.
        domain = self._normalize_company_domain(str(company.get("domain", "") or "").strip())
        # Add domain-derived slug(s) (e.g., wandb.ai -> wandb) to improve direct ATS probe accuracy.
        for dom_slug in self._domain_slug_candidates(domain):
            if dom_slug and dom_slug not in slug_candidates:
                slug_candidates.append(dom_slug)
        # Also derive slug(s) from the existing careers_url host when present (some registries omit domain).
        if existing_url:
            for dom_slug in self._domain_slug_candidates(self._url_domain(existing_url)):
                if dom_slug and dom_slug not in slug_candidates:
                    slug_candidates.append(dom_slug)
        domain_hint = self._domain_suggested_adapter(domain)
        deep_tried = False

        # 1d. Fast-path: if we already have a careers URL on the company domain, scan it for embeds
        # before burning time on global slug guessing.
        if existing_url and not self._timed_out(deadline):
            scanned = self._scan_existing_careers_url(existing_url, name, deadline)
            # If we can extract an ATS board from the page HTML, return immediately.
            if scanned and scanned.status == "FOUND":
                return scanned
            # If the careers page is blocked (403), keep it as a fallback hint but continue.
            # This is important: many companies (e.g. OpenAI) block their marketing site while
            # their ATS board (Ashby/Greenhouse/etc.) remains accessible and discoverable via direct probes.
            if scanned and scanned.status == "FALLBACK":
                res = scanned

            # When waterfall is disabled, the existing careers URL is often the best starting point.
            # If it is JS-rendered or blocked, try Playwright immediately rather than spending
            # most of the discovery budget on search retries and slug guessing.
            if deep and disable_waterfall and self.playwright and not self._timed_out(deadline):
                deep_tried = True
                deep_res = self._deep_heal(existing_url, name, domain, deadline)
                if deep_res:
                    return deep_res

        # 2. "Company First" - Waterfall Discovery + Parallel Search
        # We now run the targeted search phase concurrently with the domain waterfall
        # to ensure the 'cheat code' doesn't wait for slow marketing sites.
        with ThreadPoolExecutor(max_workers=2) as outer_executor:
            # Task A: Waterfall (Domain probing)
            existing_url_has_signal = existing_url and self._url_has_ats_signal(existing_url)
            waterfall_future = None
            # Never run the waterfall without a real domain; it generates invalid URLs like https:///careers.
            if domain and (not disable_waterfall) and (not existing_url_has_signal and not existing_url):
                waterfall_future = outer_executor.submit(self._waterfall_discovery, domain, name, deadline)
            elif domain and (not disable_waterfall) and existing_url_has_signal:
                waterfall_future = outer_executor.submit(self._waterfall_discovery, domain, name, deadline)
             
            # Task B: Targeted Search (The Cheat Code)
            search_future = outer_executor.submit(self._search_discovery, name, domain, deadline)
            
            # Use whichever finds a valid board first
            try:
                # Prioritize search results as they are usually more accurate deep-paths
                res = search_future.result(timeout=max(0.5, min(12.0, self._remaining_budget_s(deadline))))
                if res and res.status == "FOUND":
                    outer_executor.shutdown(wait=False, cancel_futures=True)
                    return res
                
                if waterfall_future:
                    res = waterfall_future.result(timeout=max(0.5, min(10.0, self._remaining_budget_s(deadline))))
                    if res and res.status == "FOUND":
                        return res
            except Exception:
                pass

        # 3. Final Fallback Search (if not already found)
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

        # Cap time spent on slug guessing so we don't burn the entire discovery budget.
        probe_deadline = self._phase_deadline(deadline, 15000)
        slugs_to_try = list(slug_candidates)
        if disable_waterfall:
            # When skipping the waterfall, be more conservative with guessing to avoid 60s timeouts.
            slugs_to_try = slugs_to_try[:2]

        def _direct_probe_tasks(slug: str):
            tasks: List[Tuple[str, str, str]] = []
            for adapter_name in direct_probe_order:
                if adapter_name == "greenhouse":
                    for gh_domain in ["job-boards.greenhouse.io", "boards.greenhouse.io"]:
                        tasks.append(("greenhouse", f"https://{gh_domain}/{slug}", slug))
                elif adapter_name == "lever":
                    tasks.append(("lever", f"https://jobs.lever.co/{slug}", slug))
                    tasks.append(("lever", f"https://api.lever.co/v0/postings/{slug}?mode=json", slug))
                elif adapter_name == "ashby":
                    tasks.append(("ashby", f"https://jobs.ashbyhq.com/{slug}", slug))
                    tasks.append(("ashby", f"https://api.ashbyhq.com/posting-api/job-board/{slug}", slug))
            return tasks

        probe_candidates: List[Tuple[str, str, str]] = []
        for s in slugs_to_try:
            probe_candidates.extend(_direct_probe_tasks(s))

        if probe_candidates and not self._timed_out(probe_deadline):
            # Run probes in parallel so a single slow endpoint doesn't consume the entire budget.
            with ThreadPoolExecutor(max_workers=6) as executor:
                futures = [
                    executor.submit(self._probe_direct, url, adapter, key, name, probe_deadline)
                    for (adapter, url, key) in probe_candidates
                ]
                try:
                    remaining = max(1.0, self._remaining_budget_s(probe_deadline))
                    for fut in as_completed(futures, timeout=remaining):
                        probe = fut.result()
                        if probe:
                            executor.shutdown(wait=False, cancel_futures=True)
                            return probe
                except Exception:
                    executor.shutdown(wait=False, cancel_futures=True)

        # 6. Workday Sweep (Guessing wd1..wd25 + outliers)
        # This is relatively expensive; only attempt when there is a Workday hint.
        if self._should_try_workday_sweep(company, existing_url, domain_hint):
            wd_res = self._probe_workday(slug, name, deadline, adapter_key=str(company.get("adapter_key") or ""))
            if wd_res:
                return wd_res

        # If we already have a blocked/fallback company careers URL and have attempted deep heal,
        # don't burn the remaining budget on crawl4ai/extra heuristics in no-waterfall mode.
        if disable_waterfall and deep_tried and res and res.status == "FALLBACK":
            return res

        # 7. Crawl4AI ATS Discovery (LLM-powered detection for complex pages)
        # Even if Workday probe missed, crawl4ai might find the embedded iframe
        if not self._timed_out(deadline) and (existing_url or domain):
            target_url = existing_url or f"https://{domain}"
            crawl4ai_res = self._discover_with_crawl4ai(target_url, name, deadline)
            if crawl4ai_res:
                return crawl4ai_res

        # 8. Final fallback
        if res:
            if deep and not self._timed_out(deadline) and res.status in {"FALLBACK", "BLOCKED"} and self.playwright:
                deep_res = self._deep_heal(res.careers_url or existing_url, name, domain, deadline)
                if deep_res: return deep_res
            return res

        # 9. Deep Heal (Final Attempt)
        if deep and not self._timed_out(deadline) and self.playwright and (existing_url or domain):
            target_url = existing_url or f"https://{domain}"
            deep_res = self._deep_heal(target_url, name, domain, deadline)
            if deep_res:
                return deep_res

        if self._timed_out(deadline):
            return DiscoveryResult(None, None, None, "NOT_FOUND", "Discovery budget exhausted")
        return DiscoveryResult(None, None, None, "NOT_FOUND", "No board detected")

    def _discover_with_crawl4ai(self, url: str, name: str, deadline: Optional[float] = None) -> Optional[DiscoveryResult]:
        """
        Use crawl4ai + LLM to detect ATS platform from a complex careers page.
        Falls back to Playwright if HTTP-only crawling fails.
        Returns DiscoveryResult if successful, None otherwise.
        """
        try:
            import crawl4ai
            from crawl4ai import AsyncWebCrawler, CrawlerRunConfig
        except ImportError:
            logger.debug("[crawl4ai] crawl4ai not installed, skipping crawl4ai discovery")
            return None

        remaining = self._remaining_budget_s(deadline)
        if remaining <= 0.0:
            return None

        try:
            import asyncio
            try:
                # Try to run async crawl
                result = asyncio.run(self._crawl4ai_ats_detection(url, name, remaining))
            except RuntimeError as e:
                # Handle "Event loop is already running" (Streamlit context)
                if "event loop" in str(e).lower():
                    try:
                        import nest_asyncio
                        nest_asyncio.apply()
                        loop = asyncio.get_event_loop()
                        result = loop.run_until_complete(self._crawl4ai_ats_detection(url, name, remaining))
                    except Exception as nested_err:
                        logger.debug(f"[crawl4ai] Nested asyncio failed: {nested_err}")
                        return None
                else:
                    raise

            if result:
                logger.info(
                    f"[crawl4ai] Detected ATS for {name}: {result.adapter} ({result.careers_url})"
                )
                return result
            else:
                logger.debug(f"[crawl4ai] No ATS detection result for {name}")
                return None

        except Exception as e:
            logger.debug(f"[crawl4ai] ATS detection failed for {url}: {e}")
            return None

    async def _crawl4ai_ats_detection(self, url: str, company_name: str, timeout_s: float) -> Optional[DiscoveryResult]:
        """Async crawl4ai extraction of ATS platform metadata."""
        from crawl4ai import AsyncWebCrawler, CrawlerRunConfig
        from crawl4ai.extraction_strategy import LLMExtractionStrategy
        import json

        ats_detection_schema = {
            "type": "object",
            "properties": {
                "ats_platform": {
                    "type": "string",
                    "description": "Detected ATS platform (e.g., 'greenhouse', 'lever', 'ashby', 'workday', 'rippling', 'smartrecruiters', or 'unknown')",
                },
                "job_board_url": {
                    "type": "string",
                    "description": "Direct URL to the job board or careers page, if detectable",
                },
                "adapter_key": {
                    "type": "string",
                    "description": "Company slug or identifier for the ATS (e.g., 'my-company-name'), if detectable",
                },
            },
            "required": ["ats_platform"],
        }

        extraction_instruction = (
            "Analyze this careers/jobs page and detect the ATS platform being used. "
            "Look for: links to job boards, job posting URLs, ATS provider footprints (Greenhouse, Lever, Ashby, Workday, etc.). "
            "If you can extract the company's job board URL, include it. "
            "If you can identify a company slug (e.g., from a Greenhouse 'for=...' parameter), include it."
        )

        try:
            crawler = AsyncWebCrawler()
            config = CrawlerRunConfig(
                word_count_threshold=10,
                extraction_strategy=LLMExtractionStrategy(
                    schema=ats_detection_schema,
                    instruction=extraction_instruction,
                    provider="google",  # Use Gemini (same as enrichment service)
                ),
                timeout=int(timeout_s),
            )

            result = await crawler.arun(url, config)

            if not result or not result.extracted_content:
                return None

            # Parse LLM extraction result
            try:
                extracted = json.loads(result.extracted_content)
            except json.JSONDecodeError:
                logger.debug(f"[crawl4ai] Failed to parse LLM extraction for {url}")
                return None

            ats_platform = extracted.get("ats_platform", "").lower().strip()
            if not ats_platform or ats_platform == "unknown":
                return None

            # Map LLM-detected platform to adapter name
            ats_map = {
                "greenhouse": "greenhouse",
                "lever": "lever",
                "ashby": "ashby",
                "workday": "workday",
                "rippling": "rippling",
                "smartrecruiters": "smartrecruiters",
            }
            adapter = ats_map.get(ats_platform)
            if not adapter:
                return None

            job_board_url = extracted.get("job_board_url", "").strip() or None
            adapter_key = extracted.get("adapter_key", "").strip() or None

            return DiscoveryResult(
                adapter=adapter,
                adapter_key=adapter_key,
                careers_url=job_board_url,
                status="FOUND",
                detail=f"Detected via crawl4ai: {ats_platform}",
            )

        except Exception as e:
            logger.debug(f"[crawl4ai] Extraction failed for {url}: {e}")
            return None

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
        """Sweep wd1..wd25 for Workday tenants by following root redirects (Fast & Accurate)."""
        from concurrent.futures import ThreadPoolExecutor, as_completed, TimeoutError as FuturesTimeout

        # Extract any hint number from existing data
        hint_nums: List[int] = []
        hint_match = re.search(r"\.wd(\d+)\.", str(adapter_key or ""), re.I)
        if hint_match:
            hint_nums = [int(hint_match.group(1))]

        # Common Workday servers + outliers
        wd_servers = sorted(list(set([1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 15, 18, 20, 22, 25] + hint_nums + [50, 103, 108, 201, 501])))

        def check_server(n):
            if self._timed_out(deadline): return None
            # Hit the root domain and follow redirects to find the real path
            url = f"https://{slug}.wd{n}.myworkdayjobs.com"
            try:
                resp = self.session.get(
                    url,
                    headers=self._get_headers(),
                    timeout=self._request_timeout("workday", deadline, default=6),
                    allow_redirects=True,
                )
                if resp.status_code == 200 and "myworkdayjobs.com" in resp.url:
                    final_url = resp.url.split('?')[0].rstrip('/')
                    parsed = urlparse(final_url)
                    
                    # If it redirected to a path (not just /login), we found it!
                    if len(parsed.path.strip('/')) > 5:
                        clean_path = re.sub(r"/[a-z]{2}-[a-z]{2}/?", "/", parsed.path, flags=re.I).rstrip("/")
                        key = f"{parsed.netloc}{clean_path}"
                        return DiscoveryResult("workday", key, final_url, "FOUND", f"Workday wd{n} (Redirect)")
                    
                    # Fallback: Try /Careers if root didn't redirect to a board
                    for alt in ["/Careers", "/External", f"/{slug.capitalize()}"]:
                        alt_url = url + alt
                        alt_resp = self.session.get(alt_url, headers=self._get_headers(), timeout=3, allow_redirects=True)
                        if alt_resp.status_code == 200 and "myworkdayjobs.com" in alt_resp.url and len(urlparse(alt_resp.url).path) > 2:
                            p2 = urlparse(alt_resp.url)
                            cp2 = re.sub(r"/[a-z]{2}-[a-z]{2}/?", "/", p2.path, flags=re.I).rstrip("/")
                            return DiscoveryResult("workday", f"{p2.netloc}{cp2}", alt_resp.url, "FOUND", f"Workday wd{n} ({alt})")
            except Exception:
                pass
            return None

        # Run probes in parallel batches
        with ThreadPoolExecutor(max_workers=8) as executor:
            futures = [executor.submit(check_server, n) for n in wd_servers]
            try:
                remaining = max(1.0, self._remaining_budget_s(deadline))
                for future in as_completed(futures, timeout=remaining):
                    res = future.result()
                    if res:
                        executor.shutdown(wait=False, cancel_futures=True)
                        return res
            except FuturesTimeout:
                executor.shutdown(wait=False, cancel_futures=True)
        
        return None

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

        # Prefer canonical hosts first. Some enterprises have invalid/parked `jobs.` subdomains
        # that trigger SSL hostname mismatch warnings and waste the entire waterfall budget.
        bases = [f"www.{domain}", domain, f"careers.{domain}", f"jobs.{domain}"]
        # Keep the waterfall tight: too many permutations + network flakiness is the main driver of
        # "Discovery budget exhausted". Prioritize the highest-signal paths first.
        high_signal_paths = [
            "/careers", "/careers/",
            "/jobs", "/jobs/",
            "/careers/search",
            "/search-jobs",
            "/open-positions",
            "/positions",
            "/careers/openings",
            "/careers/positions",
            "/about/careers",
            "/join-us",
        ]
        # For subdomains like careers./jobs., probing root path is often useful.
        root_first_paths = ["", *high_signal_paths]

        def paths_for_base(base: str) -> List[str]:
            if base.startswith(("careers.", "jobs.")):
                return root_first_paths
            return high_signal_paths

        bad_bases: set[str] = set()
        bad_bases_lock = threading.Lock()

        checked_urls = set()
        candidates = []
        for b in bases:
            for p in paths_for_base(b):
                url = urljoin(f"https://{b}", p)
                if url not in checked_urls:
                    candidates.append((b, url))
                    checked_urls.add(url)

        # Hard cap the number of waterfall probes per company to avoid eating the entire discovery budget.
        # The outer heal already has search/direct-probe/deep paths; waterfall is a "cheap hint" mechanism.
        candidates = candidates[:24]

        def check_url(base, url):
            if self._timed_out(deadline):
                return None
            with bad_bases_lock:
                if base in bad_bases:
                    return None
            try:
                resp = self.session.get(
                    url,
                    headers=self._get_headers(),
                    timeout=self._request_timeout(None, deadline, default=3),
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
                                    timeout=self._request_timeout(None, follow_deadline, default=3),
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
            except requests.exceptions.SSLError as exc:
                msg = str(exc or "").lower()
                # Hostname mismatch / bad cert on a subdomain: don't keep probing 15 other paths.
                if any(token in msg for token in ("hostname", "doesn't match", "does not match", "certificate")):
                    with bad_bases_lock:
                        bad_bases.add(base)
                logger.debug("Waterfall SSL probe failed for %s at %s: %s", name, url, exc)
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

    def _search_links_duckduckgo(self, name: str, domain: str, deadline: Optional[float], restricted_sites: Optional[List[str]] = None) -> List[str]:
        """Fetch candidate career page URLs from DuckDuckGo HTML search."""
        from urllib.parse import quote, unquote
        if not self._search_provider_allowed("ddg", deadline):
            return []
        query = quote(name)
        # DDG will sometimes drop connections under load; keep a fallback host.
        urls_to_try = [
            f"https://html.duckduckgo.com/html/?q={query}&kl=us-en",
            f"https://duckduckgo.com/html/?q={query}&kl=us-en",
        ]
        if restricted_sites:
            # Append &sites parameter to restrict results to specific domains.
            # DDG's HTML endpoint accepts "sites" in some deployments; keep it best-effort.
            try:
                cleaned = [str(s).strip().lstrip(".") for s in restricted_sites if str(s or "").strip()]
                if cleaned:
                    urls_to_try = [u + "&sites=" + quote(",".join(cleaned)) for u in urls_to_try]
            except Exception:
                pass
            
        try:
            resp = None
            with self._SEARCH_ENGINE_SEMAPHORE:
                for url in urls_to_try:
                    try:
                        resp = self.session.get(
                            url,
                            headers={**self._get_headers(), "Accept": "text/html,application/xhtml+xml"},
                            # Keep this fairly low; healer already parallelizes across companies and
                            # long search timeouts are the main cause of "budget exhausted" runs.
                            timeout=self._request_timeout(None, deadline, default=7),
                            allow_redirects=True,
                        )
                        if resp.status_code == 200:
                            break
                    except Exception:
                        resp = None
                        continue
            if not resp or resp.status_code != 200:
                self._search_record_failure("ddg")
                return []
            self._search_record_success("ddg")
            soup = BeautifulSoup(resp.text, "html.parser")
            links = []
            for a in soup.find_all("a", href=True):
                href = a["href"]
                if "uddg=" in href:
                    m = re.search(r"uddg=([^&]+)", href)
                    if m:
                        href = unquote(m.group(1))
                if not href.startswith("http") or "duckduckgo.com" in href:
                    continue

                href_l = href.lower()
                matches_company_domain = False
                if domain:
                    # Accept either exact host or suffix match (avoid substring-in-URL false positives).
                    matches_company_domain = self._matches_domain_suffix(self._url_domain(href), domain)

                matches_ats_host = self._classify_known_ats_host(href) is not None
                matches_embed_marker = any(marker in href_l for _, marker in self.EMBED_MARKERS)
                matches_careersish = self._looks_like_careersish_url(href)

                # If we don't have a reliable company domain, keep careers-ish links too and validate later.
                if matches_company_domain or matches_ats_host or matches_embed_marker or (not domain and matches_careersish):
                    links.append(href)
            return list(dict.fromkeys(links))[:10]
        except Exception:
            logger.debug("DuckDuckGo search failed for %s", name)
            self._search_record_failure("ddg")
            return []

    def _search_links_yahoo(self, query: str, domain: str, deadline: Optional[float]) -> List[str]:
        """Fetch candidate career URLs from Yahoo search (HTML) as fallback when DDG yields none."""
        from urllib.parse import quote

        if self._timed_out(deadline):
            return []
        if not self._search_provider_allowed("yahoo", deadline):
            return []

        url = f"https://search.yahoo.com/search?p={quote(query)}"
        try:
            with self._SEARCH_ENGINE_SEMAPHORE:
                resp = self.session.get(
                    url,
                    headers={**self._get_headers(), "Accept": "text/html,application/xhtml+xml"},
                    timeout=self._request_timeout(None, deadline, default=7),
                    allow_redirects=True,
                )
            if resp.status_code != 200:
                self._search_record_failure("yahoo")
                return []
            self._search_record_success("yahoo")
            soup = BeautifulSoup(resp.text, "html.parser")
            links: List[str] = []
            for a in soup.find_all("a", href=True):
                href = a.get("href") or ""
                if not href.startswith("http"):
                    continue
                href_l = href.lower()
                if "search.yahoo.com" in href_l or "r.search.yahoo.com" in href_l:
                    continue

                matches_company_domain = False
                if domain:
                    matches_company_domain = self._matches_domain_suffix(self._url_domain(href), domain)
                matches_ats_host = self._classify_known_ats_host(href) is not None
                matches_embed_marker = any(marker in href_l for _, marker in self.EMBED_MARKERS)
                matches_careersish = self._looks_like_careersish_url(href)

                if matches_company_domain or matches_ats_host or matches_embed_marker or (not domain and matches_careersish):
                    links.append(href)
            return list(dict.fromkeys(links))[:10]
        except Exception:
            logger.debug("Yahoo search failed for %s", query)
            self._search_record_failure("yahoo")
            return []

    def _search_discovery(self, name: str, domain: str, deadline: Optional[float] = None) -> Optional[DiscoveryResult]:
        """Search for the careers page using targeted queries for known ATS platforms."""
        from concurrent.futures import ThreadPoolExecutor, as_completed, TimeoutError as FuturesTimeout

        if self._timed_out(deadline):
            return None

        # If all search providers are currently tripped, don't waste budget building queries.
        if not self._search_provider_allowed("ddg", deadline) and not self._search_provider_allowed("yahoo", deadline):
            return None

        # 1. Broad search
        links = self._search_links_duckduckgo(f"{name} careers jobs", domain, deadline)
        
        # 2. Global "Cheat Code" using consolidated site: OR queries
        # We cluster domains to keep query length reasonable but comprehensive
        cluster_size = 6
        ats_search_queries = []
        for i in range(0, len(self.SEARCH_DOMAINS), cluster_size):
            cluster = self.SEARCH_DOMAINS[i : i + cluster_size]
            site_query = " OR ".join([f"site:{d}" for d in cluster])
            ats_search_queries.append(f"{name} careers {site_query}")
        
        # Run consolidated searches in parallel
        with ThreadPoolExecutor(max_workers=3) as executor:
            # Try top 3 clusters (covers ~18 most popular ATS domains)
            search_futures = [executor.submit(self._search_links_duckduckgo, q, "", deadline) for q in ats_search_queries[:3]]
            try:
                # Don't block longer than the remaining discovery budget for this company.
                for f in as_completed(search_futures, timeout=max(0.5, min(10.0, self._remaining_budget_s(deadline)))):
                    links.extend(f.result() or [])
            except Exception:
                pass

        if not links and not self._timed_out(deadline):
            links = self._search_links_yahoo(f"{name} careers jobs", domain, deadline)
        
        # Unique preserve
        links = list(dict.fromkeys(links))
        if not links:
            return None

        def verify_link(url):
            if self._timed_out(deadline):
                return None
            try:
                url_l = url.lower()

                # Reject generic ATS marketing homepages (stale / not company-specific boards).
                if self._url_is_generic_ats_homepage(url):
                    return None

                # If it's on an ATS vendor we can't scrape, return BLOCKED so CLI can route to manual_only.
                if self._url_is_unsupported_ats(url):
                    return DiscoveryResult(None, None, url, "BLOCKED", f"Unsupported ATS vendor: {url}")

                # If it's on a known ATS host, often we can fast-return without parsing HTML.
                known_adapter = self._classify_known_ats_host(url)
                if known_adapter:
                    p0 = urlparse(url)
                    path0 = (p0.path or "").strip("/")
                    # Require a non-trivial path so root/login pages aren't mistaken for boards.
                    if path0 and len(path0) >= 3:
                        key0 = self._extract_key(url, known_adapter) if known_adapter not in {"generic"} else None
                        return DiscoveryResult(known_adapter, key0, url, "FOUND", f"{known_adapter} (Search host match)")

                # If it's a known ATS domain, try to extract and return immediately
                for adapter, marker in self.EMBED_MARKERS:
                    if marker in url_l:
                        key = self._extract_key(url, adapter)
                        if key:
                            # Verify it has a real path (not just a login or root page)
                            p = urlparse(url)
                            path = p.path.strip('/')
                            if len(path) > 3 and not re.fullmatch(r"[a-z]{2}(?:-[A-Z]{2})?", path):
                                cp = re.sub(r"/[a-z]{2}-[a-z]{2}/?", "/", p.path, flags=re.I).rstrip("/")
                                return DiscoveryResult(adapter, f"{p.netloc}{cp}" if adapter == "workday" else key, url, "FOUND", f"{adapter} (Search)")

                v_resp = self.session.get(
                    url,
                    headers=self._get_headers(),
                    timeout=self._request_timeout(None, deadline, default=6),
                    allow_redirects=True,
                )
                if v_resp.status_code == 200:
                    return self._scan_content(v_resp.text, v_resp.url, name)
                if v_resp.status_code == 403 and self._classify_known_ats_host(v_resp.url or url):
                    # Some ATS boards block without a browser; keep the URL as a fallback.
                    return DiscoveryResult("generic", None, v_resp.url or url, "FALLBACK", "Potential blocked ATS board")
            except Exception:
                pass
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

        def canonical_board_url(adapter: str, key: str, raw: str) -> str:
            # For script-embed cases we often only have a key/slug. Construct a canonical board URL
            # so validation can hit the actual ATS board rather than re-validating the company page.
            try:
                key = str(key or "").strip().strip("/")
            except Exception:
                key = ""
            if not key:
                return raw
            if adapter == "greenhouse":
                return f"https://boards.greenhouse.io/{key}"
            if adapter == "lever":
                return f"https://jobs.lever.co/{key}"
            if adapter == "ashby":
                return f"https://jobs.ashbyhq.com/{key}"
            return raw
        
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
                        board_url = canonical_board_url(adapter, key, url)
                        res = DiscoveryResult(adapter, key, board_url, "FOUND", f"Embedded {adapter} found in scripts")
                        if self._validate_discovery(res, name):
                            return res

        # 2. Check all links and iframes for ATS domains
        # We need to find tags that have either href OR src
        tags = soup.find_all(["a", "iframe"])
        for tag in tags:
            href = tag.get("href") or tag.get("src")
            if not href or self._is_blacklisted(href): continue
            
            for adapter, marker in self.EMBED_MARKERS:
                if marker in href:
                    key = self._extract_key(href, adapter)
                    if key:
                        res = DiscoveryResult(adapter, key, href, "FOUND", f"Link or iframe to {adapter} board found")
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
        if any(
            marker in url_l
            for _, marker in self.EMBED_MARKERS
            if marker not in {"onetrust.com", "happydance.com"}
        ):
            return True
        return self._classify_known_ats_host(url) is not None

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
