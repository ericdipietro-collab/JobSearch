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
from dataclasses import dataclass, field
from urllib.parse import urljoin, urlparse, parse_qs
from bs4 import BeautifulSoup
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from concurrent.futures import ThreadPoolExecutor, as_completed, TimeoutError
from jobsearch.config.settings import get_headers, get_shared_session, settings
from jobsearch import ats_db
from jobsearch import ats_db as db
from jobsearch.scraper.ats_routing import CandidateJobURL, choose_extraction_route, fingerprint_ats, rank_candidates, score_candidate_url
from jobsearch.services.health_monitor import HealthMonitor
from jobsearch.scraper.scheduler_policy import EscalationPolicy

logger = logging.getLogger(__name__)

@dataclass
class DiscoveryResult:
    adapter: Optional[str]
    adapter_key: Optional[str]
    careers_url: Optional[str]
    status: str # VALID, FOUND, FALLBACK, NOT_FOUND, BLOCKED
    detail: str
    confidence: float = 1.0  # 0.0–1.0; how certain this URL+adapter pairing is
    source: str = ""  # what phase found it: embed_scan, slug_probe, search, sitemap, etc.
    candidates: List[Dict[str, Any]] = field(default_factory=list)
    route_decision: str = ""

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
        "bamboohr": [
            re.compile(r"([\w-]+)\.bamboohr\.com", re.I),
        ],
        "workable": [
            re.compile(r"workable\.com/([^/?#\s\"']+)", re.I),
        ],
        "breezy": [
            re.compile(r"([\w-]+)\.breezy\.hr", re.I),
        ],
        "jobvite": [
            re.compile(r"jobs\.jobvite\.com/([\w.-]+)", re.I),
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
        ("jobvite", "jobvite.com"),
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
        ("jobvite", "jobvite.com"),
        ("generic", "paylocity.com"),

        # Usually unsupported/blocked for automated scraping in this project
        ("generic", "icims.com"),
        ("bamboohr", "bamboohr.com"),
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
        
        self.escalation_policy = EscalationPolicy(settings)
        self.health_monitor = HealthMonitor(self.escalation_policy)
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
                result = self._scan_content(resp.text, resp.url, name)
                candidate = self._build_candidate(
                    source="existing_url",
                    url=url,
                    final_url=resp.url or url,
                    status_code=resp.status_code,
                    ats_family=fingerprint_ats(resp.url or url, html=resp.text),
                    redirect_chain=[url, resp.url or url],
                    extra_flags=["existing_assignment"],
                )
                if result:
                    merged_candidates = [candidate]
                    if result.careers_url and (result.careers_url != (resp.url or url)):
                        merged_candidates.append(
                            self._build_candidate(
                                source=result.source or "existing_scan",
                                url=result.careers_url,
                                final_url=result.careers_url,
                                status_code=200,
                                ats_family=result.adapter or fingerprint_ats(result.careers_url),
                                redirect_chain=[url, result.careers_url],
                                extra_flags=["listing_link"],
                            )
                        )
                    result.candidates = self._top_candidate_dicts(merged_candidates)
                    result.route_decision = choose_extraction_route(ats_family=result.adapter or candidate.ats_family).decision
                return result
            if resp.status_code == 403 and self._looks_like_careersish_url(resp.url or url):
                candidate = self._build_candidate(
                    source="existing_url",
                    url=url,
                    final_url=resp.url or url,
                    status_code=resp.status_code,
                    ats_family=fingerprint_ats(resp.url or url),
                    redirect_chain=[url, resp.url or url],
                    extra_flags=["blocked_existing_url"],
                )
                return DiscoveryResult(
                    "generic",
                    None,
                    resp.url or url,
                    "FALLBACK",
                    "Potential blocked career page",
                    candidates=self._top_candidate_dicts([candidate]),
                    route_decision=choose_extraction_route(ats_family=candidate.ats_family, blocked=True).decision,
                )
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

    def _search_candidate_matches_company(self, url: str, key: Optional[str], name: str, domain: str) -> bool:
        haystack = " ".join(
            part for part in [
                str(url or "").lower(),
                str(key or "").lower(),
            ] if part
        )
        signal_words = self._signal_words(name)
        if any(word in haystack for word in signal_words if len(word) >= 4):
            return True
        for slug in self._domain_slug_candidates(domain):
            if slug and slug in haystack:
                return True
        return False

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
            # 1. Check unified board_health first
            health = ats_db.get_board_health(conn, company_name)
            if health and health["cooldown_until"]:
                try:
                    until = datetime.fromisoformat(str(health["cooldown_until"]).replace("Z", "+00:00"))
                    if until.tzinfo is None:
                        until = until.replace(tzinfo=timezone.utc)
                    if until > datetime.now(timezone.utc):
                        reason = health["suppression_reason"] or "on cooldown"
                        return f"Skipped due to board cooldown until {until.isoformat()} ({reason})"
                except Exception:
                    pass

            # 2. Legacy fallbacks
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
            return self._wrap_result(company, DiscoveryResult(
                None,
                None,
                existing_url,
                "VALID",
                "User-verified (heal_skip)",
                candidates=self._top_candidate_dicts(
                    [
                        self._build_candidate(
                            source="provided_url",
                            url=existing_url,
                            final_url=existing_url,
                            status_code=200 if existing_url else None,
                            ats_family=fingerprint_ats(existing_url),
                            company_domain=str(company.get("domain", "") or ""),
                        )
                    ]
                ) if existing_url else [],
            ))

        if not force and not ignore_cooldown:
            health_skip_reason = self._persistent_health_skip_reason(company)
            if health_skip_reason:
                return self._wrap_result(company, DiscoveryResult(
                    company.get("adapter"),
                    company.get("adapter_key"),
                    existing_url,
                    "VALID",
                    health_skip_reason,
                    candidates=self._top_candidate_dicts(
                        [
                            self._build_candidate(
                                source="existing_url",
                                url=existing_url,
                                final_url=existing_url,
                                status_code=200 if existing_url else None,
                                ats_family=fingerprint_ats(existing_url),
                                company_domain=str(company.get("domain", "") or ""),
                            )
                        ]
                    ) if existing_url else [],
                ))

        # 1a. Unsupported ATS fast-exit — mark manual_only immediately, no waterfall waste.
        if existing_url and self._url_is_unsupported_ats(existing_url):
            logger.info("%s: careers_url uses unsupported ATS (%s) — marking manual_only", name, existing_url)
            return self._wrap_result(company, DiscoveryResult(
                company.get("adapter"), company.get("adapter_key"),
                existing_url, "BLOCKED",
                f"Unsupported ATS vendor: {existing_url}",
                candidates=self._top_candidate_dicts(
                    [
                        self._build_candidate(
                            source="existing_url",
                            url=existing_url,
                            final_url=existing_url,
                            status_code=200,
                            ats_family=fingerprint_ats(existing_url),
                            company_domain=str(company.get("domain", "") or ""),
                            extra_flags=["unsupported_ats"],
                        )
                    ]
                ),
                route_decision=choose_extraction_route(ats_family=fingerprint_ats(existing_url), unsupported=True).decision,
            ))

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
            return self._wrap_result(company, DiscoveryResult(
                company.get("adapter"), company.get("adapter_key"),
                existing_url, "VALID", detail,
                candidates=self._top_candidate_dicts(
                    [
                        self._build_candidate(
                            source="existing_url",
                            url=existing_url,
                            final_url=existing_url,
                            status_code=200,
                            ats_family=fingerprint_ats(existing_url),
                            company_domain=str(company.get("domain", "") or ""),
                        )
                    ]
                ) if existing_url else [],
                route_decision=choose_extraction_route(ats_family=fingerprint_ats(existing_url)).decision if existing_url else "",
            ))

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
                return self._wrap_result(company, scanned)
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
                    return self._wrap_result(company, deep_res)

        # 2. "Company First" - Waterfall Discovery + Parallel Search
        # We now run the targeted search phase concurrently with the domain waterfall
        # to ensure the 'cheat code' doesn't wait for slow marketing sites.
        outer_executor = ThreadPoolExecutor(max_workers=2)
        waterfall_future = None
        search_future = None
        all_candidates: List[CandidateJobURL] = []
        
        try:
            # Task A: Waterfall (Domain probing)
            existing_url_has_signal = existing_url and self._url_has_ats_signal(existing_url)
            if domain and (not disable_waterfall):
                waterfall_future = outer_executor.submit(self._waterfall_discovery, domain, name, deadline)

            # Task B: Targeted Search (The Cheat Code)
            search_future = outer_executor.submit(self._search_discovery, name, domain, deadline)

            # Collect results from both
            try:
                if search_future:
                    search_res = search_future.result(timeout=max(0.5, min(12.0, self._remaining_budget_s(deadline))))
                    if search_res:
                        if search_res.status == "FOUND":
                            # We found a strong candidate, but let's see if waterfall finds more
                            res = search_res
                        for c in search_res.candidates:
                            all_candidates.append(CandidateJobURL(**c))

                if waterfall_future:
                    water_res = waterfall_future.result(timeout=max(0.5, min(10.0, self._remaining_budget_s(deadline))))
                    if water_res:
                        if not res and water_res.status == "FOUND":
                            res = water_res
                        for c in water_res.candidates:
                            all_candidates.append(CandidateJobURL(**c))
            except Exception as e:
                logger.debug("Discovery task failed: %s", e)
        finally:
            outer_executor.shutdown(wait=False, cancel_futures=True)

        # 3. Final Fallback Search (if not already found)
        if not res or res.status == "FALLBACK":
            search_res = self._search_discovery(name, domain, deadline)
            if search_res:
                if search_res.status == "FOUND":
                    res = search_res
                for c in search_res.candidates:
                    all_candidates.append(CandidateJobURL(**c))

        if res and res.status == "FOUND":
            res.candidates = self._top_candidate_dicts(all_candidates)
            return self._wrap_result(company, res)

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
                            return self._wrap_result(company, probe)
                except Exception:
                    executor.shutdown(wait=False, cancel_futures=True)

        # 6. Workday Sweep (Guessing wd1..wd25 + outliers)
        # This is relatively expensive; only attempt when there is a Workday hint.
        if self._should_try_workday_sweep(company, existing_url, domain_hint):
            wd_res = self._probe_workday(slug, name, deadline, adapter_key=str(company.get("adapter_key") or ""))
            if wd_res:
                return self._wrap_result(company, wd_res)

        # If we already have a blocked/fallback company careers URL and have attempted deep heal,
        # don't burn the remaining budget on crawl4ai/extra heuristics in no-waterfall mode.
        if disable_waterfall and deep_tried and res and res.status == "FALLBACK":
            return self._wrap_result(company, res)

        # 7. Crawl4AI ATS Discovery (LLM-powered detection for complex pages)
        # Even if Workday probe missed, crawl4ai might find the embedded iframe
        if not self._timed_out(deadline) and (existing_url or domain):
            target_url = existing_url or f"https://{domain}"
            crawl4ai_res = self._discover_with_crawl4ai(target_url, name, deadline)
            if crawl4ai_res:
                return self._wrap_result(company, crawl4ai_res)

        # 8. Final fallback
        if res:
            if deep and not self._timed_out(deadline) and res.status in {"FALLBACK", "BLOCKED"} and self.playwright:
                deep_res = self._deep_heal(res.careers_url or existing_url, name, domain, deadline)
                if deep_res: return self._wrap_result(company, deep_res)
            return self._wrap_result(company, res)

        # 9. Deep Heal (Final Attempt)
        if deep and not self._timed_out(deadline) and self.playwright and (existing_url or domain):
            target_url = existing_url or f"https://{domain}"
            deep_res = self._deep_heal(target_url, name, domain, deadline)
            if deep_res:
                return self._wrap_result(company, deep_res)

        if self._timed_out(deadline):
            return self._wrap_result(company, DiscoveryResult(None, None, None, "NOT_FOUND", "Discovery budget exhausted"))
        return self._wrap_result(company, DiscoveryResult(None, None, None, "NOT_FOUND", "No board detected"))

    def _wrap_result(self, company: Dict[str, Any], result: DiscoveryResult) -> DiscoveryResult:
        """Update board_health before returning a discovery result."""
        company_name = company.get("name")
        if not company_name:
            return result
            
        conn = db.get_connection()
        try:
            # Map healer status to health monitor status
            scrape_status = "ok"
            if result.status == "BLOCKED":
                scrape_status = "blocked"
            elif result.status == "NOT_FOUND":
                scrape_status = "error"
            elif result.status == "FALLBACK":
                scrape_status = "ok"
            
            # Healer is a discovery pass, so evaluated_count is 0 unless it finds jobs
            # For Phase 1, we just report the status found.
            self.health_monitor.update_scrape_health(
                conn,
                company=company,
                adapter_name=result.adapter or company.get("adapter", "generic"),
                scrape_status=scrape_status,
                scrape_ms=0.0,
                scrape_note=f"Healer: {result.detail}",
                evaluated_count=0, # Discovery pass
                ats_family=result.adapter or "unknown",
                final_url=result.careers_url or company.get("careers_url", ""),
            )
        except Exception as e:
            logger.debug("Healer failed to update board_health for %s: %s", company_name, e)
        finally:
            conn.close()
        return result

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

        if self._is_shallow_custom_careers_url(existing_url):
            logger.info(
                "%s: careers_url is a shallow custom landing page (%s) - scanning for a stronger job listings URL",
                name,
                existing_url,
            )
            return False

        adapter = str(company.get("adapter", "") or "").lower() or None
        adapter_key = str(company.get("adapter_key", "") or "").strip() or None
        company_domain = self._normalize_company_domain(company.get("domain", ""))

        if adapter and self._looks_like_known_ats_url(existing_url):
            identity_key = adapter_key
            if not identity_key:
                identity_key = self._extract_key(existing_url, adapter) or None
            if not self._search_candidate_matches_company(existing_url, identity_key, name, company_domain):
                logger.info(
                    "%s: existing ATS assignment does not match company identity (%s) - forcing re-discovery",
                    name,
                    existing_url,
                )
                return False

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

        # For Workday, probe the CXS endpoint directly — a 404 means the site/tenant is dead or moved.
        is_workday = adapter == "workday" or "myworkdayjobs.com" in existing_url
        if is_workday:
            try:
                # Use host from careers_url if available, else from adapter_key
                p = urlparse(existing_url)
                host = p.netloc.split(":")[0] if "myworkdayjobs.com" in p.netloc else ""
                if not host and adapter_key and "myworkdayjobs.com" in adapter_key:
                    host = adapter_key.split("/")[0]
                
                if host:
                    tenant = host.split(".")[0]
                    site = ""
                    # Heuristic for site name from existing_url path
                    segments = [s for s in p.path.split("/") if s]
                    for s in segments:
                        # Filter out locales and common non-site segments
                        if len(s) > 3 and s.lower() not in {"en-us", "job", "jobs", "search-results", "apply"}:
                            site = s
                            break
                    
                    if site and tenant and host:
                        api_url = f"https://{host}/wday/cxs/{tenant}/{site}/jobs"
                        resp = self.session.post(
                            api_url,
                            json={"limit": 1, "offset": 0},
                            headers=self._get_headers(referer=existing_url),
                            timeout=6,
                        )
                        if resp.status_code in {401, 404}:
                            logger.info("%s: Workday site (%s/%s) is dead or auth-gated (%d) — forcing re-discovery", 
                                        name, tenant, site, resp.status_code)
                            return False
                        if resp.status_code == 200:
                            return True
            except Exception as exc:
                logger.debug("%s: Workday API probe failed for site '%s': %s", name, existing_url, exc)

        if adapter and self._looks_like_known_ats_url(existing_url):
            result = DiscoveryResult(adapter, adapter_key, existing_url, "VALID", "Stored ATS assignment")
            return self._validate_discovery(result, name)

        return self._validate_url(existing_url, name)

    def _is_shallow_custom_careers_url(self, url: str) -> bool:
        ats_family = fingerprint_ats(url)
        if ats_family not in {"custom", "unknown"}:
            return False
        parsed = urlparse(str(url or ""))
        if not (parsed.netloc or ""):
            return False
        segments = [seg.strip().lower() for seg in (parsed.path or "").split("/") if seg.strip()]
        if not segments:
            return True
        if len(segments) > 3:
            return False
        locale_like = re.compile(r"^[a-z]{2}(?:-[a-z]{2})?$", re.I)
        allowed = {
            "careers",
            "career",
            "jobs",
            "job",
            "join-us",
            "work-with-us",
            "work",
            "us",
            "en",
            "en-us",
            "us-en",
        }
        return all(seg in allowed or bool(locale_like.fullmatch(seg)) for seg in segments)

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

        # Fast path: check sitemap.xml before probing dozens of paths — costs at most 2 requests.
        if not self._timed_out(deadline):
            sitemap_result = self._discover_from_sitemap(domain, name, deadline)
            if sitemap_result:
                return sitemap_result

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
        all_waterfall_candidates = []
        result = None
        with ThreadPoolExecutor(max_workers=4) as executor:
            futures = [executor.submit(check_url, b, u) for b, u in candidates]
            try:
                for future in as_completed(futures, timeout=remaining):
                    res = future.result()
                    if res:
                        if res.status == "FOUND":
                            # We found a definitive one, but keep collecting others for ranking
                            if not result: result = res
                        for c in res.candidates:
                            all_waterfall_candidates.append(CandidateJobURL(**c))
            except FuturesTimeout:
                executor.shutdown(wait=False, cancel_futures=True)

        if result:
            result.candidates = self._top_candidate_dicts(all_waterfall_candidates)
            return result

        if self._timed_out(deadline):
            return None
        return self._search_discovery(name, domain, deadline)

    def _discover_from_sitemap(self, domain: str, name: str, deadline: Optional[float] = None) -> Optional[DiscoveryResult]:
        """Check sitemap.xml (and sitemap_index.xml) for career-related URLs.

        Tries /sitemap.xml then /sitemap_index.xml; recursively resolves one level of
        <sitemapindex> sub-sitemaps.  Returns the first DiscoveryResult found, or None.
        Budget: at most 2 HTTP requests; 3s timeout each; skips on non-200.
        """
        import xml.etree.ElementTree as ET

        sitemap_urls = [
            f"https://{domain}/sitemap.xml",
            f"https://www.{domain}/sitemap.xml",
            f"https://{domain}/sitemap_index.xml",
        ]
        seen: set[str] = set()

        def _extract_locs(xml_text: str) -> List[str]:
            locs: List[str] = []
            try:
                root = ET.fromstring(xml_text)
                ns = {"sm": "http://www.sitemaps.org/schemas/sitemap/0.9"}
                for elem in root.iter():
                    tag = elem.tag.split("}")[-1] if "}" in elem.tag else elem.tag
                    if tag == "loc" and elem.text:
                        locs.append(elem.text.strip())
            except ET.ParseError:
                pass
            return locs

        def _fetch_sitemap(url: str) -> Optional[str]:
            if url in seen or self._timed_out(deadline):
                return None
            seen.add(url)
            try:
                resp = self.session.get(
                    url,
                    headers=self._get_headers(),
                    timeout=self._request_timeout(None, deadline, default=3),
                    allow_redirects=True,
                )
                if resp.status_code == 200 and resp.text:
                    return resp.text
            except Exception:
                pass
            return None

        requests_made = 0
        for sitemap_url in sitemap_urls:
            if requests_made >= 2 or self._timed_out(deadline):
                break
            xml_text = _fetch_sitemap(sitemap_url)
            requests_made += 1
            if not xml_text:
                continue
            locs = _extract_locs(xml_text)
            # If this is a sitemap index, recurse one level into sub-sitemaps that contain career signals
            career_sub_sitemaps = [
                loc for loc in locs
                if any(m in loc.lower() for m in ("career", "job", "position", "opening"))
                and loc.endswith(".xml")
            ]
            if career_sub_sitemaps and requests_made < 2:
                sub_xml = _fetch_sitemap(career_sub_sitemaps[0])
                requests_made += 1
                if sub_xml:
                    locs.extend(_extract_locs(sub_xml))
            # Filter locs for career-signal paths
            career_locs = [
                loc for loc in locs
                if any(m in loc.lower() for m in self.GENERIC_POSITIVE_URL_MARKERS + ["/career", "/job-"])
                and not any(m in loc.lower() for m in self.GENERIC_NEGATIVE_URL_MARKERS)
            ]
            for loc in career_locs:
                result = self._scan_content_from_url(loc, name, deadline)
                if result:
                    result.source = "sitemap"
                    return result
            break  # Only try one sitemap file if first succeeds
        return None

    def _scan_content_from_url(self, url: str, name: str, deadline: Optional[float] = None) -> Optional[DiscoveryResult]:
        """Fetch a URL and run _scan_content on it. Returns DiscoveryResult or None."""
        if self._timed_out(deadline):
            return None
        try:
            resp = self.session.get(
                url,
                headers=self._get_headers(),
                timeout=self._request_timeout(None, deadline, default=4),
                allow_redirects=True,
            )
            if resp.status_code == 200:
                return self._scan_content(resp.text, resp.url, name)
        except Exception:
            pass
        return None

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

        verified: List[DiscoveryResult] = []
        candidate_hits: List[CandidateJobURL] = []

        def verify_link(url):
            if self._timed_out(deadline):
                return None, None
            try:
                url_l = url.lower()
                ats_family = fingerprint_ats(url)

                # Reject generic ATS marketing homepages (stale / not company-specific boards).
                if self._url_is_generic_ats_homepage(url):
                    candidate = self._build_candidate(
                        source="search",
                        url=url,
                        final_url=url,
                        status_code=200,
                        ats_family=ats_family,
                        company_domain=domain,
                        extra_flags=["generic_ats_homepage"],
                    )
                    return None, candidate

                # If it's on an ATS vendor we can't scrape, return BLOCKED so CLI can route to manual_only.
                if self._url_is_unsupported_ats(url):
                    candidate = self._build_candidate(
                        source="search",
                        url=url,
                        final_url=url,
                        status_code=200,
                        ats_family=ats_family,
                        company_domain=domain,
                        extra_flags=["unsupported_ats"],
                    )
                    result = DiscoveryResult(
                        None,
                        None,
                        url,
                        "BLOCKED",
                        f"Unsupported ATS vendor: {url}",
                        confidence=candidate.confidence_score,
                        source="search",
                        route_decision=choose_extraction_route(ats_family=ats_family, unsupported=True).decision,
                    )
                    return result, candidate

                # If it's on a known ATS host, often we can fast-return without parsing HTML.
                known_adapter = self._classify_known_ats_host(url)
                if known_adapter:
                    p0 = urlparse(url)
                    path0 = (p0.path or "").strip("/")
                    # Require a non-trivial path so root/login pages aren't mistaken for boards.
                    if path0 and len(path0) >= 3:
                        key0 = self._extract_key(url, known_adapter) if known_adapter not in {"generic"} else None
                        candidate = self._build_candidate(
                            source="search",
                            url=url,
                            final_url=url,
                            status_code=200,
                            ats_family=known_adapter,
                            company_domain=domain,
                        )
                        result = DiscoveryResult(
                            known_adapter,
                            key0,
                            url,
                            "FOUND",
                            f"{known_adapter} (Search host match)",
                            confidence=candidate.confidence_score,
                            source="search",
                            route_decision=choose_extraction_route(ats_family=known_adapter).decision,
                        )
                        if self._search_candidate_matches_company(url, key0, name, domain):
                            return result, candidate

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
                                candidate = self._build_candidate(
                                    source="search",
                                    url=url,
                                    final_url=url,
                                    status_code=200,
                                    ats_family=adapter,
                                    company_domain=domain,
                                )
                                result = DiscoveryResult(
                                    adapter,
                                    f"{p.netloc}{cp}" if adapter == "workday" else key,
                                    url,
                                    "FOUND",
                                    f"{adapter} (Search)",
                                    confidence=candidate.confidence_score,
                                    source="search",
                                    route_decision=choose_extraction_route(ats_family=adapter).decision,
                                )
                                if self._search_candidate_matches_company(url, key, name, domain):
                                    return result, candidate

                v_resp = self.session.get(
                    url,
                    headers=self._get_headers(),
                    timeout=self._request_timeout(None, deadline, default=6),
                    allow_redirects=True,
                )
                candidate = self._build_candidate(
                    source="search",
                    url=url,
                    final_url=v_resp.url or url,
                    status_code=v_resp.status_code,
                    ats_family=fingerprint_ats(v_resp.url or url, html=v_resp.text),
                    redirect_chain=[url, v_resp.url or url],
                    company_domain=domain,
                )
                if v_resp.status_code == 200:
                    result = self._scan_content(v_resp.text, v_resp.url, name)
                    if result:
                        if result.status == "FALLBACK" and not self._search_candidate_matches_company(v_resp.url or url, None, name, domain):
                            return None, candidate
                        result.confidence = max(result.confidence, candidate.confidence_score)
                        result.source = result.source or "search"
                        if not result.route_decision:
                            result.route_decision = choose_extraction_route(ats_family=result.adapter or candidate.ats_family).decision
                    return result, candidate
                if v_resp.status_code == 403 and self._classify_known_ats_host(v_resp.url or url):
                    # Some ATS boards block without a browser; keep the URL as a fallback.
                    result = DiscoveryResult(
                        "generic",
                        None,
                        v_resp.url or url,
                        "FALLBACK",
                        "Potential blocked ATS board",
                        confidence=candidate.confidence_score,
                        source="search",
                        route_decision=choose_extraction_route(ats_family=candidate.ats_family, blocked=True).decision,
                    )
                    return result, candidate
            except Exception:
                pass
            return None, None

        remaining = max(1.0, self._remaining_budget_s(deadline))
        with ThreadPoolExecutor(max_workers=3) as executor:
            futures = [executor.submit(verify_link, l) for l in links]
            try:
                for future in as_completed(futures, timeout=remaining):
                    res, candidate = future.result()
                    if candidate:
                        candidate_hits.append(candidate)
                    if res:
                        verified.append(res)
            except FuturesTimeout:
                executor.shutdown(wait=False, cancel_futures=True)
        if not verified:
            return None

        for result in verified:
            merged_candidates = list(candidate_hits)
            if result.careers_url:
                merged_candidates.append(
                    self._build_candidate(
                        source=result.source or "search",
                        url=result.careers_url,
                        final_url=result.careers_url,
                        status_code=200 if result.status == "FOUND" else None,
                        ats_family=result.adapter or "unknown",
                        company_domain=domain,
                    )
                )
            result.candidates = self._top_candidate_dicts(merged_candidates)
            if not result.route_decision:
                result.route_decision = choose_extraction_route(
                    ats_family=result.adapter or "unknown",
                    blocked=result.status == "BLOCKED",
                    unsupported=result.status == "BLOCKED" and "unsupported" in result.detail.lower(),
                ).decision

        verified.sort(
            key=lambda item: (
                0 if item.status == "FOUND" else 1,
                -(item.confidence or 0.0),
                item.careers_url or "",
            )
        )
        return verified[0]

    def _scan_content(self, html: str, url: str, name: str, domain_hint: str = "") -> Optional[DiscoveryResult]:
        if not self._looks_like_html(html):
            return None
        soup = BeautifulSoup(html, "html.parser")
        company_domain = self._normalize_company_domain(domain_hint) or self._normalize_company_domain(self._url_domain(url))

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
                        if not self._search_candidate_matches_company(src, key, name, company_domain):
                            continue
                        # Verify the key is actually a company board
                        board_url = canonical_board_url(adapter, key, url)
                        res = DiscoveryResult(
                            adapter,
                            key,
                            board_url,
                            "FOUND",
                            f"Embedded {adapter} found in scripts",
                            source="embed_scan",
                            route_decision=choose_extraction_route(ats_family=adapter).decision,
                        )
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
                        target_domain = self._normalize_company_domain(self._url_domain(href))
                        same_site_target = bool(target_domain and company_domain and (target_domain == company_domain or target_domain.endswith(f".{company_domain}")))
                        if not same_site_target and not self._search_candidate_matches_company(href, key, name, company_domain):
                            continue
                        res = DiscoveryResult(
                            adapter,
                            key,
                            href,
                            "FOUND",
                            f"Link or iframe to {adapter} board found",
                            source="embed_scan",
                            route_decision=choose_extraction_route(ats_family=adapter).decision,
                        )
                        if self._validate_discovery(res, name):
                            return res

        # 2b. Prefer same-site listing pages over shallow careers landing pages.
        base_domain = self._url_domain(url)
        listing_links: List[str] = []
        listing_markers = (
            "search-results",
            "search-jobs",
            "/jobs/search",
            "/openings",
            "/positions",
            "/job-search",
        )
        for anchor in soup.find_all("a", href=True):
            href = anchor.get("href") or ""
            if not href:
                continue
            full_url = urljoin(url, href)
            if self._url_domain(full_url) != base_domain:
                continue
            full_url_l = full_url.lower()
            if any(marker in full_url_l for marker in listing_markers):
                listing_links.append(full_url)

        for listing_url in list(dict.fromkeys(listing_links))[:3]:
            result = DiscoveryResult(
                "generic",
                None,
                listing_url,
                "FOUND",
                "Internal job listings link found",
                source="internal_link",
                route_decision=choose_extraction_route(ats_family=fingerprint_ats(listing_url)).decision,
            )
            if self._validate_discovery(result, name):
                return result

        # 3. Fallback to generic if page seems valid
        if self._is_probable_careers_page(html, url, name):
            return DiscoveryResult(
                "generic",
                None,
                url,
                "FALLBACK",
                "Generic career page discovered",
                source="embed_scan",
                route_decision=choose_extraction_route(
                    ats_family=fingerprint_ats(url, html=html),
                    has_jsonld="application/ld+json" in html.lower(),
                ).decision,
            )
            
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

    def _candidate_reason_flags(self, url: str, ats_family: str, source: str) -> List[str]:
        flags: List[str] = []
        url_l = str(url or "").lower()
        if any(token in url_l for token in self.GENERIC_POSITIVE_URL_MARKERS):
            flags.append("careersish_path")
        if ats_family and ats_family != "unknown":
            flags.append("ats_host")
        if "search" in str(source or "").lower():
            flags.append("search_discovered")
        if self._url_is_unsupported_ats(url):
            flags.append("unsupported_ats")
        return flags

    def _build_candidate(
        self,
        *,
        source: str,
        url: str,
        final_url: str,
        status_code: Optional[int],
        ats_family: str,
        redirect_chain: Optional[List[str]] = None,
        company_domain: str = "",
        extra_flags: Optional[List[str]] = None,
    ) -> CandidateJobURL:
        flags = self._candidate_reason_flags(final_url or url, ats_family, source)
        if extra_flags:
            flags.extend([flag for flag in extra_flags if flag and flag not in flags])
        candidate = CandidateJobURL(
            source=source,
            url=url,
            final_url=final_url or url,
            status_code=status_code,
            redirect_chain=list(redirect_chain or [url, final_url or url]),
            ats_family=ats_family or "unknown",
            reason_flags=flags,
        )
        candidate.confidence_score = score_candidate_url(
            candidate.final_url,
            source=source,
            ats_family=candidate.ats_family,
            status_code=status_code,
            company_domain=company_domain,
            reason_flags=candidate.reason_flags,
        )
        return candidate

    def _top_candidate_dicts(self, candidates: List[CandidateJobURL], limit: int = 3) -> List[Dict[str, Any]]:
        return [candidate.to_dict() for candidate in rank_candidates(candidates, limit=limit)]
