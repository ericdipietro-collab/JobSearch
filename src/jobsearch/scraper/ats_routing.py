from __future__ import annotations

from dataclasses import dataclass, field
from typing import Iterable, List, Optional
from urllib.parse import urlparse


SUPPORTED_ATS_FAMILIES = {
    "greenhouse",
    "lever",
    "ashby",
    "smartrecruiters",
    "workday",
    "icims",
    "taleo",
    "successfactors",
    "recruitee",
    "bamboohr",
    "jobvite",
    "phenom",
    "eightfold",
    "avature",
    "workable",
    "breezy",
    "rippling",
    "custom",
    "unknown",
}


@dataclass
class CandidateJobURL:
    source: str
    url: str
    final_url: str
    status_code: int | None = None
    redirect_chain: List[str] = field(default_factory=list)
    ats_family: str = "unknown"
    confidence_score: float = 0.0
    reason_flags: List[str] = field(default_factory=list)

    def to_dict(self) -> dict:
        return {
            "source": self.source,
            "url": self.url,
            "final_url": self.final_url,
            "status_code": self.status_code,
            "redirect_chain": list(self.redirect_chain),
            "ats_family": self.ats_family,
            "confidence_score": round(float(self.confidence_score or 0.0), 3),
            "reason_flags": list(self.reason_flags),
        }


@dataclass
class ExtractionRoute:
    ats_family: str
    decision: str
    extraction_methods: List[str] = field(default_factory=list)
    reason: str = ""

    def to_dict(self) -> dict:
        return {
            "ats_family": self.ats_family,
            "decision": self.decision,
            "extraction_methods": list(self.extraction_methods),
            "reason": self.reason,
        }


class FailureClassifier:
    """Classifies scraping failures into actionable buckets."""
    WRONG_URL = "wrong_url"
    NO_OPENINGS = "no_openings"
    SELECTOR_MISS = "selector_miss"
    HIDDEN_API_NOT_PARSED = "hidden_api_not_parsed"
    BLOCKED = "blocked_bot_protection"
    AUTH_REQUIRED = "auth_required"
    GEO_GATE = "geo_or_cookie_gate"
    BROKEN_SITE = "broken_site"
    STALE_URL = "stale_url"
    UNSUPPORTED_ATS = "unsupported_ats"
    UNKNOWN = "unknown"

    @classmethod
    def classify(
        cls,
        status_code: int | None,
        html: str = "",
        jobs_found: int = 0,
        ats_family: str = "unknown",
        extraction_method: str = "",
        final_url: str = "",
    ) -> str:
        if jobs_found > 0:
            return ""
        
        # 404 on a known ATS family usually means the URL is stale/invalid
        if status_code == 404 and ats_family != "unknown":
            return cls.STALE_URL
            
        if status_code in {403, 429}:
            return cls.BLOCKED
        if status_code == 401:
            return cls.AUTH_REQUIRED
        if status_code and status_code >= 400 and status_code != 404:
            return cls.BROKEN_SITE
        
        html_l = html.lower()
        # Bot protection markers
        if any(t in html_l for t in [
            "cloudflare", "distilnetworks", "captcha", "security check", 
            "bot detection", "access denied", "please verify you are a human",
            "incapsula", "akamai", "perimeterx", "datadome",
        ]):
            return cls.BLOCKED
            
        # Auth markers
        if any(t in html_l for t in ["login", "sign in", "password", "authenticate"]) and len(html_l) < 8000:
            return cls.AUTH_REQUIRED
            
        # Cookie/Geo gate
        if any(t in html_l for t in ["accept cookies", "cookie policy", "choose your region"]) and len(html_l) < 3000:
            return cls.GEO_GATE
        
        # Check for "no openings" text
        no_jobs_markers = [
            "no open positions", "no current openings", "check back later",
            "no jobs found", "no vacancies", "0 jobs", "0 positions",
            "search returned 0", "not hiring at the moment", "no opportunities",
            "we don't have any openings", "currently no open roles"
        ]
        if any(m in html_l for m in no_jobs_markers):
            return cls.NO_OPENINGS
        
        if ats_family == "unknown" and extraction_method in {"dom_render", "dom_anchor"}:
            return cls.SELECTOR_MISS
        
        if extraction_method == "network_interception" and jobs_found == 0:
            return cls.HIDDEN_API_NOT_PARSED
            
        if status_code == 404 and jobs_found == 0:
            return cls.WRONG_URL

        return cls.UNKNOWN


def fingerprint_ats(
    url: str,
    *,
    html: str = "",
    script_urls: Optional[Iterable[str]] = None,
    response_urls: Optional[Iterable[str]] = None,
) -> str:
    host = (urlparse(str(url or "")).netloc or "").lower()
    url_blob = "\n".join(
        part
        for part in [
            host,
            str(url or "").lower(),
            *[str(extra or "").lower() for extra in (script_urls or [])],
            *[str(extra or "").lower() for extra in (response_urls or [])],
        ]
        if part
    )
    html_blob = str(html or "").lower()

    # Domain/URL-based markers
    checks = [
        ("greenhouse", ("greenhouse.io", "boards-api.greenhouse.io", "grnh.se", "grnh.js")),
        ("lever", ("lever.co", "api.lever.co")),
        ("ashby", ("jobs.ashbyhq.com", "api.ashbyhq.com", "ashbyhq.com/widget", "ashbyhq.com/embed")),
        ("smartrecruiters", ("smartrecruiters.com", "api.smartrecruiters.com")),
        ("workday", ("myworkdayjobs.com", ".wd1.myworkdayjobs.com", ".wd3.myworkdayjobs.com", ".wd5.myworkdayjobs.com", "/wday/cxs/")),
        ("icims", ("icims.com",)),
        ("taleo", ("taleo.net", "oraclecloud.com", "candidateexperience")),
        ("successfactors", ("successfactors.com", "jobs.sap.com")),
        ("recruitee", ("recruitee.com", "recruitee.net")),
        ("bamboohr", ("bamboohr.com",)),
        ("jobvite", ("jobvite.com",)),
        ("phenom", ("phenompeople", "phenom-pro", "/phenom/", "phenom.js")),
        ("eightfold", ("eightfold.ai", "cultivate.eightfold.ai")),
        ("avature", ("avature.net", "avature",)),
        ("workable", ("workable.com",)),
        ("breezy", ("breezy.hr",)),
        ("rippling", ("rippling.com",)),
    ]
    for family, markers in checks:
        if any(marker in url_blob for marker in markers):
            return family

    # HTML-based markers (meta tags, script tags, etc.)
    html_checks = [
        ("greenhouse", ("boards-api.greenhouse.io", "grnh.se", "grnh.js", "boards.greenhouse.io")),
        ("lever", ("api.lever.co", "jobs.lever.co")),
        ("ashby", ("jobs.ashbyhq.com", "api.ashbyhq.com", "ashbyhq.com/widget", "ashbyhq.com/embed")),
        ("smartrecruiters", ("api.smartrecruiters.com", "careers.smartrecruiters.com")),
        ("workday", ("myworkdayjobs.com", "/wday/cxs/", "workday-logo")),
        ("icims", ("icims.com", "icims_player")),
        ("taleo", ("taleo.net", "candidateexperience")),
        ("successfactors", ("successfactors.com", "jobs.sap.com")),
        ("recruitee", ("recruitee.com", "recruitee.net")),
        ("bamboohr", ("bamboohr.com",)),
        ("jobvite", ("jobvite.com",)),
        ("phenom", ("phenompeople", "phenom-pro", "phenom.js", "phenom-logo", "phapp.ddo")),
        ("eightfold", ("eightfold.ai", "cultivate.eightfold.ai")),
        ("avature", ("avature.net",)),
        ("workable", ("workable.com",)),
        ("breezy", ("breezy.hr",)),
        ("rippling", ("rippling.com", "rippling-logo")),
    ]
    for family, markers in html_checks:
        if any(marker in html_blob for marker in markers):
            return family
    
    # Generic markers for potential JSON-LD or structured boards
    if any(token in (url_blob + "\n" + html_blob) for token in ("jobposting", "application/ld+json", "/careers", "/jobs")):
        return "custom"
        
    return "unknown"


def score_candidate_url(
    url: str,
    *,
    source: str,
    ats_family: str = "unknown",
    status_code: int | None = None,
    company_domain: str = "",
    reason_flags: Optional[Iterable[str]] = None,
) -> float:
    host = (urlparse(str(url or "")).netloc or "").lower()
    path = (urlparse(str(url or "")).path or "").lower()
    score = 0.15
    source_l = str(source or "").lower()
    if source_l in {"provided_url", "existing_url"}:
        score += 0.35
    elif "search" in source_l:
        score += 0.22
    elif "sitemap" in source_l:
        score += 0.25
    elif "internal" in source_l:
        score += 0.20
    elif "redirect" in source_l:
        score += 0.18

    if any(token in path for token in ("/careers", "/jobs", "/join-us", "/openings", "/positions", "/work-with-us")):
        score += 0.18
    if company_domain:
        domain = company_domain.lower().lstrip(".")
        if host == domain or host.endswith("." + domain):
            score += 0.14
    if ats_family != "unknown":
        score += 0.18
    if status_code == 200:
        score += 0.08
    elif status_code in {301, 302, 307, 308}:
        score += 0.03
    elif status_code and status_code >= 400:
        score -= 0.10

    for flag in reason_flags or []:
        if "blocked" in flag or "unsupported" in flag:
            score -= 0.20
        elif "careersish" in flag or "ats_host" in flag:
            score += 0.05

    return max(0.0, min(score, 1.0))


def rank_candidates(candidates: Iterable[CandidateJobURL], limit: int = 3) -> List[CandidateJobURL]:
    unique: dict[tuple[str, str], CandidateJobURL] = {}
    for candidate in candidates:
        key = ((candidate.final_url or candidate.url).lower(), candidate.ats_family.lower())
        existing = unique.get(key)
        if existing is None or candidate.confidence_score > existing.confidence_score:
            unique[key] = candidate
    ranked = sorted(
        unique.values(),
        key=lambda item: (
            -(item.confidence_score or 0.0),
            0 if (item.status_code == 200) else 1,
            item.final_url or item.url,
        ),
    )
    return ranked[: max(1, int(limit or 1))]


def choose_extraction_route(
    *,
    ats_family: str,
    has_jsonld: bool = False,
    has_hidden_api: bool = False,
    blocked: bool = False,
    unsupported: bool = False,
) -> ExtractionRoute:
    family = ats_family if ats_family in SUPPORTED_ATS_FAMILIES else "unknown"
    if blocked:
        return ExtractionRoute(family, "blocked", ["classify_failure"], "Board appears blocked")
    if unsupported:
        return ExtractionRoute(family, "unsupported", ["classify_failure"], "Unsupported ATS family")
    
    # Priority 1: Direct API (fastest, most reliable)
    if family in {"greenhouse", "lever", "ashby", "smartrecruiters", "workday", "bamboohr", "jobvite", "workable", "breezy", "rippling"}:
        methods = ["direct_api"]
        if has_jsonld:
            methods.append("jsonld")
        methods.extend(["network_interception", "dom_render", "classify_failure"])
        return ExtractionRoute(
            family,
            "direct_api",
            methods,
            "Known ATS with structured endpoint support",
        )
    
    # Priority 2: Intercepted API (for JS-heavy boards)
    if has_hidden_api or family in {"phenom", "eightfold", "avature", "icims"}:
        methods = ["network_interception"]
        if has_jsonld:
            methods.append("jsonld")
        methods.extend(["dom_render", "classify_failure"])
        return ExtractionRoute(
            family,
            "intercepted_api",
            methods,
            "Likely JavaScript-fed board with hidden API",
        )
    
    # Priority 3: JSON-LD (Standardized structured data)
    if has_jsonld:
        return ExtractionRoute(
            family, 
            "jsonld", 
            ["jsonld", "network_interception", "dom_render", "classify_failure"], 
            "Structured JobPosting markup detected"
        )
    
    # Priority 4: DOM Render (Fallback)
    return ExtractionRoute(
        family, 
        "dom_render", 
        ["network_interception", "dom_render", "classify_failure"], 
        "Fallback rendered extraction"
    )
