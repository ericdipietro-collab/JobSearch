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


def fingerprint_ats(
    url: str,
    *,
    html: str = "",
    script_urls: Optional[Iterable[str]] = None,
    response_urls: Optional[Iterable[str]] = None,
) -> str:
    host = (urlparse(str(url or "")).netloc or "").lower()
    haystacks = [host, str(url or "").lower(), str(html or "").lower()]
    for extra in (script_urls or []):
        haystacks.append(str(extra or "").lower())
    for extra in (response_urls or []):
        haystacks.append(str(extra or "").lower())
    blob = "\n".join(part for part in haystacks if part)

    checks = [
        ("greenhouse", ("greenhouse.io", "boards-api.greenhouse.io", "grnh.se", "grnh.js")),
        ("lever", ("lever.co", "api.lever.co")),
        ("ashby", ("ashbyhq.com", "api.ashbyhq.com")),
        ("smartrecruiters", ("smartrecruiters.com", "api.smartrecruiters.com")),
        ("workday", ("myworkdayjobs.com", "/wday/cxs/")),
        ("icims", ("icims.com",)),
        ("taleo", ("taleo.net", "oraclecloud.com", "candidateexperience")),
        ("successfactors", ("successfactors.com", "jobs.sap.com")),
        ("recruitee", ("recruitee.com", "recruitee.net")),
        ("bamboohr", ("bamboohr.com",)),
        ("jobvite", ("jobvite.com",)),
        ("phenom", ("phenom", "phenompeople", "phenom-pro")), 
        ("eightfold", ("eightfold.ai", "cultivate.eightfold.ai")),
        ("avature", ("avature.net", "avature",)),
        ("workable", ("workable.com",)),
        ("breezy", ("breezy.hr",)),
    ]
    for family, markers in checks:
        if any(marker in blob for marker in markers):
            return family
    if any(token in blob for token in ("jobposting", "application/ld+json", "/careers", "/jobs")):
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
    if family in {"greenhouse", "lever", "ashby", "smartrecruiters", "workday", "bamboohr", "jobvite", "workable", "breezy"}:
        return ExtractionRoute(
            family,
            "direct_api",
            ["direct_api", "jsonld" if has_jsonld else "dom_render"],
            "Known ATS with structured endpoint support",
        )
    if has_hidden_api or family in {"phenom", "eightfold", "avature"}:
        return ExtractionRoute(
            family,
            "intercepted_api",
            ["network_interception", "jsonld" if has_jsonld else "dom_render"],
            "Likely JavaScript-fed board with hidden API",
        )
    if has_jsonld:
        return ExtractionRoute(family, "jsonld", ["jsonld", "dom_render"], "Structured JobPosting markup detected")
    return ExtractionRoute(family, "dom_render", ["dom_render", "classify_failure"], "Fallback rendered extraction")
