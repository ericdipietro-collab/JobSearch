from typing import List, Dict, Any
import hashlib
import logging
import re
from urllib.parse import urljoin

from bs4 import BeautifulSoup

from .base import BaseAdapter
from jobsearch.scraper.models import Job
from jobsearch.scraper.jsonld_extractor import jsonld_jobs_to_canonical

logger = logging.getLogger(__name__)


class GenericAdapter(BaseAdapter):
    # ATS platform markers — if found in the fetched HTML for a generic-adapter
    # company, that company's adapter field is wrong and should be corrected by
    # the healer. Logged at WARNING so ops can spot silent coverage gaps.
    _ATS_EMBED_SIGNALS = [
        ("greenhouse", "greenhouse.io"),
        ("greenhouse", "grnh.js"),
        ("lever", "lever.co"),
        ("ashby", "ashbyhq.com"),
        ("workday", "myworkdayjobs.com"),
        ("rippling", "rippling.com"),
        ("smartrecruiters", "smartrecruiters.com"),
        ("bamboohr", "bamboohr.com"),
        ("workable", "workable.com"),
        ("breezy", "breezy.hr"),
        ("icims", "icims.com"),
    ]
    STRONG_PATH_SIGNALS = [
        "/job/",
        "/jobs/",
        "/job-posting/",
        "/job-details/",
        "/jobdetail/",
        "/positions/",
        "/position/",
        "/openings/",
        "/opening/",
        "/vacancies/",
        "/vacancy/",
        "/careers/",
        "/career/",
        "/roles/",
        "/role/",
        "/requisitions/",
        "/requisition/",
        "/search-jobs/",
        "/join-us/",
    ]
    JOB_WORDS = {
        "accountant", "administrator", "advisor", "analyst", "architect", "associate",
        "consultant", "coordinator", "designer", "developer", "engineer", "lead",
        "manager", "officer", "owner", "partner", "principal", "producer", "product",
        "program", "project", "recruiter", "scientist", "software", "solution",
        "solutions", "specialist", "strategist", "supervisor", "systems", "technical",
        "technician", "writer",
    }
    STRONG_ROLE_WORDS = {
        "accountant", "administrator", "advisor", "analyst", "architect", "associate",
        "consultant", "coordinator", "designer", "developer", "engineer", "lead",
        "manager", "officer", "owner", "principal", "producer", "program", "project",
        "recruiter", "scientist", "specialist", "strategist", "supervisor", "technician", "writer",
    }
    NOISE_WORDS = {
        "apply", "learn", "details", "read", "more", "view", "opening", "opportunity",
        "career", "job", "jobs", "position", "positions", "role", "roles", "team",
    }
    WEAK_FUNCTION_WORDS = {
        "platform", "operations", "business", "data", "security", "sales", "marketing",
        "customer", "product", "solution", "solutions", "technical", "finance", "financial",
        "governance", "integration", "integrations", "investor", "asset",
    }
    CONTRACT_MARKERS = {
        "contract", "contractor", "w2", "1099", "c2c", "corp-to-corp", "hourly", "/contract/"
    }
    CONTRACT_SOURCE_NOISE = {
        "jobs directory", "work at dice", "browse jobs", "specialties",
        "project / program management project / program management",
    }
    PAGE_OPENING_MARKERS = {
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
    }
    PAGE_NO_OPENING_MARKERS = {
        "no current openings",
        "no open positions",
        "no open roles",
        "there are no openings",
        "no opportunities available",
    }

    def scrape(self, company_config: Dict[str, Any]) -> List[Job]:
        careers_url = company_config.get("careers_url")
        if not careers_url:
            return []

        discovery_urls = company_config.get("discovery_urls") or [careers_url]
        jobs: List[Job] = []
        seen_urls = set()

        for url in discovery_urls:
            try:
                html = self.fetch_text(url)
                if not html:
                    continue

                detected_ats = self._detect_embedded_ats(html)
                if detected_ats:
                    logger.warning(
                        "generic-adapter mismatch | company=%s url=%s detected_ats=%s — "
                        "update adapter field and re-run healer to fix coverage",
                        company_config.get("name", "?"), url, detected_ats,
                    )

                company_name = company_config.get("name", "Unknown")
                jsonld_jobs = jsonld_jobs_to_canonical(
                    html,
                    base_url=url,
                    company_name=company_name,
                    adapter="generic",
                    tier=company_config.get("tier", 4),
                    source="Generic JSON-LD",
                )
                if jsonld_jobs:
                    for job in jsonld_jobs:
                        if job.url in seen_urls:
                            continue
                        seen_urls.add(job.url)
                        jobs.append(job)
                    continue

                soup = BeautifulSoup(html, "html.parser")
                if not self._page_looks_like_openings_page(url, soup, html, company_config):
                    continue

                for anchor in soup.find_all("a", href=True):
                    href = anchor["href"]
                    title = self._extract_link_text(anchor)
                    if not self._is_probable_job_link(title, href, company_config):
                        continue

                    full_url = urljoin(url, href)
                    if full_url in seen_urls:
                        continue
                    seen_urls.add(full_url)

                    location = self._guess_location(title)
                    job_id = hashlib.md5(f"{company_name}{title}{full_url}".encode()).hexdigest()

                    jobs.append(
                        Job(
                            id=job_id,
                            company=company_name,
                            role_title_raw=title,
                            location=location,
                            url=full_url,
                            source="Web Scraper",
                            adapter="generic",
                            tier=str(company_config.get("tier", 4)),
                            description_excerpt=title,
                            work_type="w2_contract" if company_config.get("contractor_source") else "",
                        )
                    )
            except Exception:
                continue

        return jobs

    def _detect_embedded_ats(self, html: str) -> str:
        """Return the name of an embedded ATS if its marker is found in the HTML, else ''."""
        html_lower = html.lower()
        for ats_name, marker in self._ATS_EMBED_SIGNALS:
            if marker in html_lower:
                return ats_name
        return ""

    def _page_looks_like_openings_page(self, url: str, soup: BeautifulSoup, html: str, company_config: Dict[str, Any]) -> bool:
        if company_config.get("contractor_source"):
            return True

        url_l = url.lower()
        if any(token in url_l for token in self.STRONG_PATH_SIGNALS):
            return True
        if any(token in url_l for token in ("/careers", "/career", "/jobs", "/join-us", "/openings", "/positions", "/roles")):
            return True

        text_l = soup.get_text(" ", strip=True).lower()[:12000]
        if any(marker in text_l for marker in self.PAGE_OPENING_MARKERS):
            return True
        if any(marker in text_l for marker in self.PAGE_NO_OPENING_MARKERS):
            return False
        return False

    def _extract_link_text(self, anchor) -> str:
        parts = [
            anchor.get_text(" ", strip=True),
            anchor.get("title", ""),
            anchor.get("aria-label", ""),
            anchor.get("data-title", ""),
        ]
        parent = anchor.parent
        if parent:
            parts.extend(
                [
                    parent.get("aria-label", ""),
                    parent.get("title", ""),
                ]
            )
            nearby = parent.get_text(" ", strip=True)
            if nearby and len(nearby) <= 160:
                parts.append(nearby)

        combined = " ".join(part for part in parts if part)
        combined = re.sub(r"\bview details for\b", "", combined, flags=re.IGNORECASE)
        combined = re.sub(r"\s+", " ", combined).strip(" -:\u2014")
        return combined

    def _is_probable_job_link(self, title: str, href: str, company_config: Dict[str, Any] | None = None) -> bool:
        title_l = title.lower()
        href_l = href.lower()
        if company_config and company_config.get("contractor_source"):
            if not self._is_probable_contractor_link(title_l, href_l):
                return False

        title_blacklist = [
            "pricing", "feature", "blog", "press",
            "about us", "our team", "leadership", "contact", "privacy",
            "terms", "cookies", "login", "sign in", "sign up", "demo", "resource",
            "event", "webinar", "news", "investor", "partner", "legal", "security",
        ]
        href_blacklist = title_blacklist + [
            "product", "customer", "company/", "/company", "governance",
            "leadership", "about-", "about/", "/about", "/solutions/", "/solution/",
            "/segments/", "/segment/", "/platform/", "/integrations/", "/integration/",
        ]
        if any(token in title_l for token in title_blacklist) or any(token in href_l for token in href_blacklist):
            return False

        if any(token in href_l for token in self.STRONG_PATH_SIGNALS):
            if not title_l or len(title_l.split()) < 2:
                return False
            if self._looks_like_job_title(title_l):
                return True
            return self._has_structured_job_path(href_l)

        words = title_l.split()
        if len(words) < 2 or len(words) > 15:
            return False
        if len(title_l) > 120 or "|" in title_l or "copyright" in title_l:
            return False

        return self._looks_like_job_title(title_l)

    def _is_probable_contractor_link(self, title: str, href: str) -> bool:
        if any(noise in title for noise in self.CONTRACT_SOURCE_NOISE):
            return False
        if "browse-jobs" in href or "?specialties=" in href or "specialties=" in href:
            return False
        if href.count("/") <= 3 and "/job-detail/" not in href:
            return False
        if "/job-detail/" in href:
            return True
        if "motionrecruitment.com" in href:
            if "/tech-jobs/" in href and re.search(r"/tech-jobs/[^/]+/contract/[^/]+/\d+$", href):
                return True
            return False
        has_contract_marker = any(marker in title or marker in href for marker in self.CONTRACT_MARKERS)
        return has_contract_marker and self._has_structured_job_path(href)

    def _has_structured_job_path(self, href: str) -> bool:
        tokens = [token for token in re.split(r"[/_\-?&=]+", href) if token]
        noisy = {"careers", "career", "jobs", "job", "positions", "position", "role", "roles"}
        informative_tokens = [token for token in tokens if token not in noisy]
        return len(informative_tokens) >= 2

    def _looks_like_job_title(self, title: str) -> bool:
        words = [word for word in re.findall(r"[a-zA-Z][a-zA-Z+&/-]*", title) if word]
        if len(words) < 2:
            return False

        if all(word in self.NOISE_WORDS for word in words[:3]):
            return False
        if any(word in self.STRONG_ROLE_WORDS for word in words):
            return True
        if sum(1 for word in words if word in self.JOB_WORDS) >= 2 and any(word in {"manager", "architect", "analyst", "consultant", "engineer"} for word in words):
            return True

        seniority_words = {"senior", "staff", "principal", "junior", "sr", "lead", "head"}
        if any(word in seniority_words for word in words) and len(words) >= 3:
            if any(word in self.STRONG_ROLE_WORDS for word in words):
                return True

        weak_hits = sum(1 for word in words if word in self.WEAK_FUNCTION_WORDS)
        return weak_hits >= 3 and any(word in self.STRONG_ROLE_WORDS for word in words)

    def _guess_location(self, text: str) -> str:
        match = re.search(r"\b([A-Z][a-z]+,\s*[A-Z]{2})\b", text)
        if match:
            return match.group(1)
        if "Remote" in text:
            return "Remote"
        return ""
