from __future__ import annotations

import hashlib
import re
from typing import Any, Dict, List
from urllib.parse import urljoin

from bs4 import BeautifulSoup

from .base import BaseAdapter
from jobsearch.scraper.models import Job


class MotionRecruitmentAdapter(BaseAdapter):
    DETAIL_RE = re.compile(r"/tech-jobs/[^/]+/contract/[^/]+/\d+$", re.IGNORECASE)

    def scrape(self, company_config: Dict[str, Any]) -> List[Job]:
        careers_url = company_config.get("careers_url")
        if not careers_url:
            return []

        html = self.fetch_text(careers_url)
        if not html:
            return []

        soup = BeautifulSoup(html, "html.parser")
        company_name = company_config.get("name", "Motion Recruitment Contract")
        jobs: List[Job] = []
        seen_urls: set[str] = set()

        for anchor in soup.find_all("a", href=True):
            href = anchor["href"]
            full_url = urljoin(careers_url, href)
            if not self.DETAIL_RE.search(full_url):
                continue
            if full_url in seen_urls:
                continue

            title = self._extract_title(anchor)
            if not title:
                continue

            seen_urls.add(full_url)
            job_id = hashlib.md5(f"{company_name}{title}{full_url}".encode()).hexdigest()
            jobs.append(
                Job(
                    id=job_id,
                    company=company_name,
                    role_title_raw=title,
                    location=self._extract_location(anchor),
                    url=full_url,
                    source="Motion Recruitment",
                    adapter="motionrecruitment",
                    tier=str(company_config.get("tier", 4)),
                    description_excerpt=title,
                    work_type="w2_contract",
                )
            )

        return jobs

    def _extract_title(self, anchor) -> str:
        pieces = [
            anchor.get_text(" ", strip=True),
            anchor.get("title", ""),
            anchor.get("aria-label", ""),
        ]
        parent = anchor.parent
        if parent and getattr(parent, "name", "") not in {"body", "html"}:
            pieces.append(parent.get("aria-label", ""))
        title = re.sub(r"\s+", " ", " ".join(piece for piece in pieces if piece)).strip(" -:\u2014")
        if not title or "contract jobs" in title.lower():
            return ""
        return title

    def _extract_location(self, anchor) -> str:
        parent = anchor.parent
        if not parent or getattr(parent, "name", "") in {"body", "html"}:
            return ""
        nearby = parent.get_text(" ", strip=True)
        match = re.search(r"\b(Remote|[A-Z][a-z]+,\s*[A-Z]{2})\b", nearby)
        return match.group(1) if match else ""
