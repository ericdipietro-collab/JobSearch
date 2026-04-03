from __future__ import annotations

import hashlib
import re
from typing import Any, Dict, List
from urllib.parse import urljoin

from bs4 import BeautifulSoup

from .base import BaseAdapter
from jobsearch.scraper.models import Job


class DiceAdapter(BaseAdapter):
    def scrape(self, company_config: Dict[str, Any]) -> List[Job]:
        careers_url = company_config.get("careers_url")
        if not careers_url:
            return []

        html = self.fetch_text(careers_url)
        if not html:
            return []

        soup = BeautifulSoup(html, "html.parser")
        company_name = company_config.get("name", "Dice Contract")
        jobs: List[Job] = []
        seen_urls: set[str] = set()

        for anchor in soup.find_all("a", href=True):
            href = anchor["href"]
            full_url = urljoin(careers_url, href)
            if "/job-detail/" not in full_url.lower():
                continue
            if full_url in seen_urls:
                continue

            title = self._extract_title(anchor)
            if not title:
                continue

            seen_urls.add(full_url)
            job_id = hashlib.md5(f"{company_name}{title}{full_url}".encode()).hexdigest()
            location = self._extract_location(anchor)

            jobs.append(
                Job(
                    id=job_id,
                    company=company_name,
                    role_title_raw=title,
                    location=location,
                    url=full_url,
                    source="Dice",
                    adapter="dice",
                    tier=str(company_config.get("tier", 4)),
                    description_excerpt=title,
                    work_type="w2_contract",
                )
            )

        return jobs

    def _extract_title(self, anchor) -> str:
        candidates = [
            anchor.get_text(" ", strip=True),
            anchor.get("title", ""),
            anchor.get("aria-label", ""),
            anchor.get("data-cy", ""),
        ]
        parent = anchor.parent
        if parent and getattr(parent, "name", "") not in {"body", "html"}:
            candidates.append(parent.get_text(" ", strip=True))
        text = " ".join(part for part in candidates if part)
        text = re.sub(r"\bview details for\b", "", text, flags=re.IGNORECASE)
        text = re.sub(r"\s+", " ", text).strip(" -:\u2014")
        if not text or "browse jobs" in text.lower():
            return ""
        return text

    def _extract_location(self, anchor) -> str:
        parent = anchor.parent
        if not parent:
            return ""
        text = parent.get_text(" ", strip=True)
        match = re.search(r"\b(Remote|[A-Z][a-z]+,\s*[A-Z]{2})\b", text)
        return match.group(1) if match else ""
