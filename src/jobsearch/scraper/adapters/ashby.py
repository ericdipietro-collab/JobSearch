from typing import List, Dict, Any
import hashlib
import re
from urllib.parse import urljoin

from bs4 import BeautifulSoup

from .base import BaseAdapter
from jobsearch.scraper.models import Job


class AshbyAdapter(BaseAdapter):
    def scrape(self, company_config: Dict[str, Any]) -> List[Job]:
        adapter_key = company_config.get("adapter_key")
        careers_url = company_config.get("careers_url") or ""
        if not adapter_key and not careers_url:
            return []

        api_jobs = self._scrape_api(company_config, adapter_key)
        if api_jobs:
            return api_jobs

        return self._scrape_html(company_config, careers_url)

    def _scrape_api(self, company_config: Dict[str, Any], adapter_key: str) -> List[Job]:
        if not adapter_key:
            return []

        jobs: List[Job] = []
        company_name = company_config.get("name", "Unknown")
        api_urls = [
            f"https://api.ashbyhq.com/posting-api/job-board/{adapter_key}?includeCompensation=true",
            f"https://api.ashbyhq.com/posting-api/job-board/{adapter_key}?includeCompensation=false",
        ]

        for url in api_urls:
            try:
                data = self.fetch_json(url)
            except Exception:
                continue

            raw_jobs = data.get("jobs") if isinstance(data, dict) else None
            if not isinstance(raw_jobs, list):
                continue

            for raw in raw_jobs:
                if not isinstance(raw, dict):
                    continue

                locations = [str(raw.get("location") or "").strip()]
                for item in raw.get("secondaryLocations") or []:
                    if isinstance(item, dict):
                        loc = str(item.get("location") or "").strip()
                        if loc:
                            locations.append(loc)

                unique_locs: List[str] = []
                seen_locs = set()
                for location in locations:
                    if location and location not in seen_locs:
                        unique_locs.append(location)
                        seen_locs.add(location)

                location = " / ".join(unique_locs)
                is_remote = bool(raw.get("isRemote")) or (raw.get("workplaceType") or "").lower() == "remote"
                if is_remote and "remote" not in location.lower():
                    location = f"{location} / Remote" if location else "Remote"

                compensation = raw.get("compensation") if isinstance(raw.get("compensation"), dict) else {}
                description = (raw.get("descriptionHtml") or raw.get("descriptionPlain") or "")
                comp_summary = str(compensation.get("summary") or "").strip()
                if comp_summary:
                    description += " " + comp_summary

                # Extract structured salary from compensation object.
                # Ashby returns compensation.minValue / maxValue with an interval field.
                # Also check nested baseSalary for some API versions.
                salary_min: float | None = None
                salary_max: float | None = None
                for comp_node in [compensation, compensation.get("baseSalary") or {}]:
                    if not isinstance(comp_node, dict):
                        continue
                    interval = str(comp_node.get("interval") or comp_node.get("type") or "").strip().upper()
                    annual = interval in {"YEAR", "YEARLY", "ANNUAL", ""}
                    raw_min = comp_node.get("minValue") or comp_node.get("min")
                    raw_max = comp_node.get("maxValue") or comp_node.get("max")
                    try:
                        cmin = float(raw_min) if raw_min is not None else None
                        cmax = float(raw_max) if raw_max is not None else None
                    except (TypeError, ValueError):
                        cmin = cmax = None
                    if cmin is not None and annual:
                        salary_min = cmin if salary_min is None else salary_min
                        salary_max = cmax if salary_max is None else salary_max
                        break

                title = str(raw.get("title") or "").strip()
                job_url = raw.get("jobUrl") or raw.get("absoluteUrl") or ""
                if not title or not job_url:
                    continue

                job_id = hashlib.md5(f"{company_name}{title}{job_url}".encode()).hexdigest()
                jobs.append(
                    Job(
                        id=job_id,
                        company=company_name,
                        role_title_raw=title,
                        location=location,
                        is_remote=is_remote,
                        url=job_url,
                        source="Ashby",
                        adapter="ashby",
                        tier=str(company_config.get("tier", 4)),
                        description_excerpt=description,
                        salary_text=comp_summary,
                        salary_min=salary_min,
                        salary_max=salary_max,
                    )
                )

            if jobs:
                return jobs

        return []

    def _scrape_html(self, company_config: Dict[str, Any], careers_url: str) -> List[Job]:
        if not careers_url:
            return []

        try:
            html = self.fetch_text(careers_url)
        except Exception:
            return []

        soup = BeautifulSoup(html, "html.parser")
        company_name = company_config.get("name", "Unknown")
        jobs: List[Job] = []
        seen_urls = set()

        for anchor in soup.find_all("a", href=True):
            href = anchor["href"]
            full_url = urljoin(careers_url, href)
            if full_url in seen_urls or "jobs.ashbyhq.com" not in full_url.lower():
                continue

            title = self._extract_title(anchor)
            if not self._looks_like_job_link(title, full_url):
                continue

            seen_urls.add(full_url)
            job_id = hashlib.md5(f"{company_name}{title}{full_url}".encode()).hexdigest()
            jobs.append(
                Job(
                    id=job_id,
                    company=company_name,
                    role_title_raw=title,
                    location="",
                    url=full_url,
                    source="Ashby HTML",
                    adapter="ashby",
                    tier=str(company_config.get("tier", 4)),
                    description_excerpt=title,
                )
            )

        return jobs

    def _extract_title(self, anchor) -> str:
        parts = [
            anchor.get_text(" ", strip=True),
            anchor.get("title", ""),
            anchor.get("aria-label", ""),
        ]
        parent = anchor.parent
        if parent:
            nearby = parent.get_text(" ", strip=True)
            if nearby and len(nearby) <= 160:
                parts.append(nearby)
        return re.sub(r"\s+", " ", " ".join(part for part in parts if part)).strip()

    def _looks_like_job_link(self, title: str, href: str) -> bool:
        title_l = title.lower()
        href_l = href.lower()
        if not title_l or len(title_l.split()) < 2:
            return False
        if any(token in href_l for token in ["/jobs/", "/job/"]):
            return True
        return bool(re.search(r"\b(manager|architect|analyst|consultant|product|principal|lead|specialist|owner)\b", title_l))
