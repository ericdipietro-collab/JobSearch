from __future__ import annotations
import logging
import re
from typing import List, Dict, Any, Optional
from urllib.parse import urljoin

from .base import BaseAdapter
from jobsearch.scraper.models import Job
from jobsearch.scraper.normalization import WorkTypeNormalizer, SourceLaneRegistry

logger = logging.getLogger(__name__)

class DiceAdapter(BaseAdapter):
    """Adapter for Dice.com (High-yield technical board)."""

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.base_url = "https://www.dice.com"

    def scrape(self, company_name: str, **kwargs) -> List[Job]:
        """Scrapes Dice.com for a specific company."""
        # Note: In a real implementation, this would use a search URL.
        # For this pass, we'll implement the parser logic.
        search_url = f"https://www.dice.com/jobs?q={company_name.replace(' ', '+')}"
        html = self.fetch_text(search_url)
        if not html:
            return []
            
        return self.parse_jobs(html, company_name)

    def parse_jobs(self, html: str, company_name: str) -> List[Job]:
        from bs4 import BeautifulSoup
        soup = BeautifulSoup(html, "html.parser")
        jobs = []
        
        # Dice uses 'card' or 'search-card' classes
        cards = soup.select(".card, .search-card, [data-testid='card']")
        for card in cards:
            try:
                title_el = card.select_one(".card-title-link, a[id^='job-title']")
                if not title_el: continue
                
                title = title_el.get_text(strip=True)
                url = urljoin(self.base_url, title_el.get('href', ''))
                
                # Dice often shows the company in a specific span
                comp_el = card.select_one(".card-company, [data-testid='company-name']")
                found_co = comp_el.get_text(strip=True) if comp_el else company_name
                
                # Location
                loc_el = card.select_one(".card-location, .search-result-location")
                location = loc_el.get_text(strip=True) if loc_el else ""
                
                # Dice has specific labels for remote
                is_remote = "remote" in location.lower() or "remote" in title.lower()
                
                # Employment type (Dice uses specific labels like 'Contracts', 'Full-time')
                type_el = card.select_one(".card-type, .search-result-type")
                raw_type = type_el.get_text(strip=True) if type_el else ""
                normalized_type = WorkTypeNormalizer.normalize(raw_type)

                job = Job(
                    company=found_co,
                    role_title_raw=title,
                    url=url,
                    location=location,
                    is_remote=is_remote,
                    work_type=normalized_type,
                    source="dice",
                    source_lane=SourceLaneRegistry.LANE_SPECIALTY_BOARD
                )
                jobs.append(job)
            except Exception as e:
                logger.debug("DiceAdapter failed to parse a card: %s", e)
                
        return jobs
