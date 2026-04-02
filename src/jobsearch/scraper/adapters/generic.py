from typing import List, Dict, Any, Optional
from .base import BaseAdapter
from ..models import Job
import hashlib
from bs4 import BeautifulSoup
from urllib.parse import urljoin, urlparse
import re

class GenericAdapter(BaseAdapter):
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
                    
                soup = BeautifulSoup(html, "html.parser")
                company_name = company_config.get("name", "Unknown")
                
                for a in soup.find_all("a", href=True):
                    href = a["href"]
                    title = a.get_text(" ", strip=True)
                    
                    if not self._is_probable_job_link(title, href):
                        continue
                        
                    full_url = urljoin(url, href)
                    if full_url in seen_urls:
                        continue
                    seen_urls.add(full_url)
                    
                    # Try to find location in the link text or nearby
                    location = self._guess_location(a.get_text())
                    
                    job_id = hashlib.md5(f"{company_name}{title}{full_url}".encode()).hexdigest()

                    job = Job(
                        id=job_id,
                        company=company_name,
                        role_title_raw=title,
                        location=location,
                        url=full_url,
                        source="Web Scraper",
                        adapter="generic",
                        tier=str(company_config.get("tier", 4)),
                        description_excerpt=title # Generic adapter often has no easy desc on listing
                    )
                    jobs.append(job)
                    
            except Exception:
                continue
                
        return jobs

    def _is_probable_job_link(self, title: str, href: str) -> bool:
        t = title.lower()
        h = href.lower()
        
        # Negative signals
        if any(x in t for x in ["sign in", "login", "privacy", "cookies", "terms"]):
            return False
            
        # Positive signals in URL
        if any(x in h for x in ["/job/", "/jobs/", "/posting/", "/career/"]):
            return True
            
        # Length check - titles are usually 3-10 words
        words = t.split()
        if len(words) < 2 or len(words) > 15:
            return False
            
        return True

    def _guess_location(self, text: str) -> str:
        # Very simple location guesser
        m = re.search(r"\b([A-Z][a-z]+,\s*[A-Z]{2})\b", text)
        if m:
            return m.group(1)
        if "Remote" in text:
            return "Remote"
        return ""
