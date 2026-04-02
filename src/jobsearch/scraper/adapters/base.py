from typing import List, Optional, Dict, Any
from abc import ABC, abstractmethod
from ..models import Job, RejectedJob

class BaseAdapter(ABC):
    def __init__(self, session=None):
        self.session = session

    @abstractmethod
    def scrape(self, company_config: Dict[str, Any]) -> List[Job]:
        """Scrape jobs for the given company."""
        pass

    def fetch_json(self, url: str) -> Dict[str, Any]:
        """Helper to fetch JSON data."""
        if not self.session:
            import requests
            return requests.get(url).json()
        return self.session.get(url).json()

    def fetch_json_post(self, url: str, payload: Dict[str, Any], referer: str = None) -> Dict[str, Any]:
        """Helper to fetch JSON data via POST."""
        headers = {"Content-Type": "application/json"}
        if referer:
            headers["Referer"] = referer
            
        if not self.session:
            import requests
            return requests.post(url, json=payload, headers=headers).json()
        return self.session.post(url, json=payload, headers=headers).json()

    def fetch_text(self, url: str) -> str:
        """Helper to fetch HTML/text data."""
        if not self.session:
            import requests
            return requests.get(url).text
        return self.session.get(url).text

    def workday_context(self, careers_url: str):
        from urllib.parse import urlparse
        import re
        parsed = urlparse(careers_url)
        host = parsed.netloc.split(":")[0]
        tenant = host.split(".")[0]
        raw_segments = [seg for seg in parsed.path.split("/") if seg]
        segments = []
        for seg in raw_segments:
            if re.fullmatch(r"[a-z]{2}(?:-[A-Z]{2})?", seg):
                continue
            if seg.lower() in {"job", "jobs", "search-results"}:
                continue
            segments.append(seg)
        
        # unique preserve
        seen = set()
        sites = []
        for s in segments + [tenant]:
            if s and s not in seen:
                sites.append(s)
                seen.add(s)
        return host, tenant, sites
