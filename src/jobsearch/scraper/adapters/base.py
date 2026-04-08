from typing import List, Optional, Dict, Any
from datetime import datetime
from abc import ABC, abstractmethod
from jobsearch.scraper.models import Job, RejectedJob


class BlockedSiteError(RuntimeError):
    def __init__(self, url: str, reason: str):
        super().__init__(reason)
        self.url = url
        self.reason = reason


class BaseAdapter(ABC):
    def __init__(self, session=None, scorer=None):
        self.session = session
        self.scorer = scorer
        self.timeout = 30 # Default 30s timeout for all network calls
        self.known_urls: Dict[str, datetime] = {}

    def set_known_urls(self, urls: Dict[str, datetime]):
        """Inject known URLs from the database to support incremental scraping."""
        self.known_urls = urls

    @abstractmethod
    def scrape(self, company_config: Dict[str, Any]) -> List[Job]:
        """Scrape jobs for the given company."""
        pass

    def fetch_json(self, url: str, timeout: float | None = None) -> Dict[str, Any]:
        """Helper to fetch JSON data."""
        response = self._request("get", url, timeout=timeout)
        return response.json()

    def fetch_json_post(
        self,
        url: str,
        payload: Dict[str, Any],
        referer: str = None,
        timeout: float | None = None,
    ) -> Dict[str, Any]:
        """Helper to fetch JSON data via POST."""
        headers = {"Content-Type": "application/json"}
        if referer:
            headers["Referer"] = referer

        response = self._request("post", url, json=payload, headers=headers, timeout=timeout)
        return response.json()

    def fetch_text(self, url: str, timeout: float | None = None) -> str:
        """Helper to fetch HTML/text data."""
        response = self._request("get", url, timeout=timeout)
        return response.text

    def _request(self, method: str, url: str, **kwargs):
        from jobsearch.config.settings import settings
        if not self.session:
            import requests
            session = requests
        else:
            session = self.session

        timeout = kwargs.pop("timeout", None)
        
        # Apply global proxies if configured
        if settings.proxies and "proxies" not in kwargs:
            proxy_val = settings.proxies.strip()
            if "://" in proxy_val:
                kwargs["proxies"] = {"http": proxy_val, "https": proxy_val}
            else:
                # Assume it might be a comma-separated list or just one host
                # For simplicity, we'll support the standard requests proxy dict format 
                # or a single URL.
                kwargs["proxies"] = {"http": proxy_val, "https": proxy_val}

        response = getattr(session, method)(url, timeout=timeout or self.timeout, **kwargs)
        self._raise_if_blocked(response, url)
        return response

    def _raise_if_blocked(self, response, url: str) -> None:
        status = int(getattr(response, "status_code", 0) or 0)
        text = (getattr(response, "text", "") or "")[:4000].lower()
        final_url = str(getattr(response, "url", "") or url)
        blocked_markers = {
            "sorry, you have been blocked": "Cloudflare block page",
            "unable to access happydance.website": "HappyDance anti-bot block",
            "attention required!": "Cloudflare challenge page",
            "cloudflare ray id": "Cloudflare challenge page",
            "forbidden request": "Forbidden request",
            "access denied": "Access denied",
        }

        for marker, reason in blocked_markers.items():
            if marker in text:
                raise BlockedSiteError(final_url, reason)

        if "happydance.website" in final_url.lower():
            raise BlockedSiteError(final_url, "HappyDance anti-bot block")

        if status in {401, 403, 429}:
            raise BlockedSiteError(final_url, f"HTTP {status} blocked response")

    def workday_context(self, careers_url: str):
        from urllib.parse import urlparse
        import re
        normalized = careers_url if careers_url.startswith("http") else f"https://{careers_url.lstrip('/')}"
        parsed = urlparse(normalized)
        host = parsed.netloc.split(":")[0]
        tenant = host.split(".")[0]
        raw_segments = [seg for seg in parsed.path.split("/") if seg]
        segments = []
        for seg in raw_segments:
            if re.fullmatch(r"[a-z]{2}(?:-[A-Z]{2})?", seg):
                continue
            if seg.lower() in {"job", "jobs", "search-results", "apply"}:
                continue
            segments.append(seg)
        
        # Build a robust list of candidate site names
        base_variants = ["External", "Careers", "Search", "Jobs", "External_Careers"]
        tenant_variants = []
        if tenant:
            t_cap = tenant.capitalize()
            tenant_variants = [
                tenant, t_cap,
                f"{tenant}Careers", f"{t_cap}Careers",
                f"{tenant}_Careers", f"{t_cap}_Careers",
                f"{tenant}Investments", f"{t_cap}Investments",
                f"{tenant}_External_Careers", f"{t_cap}_External_Careers",
                f"{tenant}External", f"{t_cap}External"
            ]
        
        # Combine all candidates, preserving order and uniqueness
        seen = set()
        sites = []
        for s in segments + base_variants + tenant_variants:
            if s and s not in seen:
                sites.append(s)
                seen.add(s)
        
        return host, tenant, sites
