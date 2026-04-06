"""Crawl4AI adapter - LLM-driven scraping for unknown career sites."""

from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import os
import re
from typing import Any, Dict, List, Optional
from urllib.parse import urljoin

from .base import BaseAdapter
from jobsearch.scraper.models import Job

logger = logging.getLogger(__name__)

# Schema for LLM extraction of job listings
JOB_SCHEMA = {
    "type": "array",
    "items": {
        "type": "object",
        "properties": {
            "title": {"type": "string", "description": "The job title or position name"},
            "url": {"type": "string", "description": "URL or href of the job listing"},
        },
        "required": ["title", "url"],
    },
}

EXTRACTION_INSTRUCTION = (
    "Extract all job listing links from this page. For each job posting, capture the job title and URL. "
    "Return only actual job opening links — exclude navigation, blog posts, about pages, and other non-job content."
)


class Crawl4AIAdapter(BaseAdapter):
    """LLM-driven web scraper using crawl4ai for complex career pages."""

    def __init__(self, session=None, scorer=None):
        super().__init__(session=session, scorer=scorer)
        self._generic = None  # Lazy fallback to GenericAdapter

    def scrape(self, company_config: Dict[str, Any]) -> List[Job]:
        """Scrape jobs using crawl4ai, with fallback to GenericAdapter."""
        try:
            import crawl4ai  # noqa: F401
        except ImportError:
            logger.debug("crawl4ai not installed, falling back to GenericAdapter")
            return self._fallback(company_config)

        try:
            # Attempt async crawling
            return asyncio.run(self._crawl(company_config))
        except RuntimeError as e:
            # Handle "Event loop is already running" (Streamlit context)
            if "event loop" in str(e).lower():
                try:
                    import nest_asyncio

                    nest_asyncio.apply()
                    loop = asyncio.get_event_loop()
                    return loop.run_until_complete(self._crawl(company_config))
                except Exception as nested_err:
                    logger.warning(
                        f"[crawl4ai] Failed with nest_asyncio: {nested_err} — falling back"
                    )
                    return self._fallback(company_config)
            raise
        except Exception as exc:
            logger.warning(f"[crawl4ai] Extraction failed: {exc} — falling back to GenericAdapter")
            return self._fallback(company_config)

    async def _crawl(self, company_config: Dict[str, Any]) -> List[Job]:
        """Async crawling with HTTP-first strategy, then Playwright fallback."""
        from crawl4ai import AsyncWebCrawler, CrawlerRunConfig
        from crawl4ai.async_configs import HTTPCrawlerConfig, LLMConfig
        from crawl4ai.async_crawler_strategy import AsyncHTTPCrawlerStrategy
        from crawl4ai.extraction_strategy import LLMExtractionStrategy

        careers_url = company_config.get("careers_url")
        if not careers_url:
            return []

        discovery_urls = company_config.get("discovery_urls") or [careers_url]
        jobs: List[Job] = []
        seen_urls = set()

        google_api_key = os.getenv("GOOGLE_API_KEY", "").strip()
        company_name = company_config.get("name", "Unknown")

        for url in discovery_urls:
            try:
                # Build extraction strategy
                if google_api_key:
                    llm_config = LLMConfig(
                        provider="gemini/gemini-2.0-flash",
                        api_token=google_api_key,
                        temperature=0.1,
                    )
                    extraction_strategy = LLMExtractionStrategy(
                        llm_config=llm_config,
                        schema=JOB_SCHEMA,
                        instruction=EXTRACTION_INSTRUCTION,
                        input_format="markdown",
                    )
                else:
                    # No API key — use markdown extraction without LLM
                    logger.debug(
                        f"[crawl4ai] No GOOGLE_API_KEY set. Using markdown-only extraction for {url}"
                    )
                    extraction_strategy = None

                # Try HTTP-only strategy first (fast, no browser)
                http_strategy = AsyncHTTPCrawlerStrategy(
                    browser_config=HTTPCrawlerConfig(
                        verify_ssl=True,
                        headers={"User-Agent": "Mozilla/5.0 (JobSearch)"},
                    )
                )

                async with AsyncWebCrawler(crawler_strategy=http_strategy) as crawler:
                    run_config = CrawlerRunConfig(
                        extraction_strategy=extraction_strategy,
                        word_count_threshold=10,
                        only_text=False,
                    )
                    result = await crawler.arun(url=url, config=run_config)

                # If HTTP failed or no results, retry with default Playwright strategy
                if not result.success or not result.extracted_content:
                    logger.debug(f"[crawl4ai] HTTP strategy failed for {url}, trying Playwright...")
                    async with AsyncWebCrawler() as crawler:
                        result = await crawler.arun(url=url, config=run_config)

                if not result.success:
                    logger.warning(f"[crawl4ai] Both strategies failed for {url}: {result.error_message}")
                    continue

                # Parse extracted content
                jobs_from_url = await self._parse_results(
                    result, url, company_name, company_config
                )
                for job in jobs_from_url:
                    if job.url not in seen_urls:
                        jobs.append(job)
                        seen_urls.add(job.url)

            except Exception as exc:
                logger.warning(f"[crawl4ai] Error crawling {url}: {exc}")
                continue

        return jobs

    async def _parse_results(
        self,
        result: Any,
        base_url: str,
        company_name: str,
        company_config: Dict[str, Any],
    ) -> List[Job]:
        """Parse crawl4ai results into Job objects."""
        jobs = []

        # Prefer fit_markdown (cleaner content without nav/headers) -> extracted_content -> markdown
        extracted_content = ""
        if hasattr(result, "fit_markdown") and result.fit_markdown:
            extracted_content = result.fit_markdown
        elif result.extracted_content:
            extracted_content = result.extracted_content
        elif hasattr(result, "markdown") and result.markdown:
            extracted_content = result.markdown

        if not extracted_content.strip():
            return jobs

        # Try to parse as JSON (LLM extraction)
        try:
            data = json.loads(extracted_content)
            if isinstance(data, list):
                job_list = data
            else:
                job_list = data.get("job_listings", []) if isinstance(data, dict) else []
        except json.JSONDecodeError:
            # Fall back to link extraction from markdown
            logger.debug("[crawl4ai] Could not parse LLM result as JSON, using markdown parsing")
            job_list = self._parse_markdown_links(extracted_content)

        for item in job_list:
            if not isinstance(item, dict):
                continue

            title = item.get("title", "").strip()
            url = item.get("url", "").strip()

            if not title or not url:
                continue

            # Resolve relative URLs
            full_url = urljoin(base_url, url)

            # Build Job object
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
                    adapter="crawl4ai",
                    tier=str(company_config.get("tier", 4)),
                    description_excerpt=title,
                    work_type="w2_contract" if company_config.get("contractor_source") else "",
                )
            )

        return jobs

    def _parse_markdown_links(self, markdown_text: str) -> List[Dict[str, str]]:
        """Extract job listings from markdown [title](url) format."""
        job_list = []
        # Pattern: [Title](URL)
        link_pattern = r"\[([^\]]+)\]\(([^)]+)\)"
        matches = re.findall(link_pattern, markdown_text)

        for title, url in matches:
            if title.strip() and url.strip():
                job_list.append({"title": title.strip(), "url": url.strip()})

        return job_list

    def _guess_location(self, text: str) -> str:
        """Extract location from job title or text."""
        # Pattern: "City, ST" or "Remote"
        city_state = re.search(r"\b([A-Z][a-z]+,\s*[A-Z]{2})\b", text)
        if city_state:
            return city_state.group(1)
        if "remote" in text.lower():
            return "Remote"
        return ""

    def _fallback(self, company_config: Dict[str, Any]) -> List[Job]:
        """Fallback to GenericAdapter if crawl4ai is not available."""
        if self._generic is None:
            from .generic import GenericAdapter

            self._generic = GenericAdapter(session=self.session, scorer=self.scorer)
        return self._generic.scrape(company_config)
