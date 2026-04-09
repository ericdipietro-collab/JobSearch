import logging
import re
from typing import List, Dict, Any, Optional
from .base import BaseAdapter
from jobsearch.scraper.models import Job

logger = logging.getLogger(__name__)

class WWRAdapter(BaseAdapter):
    """
    Adapter for WeWorkRemotely RSS feeds.
    High-stability source for remote tech roles.
    """
    
    BASE_RSS_URL = "https://weworkremotely.com/categories"

    def scrape(self, company_config: Dict[str, Any]) -> List[Job]:
        """
        Scrapes jobs from WWR RSS.
        Categories like: 'remote-product-jobs', 'remote-data-science-jobs', 'software-development'.
        """
        category = company_config.get("category", "remote-product-jobs")
        url = f"{self.BASE_RSS_URL}/{category}.rss"
        
        try:
            import feedparser
            feed = feedparser.parse(url)
            
            if not feed.entries:
                logger.info(f"WWR: No jobs found in RSS for category '{category}'")
                return []

            jobs = []
            for entry in feed.entries:
                # WWR RSS titles are usually "Company: Role"
                title_parts = entry.title.split(": ", 1)
                company = title_parts[0] if len(title_parts) > 1 else "Unknown"
                role = title_parts[1] if len(title_parts) > 1 else entry.title

                job = Job(
                    company=company,
                    role=role,
                    url=entry.link,
                    location="Remote",
                    description_excerpt=entry.description,
                    salary_text="", # RSS doesn't usually have structured salary
                    source_lane="aggregator",
                    adapter="wwr",
                    tier=str(company_config.get("tier", 4))
                )
                # Cleanup HTML from description
                if job.description_excerpt:
                    job.description_excerpt = re.sub(r'<[^>]*>', '', job.description_excerpt)
                
                jobs.append(job)
                
            return jobs

        except Exception as e:
            logger.error(f"WWR RSS scrape failed for category '{category}': {e}")
            return []
