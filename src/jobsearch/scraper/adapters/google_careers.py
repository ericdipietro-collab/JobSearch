from __future__ import annotations

import hashlib
import json
import logging
import re
from typing import Any, Dict, List, Optional
from urllib.parse import urlencode

from .base import BaseAdapter
from jobsearch.scraper.models import Job

logger = logging.getLogger(__name__)


class GoogleCareersAdapter(BaseAdapter):
    """
    Adapter for Google's own careers portal (google.com/about/careers).
    Uses the AF_initDataCallback extraction technique from jobspy-js.
    """

    BASE_URL = "https://www.google.com/about/careers/applications/jobs/results"

    def scrape(self, company_config: Dict[str, Any]) -> List[Job]:
        search_term = company_config.get("search_queries", "")
        if isinstance(search_term, list) and search_term:
            search_term = search_term[0]
        
        location = company_config.get("location_filter", "")
        
        jobs: List[Job] = []
        seen_ids: set[str] = set()
        
        # Scrape first 3 pages if needed
        for page in range(1, 4):
            params = {}
            if search_term:
                params["q"] = search_term
            if location:
                params["location"] = location
            if page > 1:
                params["page"] = page
            
            url = self.BASE_URL
            if params:
                url = f"{self.BASE_URL}?{urlencode(params)}"
            
            try:
                html = self.fetch_text(url)
                page_jobs = self._parse_af_data(html, company_config)
                
                new_count = 0
                for job in page_jobs:
                    if job.id not in seen_ids:
                        jobs.append(job)
                        seen_ids.add(job.id)
                        new_count += 1
                
                if new_count == 0:
                    break
            except Exception as e:
                logger.error(f"GoogleCareersAdapter failed on page {page}: {e}")
                break
                
        return jobs

    def _parse_af_data(self, html: str, config: Dict[str, Any]) -> List[Job]:
        """
        Extract job data from AF_initDataCallback blocks.
        Specifically looks for the 'ds:1' (search results) or similar data stream.
        """
        # Find all AF_initDataCallback calls
        # Pattern: AF_initDataCallback({key: 'ds:1', ... data: [...] ...});
        # We'll use a regex to find the data block.
        # Note: Google's JSON is often array-based and deeply nested.
        
        # Look for the 'ds:1' or 'ds:0' key which usually contains the results
        data_match = re.search(r"AF_initDataCallback\s*\(\s*\{.*?key:\s*['\"]ds:1['\"].*?data:\s*(.*?),?\s*sideChannel:.*?\}\s*\)\s*;", html, re.DOTALL)
        if not data_match:
            # Try ds:0 as fallback
            data_match = re.search(r"AF_initDataCallback\s*\(\s*\{.*?key:\s*['\"]ds:0['\"].*?data:\s*(.*?),?\s*sideChannel:.*?\}\s*\)\s*;", html, re.DOTALL)
            
        if not data_match:
            return []
            
        raw_data_str = data_match.group(1).strip()
        
        # The data string might be an array or a deeply nested structure.
        # We need to make sure we parse it correctly. 
        # Sometimes it's a JS object literal, but usually it's valid JSON for the data part.
        try:
            # We might need to handle trailing commas or other JS-isms if JSON.loads fails
            # But usually the 'data' part is a clean JSON array.
            data = json.loads(raw_data_str)
        except json.JSONDecodeError:
            # If it fails, try a more aggressive extraction or cleanup
            try:
                # Basic depth-tracking extractor for the array
                data = self._extract_json_array(raw_data_str)
            except Exception as e:
                logger.warning(f"Failed to parse Google Careers JSON data: {e}")
                return []

        if not data or not isinstance(data, list):
            return []

        # jobspy-js indicates jobs are at data[0][1]
        try:
            raw_jobs = data[0][1]
        except (IndexError, TypeError):
            return []

        if not isinstance(raw_jobs, list):
            return []

        company_name = config.get("name", "Google")
        tier = str(config.get("tier", 4))
        
        jobs: List[Job] = []
        for raw in raw_jobs:
            try:
                # jobspy-js indices:
                # ID: job[0]
                # Title: job[1]
                # Location: job[9][0] -> [city, state, country]
                # Description segments: job[10][1] (about), job[3][1] (responsibilities), 
                #                        job[4][1] (min qual), job[19][1] (pref qual)
                
                job_id_internal = str(raw[0])
                title = str(raw[1])
                
                # Construct URL
                job_url = f"https://www.google.com/about/careers/applications/jobs/results/{job_id_internal}"
                
                # Location
                location_parts = []
                try:
                    loc_arr = raw[9][0]
                    # primaryLoc[2] (city), primaryLoc[4] (state), primaryLoc[5] (country)
                    city = loc_arr[2] or loc_arr[0]
                    state = loc_arr[4]
                    country = loc_arr[5]
                    location_parts = [p for p in [city, state, country] if p]
                except (IndexError, TypeError):
                    pass
                location_str = ", ".join(location_parts)
                
                # Description
                desc_parts = []
                try:
                    # About
                    if len(raw) > 10 and len(raw[10]) > 1:
                        desc_parts.append(f"About: {raw[10][1]}")
                    # Responsibilities
                    if len(raw) > 3 and len(raw[3]) > 1:
                        desc_parts.append(f"Responsibilities: {raw[3][1]}")
                    # Min Qual
                    if len(raw) > 4 and len(raw[4]) > 1:
                        desc_parts.append(f"Minimum Qualifications: {raw[4][1]}")
                    # Pref Qual
                    if len(raw) > 19 and len(raw[19]) > 1:
                        desc_parts.append(f"Preferred Qualifications: {raw[19][1]}")
                except (IndexError, TypeError):
                    pass
                description = "\n\n".join(desc_parts)
                
                job_id = hashlib.md5(f"{company_name}{title}{job_url}".encode()).hexdigest()
                
                job = Job(
                    id=job_id,
                    company=company_name,
                    role_title_raw=title,
                    location=location_str,
                    url=job_url,
                    source="Google Careers",
                    adapter="google_careers",
                    tier=tier,
                    description_excerpt=description
                )
                jobs.append(job)
            except (IndexError, TypeError, KeyError) as e:
                logger.debug(f"Skipping malformed job record in Google Careers: {e}")
                continue
                
        return jobs

    def _extract_json_array(self, text: str) -> Any:
        """Hand-rolled bracket-matching parser for the data array."""
        depth = 0
        start = -1
        in_string = False
        escape = False
        
        for i, char in enumerate(text):
            if escape:
                escape = False
                continue
            if char == '\\':
                escape = True
                continue
            if char == '"' and not escape:
                in_string = not in_string
                continue
            
            if not in_string:
                if char == '[':
                    if depth == 0:
                        start = i
                    depth += 1
                elif char == ']':
                    depth -= 1
                    if depth == 0:
                        return json.loads(text[start : i + 1])
        return None
