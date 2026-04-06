"""Profile Service: LLM-driven resume analysis and preference generation."""

from __future__ import annotations
import json
import logging
import os
from typing import Any, Dict, List, Optional

from jobsearch.services.llm_client import LLMClient

logger = logging.getLogger(__name__)

class ProfileService:
    def __init__(
        self,
        api_key: Optional[str] = None,
        google_api_key: Optional[str] = None,
        openai_api_key: Optional[str] = None,
    ):
        # Support both legacy api_key and new multi-provider keys
        google_key = google_api_key or api_key or os.getenv("GOOGLE_API_KEY")
        openai_key = openai_api_key or os.getenv("OPENAI_API_KEY")
        self.llm_client = LLMClient(google_api_key=google_key, openai_api_key=openai_key)

    def extract_preferences(self, resume_text: str) -> Dict[str, Any]:
        """
        Uses an LLM to extract job search preferences from resume text.
        Returns a dictionary compatible with job_search_preferences.yaml.
        """
        if not self.llm_client.get_active_provider() or self.llm_client.get_active_provider() == "none":
            logger.warning("No LLM provider configured (GOOGLE_API_KEY or OPENAI_API_KEY). Cannot extract preferences.")
            return {}

        prompt = f"""
        Analyze the following resume text and extract job search preferences for a high-end job search.
        Return ONLY a JSON object with the following structure:
        {{
            "titles": {{
                "positive_keywords": ["Title 1", "Title 2"],
                "positive_weights": [["keyword", weight_int], ["keyword", weight_int]],
                "negative_disqualifiers": ["Title to avoid"],
                "must_have_modifiers": ["keyword that must be in title"]
            }},
            "keywords": {{
                "body_positive": [["skill1", weight_int], ["skill2", weight_int]],
                "body_negative": [["noise1", weight_int]]
            }}
        }}

        Guidelines:
        - Extract up to 10 relevant job titles.
        - positive_weights should be specific sub-terms (e.g., ["fintech", 10]).
        - body_positive should be technical skills, tools, and domain expertise.
        - Weights should be between 1 and 25.
        - Ensure the output is valid JSON and nothing else.

        Resume Text:
        {resume_text}
        """

        try:
            text, _ = self.llm_client.generate(prompt)

            # Clean up response text if it has markdown code blocks
            text = text.strip()
            if text.startswith("```json"):
                text = text[7:]
            if text.endswith("```"):
                text = text[:-3]
            text = text.strip()

            data = json.loads(text)
            return data
        except Exception as exc:
            logger.error(f"Failed to extract preferences: {exc}")
            return {}

    def merge_preferences(self, current_prefs: Dict[str, Any], extracted: Dict[str, Any]) -> Dict[str, Any]:
        """Merges extracted preferences into current ones without overwriting entire blocks."""
        new_prefs = dict(current_prefs)
        
        if "titles" not in new_prefs:
            new_prefs["titles"] = {}
        if "keywords" not in new_prefs:
            new_prefs["keywords"] = {}

        # Update titles
        ext_titles = extracted.get("titles", {})
        new_prefs["titles"]["positive_keywords"] = list(set(
            (new_prefs["titles"].get("positive_keywords") or []) + 
            (ext_titles.get("positive_keywords") or [])
        ))
        
        # Update body_positive keywords (merge lists of [keyword, weight])
        ext_keywords = extracted.get("keywords", {})
        current_body_pos = new_prefs["keywords"].get("body_positive") or []
        ext_body_pos = ext_keywords.get("body_positive") or []
        
        # Simple merge by keyword, taking max weight
        merged_body = {{k: v for k, v in current_body_pos}}
        for k, v in ext_body_pos:
            merged_body[k] = max(merged_body.get(k, 0), v)
        
        new_prefs["keywords"]["body_positive"] = [[k, v] for k, v in merged_body.items()]
        
        return new_prefs

    def analyze_job_fit(self, job_title: str, job_description: str, resume_text: str) -> Dict[str, Any]:
        """
        Uses an LLM to analyze how well a job fits the user's resume.
        """
        if not self.llm_client.get_active_provider() or self.llm_client.get_active_provider() == "none":
            return {}

        prompt = f"""
        Analyze the fit between this Job and the candidate's Resume.

        Job Title: {job_title}
        Job Description:
        {job_description[:5000]} # Truncate to avoid context limits

        Candidate Resume:
        {resume_text[:5000]}

        Return ONLY a JSON object with:
        {{
            "match_score": int (0-100),
            "summary": "1-2 sentence summary of why it fits or doesn't",
            "pros": ["pro 1", "pro 2"],
            "cons": ["con 1", "con 2"],
            "missing_skills": ["skill 1", "skill 2"]
        }}
        """

        try:
            text, _ = self.llm_client.generate(prompt)
            text = text.strip()
            if text.startswith("```json"):
                text = text[7:]
            if text.endswith("```"):
                text = text[:-3]
            return json.loads(text.strip())
        except Exception as exc:
            logger.error(f"Failed to analyze job fit: {exc}")
            return {}
