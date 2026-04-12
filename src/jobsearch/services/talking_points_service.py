"""Talking Points Service — generates 3-bullet per-job application materials via LLM."""

from __future__ import annotations

import json
import logging
import os
from typing import Any, Dict, List, Optional

from jobsearch.services.llm_client import LLMClient

logger = logging.getLogger(__name__)

_PROMPT_TEMPLATE = """\
You are a career coach helping a senior professional write targeted application talking points.

CANDIDATE RESUME (excerpt):
{resume_text}

TARGET ROLE:
Company: {company}
Title: {job_title}
Description excerpt:
{description_excerpt}

Matched keywords: {matched_keywords}

Generate concise application talking points in JSON format:
{{
  "opening": "One sentence connecting the candidate's seniority and domain to this specific role.",
  "bullets": [
    "Bullet 1 — link a specific JD requirement to a concrete candidate achievement",
    "Bullet 2 — link a second JD requirement to candidate experience",
    "Bullet 3 — link a third JD requirement to candidate skills"
  ],
  "company_line": "One sentence specific to this company — why them over competitors."
}}

Return ONLY the JSON object, no markdown fences, no extra text.
"""


class TalkingPointsService:
    """Generates per-job talking points using LLM, with DB caching."""

    def __init__(
        self,
        google_api_key: Optional[str] = None,
        openai_api_key: Optional[str] = None,
    ) -> None:
        self._llm = LLMClient(
            google_api_key=google_api_key or os.getenv("GOOGLE_API_KEY", ""),
            openai_api_key=openai_api_key or os.getenv("OPENAI_API_KEY", ""),
        )

    def generate(
        self,
        job_title: str,
        company: str,
        description_excerpt: str,
        resume_text: str,
        matched_keywords: Optional[List[str]] = None,
    ) -> Optional[Dict[str, Any]]:
        """
        Generate talking points for a job.

        Returns dict with keys: opening, bullets (list of 3), company_line.
        Returns None on failure.
        """
        kw_str = ", ".join(matched_keywords or []) or "N/A"
        prompt = _PROMPT_TEMPLATE.format(
            resume_text=(resume_text or "")[:3000],
            company=company or "Unknown",
            job_title=job_title or "Unknown",
            description_excerpt=(description_excerpt or "")[:2000],
            matched_keywords=kw_str,
        )
        try:
            raw = self._llm.generate(prompt)
            if not raw:
                return None
            # Strip accidental markdown fences
            raw = raw.strip()
            if raw.startswith("```"):
                raw = raw.split("```")[1]
                if raw.startswith("json"):
                    raw = raw[4:]
                raw = raw.strip()
            result = json.loads(raw)
            # Validate expected shape
            if "opening" in result and "bullets" in result and "company_line" in result:
                # Ensure exactly 3 bullets
                bullets = result["bullets"]
                if isinstance(bullets, list):
                    result["bullets"] = (bullets + [""] * 3)[:3]
                return result
            return None
        except Exception as exc:
            logger.warning("TalkingPointsService failed: %s", exc)
            return None
