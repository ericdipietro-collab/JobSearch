"""Enrichment Service: AI-driven job description analysis for enhanced scoring."""

from __future__ import annotations
import json
import logging
import os
import sqlite3
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Dict, Optional

import google.generativeai as genai

logger = logging.getLogger(__name__)


@dataclass
class TokenBudget:
    """Track token usage against a daily budget."""

    daily_budget: int = 500_000
    tokens_used_today: int = 0
    conn: Optional[sqlite3.Connection] = field(default=None, repr=False)

    def check_and_increment(self, tokens: int) -> None:
        """Check if adding tokens exceeds budget; raise if so."""
        new_total = self.tokens_used_today + tokens
        if new_total > self.daily_budget:
            raise BudgetExceededError(
                f"LLM token budget exceeded: {new_total} > {self.daily_budget}"
            )
        self.tokens_used_today = new_total
        if self.conn:
            self._log_usage(tokens)

    def _log_usage(self, tokens: int) -> None:
        """Log token usage to database."""
        try:
            now = datetime.now(timezone.utc).isoformat()
            self.conn.execute(
                "INSERT INTO llm_cost_log (run_date, tokens_used, model, service, created_at) VALUES (?, ?, ?, ?, ?)",
                (now[:10], tokens, "gemini-1.5-flash", "enrichment", now),
            )
        except Exception as e:
            logger.warning(f"Failed to log token usage: {e}")

    @property
    def percent_used(self) -> float:
        """Return percentage of budget used."""
        return 100 * self.tokens_used_today / self.daily_budget if self.daily_budget > 0 else 0


class BudgetExceededError(Exception):
    """Raised when token budget is exceeded."""

    pass


class EnrichmentService:
    """Uses LLM to analyze job descriptions for visa sponsorship, tech stack, and IC vs Manager."""

    def __init__(
        self,
        api_key: Optional[str] = None,
        db_conn: Optional[sqlite3.Connection] = None,
        daily_token_budget: int = 500_000,
    ):
        self.api_key = api_key or os.getenv("GOOGLE_API_KEY")
        self.budget = TokenBudget(daily_budget=daily_token_budget, conn=db_conn)
        if self.api_key:
            genai.configure(api_key=self.api_key)

    def enrich_job(self, job_title: str, job_description: str) -> Dict[str, Any]:
        """
        Analyzes a job description for specific attributes.
        Returns a dict with: visa_sponsor, tech_stack, ic_vs_manager, and enrichment_status.
        """
        if not self.api_key:
            logger.warning("No GOOGLE_API_KEY found. Cannot enrich jobs.")
            return {
                "visa_sponsor": None,
                "tech_stack": [],
                "ic_vs_manager": None,
                "enrichment_status": "skipped_no_api",
            }

        prompt = f"""
        Analyze this job description and extract the following information.
        Return ONLY a valid JSON object with no markdown formatting.

        Job Title: {job_title}
        Job Description:
        {job_description[:3000]}

        Return exactly this JSON structure:
        {{
            "visa_sponsor": true/false/null,
            "tech_stack": ["technology1", "technology2", ...],
            "ic_vs_manager": "individual_contributor" or "manager" or "mixed" or null,
            "reasoning": "brief explanation of how you determined these values"
        }}

        Guidelines:
        - visa_sponsor: true if description explicitly mentions visa sponsorship, false if explicitly says no sponsorship, null if unclear
        - tech_stack: list the primary technologies, frameworks, and languages mentioned (e.g., ["Python", "AWS", "React"])
        - ic_vs_manager: determine if the role is primarily IC (engineer, analyst), manager (lead, manager, director), or mixed
        - Keep the response as raw JSON only, no markdown code blocks
        """

        try:
            model = genai.GenerativeModel("gemini-1.5-flash")
            response = model.generate_content(prompt)

            # Track token usage
            tokens_used = 0
            if hasattr(response, "usage_metadata") and response.usage_metadata:
                tokens_used = response.usage_metadata.total_token_count
                try:
                    self.budget.check_and_increment(tokens_used)
                except BudgetExceededError as e:
                    logger.warning(f"[enrichment] {e}")
                    return {
                        "visa_sponsor": None,
                        "tech_stack": [],
                        "ic_vs_manager": None,
                        "enrichment_status": "failed_budget_exceeded",
                    }

            text = response.text.strip()

            # Remove markdown code blocks if present
            if text.startswith("```json"):
                text = text[7:]
            if text.startswith("```"):
                text = text[3:]
            if text.endswith("```"):
                text = text[:-3]
            text = text.strip()

            data = json.loads(text)
            data["enrichment_status"] = "success"
            data["tokens_used"] = tokens_used
            return data
        except json.JSONDecodeError as exc:
            logger.error(f"Failed to parse enrichment JSON: {exc}")
            return {
                "visa_sponsor": None,
                "tech_stack": [],
                "ic_vs_manager": None,
                "enrichment_status": "failed_parse",
            }
        except BudgetExceededError:
            # Re-raise budget errors to be handled at batch level
            raise
        except Exception as exc:
            logger.error(f"Failed to enrich job: {exc}")
            return {
                "visa_sponsor": None,
                "tech_stack": [],
                "ic_vs_manager": None,
                "enrichment_status": f"failed_{type(exc).__name__}",
            }

    def batch_enrich_jobs(self, jobs: list[Dict[str, Any]]) -> list[Dict[str, Any]]:
        """
        Enriches a batch of jobs. Each job should have 'title' and 'description' keys.
        Returns the same list with 'enriched_data' added to each job.
        Stops early if budget is exceeded.
        """
        enriched_jobs = []
        for job in jobs:
            title = job.get("title", "")
            description = job.get("description", "")
            try:
                enrichment = self.enrich_job(title, description)
            except BudgetExceededError:
                logger.warning(f"[enrichment] Budget exceeded after {len(enriched_jobs)} jobs. Stopping batch.")
                # Still add jobs processed so far
                job_copy = dict(job)
                job_copy["enriched_data"] = {
                    "visa_sponsor": None,
                    "tech_stack": [],
                    "ic_vs_manager": None,
                    "enrichment_status": "failed_budget_exceeded",
                }
                enriched_jobs.append(job_copy)
                break
            job_copy = dict(job)
            job_copy["enriched_data"] = enrichment
            enriched_jobs.append(job_copy)
        return enriched_jobs
