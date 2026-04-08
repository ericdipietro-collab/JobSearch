"""Enrichment Service: AI-driven job description analysis for enhanced scoring."""

from __future__ import annotations
import json
import logging
import os
import sqlite3
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Dict, Optional

from jobsearch.services.llm_client import LLMClient

logger = logging.getLogger(__name__)


@dataclass
class TokenBudget:
    """Track token usage against a daily budget."""

    daily_budget: int = 500_000
    tokens_used_today: int = 0
    conn: Optional[sqlite3.Connection] = field(default=None, repr=False)

    def check_and_increment(self, tokens: int, model: str = "gemini-2.5-flash-lite") -> None:
        """Check if adding tokens exceeds budget; raise if so."""
        new_total = self.tokens_used_today + tokens
        if new_total > self.daily_budget:
            raise BudgetExceededError(
                f"LLM token budget exceeded: {new_total} > {self.daily_budget}"
            )
        self.tokens_used_today = new_total
        if self.conn:
            self._log_usage(tokens, model=model)

    def _log_usage(self, tokens: int, model: str = "gemini-2.5-flash-lite") -> None:
        """Log token usage to database."""
        try:
            now = datetime.now(timezone.utc).isoformat()
            self.conn.execute(
                "INSERT INTO llm_cost_log (run_date, tokens_used, model, service, created_at) VALUES (?, ?, ?, ?, ?)",
                (now[:10], tokens, model, "enrichment", now),
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
    """Uses LLM to analyze job descriptions for visa sponsorship, tech stack, IC vs Manager, and skills gaps."""

    def __init__(
        self,
        api_key: Optional[str] = None,
        db_conn: Optional[sqlite3.Connection] = None,
        daily_token_budget: int = 500_000,
        user_skills: Optional[list[str]] = None,
        google_api_key: Optional[str] = None,
        openai_api_key: Optional[str] = None,
        preferred_provider: Optional[str] = None,
        ollama_base_url: Optional[str] = None,
        ollama_model: Optional[str] = None,
    ):
        # Support both legacy api_key and new multi-provider keys
        google_key = google_api_key or api_key or os.getenv("GOOGLE_API_KEY")
        openai_key = openai_api_key or os.getenv("OPENAI_API_KEY")

        self.llm_client = LLMClient(
            google_api_key=google_key,
            openai_api_key=openai_key,
            preferred_provider=preferred_provider,
            ollama_base_url=ollama_base_url,
            ollama_model=ollama_model,
        )
        self.budget = TokenBudget(daily_budget=daily_token_budget, conn=db_conn)
        self.user_skills = user_skills or []

    def enrich_job(self, job_title: str, job_description: str) -> Dict[str, Any]:
        """
        Analyzes a job description for specific attributes.
        Returns a dict with: visa_sponsor, tech_stack, ic_vs_manager, missing_skills, and enrichment_status.
        """
        if not self.llm_client.get_active_provider() or self.llm_client.get_active_provider() == "none":
            logger.warning("No LLM provider configured (GOOGLE_API_KEY or OPENAI_API_KEY). Cannot enrich jobs.")
            return {
                "visa_sponsor": None,
                "tech_stack": [],
                "ic_vs_manager": None,
                "missing_skills": [],
                "enrichment_status": "skipped_no_api",
            }

        # Build user skills section for the prompt
        user_skills_section = ""
        if self.user_skills:
            user_skills_str = ", ".join(self.user_skills)
            user_skills_section = f"\n\nUser's Current Skills: {user_skills_str}"

        jd_limit = 1500 if self.llm_client.is_local() else 3000

        # Data first, schema last — optimal for local model recency bias
        prompt = f"""Analyze this job posting and extract structured metadata.

Job Title: {job_title}
Job Description:
{job_description[:jd_limit]}{user_skills_section}

Return ONLY this JSON structure:
{{
    "visa_sponsor": true,
    "tech_stack": ["Python", "AWS", "React"],
    "ic_vs_manager": "individual_contributor",
    "missing_skills": ["skill not in user skills but required by JD"],
    "reasoning": "one sentence explaining key signals"
}}

Rules:
- visa_sponsor: true if JD explicitly offers sponsorship, false if it says no sponsorship, null if not mentioned
- tech_stack: primary technologies, frameworks, platforms named in the JD
- ic_vs_manager: "individual_contributor", "manager", or "mixed" based on responsibilities described
- missing_skills: skills explicitly required by the JD that are absent from the user's skill list; empty list if all covered
- reasoning: brief (one sentence) rationale for your determinations"""

        try:
            text, usage_metadata = self.llm_client.generate(prompt)

            # Track token usage
            tokens_used = 0
            if usage_metadata and "total_tokens" in usage_metadata:
                tokens_used = usage_metadata["total_tokens"]
                provider = usage_metadata.get("provider", "unknown")
                model = usage_metadata.get("model") or f"llm-{provider}"
                try:
                    self.budget.check_and_increment(tokens_used, model=model)
                except BudgetExceededError as e:
                    logger.warning(f"[enrichment] {e}")
                    return {
                        "visa_sponsor": None,
                        "tech_stack": [],
                        "ic_vs_manager": None,
                        "missing_skills": [],
                        "enrichment_status": "failed_budget_exceeded",
                    }

            data = json.loads(LLMClient.strip_json_response(text))
            data["enrichment_status"] = "success"
            data["tokens_used"] = tokens_used
            return data
        except json.JSONDecodeError as exc:
            logger.error(f"Failed to parse enrichment JSON: {exc}")
            return {
                "visa_sponsor": None,
                "tech_stack": [],
                "ic_vs_manager": None,
                "missing_skills": [],
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
                "missing_skills": [],
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
                    "missing_skills": [],
                    "enrichment_status": "failed_budget_exceeded",
                }
                enriched_jobs.append(job_copy)
                break
            job_copy = dict(job)
            job_copy["enriched_data"] = enrichment
            enriched_jobs.append(job_copy)
        return enriched_jobs
