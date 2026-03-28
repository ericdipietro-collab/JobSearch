"""db/models.py — Dataclass models for the ATS tracking layer."""

from __future__ import annotations

import hashlib
from dataclasses import dataclass, field
from typing import Optional


@dataclass
class Opportunity:
    """A single job opportunity tracked through the pipeline."""

    # Stable identifier: SHA-256 of "company|normalized_title|url"
    id: str

    # Core job identity
    company: str
    role_title_raw: str
    role_title_normalized: str
    location: str
    is_remote: bool
    source: str
    url: str

    # Compensation
    salary_min: Optional[float]
    salary_max: Optional[float]
    salary_text: str

    # Scoring / fit
    score: float
    fit_band: str

    # Pipeline state
    current_stage: str

    # Timestamps (ISO-8601 strings for simplicity; sqlite stores as TEXT)
    date_discovered: str
    last_updated: str
    date_applied: Optional[str]

    # Scraper metadata
    adapter: str
    tier: str
    matched_keywords: str
    penalized_keywords: str
    decision_reason: str
    description_excerpt: str

    # User-controlled fields
    user_priority: int = 0
    notes: str = ""

    # ── Factory helpers ────────────────────────────────────────────────────────

    @staticmethod
    def make_id(company: str, title: str, url: str) -> str:
        """
        Produce a stable SHA-256 hex digest from the three identity fields.
        Mirrors the job_id pattern used in job_search_v6.py so existing keys
        remain consistent after migration.
        """
        raw = f"{company.strip().lower()}|{title.strip().lower()}|{url.strip().lower()}"
        return hashlib.sha256(raw.encode("utf-8")).hexdigest()


@dataclass
class StageHistory:
    """Immutable audit record written every time an opportunity changes stage."""

    opportunity_id: str
    to_stage: str

    # from_stage is None when the opportunity is first inserted
    from_stage: Optional[str] = None
    timestamp: str = ""           # ISO-8601; set by service layer if empty
    note: str = ""

    # Auto-assigned by SQLite; None before first INSERT
    id: Optional[int] = None


@dataclass
class Activity:
    """
    A scheduled or completed interaction with an employer (phone screens,
    interviews, offers, rejections, freeform notes, etc.).
    """

    opportunity_id: str
    # One of: screen | hm | panel | final | offer | rejection | note
    activity_type: str

    scheduled_date: Optional[str] = None   # ISO date string
    completed_date: Optional[str] = None   # ISO date string
    outcome: str = ""
    interviewer: str = ""
    notes: str = ""

    # Auto-assigned by SQLite
    id: Optional[int] = None
