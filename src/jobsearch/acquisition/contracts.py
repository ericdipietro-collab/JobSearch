"""Core contracts for JobSearch V2 acquisition/evaluation handoff."""

from __future__ import annotations
from dataclasses import dataclass, field
from typing import List, Dict, Any, Optional
from jobsearch.scraper.models import Job

@dataclass
class AcquisitionJob:
    """Represents a job as initially acquired, before evaluation."""
    job: Job
    metadata: Dict[str, Any] = field(default_factory=dict)

@dataclass
class EvaluationResult:
    """Represents the results of an evaluation (scoring) pass."""
    score: float
    fit_band: str
    decision_reason: str
    matched_keywords: str
    penalized_keywords: str
    apply_now_eligible: bool = True
    v2_breakdown: Optional[Dict[str, Any]] = None
    metadata: Dict[str, Any] = field(default_factory=dict)
