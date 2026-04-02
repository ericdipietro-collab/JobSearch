from datetime import datetime
from typing import Optional, List, Dict, Any
from pydantic import BaseModel, Field

class Job(BaseModel):
    id: str
    company: str
    role_title_raw: str
    role_title_normalized: str = ""
    location: str = ""
    is_remote: bool = False
    source: str = ""
    url: str
    salary_min: Optional[float] = None
    salary_max: Optional[float] = None
    salary_text: str = ""
    score: float = 0.0
    fit_band: str = ""
    current_stage: str = "New"
    date_discovered: Optional[str] = None
    last_updated: Optional[str] = None
    date_applied: Optional[str] = None
    adapter: str = ""
    tier: str = ""
    matched_keywords: str = ""
    penalized_keywords: str = ""
    decision_reason: str = ""
    description_excerpt: str = ""
    user_priority: int = 0
    notes: str = ""

class RejectedJob(BaseModel):
    company: str
    title: str
    location: str
    url: str
    source: str
    description: str
    posted_dt: Optional[datetime] = None
    drop_stage: str
    drop_reason: str
    adjacent_title_tokens_debug: str = ""
    adjacent_pattern_tokens_debug: str = ""
    adjacent_match_attempts: str = ""
