import yaml
import requests
import pandas as pd
import re
import time
import shutil
import sys
from datetime import datetime, timedelta
from dataclasses import dataclass, field, asdict
from pathlib import Path
from typing import Optional, List, Dict, Any, Tuple
from bs4 import BeautifulSoup

# --- 1. RELATIVE PATHING (GitHub Ready) ---
BASE_DIR = Path(__file__).parent
CONFIG_DIR = BASE_DIR / "config"
RESULTS_DIR = BASE_DIR / "results"
RESULTS_DIR.mkdir(exist_ok=True)

YAML_COMPANIES = CONFIG_DIR / "job_search_companies.yaml"
YAML_PREFERENCES = CONFIG_DIR / "job_search_preferences.yaml"
OUTPUT_EXCEL = RESULTS_DIR / "job_search_results.xlsx"
LOG_FILE = RESULTS_DIR / "job_search.log"

# --- 2. UPDATED DATACLASSES (Fixes Log Crashes) ---
@dataclass
class Job:
    company: str
    tier: int
    title: str
    location: str
    url: str
    source: str
    score: float
    fit_band: str
    decision_reason: str
    matched_keywords: str = ""
    penalized_keywords: str = ""
    salary_range: str = ""
    age_days: Optional[int] = None
    keep: bool = True
    drop_stage: str = ""
    drop_reason: str = ""

@dataclass
class RejectedJob:
    company: str
    title: str
    location: str
    url: str
    source: str
    description: str
    posted_dt: Optional[datetime]
    drop_stage: str
    drop_reason: str
    # Added to fix "unexpected keyword argument" errors in logs
    adjacent_title_tokens_debug: str = ""
    adjacent_pattern_tokens_debug: str = ""
    adjacent_match_attempts: str = ""

# --- 3. BOUNDARY-SAFE KEYWORD MATCHING ---
def _kw_match(keyword: str, text: str) -> bool:
    """Uses word boundaries for clean matching (e.g., API vs capability)."""
    if not keyword or not text: return False
    kw = keyword.lower().strip()
    blob = text.lower()
    
    # Handle technical tokens with special chars (.NET, C++)
    escaped = re.escape(kw).replace(r"\ ", r"\s+")
    if re.match(r"^\w", kw) and re.search(r"\w$", kw):
        pattern = rf"\b{escaped}\b"
    else:
        pattern = rf"(?<!\w){escaped}(?!\w)"
    return re.search(pattern, blob, flags=re.IGNORECASE) is not None

# --- 4. TIERED SCORING LOGIC ---
def score_job(title: str, description: str, preferences: Dict) -> Dict[str, Any]:
    title_l = title.lower()
    desc_l = description.lower()
    score = 0.0
    
    # Gating Check: 5% Salary Buffer
    # (Simplified logic for brevity; assumes preferences loaded)
    salary_floor = preferences.get('min_salary_usd', 170000) * 0.95 
    
    # Title Fast-Track (Base 50)
    fast_track_triggered = False
    title_hits = []
    for kw, weight in preferences.get('title_weights', {}).items():
        if _kw_match(kw, title_l):
            if weight >= 8:
                score = 50.0
                fast_track_triggered = True
                title_hits.append(kw)
            else:
                score += weight

    # JD Multiplier: Anti-Double-Dipping
    # If title is already a "Fast-Track" match, JD points are worth half.
    jd_multiplier = 0.5 if fast_track_triggered else 1.0
    
    pos_points = 0
    matched_kws = []
    for kw, weight in preferences.get('jd_positive_keywords', {}).items():
        if _kw_match(kw, desc_l):
            pos_points += weight
            matched_kws.append(kw)
            
    score += (min(pos_points, 40) * jd_multiplier)
    
    return {
        "score": min(score, 100),
        "decision_reason": f"Title Match: {', '.join(title_hits)} | JD Multiplier: {jd_multiplier}x",
        "matched_keywords": ", ".join(matched_kws)
    }

# --- 5. UPDATED EVALUATION FLOW ---
def evaluate_job(company_data, title, location, description, preferences):
    """Returns a dict with keep=True/False for the UI Audit Log."""
    # 1. Hard Gate: Title Blacklist
    for blacklisted in preferences.get('title_negative_disqualifiers', []):
        if _kw_match(blacklisted, title):
            return {"keep": False, "drop_stage": "Title Blacklist", "drop_reason": f"Matched: {blacklisted}"}

    # 2. Score the job
    results = score_job(title, description, preferences)
    results["keep"] = results["score"] >= preferences.get('min_score_to_keep', 35)
    
    if not results["keep"]:
        results["drop_stage"] = "Low Score"
        results["drop_reason"] = f"Score {results['score']} below threshold"
        
    return results

# ... (Main loop and company loading logic follows standard structure)