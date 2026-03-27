import yaml
import requests
import pandas as pd
import re
import os
import time
from datetime import datetime
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional, List, Dict, Any

# --- PATH FIX: Dynamic Relative Paths ---
# This ensures the script finds files inside your project folder instead of 'Downloads'
BASE_DIR = Path(__file__).parent
CONFIG_DIR = BASE_DIR / "config"
RESULTS_DIR = BASE_DIR / "results"
RESULTS_DIR.mkdir(exist_ok=True) # Automatically creates the results folder

YAML_PREFERENCES = CONFIG_DIR / "job_search_preferences.yaml"
YAML_COMPANIES = CONFIG_DIR / "job_search_companies.yaml"
# ----------------------------------------

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
    # FIX: Added missing fields to stop the crashes found in your logs
    adjacent_title_tokens_debug: str = ""
    adjacent_pattern_tokens_debug: str = ""
    adjacent_match_attempts: str = ""

# ... [Include your existing helper functions like _kw_match and clean here] ...

def evaluate_job(company_data, title, location, description, preferences):
    """Full soft-drop gating logic for the UI Audit Log."""
    norm_title = title.lower()
    
    # 1. Hard Gate: Title Blacklist
    # Note: Using 'negative_disqualifiers' to match your YAML
    for blacklisted in preferences.get('negative_disqualifiers', []):
        if _kw_match(blacklisted, norm_title):
            return {"keep": False, "drop_stage": "Title Blacklist", "drop_reason": f"Matched: {blacklisted}"}

    # 2. Score the job using weighted analysis
    results = score_job(title, description, preferences)
    
    # 3. Soft Gate: Minimum Score
    # Using 35 as established in your preferences
    min_score = preferences.get('minimum_score_to_keep', 35)
    results["keep"] = results["score"] >= min_score
    
    if not results["keep"]:
        results["drop_stage"] = "Low Score"
        results["drop_reason"] = f"Score {results['score']:.1f} < {min_score}"
        
    return results