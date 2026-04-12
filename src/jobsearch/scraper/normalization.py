from __future__ import annotations
import re
from typing import Dict, Any, Optional

class WorkTypeNormalizer:
    """Rigorous normalization for employment types (FTE, Contract, C2H, etc.)."""
    
    FTE_PATTERNS = [r"full-time", r"permanent", r"\bfte\b", r"direct hire"]
    CONTRACT_PATTERNS = [r"contract", r"temporary", r"temp", r"consultant", r"freelance", r"statement of work"]
    C2H_PATTERNS = [r"contract-to-hire", r"cth", r"contract to hire", r"temp-to-perm"]
    W2_PATTERNS = [r"w2 hourly", r"w-2", r"w2 only"]
    C2C_PATTERNS = [r"c2c", r"corp-to-corp", r"1099", r"independent contractor"]

    @classmethod
    def normalize(cls, text: str) -> str:
        if not text:
            return "unknown"
        text = text.lower()
        
        if any(re.search(p, text) for p in cls.C2H_PATTERNS):
            return "contract_to_hire"
        if any(re.search(p, text) for p in cls.CONTRACT_PATTERNS):
            if any(re.search(p, text) for p in cls.C2C_PATTERNS):
                return "1099_contract"
            if any(re.search(p, text) for p in cls.W2_PATTERNS):
                return "w2_contract"
            return "contract"
        if any(re.search(p, text) for p in cls.FTE_PATTERNS):
            return "fte"
            
        return "unknown"

class SourceLaneRegistry:
    """Trust hierarchy for the ATS Search Net v2."""
    
    # Tier 1: Primary Truth
    LANE_EMPLOYER_ATS = "employer_ats"
    
    # Tier 2: Validated Discovery
    LANE_ATS_DISCOVERY = "ats_discovery"
    
    # Tier 3: High-Yield Specialty
    LANE_SPECIALTY_BOARD = "specialty_board" # e.g. Dice
    
    # Tier 4: Supplemental Recall
    LANE_SEARCH_RECALL = "search_recall" # e.g. Google Jobs
    
    # Tier 5: Broad Aggregators
    LANE_AGGREGATOR = "aggregator"
    
    # Tier 6: Manual / Browser Assist
    LANE_BROWSER_ASSIST = "browser_assist"

    TRUST_RANKS = {
        LANE_EMPLOYER_ATS: 0,
        LANE_ATS_DISCOVERY: 1,
        LANE_SPECIALTY_BOARD: 2,
        LANE_BROWSER_ASSIST: 3,
        LANE_SEARCH_RECALL: 4,
        LANE_AGGREGATOR: 5,
    }

    @classmethod
    def get_rank(cls, lane: str) -> int:
        return cls.TRUST_RANKS.get(lane, 99)
