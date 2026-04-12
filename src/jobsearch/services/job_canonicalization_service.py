from __future__ import annotations
import logging
import sqlite3
from dataclasses import dataclass, field
from typing import List, Dict, Any, Optional

from jobsearch.scraper.models import Job
from jobsearch.scraper.normalization import SourceLaneRegistry

logger = logging.getLogger(__name__)

@dataclass
class CanonicalDecision:
    canonical_group_id: Optional[str]
    is_canonical: int  # 1 or 0
    rationale: str
    is_new_group: bool = False
    superseded_id: Optional[int] = None
    conflicts: List[str] = field(default_factory=list)

class JobCanonicalizationService:
    """Service for identifying and consolidating duplicate job postings with provenance."""

    def __init__(self, conn: sqlite3.Connection):
        self.conn = conn

    def canonicalize_incremental(self, job: Job) -> CanonicalDecision:
        """
        Determines if a new job belongs to an existing canonical group.
        Implements Tiered matching:
        1. Exact req_id
        2. (Company + Normalized Title + Normalized Location)
        """
        company = job.company
        title_norm = job.role_title_normalized
        loc_norm = str(job.location or "").strip().lower()
        req_id = getattr(job, "req_id", None)
        
        # Try to find existing group
        existing_canon = None
        match_rationale = ""

        # 1. Match by req_id
        if req_id:
            existing_canon = self.conn.execute(
                "SELECT * FROM applications WHERE company = ? AND req_id = ? AND is_canonical = 1 LIMIT 1",
                (company, req_id)
            ).fetchone()
            if existing_canon:
                match_rationale = f"Exact req_id match: {req_id}"

        # 2. Match by Identity (Company, Title, Location)
        if not existing_canon:
            existing_canon = self.conn.execute(
                """
                SELECT * FROM applications 
                WHERE company = ? AND role_normalized = ? AND LOWER(TRIM(COALESCE(location, ''))) = ?
                AND is_canonical = 1
                LIMIT 1
                """,
                (company, title_norm, loc_norm)
            ).fetchone()
            if existing_canon:
                match_rationale = "Matched existing canonical by title and location."

        if existing_canon:
            gid = existing_canon['canonical_group_id'] or f"group_{existing_canon['id']}"
            
            # Conflict Detection
            conflicts = self._detect_conflicts(job, existing_canon)
            
            # Trust Rank Check: Should we supersede?
            new_rank = SourceLaneRegistry.get_rank(job.source_lane)
            cur_rank = SourceLaneRegistry.get_rank(existing_canon['source_lane'])
            
            if new_rank < cur_rank:
                return CanonicalDecision(
                    canonical_group_id=gid,
                    is_canonical=1,
                    rationale=f"Superseded existing canonical (rank {cur_rank} -> {new_rank}). {match_rationale}",
                    superseded_id=existing_canon['id'],
                    conflicts=conflicts
                )
            else:
                return CanonicalDecision(
                    canonical_group_id=gid,
                    is_canonical=0,
                    rationale=match_rationale,
                    conflicts=conflicts
                )

        # 3. New unique job
        return CanonicalDecision(
            canonical_group_id=None,
            is_canonical=1,
            rationale="No duplicates found; marked as new canonical.",
            is_new_group=True
        )

    def _detect_conflicts(self, new_job: Job, existing: sqlite3.Row) -> List[str]:
        conflicts = []
        
        # Salary Conflict
        new_sal = new_job.salary_max or 0
        old_sal = existing['salary_high'] or 0
        if new_sal > 0 and old_sal > 0:
            diff = abs(new_sal - old_sal) / max(new_sal, old_sal)
            if diff > 0.2:
                conflicts.append(f"Salary mismatch: {old_sal} vs {new_sal}")
        
        # Work Type Conflict
        new_wt = (new_job.work_type or "").lower()
        old_wt = (existing['work_type'] or "").lower()
        if new_wt and old_wt and new_wt != old_wt:
            conflicts.append(f"Work type conflict: {old_wt} vs {new_wt}")
            
        return conflicts

    def get_group_members(self, group_id: str) -> List[Dict[str, Any]]:
        """Returns all members of a canonical group."""
        rows = self.conn.execute(
            "SELECT * FROM applications WHERE canonical_group_id = ? OR id = ?",
            (group_id, group_id.replace("group_", ""))
        ).fetchall()
        return [dict(r) for r in rows]
