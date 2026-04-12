from __future__ import annotations
import logging
import sqlite3
import hashlib
from difflib import SequenceMatcher
from typing import List, Dict, Any, Optional

from jobsearch import ats_db
from jobsearch.scraper.normalization import SourceLaneRegistry

logger = logging.getLogger(__name__)

class JobCanonicalizationService:
    """Service for identifying and consolidating duplicate job postings."""

    def __init__(self, conn: sqlite3.Connection):
        self.conn = conn

    def find_duplicates(self):
        """Tiered duplicate detection across the entire application set."""
        # Reset current canonical markers
        self.conn.execute("UPDATE applications SET is_canonical = 1, canonical_group_id = NULL")
        
        # 1. Tier 1: Exact matches on (Company, Normalized Title, Location)
        rows = self.conn.execute(
            """
            SELECT company, role_normalized, location, COUNT(*) as n, GROUP_CONCAT(id) as ids
            FROM applications
            GROUP BY company, role_normalized, location
            HAVING n > 1
            """
        ).fetchall()
        
        for row in rows:
            ids = [int(i) for i in row['ids'].split(',')]
            self._merge_group(ids, "Exact match on Company, Title, and Location.")

        # 2. Tier 2: Fuzzy Description Match for same Company + Title
        # (Heuristic: Similarity > 0.8)
        self.conn.commit()

    def canonicalize_incremental(self, job: Job) -> tuple[str, int, str]:
        """
        Determines if a new job belongs to an existing canonical group.
        Returns (canonical_group_id, is_canonical, rationale).
        """
        company = job.company
        title_norm = job.role_title_normalized
        loc_norm = str(job.location or "").strip().lower()
        req_id = getattr(job, "req_id", None)
        
        # 1. High Confidence: Match by req_id
        if req_id:
            match = self.conn.execute(
                "SELECT canonical_group_id, id FROM applications WHERE company = ? AND scraper_key LIKE ? LIMIT 1",
                (company, f"%{req_id}%")
            ).fetchone()
            if match:
                gid = match['canonical_group_id'] or f"group_{match['id']}"
                return gid, 0, f"Matched existing req_id: {req_id}"

        # 2. Medium-High Confidence: Match by (Company, Title, Location)
        match = self.conn.execute(
            """
            SELECT canonical_group_id, id, source_lane, extraction_method 
            FROM applications 
            WHERE company = ? AND role_normalized = ? AND LOWER(TRIM(COALESCE(location, ''))) = ?
            AND is_canonical = 1
            LIMIT 1
            """,
            (company, title_norm, loc_norm)
        ).fetchone()
        
        if match:
            gid = match['canonical_group_id'] or f"group_{match['id']}"
            
            # Determine if new job should supersede current canonical
            should_supersede = self._should_supersede(job, match)
            if should_supersede:
                # We'll mark this one as canonical and the existing one as duplicate
                # Note: This requires a bit of state management in the caller
                return gid, 1, f"Superseded existing canonical due to better source lane ({job.source_lane})"
            
            return gid, 0, "Matched existing canonical by title and location."

        # 3. New unique job
        return None, 1, "No duplicates found; marked as new canonical."

    def _should_supersede(self, new_job: Job, current_canon: sqlite3.Row) -> bool:
        """Determines if a new job source is 'better' than the current canonical."""
        new_rank = SourceLaneRegistry.get_rank(new_job.source_lane)
        cur_rank = SourceLaneRegistry.get_rank(current_canon['source_lane'])
        
        if new_rank < cur_rank:
            return True
        return False

    def _merge_group(self, ids: List[int], rationale: str):
        """Consolidates a group of IDs under one canonical record."""
        if not ids: return
        
        ids.sort() # First ID becomes canonical (usually oldest seen)
        canonical_id = ids[0]
        group_id = f"group_{canonical_id}"
        
        # Mark all as part of group
        id_str = ",".join(map(str, ids))
        self.conn.execute(
            f"UPDATE applications SET canonical_group_id = ?, is_canonical = 0 WHERE id IN ({id_str})",
            (group_id,)
        )
        # Re-mark the primary one
        self.conn.execute(
            "UPDATE applications SET is_canonical = 1, notes = notes || ? WHERE id = ?",
            (f"\nCanonical Merge: {rationale} ({len(ids)} sources)", canonical_id)
        )

    def get_canonical_jobs(self, status: Optional[str] = None) -> List[Dict[str, Any]]:
        """Returns only canonical job records."""
        query = "SELECT * FROM applications WHERE is_canonical = 1"
        params = []
        if status:
            query += " AND status = ?"
            params.append(status)
        
        rows = self.conn.execute(query, params).fetchall()
        return [dict(r) for r in rows]
