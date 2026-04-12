"""Full-text search service using SQLite FTS5 for job discovery."""

from __future__ import annotations

import logging
import sqlite3
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)


def search_jobs(
    conn: sqlite3.Connection,
    query: str,
    limit: int = 50,
    include_filtered: bool = False,
    is_canonical: Optional[int] = None,
) -> List[Dict[str, Any]]:
    """
    Search jobs using FTS5 BM25 ranking.

    Args:
        conn: SQLite database connection
        query: Search query (e.g., "Python backend remote")
        limit: Maximum results to return (default 50)
        include_filtered: If True, include all jobs; if False, exclude filtered statuses
        is_canonical: 1 for canonical only, 0 for duplicates only, None for all

    Returns:
        List of matching jobs with rank and score
    """
    if not query or not query.strip():
        return []

    query = query.strip()

    # Escape FTS5 special characters
    fts_query = query.replace('"', '""')

    try:
        cur = conn.cursor()

        # Build the WHERE clause filters
        where_parts = ["jobs_fts MATCH ?"]
        params = [fts_query]

        if not include_filtered:
            where_parts.append("a.status NOT IN ('rejected', 'archived', 'withdrawn')")
        
        if is_canonical is not None:
            where_parts.append("a.is_canonical = ?")
            params.append(is_canonical)

        where_clause = " AND ".join(where_parts)
        params.append(limit)

        # Use BM25 ranking with FTS5
        sql = f"""
        SELECT
            a.id,
            a.company,
            a.role,
            a.location,
            a.score,
            a.fit_band,
            a.source,
            a.source_lane,
            a.job_url,
            a.salary_text,
            a.work_type,
            a.description_excerpt,
            a.status,
            rank
        FROM jobs_fts
        JOIN applications a ON jobs_fts.rowid = a.id
        WHERE {where_clause}
        ORDER BY rank
        LIMIT ?
        """

        results = []
        for row in cur.execute(sql, tuple(params)).fetchall():
            results.append({
                "id": row[0],
                "company": row[1],
                "role": row[2],
                "location": row[3],
                "score": row[4],
                "fit_band": row[5],
                "source": row[6],
                "source_lane": row[7],
                "url": row[8],
                "salary_text": row[9],
                "work_type": row[10],
                "description_excerpt": row[11],
                "status": row[12],
                "rank": row[13],
            })

        return results

    except Exception as e:
        logger.error(f"FTS5 search failed for query '{query}': {e}")
        return []


def rebuild_fts_index(conn: sqlite3.Connection) -> None:
    """Rebuild the FTS5 index from current applications table."""
    try:
        cur = conn.cursor()
        cur.execute("INSERT INTO jobs_fts(jobs_fts) VALUES('rebuild')")
        conn.commit()
        logger.info("FTS5 index rebuilt successfully")
    except Exception as e:
        logger.error(f"Failed to rebuild FTS5 index: {e}")


def search_jobs_by_company(
    conn: sqlite3.Connection,
    company: str,
    limit: int = 50,
) -> List[Dict[str, Any]]:
    """
    Search jobs by company name using FTS5.

    Args:
        conn: SQLite database connection
        company: Company name to search for
        limit: Maximum results to return

    Returns:
        List of matching jobs
    """
    return search_jobs(conn, f"company:{company}", limit=limit)


def search_jobs_by_keyword(
    conn: sqlite3.Connection,
    keyword: str,
    limit: int = 50,
) -> List[Dict[str, Any]]:
    """
    Search jobs by keyword in title and description using FTS5.

    Args:
        conn: SQLite database connection
        keyword: Keyword to search for
        limit: Maximum results to return

    Returns:
        List of matching jobs
    """
    return search_jobs(conn, keyword, limit=limit)
