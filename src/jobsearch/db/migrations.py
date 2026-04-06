"""Shared database migration helpers."""

from __future__ import annotations

import sqlite3


def table_columns(conn: sqlite3.Connection, table: str) -> set[str]:
    return {str(row[1]) for row in conn.execute(f"PRAGMA table_info({table})").fetchall()}


def migrate_stage_history(conn: sqlite3.Connection) -> None:
    existing = table_columns(conn, "stage_history")
    if not existing or "application_id" in existing:
        return
    if "opportunity_id" not in existing:
        return

    conn.execute("DROP TABLE IF EXISTS stage_history_legacy")
    conn.execute("ALTER TABLE stage_history RENAME TO stage_history_legacy")
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS stage_history (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            application_id  INTEGER NOT NULL REFERENCES applications(id) ON DELETE CASCADE,
            from_stage      TEXT,
            to_stage        TEXT NOT NULL,
            timestamp       TEXT NOT NULL,
            note            TEXT NOT NULL DEFAULT ''
        )
        """
    )
    note_expr = "COALESCE(sh.note, '')" if "note" in existing else "''"
    conn.execute(
        f"""
        INSERT INTO stage_history (id, application_id, from_stage, to_stage, timestamp, note)
        SELECT sh.id, sh.opportunity_id, sh.from_stage, sh.to_stage, sh.timestamp, {note_expr}
        FROM stage_history_legacy AS sh
        INNER JOIN applications AS a ON a.id = sh.opportunity_id
        """
    )
    conn.execute("DROP TABLE stage_history_legacy")


def migrate_content_hash(conn: sqlite3.Connection) -> None:
    """Add content_hash column for preserving annotations on re-scrape."""
    existing = table_columns(conn, "applications")
    if "content_hash" in existing:
        return
    conn.execute("ALTER TABLE applications ADD COLUMN content_hash TEXT")

