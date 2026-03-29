"""db/schema.py — DDL constants and database initialisation."""

from __future__ import annotations

import sqlite3

# Bump this when any DDL change requires a migration.
SCHEMA_VERSION: int = 1

# ── DDL ───────────────────────────────────────────────────────────────────────

_CREATE_OPPORTUNITIES = """
CREATE TABLE IF NOT EXISTS opportunities (
    id                  TEXT PRIMARY KEY,
    company             TEXT NOT NULL,
    role_title_raw      TEXT NOT NULL DEFAULT '',
    role_title_normalized TEXT NOT NULL DEFAULT '',
    location            TEXT NOT NULL DEFAULT '',
    is_remote           INTEGER NOT NULL DEFAULT 0,   -- SQLite boolean (0/1)
    source              TEXT NOT NULL DEFAULT '',
    url                 TEXT NOT NULL DEFAULT '',
    salary_min          REAL,
    salary_max          REAL,
    salary_text         TEXT NOT NULL DEFAULT '',
    score               REAL NOT NULL DEFAULT 0.0,
    fit_band            TEXT NOT NULL DEFAULT '',
    current_stage       TEXT NOT NULL DEFAULT 'New',
    date_discovered     TEXT NOT NULL DEFAULT '',
    last_updated        TEXT NOT NULL DEFAULT '',
    date_applied        TEXT,
    adapter             TEXT NOT NULL DEFAULT '',
    tier                TEXT NOT NULL DEFAULT '',
    matched_keywords    TEXT NOT NULL DEFAULT '',
    penalized_keywords  TEXT NOT NULL DEFAULT '',
    decision_reason     TEXT NOT NULL DEFAULT '',
    description_excerpt TEXT NOT NULL DEFAULT '',
    user_priority       INTEGER NOT NULL DEFAULT 0,
    notes               TEXT NOT NULL DEFAULT ''
)
"""

_CREATE_STAGE_HISTORY = """
CREATE TABLE IF NOT EXISTS stage_history (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    opportunity_id  TEXT NOT NULL REFERENCES opportunities(id) ON DELETE CASCADE,
    from_stage      TEXT,           -- NULL for initial creation
    to_stage        TEXT NOT NULL,
    timestamp       TEXT NOT NULL,  -- ISO-8601
    note            TEXT NOT NULL DEFAULT ''
)
"""

_CREATE_ACTIVITIES = """
CREATE TABLE IF NOT EXISTS activities (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    opportunity_id  TEXT NOT NULL REFERENCES opportunities(id) ON DELETE CASCADE,
    activity_type   TEXT NOT NULL,  -- screen|hm|panel|final|offer|rejection|note
    scheduled_date  TEXT,           -- ISO date
    completed_date  TEXT,           -- ISO date
    outcome         TEXT NOT NULL DEFAULT '',
    interviewer     TEXT NOT NULL DEFAULT '',
    notes           TEXT NOT NULL DEFAULT ''
)
"""

# A lightweight version table so future migrations can check the current state.
_CREATE_SCHEMA_META = """
CREATE TABLE IF NOT EXISTS schema_meta (
    key   TEXT PRIMARY KEY,
    value TEXT NOT NULL
)
"""

# Ordered list of DDL statements executed by init_db()
_DDL_STATEMENTS: list[str] = [
    _CREATE_OPPORTUNITIES,
    _CREATE_STAGE_HISTORY,
    _CREATE_ACTIVITIES,
    _CREATE_SCHEMA_META,
]

# Index definitions (table, index_name, columns, unique)
_INDEXES: list[tuple[str, str, str, bool]] = [
    ("stage_history", "idx_sh_opportunity", "opportunity_id", False),
    ("stage_history", "idx_sh_timestamp",   "timestamp",      False),
    ("activities",    "idx_act_opportunity", "opportunity_id", False),
    ("opportunities", "idx_opp_company",     "company",        False),
    ("opportunities", "idx_opp_stage",       "current_stage",  False),
    ("opportunities", "idx_opp_score",       "score",          False),
]


def _build_index_ddl(table: str, name: str, columns: str, unique: bool) -> str:
    unique_kw = "UNIQUE " if unique else ""
    return f"CREATE {unique_kw}INDEX IF NOT EXISTS {name} ON {table} ({columns})"


# ── Public API ─────────────────────────────────────────────────────────────────

def init_db(conn: sqlite3.Connection) -> None:
    """
    Idempotently create all tables, indexes, and seed schema_meta.
    Safe to call on every application start.
    """
    cur = conn.cursor()

    # Enable WAL mode for better concurrent read performance
    cur.execute("PRAGMA journal_mode=WAL")
    # Enforce FK constraints (off by default in SQLite)
    cur.execute("PRAGMA foreign_keys=ON")

    for ddl in _DDL_STATEMENTS:
        cur.execute(ddl)

    for table, name, columns, unique in _INDEXES:
        cur.execute(_build_index_ddl(table, name, columns, unique))

    # Record schema version (INSERT OR IGNORE so it's not overwritten on upgrade)
    cur.execute(
        "INSERT OR IGNORE INTO schema_meta (key, value) VALUES (?, ?)",
        ("schema_version", str(SCHEMA_VERSION)),
    )

    conn.commit()
