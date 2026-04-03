"""Unified database schema for JobSearch v2."""

from __future__ import annotations
import sqlite3

SCHEMA_VERSION: int = 2

# ── DDL Statements ───────────────────────────────────────────────────────────

_CREATE_APPLICATIONS = """
CREATE TABLE IF NOT EXISTS applications (
    id                 INTEGER PRIMARY KEY AUTOINCREMENT,
    company            TEXT    NOT NULL,
    role               TEXT    NOT NULL,
    role_normalized    TEXT,
    job_url            TEXT,
    source             TEXT,
    scraper_key        TEXT    UNIQUE, -- Map to old Opportunity.id (company||title||url)
    status             TEXT    NOT NULL DEFAULT 'considering',
    
    -- Scraper specific fields
    score              REAL    NOT NULL DEFAULT 0.0,
    fit_band           TEXT,
    matched_keywords   TEXT,
    penalized_keywords TEXT,
    decision_reason    TEXT,
    description_excerpt TEXT,
    
    -- CRM & Profile fields
    location           TEXT,
    is_remote          INTEGER NOT NULL DEFAULT 0,
    salary_text        TEXT,
    salary_low         REAL,
    salary_high        REAL,
    referral           TEXT,
    jd_summary         TEXT,
    notes              TEXT,
    user_priority      INTEGER NOT NULL DEFAULT 0,
    fit_stars          INTEGER, -- For UI compatibility
    
    -- Tracking & Pipeline
    date_discovered    TEXT,
    date_added         TEXT,
    date_applied       TEXT,
    date_closed        TEXT,
    follow_up_date     TEXT,
    follow_up_notes    TEXT,
    resume_version     TEXT,
    cover_letter_notes TEXT,
    entry_type         TEXT NOT NULL DEFAULT 'application',
    salary_range       TEXT,
    
    -- Document URLs
    resume_url         TEXT,
    cover_letter_url   TEXT,
    job_description    TEXT,
    
    -- Interview Prep (Legacy fields from ats_db)
    prep_company       TEXT,
    prep_why           TEXT,
    prep_tyabt         TEXT,
    prep_questions     TEXT,
    prep_notes         TEXT,
    
    -- Offer Details
    offer_base         INTEGER,
    offer_bonus_pct    INTEGER,
    offer_equity       TEXT,
    offer_signing      INTEGER,
    offer_pto_days     INTEGER,
    offer_k401_match   TEXT,
    offer_remote_policy TEXT,
    offer_start_date   TEXT,
    offer_expiry_date  TEXT,
    offer_notes        TEXT,
    
    -- Meta
    created_at         TEXT NOT NULL,
    updated_at         TEXT NOT NULL
)
"""

_CREATE_EVENTS = """
CREATE TABLE IF NOT EXISTS events (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    application_id  INTEGER NOT NULL REFERENCES applications(id) ON DELETE CASCADE,
    event_type      TEXT    NOT NULL,
    event_date      TEXT    NOT NULL,   -- ISO datetime
    title           TEXT,
    notes           TEXT,
    created_at      TEXT NOT NULL
)
"""

_CREATE_STAGE_HISTORY = """
CREATE TABLE IF NOT EXISTS stage_history (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    application_id  INTEGER NOT NULL REFERENCES applications(id) ON DELETE CASCADE,
    from_stage      TEXT,
    to_stage        TEXT NOT NULL,
    timestamp       TEXT NOT NULL,
    note            TEXT NOT NULL DEFAULT ''
)
"""

_CREATE_CONTACTS = """
CREATE TABLE IF NOT EXISTS contacts (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    application_id  INTEGER NOT NULL REFERENCES applications(id) ON DELETE CASCADE,
    name            TEXT    NOT NULL,
    title           TEXT,
    email           TEXT,
    phone           TEXT,
    linkedin_url    TEXT,
    role_in_process TEXT,
    notes           TEXT,
    created_at      TEXT NOT NULL
)
"""

_CREATE_INTERVIEWS = """
CREATE TABLE IF NOT EXISTS interviews (
    id                INTEGER PRIMARY KEY AUTOINCREMENT,
    application_id    INTEGER NOT NULL REFERENCES applications(id) ON DELETE CASCADE,
    round_number      INTEGER,
    interview_type    TEXT,
    scheduled_at      TEXT,
    duration_mins     INTEGER,
    interviewer_names TEXT,
    format            TEXT,
    location          TEXT,
    prep_notes        TEXT,
    outcome           TEXT    DEFAULT 'pending',
    outcome_notes     TEXT,
    created_at        TEXT NOT NULL,
    updated_at        TEXT NOT NULL
)
"""

_CREATE_TRAINING = """
CREATE TABLE IF NOT EXISTS training (
    id                INTEGER PRIMARY KEY AUTOINCREMENT,
    name              TEXT NOT NULL,
    provider          TEXT,
    category          TEXT,
    status            TEXT NOT NULL DEFAULT 'planned',
    url               TEXT,
    start_date        TEXT,
    target_date       TEXT,
    completion_date   TEXT,
    certificate_url   TEXT,
    estimated_hours   INTEGER,
    weekly_hours      INTEGER,
    notes             TEXT,
    created_at        TEXT NOT NULL,
    updated_at        TEXT NOT NULL
)
"""

_CREATE_SETTINGS = """
CREATE TABLE IF NOT EXISTS settings (
    key        TEXT PRIMARY KEY,
    value      TEXT NOT NULL,
    updated_at TEXT NOT NULL
)
"""

_CREATE_EMAIL_TEMPLATES = """
CREATE TABLE IF NOT EXISTS email_templates (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    name         TEXT NOT NULL,
    template_type TEXT NOT NULL DEFAULT 'other',
    subject      TEXT,
    body         TEXT NOT NULL,
    created_at   TEXT NOT NULL,
    updated_at   TEXT NOT NULL
)
"""

_CREATE_JOURNAL_ENTRIES = """
CREATE TABLE IF NOT EXISTS journal_entries (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    entry_date   TEXT    NOT NULL,
    mood         TEXT,
    content      TEXT    NOT NULL,
    created_at   TEXT    NOT NULL,
    updated_at   TEXT    NOT NULL
)
"""

_CREATE_NETWORK_CONTACTS = """
CREATE TABLE IF NOT EXISTS network_contacts (
    id                INTEGER PRIMARY KEY AUTOINCREMENT,
    name              TEXT    NOT NULL,
    company           TEXT,
    title             TEXT,
    email             TEXT,
    phone             TEXT,
    linkedin_url      TEXT,
    relationship      TEXT,
    notes             TEXT,
    last_contact_date TEXT,
    follow_up_date    TEXT,
    created_at        TEXT    NOT NULL,
    updated_at        TEXT    NOT NULL
)
"""

_CREATE_JOB_ANNOTATIONS = """
CREATE TABLE IF NOT EXISTS job_annotations (
    job_key    TEXT    PRIMARY KEY,
    note       TEXT,
    tag        TEXT,
    created_at TEXT    NOT NULL,
    updated_at TEXT    NOT NULL
)
"""

_CREATE_QUESTION_BANK = """
CREATE TABLE IF NOT EXISTS question_bank (
    id             INTEGER PRIMARY KEY AUTOINCREMENT,
    question       TEXT    NOT NULL,
    category       TEXT    NOT NULL DEFAULT 'behavioral',
    star_situation TEXT,
    star_task      TEXT,
    star_action    TEXT,
    star_result    TEXT,
    tags           TEXT,
    created_at     TEXT    NOT NULL,
    updated_at     TEXT    NOT NULL
)
"""

_CREATE_COMPANY_PROFILES = """
CREATE TABLE IF NOT EXISTS company_profiles (
    id               INTEGER PRIMARY KEY AUTOINCREMENT,
    name             TEXT    NOT NULL UNIQUE,
    website_url      TEXT,
    linkedin_url     TEXT,
    glassdoor_url    TEXT,
    about            TEXT,
    culture_notes    TEXT,
    interview_process TEXT,
    red_flags        TEXT,
    created_at       TEXT    NOT NULL,
    updated_at       TEXT    NOT NULL
)
"""

_CREATE_SCHEMA_META = """
CREATE TABLE IF NOT EXISTS schema_meta (
    key   TEXT PRIMARY KEY,
    value TEXT NOT NULL
)
"""

_DDL_STATEMENTS = [
    _CREATE_APPLICATIONS,
    _CREATE_EVENTS,
    _CREATE_STAGE_HISTORY,
    _CREATE_CONTACTS,
    _CREATE_INTERVIEWS,
    _CREATE_TRAINING,
    _CREATE_SETTINGS,
    _CREATE_EMAIL_TEMPLATES,
    _CREATE_JOURNAL_ENTRIES,
    _CREATE_NETWORK_CONTACTS,
    _CREATE_JOB_ANNOTATIONS,
    _CREATE_QUESTION_BANK,
    _CREATE_COMPANY_PROFILES,
    _CREATE_SCHEMA_META,
]


def _add_column_if_missing(
    cur: sqlite3.Cursor,
    table: str,
    column: str,
    ddl: str,
) -> None:
    existing = {
        row[1]
        for row in cur.execute(f"PRAGMA table_info({table})").fetchall()
    }
    if column not in existing:
        cur.execute(f"ALTER TABLE {table} ADD COLUMN {ddl}")


def init_db(conn: sqlite3.Connection) -> None:
    cur = conn.cursor()
    cur.execute("PRAGMA journal_mode=WAL")
    cur.execute("PRAGMA foreign_keys=ON")
    
    for ddl in _DDL_STATEMENTS:
        cur.execute(ddl)
        
    cur.execute(
        "INSERT OR IGNORE INTO schema_meta (key, value) VALUES (?, ?)",
        ("schema_version", str(SCHEMA_VERSION)),
    )
    _add_column_if_missing(cur, "applications", "tier", "tier INTEGER")
    conn.commit()
