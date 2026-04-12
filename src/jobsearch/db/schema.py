"""Unified database schema for JobSearch v2."""

from __future__ import annotations
import sqlite3

from jobsearch.db.migrations import migrate_stage_history, migrate_content_hash

SCHEMA_VERSION: int = 6

# ── DDL Statements ───────────────────────────────────────────────────────────

_CREATE_APPLICATIONS = """
CREATE TABLE IF NOT EXISTS applications (
    id                 INTEGER PRIMARY KEY AUTOINCREMENT,
    company            TEXT    NOT NULL,
    role               TEXT    NOT NULL,
    role_normalized    TEXT,
    job_url            TEXT,
    source             TEXT,
    source_lane        TEXT    NOT NULL DEFAULT 'employer_ats',
    canonical_job_url  TEXT,
    scraper_key        TEXT    UNIQUE, -- Map to old Opportunity.id (company||title||url)
    status             TEXT    NOT NULL DEFAULT 'considering',
    
    -- Scraper specific fields
    score              REAL    NOT NULL DEFAULT 0.0,
    fit_band           TEXT,
    matched_keywords   TEXT,
    penalized_keywords TEXT,
    decision_reason    TEXT,
    description_excerpt TEXT,
    enriched_data      TEXT,  -- JSON: {visa_sponsor, tech_stack, ic_vs_manager, enrichment_status}
    content_hash       TEXT,  -- Hash of (title||url||description_excerpt) for change detection
    
    -- CRM & Profile fields
    location           TEXT,
    is_remote          INTEGER NOT NULL DEFAULT 0,
    salary_text        TEXT,
    salary_low         REAL,
    salary_high        REAL,
    work_type          TEXT,
    compensation_unit  TEXT,
    hourly_rate        REAL,
    hours_per_week     REAL,
    weeks_per_year     REAL,
    normalized_compensation_usd REAL,
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

_CREATE_BOARD_HEALTH = """
CREATE TABLE IF NOT EXISTS board_health (
    company             TEXT PRIMARY KEY,
    adapter             TEXT,
    careers_url         TEXT,
    board_state         TEXT NOT NULL DEFAULT 'healthy',
    last_success_method TEXT,
    last_http_status    INTEGER,
    manual_review_required INTEGER DEFAULT 0,
    suppression_reason  TEXT,
    consecutive_failures INTEGER DEFAULT 0,
    last_success_at     TEXT,
    last_attempt_at     TEXT,
    last_healed_at      TEXT,
    cooldown_until      TEXT,
    notes               TEXT,
    updated_at          TEXT NOT NULL
)
"""

_CREATE_BOARD_HEALTH_EVENTS = """
CREATE TABLE IF NOT EXISTS board_health_events (
    id                  INTEGER PRIMARY KEY AUTOINCREMENT,
    timestamp           TEXT NOT NULL,
    company             TEXT NOT NULL,
    adapter             TEXT,
    from_state          TEXT,
    to_state            TEXT NOT NULL,
    trigger_subsystem   TEXT NOT NULL, -- 'scrape_run', 'healer', 'dashboard', 'scheduler'
    reason              TEXT,
    extraction_method   TEXT,
    extraction_confidence REAL,
    metadata            TEXT -- JSON blob for extra context
)
"""

_CREATE_USER_ACTIONS = """
CREATE TABLE IF NOT EXISTS user_actions (
    id                  INTEGER PRIMARY KEY AUTOINCREMENT,
    entity_type         TEXT NOT NULL, -- 'job', 'application', 'contact', 'interview'
    entity_id           TEXT NOT NULL, -- scraper_key or row ID
    action_key          TEXT NOT NULL, -- 'apply', 'follow_up', 'outreach', 'prep', 'archive'
    status              TEXT NOT NULL DEFAULT 'active', -- 'active', 'completed', 'dismissed', 'snoozed'
    snoozed_until       TEXT,
    metadata            TEXT, -- JSON context
    created_at          TEXT NOT NULL,
    updated_at          TEXT NOT NULL,
    UNIQUE(entity_type, entity_id, action_key)
)
"""

_CREATE_TAILORED_ARTIFACTS = """
CREATE TABLE IF NOT EXISTS tailored_artifacts (
    id                  INTEGER PRIMARY KEY AUTOINCREMENT,
    application_id      INTEGER NOT NULL REFERENCES applications(id) ON DELETE CASCADE,
    artifact_type       TEXT NOT NULL, -- 'resume_summary', 'resume_bullets', 'cover_letter', 'outreach_note', 'why_company', 'why_role'
    content             TEXT,
    notes               TEXT,
    version             INTEGER DEFAULT 1,
    created_at          TEXT NOT NULL,
    updated_at          TEXT NOT NULL
)
"""

_CREATE_SCHEMA_META = """
CREATE TABLE IF NOT EXISTS schema_meta (
    key   TEXT PRIMARY KEY,
    value TEXT NOT NULL
)
"""

_CREATE_LLM_COST_LOG = """
CREATE TABLE IF NOT EXISTS llm_cost_log (
    id         INTEGER PRIMARY KEY AUTOINCREMENT,
    run_date   TEXT    NOT NULL,  -- ISO date YYYY-MM-DD
    tokens_used INTEGER NOT NULL,
    model      TEXT    NOT NULL,  -- e.g., "gemini-1.5-flash", "gpt-4o-mini"
    service    TEXT    NOT NULL,  -- e.g., "enrichment", "profile"
    created_at TEXT    NOT NULL   -- ISO datetime
)
"""

_CREATE_JOBS_FTS = """
CREATE VIRTUAL TABLE IF NOT EXISTS jobs_fts USING fts5(
    company,
    role,
    description_excerpt,
    jd_summary,
    content='applications',
    content_rowid='id',
    tokenize='porter ascii'
)
"""

_CREATE_JOBS_FTS_AI = """
CREATE TRIGGER IF NOT EXISTS jobs_fts_ai AFTER INSERT ON applications BEGIN
  INSERT INTO jobs_fts(rowid, company, role, description_excerpt, jd_summary)
  VALUES (new.id, new.company, new.role, new.description_excerpt, new.jd_summary);
END
"""

_CREATE_JOBS_FTS_AD = """
CREATE TRIGGER IF NOT EXISTS jobs_fts_ad AFTER DELETE ON applications BEGIN
  DELETE FROM jobs_fts WHERE rowid = old.id;
END
"""

_CREATE_JOBS_FTS_AU = """
CREATE TRIGGER IF NOT EXISTS jobs_fts_au AFTER UPDATE ON applications BEGIN
  DELETE FROM jobs_fts WHERE rowid = old.id;
  INSERT INTO jobs_fts(rowid, company, role, description_excerpt, jd_summary)
  VALUES (new.id, new.company, new.role, new.description_excerpt, new.jd_summary);
END
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
    _CREATE_BOARD_HEALTH,
    _CREATE_BOARD_HEALTH_EVENTS,
    _CREATE_USER_ACTIONS,
    _CREATE_TAILORED_ARTIFACTS,
    _CREATE_SCHEMA_META,
    _CREATE_LLM_COST_LOG,
    _CREATE_JOBS_FTS,
    _CREATE_JOBS_FTS_AI,
    _CREATE_JOBS_FTS_AD,
    _CREATE_JOBS_FTS_AU,
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
    _add_column_if_missing(cur, "applications", "work_type", "work_type TEXT")
    _add_column_if_missing(cur, "applications", "compensation_unit", "compensation_unit TEXT")
    _add_column_if_missing(cur, "applications", "hourly_rate", "hourly_rate REAL")
    _add_column_if_missing(cur, "applications", "hours_per_week", "hours_per_week REAL")
    _add_column_if_missing(cur, "applications", "weeks_per_year", "weeks_per_year REAL")
    _add_column_if_missing(cur, "applications", "normalized_compensation_usd", "normalized_compensation_usd REAL")
    _add_column_if_missing(cur, "applications", "status", "TEXT NOT NULL DEFAULT 'considering'")
    _add_column_if_missing(cur, "applications", "ai_analysis", "TEXT")
    _add_column_if_missing(cur, "applications", "ai_match_score", "REAL")
    _add_column_if_missing(cur, "applications", "enriched_data", "enriched_data TEXT")
    # V2 scoring columns — added alongside V1 columns for A/B comparison
    _add_column_if_missing(cur, "applications", "score_v2", "score_v2 REAL")
    _add_column_if_missing(cur, "applications", "fit_band_v2", "fit_band_v2 TEXT")
    _add_column_if_missing(cur, "applications", "v2_canonical_title", "v2_canonical_title TEXT")
    _add_column_if_missing(cur, "applications", "v2_seniority_band", "v2_seniority_band TEXT")
    _add_column_if_missing(cur, "applications", "v2_anchor_score", "v2_anchor_score REAL")
    _add_column_if_missing(cur, "applications", "v2_baseline_score", "v2_baseline_score REAL")
    _add_column_if_missing(cur, "applications", "v2_flags", "v2_flags TEXT")
    _add_column_if_missing(cur, "applications", "talking_points", "talking_points TEXT")
    _add_column_if_missing(cur, "applications", "extraction_method", "extraction_method TEXT")
    _add_column_if_missing(cur, "applications", "extraction_confidence", "extraction_confidence REAL")
    _add_column_if_missing(cur, "applications", "last_exported_at", "last_exported_at TEXT")
    _add_column_if_missing(cur, "applications", "role_cluster", "role_cluster TEXT")
    _add_column_if_missing(cur, "applications", "canonical_group_id", "canonical_group_id TEXT")
    _add_column_if_missing(cur, "applications", "is_canonical", "INTEGER DEFAULT 1")
    _add_column_if_missing(cur, "applications", "canonical_merge_rationale", "TEXT")
    migrate_stage_history(conn)
    migrate_content_hash(conn)
    cur.execute("CREATE TABLE IF NOT EXISTS schema_meta (key TEXT PRIMARY KEY, value TEXT NOT NULL)")
    cur.execute(
        """
        INSERT INTO schema_meta (key, value) VALUES (?, ?)
        ON CONFLICT(key) DO UPDATE SET value = excluded.value
        """,
        ("schema_version", str(SCHEMA_VERSION)),
    )

    # Rebuild FTS index if needed (for existing databases)
    try:
        fts_count = cur.execute("SELECT COUNT(*) FROM jobs_fts").fetchone()[0]
        app_count = cur.execute("SELECT COUNT(*) FROM applications").fetchone()[0]
        if app_count > 0 and fts_count == 0:
            cur.execute("INSERT INTO jobs_fts(jobs_fts) VALUES('rebuild')")
    except Exception:
        pass  # FTS may not be fully initialized; will be rebuilt on next init

    conn.commit()
