"""
SQLite database layer for the Job Application Tracker.

Tables
------
applications   – one row per job application (the "deal" in CRM terms)
events         – ordered timeline of everything that happened
contacts       – people associated with an application
interviews     – scheduled / completed interview rounds
"""

import csv
import re
import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

_BASE_DIR = Path(__file__).resolve().parent
BASE_DIR  = _BASE_DIR   # public alias
DB_PATH   = _BASE_DIR / "results" / "jobsearch.db"

# ── Status vocabulary ──────────────────────────────────────────────────────────
STATUSES = [
    "exploring",    # network opportunity — active conversation, no formal application yet
    "considering",
    "applied",
    "screening",
    "interviewing",
    "offer",
    "accepted",
    "rejected",
    "withdrawn",
]

STATUS_COLORS = {
    "exploring":    "#7c3aed",   # violet — opportunity in progress
    "considering":  "#6b7280",   # gray
    "applied":      "#3b82f6",   # blue
    "screening":    "#8b5cf6",   # purple
    "interviewing": "#f59e0b",   # amber
    "offer":        "#10b981",   # green
    "accepted":     "#059669",   # dark green
    "rejected":     "#ef4444",   # red
    "withdrawn":    "#9ca3af",   # light gray
}

ENTRY_TYPES = ["application", "opportunity", "job_fair"]

TRAINING_STATUSES    = ["planned", "in_progress", "completed", "paused"]
TRAINING_CATEGORIES  = ["AI / ML", "Cloud", "Data & Analytics", "Programming", "Certification Prep", "Business / Leadership", "Other"]
TRAINING_PROVIDERS   = ["AWS", "Snowflake", "Google", "Microsoft", "Coursera", "Udemy", "LinkedIn Learning", "freeCodeCamp", "Pluralsight", "Other"]
TRAINING_STATUS_COLORS = {
    "planned":     "#6b7280",
    "in_progress": "#f59e0b",
    "completed":   "#10b981",
    "paused":      "#9ca3af",
}

EVENT_TYPES = [
    "applied",
    "conversation",         # networking / informal call
    "networking_call",      # scheduled call from network
    "recruiter_outreach",
    "screening_scheduled",
    "screening_complete",
    "interview_scheduled",
    "interview_complete",
    "offer_received",
    "offer_negotiating",
    "offer_accepted",
    "offer_declined",
    "rejected",
    "withdrawn",
    "follow_up_sent",
    "note",
]

INTERVIEW_TYPES  = ["phone_screen", "video", "onsite", "panel", "take_home", "final"]
INTERVIEW_FORMATS = ["behavioral", "technical", "case_study", "mixed", "other"]
CONTACT_ROLES    = ["recruiter", "hiring_manager", "interviewer", "referral", "network_contact", "other"]
OUTCOME_OPTIONS  = ["pending", "passed", "failed"]

NETWORK_RELATIONSHIPS = ["former_colleague", "recruiter", "mentor", "referral", "friend", "other"]
QUESTION_CATEGORIES   = ["behavioral", "situational", "leadership", "role-specific", "technical", "other"]

# Human-readable labels for event types used in reports
EVENT_LABELS = {
    "applied":             "Submitted application",
    "conversation":        "Networking conversation",
    "networking_call":     "Networking call",
    "recruiter_outreach":  "Recruiter contact",
    "screening_scheduled": "Phone screen scheduled",
    "screening_complete":  "Phone screen completed",
    "interview_scheduled": "Interview scheduled",
    "interview_complete":  "Interview completed",
    "offer_received":      "Received offer",
    "offer_negotiating":   "Negotiating offer",
    "offer_accepted":      "Accepted offer",
    "offer_declined":      "Declined offer",
    "rejected":            "Received rejection",
    "withdrawn":           "Withdrew application",
    "follow_up_sent":      "Sent follow-up",
    "note":                "Note",
}

# Which event types count as reportable job search activities for unemployment
REPORTABLE_EVENT_TYPES = {
    "applied", "conversation", "networking_call", "recruiter_outreach",
    "screening_scheduled", "screening_complete",
    "interview_scheduled", "interview_complete",
    "follow_up_sent",
}


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


# ── Connection ─────────────────────────────────────────────────────────────────

def get_connection() -> sqlite3.Connection:
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(DB_PATH))
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")
    return conn


# ── Schema ─────────────────────────────────────────────────────────────────────

def _add_columns_if_missing(conn: sqlite3.Connection, table: str, columns: Dict[str, str]) -> None:
    """Add columns that don't yet exist — safe to call on every startup."""
    existing = {row[1] for row in conn.execute(f"PRAGMA table_info({table})")}
    for col, typedef in columns.items():
        if col not in existing:
            try:
                conn.execute(f"ALTER TABLE {table} ADD COLUMN {col} {typedef}")
            except Exception:
                pass
    conn.commit()


def init_db(conn: sqlite3.Connection) -> None:
    conn.executescript("""
    CREATE TABLE IF NOT EXISTS applications (
        id                 INTEGER PRIMARY KEY AUTOINCREMENT,
        company            TEXT    NOT NULL,
        role               TEXT    NOT NULL,
        job_url            TEXT,
        source             TEXT,
        scraper_key        TEXT,
        status             TEXT    NOT NULL DEFAULT 'considering',
        fit_stars          INTEGER,
        salary_range       TEXT,
        salary_low         INTEGER,
        salary_high        INTEGER,
        referral           TEXT,
        jd_summary         TEXT,
        notes              TEXT,
        date_added         TEXT,
        date_applied       TEXT,
        date_closed        TEXT,
        follow_up_date     TEXT,
        follow_up_notes    TEXT,
        resume_version     TEXT,
        cover_letter_notes TEXT,
        entry_type         TEXT NOT NULL DEFAULT 'application',
        created_at         TEXT NOT NULL,
        updated_at         TEXT NOT NULL
    );

    CREATE TABLE IF NOT EXISTS events (
        id              INTEGER PRIMARY KEY AUTOINCREMENT,
        application_id  INTEGER NOT NULL REFERENCES applications(id) ON DELETE CASCADE,
        event_type      TEXT    NOT NULL,
        event_date      TEXT    NOT NULL,   -- ISO datetime
        title           TEXT,
        notes           TEXT,
        created_at      TEXT NOT NULL
    );

    CREATE TABLE IF NOT EXISTS contacts (
        id              INTEGER PRIMARY KEY AUTOINCREMENT,
        application_id  INTEGER NOT NULL REFERENCES applications(id) ON DELETE CASCADE,
        name            TEXT    NOT NULL,
        title           TEXT,
        email           TEXT,
        phone           TEXT,
        linkedin_url    TEXT,
        role_in_process TEXT,   -- recruiter | hiring_manager | interviewer | referral | other
        notes           TEXT,
        created_at      TEXT NOT NULL
    );

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
    );

    CREATE TABLE IF NOT EXISTS interviews (
        id                INTEGER PRIMARY KEY AUTOINCREMENT,
        application_id    INTEGER NOT NULL REFERENCES applications(id) ON DELETE CASCADE,
        round_number      INTEGER,
        interview_type    TEXT,   -- phone_screen | video | onsite | panel | take_home | final
        scheduled_at      TEXT,   -- ISO datetime
        duration_mins     INTEGER,
        interviewer_names TEXT,   -- comma-separated
        format            TEXT,   -- behavioral | technical | case_study | mixed | other
        location          TEXT,   -- video link, address, etc.
        prep_notes        TEXT,
        outcome           TEXT    DEFAULT 'pending',   -- pending | passed | failed
        outcome_notes     TEXT,
        created_at        TEXT NOT NULL,
        updated_at        TEXT NOT NULL
    );

    CREATE TABLE IF NOT EXISTS settings (
        key        TEXT PRIMARY KEY,
        value      TEXT NOT NULL,
        updated_at TEXT NOT NULL
    );

    CREATE TABLE IF NOT EXISTS email_templates (
        id           INTEGER PRIMARY KEY AUTOINCREMENT,
        name         TEXT NOT NULL,
        template_type TEXT NOT NULL DEFAULT 'other',
        subject      TEXT,
        body         TEXT NOT NULL,
        created_at   TEXT NOT NULL,
        updated_at   TEXT NOT NULL
    );

    CREATE TABLE IF NOT EXISTS journal_entries (
        id           INTEGER PRIMARY KEY AUTOINCREMENT,
        entry_date   TEXT    NOT NULL,
        mood         TEXT,
        content      TEXT    NOT NULL,
        created_at   TEXT    NOT NULL,
        updated_at   TEXT    NOT NULL
    );

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
    );

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
    );

    CREATE TABLE IF NOT EXISTS job_annotations (
        job_key    TEXT    PRIMARY KEY,
        note       TEXT,
        tag        TEXT,
        created_at TEXT    NOT NULL,
        updated_at TEXT    NOT NULL
    );

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
    );
    """)
    # Migrate existing DB — add any columns introduced after initial release
    _add_columns_if_missing(conn, "applications", {
        "follow_up_date":     "TEXT",
        "follow_up_notes":    "TEXT",
        "resume_version":     "TEXT",
        "cover_letter_notes": "TEXT",
        "entry_type":         "TEXT NOT NULL DEFAULT 'application'",
        # P0: document URLs + full JD storage
        "resume_url":         "TEXT",
        "cover_letter_url":   "TEXT",
        "job_description":    "TEXT",
        # P0: interview prep notes
        "prep_company":       "TEXT",
        "prep_why":           "TEXT",
        "prep_tyabt":         "TEXT",
        "prep_questions":     "TEXT",
        "prep_notes":         "TEXT",
        # P1: offer details
        "offer_base":         "INTEGER",
        "offer_bonus_pct":    "INTEGER",
        "offer_equity":       "TEXT",
        "offer_signing":      "INTEGER",
        "offer_pto_days":     "INTEGER",
        "offer_k401_match":   "TEXT",
        "offer_remote_policy": "TEXT",
        "offer_start_date":    "TEXT",
        "offer_expiry_date":   "TEXT",
        "offer_notes":         "TEXT",
        # P3: negotiation worksheet
        "nego_target_base":    "INTEGER",
        "nego_walkaway_base":  "INTEGER",
        "nego_market_low":     "INTEGER",
        "nego_market_high":    "INTEGER",
        "nego_notes":          "TEXT",
    })
    seed_default_templates(conn)
    seed_default_questions(conn)
    conn.commit()


# ── Applications ───────────────────────────────────────────────────────────────

def add_application(conn: sqlite3.Connection, **kwargs) -> int:
    now = _now()
    kwargs.setdefault("status",     "considering")
    kwargs.setdefault("created_at", now)
    kwargs.setdefault("updated_at", now)
    cols   = ", ".join(kwargs.keys())
    places = ", ".join("?" for _ in kwargs)
    cur = conn.execute(f"INSERT INTO applications ({cols}) VALUES ({places})", list(kwargs.values()))
    conn.commit()
    return cur.lastrowid


def update_application(conn: sqlite3.Connection, app_id: int, **kwargs) -> None:
    kwargs["updated_at"] = _now()
    sets = ", ".join(f"{k} = ?" for k in kwargs)
    conn.execute(f"UPDATE applications SET {sets} WHERE id = ?", [*kwargs.values(), app_id])
    conn.commit()


def get_application(conn: sqlite3.Connection, app_id: int) -> Optional[sqlite3.Row]:
    return conn.execute("SELECT *, user_priority AS fit_stars FROM applications WHERE id = ?", (app_id,)).fetchone()


def get_applications(
    conn: sqlite3.Connection,
    status: Optional[str] = None,
    entry_type: Optional[str] = None,
) -> List[sqlite3.Row]:
    clauses, params = [], []
    if status:
        clauses.append("status = ?")
        params.append(status)
    if entry_type:
        clauses.append("entry_type = ?")
        params.append(entry_type)
    where = ("WHERE " + " AND ".join(clauses)) if clauses else ""
    return conn.execute(
        f"SELECT *, user_priority AS fit_stars FROM applications {where} ORDER BY date_applied DESC, created_at DESC",
        params,
    ).fetchall()


def delete_application(conn: sqlite3.Connection, app_id: int) -> None:
    conn.execute("DELETE FROM applications WHERE id = ?", (app_id,))
    conn.commit()


# ── Events ─────────────────────────────────────────────────────────────────────

def add_event(conn: sqlite3.Connection, application_id: int, event_type: str,
              event_date: str, title: str = "", notes: str = "") -> int:
    cur = conn.execute(
        "INSERT INTO events (application_id, event_type, event_date, title, notes, created_at) "
        "VALUES (?, ?, ?, ?, ?, ?)",
        (application_id, event_type, event_date, title, notes, _now()),
    )
    conn.commit()
    return cur.lastrowid


def get_events(conn: sqlite3.Connection, application_id: int) -> List[sqlite3.Row]:
    return conn.execute(
        "SELECT * FROM events WHERE application_id = ? ORDER BY event_date ASC",
        (application_id,),
    ).fetchall()


def delete_event(conn: sqlite3.Connection, event_id: int) -> None:
    conn.execute("DELETE FROM events WHERE id = ?", (event_id,))
    conn.commit()


# ── Contacts ───────────────────────────────────────────────────────────────────

def add_contact(conn: sqlite3.Connection, application_id: int, **kwargs) -> int:
    kwargs["application_id"] = application_id
    kwargs.setdefault("created_at", _now())
    cols   = ", ".join(kwargs.keys())
    places = ", ".join("?" for _ in kwargs)
    cur = conn.execute(f"INSERT INTO contacts ({cols}) VALUES ({places})", list(kwargs.values()))
    conn.commit()
    return cur.lastrowid


def update_contact(conn: sqlite3.Connection, contact_id: int, **kwargs) -> None:
    sets = ", ".join(f"{k} = ?" for k in kwargs)
    conn.execute(f"UPDATE contacts SET {sets} WHERE id = ?", [*kwargs.values(), contact_id])
    conn.commit()


def get_contacts(conn: sqlite3.Connection, application_id: int) -> List[sqlite3.Row]:
    return conn.execute(
        "SELECT * FROM contacts WHERE application_id = ? ORDER BY created_at",
        (application_id,),
    ).fetchall()


def delete_contact(conn: sqlite3.Connection, contact_id: int) -> None:
    conn.execute("DELETE FROM contacts WHERE id = ?", (contact_id,))
    conn.commit()


# ── Interviews ─────────────────────────────────────────────────────────────────

def add_interview(conn: sqlite3.Connection, application_id: int, **kwargs) -> int:
    now = _now()
    kwargs["application_id"] = application_id
    kwargs.setdefault("outcome",     "pending")
    kwargs.setdefault("created_at",  now)
    kwargs.setdefault("updated_at",  now)
    cols   = ", ".join(kwargs.keys())
    places = ", ".join("?" for _ in kwargs)
    cur = conn.execute(f"INSERT INTO interviews ({cols}) VALUES ({places})", list(kwargs.values()))
    conn.commit()
    return cur.lastrowid


def update_interview(conn: sqlite3.Connection, interview_id: int, **kwargs) -> None:
    kwargs["updated_at"] = _now()
    sets = ", ".join(f"{k} = ?" for k in kwargs)
    conn.execute(f"UPDATE interviews SET {sets} WHERE id = ?", [*kwargs.values(), interview_id])
    conn.commit()


def get_interviews(conn: sqlite3.Connection, application_id: int) -> List[sqlite3.Row]:
    return conn.execute(
        "SELECT * FROM interviews WHERE application_id = ? ORDER BY scheduled_at ASC",
        (application_id,),
    ).fetchall()


def delete_interview(conn: sqlite3.Connection, interview_id: int) -> None:
    conn.execute("DELETE FROM interviews WHERE id = ?", (interview_id,))
    conn.commit()


# ── Training ────────────────────────────────────────────────────────────────────

def add_training(conn: sqlite3.Connection, **kwargs) -> int:
    now = _now()
    kwargs.setdefault("status",     "planned")
    kwargs.setdefault("created_at", now)
    kwargs.setdefault("updated_at", now)
    cols   = ", ".join(kwargs.keys())
    places = ", ".join("?" for _ in kwargs)
    cur = conn.execute(f"INSERT INTO training ({cols}) VALUES ({places})", list(kwargs.values()))
    conn.commit()
    return cur.lastrowid


def update_training(conn: sqlite3.Connection, training_id: int, **kwargs) -> None:
    kwargs["updated_at"] = _now()
    sets = ", ".join(f"{k} = ?" for k in kwargs)
    conn.execute(f"UPDATE training SET {sets} WHERE id = ?", [*kwargs.values(), training_id])
    conn.commit()


def get_training(conn: sqlite3.Connection, training_id: int) -> Optional[sqlite3.Row]:
    return conn.execute("SELECT * FROM training WHERE id = ?", (training_id,)).fetchone()


def get_all_training(conn: sqlite3.Connection, status: Optional[str] = None) -> List[sqlite3.Row]:
    if status:
        return conn.execute(
            "SELECT * FROM training WHERE status = ? ORDER BY status, name",
            (status,),
        ).fetchall()
    return conn.execute(
        "SELECT * FROM training ORDER BY "
        "CASE status WHEN 'in_progress' THEN 0 WHEN 'planned' THEN 1 "
        "WHEN 'paused' THEN 2 ELSE 3 END, name"
    ).fetchall()


def delete_training(conn: sqlite3.Connection, training_id: int) -> None:
    conn.execute("DELETE FROM training WHERE id = ?", (training_id,))
    conn.commit()


def training_status_counts(conn: sqlite3.Connection) -> Dict[str, int]:
    rows = conn.execute(
        "SELECT status, COUNT(*) AS n FROM training GROUP BY status"
    ).fetchall()
    return {r["status"]: r["n"] for r in rows}


# ── Summary stats ──────────────────────────────────────────────────────────────

def status_counts(conn: sqlite3.Connection) -> Dict[str, int]:
    rows = conn.execute(
        "SELECT status, COUNT(*) AS n FROM applications GROUP BY status"
    ).fetchall()
    return {r["status"]: r["n"] for r in rows}


def follow_up_due(conn: sqlite3.Connection) -> List[sqlite3.Row]:
    """Applications whose follow_up_date is today or overdue, still active."""
    today = datetime.now().date().isoformat()
    return conn.execute(
        """
        SELECT a.*,
            GROUP_CONCAT(c.name || ' (' || COALESCE(c.role_in_process,'') || ')', ', ') AS contact_summary,
            (SELECT email FROM contacts WHERE application_id = a.id ORDER BY created_at LIMIT 1) AS first_contact_email,
            (SELECT linkedin_url FROM contacts WHERE application_id = a.id ORDER BY created_at LIMIT 1) AS first_contact_linkedin
        FROM applications a
        LEFT JOIN contacts c ON c.application_id = a.id
        WHERE a.follow_up_date IS NOT NULL
          AND a.follow_up_date <= ?
          AND a.status NOT IN ('rejected', 'withdrawn', 'accepted')
        GROUP BY a.id
        ORDER BY a.follow_up_date ASC
        """,
        (today,),
    ).fetchall()


def follow_up_upcoming(conn: sqlite3.Connection, days: int = 3) -> List[sqlite3.Row]:
    """Applications with a follow-up due within the next N days."""
    from datetime import timedelta
    today = datetime.now().date()
    cutoff = (today + timedelta(days=days)).isoformat()
    today_str = today.isoformat()
    return conn.execute(
        """
        SELECT a.*,
            GROUP_CONCAT(c.name || ' (' || COALESCE(c.role_in_process,'') || ')', ', ') AS contact_summary,
            (SELECT email FROM contacts WHERE application_id = a.id ORDER BY created_at LIMIT 1) AS first_contact_email,
            (SELECT linkedin_url FROM contacts WHERE application_id = a.id ORDER BY created_at LIMIT 1) AS first_contact_linkedin
        FROM applications a
        LEFT JOIN contacts c ON c.application_id = a.id
        WHERE a.follow_up_date IS NOT NULL
          AND a.follow_up_date > ?
          AND a.follow_up_date <= ?
          AND a.status NOT IN ('rejected', 'withdrawn', 'accepted')
        GROUP BY a.id
        ORDER BY a.follow_up_date ASC
        """,
        (today_str, cutoff),
    ).fetchall()


def upcoming_interviews(conn: sqlite3.Connection, limit: int = 5) -> List[sqlite3.Row]:
    return conn.execute(
        """
        SELECT i.*, a.company, a.role
        FROM interviews i
        JOIN applications a ON a.id = i.application_id
        WHERE i.outcome = 'pending'
          AND i.scheduled_at >= datetime('now')
        ORDER BY i.scheduled_at ASC
        LIMIT ?
        """,
        (limit,),
    ).fetchall()


def get_training_for_report(
    conn: sqlite3.Connection,
    start_date: str,
    end_date: str,
) -> List[sqlite3.Row]:
    """
    Return training rows that were active during the date range:
    - in_progress courses that started on or before end_date
    - completed courses whose completion_date falls within the range
    """
    return conn.execute(
        """
        SELECT * FROM training
        WHERE (
            status = 'in_progress'
            AND (start_date IS NULL OR start_date <= ?)
        ) OR (
            status = 'completed'
            AND completion_date >= ?
            AND completion_date <= ?
        )
        ORDER BY status DESC, name
        """,
        (end_date, start_date, end_date),
    ).fetchall()


def get_activity_report(
    conn: sqlite3.Connection,
    start_date: str,
    end_date: str,
) -> List[sqlite3.Row]:
    """
    Return all reportable events in the date range, joined with application/opportunity context.
    Ordered by event_date descending.
    """
    placeholders = ",".join(f"'{t}'" for t in REPORTABLE_EVENT_TYPES)
    return conn.execute(
        f"""
        SELECT
            e.id            AS event_id,
            e.event_date,
            e.event_type,
            e.title         AS event_title,
            e.notes         AS event_notes,
            a.id            AS app_id,
            a.company,
            a.role,
            a.entry_type,
            a.job_url,
            a.status,
            GROUP_CONCAT(c.name, ', ') AS contact_names
        FROM events e
        JOIN applications a ON a.id = e.application_id
        LEFT JOIN contacts c ON c.application_id = a.id
        WHERE e.event_date >= ?
          AND e.event_date <= ?
          AND e.event_type IN ({placeholders})
        GROUP BY e.id
        ORDER BY e.event_date DESC, a.company
        """,
        (start_date, end_date),
    ).fetchall()


# ── Settings ──────────────────────────────────────────────────────────────────

def get_setting(conn: sqlite3.Connection, key: str, default: str = "") -> str:
    row = conn.execute("SELECT value FROM settings WHERE key = ?", (key,)).fetchone()
    return row["value"] if row else default


def set_setting(conn: sqlite3.Connection, key: str, value: str) -> None:
    now = _now()
    conn.execute(
        "INSERT INTO settings (key, value, updated_at) VALUES (?, ?, ?) "
        "ON CONFLICT(key) DO UPDATE SET value = excluded.value, updated_at = excluded.updated_at",
        (key, str(value), now),
    )
    conn.commit()


# ── Home dashboard queries ─────────────────────────────────────────────────────

def get_recent_events(conn: sqlite3.Connection, limit: int = 10) -> List[sqlite3.Row]:
    """Most recent events across all applications, with company/role context."""
    return conn.execute(
        """
        SELECT e.*, a.company, a.role, a.entry_type
        FROM events e
        JOIN applications a ON a.id = e.application_id
        ORDER BY e.event_date DESC, e.created_at DESC
        LIMIT ?
        """,
        (limit,),
    ).fetchall()


def pipeline_snapshot(conn: sqlite3.Connection) -> Dict[str, int]:
    """Count of applications per status (includes high-score matches as 'considering')."""
    rows = conn.execute(
        """
        SELECT status, COUNT(*) AS n FROM applications
        GROUP BY status
        """
    ).fetchall()
    counts = {r["status"]: r["n"] for r in rows}
    
    # Scraper 'Apply Now' matches (score >= 85) that are still 'considering'
    high_score = conn.execute(
        "SELECT COUNT(*) AS n FROM applications WHERE status = 'considering' AND score >= 85"
    ).fetchone()["n"]
    counts["apply_now"] = high_score
    
    return counts


def upcoming_interviews_this_week(conn: sqlite3.Connection) -> List[sqlite3.Row]:
    """Interviews scheduled from today through end of this week."""
    from datetime import timedelta
    today = datetime.now().date()
    end_of_week = today + timedelta(days=(6 - today.weekday()))
    return conn.execute(
        """
        SELECT i.*, a.company, a.role
        FROM interviews i
        JOIN applications a ON a.id = i.application_id
        WHERE i.outcome = 'pending'
          AND date(i.scheduled_at) >= ?
          AND date(i.scheduled_at) <= ?
        ORDER BY i.scheduled_at ASC
        """,
        (today.isoformat(), end_of_week.isoformat()),
    ).fetchall()


def apply_now_count(conn: sqlite3.Connection) -> int:
    """
    Count of active jobs in the unified pipeline.
    Includes both high-score scraper matches and active tracker entries.
    """
    # Active tracker entries (Applied and beyond)
    active_tracker = conn.execute(
        "SELECT COUNT(*) AS n FROM applications WHERE status IN ('applied','screening','interviewing','offer')"
    ).fetchone()["n"]
    
    # Scraper matches with score >= 85 that aren't yet Applied
    apply_now_scraper = conn.execute(
        "SELECT COUNT(*) AS n FROM applications WHERE status = 'considering' AND score >= 85"
    ).fetchone()["n"]
    
    return active_tracker + apply_now_scraper


def weekly_activity_count(conn: sqlite3.Connection, start_date: str, end_date: str) -> int:
    """Number of reportable events in the date range (for compliance progress bar)."""
    placeholders = ",".join(f"'{t}'" for t in REPORTABLE_EVENT_TYPES)
    row = conn.execute(
        f"""
        SELECT COUNT(*) AS n FROM events
        WHERE event_date >= ? AND event_date <= ?
          AND event_type IN ({placeholders})
        """,
        (start_date, end_date),
    ).fetchone()
    return row["n"] if row else 0


def get_applications_with_offers(conn: sqlite3.Connection) -> List[sqlite3.Row]:
    """Applications that have offer details recorded (offer_base is set or status=offer/accepted)."""
    return conn.execute(
        """
        SELECT * FROM applications
        WHERE status IN ('offer', 'accepted')
           OR offer_base IS NOT NULL
        ORDER BY updated_at DESC
        """
    ).fetchall()


# ── Email templates ────────────────────────────────────────────────────────────

TEMPLATE_TYPES = ["follow_up", "thank_you", "networking", "recruiter", "offer", "withdrawal", "other"]

DEFAULT_TEMPLATES = [
    {
        "name": "Application Follow-Up",
        "template_type": "follow_up",
        "subject": "Following up — {role} application",
        "body": (
            "Hi {contact_name},\n\n"
            "I wanted to follow up on my application for the {role} position at {company}. "
            "I'm still very interested in the opportunity and would love to connect to discuss next steps.\n\n"
            "Please let me know if there's anything else you need from me.\n\n"
            "Best,\n{my_name}"
        ),
    },
    {
        "name": "Post-Interview Thank You",
        "template_type": "thank_you",
        "subject": "Thank you — {role} interview",
        "body": (
            "Hi {contact_name},\n\n"
            "Thank you for taking the time to speak with me today about the {role} role at {company}. "
            "I really enjoyed learning more about the team and the work you're doing.\n\n"
            "Our conversation reinforced my excitement about this opportunity, and I'm confident I can "
            "bring strong value to the role. I look forward to hearing about next steps.\n\n"
            "Best,\n{my_name}"
        ),
    },
    {
        "name": "Networking Introduction",
        "template_type": "networking",
        "subject": "Connecting — {my_name}",
        "body": (
            "Hi {contact_name},\n\n"
            "I hope you're doing well. I'm currently exploring new opportunities after a recent transition "
            "and wanted to reconnect. I'd love to hear what you've been up to and share what I've been "
            "working on — and of course, any introductions or advice would be much appreciated.\n\n"
            "Would you have 20–30 minutes for a call sometime this week or next?\n\n"
            "Best,\n{my_name}"
        ),
    },
    {
        "name": "Recruiter Response",
        "template_type": "recruiter",
        "subject": "Re: {role} opportunity at {company}",
        "body": (
            "Hi {contact_name},\n\n"
            "Thanks for reaching out! The {role} role at {company} looks like a great fit — "
            "I'd love to learn more. I'm available for a call this week. "
            "Please let me know a time that works for you.\n\n"
            "Best,\n{my_name}"
        ),
    },
    {
        "name": "Offer Acknowledgment",
        "template_type": "offer",
        "subject": "Re: Offer — {role} at {company}",
        "body": (
            "Hi {contact_name},\n\n"
            "Thank you so much for the offer to join {company} as {role}. "
            "I'm very excited about this opportunity and want to take a few days to review the details carefully.\n\n"
            "I'll get back to you by [DATE]. Please don't hesitate to reach out if you need anything in the meantime.\n\n"
            "Best,\n{my_name}"
        ),
    },
    {
        "name": "Withdrawal",
        "template_type": "withdrawal",
        "subject": "Withdrawing application — {role} at {company}",
        "body": (
            "Hi {contact_name},\n\n"
            "I wanted to let you know that I'm withdrawing my application for the {role} position at {company}. "
            "I've decided to pursue a different direction at this time.\n\n"
            "I appreciate the time you and the team have invested in my candidacy, "
            "and I hope our paths cross again in the future.\n\n"
            "Best,\n{my_name}"
        ),
    },
]


def seed_default_templates(conn: sqlite3.Connection) -> None:
    """Idempotently insert default templates on first run."""
    row = conn.execute("SELECT COUNT(*) AS n FROM email_templates").fetchone()
    if row and row["n"] > 0:
        return
    now = _now()
    for t in DEFAULT_TEMPLATES:
        conn.execute(
            "INSERT INTO email_templates (name, template_type, subject, body, created_at, updated_at) "
            "VALUES (?, ?, ?, ?, ?, ?)",
            (t["name"], t["template_type"], t.get("subject", ""), t["body"], now, now),
        )
    conn.commit()


def get_all_templates(conn: sqlite3.Connection) -> List[sqlite3.Row]:
    return conn.execute(
        "SELECT * FROM email_templates ORDER BY template_type, name"
    ).fetchall()


def get_template(conn: sqlite3.Connection, template_id: int) -> Optional[sqlite3.Row]:
    return conn.execute(
        "SELECT * FROM email_templates WHERE id = ?", (template_id,)
    ).fetchone()


def add_template(conn: sqlite3.Connection, **kwargs) -> int:
    now = _now()
    kwargs.setdefault("created_at", now)
    kwargs.setdefault("updated_at", now)
    cols   = ", ".join(kwargs.keys())
    places = ", ".join("?" for _ in kwargs)
    cur = conn.execute(
        f"INSERT INTO email_templates ({cols}) VALUES ({places})", list(kwargs.values())
    )
    conn.commit()
    return cur.lastrowid


def update_template(conn: sqlite3.Connection, template_id: int, **kwargs) -> None:
    kwargs["updated_at"] = _now()
    sets = ", ".join(f"{k} = ?" for k in kwargs)
    conn.execute(
        f"UPDATE email_templates SET {sets} WHERE id = ?", [*kwargs.values(), template_id]
    )
    conn.commit()


def delete_template(conn: sqlite3.Connection, template_id: int) -> None:
    conn.execute("DELETE FROM email_templates WHERE id = ?", (template_id,))
    conn.commit()


# ── Bulk operations ────────────────────────────────────────────────────────────

def bulk_update_status(conn: sqlite3.Connection, app_ids: List[int], status: str) -> int:
    if not app_ids:
        return 0
    placeholders = ",".join("?" * len(app_ids))
    cur = conn.execute(
        f"UPDATE applications SET status = ?, updated_at = ? WHERE id IN ({placeholders})",
        [status, _now(), *app_ids],
    )
    conn.commit()
    return cur.rowcount


def bulk_delete_applications(conn: sqlite3.Connection, app_ids: List[int]) -> int:
    if not app_ids:
        return 0
    placeholders = ",".join("?" * len(app_ids))
    cur = conn.execute(
        f"DELETE FROM applications WHERE id IN ({placeholders})", app_ids
    )
    conn.commit()
    return cur.rowcount


# ── Journal ────────────────────────────────────────────────────────────────────

def add_journal_entry(conn: sqlite3.Connection, entry_date: str, content: str,
                      mood: Optional[str] = None) -> int:
    now = _now()
    cur = conn.execute(
        "INSERT INTO journal_entries (entry_date, mood, content, created_at, updated_at) "
        "VALUES (?, ?, ?, ?, ?)",
        (entry_date, mood, content, now, now),
    )
    conn.commit()
    return cur.lastrowid


def get_journal_entries(conn: sqlite3.Connection, limit: int = 100) -> List[sqlite3.Row]:
    return conn.execute(
        "SELECT * FROM journal_entries ORDER BY entry_date DESC, created_at DESC LIMIT ?",
        (limit,),
    ).fetchall()


def update_journal_entry(conn: sqlite3.Connection, entry_id: int, **kwargs) -> None:
    kwargs["updated_at"] = _now()
    sets = ", ".join(f"{k} = ?" for k in kwargs)
    conn.execute(f"UPDATE journal_entries SET {sets} WHERE id = ?", [*kwargs.values(), entry_id])
    conn.commit()


def delete_journal_entry(conn: sqlite3.Connection, entry_id: int) -> None:
    conn.execute("DELETE FROM journal_entries WHERE id = ?", (entry_id,))
    conn.commit()


# ── Network contacts ───────────────────────────────────────────────────────────

def add_network_contact(conn: sqlite3.Connection, name: str, **kwargs) -> int:
    now = _now()
    kwargs["name"] = name
    kwargs.setdefault("created_at", now)
    kwargs.setdefault("updated_at", now)
    cols   = ", ".join(kwargs.keys())
    places = ", ".join("?" for _ in kwargs)
    cur = conn.execute(
        f"INSERT INTO network_contacts ({cols}) VALUES ({places})", list(kwargs.values())
    )
    conn.commit()
    return cur.lastrowid


def update_network_contact(conn: sqlite3.Connection, contact_id: int, **kwargs) -> None:
    kwargs["updated_at"] = _now()
    sets = ", ".join(f"{k} = ?" for k in kwargs)
    conn.execute(f"UPDATE network_contacts SET {sets} WHERE id = ?", [*kwargs.values(), contact_id])
    conn.commit()


def get_network_contacts(
    conn: sqlite3.Connection,
    relationship: Optional[str] = None,
    search: Optional[str] = None,
) -> List[sqlite3.Row]:
    clauses, params = [], []
    if relationship:
        clauses.append("relationship = ?")
        params.append(relationship)
    if search:
        clauses.append("(name LIKE ? OR company LIKE ?)")
        params.extend([f"%{search}%", f"%{search}%"])
    where = ("WHERE " + " AND ".join(clauses)) if clauses else ""
    return conn.execute(
        f"SELECT * FROM network_contacts {where} ORDER BY name ASC", params
    ).fetchall()


def get_network_contact(conn: sqlite3.Connection, contact_id: int) -> Optional[sqlite3.Row]:
    return conn.execute(
        "SELECT * FROM network_contacts WHERE id = ?", (contact_id,)
    ).fetchone()


def delete_network_contact(conn: sqlite3.Connection, contact_id: int) -> None:
    conn.execute("DELETE FROM network_contacts WHERE id = ?", (contact_id,))
    conn.commit()


def network_contacts_follow_up_due(conn: sqlite3.Connection) -> List[sqlite3.Row]:
    today = datetime.now().date().isoformat()
    return conn.execute(
        "SELECT * FROM network_contacts WHERE follow_up_date IS NOT NULL "
        "AND follow_up_date <= ? ORDER BY follow_up_date ASC",
        (today,),
    ).fetchall()


# ── Question bank ──────────────────────────────────────────────────────────────

DEFAULT_QUESTIONS = [
    {"question": "Tell me about yourself.",                                   "category": "behavioral",  "tags": "intro,general"},
    {"question": "Describe a time you handled a difficult stakeholder.",       "category": "behavioral",  "tags": "conflict,stakeholder"},
    {"question": "Tell me about a project where you had to learn quickly.",    "category": "behavioral",  "tags": "learning,adaptability"},
    {"question": "Describe a situation where you failed and what you learned.","category": "behavioral",  "tags": "failure,growth"},
    {"question": "Tell me about a time you led without formal authority.",     "category": "leadership",  "tags": "influence,leadership"},
    {"question": "How do you prioritize competing deadlines?",                 "category": "situational", "tags": "prioritization,time-management"},
    {"question": "Describe a time you improved a process.",                    "category": "behavioral",  "tags": "process,improvement"},
    {"question": "Tell me about a time you disagreed with your manager.",      "category": "behavioral",  "tags": "conflict,communication"},
    {"question": "Describe a data-driven decision you made.",                  "category": "behavioral",  "tags": "data,decision-making"},
    {"question": "Why are you interested in this role?",                       "category": "role-specific","tags": "motivation,fit"},
]


def seed_default_questions(conn: sqlite3.Connection) -> None:
    row = conn.execute("SELECT COUNT(*) AS n FROM question_bank").fetchone()
    if row and row["n"] > 0:
        return
    now = _now()
    for q in DEFAULT_QUESTIONS:
        conn.execute(
            "INSERT INTO question_bank (question, category, tags, created_at, updated_at) "
            "VALUES (?, ?, ?, ?, ?)",
            (q["question"], q["category"], q.get("tags", ""), now, now),
        )
    conn.commit()


def add_question(conn: sqlite3.Connection, question: str,
                 category: str = "behavioral", **kwargs) -> int:
    now = _now()
    kwargs["question"]   = question
    kwargs["category"]   = category
    kwargs.setdefault("created_at", now)
    kwargs.setdefault("updated_at", now)
    cols   = ", ".join(kwargs.keys())
    places = ", ".join("?" for _ in kwargs)
    cur = conn.execute(
        f"INSERT INTO question_bank ({cols}) VALUES ({places})", list(kwargs.values())
    )
    conn.commit()
    return cur.lastrowid


def update_question(conn: sqlite3.Connection, question_id: int, **kwargs) -> None:
    kwargs["updated_at"] = _now()
    sets = ", ".join(f"{k} = ?" for k in kwargs)
    conn.execute(f"UPDATE question_bank SET {sets} WHERE id = ?", [*kwargs.values(), question_id])
    conn.commit()


def get_questions(conn: sqlite3.Connection,
                  category: Optional[str] = None) -> List[sqlite3.Row]:
    if category:
        return conn.execute(
            "SELECT * FROM question_bank WHERE category = ? ORDER BY category, id",
            (category,),
        ).fetchall()
    return conn.execute(
        "SELECT * FROM question_bank ORDER BY category, id"
    ).fetchall()


def delete_question(conn: sqlite3.Connection, question_id: int) -> None:
    conn.execute("DELETE FROM question_bank WHERE id = ?", (question_id,))
    conn.commit()


# ── Job annotations ────────────────────────────────────────────────────────────

def upsert_annotation(conn: sqlite3.Connection, job_key: str,
                       note: Optional[str] = None, tag: Optional[str] = None) -> None:
    now = _now()
    conn.execute(
        "INSERT INTO job_annotations (job_key, note, tag, created_at, updated_at) "
        "VALUES (?, ?, ?, ?, ?) "
        "ON CONFLICT(job_key) DO UPDATE SET note = excluded.note, tag = excluded.tag, "
        "updated_at = excluded.updated_at",
        (job_key, note, tag, now, now),
    )
    conn.commit()


def get_annotation(conn: sqlite3.Connection, job_key: str) -> Optional[sqlite3.Row]:
    return conn.execute(
        "SELECT * FROM job_annotations WHERE job_key = ?", (job_key,)
    ).fetchone()


def get_all_annotations(conn: sqlite3.Connection) -> List[sqlite3.Row]:
    return conn.execute("SELECT * FROM job_annotations").fetchall()


def delete_annotation(conn: sqlite3.Connection, job_key: str) -> None:
    conn.execute("DELETE FROM job_annotations WHERE job_key = ?", (job_key,))
    conn.commit()


# ── Company profiles ────────────────────────────────────────────────────────────

def get_company_profile(conn: sqlite3.Connection, name: str) -> Optional[sqlite3.Row]:
    return conn.execute(
        "SELECT * FROM company_profiles WHERE LOWER(name) = LOWER(?)", (name,)
    ).fetchone()


def get_all_company_profiles(conn: sqlite3.Connection, search: Optional[str] = None) -> List[sqlite3.Row]:
    if search:
        return conn.execute(
            "SELECT * FROM company_profiles WHERE name LIKE ? ORDER BY name ASC",
            (f"%{search}%",),
        ).fetchall()
    return conn.execute(
        "SELECT * FROM company_profiles ORDER BY name ASC"
    ).fetchall()


def upsert_company_profile(conn: sqlite3.Connection, name: str, **kwargs) -> int:
    """Insert or update a company profile by name (case-insensitive match)."""
    now = _now()
    existing = get_company_profile(conn, name)
    if existing:
        kwargs["updated_at"] = now
        sets = ", ".join(f"{k} = ?" for k in kwargs)
        conn.execute(
            f"UPDATE company_profiles SET {sets} WHERE id = ?",
            [*kwargs.values(), existing["id"]],
        )
        conn.commit()
        return existing["id"]
    else:
        kwargs["name"] = name
        kwargs.setdefault("created_at", now)
        kwargs.setdefault("updated_at", now)
        cols   = ", ".join(kwargs.keys())
        places = ", ".join("?" for _ in kwargs)
        cur = conn.execute(
            f"INSERT INTO company_profiles ({cols}) VALUES ({places})", list(kwargs.values())
        )
        conn.commit()
        return cur.lastrowid


def delete_company_profile(conn: sqlite3.Connection, profile_id: int) -> None:
    conn.execute("DELETE FROM company_profiles WHERE id = ?", (profile_id,))
    conn.commit()


# ── Chart data ─────────────────────────────────────────────────────────────────

def applications_by_week(conn: sqlite3.Connection, weeks: int = 8) -> List[Dict[str, Any]]:
    """Count of 'applied' events grouped by ISO week, for the last N weeks."""
    cutoff = f"date('now', '-{weeks * 7} days')"
    rows = conn.execute(
        f"""
        SELECT strftime('%Y-W%W', event_date) AS week, COUNT(*) AS count
        FROM events
        WHERE event_type = 'applied'
          AND event_date >= {cutoff}
        GROUP BY week
        ORDER BY week ASC
        """
    ).fetchall()
    return [{"week": r["week"], "count": r["count"]} for r in rows]


# ── CSV Migration ──────────────────────────────────────────────────────────────

def _parse_csv_date(raw: str) -> Optional[str]:
    """Best-effort parse of the messy date formats in ApplicationTracker.csv."""
    if not raw or raw.strip() in ("—", "–", "-", ""):
        return None
    raw = raw.strip()
    for fmt in (
        "%Y-%m-%dT%H:%M:%S",
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%d %H:%M",
        "%Y-%m-%d",
        "%b %d, %Y",
        "%b %d",
        "%b %d %Y",
    ):
        try:
            dt = datetime.strptime(raw.split(".")[0], fmt)
            if dt.year == 1900:          # strptime fills year=1900 when absent
                dt = dt.replace(year=2026)
            return dt.date().isoformat()
        except ValueError:
            continue
    # Last resort: grab the first date-like substring
    m = re.search(r"\d{4}-\d{2}-\d{2}", raw)
    return m.group(0) if m else None


def _map_stage_to_status(stage: str) -> str:
    s = stage.lower().strip()
    if "reject" in s:
        return "rejected"
    if "offer" in s or "accepted" in s:
        return "accepted"
    if "withdrawn" in s or "withdrew" in s:
        return "withdrawn"
    if "round" in s or "interview" in s:
        return "interviewing"
    if "screen" in s:
        return "screening"
    if "applied" in s or "backlog" in s:
        return "applied"
    if "considering" in s:
        return "considering"
    return "applied"


def migrate_from_csv(conn: sqlite3.Connection, csv_path: Path) -> int:
    """
    Import ApplicationTracker.csv into the database.
    Skips rows that already exist (matched on company + role).
    Returns the number of rows imported.
    """
    if not csv_path.exists():
        return 0

    existing = {
        (row["company"].lower(), row["role"].lower())
        for row in conn.execute("SELECT company, role FROM applications").fetchall()
    }

    imported = 0
    with csv_path.open(encoding="utf-8", errors="replace") as f:
        for row in csv.DictReader(f):
            company = (row.get("Company") or "").strip()
            role    = (row.get("Role")    or "").strip()
            if not company or not role:
                continue
            if (company.lower(), role.lower()) in existing:
                continue

            stage        = (row.get("Stage")    or "").strip()
            status       = _map_stage_to_status(stage)
            date_applied = _parse_csv_date(row.get("Applied Date") or "")
            date_closed  = date_applied if status in ("rejected", "accepted", "withdrawn") else None

            # Salary: strip stars / non-numeric clutter
            raw_sal = re.sub(r"[⭐★\*]", "", row.get("Salary Range") or "").strip()

            # Fit stars: count star characters
            raw_fit = row.get("Fit") or ""
            stars   = raw_fit.count("⭐") or raw_fit.count("★") or None

            contact_name = (row.get("Contact") or "").strip()
            if contact_name in ("—", "–", "-"):
                contact_name = ""

            referral = (row.get("Referral") or "").strip()
            if referral in ("—", "–", "-"):
                referral = ""

            app_id = add_application(
                conn,
                company      = company,
                role         = role,
                source       = "manual",
                status       = status,
                fit_stars    = stars,
                salary_range = raw_sal or None,
                referral     = referral or None,
                jd_summary   = (row.get("JD Summary / Key Requirements") or "").strip() or None,
                notes        = (row.get("Notes") or "").strip() or None,
                date_applied = date_applied,
                date_closed  = date_closed,
            )

            # Seed the timeline with an "applied" event
            if date_applied:
                add_event(conn, app_id, "applied", date_applied,
                          title=f"Applied to {company}", notes="")

            # Seed rejection event
            if status == "rejected":
                add_event(conn, app_id, "rejected",
                          date_closed or date_applied or _now()[:10],
                          title="Rejected", notes=stage)

            # Seed interview event from Interview Date column
            interview_raw = (row.get("Interview Date") or "").strip()
            if interview_raw and interview_raw not in ("—", "–", "-", ""):
                iv_date = _parse_csv_date(interview_raw)
                if iv_date:
                    add_event(conn, app_id, "interview_scheduled", iv_date,
                              title="Interview / screening", notes=interview_raw)

            # Seed contact
            if contact_name:
                add_contact(conn, app_id, name=contact_name,
                            role_in_process="recruiter")

            existing.add((company.lower(), role.lower()))
            imported += 1

    return imported
