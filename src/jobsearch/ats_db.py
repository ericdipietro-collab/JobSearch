"""SQLite compatibility layer for the Streamlit ATS product."""

from __future__ import annotations

import csv
import hashlib
import re
import sqlite3
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

from pydantic import BaseModel

from jobsearch.config.settings import settings

_BASE_DIR = settings.base_dir
BASE_DIR = settings.base_dir
DB_PATH = settings.db_path

STATUSES = [
    "exploring",
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
    "exploring": "#7c3aed",
    "considering": "#6b7280",
    "applied": "#3b82f6",
    "screening": "#8b5cf6",
    "interviewing": "#f59e0b",
    "offer": "#10b981",
    "accepted": "#059669",
    "rejected": "#ef4444",
    "withdrawn": "#9ca3af",
}

ENTRY_TYPES = ["application", "opportunity", "job_fair"]
TRAINING_STATUSES = ["planned", "in_progress", "completed", "paused"]
TRAINING_CATEGORIES = [
    "AI / ML",
    "Cloud",
    "Data & Analytics",
    "Programming",
    "Certification Prep",
    "Business / Leadership",
    "Other",
]
TRAINING_PROVIDERS = [
    "AWS",
    "Snowflake",
    "Google",
    "Microsoft",
    "Coursera",
    "Udemy",
    "LinkedIn Learning",
    "freeCodeCamp",
    "Pluralsight",
    "Other",
]
TRAINING_STATUS_COLORS = {
    "planned": "#6b7280",
    "in_progress": "#f59e0b",
    "completed": "#10b981",
    "paused": "#9ca3af",
}
EVENT_TYPES = [
    "applied",
    "conversation",
    "networking_call",
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
INTERVIEW_TYPES = ["phone_screen", "video", "onsite", "panel", "take_home", "final"]
INTERVIEW_FORMATS = ["behavioral", "technical", "case_study", "mixed", "other"]
CONTACT_ROLES = ["recruiter", "hiring_manager", "interviewer", "referral", "network_contact", "other"]
OUTCOME_OPTIONS = ["pending", "passed", "failed"]
NETWORK_RELATIONSHIPS = ["former_colleague", "recruiter", "mentor", "referral", "friend", "other"]
QUESTION_CATEGORIES = ["behavioral", "situational", "leadership", "role-specific", "technical", "other"]

EVENT_LABELS = {
    "applied": "Submitted application",
    "conversation": "Networking conversation",
    "networking_call": "Networking call",
    "recruiter_outreach": "Recruiter contact",
    "screening_scheduled": "Phone screen scheduled",
    "screening_complete": "Phone screen completed",
    "interview_scheduled": "Interview scheduled",
    "interview_complete": "Interview completed",
    "offer_received": "Received offer",
    "offer_negotiating": "Negotiating offer",
    "offer_accepted": "Accepted offer",
    "offer_declined": "Declined offer",
    "rejected": "Received rejection",
    "withdrawn": "Withdrew application",
    "follow_up_sent": "Sent follow-up",
    "note": "Note",
}

REPORTABLE_EVENT_TYPES = {
    "applied",
    "conversation",
    "networking_call",
    "recruiter_outreach",
    "screening_scheduled",
    "screening_complete",
    "interview_scheduled",
    "interview_complete",
    "follow_up_sent",
}


class Opportunity(BaseModel):
    id: str
    company: str
    role_title_raw: str
    role_title_normalized: str = ""
    location: str = ""
    is_remote: bool = False
    source: str = ""
    url: str
    salary_min: Optional[float] = None
    salary_max: Optional[float] = None
    salary_text: str = ""
    work_type: str = ""
    compensation_unit: str = ""
    hourly_rate: Optional[float] = None
    hours_per_week: Optional[float] = None
    weeks_per_year: Optional[float] = None
    normalized_compensation_usd: Optional[float] = None
    score: float = 0.0
    fit_band: str = ""
    current_stage: str = "New"
    date_discovered: Optional[str] = None
    last_updated: Optional[str] = None
    date_applied: Optional[str] = None
    adapter: str = ""
    tier: int = 4
    matched_keywords: str = ""
    penalized_keywords: str = ""
    decision_reason: str = ""
    description_excerpt: str = ""
    user_priority: int = 0
    notes: str = ""

    @staticmethod
    def make_id(company: str, title: str, url: str) -> str:
        raw = f"{company.lower()}|{title.lower()}|{url.lower()}"
        return hashlib.md5(raw.encode()).hexdigest()


Job = Opportunity


class Activity(BaseModel):
    id: Optional[int] = None
    opportunity_id: str
    activity_type: str
    scheduled_date: Optional[str] = None
    interviewer: str = ""
    notes: str = ""
    created_at: Optional[str] = None


class StageHistory(BaseModel):
    id: Optional[int] = None
    opportunity_id: str
    from_stage: Optional[str] = None
    to_stage: str
    timestamp: str
    note: str = ""


class RejectedJob(BaseModel):
    company: str
    title: str
    location: str = ""
    url: str = ""
    source: str = ""
    description: str = ""
    posted_dt: Optional[datetime] = None
    drop_stage: str = ""
    drop_reason: str = ""


DEFAULT_TEMPLATES = [
    {
        "name": "Follow-Up After Application",
        "template_type": "follow_up",
        "subject": "Following up on {role} application at {company}",
        "body": "Hi {contact_name},\n\nI wanted to follow up on my application for the {role} role at {company}. I remain very interested and would welcome the chance to speak further.\n\nBest,\n{my_name}",
    },
    {
        "name": "Thank You",
        "template_type": "thank_you",
        "subject": "Thank you — {role} interview",
        "body": "Hi {contact_name},\n\nThank you for taking the time to speak with me about the {role} role at {company}. I enjoyed learning more about the team and remain excited about the opportunity.\n\nBest,\n{my_name}",
    },
]

DEFAULT_QUESTIONS = [
    {"question": "Tell me about yourself.", "category": "behavioral", "tags": "intro,general"},
    {"question": "Describe a time you handled a difficult stakeholder.", "category": "behavioral", "tags": "conflict,stakeholder"},
    {"question": "Describe a time you improved a process.", "category": "behavioral", "tags": "process,improvement"},
]


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _add_columns_if_missing(conn: sqlite3.Connection, table: str, columns: Dict[str, str]) -> None:
    existing = {row[1] for row in conn.execute(f"PRAGMA table_info({table})")}
    for col, typedef in columns.items():
        if col not in existing:
            conn.execute(f"ALTER TABLE {table} ADD COLUMN {col} {typedef}")


def get_connection() -> sqlite3.Connection:
    settings.results_dir.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(DB_PATH))
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")
    init_db(conn)
    return conn


def init_db(conn: sqlite3.Connection) -> None:
    conn.executescript(
        """
        CREATE TABLE IF NOT EXISTS applications (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            company TEXT NOT NULL,
            role TEXT NOT NULL,
            job_url TEXT,
            source TEXT,
            scraper_key TEXT,
            status TEXT NOT NULL DEFAULT 'considering',
            fit_stars INTEGER,
            user_priority INTEGER,
            tier INTEGER,
            score REAL,
            fit_band TEXT,
            salary_range TEXT,
            salary_low INTEGER,
            salary_high INTEGER,
            salary_text TEXT,
            work_type TEXT,
            compensation_unit TEXT,
            hourly_rate REAL,
            hours_per_week REAL,
            weeks_per_year REAL,
            normalized_compensation_usd REAL,
            referral TEXT,
            jd_summary TEXT,
            notes TEXT,
            location TEXT,
            date_discovered TEXT,
            date_added TEXT,
            date_applied TEXT,
            date_closed TEXT,
            follow_up_date TEXT,
            follow_up_notes TEXT,
            resume_version TEXT,
            cover_letter_notes TEXT,
            entry_type TEXT NOT NULL DEFAULT 'application',
            resume_url TEXT,
            cover_letter_url TEXT,
            job_description TEXT,
            prep_company TEXT,
            prep_why TEXT,
            prep_tyabt TEXT,
            prep_questions TEXT,
            prep_notes TEXT,
            offer_base INTEGER,
            offer_bonus_pct INTEGER,
            offer_equity TEXT,
            offer_signing INTEGER,
            offer_pto_days INTEGER,
            offer_k401_match TEXT,
            offer_remote_policy TEXT,
            offer_start_date TEXT,
            offer_expiry_date TEXT,
            offer_notes TEXT,
            nego_target_base INTEGER,
            nego_walkaway_base INTEGER,
            nego_market_low INTEGER,
            nego_market_high INTEGER,
            nego_notes TEXT,
            matched_keywords TEXT,
            penalized_keywords TEXT,
            decision_reason TEXT,
            description_excerpt TEXT,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS events (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            application_id INTEGER NOT NULL REFERENCES applications(id) ON DELETE CASCADE,
            event_type TEXT NOT NULL,
            event_date TEXT NOT NULL,
            title TEXT,
            notes TEXT,
            created_at TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS contacts (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            application_id INTEGER NOT NULL REFERENCES applications(id) ON DELETE CASCADE,
            name TEXT NOT NULL,
            title TEXT,
            email TEXT,
            phone TEXT,
            linkedin_url TEXT,
            role_in_process TEXT,
            notes TEXT,
            created_at TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS interviews (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            application_id INTEGER NOT NULL REFERENCES applications(id) ON DELETE CASCADE,
            round_number INTEGER,
            interview_type TEXT,
            scheduled_at TEXT,
            duration_mins INTEGER,
            interviewer_names TEXT,
            format TEXT,
            location TEXT,
            prep_notes TEXT,
            outcome TEXT DEFAULT 'pending',
            outcome_notes TEXT,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS training (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL,
            provider TEXT,
            category TEXT,
            status TEXT NOT NULL DEFAULT 'planned',
            url TEXT,
            start_date TEXT,
            target_date TEXT,
            completion_date TEXT,
            certificate_url TEXT,
            estimated_hours INTEGER,
            weekly_hours INTEGER,
            notes TEXT,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS settings (
            key TEXT PRIMARY KEY,
            value TEXT NOT NULL,
            updated_at TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS email_templates (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL,
            template_type TEXT NOT NULL DEFAULT 'other',
            subject TEXT,
            body TEXT NOT NULL,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS journal_entries (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            entry_date TEXT NOT NULL,
            mood TEXT,
            content TEXT NOT NULL,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS network_contacts (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL,
            company TEXT,
            title TEXT,
            email TEXT,
            phone TEXT,
            linkedin_url TEXT,
            relationship TEXT,
            notes TEXT,
            last_contact_date TEXT,
            follow_up_date TEXT,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS question_bank (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            question TEXT NOT NULL,
            category TEXT NOT NULL DEFAULT 'behavioral',
            star_situation TEXT,
            star_task TEXT,
            star_action TEXT,
            star_result TEXT,
            tags TEXT,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS job_annotations (
            job_key TEXT PRIMARY KEY,
            note TEXT,
            tag TEXT,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS company_profiles (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL UNIQUE,
            website_url TEXT,
            linkedin_url TEXT,
            glassdoor_url TEXT,
            about TEXT,
            culture_notes TEXT,
            interview_process TEXT,
            red_flags TEXT,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS stage_history (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            application_id INTEGER NOT NULL REFERENCES applications(id) ON DELETE CASCADE,
            from_stage TEXT,
            to_stage TEXT NOT NULL,
            note TEXT,
            timestamp TEXT NOT NULL
        );
        """
    )
    _add_columns_if_missing(
        conn,
        "applications",
        {
            "fit_stars": "INTEGER",
            "user_priority": "INTEGER",
            "tier": "INTEGER",
            "score": "REAL",
            "fit_band": "TEXT",
            "salary_range": "TEXT",
            "salary_low": "INTEGER",
            "salary_high": "INTEGER",
            "salary_text": "TEXT",
            "work_type": "TEXT",
            "compensation_unit": "TEXT",
            "hourly_rate": "REAL",
            "hours_per_week": "REAL",
            "weeks_per_year": "REAL",
            "normalized_compensation_usd": "REAL",
            "referral": "TEXT",
            "jd_summary": "TEXT",
            "notes": "TEXT",
            "location": "TEXT",
            "date_discovered": "TEXT",
            "date_added": "TEXT",
            "date_applied": "TEXT",
            "date_closed": "TEXT",
            "follow_up_date": "TEXT",
            "follow_up_notes": "TEXT",
            "resume_version": "TEXT",
            "cover_letter_notes": "TEXT",
            "entry_type": "TEXT NOT NULL DEFAULT 'application'",
            "resume_url": "TEXT",
            "cover_letter_url": "TEXT",
            "job_description": "TEXT",
            "prep_company": "TEXT",
            "prep_why": "TEXT",
            "prep_tyabt": "TEXT",
            "prep_questions": "TEXT",
            "prep_notes": "TEXT",
            "offer_base": "INTEGER",
            "offer_bonus_pct": "INTEGER",
            "offer_equity": "TEXT",
            "offer_signing": "INTEGER",
            "offer_pto_days": "INTEGER",
            "offer_k401_match": "TEXT",
            "offer_remote_policy": "TEXT",
            "offer_start_date": "TEXT",
            "offer_expiry_date": "TEXT",
            "offer_notes": "TEXT",
            "nego_target_base": "INTEGER",
            "nego_walkaway_base": "INTEGER",
            "nego_market_low": "INTEGER",
            "nego_market_high": "INTEGER",
            "nego_notes": "TEXT",
            "matched_keywords": "TEXT",
            "penalized_keywords": "TEXT",
            "decision_reason": "TEXT",
            "description_excerpt": "TEXT",
        },
    )
    conn.execute("CREATE INDEX IF NOT EXISTS idx_events_app_date ON events(application_id, event_date)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_interviews_app_date ON interviews(application_id, scheduled_at)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_apps_status ON applications(status)")
    seed_default_templates(conn)
    seed_default_questions(conn)
    conn.commit()


def add_application(conn: sqlite3.Connection, **kwargs) -> int:
    now = _now()
    kwargs.setdefault("status", "considering")
    kwargs.setdefault("created_at", now)
    kwargs.setdefault("updated_at", now)
    cols = ", ".join(kwargs.keys())
    places = ", ".join("?" for _ in kwargs)
    cur = conn.execute(f"INSERT INTO applications ({cols}) VALUES ({places})", list(kwargs.values()))
    app_id = cur.lastrowid
    conn.execute(
        "INSERT INTO stage_history (application_id, from_stage, to_stage, note, timestamp) VALUES (?, ?, ?, ?, ?)",
        (app_id, None, kwargs["status"], "created", now),
    )
    conn.commit()
    return app_id


def update_application(conn: sqlite3.Connection, app_id: int, **kwargs) -> None:
    current = conn.execute("SELECT status FROM applications WHERE id = ?", (app_id,)).fetchone()
    old_status = current["status"] if current else None
    new_status = kwargs.get("status", old_status)
    kwargs["updated_at"] = _now()
    sets = ", ".join(f"{k} = ?" for k in kwargs)
    conn.execute(f"UPDATE applications SET {sets} WHERE id = ?", [*kwargs.values(), app_id])
    if new_status and new_status != old_status:
        conn.execute(
            "INSERT INTO stage_history (application_id, from_stage, to_stage, note, timestamp) VALUES (?, ?, ?, ?, ?)",
            (app_id, old_status, new_status, "status update", _now()),
        )
    conn.commit()


def get_application(conn: sqlite3.Connection, app_id: int) -> Optional[sqlite3.Row]:
    return conn.execute(
        """
        SELECT *,
               COALESCE(fit_stars, user_priority, 0) AS fit_stars,
               COALESCE(user_priority, fit_stars, 0) AS user_priority
        FROM applications
        WHERE id = ?
        """,
        (app_id,),
    ).fetchone()


def get_applications(
    conn: sqlite3.Connection,
    status: Optional[str] = None,
    entry_type: Optional[str] = None,
) -> List[sqlite3.Row]:
    clauses = []
    params: List[Any] = []
    if status:
        clauses.append("status = ?")
        params.append(status)
    if entry_type:
        clauses.append("entry_type = ?")
        params.append(entry_type)
    where = f"WHERE {' AND '.join(clauses)}" if clauses else ""
    return conn.execute(
        f"""
        SELECT *,
               COALESCE(fit_stars, user_priority, 0) AS fit_stars,
               COALESCE(user_priority, fit_stars, 0) AS user_priority
        FROM applications
        {where}
        ORDER BY COALESCE(date_applied, date_discovered, created_at) DESC, updated_at DESC
        """,
        params,
    ).fetchall()


def delete_application(conn: sqlite3.Connection, app_id: int) -> None:
    conn.execute("DELETE FROM applications WHERE id = ?", (app_id,))
    conn.commit()


def add_event(conn: sqlite3.Connection, application_id: int, event_type: str, event_date: str, title: str = "", notes: str = "") -> int:
    cur = conn.execute(
        "INSERT INTO events (application_id, event_type, event_date, title, notes, created_at) VALUES (?, ?, ?, ?, ?, ?)",
        (application_id, event_type, event_date, title, notes, _now()),
    )
    conn.commit()
    return cur.lastrowid


def get_events(conn: sqlite3.Connection, application_id: int) -> List[sqlite3.Row]:
    return conn.execute(
        "SELECT * FROM events WHERE application_id = ? ORDER BY event_date ASC, id ASC",
        (application_id,),
    ).fetchall()


def delete_event(conn: sqlite3.Connection, event_id: int) -> None:
    conn.execute("DELETE FROM events WHERE id = ?", (event_id,))
    conn.commit()


def add_contact(conn: sqlite3.Connection, application_id: int, **kwargs) -> int:
    kwargs["application_id"] = application_id
    kwargs.setdefault("created_at", _now())
    cols = ", ".join(kwargs.keys())
    places = ", ".join("?" for _ in kwargs)
    cur = conn.execute(f"INSERT INTO contacts ({cols}) VALUES ({places})", list(kwargs.values()))
    conn.commit()
    return cur.lastrowid


def update_contact(conn: sqlite3.Connection, contact_id: int, **kwargs) -> None:
    sets = ", ".join(f"{k} = ?" for k in kwargs)
    conn.execute(f"UPDATE contacts SET {sets} WHERE id = ?", [*kwargs.values(), contact_id])
    conn.commit()


def get_contacts(conn: sqlite3.Connection, application_id: int) -> List[sqlite3.Row]:
    return conn.execute("SELECT * FROM contacts WHERE application_id = ? ORDER BY created_at ASC", (application_id,)).fetchall()


def delete_contact(conn: sqlite3.Connection, contact_id: int) -> None:
    conn.execute("DELETE FROM contacts WHERE id = ?", (contact_id,))
    conn.commit()


def add_interview(conn: sqlite3.Connection, application_id: int, **kwargs) -> int:
    now = _now()
    kwargs["application_id"] = application_id
    kwargs.setdefault("outcome", "pending")
    kwargs.setdefault("created_at", now)
    kwargs.setdefault("updated_at", now)
    cols = ", ".join(kwargs.keys())
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
    return conn.execute("SELECT * FROM interviews WHERE application_id = ? ORDER BY scheduled_at ASC", (application_id,)).fetchall()


def delete_interview(conn: sqlite3.Connection, interview_id: int) -> None:
    conn.execute("DELETE FROM interviews WHERE id = ?", (interview_id,))
    conn.commit()


def add_training(conn: sqlite3.Connection, **kwargs) -> int:
    now = _now()
    kwargs.setdefault("status", "planned")
    kwargs.setdefault("created_at", now)
    kwargs.setdefault("updated_at", now)
    cols = ", ".join(kwargs.keys())
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
        return conn.execute("SELECT * FROM training WHERE status = ? ORDER BY updated_at DESC, name ASC", (status,)).fetchall()
    return conn.execute("SELECT * FROM training ORDER BY CASE status WHEN 'in_progress' THEN 0 WHEN 'planned' THEN 1 WHEN 'paused' THEN 2 ELSE 3 END, updated_at DESC, name ASC").fetchall()


def delete_training(conn: sqlite3.Connection, training_id: int) -> None:
    conn.execute("DELETE FROM training WHERE id = ?", (training_id,))
    conn.commit()


def training_status_counts(conn: sqlite3.Connection) -> Dict[str, int]:
    rows = conn.execute("SELECT status, COUNT(*) AS n FROM training GROUP BY status").fetchall()
    counts = {row["status"]: row["n"] for row in rows}
    return {status: counts.get(status, 0) for status in TRAINING_STATUSES}


def add_journal_entry(conn: sqlite3.Connection, entry_date: str, content: str, mood: Optional[str] = None) -> int:
    now = _now()
    cur = conn.execute(
        "INSERT INTO journal_entries (entry_date, mood, content, created_at, updated_at) VALUES (?, ?, ?, ?, ?)",
        (entry_date, mood, content, now, now),
    )
    conn.commit()
    return cur.lastrowid


def get_journal_entries(conn: sqlite3.Connection, limit: int = 100) -> List[sqlite3.Row]:
    return conn.execute("SELECT * FROM journal_entries ORDER BY entry_date DESC, created_at DESC LIMIT ?", (limit,)).fetchall()


def update_journal_entry(conn: sqlite3.Connection, entry_id: int, **kwargs) -> None:
    kwargs["updated_at"] = _now()
    sets = ", ".join(f"{k} = ?" for k in kwargs)
    conn.execute(f"UPDATE journal_entries SET {sets} WHERE id = ?", [*kwargs.values(), entry_id])
    conn.commit()


def delete_journal_entry(conn: sqlite3.Connection, entry_id: int) -> None:
    conn.execute("DELETE FROM journal_entries WHERE id = ?", (entry_id,))
    conn.commit()


def add_network_contact(conn: sqlite3.Connection, name: str, **kwargs) -> int:
    now = _now()
    kwargs["name"] = name
    kwargs.setdefault("created_at", now)
    kwargs.setdefault("updated_at", now)
    cols = ", ".join(kwargs.keys())
    places = ", ".join("?" for _ in kwargs)
    cur = conn.execute(f"INSERT INTO network_contacts ({cols}) VALUES ({places})", list(kwargs.values()))
    conn.commit()
    return cur.lastrowid


def update_network_contact(conn: sqlite3.Connection, contact_id: int, **kwargs) -> None:
    kwargs["updated_at"] = _now()
    sets = ", ".join(f"{k} = ?" for k in kwargs)
    conn.execute(f"UPDATE network_contacts SET {sets} WHERE id = ?", [*kwargs.values(), contact_id])
    conn.commit()


def get_network_contacts(conn: sqlite3.Connection, relationship: Optional[str] = None, search: Optional[str] = None) -> List[sqlite3.Row]:
    clauses = []
    params: List[Any] = []
    if relationship:
        clauses.append("relationship = ?")
        params.append(relationship)
    if search:
        clauses.append("(name LIKE ? OR company LIKE ?)")
        params.extend([f"%{search}%", f"%{search}%"])
    where = f"WHERE {' AND '.join(clauses)}" if clauses else ""
    return conn.execute(f"SELECT * FROM network_contacts {where} ORDER BY name ASC", params).fetchall()


def get_network_contact(conn: sqlite3.Connection, contact_id: int) -> Optional[sqlite3.Row]:
    return conn.execute("SELECT * FROM network_contacts WHERE id = ?", (contact_id,)).fetchone()


def delete_network_contact(conn: sqlite3.Connection, contact_id: int) -> None:
    conn.execute("DELETE FROM network_contacts WHERE id = ?", (contact_id,))
    conn.commit()


def network_contacts_follow_up_due(conn: sqlite3.Connection) -> List[sqlite3.Row]:
    today = date.today().isoformat()
    return conn.execute(
        "SELECT * FROM network_contacts WHERE follow_up_date IS NOT NULL AND follow_up_date <= ? ORDER BY follow_up_date ASC",
        (today,),
    ).fetchall()


def add_question(conn: sqlite3.Connection, question: str, category: str = "behavioral", **kwargs) -> int:
    now = _now()
    kwargs["question"] = question
    kwargs["category"] = category
    kwargs.setdefault("created_at", now)
    kwargs.setdefault("updated_at", now)
    cols = ", ".join(kwargs.keys())
    places = ", ".join("?" for _ in kwargs)
    cur = conn.execute(f"INSERT INTO question_bank ({cols}) VALUES ({places})", list(kwargs.values()))
    conn.commit()
    return cur.lastrowid


def update_question(conn: sqlite3.Connection, question_id: int, **kwargs) -> None:
    kwargs["updated_at"] = _now()
    sets = ", ".join(f"{k} = ?" for k in kwargs)
    conn.execute(f"UPDATE question_bank SET {sets} WHERE id = ?", [*kwargs.values(), question_id])
    conn.commit()


def get_questions(conn: sqlite3.Connection, category: Optional[str] = None) -> List[sqlite3.Row]:
    if category:
        return conn.execute("SELECT * FROM question_bank WHERE category = ? ORDER BY id ASC", (category,)).fetchall()
    return conn.execute("SELECT * FROM question_bank ORDER BY category ASC, id ASC").fetchall()


def delete_question(conn: sqlite3.Connection, question_id: int) -> None:
    conn.execute("DELETE FROM question_bank WHERE id = ?", (question_id,))
    conn.commit()


def seed_default_questions(conn: sqlite3.Connection) -> None:
    row = conn.execute("SELECT COUNT(*) AS n FROM question_bank").fetchone()
    if row and row["n"] > 0:
        return
    now = _now()
    for q in DEFAULT_QUESTIONS:
        conn.execute(
            "INSERT INTO question_bank (question, category, tags, created_at, updated_at) VALUES (?, ?, ?, ?, ?)",
            (q["question"], q["category"], q.get("tags", ""), now, now),
        )


def add_template(conn: sqlite3.Connection, **kwargs) -> int:
    now = _now()
    kwargs.setdefault("created_at", now)
    kwargs.setdefault("updated_at", now)
    cols = ", ".join(kwargs.keys())
    places = ", ".join("?" for _ in kwargs)
    cur = conn.execute(f"INSERT INTO email_templates ({cols}) VALUES ({places})", list(kwargs.values()))
    conn.commit()
    return cur.lastrowid


def update_template(conn: sqlite3.Connection, template_id: int, **kwargs) -> None:
    kwargs["updated_at"] = _now()
    sets = ", ".join(f"{k} = ?" for k in kwargs)
    conn.execute(f"UPDATE email_templates SET {sets} WHERE id = ?", [*kwargs.values(), template_id])
    conn.commit()


def delete_template(conn: sqlite3.Connection, template_id: int) -> None:
    conn.execute("DELETE FROM email_templates WHERE id = ?", (template_id,))
    conn.commit()


def get_template(conn: sqlite3.Connection, template_id: int) -> Optional[sqlite3.Row]:
    return conn.execute("SELECT * FROM email_templates WHERE id = ?", (template_id,)).fetchone()


def get_all_templates(conn: sqlite3.Connection) -> List[sqlite3.Row]:
    return conn.execute("SELECT * FROM email_templates ORDER BY template_type ASC, name ASC").fetchall()


def seed_default_templates(conn: sqlite3.Connection) -> None:
    row = conn.execute("SELECT COUNT(*) AS n FROM email_templates").fetchone()
    if row and row["n"] > 0:
        return
    now = _now()
    for t in DEFAULT_TEMPLATES:
        conn.execute(
            "INSERT INTO email_templates (name, template_type, subject, body, created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?)",
            (t["name"], t["template_type"], t.get("subject", ""), t["body"], now, now),
        )


def upsert_annotation(conn: sqlite3.Connection, job_key: str, note: Optional[str] = None, tag: Optional[str] = None) -> None:
    now = _now()
    conn.execute(
        """
        INSERT INTO job_annotations (job_key, note, tag, created_at, updated_at)
        VALUES (?, ?, ?, ?, ?)
        ON CONFLICT(job_key) DO UPDATE SET
            note = excluded.note,
            tag = excluded.tag,
            updated_at = excluded.updated_at
        """,
        (job_key, note, tag, now, now),
    )
    conn.commit()


def get_annotation(conn: sqlite3.Connection, job_key: str) -> Optional[sqlite3.Row]:
    return conn.execute("SELECT * FROM job_annotations WHERE job_key = ?", (job_key,)).fetchone()


def get_all_annotations(conn: sqlite3.Connection) -> List[sqlite3.Row]:
    return conn.execute("SELECT * FROM job_annotations").fetchall()


def delete_annotation(conn: sqlite3.Connection, job_key: str) -> None:
    conn.execute("DELETE FROM job_annotations WHERE job_key = ?", (job_key,))
    conn.commit()


def get_company_profile(conn: sqlite3.Connection, name: str) -> Optional[sqlite3.Row]:
    return conn.execute("SELECT * FROM company_profiles WHERE LOWER(name) = LOWER(?)", (name,)).fetchone()


def get_all_company_profiles(conn: sqlite3.Connection, search: Optional[str] = None) -> List[sqlite3.Row]:
    if search:
        return conn.execute("SELECT * FROM company_profiles WHERE name LIKE ? ORDER BY name ASC", (f"%{search}%",)).fetchall()
    return conn.execute("SELECT * FROM company_profiles ORDER BY name ASC").fetchall()


def upsert_company_profile(conn: sqlite3.Connection, name: str, **kwargs) -> int:
    now = _now()
    existing = get_company_profile(conn, name)
    if existing:
        kwargs["updated_at"] = now
        sets = ", ".join(f"{k} = ?" for k in kwargs)
        conn.execute(f"UPDATE company_profiles SET {sets} WHERE id = ?", [*kwargs.values(), existing["id"]])
        conn.commit()
        return existing["id"]
    kwargs["name"] = name
    kwargs.setdefault("created_at", now)
    kwargs.setdefault("updated_at", now)
    cols = ", ".join(kwargs.keys())
    places = ", ".join("?" for _ in kwargs)
    cur = conn.execute(f"INSERT INTO company_profiles ({cols}) VALUES ({places})", list(kwargs.values()))
    conn.commit()
    return cur.lastrowid


def delete_company_profile(conn: sqlite3.Connection, profile_id: int) -> None:
    conn.execute("DELETE FROM company_profiles WHERE id = ?", (profile_id,))
    conn.commit()


def get_setting(conn: sqlite3.Connection, key: str, default: str = "") -> str:
    row = conn.execute("SELECT value FROM settings WHERE key = ?", (key,)).fetchone()
    return row["value"] if row else default


def set_setting(conn: sqlite3.Connection, key: str, value: str) -> None:
    conn.execute(
        """
        INSERT INTO settings (key, value, updated_at) VALUES (?, ?, ?)
        ON CONFLICT(key) DO UPDATE SET value = excluded.value, updated_at = excluded.updated_at
        """,
        (key, value, _now()),
    )
    conn.commit()


def follow_up_due(conn: sqlite3.Connection) -> List[sqlite3.Row]:
    today = date.today().isoformat()
    return conn.execute(
        """
        SELECT a.*,
               GROUP_CONCAT(c.name, ', ') AS contact_summary,
               MAX(c.email) AS first_contact_email,
               MAX(c.linkedin_url) AS first_contact_linkedin
        FROM applications a
        LEFT JOIN contacts c ON c.application_id = a.id
        WHERE a.follow_up_date IS NOT NULL
          AND a.follow_up_date <= ?
          AND a.status NOT IN ('accepted', 'rejected', 'withdrawn')
        GROUP BY a.id
        ORDER BY a.follow_up_date ASC
        """,
        (today,),
    ).fetchall()


def follow_up_upcoming(conn: sqlite3.Connection, days: int = 3) -> List[sqlite3.Row]:
    start = date.today().isoformat()
    end = (date.today() + timedelta(days=days)).isoformat()
    return conn.execute(
        """
        SELECT a.*,
               GROUP_CONCAT(c.name, ', ') AS contact_summary,
               MAX(c.email) AS first_contact_email,
               MAX(c.linkedin_url) AS first_contact_linkedin
        FROM applications a
        LEFT JOIN contacts c ON c.application_id = a.id
        WHERE a.follow_up_date IS NOT NULL
          AND a.follow_up_date > ?
          AND a.follow_up_date <= ?
          AND a.status NOT IN ('accepted', 'rejected', 'withdrawn')
        GROUP BY a.id
        ORDER BY a.follow_up_date ASC
        """,
        (start, end),
    ).fetchall()


def upcoming_interviews(conn: sqlite3.Connection, days: int = 14, limit: Optional[int] = None) -> List[sqlite3.Row]:
    start = datetime.now().isoformat()
    end = (datetime.now() + timedelta(days=days)).isoformat()
    query = """
        SELECT i.*, a.company, a.role
        FROM interviews i
        JOIN applications a ON a.id = i.application_id
        WHERE i.scheduled_at IS NOT NULL
          AND i.scheduled_at >= ?
          AND i.scheduled_at <= ?
        ORDER BY i.scheduled_at ASC
    """
    params: List[Any] = [start, end]
    if limit is not None:
        query += " LIMIT ?"
        params.append(limit)
    return conn.execute(query, params).fetchall()


def upcoming_interviews_this_week(conn: sqlite3.Connection) -> List[sqlite3.Row]:
    today = date.today()
    week_end = today + timedelta(days=(6 - today.weekday()))
    start = datetime.combine(today, datetime.min.time()).isoformat()
    end = datetime.combine(week_end, datetime.max.time()).isoformat()
    return conn.execute(
        """
        SELECT i.*, a.company, a.role
        FROM interviews i
        JOIN applications a ON a.id = i.application_id
        WHERE i.scheduled_at IS NOT NULL
          AND i.scheduled_at >= ?
          AND i.scheduled_at <= ?
        ORDER BY i.scheduled_at ASC
        """,
        (start, end),
    ).fetchall()


def weekly_activity_count(conn: sqlite3.Connection, start_date: str, end_date: str) -> int:
    row = conn.execute(
        """
        SELECT COUNT(*) AS n
        FROM events
        WHERE event_type IN ({})
          AND substr(event_date, 1, 10) >= ?
          AND substr(event_date, 1, 10) <= ?
        """.format(",".join("?" for _ in REPORTABLE_EVENT_TYPES)),
        [*sorted(REPORTABLE_EVENT_TYPES), start_date, end_date],
    ).fetchone()
    return row["n"] if row else 0


def apply_now_count(conn: sqlite3.Connection) -> int:
    row = conn.execute("SELECT COUNT(*) AS n FROM applications WHERE status IN ('applied', 'screening', 'interviewing', 'offer')").fetchone()
    return row["n"] if row else 0


def pipeline_snapshot(conn: sqlite3.Connection) -> Dict[str, int]:
    rows = conn.execute("SELECT status, COUNT(*) AS n FROM applications GROUP BY status").fetchall()
    counts = {row["status"]: row["n"] for row in rows}
    return {status: counts.get(status, 0) for status in STATUSES}


def get_recent_events(conn: sqlite3.Connection, limit: int = 10) -> List[sqlite3.Row]:
    return conn.execute(
        """
        SELECT e.*, a.company, a.role
        FROM events e
        JOIN applications a ON a.id = e.application_id
        ORDER BY e.event_date DESC, e.id DESC
        LIMIT ?
        """,
        (limit,),
    ).fetchall()


def applications_by_week(conn: sqlite3.Connection, weeks: int = 8) -> List[Dict[str, Any]]:
    rows = conn.execute(
        f"""
        SELECT strftime('%Y-W%W', event_date) AS week, COUNT(*) AS count
        FROM events
        WHERE event_type = 'applied'
          AND date(substr(event_date, 1, 10)) >= date('now', '-{weeks * 7} days')
        GROUP BY week
        ORDER BY week ASC
        """
    ).fetchall()
    return [{"week": row["week"], "count": row["count"]} for row in rows]


def get_activity_report(conn: sqlite3.Connection, start_date: str, end_date: str) -> List[sqlite3.Row]:
    return conn.execute(
        """
        SELECT e.event_type,
               e.event_date,
               e.title AS event_title,
               e.notes AS event_notes,
               a.company,
               a.role,
               a.entry_type,
               GROUP_CONCAT(c.name, ', ') AS contact_names
        FROM events e
        JOIN applications a ON a.id = e.application_id
        LEFT JOIN contacts c ON c.application_id = a.id
        WHERE e.event_type IN ({})
          AND substr(e.event_date, 1, 10) >= ?
          AND substr(e.event_date, 1, 10) <= ?
        GROUP BY e.id
        ORDER BY e.event_date DESC, e.id DESC
        """.format(",".join("?" for _ in REPORTABLE_EVENT_TYPES)),
        [*sorted(REPORTABLE_EVENT_TYPES), start_date, end_date],
    ).fetchall()


def get_training_for_report(conn: sqlite3.Connection, start_date: str, end_date: str) -> List[sqlite3.Row]:
    return conn.execute(
        """
        SELECT *
        FROM training
        WHERE (completion_date IS NOT NULL AND completion_date >= ? AND completion_date <= ?)
           OR (
                status IN ('planned', 'in_progress')
                AND (
                    (start_date IS NULL OR start_date <= ?)
                    AND (target_date IS NULL OR target_date >= ?)
                )
           )
        ORDER BY updated_at DESC, name ASC
        """,
        (start_date, end_date, end_date, start_date),
    ).fetchall()


def get_applications_with_offers(conn: sqlite3.Connection) -> List[sqlite3.Row]:
    return conn.execute("SELECT * FROM applications WHERE status IN ('offer', 'accepted') ORDER BY updated_at DESC").fetchall()


def bulk_update_status(conn: sqlite3.Connection, app_ids: List[int], status: str) -> int:
    if not app_ids:
        return 0
    updated = 0
    for app_id in app_ids:
        update_application(conn, app_id, status=status)
        updated += 1
    return updated


def bulk_delete_applications(conn: sqlite3.Connection, app_ids: List[int]) -> int:
    if not app_ids:
        return 0
    placeholders = ",".join("?" for _ in app_ids)
    cur = conn.execute(f"DELETE FROM applications WHERE id IN ({placeholders})", app_ids)
    conn.commit()
    return cur.rowcount


def status_counts(conn: sqlite3.Connection) -> Dict[str, int]:
    return pipeline_snapshot(conn)


def _parse_csv_date(raw: str) -> Optional[str]:
    if not raw or raw.strip() in {"—", "–", "-", ""}:
        return None
    raw = raw.strip()
    for fmt in ("%Y-%m-%dT%H:%M:%S", "%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M", "%Y-%m-%d", "%b %d, %Y", "%b %d", "%b %d %Y"):
        try:
            dt = datetime.strptime(raw.split(".")[0], fmt)
            if dt.year == 1900:
                dt = dt.replace(year=datetime.now().year)
            return dt.date().isoformat()
        except ValueError:
            continue
    match = re.search(r"\d{4}-\d{2}-\d{2}", raw)
    return match.group(0) if match else None


def _map_stage_to_status(stage: str) -> str:
    s = stage.lower().strip()
    if "reject" in s:
        return "rejected"
    if "offer" in s:
        return "offer"
    if "accept" in s:
        return "accepted"
    if "withdraw" in s:
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
    if not csv_path.exists():
        return 0
    existing = {(row["company"].lower(), row["role"].lower()) for row in conn.execute("SELECT company, role FROM applications").fetchall()}
    imported = 0
    with csv_path.open(encoding="utf-8", errors="replace") as handle:
        for row in csv.DictReader(handle):
            company = (row.get("Company") or "").strip()
            role = (row.get("Role") or "").strip()
            if not company or not role:
                continue
            if (company.lower(), role.lower()) in existing:
                continue
            stage = (row.get("Stage") or "").strip()
            status = _map_stage_to_status(stage)
            date_applied = _parse_csv_date(row.get("Applied Date") or "")
            date_closed = date_applied if status in {"rejected", "accepted", "withdrawn"} else None
            raw_sal = re.sub(r"[⭐★*]", "", row.get("Salary Range") or "").strip()
            raw_fit = row.get("Fit") or ""
            stars = raw_fit.count("⭐") or raw_fit.count("★") or None
            referral = (row.get("Referral") or "").strip()
            app_id = add_application(
                conn,
                company=company,
                role=role,
                source="manual",
                status=status,
                fit_stars=stars,
                salary_range=raw_sal or None,
                referral=referral or None,
                jd_summary=(row.get("JD Summary / Key Requirements") or "").strip() or None,
                notes=(row.get("Notes") or "").strip() or None,
                date_applied=date_applied,
                date_closed=date_closed,
            )
            if date_applied:
                add_event(conn, app_id, "applied", date_applied, title=f"Applied to {company}")
            interview_raw = (row.get("Interview Date") or "").strip()
            iv_date = _parse_csv_date(interview_raw)
            if iv_date:
                add_event(conn, app_id, "interview_scheduled", iv_date, title="Interview / screening", notes=interview_raw)
            contact_name = (row.get("Contact") or "").strip()
            if contact_name and contact_name not in {"—", "–", "-"}:
                add_contact(conn, app_id, name=contact_name, role_in_process="recruiter")
            existing.add((company.lower(), role.lower()))
            imported += 1
    return imported
