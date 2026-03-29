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
DB_PATH   = _BASE_DIR / "results" / "job_applications.db"

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

ENTRY_TYPES = ["application", "opportunity"]

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
    """)
    # Migrate existing DB — add any columns introduced after initial release
    _add_columns_if_missing(conn, "applications", {
        "follow_up_date":     "TEXT",
        "follow_up_notes":    "TEXT",
        "resume_version":     "TEXT",
        "cover_letter_notes": "TEXT",
        "entry_type":         "TEXT NOT NULL DEFAULT 'application'",
    })
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
    return conn.execute("SELECT * FROM applications WHERE id = ?", (app_id,)).fetchone()


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
        f"SELECT * FROM applications {where} ORDER BY date_applied DESC, created_at DESC",
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
        SELECT a.*, GROUP_CONCAT(c.name || ' (' || COALESCE(c.role_in_process,'') || ')', ', ') AS contact_summary
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
        SELECT a.*, GROUP_CONCAT(c.name || ' (' || COALESCE(c.role_in_process,'') || ')', ', ') AS contact_summary
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
