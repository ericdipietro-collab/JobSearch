"""db/connection.py — SQLite connection helpers."""

from __future__ import annotations

import sqlite3
from contextlib import contextmanager
from pathlib import Path
from typing import Generator

import streamlit as st

from db.schema import init_db

# ── Path resolution ────────────────────────────────────────────────────────────

# BASE_DIR is two levels up from this file (db/ → project root)
BASE_DIR: Path = Path(__file__).resolve().parent.parent
DB_PATH: Path = BASE_DIR / "results" / "jobsearch.db"


# ── Low-level opener ───────────────────────────────────────────────────────────

def get_connection(path: Path = DB_PATH) -> sqlite3.Connection:
    """
    Open (or create) the SQLite database and return a connection with:
      - Row factory so columns are accessible by name
      - Foreign-key enforcement enabled
      - Database and tables initialised if absent
    """
    path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(path), check_same_thread=False)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys=ON")
    init_db(conn)
    return conn


# ── Streamlit-cached resource ─────────────────────────────────────────────────

@st.cache_resource
def get_db() -> sqlite3.Connection:
    """
    Return a single shared connection for the entire Streamlit session.
    Cached with @st.cache_resource so it is created once per server process
    (not once per user interaction).

    NOTE: This connection is long-lived. For writes that must be isolated, use
    open_db() instead.
    """
    return get_connection()


# ── Context manager for transactional writes ──────────────────────────────────

@contextmanager
def open_db(path: Path = DB_PATH) -> Generator[sqlite3.Connection, None, None]:
    """
    Context manager that opens a fresh connection, yields it, and then
    commits on success or rolls back on exception before closing.

    Usage::

        with open_db() as conn:
            conn.execute("INSERT INTO ...")
    """
    conn = get_connection(path)
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()
