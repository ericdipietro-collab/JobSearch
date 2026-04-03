import sqlite3
from pathlib import Path
from typing import Optional

from jobsearch.config.settings import settings
from jobsearch.db.schema import init_db

def get_connection(db_path: Optional[Path] = None) -> sqlite3.Connection:
    """Get a connection to the unified SQLite database."""
    path = db_path or settings.db_path
    path.parent.mkdir(parents=True, exist_ok=True)
    
    conn = sqlite3.connect(str(path))
    conn.row_factory = sqlite3.Row
    
    # Initialize schema if it's a new DB or needs update
    init_db(conn)
    
    return conn
