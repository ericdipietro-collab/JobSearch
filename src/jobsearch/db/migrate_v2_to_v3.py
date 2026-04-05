"""Migration script from schema v2 to v3 aggregator-ready application columns."""

from __future__ import annotations

import shutil
import sqlite3
import sys
from pathlib import Path

from jobsearch.config.settings import settings


def _column_names(conn: sqlite3.Connection, table: str) -> set[str]:
    return {str(row[1]) for row in conn.execute(f"PRAGMA table_info({table})").fetchall()}


def migrate(db_path: Path | None = None) -> None:
    path = Path(db_path or settings.db_path)
    if not path.exists():
        print(f"No database found at {path}")
        return

    backup_path = path.with_suffix(path.suffix + ".bak_v3")
    shutil.copy(path, backup_path)
    print(f"Backup created at {backup_path}")

    conn = sqlite3.connect(str(path))
    try:
        before = _column_names(conn, "applications")
        with conn:
            if "source_lane" not in before:
                conn.execute(
                    "ALTER TABLE applications ADD COLUMN source_lane TEXT NOT NULL DEFAULT 'employer_ats'"
                )
            if "canonical_job_url" not in before:
                conn.execute(
                    "ALTER TABLE applications ADD COLUMN canonical_job_url TEXT"
                )
            conn.execute(
                """
                UPDATE applications
                SET source_lane = 'employer_ats'
                WHERE source_lane IS NULL OR TRIM(source_lane) = ''
                """
            )
        after = _column_names(conn, "applications")
        print(f"Applications columns before: {len(before)}")
        print(f"Applications columns after:  {len(after)}")
        print("Migration complete.")
    finally:
        conn.close()


if __name__ == "__main__":
    migrate(Path(sys.argv[1]) if len(sys.argv) > 1 else None)
