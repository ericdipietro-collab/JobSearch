"""Runtime compatibility shims for the JobSearch repo.

Python automatically imports ``sitecustomize`` on startup when it is importable
from ``sys.path``. Running scripts from the repository root makes this file
available, which lets us patch small boot-time issues without requiring the
main script to import anything manually.
"""

from __future__ import annotations

import builtins
from pathlib import Path


def _repo_base_dir() -> Path:
    return Path(__file__).resolve().parent


def _default_company_registry_candidates() -> list[Path]:
    base_dir = _repo_base_dir()
    return [
        base_dir / "config" / "job_search_companies.yaml",
        base_dir / "job_search_companies_test.yaml",
        base_dir / "job_search_companies.yaml",
        base_dir / "config" / "companies.yaml",
        base_dir / "companies.yaml",
    ]


def _default_preferences_candidates() -> list[Path]:
    base_dir = _repo_base_dir()
    return [
        base_dir / "config" / "job_search_preferences.yaml",
        base_dir / "job_search_preferences_tiered_softdrop_final_patched2.yaml",
        base_dir / "job_search_preferences_tiered_softdrop_final_patched.yaml",
        base_dir / "job_search_preferences_tiered_softdrop_final.yaml",
        base_dir / "job_search_preferences_transparent_final.yaml",
        base_dir / "job_search_preferences.yaml",
        base_dir / "config" / "preferences.yaml",
        base_dir / "preferences.yaml",
    ]


if not hasattr(builtins, "COMPANY_REGISTRY_FILE_CANDIDATES"):
    builtins.COMPANY_REGISTRY_FILE_CANDIDATES = _default_company_registry_candidates()

if not hasattr(builtins, "PREFERENCES_FILE_CANDIDATES"):
    builtins.PREFERENCES_FILE_CANDIDATES = _default_preferences_candidates()
