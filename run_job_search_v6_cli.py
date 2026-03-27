"""CLI-aware compatibility launcher for transitional job_search_v6.py.

This wrapper injects missing startup globals *before* job_search_v6.py runs,
including honoring --prefs / --companies command-line overrides that the main
script currently reads too late.
"""

from __future__ import annotations

import builtins
import runpy
import sys
from pathlib import Path
from typing import Optional

BASE_DIR = Path(__file__).resolve().parent


def _extract_arg_value(flag: str) -> Optional[str]:
    argv = sys.argv[1:]
    for i, token in enumerate(argv):
        if token == flag and i + 1 < len(argv):
            return argv[i + 1]
        if token.startswith(flag + "="):
            return token.split("=", 1)[1]
    return None


def _resolve_candidate(path_str: Optional[str]) -> Optional[Path]:
    if not path_str:
        return None
    p = Path(path_str).expanduser()
    if not p.is_absolute():
        p = (BASE_DIR / p).resolve()
    else:
        p = p.resolve()
    return p


prefs_override = _resolve_candidate(_extract_arg_value("--prefs"))
companies_override = _resolve_candidate(_extract_arg_value("--companies"))

builtins.CLI_PREFERENCES_PATH_OVERRIDE = str(prefs_override) if prefs_override else None
builtins.CLI_COMPANIES_PATH_OVERRIDE = str(companies_override) if companies_override else None

company_candidates = [
    companies_override,
    BASE_DIR / "config" / "job_search_companies.yaml",
    BASE_DIR / "job_search_companies_test.yaml",
    BASE_DIR / "job_search_companies.yaml",
    BASE_DIR / "config" / "companies.yaml",
    BASE_DIR / "companies.yaml",
]

pref_candidates = [
    prefs_override,
    BASE_DIR / "config" / "job_search_preferences.yaml",
    BASE_DIR / "job_search_preferences_tiered_softdrop_final_patched2.yaml",
    BASE_DIR / "job_search_preferences_tiered_softdrop_final_patched.yaml",
    BASE_DIR / "job_search_preferences_tiered_softdrop_final.yaml",
    BASE_DIR / "job_search_preferences_transparent_final.yaml",
    BASE_DIR / "job_search_preferences.yaml",
    BASE_DIR / "config" / "preferences.yaml",
    BASE_DIR / "preferences.yaml",
]

builtins.COMPANY_REGISTRY_FILE_CANDIDATES = [p for p in company_candidates if p is not None]
builtins.PREFERENCES_FILE_CANDIDATES = [p for p in pref_candidates if p is not None]

runpy.run_path(str(BASE_DIR / "job_search_v6.py"), run_name="__main__")
