"""Compatibility launcher for job_search_v6.py.

This wrapper injects the missing registry/preference candidate globals before
executing the main script. Use it when the repository contains a transitional
job_search_v6.py that still expects those names at import time.
"""

from __future__ import annotations

import builtins
import runpy
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent

builtins.COMPANY_REGISTRY_FILE_CANDIDATES = [
    BASE_DIR / "config" / "job_search_companies.yaml",
    BASE_DIR / "job_search_companies_test.yaml",
    BASE_DIR / "job_search_companies.yaml",
    BASE_DIR / "config" / "companies.yaml",
    BASE_DIR / "companies.yaml",
]

builtins.PREFERENCES_FILE_CANDIDATES = [
    BASE_DIR / "config" / "job_search_preferences.yaml",
    BASE_DIR / "job_search_preferences_tiered_softdrop_final_patched2.yaml",
    BASE_DIR / "job_search_preferences_tiered_softdrop_final_patched.yaml",
    BASE_DIR / "job_search_preferences_tiered_softdrop_final.yaml",
    BASE_DIR / "job_search_preferences_transparent_final.yaml",
    BASE_DIR / "job_search_preferences.yaml",
    BASE_DIR / "config" / "preferences.yaml",
    BASE_DIR / "preferences.yaml",
]

runpy.run_path(str(BASE_DIR / "job_search_v6.py"), run_name="__main__")
