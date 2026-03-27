"""CLI-aware launcher with runtime hotfixes for transitional job_search_v6.py.

This wrapper:
1. Injects startup globals before the main script imports.
2. Honors --prefs / --companies overrides early.
3. Applies a small source patch for the known evaluate_job() NameError where
   is_hybrid/is_remote/is_non_us are referenced before assignment.
"""

from __future__ import annotations

import builtins
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


def _prepare_builtins() -> None:
    prefs_override = _resolve_candidate(_extract_arg_value("--prefs") or _extract_arg_value("--preferences"))
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
        BASE_DIR / "config" / "job_search_preferences_tiered_softdrop_final_patched2.yaml",
        BASE_DIR / "config" / "job_search_preferences_tiered_softdrop_final_patched.yaml",
        BASE_DIR / "config" / "job_search_preferences_tiered_softdrop_final.yaml",
        BASE_DIR / "config" / "job_search_preferences_transparent_final.yaml",
        BASE_DIR / "job_search_preferences.yaml",
        BASE_DIR / "config" / "preferences.yaml",
        BASE_DIR / "preferences.yaml",
    ]

    builtins.COMPANY_REGISTRY_FILE_CANDIDATES = [p for p in company_candidates if p is not None]
    builtins.PREFERENCES_FILE_CANDIDATES = [p for p in pref_candidates if p is not None]


def _patch_source(source: str) -> str:
    target = (
        "    role_family = classify_role_family(norm_title)\n"
        "    salary_info = extract_salary_info(desc)\n"
    )
    replacement = (
        "    role_family = classify_role_family(norm_title)\n"
        "    is_remote, is_hybrid, is_non_us = location_flags(location)\n"
        "    salary_info = extract_salary_info(desc)\n"
    )
    if target in source and replacement not in source:
        source = source.replace(target, replacement, 1)
    return source


def main() -> None:
    _prepare_builtins()
    script_path = BASE_DIR / "job_search_v6.py"
    source = script_path.read_text(encoding="utf-8")
    source = _patch_source(source)
    globals_dict = {
        "__name__": "__main__",
        "__file__": str(script_path),
        "__package__": None,
        "__cached__": None,
    }
    exec(compile(source, str(script_path), "exec"), globals_dict)


if __name__ == "__main__":
    main()
