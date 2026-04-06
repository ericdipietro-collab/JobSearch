from __future__ import annotations

import re
from typing import Any, Dict, List

from jobsearch.config.settings import settings

ALLOWED_JOBSPY_SITES = {
    "google",
    "linkedin",
    "indeed",
    "glassdoor",
    "zip_recruiter",
}

_SITE_ALIASES = {
    "ziprecruiter": "zip_recruiter",
}


def _as_bool(value: Any, default: bool = False) -> bool:
    if value is None:
        return default
    if isinstance(value, bool):
        return value
    text = str(value).strip().lower()
    if text in {"1", "true", "yes", "on"}:
        return True
    if text in {"0", "false", "no", "off"}:
        return False
    return default


def _as_int(value: Any, default: int) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def _split_sites(raw: Any) -> list[str]:
    if isinstance(raw, (list, tuple)):
        parts = [str(item).strip().lower() for item in raw]
    else:
        parts = [part.strip().lower() for part in str(raw or "").split(",")]
    seen: set[str] = set()
    sites: list[str] = []
    for part in parts:
        if not part:
            continue
        normalized = _SITE_ALIASES.get(part, part)
        if normalized in seen:
            continue
        seen.add(normalized)
        sites.append(normalized)
    return sites


def _company_sites(company_config: Dict[str, Any]) -> list[str]:
    return _split_sites(company_config.get("site_names"))


def split_jobspy_queries(raw: Any) -> list[str]:
    if isinstance(raw, (list, tuple)):
        parts = [str(item).strip() for item in raw]
    else:
        text = str(raw or "").replace("\r", "\n")
        parts = [part.strip() for part in re.split(r"[\n|]+", text)]
    return [part for part in parts if part]


def load_jobspy_settings(preferences: Dict[str, Any], company_config: Dict[str, Any]) -> Dict[str, Any]:
    cfg = dict((preferences.get("jobspy_experimental") or {}))
    company_sites = _company_sites(company_config)
    enabled_sites = company_sites or _split_sites(
        cfg.get("enabled_sites")
        or settings.jobspy_site_names
        or "google"
    )
    valid_sites = [site for site in enabled_sites if site in ALLOWED_JOBSPY_SITES]
    invalid_sites = [site for site in enabled_sites if site not in ALLOWED_JOBSPY_SITES]
    if not valid_sites:
        valid_sites = ["google"]
    results_wanted_per_site = max(
        1,
        _as_int(
            company_config.get("results_wanted")
            or cfg.get("results_wanted_per_site")
            or settings.jobspy_results_per_run,
            settings.jobspy_results_per_run,
        ),
    )
    max_total_results = max(
        1,
        _as_int(
            company_config.get("max_total_results")
            or cfg.get("max_total_results")
            or settings.jobspy_max_total_results
            or results_wanted_per_site,
            settings.jobspy_max_total_results or results_wanted_per_site,
        ),
    )
    return {
        "enabled_sites": valid_sites,
        "invalid_sites": invalid_sites,
        "results_wanted_per_site": results_wanted_per_site,
        "hours_old": max(
            1,
            _as_int(
                company_config.get("hours_old")
                or cfg.get("hours_old")
                or settings.jobspy_hours_old,
                settings.jobspy_hours_old,
            ),
        ),
        "country_indeed": str(
            company_config.get("country_indeed")
            or cfg.get("country_indeed")
            or settings.jobspy_country_indeed
            or "USA"
        ).strip()
        or "USA",
        "concurrency": max(
            1,
            _as_int(
                company_config.get("concurrency")
                or cfg.get("concurrency")
                or settings.scrape_jobspy_concurrency,
                settings.scrape_jobspy_concurrency,
            ),
        ),
        "is_remote": _as_bool(company_config.get("is_remote"), _as_bool(cfg.get("is_remote"), settings.jobspy_is_remote)),
        "job_type": str(company_config.get("job_type") or cfg.get("job_type") or settings.jobspy_job_type or "").strip(),
        "linkedin_fetch_description": _as_bool(
            company_config.get("linkedin_fetch_description"),
            _as_bool(cfg.get("linkedin_fetch_description"), settings.jobspy_linkedin_fetch_description),
        ),
        "google_search_term_template": str(
            company_config.get("google_search_term_template")
            or cfg.get("google_search_term_template")
            or settings.jobspy_google_search_term_template
            or "{query}"
        ).strip()
        or "{query}",
        "continue_on_site_failure": _as_bool(
            company_config.get("continue_on_site_failure"),
            _as_bool(cfg.get("continue_on_site_failure"), settings.jobspy_continue_on_site_failure),
        ),
        "max_total_results": max_total_results,
    }


def validate_jobspy_settings(settings_map: Dict[str, Any]) -> list[str]:
    issues: list[str] = []
    invalid_sites = list(settings_map.get("invalid_sites") or [])
    if invalid_sites:
        issues.append(f"Unsupported JobSpy sites: {', '.join(invalid_sites)}")
    enabled_sites = list(settings_map.get("enabled_sites") or [])
    if "indeed" in enabled_sites and not str(settings_map.get("country_indeed") or "").strip():
        issues.append("Indeed requires country_indeed")
    if int(settings_map.get("results_wanted_per_site") or 0) <= 0:
        issues.append("results_wanted_per_site must be positive")
    if int(settings_map.get("max_total_results") or 0) <= 0:
        issues.append("max_total_results must be positive")
    return issues
