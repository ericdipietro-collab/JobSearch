"""
First-run setup checklist — surfaces on the Home page when the job search
hasn't been fully configured yet.
"""
from __future__ import annotations

import streamlit as st
from jobsearch.config.settings import settings

_PREFS_YAML = settings.prefs_yaml
_COMPANIES_YAML = settings.companies_yaml
_RESULTS_XLSX = settings.results_dir / "job_search_v6_results.xlsx"
_RESULTS_CSV = settings.results_dir / "job_search_v6_results.csv"


def _check_prefs_customized() -> tuple[bool, str]:
    """True if the preferences YAML exists and has been filled in (no placeholder values)."""
    if not _PREFS_YAML.exists():
        return False, "No preferences file found — run the launcher to auto-create it."
    text = _PREFS_YAML.read_text(encoding="utf-8", errors="ignore")
    if len(text.strip()) < 100:
        return False, "Preferences file is empty — copy config/job_search_preferences.example.yaml."
    if "YOUR_ZIP" in text or "YOUR_CITY" in text or "150000" in text:
        return False, "Preferences still have placeholder values — open Search Settings to customise salary and location."
    return True, "Salary, location, and keyword preferences configured."


def _check_companies() -> tuple[bool, str]:
    """True if at least one active company is registered."""
    if not _COMPANIES_YAML.exists():
        return False, "No company registry found at `config/job_search_companies.yaml`."
    try:
        import yaml
        data = yaml.safe_load(_COMPANIES_YAML.read_text(encoding="utf-8")) or {}
        companies = data.get("companies", [])
        active = [c for c in companies if c.get("active", True)]
        if not active:
            return False, "No active companies in the registry."
        return True, f"{len(active)} active company target(s) registered."
    except Exception as exc:
        return False, f"Could not parse company registry: {exc}"


def _check_scraper_run(conn) -> tuple[bool, str]:
    """True if the scraper has run at least once (checks DB metrics or results)."""
    # 1. Check for explicit last run timestamp
    try:
        from jobsearch import ats_db
        last_run = ats_db.get_setting(conn, "last_pipeline_run")
        if last_run:
            import datetime
            try:
                dt = datetime.datetime.fromisoformat(last_run)
                return True, f"Scraper run complete (last run {dt.strftime('%b %d, %Y')})."
            except Exception:
                return True, "Scraper run complete."
    except Exception:
        pass

    # 2. Check health tables for any activity (fallback for older runs or if timestamp missing)
    try:
        row = conn.execute("SELECT COUNT(*) FROM workday_target_health").fetchone()
        if row and row[0] > 0:
            return True, "Scraper results found in health metrics."
        row = conn.execute("SELECT COUNT(*) FROM generic_target_health").fetchone()
        if row and row[0] > 0:
            return True, "Scraper results found in health metrics."
    except Exception:
        pass

    # 3. Check for legacy files (backwards compatibility)
    if _RESULTS_XLSX.exists():
        import datetime
        mtime = datetime.datetime.fromtimestamp(_RESULTS_XLSX.stat().st_mtime)
        return True, f"Legacy results found ({mtime.strftime('%b %d, %Y')})."
    
    return False, "No results yet — run the pipeline from **Run Job Search**."


def _check_first_app(conn) -> tuple[bool, str]:
    """True if at least one application has been added to the tracker."""
    try:
        row = conn.execute("SELECT COUNT(*) FROM applications").fetchone()
        count = row[0] if row else 0
        if count == 0:
            return False, "No applications tracked yet — add your first one in **My Applications**."
        return True, f"{count} application(s) tracked."
    except Exception:
        return False, "Could not read applications database."


def setup_complete(conn) -> bool:
    """Return True only if all checklist items pass (used to hide the wizard)."""
    checks = [
        _check_prefs_customized()[0],
        _check_companies()[0],
        _check_scraper_run(conn)[0],
        _check_first_app(conn)[0],
    ]
    return all(checks)


def render_setup_checklist(conn) -> None:
    """
    Render the setup checklist card.  Call this from render_home() — it renders
    nothing once all items are complete.
    """
    items = [
        ("Configure search preferences",   *_check_prefs_customized(), "Search Settings"),
        ("Register target companies",       *_check_companies(),        "Target Companies"),
        ("Run the job search pipeline",     *_check_scraper_run(conn),   "Run Job Search"),
        ("Track your first application",    *_check_first_app(conn),    "My Applications"),
    ]

    done_count = sum(1 for _, ok, _, _ in items if ok)
    all_done   = done_count == len(items)

    if all_done:
        return  # Nothing to show

    # Build entire card as one HTML string — Streamlit wraps each st.markdown()
    # call in its own DOM element, so a multi-call approach can't share a parent div.
    rows_html = ""
    for label, ok, detail, nav_page in items:
        icon  = "✅" if ok else "⬜"
        color = "#10b981" if ok else "#9ca3af"
        weight = "400" if ok else "600"
        label_color = color if ok else "inherit"
        rows_html += (
            f'<div class="js-setup-row">'
            f'<span style="font-size:1.1rem;line-height:1.4">{icon}</span>'
            f'<div>'
            f'<span style="font-weight:{weight};color:{label_color}">{label}</span>'
            f'<br><span style="font-size:.78rem;color:{color}">{detail}</span>'
            f'</div>'
            f'</div>'
        )

    next_hint = ""
    if done_count < len(items):
        next_nav = next((nav for _, ok, _, nav in items if not ok), None)
        if next_nav:
            next_hint = (
                f'<p class="js-setup-hint">'
                f'Next step: navigate to <strong>{next_nav}</strong> in the sidebar.</p>'
            )

    st.markdown(
        f'<div class="js-setup-card">'
        f'<div class="js-setup-title">'
        f'🚀 Setup Checklist &mdash; {done_count}&thinsp;/&thinsp;{len(items)} complete'
        f'</div>'
        f'{rows_html}'
        f'{next_hint}'
        f'</div>',
        unsafe_allow_html=True,
    )
