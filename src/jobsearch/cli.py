import subprocess
import sys
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

if __package__ in (None, ""):
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import click
import yaml

if sys.platform == "win32":
    try:
        sys.stdout.reconfigure(encoding="utf-8")
        sys.stderr.reconfigure(encoding="utf-8")
    except Exception:
        pass

BASE_DIR = Path(__file__).resolve().parent.parent.parent

from jobsearch.config.settings import rotate_log_file, settings
from jobsearch.scraper.engine import ScraperEngine
from jobsearch.services.healer_service import ATSHealer


# Fields that constitute a meaningful change to a company record.
# last_healed and discovery_method are operational metadata — excluded so the
# changed_count and YAML-rewrite decision are not inflated on every run.
_SUBSTANTIVE_HEAL_FIELDS = frozenset({
    "adapter", "adapter_key", "careers_url", "status", "active", "manual_only",
    "heal_failure_streak", "cooldown_until", "manual_only_suggested",
})


def _sync_heal_cooldown_to_db(company: dict, cooldown_days: int) -> None:
    """Mirror the healer's YAML cooldown into the scraper health DB tables.

    Without this, the scraper would still attempt companies that the healer
    has marked as broken, wasting time and inflating failure counts.
    """
    from jobsearch import ats_db as db
    name = company.get("name", "")
    adapter = str(company.get("adapter", "") or "").lower()
    careers_url = str(company.get("careers_url", "") or "")
    conn = db.get_connection()
    try:
        if adapter in {"workday", "workday_manual"}:
            db.update_workday_target_health(
                conn, company=name, careers_url=careers_url,
                status="blocked", elapsed_ms=0.0, evaluated_count=0,
                cooldown_days=cooldown_days, notes="Healer failure cooldown",
            )
        elif adapter in {"generic", ""} or not adapter:
            db.update_generic_target_health(
                conn, company=name, careers_url=careers_url,
                status="blocked", elapsed_ms=0.0, evaluated_count=0,
                cooldown_days=cooldown_days, notes="Healer failure cooldown",
            )
        conn.commit()
    except Exception:
        pass
    finally:
        conn.close()


def _merge_company_lists(*lists):
    merged = []
    seen = set()
    for companies in lists:
        for company in companies or []:
            if not isinstance(company, dict):
                continue
            key = (
                str(company.get("name", "")).strip().lower(),
                str(company.get("careers_url", "")).strip().lower(),
            )
            if key in seen:
                continue
            seen.add(key)
            merged.append(company)
    return merged


def _parse_iso_datetime(value):
    if not value:
        return None
    try:
        text = str(value).strip().replace("Z", "+00:00")
        parsed = datetime.fromisoformat(text)
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=timezone.utc)
        return parsed.astimezone(timezone.utc)
    except Exception:
        return None


def _healer_cooldown_reason(company, now_utc: datetime) -> str | None:
    if company.get("manual_only") or str(company.get("status", "")).lower() == "manual_only":
        return "manual_only"
    cooldown_until = _parse_iso_datetime(company.get("cooldown_until"))
    if cooldown_until and cooldown_until > now_utc:
        return f"cooldown until {cooldown_until.isoformat()}"
    return None


def _reset_heal_failure_state(company):
    company["heal_failure_streak"] = 0
    company.pop("cooldown_until", None)
    company["manual_only_suggested"] = False
    company.pop("heal_last_failure_detail", None)


def _priority_is_high(company) -> bool:
    priority = str(company.get("priority", "")).lower()
    try:
        tier = int(company.get("tier") or 4)
    except Exception:
        tier = 4
    return priority == "high" or tier <= 2


def _apply_heal_failure_policy(company, result_status: str, detail: str, now_utc: datetime) -> tuple[bool, str]:
    streak = int(company.get("heal_failure_streak") or 0) + 1
    company["heal_failure_streak"] = streak
    company["heal_last_failure_detail"] = detail
    company["manual_only_suggested"] = streak >= 3

    if result_status == "BLOCKED":
        cooldown_days = 7 if streak >= 2 else 2
    else:
        cooldown_days = 3 if streak >= 2 else 1
    company["cooldown_until"] = (now_utc + timedelta(days=cooldown_days)).isoformat()

    # Mirror the cooldown into the scraper DB so the scraper also skips this company.
    _sync_heal_cooldown_to_db(company, cooldown_days)

    promoted = False
    extra_detail = detail
    if streak >= 5 and not _priority_is_high(company):
        company["manual_only"] = True
        company["active"] = False
        company["status"] = "manual_only"
        promoted = True
        extra_detail = f"{detail} | Auto-marked manual_only after repeated failures"
    elif result_status not in ("FOUND", "FALLBACK", "VALID"):
        company["status"] = "broken"
    return promoted, extra_detail


@click.group()
def main():
    """JobSearch CLI: manage the job search pipeline."""


@main.command()
@click.option("--all", "heal_all", is_flag=True, help="Scan all companies.")
@click.option("--force", is_flag=True, help="Process all companies, including active ones.")
@click.option("--deep", is_flag=True, help="Enable deep heal using Playwright.")
@click.option("--workers", default=5, help="Number of parallel workers.")
@click.option("--deep-timeout", default=20.0, type=float, help="Maximum seconds to spend in deep heal per company.")
@click.option("--chronic-only", is_flag=True, help="Target only chronic failures (streak >= 3), bypassing cooldown. Enables --deep automatically.")
@click.option("--min-streak", default=0, type=int, help="Only process companies with heal_failure_streak >= N (implies force).")
def heal(heal_all, force, deep, workers, deep_timeout, chronic_only, min_streak):
    """Heal and verify ATS URLs in the company registry."""
    # --chronic-only implies force + deep since these are the hardest cases
    if chronic_only:
        force = True
        deep = True
        if min_streak == 0:
            min_streak = 3
        if deep_timeout == 20.0:  # user didn't override — use generous timeout for hard cases
            deep_timeout = 45.0

    click.echo(f"Starting ATS Registry Healer with {workers} workers...")

    comp_path = settings.companies_yaml
    if not comp_path.exists():
        click.echo(f"Companies file not found at {comp_path}", err=True)
        return

    with comp_path.open("r", encoding="utf-8") as handle:
        data = yaml.safe_load(handle) or {}

    companies = [
        company
        for company in (data.get("companies", []) or [])
        if str(company.get("source_lane") or "").lower() != "aggregator"
    ]
    now_utc = datetime.now(timezone.utc)
    to_heal = []
    skipped = []
    for company in companies:
        streak = int(company.get("heal_failure_streak") or 0)

        # Streak filter: skip companies that don't meet the minimum streak threshold
        if min_streak > 0 and streak < min_streak:
            continue

        if not (heal_all or force or company.get("status") != "active"):
            continue
        if not force:
            skip_reason = _healer_cooldown_reason(company, now_utc)
            if skip_reason:
                skipped.append((company.get("name", "Unknown"), skip_reason))
                continue
        to_heal.append(company)
    if not to_heal:
        click.echo("Done. No companies need healing.")
        return

    if min_streak > 0:
        click.echo(f"Targeting {len(to_heal)} companies with streak >= {min_streak} (deep={deep})")
    else:
        click.echo(f"Total companies to process: {len(to_heal)}")

    import threading
    from concurrent.futures import ThreadPoolExecutor

    log_path = settings.results_dir / "ats_heal.log"
    rotate_log_file(log_path)
    try:
        log_path.touch(exist_ok=True)
    except Exception:
        pass

    changed_count = 0
    lock = threading.Lock()
    run_started_at = time.perf_counter()
    metrics = {
        "processed": 0,
        "status_counts": {},
        "company_times": [],
        "slowest": [],
        "skipped": len(skipped),
    }

    # Single healer instance shared across all threads — caches URL validations
    # so the same ATS endpoint is never fetched twice within one heal run.
    healer = ATSHealer(deep_timeout_s=deep_timeout)

    log_handle = None
    try:
        log_handle = log_path.open("a", encoding="utf-8")
    except Exception:
        pass

    def log_msg(message: str):
        click.echo(message)
        if log_handle:
            try:
                log_handle.write(f"{datetime.now().strftime('%H:%M:%S')} | {message}\n")
                log_handle.flush()
            except Exception:
                pass

    log_msg(
        f"Heal run start | all={heal_all} force={force} deep={deep} "
        f"workers={workers} deep_timeout_s={deep_timeout}"
    )
    for name, reason in skipped:
        log_msg(f"  SKIP {name}: {reason}")

    def heal_one(company):
        nonlocal changed_count
        name = company.get("name", "Unknown")
        with lock:
            log_msg(f"Checking {name}...")

        started_at = time.perf_counter()
        result = healer.discover(company, force=force, deep=deep)
        elapsed_ms = round((time.perf_counter() - started_at) * 1000, 1)

        with lock:
            before = {f: company.get(f) for f in _SUBSTANTIVE_HEAL_FIELDS}
            transitioned_manual = False

            if result.status == "FOUND":
                company["adapter"] = result.adapter or company.get("adapter")
                if result.adapter_key:
                    company["adapter_key"] = result.adapter_key
                else:
                    company.pop("adapter_key", None)
                company["careers_url"] = result.careers_url or company.get("careers_url")
                company["status"] = "active"
                company["active"] = True
                company["manual_only"] = False
                _reset_heal_failure_state(company)
            elif result.status == "FALLBACK":
                # FALLBACK = 403 blocked page; record the URL but keep manual_only
                # so the scraper does not attempt it automatically.
                company["adapter"] = result.adapter or company.get("adapter")
                company.pop("adapter_key", None)
                company["careers_url"] = result.careers_url or company.get("careers_url")
                company["status"] = "manual_only"
                company["active"] = True
                company["manual_only"] = True
            elif result.status == "BLOCKED":
                # BLOCKED = unsupported ATS vendor or confirmed bot-wall.
                # Set manual_only=True permanently; do NOT increment the failure streak
                # since this is a known state, not a transient error.
                company["status"] = "manual_only"
                company["active"] = True
                company["manual_only"] = True
                if result.careers_url:
                    company["careers_url"] = result.careers_url
            elif result.status == "VALID":
                company["status"] = "active"
                company["active"] = True
                if not company.get("manual_only"):
                    _reset_heal_failure_state(company)
            else:
                transitioned_manual, detail_override = _apply_heal_failure_policy(
                    company,
                    result.status,
                    result.detail,
                    datetime.now(timezone.utc),
                )
                result = type(result)(
                    adapter=result.adapter,
                    adapter_key=result.adapter_key,
                    careers_url=result.careers_url,
                    status=result.status,
                    detail=detail_override,
                )

            company["discovery_method"] = result.detail
            company["last_healed"] = datetime.now().isoformat()

            # Count as changed only when a substantive field differs.
            after = {f: company.get(f) for f in _SUBSTANTIVE_HEAL_FIELDS}
            if after != before:
                changed_count += 1

            metrics["processed"] += 1
            metrics["status_counts"][result.status] = metrics["status_counts"].get(result.status, 0) + 1
            metrics["company_times"].append(elapsed_ms)
            metrics["slowest"].append((elapsed_ms, name, result.status, result.adapter or "-"))
            metrics["slowest"] = sorted(metrics["slowest"], reverse=True)[:10]

            if result.status == "FOUND":
                log_msg(f"  OK {name}: {result.adapter} ({result.detail}) | elapsed_ms={elapsed_ms}")
            elif result.status == "FALLBACK":
                log_msg(f"  MANUAL {name}: blocked page recorded as manual_only ({result.detail}) | elapsed_ms={elapsed_ms}")
            elif result.status == "BLOCKED":
                log_msg(f"  BLOCKED {name}: unsupported ATS — manual_only ({result.detail}) | elapsed_ms={elapsed_ms}")
            elif result.status == "VALID":
                log_msg(f"  OK {name}: existing URL confirmed | elapsed_ms={elapsed_ms}")
            else:
                outcome = "PROMOTE" if transitioned_manual else "FAIL"
                log_msg(f"  {outcome} {name}: {result.detail} | elapsed_ms={elapsed_ms}")

    try:
        with ThreadPoolExecutor(max_workers=workers) as executor:
            list(executor.map(heal_one, to_heal))
    except Exception as exc:
        click.echo(f"Heal failed: {exc}", err=True)
        raise

    if changed_count > 0:
        with comp_path.open("w", encoding="utf-8") as handle:
            yaml.safe_dump(data, handle, sort_keys=False, allow_unicode=True)
        log_msg(f"Updated {changed_count} companies.")
    else:
        log_msg("Done. No changes made.")

    total_elapsed_ms = round((time.perf_counter() - run_started_at) * 1000, 1)
    avg_elapsed_ms = round(sum(metrics["company_times"]) / len(metrics["company_times"]), 1) if metrics["company_times"] else 0.0
    status_summary = ", ".join(
        f"{status}={count}" for status, count in sorted(metrics["status_counts"].items())
    ) or "none"
    log_msg(
        f"Heal summary | processed={metrics['processed']} changed={changed_count} "
        f"elapsed_ms={total_elapsed_ms} avg_company_ms={avg_elapsed_ms} skipped={metrics['skipped']} statuses=[{status_summary}]"
    )
    for elapsed_ms, name, status, adapter in metrics["slowest"]:
        log_msg(f"Heal slowest | company={name} status={status} adapter={adapter} elapsed_ms={elapsed_ms}")

    # Surface chronic failures so the operator knows which companies to remove.
    chronic = sorted(
        [(c.get("name", "?"), int(c.get("heal_failure_streak") or 0))
         for c in companies if int(c.get("heal_failure_streak") or 0) >= 3],
        key=lambda x: -x[1],
    )
    if chronic:
        log_msg(f"Chronic failures (streak >= 3): {len(chronic)} companies — consider marking manual_only or removing")
        for cname, streak in chronic[:20]:
            log_msg(f"  Chronic | {cname}: streak={streak}")

    if log_handle:
        try:
            log_handle.close()
        except Exception:
            pass


@main.command()
@click.option("--deep-search", is_flag=True, help="Enable deep search using Playwright.")
@click.option("--full-refresh", is_flag=True, help="Re-fetch descriptions for all jobs, even if already known.")
@click.option("--test-companies", is_flag=True, help="Use the test company list.")
@click.option("--contract-sources", is_flag=True, help="Use the contractor-source company list.")
@click.option("--aggregator-sources", is_flag=True, help="Include aggregator job board sources.")
@click.option("--jobspy-sources", is_flag=True, help="Include JobSpy experimental sources.")
@click.option("--workers", default=8, help="Number of parallel workers for the scraper.")
@click.option("--prefs", "--preferences", type=click.Path(exists=True), help="Path to preferences YAML.")
@click.option("--companies", type=click.Path(exists=True), help="Path to companies YAML.")
@click.option("--legacy", is_flag=True, help="Use the legacy scraper script if it exists.")
@click.argument("extra_args", nargs=-1)
def run(deep_search, full_refresh, test_companies, contract_sources, aggregator_sources, jobspy_sources, workers, prefs, companies, legacy, extra_args):
    """Run the job search pipeline."""
    click.echo("Starting Job Search Pipeline...")

    if legacy:
        legacy_script = BASE_DIR / "run_job_search_v6.py"
        if not legacy_script.exists():
            click.echo("Legacy mode is unavailable: run_job_search_v6.py is not present in this checkout.", err=True)
            sys.exit(1)

        cmd = [sys.executable, str(legacy_script)]
        if deep_search:
            cmd.append("--deep-search")
        if test_companies:
            cmd.append("--test-companies")
        if workers:
            cmd.extend(["--workers", str(workers)])
        if prefs:
            cmd.extend(["--prefs", prefs])
        if companies:
            cmd.extend(["--companies", companies])
        if extra_args:
            cmd.extend(extra_args)
        try:
            subprocess.run(cmd, check=True)
            return
        except subprocess.CalledProcessError as exc:
            click.echo(f"Legacy pipeline failed with exit code {exc.returncode}", err=True)
            sys.exit(exc.returncode)

    prefs_path = Path(prefs) if prefs else settings.preferences_yaml
    comp_path = Path(companies) if companies else settings.companies_yaml
    if test_companies and not companies:
        comp_path = BASE_DIR / "config" / "job_search_companies_test.yaml"

    if not prefs_path.exists():
        click.echo(f"Preferences file not found at {prefs_path}", err=True)
        return
    if not comp_path.exists():
        click.echo(f"Companies file not found at {comp_path}", err=True)
        return

    click.echo(f"Loading config from {prefs_path.name} and {comp_path.name}...")
    with prefs_path.open("r", encoding="utf-8") as handle:
        prefs_data = yaml.safe_load(handle) or {}
    with comp_path.open("r", encoding="utf-8") as handle:
        comp_data = (yaml.safe_load(handle) or {}).get("companies", [])
    if contract_sources:
        contract_path = settings.contract_companies_yaml
        if contract_path.exists() and contract_path != comp_path:
            with contract_path.open("r", encoding="utf-8") as handle:
                contract_data = (yaml.safe_load(handle) or {}).get("companies", [])
            comp_data = _merge_company_lists(comp_data, contract_data)
    if aggregator_sources:
        aggregator_path = settings.aggregator_companies_yaml
        if aggregator_path.exists() and aggregator_path != comp_path:
            with aggregator_path.open("r", encoding="utf-8") as handle:
                aggregator_data = (yaml.safe_load(handle) or {}).get("companies", [])
            comp_data = _merge_company_lists(comp_data, aggregator_data)
    if jobspy_sources:
        jobspy_path = settings.jobspy_companies_yaml
        if jobspy_path.exists() and jobspy_path != comp_path:
            with jobspy_path.open("r", encoding="utf-8") as handle:
                jobspy_data = (yaml.safe_load(handle) or {}).get("companies", [])
            comp_data = _merge_company_lists(comp_data, jobspy_data)

    engine = ScraperEngine(prefs_data, comp_data, deep_search=deep_search, full_refresh=full_refresh)
    engine.run(max_workers=workers)
    click.echo("Pipeline run complete.")


@main.command()
@click.option("--port", default=8501, help="Port to run the Streamlit dashboard.")
@click.option("--headless", is_flag=True, default=False, help="Run Streamlit in headless mode.")
def dashboard(port, headless):
    """Launch the Streamlit dashboard."""
    click.echo(f"Starting Dashboard on port {port}...")
    cmd = [
        sys.executable,
        "-m",
        "streamlit",
        "run",
        str(BASE_DIR / "app.py"),
        "--server.port",
        str(port),
        "--server.headless",
        str(headless).lower(),
        "--browser.gatherUsageStats",
        "false",
    ]
    try:
        subprocess.run(cmd, check=True)
    except subprocess.CalledProcessError as exc:
        click.echo(f"Dashboard failed with exit code {exc.returncode}", err=True)
        sys.exit(exc.returncode)


if __name__ == "__main__":
    main()
