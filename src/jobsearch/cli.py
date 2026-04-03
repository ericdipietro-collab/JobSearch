import subprocess
import sys
import time
from pathlib import Path

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


@click.group()
def main():
    """JobSearch CLI: manage the job search pipeline."""


@main.command()
@click.option("--all", "heal_all", is_flag=True, help="Scan all companies.")
@click.option("--force", is_flag=True, help="Process all companies, including active ones.")
@click.option("--deep", is_flag=True, help="Enable deep heal using Playwright.")
@click.option("--workers", default=5, help="Number of parallel workers.")
@click.option("--deep-timeout", default=20.0, type=float, help="Maximum seconds to spend in deep heal per company.")
def heal(heal_all, force, deep, workers, deep_timeout):
    """Heal and verify ATS URLs in the company registry."""
    click.echo(f"Starting ATS Registry Healer with {workers} workers...")

    comp_path = settings.companies_yaml
    if not comp_path.exists():
        click.echo(f"Companies file not found at {comp_path}", err=True)
        return

    with comp_path.open("r", encoding="utf-8") as handle:
        data = yaml.safe_load(handle) or {}

    companies = data.get("companies", [])
    to_heal = [company for company in companies if heal_all or force or company.get("status") != "active"]
    if not to_heal:
        click.echo("Done. No companies need healing.")
        return

    click.echo(f"Total companies to process: {len(to_heal)}")

    import threading
    from concurrent.futures import ThreadPoolExecutor
    from datetime import datetime

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
    }

    def log_msg(message: str):
        click.echo(message)
        try:
            with log_path.open("a", encoding="utf-8") as handle:
                handle.write(f"{datetime.now().strftime('%H:%M:%S')} | {message}\n")
        except Exception:
            pass

    log_msg(
        f"Heal run start | all={heal_all} force={force} deep={deep} "
        f"workers={workers} deep_timeout_s={deep_timeout}"
    )

    def heal_one(company):
        nonlocal changed_count
        name = company.get("name", "Unknown")
        with lock:
            log_msg(f"Checking {name}...")

        started_at = time.perf_counter()
        healer = ATSHealer(deep_timeout_s=deep_timeout)
        result = healer.discover(company, force=force, deep=deep)
        elapsed_ms = round((time.perf_counter() - started_at) * 1000, 1)

        with lock:
            before = dict(company)
            if result.status in ("FOUND", "FALLBACK"):
                company["adapter"] = result.adapter or company.get("adapter")
                if result.adapter_key:
                    company["adapter_key"] = result.adapter_key
                else:
                    company.pop("adapter_key", None)
                company["careers_url"] = result.careers_url or company.get("careers_url")
                company["status"] = "active"
            elif result.status == "VALID":
                company["status"] = "active"
            else:
                company["status"] = "broken"

            company["discovery_method"] = result.detail
            company["last_healed"] = datetime.now().isoformat()

            if company != before:
                changed_count += 1

            metrics["processed"] += 1
            metrics["status_counts"][result.status] = metrics["status_counts"].get(result.status, 0) + 1
            metrics["company_times"].append(elapsed_ms)
            metrics["slowest"].append((elapsed_ms, name, result.status, result.adapter or "-"))
            metrics["slowest"] = sorted(metrics["slowest"], reverse=True)[:10]

            if result.status in ("FOUND", "FALLBACK"):
                log_msg(f"  OK {name}: {result.adapter} ({result.detail}) | elapsed_ms={elapsed_ms}")
            elif result.status == "VALID":
                log_msg(f"  OK {name}: existing URL confirmed | elapsed_ms={elapsed_ms}")
            else:
                log_msg(f"  FAIL {name}: {result.detail} | elapsed_ms={elapsed_ms}")

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
        f"elapsed_ms={total_elapsed_ms} avg_company_ms={avg_elapsed_ms} statuses=[{status_summary}]"
    )
    for elapsed_ms, name, status, adapter in metrics["slowest"]:
        log_msg(f"Heal slowest | company={name} status={status} adapter={adapter} elapsed_ms={elapsed_ms}")


@main.command()
@click.option("--deep-search", is_flag=True, help="Enable deep search using Playwright.")
@click.option("--test-companies", is_flag=True, help="Use the test company list.")
@click.option("--workers", default=8, help="Number of parallel workers for the scraper.")
@click.option("--prefs", "--preferences", type=click.Path(exists=True), help="Path to preferences YAML.")
@click.option("--companies", type=click.Path(exists=True), help="Path to companies YAML.")
@click.option("--legacy", is_flag=True, help="Use the legacy scraper script if it exists.")
@click.argument("extra_args", nargs=-1)
def run(deep_search, test_companies, workers, prefs, companies, legacy, extra_args):
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

    engine = ScraperEngine(prefs_data, comp_data, deep_search=deep_search)
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
