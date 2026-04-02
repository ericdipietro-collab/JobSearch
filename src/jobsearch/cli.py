import sys
import subprocess
import yaml
from pathlib import Path
import click

# Add the project root to sys.path to allow imports from src/
BASE_DIR = Path(__file__).resolve().parent.parent.parent
sys.path.append(str(BASE_DIR))

from src.jobsearch.scraper.engine import ScraperEngine
from src.jobsearch.services.healer_service import ATSHealer
from src.jobsearch.config.settings import settings

@click.group()
def main():
    """JobSearch CLI: Manage your automated job search pipeline."""
    pass

@main.command()
@click.option("--all", "heal_all", is_flag=True, help="Scan all companies.")
@click.option("--force", is_flag=True, help="Force rediscovery for all.")
@click.option("--deep", is_flag=True, help="Enable deep heal using Playwright.")
@click.option("--workers", default=5, help="Number of parallel workers.")
def heal(heal_all, force, deep, workers):
    """Heal and verify ATS URLs in the company registry."""
    click.echo(f"🩹 Starting ATS Registry Healer with {workers} workers...")
    
    comp_path = settings.companies_yaml
    if not comp_path.exists():
        click.echo(f"❌ Companies file not found at {comp_path}", err=True)
        return

    with open(comp_path, "r", encoding="utf-8") as f:
        data = yaml.safe_load(f) or {}
    
    companies = data.get("companies", [])
    to_heal = []
    for c in companies:
        if heal_all or force or c.get("status") != "active":
            to_heal.append(c)

    if not to_heal:
        click.echo("Done. No companies need healing.")
        return

    from concurrent.futures import ThreadPoolExecutor, as_completed
    updated_count = 0
    
    def heal_one(company):
        name = company.get("name", "Unknown")
        healer = ATSHealer() # New session per thread
        result = healer.discover(company, force=force, deep=deep)
        return company, result

    with ThreadPoolExecutor(max_workers=workers) as executor:
        futures = {executor.submit(heal_one, c): c for c in to_heal}
        for future in as_completed(futures):
            company, result = future.result()
            name = company.get("name", "Unknown")
            
            if result.status in ("FOUND", "FALLBACK"):
                if result.adapter:
                    company["adapter"] = result.adapter
                if result.adapter_key:
                    company["adapter_key"] = result.adapter_key
                if result.careers_url:
                    company["careers_url"] = result.careers_url
                company["status"] = "active"
                updated_count += 1
                click.echo(f"  ✅ {name}: {result.adapter} | {result.careers_url}")
            elif result.status == "VALID":
                click.echo(f"  OK: {name}")
            else:
                company["status"] = "broken"
                click.echo(f"  ❌ {name}: Not found")

    if updated_count > 0:
        with open(comp_path, "w", encoding="utf-8") as f:
            yaml.safe_dump(data, f, sort_keys=False, allow_unicode=True)
        click.echo(f"✨ Updated {updated_count} companies.")
    else:
        click.echo("Done. No changes made.")

@main.command()
@click.option("--deep-search", is_flag=True, help="Enable deep search using Playwright.")
@click.option("--test-companies", is_flag=True, help="Use the test company list.")
@click.option("--prefs", "--preferences", type=click.Path(exists=True), help="Path to preferences YAML.")
@click.option("--companies", type=click.Path(exists=True), help="Path to companies YAML.")
@click.option("--legacy", is_flag=True, help="Use the legacy scraper script.")
@click.argument("extra_args", nargs=-1)
def run(deep_search, test_companies, prefs, companies, legacy, extra_args):
    """Run the job search pipeline."""
    click.echo("🚀 Starting Job Search Pipeline...")
    
    if legacy:
        click.echo("📜 Using legacy scraper script...")
        cmd = [sys.executable, str(BASE_DIR / "run_job_search_v6.py")]
        if deep_search: cmd.append("--deep-search")
        if test_companies: cmd.append("--test-companies")
        if prefs: cmd.extend(["--prefs", prefs])
        if companies: cmd.extend(["--companies", companies])
        if extra_args: cmd.extend(extra_args)
        
        try:
            subprocess.run(cmd, check=True)
            return
        except subprocess.CalledProcessError as e:
            click.echo(f"❌ Legacy pipeline failed with exit code {e.returncode}", err=True)
            sys.exit(e.returncode)

    # Modular scraper implementation
    prefs_path = Path(prefs) if prefs else settings.preferences_yaml
    comp_path = Path(companies) if companies else settings.companies_yaml
    
    if test_companies and not companies:
        comp_path = BASE_DIR / "config" / "job_search_companies_test.yaml"

    if not prefs_path.exists():
        click.echo(f"❌ Preferences file not found at {prefs_path}", err=True)
        return
    if not comp_path.exists():
        click.echo(f"❌ Companies file not found at {comp_path}", err=True)
        return

    click.echo(f"⚙️  Loading config from {prefs_path.name} and {comp_path.name}...")
    
    with open(prefs_path, "r", encoding="utf-8") as f:
        prefs_data = yaml.safe_load(f)
    with open(comp_path, "r", encoding="utf-8") as f:
        comp_data = yaml.safe_load(f).get("companies", [])

    engine = ScraperEngine(prefs_data, comp_data)
    engine.run()
    click.echo("✨ Pipeline run complete.")

@main.command()
@click.option("--port", default=8501, help="Port to run the Streamlit dashboard.")
@click.option("--headless", is_flag=True, default=False, help="Run Streamlit in headless mode.")
def dashboard(port, headless):
    """Launch the Streamlit dashboard."""
    click.echo(f"📊 Starting Dashboard on port {port}...")
    cmd = [
        sys.executable, "-m", "streamlit", "run",
        str(BASE_DIR / "app.py"),
        "--server.port", str(port),
        "--server.headless", str(headless).lower(),
        "--browser.gatherUsageStats", "false"
    ]
    try:
        subprocess.run(cmd, check=True)
    except subprocess.CalledProcessError as e:
        click.echo(f"❌ Dashboard failed with exit code {e.returncode}", err=True)
        sys.exit(e.returncode)

if __name__ == "__main__":
    main()
