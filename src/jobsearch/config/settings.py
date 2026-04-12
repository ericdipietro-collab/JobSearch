"""src/jobsearch/config/settings.py — Central source of truth for paths and environment."""

import os
import shutil
import sqlite3
from pathlib import Path
from typing import Dict, Any, List

# ── Absolute Path Discovery ──────────────────────────────────────────────────
# This file is in src/jobsearch/config/settings.py
_CONFIG_DIR = Path(__file__).resolve().parent
_PACKAGE_DIR = _CONFIG_DIR.parent  # src/jobsearch/
_SRC_DIR = _PACKAGE_DIR.parent     # src/
BASE_DIR = _SRC_DIR.parent         # Project Root


def _resolve_runtime_dir() -> Path:
    override = os.getenv("JOBSEARCH_HOME", "").strip()
    if override:
        return Path(override).expanduser()
    return BASE_DIR


class Settings:
    def __init__(self):
        # Directories
        self.base_dir = BASE_DIR
        self.runtime_dir = _resolve_runtime_dir()
        self.config_dir = self.runtime_dir / "config"
        self.results_dir = self.runtime_dir / "results"
        self.data_dir = self.runtime_dir / "data"
        self.aggregator_import_dir = self.results_dir / "aggregator_imports"
        
        # Files
        self.db_path = self.results_dir / "jobsearch.db"
        self.prefs_yaml = self.config_dir / "job_search_preferences.yaml"
        self.companies_yaml = self.config_dir / "job_search_companies.yaml"
        self.contract_companies_yaml = self.config_dir / "job_search_companies_contract.yaml"
        self.aggregator_companies_yaml = self.config_dir / "job_search_companies_aggregators.yaml"
        self.jobspy_companies_yaml = self.config_dir / "job_search_companies_jobspy.yaml"
        self.history_json = self.results_dir / "job_search_history_v6.json"
        self.rejected_csv = self.results_dir / "job_search_v6_rejected.csv"
        self.log_file = self.results_dir / "job_search_v6.log"
        self.manual_review_file = self.results_dir / "job_search_manual_review.txt"
        self.debug_artifacts_dir = self.results_dir / "debug_artifacts"
        
        # Registry Discovery
        self.registry_patterns = ["job_search_companies*.yaml"]
        
        self.gmail_address = os.getenv("JOBSEARCH_GMAIL_ADDRESS", "").strip()
        self.gmail_app_password = os.getenv("JOBSEARCH_GMAIL_APP_PASSWORD", "").strip()
        self.gmail_imap_host = os.getenv("JOBSEARCH_GMAIL_IMAP_HOST", "imap.gmail.com").strip() or "imap.gmail.com"
        self.heal_discovery_budget_ms = int(os.getenv("JOBSEARCH_HEAL_DISCOVERY_BUDGET_MS", "60000"))
        self.heal_waterfall_follow_budget_ms = int(os.getenv("JOBSEARCH_HEAL_WATERFALL_FOLLOW_BUDGET_MS", "12000"))
        self.scrape_jitter_min_ms = int(os.getenv("JOBSEARCH_SCRAPE_JITTER_MIN_MS", "100"))
        self.scrape_jitter_max_ms = int(os.getenv("JOBSEARCH_SCRAPE_JITTER_MAX_MS", "500"))
        self.heal_jitter_min_ms = int(os.getenv("JOBSEARCH_HEAL_JITTER_MIN_MS", "100"))
        self.heal_jitter_max_ms = int(os.getenv("JOBSEARCH_HEAL_JITTER_MAX_MS", "500"))
        self.scrape_workday_concurrency = int(os.getenv("JOBSEARCH_SCRAPE_WORKDAY_CONCURRENCY", "2"))
        self.scrape_generic_concurrency = int(os.getenv("JOBSEARCH_SCRAPE_GENERIC_CONCURRENCY", "3"))
        self.scrape_greenhouse_concurrency = int(os.getenv("JOBSEARCH_SCRAPE_GREENHOUSE_CONCURRENCY", "4"))
        self.scrape_lever_concurrency = int(os.getenv("JOBSEARCH_SCRAPE_LEVER_CONCURRENCY", "4"))
        self.scrape_ashby_concurrency = int(os.getenv("JOBSEARCH_SCRAPE_ASHBY_CONCURRENCY", "4"))
        self.scrape_rippling_concurrency = int(os.getenv("JOBSEARCH_SCRAPE_RIPPLING_CONCURRENCY", "2"))
        self.scrape_smartrecruiters_concurrency = int(os.getenv("JOBSEARCH_SCRAPE_SMARTRECRUITERS_CONCURRENCY", "2"))
        self.scrape_dice_concurrency = int(os.getenv("JOBSEARCH_SCRAPE_DICE_CONCURRENCY", "2"))
        self.scrape_motionrecruitment_concurrency = int(os.getenv("JOBSEARCH_SCRAPE_MOTIONRECRUITMENT_CONCURRENCY", "2"))
        self.scrape_indeed_connector_concurrency = int(os.getenv("JOBSEARCH_SCRAPE_INDEED_CONNECTOR_CONCURRENCY", "1"))
        self.scrape_usajobs_concurrency = int(os.getenv("JOBSEARCH_SCRAPE_USAJOBS_CONCURRENCY", "1"))
        self.scrape_adzuna_concurrency = int(os.getenv("JOBSEARCH_SCRAPE_ADZUNA_CONCURRENCY", "1"))
        self.scrape_jooble_concurrency = int(os.getenv("JOBSEARCH_SCRAPE_JOOBLE_CONCURRENCY", "1"))
        self.scrape_themuse_concurrency = int(os.getenv("JOBSEARCH_SCRAPE_THEMUSE_CONCURRENCY", "1"))
        self.scrape_jobspy_concurrency = int(os.getenv("JOBSEARCH_SCRAPE_JOBSPY_CONCURRENCY", "1"))
        self.scrape_careeronestop_concurrency = int(os.getenv("JOBSEARCH_SCRAPE_CAREERONESTOP_CONCURRENCY", "1"))
        self.scrape_deep_search_concurrency = int(os.getenv("JOBSEARCH_SCRAPE_DEEP_SEARCH_CONCURRENCY", "1"))
        self.careeronestop_userid = os.getenv("JOBSEARCH_CAREERONESTOP_USERID", "HeokPYBQfpSkYo1").strip()
        self.careeronestop_token = os.getenv("JOBSEARCH_CAREERONESTOP_TOKEN", "9Oe3Z0siESw11yqns8PuEzqoK9CDR8shZkU6Yu8VvnL/XS76J0nRhKFkEgj4PX/IIz6S+tetAAzBsfJICjjLpw==").strip()
        self.usajobs_api_key = os.getenv("JOBSEARCH_USAJOBS_API_KEY", "").strip()
        self.usajobs_user_agent = os.getenv("JOBSEARCH_USAJOBS_USER_AGENT", "").strip()
        self.usajobs_max_requests_per_run = int(os.getenv("JOBSEARCH_USAJOBS_MAX_REQUESTS_PER_RUN", "3"))
        self.adzuna_app_id = os.getenv("JOBSEARCH_ADZUNA_APP_ID", "").strip()
        self.adzuna_app_key = os.getenv("JOBSEARCH_ADZUNA_APP_KEY", "").strip()
        self.adzuna_country = os.getenv("JOBSEARCH_ADZUNA_COUNTRY", "us").strip().lower() or "us"
        self.adzuna_max_requests_per_run = int(os.getenv("JOBSEARCH_ADZUNA_MAX_REQUESTS_PER_RUN", "4"))
        self.jooble_api_key = os.getenv("JOBSEARCH_JOOBLE_API_KEY", "").strip()
        self.findwork_api_key = os.getenv("JOBSEARCH_FINDWORK_API_KEY", "").strip()
        self.jooble_max_requests_per_run = int(os.getenv("JOBSEARCH_JOOBLE_MAX_REQUESTS_PER_RUN", "3"))
        self.themuse_max_requests_per_run = int(os.getenv("JOBSEARCH_THEMUSE_MAX_REQUESTS_PER_RUN", "3"))
        self.themuse_api_key = os.getenv("JOBSEARCH_THEMUSE_API_KEY", "").strip()
        self.jobspy_site_names = os.getenv("JOBSEARCH_JOBSPY_SITE_NAMES", "google").strip()
        self.jobspy_results_per_run = int(os.getenv("JOBSEARCH_JOBSPY_RESULTS_PER_RUN", "20"))
        self.jobspy_hours_old = int(os.getenv("JOBSEARCH_JOBSPY_HOURS_OLD", "72"))
        self.jobspy_country_indeed = os.getenv("JOBSEARCH_JOBSPY_COUNTRY_INDEED", "USA").strip() or "USA"
        self.jobspy_is_remote = str(os.getenv("JOBSEARCH_JOBSPY_IS_REMOTE", "false")).strip().lower() in {"1", "true", "yes", "on"}
        self.jobspy_job_type = os.getenv("JOBSEARCH_JOBSPY_JOB_TYPE", "").strip()
        self.jobspy_linkedin_fetch_description = str(os.getenv("JOBSEARCH_JOBSPY_LINKEDIN_FETCH_DESCRIPTION", "false")).strip().lower() in {"1", "true", "yes", "on"}
        self.jobspy_google_search_term_template = os.getenv("JOBSEARCH_JOBSPY_GOOGLE_SEARCH_TERM_TEMPLATE", "{query}").strip() or "{query}"
        self.jobspy_continue_on_site_failure = str(os.getenv("JOBSEARCH_JOBSPY_CONTINUE_ON_SITE_FAILURE", "true")).strip().lower() in {"1", "true", "yes", "on"}
        self.jobspy_max_total_results = int(os.getenv("JOBSEARCH_JOBSPY_MAX_TOTAL_RESULTS", "20"))
        self.workday_scrape_budget_ms = int(os.getenv("JOBSEARCH_WORKDAY_SCRAPE_BUDGET_MS", "35000"))
        self.workday_html_fallback_budget_ms = int(os.getenv("JOBSEARCH_WORKDAY_HTML_FALLBACK_BUDGET_MS", "10000"))
        self.workday_empty_cooldown_days = int(os.getenv("JOBSEARCH_WORKDAY_EMPTY_COOLDOWN_DAYS", "7"))
        self.workday_empty_cooldown_threshold = int(os.getenv("JOBSEARCH_WORKDAY_EMPTY_COOLDOWN_THRESHOLD", "2"))
        self.generic_empty_cooldown_days = int(os.getenv("JOBSEARCH_GENERIC_EMPTY_COOLDOWN_DAYS", "7"))
        self.generic_empty_cooldown_threshold = int(os.getenv("JOBSEARCH_GENERIC_EMPTY_COOLDOWN_THRESHOLD", "3"))
        self.generic_slow_empty_ms = int(os.getenv("JOBSEARCH_GENERIC_SLOW_EMPTY_MS", "15000"))
        self.generic_low_signal_cooldown_days = int(os.getenv("JOBSEARCH_GENERIC_LOW_SIGNAL_COOLDOWN_DAYS", "14"))
        self.proxies = os.getenv("JOBSEARCH_PROXIES", "").strip()
        self.llm_daily_token_budget = int(os.getenv("JOBSEARCH_LLM_DAILY_TOKEN_BUDGET", "500000"))
        self.scrape_debug_artifacts = str(os.getenv("JOBSEARCH_SCRAPE_DEBUG_ARTIFACTS", "false")).strip().lower() in {"1", "true", "yes", "on"}

        # Scraper Cooldowns (days)
        self.cooldown_days_blocked = int(os.getenv("JOBSEARCH_COOLDOWN_DAYS_BLOCKED", "7"))
        self.cooldown_days_broken = int(os.getenv("JOBSEARCH_COOLDOWN_DAYS_BROKEN", "3"))
        self.cooldown_days_stale = int(os.getenv("JOBSEARCH_COOLDOWN_DAYS_STALE", "5"))
        self.cooldown_days_auth = int(os.getenv("JOBSEARCH_COOLDOWN_DAYS_AUTH", "14"))
        self.cooldown_days_geo_gate = int(os.getenv("JOBSEARCH_COOLDOWN_DAYS_GEO_GATE", "2"))
        self.cooldown_days_no_openings = int(os.getenv("JOBSEARCH_COOLDOWN_DAYS_NO_OPENINGS", "1"))
        self.cooldown_days_selector_miss = int(os.getenv("JOBSEARCH_COOLDOWN_DAYS_SELECTOR_MISS", "1"))

        # Scheduling Policy (Priority Weights)
        self.scheduler_weight_tier_1 = float(os.getenv("JOBSEARCH_SCHEDULER_WEIGHT_TIER_1", "80"))
        self.scheduler_weight_tier_2 = float(os.getenv("JOBSEARCH_SCHEDULER_WEIGHT_TIER_2", "60"))
        self.scheduler_weight_tier_3 = float(os.getenv("JOBSEARCH_SCHEDULER_WEIGHT_TIER_3", "40"))
        self.scheduler_weight_tier_4 = float(os.getenv("JOBSEARCH_SCHEDULER_WEIGHT_TIER_4", "20"))
        self.scheduler_bonus_api = float(os.getenv("JOBSEARCH_SCHEDULER_BONUS_API", "10"))
        self.scheduler_bonus_new = float(os.getenv("JOBSEARCH_SCHEDULER_BONUS_NEW", "15"))
        self.scheduler_penalty_fail_streak = float(os.getenv("JOBSEARCH_SCHEDULER_PENALTY_FAIL_STREAK", "5"))
        self.scheduler_healer_auto_trigger = str(os.getenv("JOBSEARCH_SCHEDULER_HEALER_AUTO_TRIGGER", "true")).strip().lower() in {"1", "true", "yes", "on"}

        # Flapping & Escalation Policy
        self.health_flapping_window_days = int(os.getenv("JOBSEARCH_HEALTH_FLAPPING_WINDOW_DAYS", "7"))
        self.health_flapping_threshold_low = int(os.getenv("JOBSEARCH_HEALTH_FLAPPING_THRESHOLD_LOW", "3"))
        self.health_flapping_threshold_med = int(os.getenv("JOBSEARCH_HEALTH_FLAPPING_THRESHOLD_MED", "5"))
        self.health_flapping_threshold_high = int(os.getenv("JOBSEARCH_HEALTH_FLAPPING_THRESHOLD_HIGH", "7"))
        
        self.health_escalation_window_days = int(os.getenv("JOBSEARCH_HEALTH_ESCALATION_WINDOW_DAYS", "7"))
        self.health_escalation_multiplier_2x = int(os.getenv("JOBSEARCH_HEALTH_ESCALATION_MULTIPLIER_2X", "2"))
        self.health_escalation_multiplier_4x = int(os.getenv("JOBSEARCH_HEALTH_ESCALATION_MULTIPLIER_4X", "4"))
        self.health_escalation_max_days = int(os.getenv("JOBSEARCH_HEALTH_ESCALATION_MAX_DAYS", "30"))
        self.health_escalation_reset_days = int(os.getenv("JOBSEARCH_HEALTH_ESCALATION_RESET_DAYS", "14"))

        # Tailoring & Export
        self.tailoring_block_on_critical = str(os.getenv("JOBSEARCH_TAILORING_BLOCK_ON_CRITICAL", "true")).strip().lower() in {"1", "true", "yes", "on"}
        self.tailoring_export_max_age_hours = int(os.getenv("JOBSEARCH_TAILORING_EXPORT_MAX_AGE_HOURS", "24"))

        # Ensure directories exist
        self.results_dir.mkdir(parents=True, exist_ok=True)
        self.config_dir.mkdir(parents=True, exist_ok=True)
        self.aggregator_import_dir.mkdir(parents=True, exist_ok=True)
        self.debug_artifacts_dir.mkdir(parents=True, exist_ok=True)
        self._seed_runtime_config()

    def _seed_runtime_config(self) -> None:
        if self.runtime_dir == self.base_dir:
            return
        
        # 1. Sync static defaults
        defaults = {
            "job_search_preferences.yaml": "job_search_preferences.example.yaml",
        }
        packaged_config_dir = self.base_dir / "config"
        for runtime_name, packaged_name in defaults.items():
            runtime_path = self.config_dir / runtime_name
            if runtime_path.exists():
                continue
            packaged_path = packaged_config_dir / packaged_name
            if packaged_path.exists():
                try:
                    shutil.copyfile(packaged_path, runtime_path)
                except Exception:
                    pass
        
        # 2. Sync ALL company registries from project root to runtime config
        # This allows adding new industry-specific yamls to the git repo and 
        # having them automatically appear in the AppData runtime.
        if packaged_config_dir.exists():
            for pattern in self.registry_patterns:
                for packaged_file in packaged_config_dir.glob(pattern):
                    runtime_file = self.config_dir / packaged_file.name
                    # Only copy if it doesn't exist, OR if we want to sync new ones
                    if not runtime_file.exists():
                        try:
                            shutil.copyfile(packaged_file, runtime_file)
                        except Exception:
                            pass

    def get_company_registries(self) -> List[Path]:
        """Discover all company registry YAML files in the config directory."""
        registries = []
        for pattern in self.registry_patterns:
            registries.extend(list(self.config_dir.glob(pattern)))
        # Filter out backups and ensure uniqueness
        unique_registries = {r.resolve() for r in registries if ".bak" not in r.name}
        return sorted(list(unique_registries))

    # Suffixes that identify test-only or specialized opt-in registries.
    # These are excluded from default production scrape runs.
    _PRODUCTION_REGISTRY_EXCLUDES = frozenset({
        "_test", "_contract_test", "_contract", "_aggregators", "_jobspy"
    })

    def get_production_registries(self) -> List[Path]:
        """All curated registries for a default production run.

        Excludes test files and specialized opt-in sources (contract, aggregator, jobspy).
        Those are still available via --contract-sources, --aggregator-sources, --jobspy-sources.
        """
        return [
            r for r in self.get_company_registries()
            if not any(r.stem.endswith(suffix) for suffix in self._PRODUCTION_REGISTRY_EXCLUDES)
        ]

    @property
    def preferences_yaml(self) -> Path:
        return self.prefs_yaml

    @property
    def gmail_sync_enabled(self) -> bool:
        return bool(self.gmail_address and self.gmail_app_password)

    @property
    def shared_session_pool_size(self) -> int:
        return 100

settings = Settings()


def get_runtime_setting(key: str, env_default: str = "") -> str:
    """Read a local runtime setting, falling back to env/default values."""
    if env_default:
        return env_default
    db_path = settings.db_path
    if not db_path.exists():
        return ""
    try:
        conn = sqlite3.connect(db_path)
        conn.row_factory = sqlite3.Row
        try:
            row = conn.execute("SELECT value FROM settings WHERE key = ?", (key,)).fetchone()
            return str(row["value"]) if row and row["value"] is not None else ""
        finally:
            conn.close()
    except Exception:
        return ""


def rotate_log_file(path: Path, keep: int = 5, max_bytes: int = 1_000_000) -> None:
    """Rotate a log file in place when it grows beyond the configured size."""
    try:
        if not path.exists() or path.stat().st_size < max_bytes:
            return

        oldest = path.with_name(f"{path.name}.{keep}")
        if oldest.exists():
            oldest.unlink()

        for index in range(keep - 1, 0, -1):
            src = path.with_name(f"{path.name}.{index}")
            dst = path.with_name(f"{path.name}.{index + 1}")
            if src.exists():
                src.replace(dst)

        path.replace(path.with_name(f"{path.name}.1"))
    except Exception:
        pass

# Network Helpers
def get_shared_session():
    import requests
    from requests.adapters import HTTPAdapter
    from urllib3.util.retry import Retry
    
    session = requests.Session()
    retry_strategy = Retry(total=3, backoff_factor=1, status_forcelist=[429, 500, 502, 503, 504])
    adapter = HTTPAdapter(pool_connections=settings.shared_session_pool_size, 
                         pool_maxsize=settings.shared_session_pool_size, 
                         max_retries=retry_strategy)
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    return session

def get_headers(referer: str | None = None):
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36",
        "Accept": "application/json, text/plain, */*",
        "Accept-Language": "en-US,en;q=0.9",
    }
    if referer:
        headers["Referer"] = referer
    return headers
