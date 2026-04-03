"""src/jobsearch/config/settings.py — Central source of truth for paths and environment."""

from pathlib import Path
from typing import Dict, Any

# ── Absolute Path Discovery ──────────────────────────────────────────────────
# This file is in src/jobsearch/config/settings.py
_CONFIG_DIR = Path(__file__).resolve().parent
_PACKAGE_DIR = _CONFIG_DIR.parent  # src/jobsearch/
_SRC_DIR = _PACKAGE_DIR.parent     # src/
BASE_DIR = _SRC_DIR.parent         # Project Root

class Settings:
    def __init__(self):
        # Directories
        self.base_dir = BASE_DIR
        self.config_dir = BASE_DIR / "config"
        self.results_dir = BASE_DIR / "results"
        self.data_dir = BASE_DIR / "data"
        
        # Files
        self.db_path = self.results_dir / "jobsearch.db"
        self.prefs_yaml = self.config_dir / "job_search_preferences.yaml"
        self.companies_yaml = self.config_dir / "job_search_companies.yaml"
        self.contract_companies_yaml = self.config_dir / "job_search_companies_contract.yaml"
        self.history_json = self.results_dir / "job_search_history_v6.json"
        self.rejected_csv = self.results_dir / "job_search_v6_rejected.csv"
        self.log_file = self.results_dir / "job_search_v6.log"
        self.manual_review_file = self.results_dir / "job_search_manual_review.txt"

        # Ensure directories exist
        self.results_dir.mkdir(parents=True, exist_ok=True)
        self.config_dir.mkdir(parents=True, exist_ok=True)

    @property
    def preferences_yaml(self) -> Path:
        return self.prefs_yaml

    @property
    def shared_session_pool_size(self) -> int:
        return 100

settings = Settings()


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
