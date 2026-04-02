from pathlib import Path
from typing import List, Optional
from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict

BASE_DIR = Path(__file__).resolve().parent.parent.parent.parent

class Settings(BaseSettings):
    # Paths
    config_dir: Path = Field(default=BASE_DIR / "config")
    results_dir: Path = Field(default=BASE_DIR / "results")
    
    # Files
    companies_yaml: Path = Field(default=BASE_DIR / "config" / "job_search_companies.yaml")
    preferences_yaml: Path = Field(default=BASE_DIR / "config" / "job_search_preferences.yaml")
    db_path: Path = Field(default=BASE_DIR / "results" / "jobsearch.db")
    
    # Scraper Settings
    deep_search_enabled: bool = False
    request_timeout: int = 25
    max_job_age_days: int = 21
    min_score_to_keep: int = 35
    
    model_config = SettingsConfigDict(env_prefix="JOBSEARCH_")

    def resolve_paths(self):
        """Ensure critical directories exist."""
        self.config_dir.mkdir(parents=True, exist_ok=True)
        self.results_dir.mkdir(parents=True, exist_ok=True)

settings = Settings()
