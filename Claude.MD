# Job Search Healer - Technical Specification & Guide

## 🚀 Build & Run Commands
- **Main CLI:** `python src/jobsearch/cli.py`
- **Run Scraper:** `python src/jobsearch/cli.py run`
- **Run Scraper (deep search):** `python src/jobsearch/cli.py run --deep-search`
- **Dashboard (Main UI):** `python src/jobsearch/cli.py dashboard`
- **Install Environment:** `pip install -e .`
- **Install Deep Search add-on (Windows):** `deep_search\install_deep_search.bat`

## 🎯 Bucket Filter & Scoring Logic
The pipeline follows a modular "Soft-Drop" funnel implemented in `src/jobsearch/scraper/scoring.py`.

### 1. Scraper Adapters
Modular adapters in `src/jobsearch/scraper/adapters/` handle ATS-specific parsing:
- `GreenhouseAdapter`
- `LeverAdapter`
- `AshbyAdapter`
- `WorkdayAdapter`
- `GenericAdapter` (BeautifulSoup fallback)

### 2. Scoring Funnel
- **Title Evaluation:** Disqualifies based on `negative_disqualifiers`. High-weight matches (>=8) trigger "Fast-Track" (50 pts base).
- **JD Refinement:** Positive/Negative keyword scoring with boundary-safe matching.
- **Direct Persistence:** Scraped jobs are upserted directly into the unified SQLite database via `src/jobsearch/services/opportunity_service.py`.

## 📂 Project Structure
- **`src/jobsearch/`**:
    - `cli.py`: Unified entry point.
    - `scraper/`: Engine, scoring, and ATS adapters.
    - `db/`: Unified schema and connection management.
    - `config/`: Settings and configuration loading.
- **`/config`**: User YAML configurations.
- **`/results`**:
    - `jobsearch.db`: Unified SQLite database (matches + tracker).
    - `job_search_v6.log`: Technical execution logs.

## 📜 Development Standards
- **Unified Database:** All data must live in `results/jobsearch.db`.
- **Modular Scraping:** New ATS providers must be implemented as subclasses of `BaseAdapter`.
- **Privacy:** Keep personal data within `/config` or `/results` (gitignored).