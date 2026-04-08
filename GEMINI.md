# Job Search Automation Platform - Technical Context

Comprehensive, local job-search dashboard that discovers jobs from target companies, scores them against preferences, and tracks the search lifecycle in a local SQLite database.

## 🏗 Architecture Overview

- **Frontend:** [Streamlit](https://streamlit.io/) dashboard providing real-time analytics, job matching UI, and application tracking.
- **Backend:** Python-based automation pipeline with a modular scraping engine and heuristic scoring.
- **Database:** SQLite (`results/jobsearch.db`) using FTS5 for semantic search and triggers for index synchronization.
- **Scraper:** Modular adapter-based system (Greenhouse, Lever, Workday, etc.) with support for "Deep Search" via Playwright.
- **AI Integration:** Google Gemini, OpenAI, or Ollama for job description enrichment, tech stack analysis, and visa sponsorship detection.

## 🚀 Key Commands

### Environment Setup
- **Install dependencies:** `pip install -e .` (Python 3.11 recommended).
- **Deep Search add-on:** `deep_search\install_deep_search.bat`.

### Running the Platform
- **Launch Dashboard:** `launch.bat` (Windows) or `python -m streamlit run app.py`.
- **Run Scraper:** `python -m jobsearch.cli run`.
- **Run Scraper (Aggregators):** `python -m jobsearch.cli run --aggregator-sources`.
- **Run Scraper (Deep):** `python -m jobsearch.cli run --deep-search`.
- **Heal ATS Registry:** `python -m jobsearch.cli heal --all`.

### Testing
- **Run tests:** `python -m unittest discover tests`.

## 📁 Project Structure

- `src/jobsearch/`: Core package.
    - `scraper/`: Scraping engine and ATS adapters.
    - `services/`: Business logic (Healer, LLM enrichment, Search service).
    - `views/`: Streamlit page implementations.
    - `db/`: Database schema, migrations, and repository logic.
    - `config/`: Centralized settings and environment management.
- `config/`: YAML registries for companies and user preferences.
- `results/`: Runtime data, including the SQLite database and logs (Gitignored).
- `installer/`: Scripts for building Windows installers and portable distributions.

## 🛠 Development Standards

### 1. Unified Database
All data must live in `results/jobsearch.db`. Use `src/jobsearch/db/schema.py` for schema definitions and `src/jobsearch/ats_db.py` for the primary database interface.

### 2. Modular Scraping
New ATS providers or job boards must be implemented as subclasses of `BaseAdapter` in `src/jobsearch/scraper/adapters/`. Use shared request/session helpers from `jobsearch.config.settings`.

### 3. Scoring Logic
The "Soft-Drop" funnel is implemented in `src/jobsearch/scraper/scoring.py`. It handles title disqualification, location/salary gating, and keyword-based scoring.

### 4. Configuration
- Global settings live in `src/jobsearch/config/settings.py`.
- User preferences (weights, keywords) are stored in `config/job_search_preferences.yaml`.
- Runtime overrides can be stored in the `settings` table of the database.

### 5. Resilience
- Handle Cloudflare/anti-bot blocks by routing to `manual_review` instead of failing.
- Use `rotate_log_file` for managing growth of `job_search_v6.log` and `ats_heal.log`.

## 📦 Runtime Outputs
- **Scrape log:** `results/job_search_v6.log`
- **Healer log:** `results/ats_heal.log`
- **Rejects:** `results/job_search_v6_rejected.csv`
- **Manual review:** `results/job_search_manual_review.txt`
