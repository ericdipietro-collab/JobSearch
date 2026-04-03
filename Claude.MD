# Job Search Healer - Technical Specification & Guide

## 🚀 Build & Run Commands
- **Main CLI:** `python -m jobsearch.cli`
- **Run Scraper:** `python -m jobsearch.cli run`
- **Run Scraper (deep search):** `python -m jobsearch.cli run --deep-search`
- **Dashboard (Main UI):** `launch.bat` on Windows or `python -m streamlit run app.py`
- **Install Environment:** `pip install -e .`
- **Install Deep Search add-on (Windows):** `deep_search\install_deep_search.bat`

## 🎯 Bucket Filter & Scoring Logic
The pipeline follows a modular "Soft-Drop" funnel implemented in `src/jobsearch/scraper/scoring.py`.

### 1. Scraper Adapters
Modular adapters in `src/jobsearch/scraper/adapters/` handle ATS-specific parsing:
- `GreenhouseAdapter` (with API-first & embed support)
- `LeverAdapter`
- `AshbyAdapter`
- `WorkdayAdapter` (with wd1..wd25 sweep)
- `RipplingAdapter`
- `SmartRecruitersAdapter`
- `GenericAdapter` (with product/staff filters)

### 2. Scoring Funnel
- **Soft-Drop Phase:** Hard disqualification for roles containing `negative_disqualifiers` (e.g., "Meet the Staff", "Product", "Blog").
- **Title Evaluation:** Disqualifies based on location/salary gating. High-weight matches (>=8) trigger "Fast-Track" (50 pts base).
- **JD Refinement:** Positive/Negative keyword scoring with boundary-safe matching.
- **Direct Persistence:** Scraped jobs are upserted directly into the unified SQLite database.

## 📜 Development Standards
- **Unified Database:** All data must live in `results/jobsearch.db`.
- **Modular Scraping:** New ATS providers must be implemented as subclasses of `BaseAdapter`.
- **Resilience:** Use the shared request/session helpers from `jobsearch.config.settings`.
- **Protected Sites:** Cloudflare / happydance / similar anti-bot pages must be classified as `blocked` and routed to manual review, not treated as ordinary zero-result scrapes.
- **Privacy:** Keep personal data within `/config` or `/results` (gitignored).

## 📦 Current Runtime Outputs
- Scrape log: `results/job_search_v6.log`
- Healer log: `results/ats_heal.log`
- Score rejects: `results/job_search_v6_rejected.csv`
- Manual review list: `results/job_search_manual_review.txt`
