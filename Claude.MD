# Job Search Healer - Technical Specification & Guide

## 🚀 Build & Run Commands
- **Main CLI:** `python -m jobsearch.cli`
- **Run Scraper:** `python -m jobsearch.cli run`
- **Run Scraper (deep search):** `python -m jobsearch.cli run --deep-search`
- **Dashboard (Main UI):** `launch.bat` on Windows or `python -m streamlit run app.py`
- **Install Environment:** `pip install -e .`
- **Install Deep Search add-on (Windows):** `deep_search\install_deep_search.bat`

## 🎯 Bucket Filter & Scoring Logic
The pipeline uses a two-layer scoring system. V1 (`scoring.py`) handles hard gates; V2 (`scoring_v2.py`) is primary for all scored jobs.

### 1. Scraper Adapters
Modular adapters in `src/jobsearch/scraper/adapters/` handle ATS-specific parsing:
- `GreenhouseAdapter` (with API-first & embed support)
- `LeverAdapter`
- `AshbyAdapter`
- `WorkdayAdapter` (with wd1..wd25 sweep)
- `RipplingAdapter`
- `SmartRecruitersAdapter`
- `GenericAdapter` (with product/staff filters)

### 2. Scoring Funnel (V2 Primary)
- **V1 Hard Gates** (`scoring.py`): Disqualifies roles containing `negative_disqualifiers`, enforces location/compensation filters. Produces `Disqualified` or `Filtered Out` fit bands — these are final and V2 does not override them.
- **V2 Scoring** (`scoring_v2.py`): For all non-disqualified jobs, computes `final_score = (title_base_pts + keyword.total) * seniority_multiplier * quality_drags`. Writes primary `score` and `fit_band` columns. V2 breakdown columns (`score_v2`, `fit_band_v2`, `v2_*`) retained as annotations.
- **Bucket Thresholds:** Configurable in Search Settings → Scoring Settings. Defaults: Apply Now ≥ 85, Review Today ≥ 70, Watch ≥ 50.
- **Direct Persistence:** Scraped jobs are upserted directly into the unified SQLite database.

### 3. Manual Job Injection (Bookmarklet)
- A bookmarklet in **Search Settings → Extensions & Tools** sends any job page to the dashboard via `POST http://localhost:8505/inject-job` (FastAPI, `api_service.py`).
- LinkedIn-aware: extracts `currentJobId` from the URL and targets the job details panel rather than full page text.
- LLM extraction (Gemini/OpenAI) cleans up company/title/description before scoring.
- Injected jobs are auto-tagged `WATCH` in `job_annotations` so they always surface in Job Matches regardless of score.
- The API server starts automatically when the dashboard launches.

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
