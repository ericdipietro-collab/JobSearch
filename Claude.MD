# Job Search Healer - Technical Specification & Guide

## 🚀 Build & Run Commands
- **Main Scraper:** `python run_job_search_v6.py`
- **Main Scraper (deep search):** `python run_job_search_v6.py --deep-search`
- **Dashboard (Main UI):** `streamlit run app.py`
- **ATS URL Repair Tool:** `python heal_ats_yaml.py`
- **ATS URL Repair Tool (deep heal):** `python heal_ats_yaml.py --deep-heal`
- **Install Environment:** `pip install -r requirements.txt`
- **Install Deep Search add-on (Windows):** `deep_search\install_deep_search.bat`
- **Install Deep Search add-on (Mac/Linux):** `bash deep_search/install_deep_search.sh`

## 🎯 Bucket Filter & Scoring Logic
The pipeline follows a strict multi-stage "Soft-Drop" funnel:

### 1. Hard Gating (Location & Salary)
- **Location Policy:** Filter by Remote or Hybrid based on user-configured location preferences.
- **Salary Floor:** Check against `min_salary_usd`. Apply a **5% negotiation buffer** ($0.95$ multiplier). Support different minimums for Remote vs. Hybrid roles.

### 2. Job Title Evaluation
- **Disqualification:** Immediate rejection if any keyword from the `negative_disqualifiers` list is present in the title.
- **Scoring Boost:** Titles matching high-weight positive keywords (weight >= 8) receive a **base score of 50** ("Fast-Track").

### 3. JD Refinement (Positive/Negative Scoring)
- **JD Multiplier:** If a job is a "Fast-Track" title match, JD keyword points are worth **0.5x** to prevent score inflation.
- **Refinement:** Search the JD for positive/negative keywords to calculate the final score (0–100).
- **Keyword Integrity:** Use word boundaries (`\b`) to ensure "SQL" does not match "sequelized."

## 📂 Project Structure & State
- **`/config`**:
    - `job_search_companies.yaml`: Registry of targets and ATS adapters.
    - `job_search_preferences.yaml`: Gating rules, keyword weights, and salary floors.
- **`/results`**:
    - `job_search_results.xlsx`: Main data store (Kept, Rejected, and History sheets).
    - `job_search.log`: Technical execution logs.
- **History Tracking:** Jobs existing in the results history must be ignored on subsequent runs to prevent duplicate effort.

## 🖥️ UI Requirements & Buckets
The Streamlit UI must support the following workflow:
- **Ranking Buckets:** Display jobs in three distinct views: `Apply Now`, `Further Research`, and `Rejected`.
- **Status Management:** Allow updating records to transition jobs:
    - `Apply Now` → `Applied` or `Rejected`.
    - `Further Research` → `Apply Now` or `Rejected`.
- **Configuration Modules:**
    - **Preferences Editor:** UI to modify salary floors, location settings, and keyword weights.
    - **Company Manager:** UI to add/edit/delete companies in the registry.

## 📜 Development Standards
- **Pathing:** Use `pathlib` for all file operations. No absolute paths.
- **Privacy:** Keep personal data (salary targets, specific address) within the `/config` or `/results` folders, which are excluded via `.gitignore`.
- **Repair Logic:** Use `heal_ats_yaml.py` to maintain the company registry by probing for Greenhouse, Lever, Ashby, or Workday patterns.