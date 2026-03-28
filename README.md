# Job Search Automation Platform

A high-precision job search automation tool that scrapes company career pages, scores opportunities using a tiered keyword funnel, and tracks your pipeline through a full Streamlit dashboard.

Designed for professionals in **Financial Services**, **FinTech**, and adjacent technical domains — but fully configurable for any field.

---

## Features

- **Multi-stage funnel scoring** — hard gating (location, salary) → title gate → JD keyword scoring → action bucket assignment
- **Weighted keyword engine** — positive/negative scores for title keywords and JD body, with a fast-track tier for high-signal titles
- **Company registry with ATS adapters** — supports Greenhouse, Lever, Ashby, Workday, and manual/custom fallback
- **Automatic ATS healing** — probes for correct Workday subdomain (`wd1–wd25`), discovers Greenhouse/Lever/Ashby URLs
- **Full Streamlit UI** — results dashboard, pipeline tracker, company manager, preferences editor, run controls
- **Personal ATS** — SQLite-backed opportunity tracking with stage machine (New → Applied → Offer/Rejected), activity log, and analytics
- **History deduplication** — jobs seen in prior runs are skipped automatically

---

## Quick Start

### 1. Clone and install

```bash
git clone <your-repo-url>
cd JobSearch
pip install -r requirements.txt
```

### 2. Configure preferences

```bash
cp config/job_search_preferences.example.yaml config/job_search_preferences.yaml
```

Open `config/job_search_preferences.yaml` and fill in:
- `search.location_preferences.local_hybrid.primary_zip` — your zip code
- `search.location_preferences.local_hybrid.markers` — nearby city names that appear in job listings
- `search.compensation.target_salary_usd` / `min_salary_usd` — your salary floor

The keywords and scoring weights are pre-configured for a FinTech/FinServ product/architect role family. Edit `keywords.body_positive`, `keywords.body_negative`, and `titles.*` sections to match your own target profile.

### 3. Add target companies

Edit `config/job_search_companies.yaml` to add or remove companies. Each entry looks like:

```yaml
- name: Acme Corp
  domain: acmecorp.com
  adapter: greenhouse           # greenhouse | lever | ashby | workday | custom_manual
  adapter_key: acmecorp         # ATS tenant slug
  careers_url: https://boards.greenhouse.io/acmecorp
  tier: 1                       # 1 = top priority, 2 = standard, 3 = opportunistic
  industry: [fintech]
  status: new                   # new | active | changed | broken
```

### 4. Run the scraper

```bash
python run_job_search_v6.py
```

Results land in `results/job_search_results.xlsx` with three sheets: **All Jobs**, **Kept**, and **Rejected**.

### 5. Open the dashboard

```bash
streamlit run app.py
```

---

## Command-line options

```
python run_job_search_v6.py [options]

  --prefs PATH         Override preferences file path
  --companies PATH     Override companies registry path
  --test-companies     Use config/job_search_companies_test.yaml (small test set)
```

---

## Project structure

```
JobSearch/
├── run_job_search_v6.py          # Single entrypoint (launcher + source patcher)
├── job_search_v6.py              # Core scraper engine
├── app.py                        # Streamlit dashboard
├── heal_ats_yaml.py              # ATS URL repair tool
│
├── config/
│   ├── job_search_companies.yaml         # Company registry (edit this)
│   ├── job_search_companies_test.yaml    # Small test set for --test-companies
│   ├── job_search_preferences.yaml       # Your preferences (gitignored)
│   └── job_search_preferences.example.yaml  # Template — copy and fill in
│
├── db/
│   ├── connection.py     # SQLite connection + Streamlit cache
│   ├── models.py         # Opportunity, StageHistory, Activity dataclasses
│   └── schema.py         # DDL + init_db()
│
├── services/
│   ├── opportunity_service.py    # Upsert, sync from Excel, list/filter
│   ├── pipeline_service.py       # Stage machine + transitions
│   ├── analytics_service.py      # Funnel metrics, score analysis
│   └── importer.py               # Import from external tracker CSV
│
├── pages/
│   ├── pipeline_page.py          # Pipeline Kanban/list view
│   └── analytics_page.py         # Analytics dashboard tabs
│
└── results/                      # Runtime outputs (gitignored)
    ├── job_search_results.xlsx
    ├── jobsearch.db
    └── *.log
```

---

## Scoring system

### Hard gates (jobs are dropped before scoring if they fail)

| Gate | Config key | Behavior |
|---|---|---|
| Location | `search.location_policy` | `remote_only`, `remote_or_hybrid`, or `any` |
| Salary floor | `search.compensation.min_salary_usd` | 5% negotiation buffer applied |
| Job age | `search.recency.max_job_age_days` | Default 21 days |

### Title gate

- Title must contain at least one keyword from `titles.positive_keywords`
- Title is immediately rejected if it matches any `titles.negative_disqualifiers`
- High-weight titles (weight ≥ `fast_track_min_weight`) start with `fast_track_base_score` (default 50) and receive JD keyword scores at **0.5×**

### JD keyword scoring

- `keywords.body_positive` — each match adds points (capped at `positive_keyword_cap`)
- `keywords.body_negative` — each match subtracts points (capped at `negative_keyword_cap`)
- Only unique matches count by default (`count_unique_matches_only: true`)

### Action buckets

Jobs that pass all gates and meet `minimum_score_to_keep` are assigned a bucket:

| Bucket | Meaning |
|---|---|
| APPLY NOW | High score + strong title + known salary |
| REVIEW TODAY | High score or strong tier |
| WATCH | Moderate score |
| MANUAL REVIEW | Flagged for manual inspection |
| IGNORE | Below score threshold |

---

## ATS adapters

The scraper supports these ATS types out of the box:

| Adapter | Description |
|---|---|
| `greenhouse` | `boards.greenhouse.io/{key}` |
| `lever` | `jobs.lever.co/{key}` |
| `ashby` | `jobs.ashbyhq.com/{key}` |
| `workday` | `{key}.wd{N}.myworkdayjobs.com` — N is discovered automatically |
| `custom_manual` | Fetches `careers_url` directly; falls back to domain search |

### Healing the registry

Run the ATS healer to validate and fix URLs for companies marked `new`, `changed`, or `broken`:

```bash
python heal_ats_yaml.py          # Only processes new/changed/broken entries
python heal_ats_yaml.py --all    # Re-validates all entries
```

The healer can also be triggered from the **Companies** tab in the Streamlit UI.

---

## Personal ATS (pipeline tracking)

After a scraper run, the UI syncs results into a local SQLite database (`results/jobsearch.db`). From the **Pipeline** page you can:

- Move opportunities through stages: New → Scored → Shortlisted → Applied → Recruiter Screen → Hiring Manager → Panel → Final Round → Offer / Rejected / Archived
- Log activities (calls, interviews, emails) against each opportunity
- Add notes and set priority flags
- Bulk-transition multiple opportunities at once

The **Analytics** page shows funnel metrics, conversion rates, score distributions, and time-in-stage analysis.

---

## Privacy

The following are gitignored and never committed:

- `config/job_search_preferences.yaml` — contains your salary targets and location
- `results/` — all run outputs, Excel files, and the SQLite ATS database
- `*.db`, `*.db-shm`, `*.db-wal` — SQLite files anywhere in the repo
- `*_active_config.json`, `*_history_v6.json` — runtime state files

Only `config/job_search_preferences.example.yaml` (this template) is tracked by git.

---

## Requirements

- Python 3.9+
- See `requirements.txt` for package versions

Key dependencies: `requests`, `PyYAML`, `beautifulsoup4`, `pandas`, `openpyxl`, `streamlit`
