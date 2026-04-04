# Getting Started — v2.0

Everything needed to go from download to a working local dashboard in a few minutes.

## Prerequisites

- Windows installer: no manual setup required
- Manual install: Python 3.9 or newer

If you install Python manually, make sure `python` is on `PATH`.

## Install

### Option A: Windows installer

1. Download `JobSearchSetup.exe` from the Releases page.
2. Run the installer.
3. Launch the app from the Desktop or Start Menu shortcut.

The installer pre-warms the runtime environment and can install from bundled wheels, so first launch is faster and less dependent on live package downloads.

### Option B: Manual install

1. Download this repo as a ZIP or clone it.
2. Open the repo folder.
3. Run `launch.bat` on Windows, `launch.command` on macOS, or `bash launch.sh` on Linux.

On first launch the app will:

1. create `.venv`
2. install dependencies
3. copy `config/job_search_preferences.example.yaml` to `config/job_search_preferences.yaml` if missing
4. start Streamlit at `http://localhost:8501`

## First Run

Use the left sidebar to complete the initial setup.

### Search Settings

The active tabs are:

- `Compensation & Location`
- `Job Title Settings`
- `Job Description Keywords`
- `Scoring Settings`
- `Advanced Editor`
- `App Settings`
- `Base Resume`

Start by setting:

- salary floor and target
- work location policy (remote only, hybrid, or both)
- job title keywords and weights
- Gmail settings if you want live inbox sync
- your base resume text or uploaded resume file for gap analysis and tailoring

### Target Companies

The active tabs are:

- `List`
- `Add / Edit`
- `Fix Job Listings` — automatically rediscovers broken or stale job board URLs
- `Advanced Editor`
- `Scraper Health` — shows which companies have gone dark and how long they have been returning no results

Switch between the **Main Company List** and the **Contractor Company List** using the selector at the top of the page.

Use `Add / Edit` to add or update companies. For each company you usually want:

- `Company Name`
- `Website Domain`
- `Careers Page URL`
- `Job Board Type` (Greenhouse, Lever, Ashby, Workday, etc.)
- `Job Board Identifier` when known
- `Priority Tier (1 = highest)`

Enable `Search Manually (skip automatic scraping)` for targets you want to keep in the list but handle yourself.

### Run Job Search

Use `Run Job Search` to start the scraper from the dashboard, or run:

```bash
python -m jobsearch.cli run
```

For contract-focused discovery, enable `Use Contractor Sources` in the dashboard or run:

```bash
python -m jobsearch.cli run --contract-sources
```

To combine your normal ATS targets with contractor sources in one run, keep the primary companies file selected and enable `Use Contractor Sources`.

### Track Applications

Use `My Applications` to track the pipeline once jobs are saved into the database.

Current workflow highlights:

- live Gmail inbox sync for missed applications, rejections, and interview requests
- follow-up reminders with quick snooze / sent actions
- offer comparison and negotiation worksheet
- interview debrief capture
- resume tailoring per application from the stored base resume
- question bank linked to companies and applications

### Analytics

Key analytics currently available:

- funnel overview (discovered → applied → interviewed → offers)
- score distribution by pipeline stage
- resume keyword gap analysis — shows terms that appear in high-scoring roles but are missing from your stored resume
- rejection pattern intelligence
- interview signal / debrief trends
- score vs. outcome correlation

## Deep Search / Deep Heal

These are optional and slower than the standard static flow.

Install the add-on scripts from the `deep_search/` folder:

- Windows: `deep_search\install_deep_search.bat`
- macOS / Linux: `bash deep_search/install_deep_search.sh`

Use:

- `Run Job Search` → enable **Deep Search (slower — uses browser)** for JavaScript-heavy careers pages
- `Target Companies` → `Fix Job Listings` → enable **Deep Search** for ATS rediscovery and rendered-board detection
- Repeated failures enter a short cooldown automatically; lower-priority targets can be promoted to manual-only after repeated failures
- Check `Target Companies` → `Scraper Health` to see which companies are dark and for how long

CLI equivalents:

```bash
python -m jobsearch.cli run --deep-search
python -m jobsearch.cli heal --deep --all
```

## Data and Backups

Your data is local.

- Jobs and tracking data: `results/jobsearch.db`
- Scrape log: `results/job_search_v6.log`
- Heal log: `results/ats_heal.log`
- Manual-review list: `results/job_search_manual_review.txt`
- Score-rejected jobs: `results/job_search_v6_rejected.csv`
- Personal preferences: `config/job_search_preferences.yaml`
- Company registry: `config/job_search_companies.yaml`
- Contractor registry: `config/job_search_companies_contract.yaml`

Additional state stored in the database:

- app settings
- Gmail sync settings
- base resume text
- email signals and interview extraction

To back up the app state, copy:

- `results/`
- `config/job_search_preferences.yaml`
- `config/job_search_companies.yaml`
- `config/job_search_companies_contract.yaml`

## Troubleshooting

If the dashboard does not open automatically, browse to `http://localhost:8501`.

If you get no useful matches:

- lower the salary floor
- relax title or keyword weights
- run Heal ATS first if company URLs look stale

If a scrape or heal run behaves unexpectedly, inspect:

- `results/job_search_v6.log`
- `results/ats_heal.log`
- `results/job_search_manual_review.txt`
- `results/job_search_v6_rejected.csv`
- `Target Companies` → `Scraper Health` for companies returning zero results

If Gmail sync says authentication failed:

- enable 2-Step Verification on the Google account
- create a Google App Password at `https://myaccount.google.com/apppasswords`
- save the Gmail address and App Password in `Search Settings -> App Settings`

If resume gap analysis or tailoring is empty:

- upload or paste your master resume in `Search Settings -> Base Resume`
- make sure the jobs you want to analyze have recent scraper data

If Python is not found on manual install, reinstall Python and make sure it is added to `PATH`.
