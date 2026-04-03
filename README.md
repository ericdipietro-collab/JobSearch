# Job Search Automation Platform

A local job-search dashboard that discovers jobs from target companies, scores them against your preferences, and tracks applications in a single SQLite database.

**[→ User Guide](USER_GUIDE.md)** — full walkthrough of every feature

## Installation

### Windows installer

1. Download `JobSearchSetup.exe` from the Releases page.
2. Run the installer.
3. Launch from the Desktop or Start Menu shortcut.

The installer supports a per-user install, bundles pinned runtime wheels, and pre-warms the runtime environment on first install.

### Manual install

See [GETTING_STARTED.md](GETTING_STARTED.md).

Short version:

1. Install Python 3.9+.
2. Download or clone this repo.
3. Run `launch.bat` on Windows, `launch.command` on macOS, or `bash launch.sh` on Linux.

## What It Does

- Scrapes company careers pages from ATS providers like Greenhouse, Lever, Ashby, Workday, Rippling, and SmartRecruiters
- Supports an optional contractor lane using curated external contract-oriented sources
- Saves jobs directly into `results/jobsearch.db`
- Scores jobs using your title, keyword, salary, and tier preferences
- Lets you manage applications, contacts, journals, templates, training notes, and reports from the dashboard
- Syncs Gmail signals to detect missed applications, rejections, and interview requests
- Stores a base resume locally for keyword-gap analysis and per-application tailoring
- Includes offer comparison, negotiation planning, interview debriefs, and network leverage views
- Includes ATS healing to repair or rediscover stale careers URLs

## Current Product Surface

Main dashboard pages:

- `Home`
- `Job Matches`
- `My Applications`
- `Journal`
- `Contacts`
- `Company Profiles`
- `Training`
- `Question Bank`
- `Weekly Report`
- `Templates`
- `Pipeline`
- `Analytics`
- `Run Job Search`
- `Search Settings`
- `Target Companies`

### Search Settings tabs

- `Compensation & Location`
- `Title Evaluation`
- `JD Evaluation`
- `Scoring & Rescue`
- `Full YAML Editor`
- `App Settings`
- `Base Resume`

### Target Companies tabs

- `List`
- `Add / Edit`
- `Heal ATS`
- `YAML Editor`

### High-leverage workflows

- `My Applications`
  - Gmail inbox signal import / live sync
  - follow-up scheduler
  - offer comparison
  - negotiation playbook
  - interview debriefs
  - resume tailoring per application
- `Analytics`
  - resume keyword gap analysis
  - rejection pattern intelligence
  - interview signal correlations
- `Company Profiles` / `Contacts`
  - network leverage scoring by company
- `Job Matches`
  - role velocity
  - contractor/full-time filtering
  - structured manual review queue

## CLI

Run the scraper:

```bash
python -m jobsearch.cli run
```

Run the contractor lane:

```bash
python -m jobsearch.cli run --contract-sources
```

Run ATS and contractor sources together:

```bash
python -m jobsearch.cli run --contract-sources --companies config/job_search_companies.yaml
```

Run ATS healing:

```bash
python -m jobsearch.cli heal --all
```

Launch the dashboard:

```bash
python -m jobsearch.cli dashboard
```

If running from a raw checkout without installing the package, the checkout-safe entrypoints are:

```bash
python src/jobsearch/cli.py dashboard
python src/jobsearch/cli.py run
```

## Deep Search and Deep Heal

The optional `deep_search/` add-on uses Playwright and Chromium for JavaScript-heavy sites.

Install:

- Windows: `deep_search\install_deep_search.bat`
- macOS / Linux: `bash deep_search/install_deep_search.sh`

Usage:

```bash
python -m jobsearch.cli run --deep-search
python -m jobsearch.cli heal --deep --all
```

These modes are slower, but they improve coverage on JS-rendered sites. Protected sites may still be routed to manual review.
Repeated Heal ATS failures now enter a short cooldown automatically, and lower-priority targets can be suggested or promoted to `manual_only` after repeated failures.

## Data and Logs

Everything stays local.

- Database: `results/jobsearch.db`
- Scrape log: `results/job_search_v6.log`
- Heal log: `results/ats_heal.log`
- Manual-review list: `results/job_search_manual_review.txt`
- Score-rejected jobs: `results/job_search_v6_rejected.csv`
- Preferences: `config/job_search_preferences.yaml`
- Company registry: `config/job_search_companies.yaml`
- Contractor registry: `config/job_search_companies_contract.yaml`

Additional local state:

- Base resume and app settings are stored in `results/jobsearch.db`
- Gmail sync settings are stored locally in the app settings table

To back up your state, copy:

- `results/`
- `config/job_search_preferences.yaml`
- `config/job_search_companies.yaml`
- `config/job_search_companies_contract.yaml`

## Notes

- The current modular app lives under `src/jobsearch/`
- `app.py` is the Streamlit entrypoint
- The installer should package `src/` and `deep_search/`, not the old root-level pre-refactor modules

## Troubleshooting

If no results are being kept:

- lower the salary floor
- relax title or keyword weights
- inspect `results/job_search_v6.log` for adapter timing and score breakdowns
- inspect `results/job_search_v6_rejected.csv` for score rejects

If Gmail sync fails:

- for Gmail accounts, use a Google App Password rather than your main account password
- App Passwords are managed at `https://myaccount.google.com/apppasswords`
- store Gmail settings in `Search Settings -> App Settings`

If the resume gap analysis is empty:

- add your master resume in `Search Settings -> Base Resume`
- rerun the scraper so matched keywords are current

If companies are blocked or require manual follow-up:

- inspect `results/job_search_manual_review.txt`
- review those targets manually instead of retrying aggressively

If healing is slow:

- inspect `results/ats_heal.log`
- reduce workers or disable deep heal for maintenance runs
- repeated failures now cool down automatically; review `manual_only` targets in `Target Companies` before forcing another broad heal run

If a company goes stale:

- use `Target Companies` -> `Heal ATS`
- use `Deep` when static probing is not enough
