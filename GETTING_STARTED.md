# Getting Started

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
- `Title Evaluation`
- `JD Evaluation`
- `Scoring & Rescue`
- `Full YAML Editor`

Start by setting:

- salary floor
- location policy
- title weights

### Target Companies

The active tabs are:

- `List`
- `Add / Edit`
- `Heal ATS`
- `YAML Editor`

Use `Add / Edit` to add or update companies. For each company you usually want:

- `name`
- `domain`
- `careers_url`
- `adapter`
- `adapter_key` when known
- `tier`

### Run Job Search

Use `Run Job Search` to start the scraper from the dashboard, or run:

```bash
python -m jobsearch.cli run
```

### Track Applications

Use `My Applications` to track the pipeline once jobs are saved into the database.

## Deep Search / Deep Heal

These are optional and slower than the standard static flow.

Install the add-on scripts from the `deep_search/` folder:

- Windows: `deep_search\install_deep_search.bat`
- macOS / Linux: `bash deep_search/install_deep_search.sh`

Use:

- `Run Job Search` + Deep Search for JavaScript-heavy careers pages
- `Target Companies` + Heal ATS + Deep for ATS rediscovery and rendered-board detection

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

To back up the app state, copy:

- `results/`
- `config/job_search_preferences.yaml`
- `config/job_search_companies.yaml`

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

If Python is not found on manual install, reinstall Python and make sure it is added to `PATH`.
