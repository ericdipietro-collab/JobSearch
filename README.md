# Job Search Automation Platform v2.0

A local job-search dashboard that discovers jobs from target companies, scores them against your preferences, and tracks your search in a single SQLite database on your own machine.

**[Full User Guide](USER_GUIDE.md)** for the full walkthrough.

---

## Installation

### Source-first install (recommended)

This is the recommended public install path right now.

Why:
- Windows Smart App Control may block unsigned installers, batch files, and portable launchers downloaded from GitHub
- the source workflow avoids shipping a reputationless binary as the primary install method

1. Install Python 3.11 from https://www.python.org/downloads/release/python-3119/
2. Clone the repo:

```powershell
git clone https://github.com/ericdipietro-collab/JobSearch.git
cd JobSearch
```

3. Run the launcher:

```powershell
.\launch.bat
```

On first launch the app will:
- create a Python 3.11 virtual environment
- install dependencies
- create runtime state under `%LOCALAPPDATA%\JobSearchDashboardData`
- start the dashboard at `http://localhost:8501`

If you prefer not to use Git, download the source ZIP from GitHub, extract it, open the extracted folder, and run `launch.bat`.

Important:
- release artifacts and the bundled wheel cache are built for Python 3.11
- other Python versions such as 3.12 or 3.13 may trigger source builds for native dependencies and fail while looking for Visual Studio tools

### Windows installer and portable zip

The release may also include:
- `JobSearchSetup.exe`
- `JobSearchDashboard-portable-<version>.zip`

Important:
- these artifacts are unsigned
- Windows Smart App Control may block them with no bypass option
- if that happens, use the source-first install path above instead

### If you choose to disable Smart App Control

Only do this if you understand the security tradeoff and trust the download source.

Path:
- Windows Security
- `App & browser control`
- `Smart App Control`

After changing the setting, rerun the installer or launcher.

---

## What It Does

- Scrapes target company careers pages across Greenhouse, Lever, Ashby, Workday, Rippling, and SmartRecruiters
- Supports a contractor sourcing lane with Dice and Motion Recruitment
- Scores jobs with configurable title, JD, salary, location, and tier weighting
- Tracks applications, contacts, interviews, offers, and rejections
- Syncs Gmail signals for missed applications, rejections, and interview scheduling
- Stores your base resume for keyword-gap analysis and per-application tailoring
- Includes offer comparison, negotiation planning, and interview debrief tools
- Repairs stale company careers URLs with the ATS Healer

---

## Screenshots

### Home dashboard

![Home dashboard](docs/Screenshots/Homepage.png)

### Job matches

![Job matches](docs/Screenshots/jobmatches.png)

### My Applications

![My Applications](docs/Screenshots/myapplications.png)

### Analytics

![Analytics](docs/Screenshots/analytics.png)

---

## Quick Start

1. Launch the app with `.\launch.bat`
2. Open `Search Settings`
3. Set:
- compensation and location preferences
- job title preferences
- JD keywords
- Gmail settings if desired
- your base resume
4. Review `Target Companies`
5. Run `Run Job Search`

---

## CLI

```bash
python -m jobsearch.cli run
python -m jobsearch.cli run --contract-sources
python -m jobsearch.cli heal --all
python -m jobsearch.cli dashboard
```

---

## Deep Search

Optional browser-assisted scraping and healing for JS-heavy sites:

```bash
deep_search\install_deep_search.bat
python -m jobsearch.cli run --deep-search
python -m jobsearch.cli heal --deep --all
```

Use deep runs sparingly. The static pipeline should be your default.

---

## Data Location

Runtime state now lives under:

```text
%LOCALAPPDATA%\JobSearchDashboardData
```

Important files:
- `config\job_search_preferences.yaml`
- `config\job_search_companies.yaml`
- `config\job_search_companies_contract.yaml`
- `results\jobsearch.db`
- `results\job_search_v6.log`
- `results\ats_heal.log`
- `results\job_search_v6_rejected.csv`
- `results\job_search_manual_review.txt`

This keeps state outside the install or extracted app folder, which is safer for upgrades and portable use.

---

## Troubleshooting

**Windows blocks the installer or launcher**
- This is usually Smart App Control or SmartScreen reacting to an unsigned download
- The recommended workaround is to install from source instead

**No useful matches are being kept**
- Lower the salary floor in `Search Settings`
- Relax title or JD keyword weights
- Inspect the rejected CSV and scoring details

**Gmail sync fails**
- Use a Google App Password, not your main password
- Create one at `https://myaccount.google.com/apppasswords`

**Resume gap analysis is empty**
- Upload or paste your resume in `Search Settings -> Base Resume`
- Rerun the scraper so matched keywords are current

**Companies are blocked or stale**
- Run `Target Companies -> Heal ATS`
- Review `Target Companies -> Scraper Health`

See [GETTING_STARTED.md](GETTING_STARTED.md) for the step-by-step setup flow.
