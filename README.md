# Job Search Automation Platform v2.1.1

A comprehensive, local job-search dashboard that discovers jobs from target companies, scores them against your preferences, auto-refreshes on a schedule, and tracks your entire search lifecycle in a single SQLite database on your own machine.

**[Full User Guide](USER_GUIDE.md)** for the full walkthrough.

[![Buy Me A Coffee](https://img.shields.io/badge/Buy%20Me%20A%20Coffee-Donate-orange?style=flat-square&logo=buy-me-a-coffee)](https://www.buymeacoffee.com/ericdipietro)

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
- if you are upgrading from the older split-database v1 release, run the legacy migration script once before launching the new build; current builds upgrade the unified SQLite schema in place, but they do not auto-merge the old split DB pair

### Windows installer and portable zip

The release may also include:
- `JobSearchSetup.exe`
- `JobSearchDashboard-portable-<version>.zip`

Important:
- these artifacts are unsigned
- Windows Smart App Control may block them with no bypass option
- if that happens, use the source-first install path above instead
- if you are upgrading from the older split-database v1 release, run the legacy migration script once before launching the new build; current builds upgrade the unified SQLite schema in place, but they do not auto-merge the old split DB pair

### If you choose to disable Smart App Control

Only do this if you understand the security tradeoff and trust the download source.

Path:
- Windows Security
- `App & browser control`
- `Smart App Control`

After changing the setting, rerun the installer or launcher.

---

## What It Does

### Core Scraping & Discovery
- **18 ATS Adapters** — Scrapes target company careers pages across Greenhouse, Lever, Ashby, Workday, Rippling, SmartRecruiters, USAJOBS, Google Careers, Indeed Connector, and more
- **Contractor sourcing lane** — Dice and Motion Recruitment for contract-to-hire opportunities
- **Aggregator lane** — Adzuna, USAJobs, Jooble, TheMuse for broader board discovery
- **JobSpy experimental lane** — Proprietary job board discovery without inflating ATS metrics
- **ATS Healer** — Auto-discovers stale/missing company careers URLs and detects ATS platforms, even for complex SPA careers pages via Crawl4AI

### Intelligent Scoring & Filtering
- **Two-layer scoring engine** — V1 applies hard gates (negative title disqualifiers, location, work-type filters); V2 is the primary engine that resolves canonical title families, detects seniority, and computes an anchor + baseline keyword score against your preferences
- **Market Strategy & Canonicalization** — Automatically identifies and consolidates duplicate job postings across lanes (ATS vs aggregators). Clusters jobs into market segments (e.g., Fintech, Platform, Data) to visualize market maps and identify strategic resume-to-market gaps.
- **Configurable V2 controls** — Fast-Track thresholds, anchor/baseline/negative caps, and bucket cutoffs (Apply Now / Review Today / Watch) all exposed as interactive sliders in Scoring Settings
- **Experience tolerance gap** — Soft-drop jobs requiring significantly more experience; soft-penalty for near-fit roles
- **Re-scoring without re-scraping** — Update scoring weights and re-score all saved jobs instantly
- **Full-text search (FTS5)** — Sub-15ms semantic search across 10K+ jobs with BM25 ranking
- **Health-Aware Scheduling** — Smart retry prioritization that sorts the scrape queue by high-value, recoverable targets while automatically applying escalating cooldowns to blocked or broken boards.

### Automation & Alerts
- **Background auto-refresh** — Scraper runs on configurable schedule (1-24 hours) without manual intervention
- **High-score job alerts** — Toast notifications when new jobs above "Apply Now" threshold are discovered
- **LLM enrichment** — Google Gemini or OpenAI analyzes JDs for visa sponsorship, tech stack, IC vs Manager roles, and skills gaps
- **Content-hash sync** — Re-scrapes preserve your annotations (notes, status, date_applied); only updates job-provided fields

### Application Tracking & Analysis
- **Action Center** — A "Daily Operating Cockpit" that prioritizes your best moves: Apply Now, Follow Up, or Prep for upcoming interviews based on urgency and impact scores.
- **Tailoring Studio** — Professional-grade resume customization with a locked "Andy Warthog" teal template. Features a structured form editor, AI-powered bullet refinement, and keyword gap detection.
- **Submission Review Cockpit** — A guided human-in-the-loop workflow for manual applications. Includes a pre-flight checklist, export freshness warnings, and structured friction/blocker logging.
- **Learning Loop** — Outcomes-based calibration that measures conversion funnels and identifies "Score Inversions" where low-scored jobs might be outperforming top matches.
- **Rich application tracking** — Records applications, contacts, interviews, offers, rejections
- **Gmail sync** — Auto-detects missed applications, rejections, and interview scheduling emails
- **Skills gap reports** — Per-job missing-skills analysis for targeted interview prep
- **Excel, PDF & DOCX export** — Bundles full application packages (Styled Resume, ATS Resume, Cover Letter) into a single ZIP for submission.
- **Offer comparison & negotiation planning** — Side-by-side offer analysis and negotiation playbook
- **Interview debrief & feedback** — Post-interview notes and learnings capture

---

## Screenshots

### Home dashboard

![Home dashboard](docs/Screenshots/dashboard.png)

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
- scoring caps for title vs. JD weighting
- Gmail settings if desired
- your base resume
4. Review `Target Companies`
5. Run `Run Job Search`

---

## CLI

```bash
python -m jobsearch.cli run
python -m jobsearch.cli run --score-only
python -m jobsearch.cli run --contract-sources
python -m jobsearch.cli run --aggregator-sources
python -m jobsearch.cli run --jobspy-sources
python -m jobsearch.cli heal --all
python -m jobsearch.cli dashboard
```

---

## Building Release Binaries

Build release artifacts on Windows with Python 3.11 installed.

### Classic installer

```powershell
cd installer
.\build_installer.bat
```

Output:
- `dist\JobSearchSetup.exe`

### Portable zip

```powershell
cd installer\portable
.\build_portable.bat
```

Output:
- `dist\JobSearchDashboard-portable-2.0.0.zip`

### MSIX

```powershell
cd installer\msix
.\build_msix.bat
```

Output:
- `dist\JobSearchDashboard_2.0.0.0_x64.msix`

Optional:
- set `JOBSEARCH_APPINSTALLER_BASE_URI` before running `build_msix.bat` to also generate `dist\JobSearchDashboard.appinstaller`
- set `JOBSEARCH_MSIX_PFX_PATH` and `JOBSEARCH_MSIX_PFX_PASSWORD` to sign the MSIX during build

Important:
- rebuild binaries after any runtime-visible code or config change
- portable, installer, and MSIX builds should ship the current registry defaults, including the aggregator and JobSpy registries

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
- `config\job_search_companies_aggregators.yaml`
- `config\job_search_companies_jobspy.yaml`
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
- Relax title weighting, JD keyword caps, or title gates in `Search Settings -> Scoring Settings`
- Inspect the rejected CSV and scoring details

**Too many out-of-area onsite or hybrid roles are showing up**
- Use `Search Settings -> Compensation & Location`
- The app now treats non-local onsite/hybrid roles as a hard filter
- After changing settings, click `Search Settings -> Scoring Settings -> Re-score Saved Jobs`

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
