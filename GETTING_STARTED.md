# Getting Started v2.0

This is the fastest path from download to a working local dashboard.

## Recommended install path

Use the source-first path.

Why:
- Windows Smart App Control may block unsigned installers and downloaded launch scripts
- running from source is the most reliable zero-cost install path right now

## Prerequisites

- Windows
- Python 3.9 or newer
- Git optional but recommended

Install Python from:
- https://www.python.org/downloads/

Make sure Python is available on `PATH`.

## Option A: clone with Git

```powershell
git clone https://github.com/ericdipietro-collab/JobSearch.git
cd JobSearch
.\launch.bat
```

## Option B: download source ZIP

1. Download the source ZIP from GitHub
2. Extract it
3. Open the extracted folder in File Explorer
4. Run:

```powershell
.\launch.bat
```

## What happens on first launch

The launcher will:
- find Python
- create a virtual environment
- install dependencies
- seed default config if missing
- migrate older install-local config/database forward if present
- create runtime state under `%LOCALAPPDATA%\JobSearchDashboardData`
- start Streamlit at `http://localhost:8501`

## Runtime data location

The app now stores writable state here:

```text
%LOCALAPPDATA%\JobSearchDashboardData
```

Important paths:
- `%LOCALAPPDATA%\JobSearchDashboardData\config`
- `%LOCALAPPDATA%\JobSearchDashboardData\results`
- `%LOCALAPPDATA%\JobSearchDashboardData\data`
- `%LOCALAPPDATA%\JobSearchDashboardData\.venv`

## Initial setup in the app

### Search Settings

Set:
- compensation and location preferences
- job title preferences
- JD keyword preferences
- Gmail settings if using inbox sync
- your base resume text or uploaded file

### Target Companies

Review:
- primary ATS registry
- contractor registry
- scraper health
- ATS healer

### Run Job Search

Start with:
- `Deep Search`: off
- your primary companies file selected
- `Use Contractor Sources`: optional

## Optional advanced paths

### Unsigned installer

You may also see release assets like `JobSearchSetup.exe` or a portable zip.

Important:
- they are unsigned
- Windows Smart App Control may block them with no bypass option
- if that happens, use the source-first install path instead

### If you choose to disable Smart App Control

Only do this if you understand the security tradeoff and trust the download source.

Path:
- Windows Security
- `App & browser control`
- `Smart App Control`

After changing the setting, rerun the installer or launcher.

### Deep Search / Deep Heal

Install the optional deep-search add-on:

```powershell
deep_search\install_deep_search.bat
```

Then use:

```powershell
python -m jobsearch.cli run --deep-search
python -m jobsearch.cli heal --deep --all
```

Use deep runs sparingly. Static scraping should be the default.

## Troubleshooting

**Python not found**
- reinstall Python
- make sure `Add Python to PATH` was selected
- reopen PowerShell and rerun `.\launch.bat`

**Gmail sync fails**
- use a Google App Password
- create one at `https://myaccount.google.com/apppasswords`

**No useful jobs are being kept**
- lower the salary floor
- relax title/JD keyword settings
- inspect the rejected CSV and score details

**Companies are stale or blocked**
- use `Target Companies -> Heal ATS`
- review `Target Companies -> Scraper Health`
