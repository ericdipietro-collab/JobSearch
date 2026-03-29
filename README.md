# Job Search Automation Platform

A personal job search tool that automatically finds open roles at companies you care about, scores them against your preferences, and helps you track everything from first contact through offer.

Built for anyone actively job searching. No subscriptions, no data sold to recruiters — everything runs locally on your own computer.

---

## Quick start

**→ [GETTING_STARTED.md](GETTING_STARTED.md) — step-by-step setup guide for non-technical users**

Short version:
1. Install [Python 3.9+](https://www.python.org/downloads/) — check "Add Python to PATH"
2. Download this repo as a ZIP and unzip it
3. Double-click `launch.bat` (Windows) or `launch.command` (macOS)
4. Browser opens to the dashboard — follow the setup checklist

The launcher creates a virtual environment, installs all dependencies, and copies the example config automatically on first run.

---

## What it does

| Feature | What you get |
|---|---|
| **Automated job scraping** | Checks career pages at companies you choose, pulls open roles, filters by your salary floor and location |
| **Smart scoring** | Ranks jobs by how well they match your background using keyword weights you define |
| **Home dashboard** | KPIs, pipeline snapshot, activity trend charts, overdue follow-ups, and upcoming interviews at a glance |
| **Job Matches** | Apply Now / Review Today / Watch buckets with one-click Apply & Track |
| **My Applications** | Full CRM: timeline, interview log, contacts, prep tab, offer negotiation worksheet |
| **Offer comparison** | Side-by-side table of base, bonus, equity, PTO, and total comp across all active offers |
| **Email Templates** | Thank-you notes, follow-up templates, cold outreach — with variable substitution |
| **Job Search Journal** | Daily mood-tagged notes to keep perspective during a long search |
| **Networking Contacts** | Contact book with follow-up reminders and direct mailto/LinkedIn links |
| **Interview Question Bank** | Store and refine your STAR-method answers by category |
| **Company Research Profiles** | Persistent research notes (culture, interview process, red flags) shared across all apps to that company |
| **Training tracker** | Plan and track courses/certifications with status and completion dates |
| **Weekly Activity Report** | Configurable weekly goal, activity log for any date range, copy-paste unemployment certification text |
| **Target Companies manager** | Add, edit, and bulk-update the list of companies being scraped — ATS Healer auto-fixes broken URLs |
| **Search Settings** | Edit salary floor, location, keyword weights, and scoring thresholds in the UI — no YAML editing required |
| **Backup & Restore** | One-click ZIP backup of your database and config; restore from backup on a new machine |

---

## Setup checklist (first run)

The Home page shows a setup checklist that guides you through:

1. **Configure preferences** — set your salary floor and location policy in Search Settings
2. **Register companies** — add target companies in the Target Companies page
3. **Run the pipeline** — fetch your first batch of job matches
4. **Track an application** — add your first entry in My Applications

The checklist disappears once all four steps are complete.

---

## Configuring your search

Two config files live in `config/`. Both are gitignored so your personal salary and location data are never uploaded to GitHub.

### `job_search_preferences.yaml`

Controls salary floors, location policy, and the keyword scoring engine. The launcher auto-creates this from the example file on first run. Then open **Search Settings** in the dashboard to customise it — no YAML editing needed.

**Fastest way to fill this out:** use the AI prompts in [docs/AI_SETUP_PROMPTS.md](docs/AI_SETUP_PROMPTS.md) to generate a customised version from your resume.

### `job_search_companies.yaml`

**A starter list of ~485 companies is already included** — weighted toward FinTech, FinServ, and adjacent tech (wealth management, payments, data platforms, insurtech, banking software, enterprise SaaS). Add, edit, or remove companies from the **Target Companies** page in the dashboard.

Each entry looks like:

```yaml
- name: Acme Corp
  domain: acmecorp.com
  adapter: greenhouse       # greenhouse | lever | ashby | workday | custom_manual
  adapter_key: acmecorp     # slug used by their ATS
  careers_url: https://boards.greenhouse.io/acmecorp
  tier: 1                   # 1 = top priority, 2 = standard, 3 = opportunistic
  status: active
```

**Need companies for a different field?** Use Prompt 2 in [docs/AI_SETUP_PROMPTS.md](docs/AI_SETUP_PROMPTS.md).

---

## For co-workers sharing this tool

Each person runs their own local copy. Your salary, location, and application data never leave your computer.

1. Download as ZIP from GitHub and unzip
2. Double-click the launcher for your OS
3. The setup checklist guides you through the rest

**What gets shared (in the repo):**
- The app code and `config/job_search_preferences.example.yaml`
- `config/job_search_companies.yaml` — starter company list (everyone customises this)

**What stays private (gitignored, never uploaded):**
- `config/job_search_preferences.yaml` — your salary targets and location
- `results/` — all scraped jobs, your applications database, and any state files

---

## Data & privacy

Everything runs locally. Your database is `results/jobsearch.db` — a single SQLite file on your computer. No account required, no data sent anywhere.

**Back up your data:** Search Settings → Backup & Restore → Create Backup.

---

## Requirements

- Python 3.9+
- All package dependencies are in `requirements.txt` and installed automatically by the launcher

Key packages: `streamlit`, `requests`, `PyYAML`, `beautifulsoup4`, `pandas`, `openpyxl`

---

## Troubleshooting

**"No results" after running the pipeline**
Your salary floor might be filtering everything out — try lowering `min_salary_usd` in Search Settings, or run **Clear History** in Run Job Search and try again.

**The launcher says Python not found**
Re-run the Python installer, choose Modify, and add Python to PATH. Then re-run the launcher.

**macOS blocks launch.command**
Right-click → Open → Open. You only need to do this once.

**Companies showing as "broken"**
Run the ATS Healer from the Target Companies page — it automatically finds and fixes most broken URLs.

**Something else is wrong**
Check the Scraper Run Log in the Run Job Search page, or open an issue on GitHub.
