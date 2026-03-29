# Getting Started

Everything you need to go from download to running dashboard in about 5 minutes.

---

## What you need first

**Python 3.9 or newer.** That's it. The launcher handles everything else automatically.

> **Don't have Python?**
> 1. Go to [python.org/downloads](https://www.python.org/downloads/) and click the big yellow button
> 2. Run the installer
> 3. **Important:** on the first installer screen, check the box that says **"Add Python to PATH"**
> 4. Finish the install, then come back here

---

## Step 1 — Download the app

**Option A: Download as ZIP (no Git required)**
1. On this GitHub page, click the green **Code** button
2. Click **Download ZIP**
3. Unzip it anywhere you like (Desktop, Documents, etc.)

**Option B: Clone with Git**
```
git clone https://github.com/YOUR_USERNAME/JobSearch.git
```

---

## Step 2 — Launch the app

Open the folder you just unzipped/cloned and double-click the launcher for your operating system:

| Operating system | File to double-click |
|---|---|
| **Windows** | `launch.bat` |
| **macOS** | `launch.command` |
| **Linux** | Right-click → Run as Program, or `bash launch.sh` in terminal |

> **macOS note:** The first time you run `launch.command`, macOS may block it with a security warning.
> Right-click the file → **Open** → **Open** to allow it. You only need to do this once.

**What happens on first launch:**
1. The launcher checks your Python version
2. Creates a private `.venv` folder with all dependencies (~1 minute, one-time only)
3. Copies the example config to `config/job_search_preferences.yaml`
4. Opens the dashboard in your browser at `http://localhost:8501`

Subsequent launches skip straight to step 4 — it's fast.

---

## Step 3 — Complete the setup checklist

When the dashboard opens you'll see a **Setup Checklist** at the top of the Home page. It walks you through four steps:

### ① Configure search preferences

Go to **Search Settings** in the left sidebar.

Under **Compensation & Location**, set:
- Your **minimum salary** (the floor — jobs below this are filtered out)
- Whether you want **Remote only** or **Remote or Hybrid**

Hit **Save Compensation & Location**. ✓

> You can also edit keyword weights, scoring thresholds, and other preferences here,
> but the defaults work well to start.

### ② Register target companies

Go to **Target Companies** → **Add / Edit Company** tab.

Add the companies whose job pages you want monitored. For each one you need:
- Company name
- The URL of their careers page
- The ATS type (Greenhouse, Lever, Ashby, Workday, or custom)

> **Not sure which ATS a company uses?**
> Go to **Heal ATS** tab and let the tool detect it automatically.

### ③ Run the job search pipeline

Go to **Run Job Search** and click **Start Pipeline**.

The scraper visits each company's careers page and pulls open roles. Results appear
in **Job Matches** when it finishes. Your first run typically takes 5–15 minutes depending
on how many companies you've added.

### ④ Track your first application

Go to **My Applications** → click **➕ Add** → fill in a company and role.

You can add applications manually here, or use the **✅ Apply & Track** button on any
job in the **Job Matches** page to create one automatically.

---

## Daily workflow

| What you want to do | Where to go |
|---|---|
| See today's priorities | **Home** — KPIs, overdue follow-ups, this week's interviews |
| Check new job matches | **Job Matches** → Apply Now tab |
| Log that you applied somewhere | **My Applications** → open the app → Timeline tab → Add event |
| Prep for an interview | **My Applications** → open the app → Prep tab |
| Track a networking conversation | **My Applications** → ➕ Add → choose "Opportunity" |
| Review your weekly job search activity | **Weekly Report** |
| Research a company | **Company Profiles** → ➕ New Profile |
| Practice interview answers | **Question Bank** |
| Draft a follow-up email | **Templates** |

---

## Keeping your data safe

Your data lives in `results/jobsearch.db` on your own computer — nothing is sent anywhere.

**To back up:** Go to **Search Settings** → **Backup & Restore** tab → **Create Backup**.
Download the ZIP and store it somewhere safe (Google Drive, external drive, etc.).

**To restore on a new machine:** Follow Steps 1–2, then go to Backup & Restore → upload
your backup ZIP → click **Restore Files**.

---

## Frequently asked questions

**The browser doesn't open automatically.**
Navigate to `http://localhost:8501` manually.

**I see "No results yet" on Job Matches.**
You need to run the pipeline first (Step 3 above). If you've run it and still see nothing,
go to **Run Job Search** → **Clear History**, then run again.

**The app won't start — Python not found.**
Make sure you checked "Add Python to PATH" during install. If you missed it, re-run the
Python installer, choose "Modify", and add PATH. Or uninstall and reinstall.

**I want to stop the app.**
Close the terminal / command prompt window that opened when you launched.
The browser tab will stop working, but your data is saved.

**Can two people use the same installation?**
The app is designed for one person — the SQLite database is a single file on your computer.
Each person should download and run their own copy.

**I already have job search data in a spreadsheet.**
Go to **My Applications** → expand the **LinkedIn CSV Import** section at the top.
You can upload a LinkedIn job application export (or any CSV with Company/Role/Date columns).

---

## Troubleshooting

If something goes wrong after a launch, the terminal window shows the full error message.

You can also check the scraper log: **Run Job Search** → scroll down → **Scraper Run Log** expander.

If you're stuck, open an issue at [github.com/YOUR_USERNAME/JobSearch/issues](https://github.com/YOUR_USERNAME/JobSearch/issues).
