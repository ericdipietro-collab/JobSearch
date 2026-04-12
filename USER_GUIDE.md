# User Guide — v2.2

This guide covers daily use of the Job Search dashboard — from first-time setup through running searches, tailoring applications, managing submissions, and analyzing your results.

**What's new in v2.2:** 
- **Action Center**: A daily वर्क queue that prioritizes your best moves.
- **Tailoring Studio**: professional resume customization with the "Andy Warthog" template.
- **Submission Review**: guided cockpit for finalizing and confirming manual applications.
- **Market Strategy**: role clustering and strategic resume-to-market gap analysis.
- **Learning Loop**: analytics that correlate scores with real-world interview outcomes.

---

## Contents

1. [What This App Does](#1-what-this-app-does)
2. [Installation and Launch](#2-installation-and-launch)
3. [First-Time Setup](#3-first-time-setup)
4. [Action Center — Your Daily Cockpit](#4-action-center--your-daily-cockpit)
5. [Finding Jobs — The Scraper](#5-finding-jobs--the-scraper)
6. [Job Matches](#6-job-matches)
7. [Tailoring Studio](#7-tailoring-studio)
8. [Submission Review — The Final Mile](#8-submission-review--the-final-mile)
9. [My Applications — Tracking Your Pipeline](#9-my-applications--tracking-your-pipeline)
10. [Market Strategy](#10-market-strategy)
11. [Learning Loop — Outcomes and Calibration](#11-learning-loop--outcomes-and-calibration)
12. [Pipeline View](#12-pipeline-view)
13. [Home Dashboard](#13-home-dashboard)
14. [Analytics](#14-analytics)
15. [Company Profiles & Intelligence](#15-company-profiles--intelligence)
16. [Contacts](#16-contacts)
17. [Interview Question Bank](#17-interview-question-bank)
18. [Email Templates](#18-email-templates)
19. [Journal](#19-journal)
20. [Training Tracker](#20-training-tracker)
21. [Weekly Report](#21-weekly-report)
22. [Search Settings Reference](#22-search-settings-reference)
23. [Target Companies Reference](#23-target-companies-reference)
24. [CLI Reference](#24-cli-reference)
25. [Tips and Workflow](#25-tips-and-workflow)
26. [Troubleshooting](#26-troubleshooting)

---

## 1. What This App Does

The dashboard is a self-hosted job search system that combines four things most job seekers manage in separate tools:

- **Automated discovery** — scrapes careers pages at your target companies daily or on demand, scores every role against your salary, location, and keyword preferences.
- **Tailoring & Generation** — professional-grade resume and cover letter customization grounded in your background and specific job requirements.
- **Submission Workflow** — a guided process for manual applications that ensures your materials are fresh and your tracking is accurate.
- **Strategic Calibration** — analytics that show which clusters are hiring and which resume gaps are holding you back.

Everything stays local. No data leaves your machine.

If the dashboard has been useful in your search, you can support ongoing development here:

[Buy Me a Coffee](https://www.buymeacoffee.com/ericdipietro)

---

## 2. Installation and Launch

### Windows (installer)

1. Download `JobSearchSetup.exe` from the Releases page.
2. Run the installer and follow the prompts.
3. Launch from the Desktop or Start Menu shortcut.

### Manual (any platform)

1. Install Python 3.11 and make sure it is on `PATH`.
2. Clone or download this repo.
3. Run the appropriate launcher:
   - Windows: `launch.bat`
   - macOS: `launch.command`
   - Linux: `bash launch.sh`

On first launch the launcher creates a virtual environment, installs dependencies, and copies the example preferences file if none exists. The dashboard opens automatically at `http://localhost:8501`.

---

## 3. First-Time Setup

When you first open the app, the Home page shows a setup checklist. Work through it top to bottom before running your first search.

![Setup checklist on first launch](docs/Screenshots/dashboard.png)

### Step 1 — Configure Search Settings

Open **Search Settings** in the left sidebar. 

**Compensation & Location**
- Set your salary floor and target salary.
- Enable contractor preferences if desired.

**Job Title Settings**
- Add positive title keywords with weights.
- Add negative disqualifiers to drop unwanted roles early.

**Job Description Keywords**
- Add keywords that boost or reduce your score based on your preferred tech stack.

**Scoring Settings**
- Adjust thresholds for **Apply Now**, **Review Today**, and **Watch**.

### Step 2 — Add Target Companies

Open **Target Companies** → **Add / Edit** tab. Set the adapter to `generic` if you are unsure; the healer will attempt to identify the ATS for you.

---

## 4. Action Center — Your Daily Cockpit

The **Action Center** is the default landing page and serves as your daily prioritized work queue. It synthesizes data from jobs, applications, contacts, and interviews to recommend your "Next Best Action."

### How It Works
The engine assigns an **Urgency** and **Impact** score to every candidate object:
- **Urgency (0-100)**: Time-sensitive factors (interview tomorrow, 10 days since application, newly discovered high-match job).
- **Impact (0-100)**: Value-based factors (fit score, company tier, relationship strength).

### Common Recommendations
- **Apply Now**: High-fit roles discovered in the last 7 days.
- **Refresh Export**: Materials were edited in Tailoring Studio after you already prepared the package.
- **Send Follow-up**: Applied roles with no activity in 7+ days.
- **Prepare for Interview**: Interviews scheduled in the next 48 hours.
- **Networking Outreach**: Contacts at target companies with no touchpoint in 21 days.

### Actions
- **Done**: Marks the action as complete.
- **Snooze (3d)**: Hides the recommendation for 3 days.
- **Dismiss**: Permanently removes this specific recommendation.
- **Quick Export**: Download the submission ZIP directly from the card if ready.

---

## 5. Finding Jobs — The Scraper

### How Scoring Works

Scoring is a two-layer pipeline:

**Layer 1 — V1 Hard Gates** (runs first, applied to every job)
- Titles matching `negative_disqualifiers` are dropped immediately.
- Non-local onsite/hybrid roles are hard-filtered.
- Work-type mismatches are filtered out.

**Layer 2 — V2 Primary Scoring**
1. **Title resolution** — job family and seniority band detection.
2. **Fast-Track check** — instant head-start score for strong title matches.
3. **Anchor & Baseline** — keyword matches in JD add to or subtract from the score.
4. **Tier Bonus** — companies in higher tiers receive a persistent score boost.

### Running Options

| Mode | When to use |
|---|---|
| Standard run | Daily use — fast, covers all active companies |
| `--deep-search` | Sites that require browser rendering (Playwright required) |
| `--aggregator-sources` | Adds Adzuna, USAJobs, Jooble, and The Muse |
| `--jobspy-sources` | Adds the experimental JobSpy lane for broader discovery |

---

## 6. Job Matches

The **Job Matches** page shows all scraped jobs that passed the scoring threshold.

### Search and Analysis
- **Semantic Search**: Sub-15ms search across 10K+ jobs with BM25 ranking.
- **AI Fit Analysis**: LLM breakdown of tech stack, visa sponsorship, and IC vs. Manager alignment.
- **Skills Gap**: Identification of missing keywords compared to your resume.

---

## 7. Tailoring Studio

The **Tailoring Studio** is a professional workspace for customizing your resume and application materials for a specific role.

### Key Features
- **Structured Form Editor**: Edit your resume content via a clean UI.
- **Andy Warthog Template**: A locked, designer-grade presentation layer with fixed typography and teal accents.
- **AI Drafting**: Generate tailored summaries, bullets, and cover letters grounded in the JD.
- **Normalization UX**: Real-time warnings if your content exceeds template limits.

---

## 8. Submission Review — The Final Mile

The **Submission Review** page is a guided cockpit for manual applications.

### Pre-flight Checklist
- **Package Freshness**: Warns if your exported ZIP is stale (older than 24h).
- **Readiness**: Blocks export if critical issues (like missing placeholders) are detected.

### Workflow
1. **Open Apply Portal**: Opens the company's careers page.
2. **Export Package (ZIP)**: Downloads your submission bundle (Styled PDF, ATS-Safe PDF, Cover Letter).
3. **Log Outcome**: Confirm submission or log friction (e.g., portal too long).

---

## 9. My Applications — Tracking Your Pipeline

The tracker is your core CRM. Stage workflow:
`Exploring → Considering → Prepared → Applied → Screening → Interviewing → Offer`

Move an application to the next stage using the status dropdown. 

---

## 10. Market Strategy

The **Market Strategy** dashboard helps you see high-level patterns.

- **Role Clustering**: Groups jobs into themes like *Fintech* or *Data*.
- **Resume-to-Market Gaps**: Compares market demand against your base resume.
- **Data Canonicalization**: Merges duplicate postings from multiple sources.

---

## 11. Learning Loop — Outcomes and Calibration

- **Score Correlation**: Checks if high-score matches actually lead to interviews.
- **Submission Friction**: Measures the funnel from Ready to Applied.
- **Keyword ROI**: Surfaces keywords that correlate with actual interview success.

---

## 12. Pipeline View

A quick tabular Kanban across active stages. Applications stuck in the same stage for more than 7 days are highlighted.

---

## 13. Home Dashboard

KPIs, pipeline summary, and activity trends.prominently surfaces overdue follow-ups and upcoming interviews.

---

## 14. Analytics

Funnel overview, time-in-stage, rejection pattern analysis, and resume keyword gaps.

---

## 15. Company Profiles & Intelligence

Persistent research notes and strategic playbooks for each employer.

---

## 16. Contacts

Networking CRM. Tracks name, relationship, last contact, and follow-up dates.

---

## 17. Interview Question Bank

Behavioral story library using the **STAR** method. Links relevant stories to specific roles in the Interview Prep tab.

---

## 18. Email Templates

Reusable messages with `{company}`, `{role}`, and `{my_name}` placeholders.

---

## 19. Journal

Private daily log with mood tags.

---

## 20. Training Tracker

Track certifications and skill-building programs.

---

## 21. Weekly Report

Generates structured activity logs for unemployment benefit paperwork or personal reviews.

---

## 22. Search Settings Reference

Detailed reference for compensation, location, title, and keyword weights.

---

## 23. Target Companies Reference

Reference for company fields and supported ATS adapters.

---

## 24. CLI Reference

Command line usage for running scrapers, healers, and the dashboard.

```bash
# Run the scraper (standard)
python -m jobsearch.cli run

# Run evaluation pass only (rescore without scraping)
python -m jobsearch.cli run --score-only

# Run with deep search (Playwright, slower)
python -m jobsearch.cli run --deep-search

# Include contractor sources
python -m jobsearch.cli run --contract-sources

# Include API aggregators
python -m jobsearch.cli run --aggregator-sources

# Include JobSpy experimental sources
python -m jobsearch.cli run --jobspy-sources

# Run ATS healing on all companies
python -m jobsearch.cli heal --all

# Launch the dashboard
python -m jobsearch.cli dashboard
```

---

## 25. Tips and Workflow

- **Daily**: Review Action Center and Submission Review.
- **Weekly**: Run scraper, Fix Job Listings, and review Learning Loop.

---

## 26. Troubleshooting

Common fixes for dashboard issues, zero-result runs, and blocked companies.

---

*All data lives in `results/jobsearch.db`, `config/job_search_preferences.yaml`, and `config/job_search_companies.yaml`. Back up these files regularly.*
