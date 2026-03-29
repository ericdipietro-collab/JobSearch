# Job Search Automation Platform

A personal job search tool that automatically finds open roles at companies you care about, scores them against your preferences, and helps you track everything from first contact through offer — including the weekly activity log required for unemployment benefits.

Built for anyone actively job searching after a layoff. No subscriptions, no data sold to recruiters — everything runs locally on your own computer.

---

## What it does

| Feature | What you get |
|---|---|
| **Automated job scraping** | Checks career pages at companies you choose, pulls open roles, filters by your salary floor and location |
| **Smart scoring** | Ranks jobs by how well they match your background using keyword weights you define |
| **Job Matches dashboard** | Three buckets: Apply Now, Review Today, Watch — with one-click status updates |
| **My Applications** | Track every application and network conversation with a full timeline, interview log, and contact list |
| **Opportunities** | Log networking conversations (like a call with a former manager) that aren't formal applications yet |
| **Job Fairs** | Track job fair attendance as a reportable activity |
| **Training tracker** | Plan and track courses/certifications (AWS, Snowflake, AI, etc.) with status and progress |
| **Weekly Activity Report** | Shows all job search activities for any date range, tells you if you've hit your state's weekly minimum, and generates copy-paste text for your unemployment certification |
| **Target Companies manager** | Add, edit, and bulk-update the list of companies being scraped |
| **Search Settings** | Edit your salary floor, location, and keyword weights directly in the UI — no YAML editing required |

---

## Quick Start

### Prerequisites

- Python 3.9 or higher ([download here](https://www.python.org/downloads/))
- Git ([download here](https://git-scm.com/downloads))

### 1. Clone the repository

```bash
git clone https://github.com/ericdipietro-collab/JobSearch.git
cd JobSearch
```

### 2. Install dependencies

```bash
pip install -r requirements.txt
```

> **Having trouble?** Try `pip3 install -r requirements.txt` or `python -m pip install -r requirements.txt`

### 3. Set up your configuration

```bash
cp config/job_search_preferences.example.yaml config/job_search_preferences.yaml
```

Open `config/job_search_preferences.yaml` in any text editor and update these three things:

```yaml
search:
  location_preferences:
    local_hybrid:
      primary_zip: '80504'        # ← your zip code
      markers: [Firestone, Denver, Boulder]  # ← cities near you

  compensation:
    min_salary_usd: 120000        # ← your minimum acceptable salary
```

> **Not sure what keywords to use?** See [docs/AI_SETUP_PROMPTS.md](docs/AI_SETUP_PROMPTS.md) for prompts you can paste into ChatGPT or Claude to generate your entire config from your resume.

### 4. Open the dashboard

```bash
streamlit run app.py
```

Your browser will open automatically at `http://localhost:8501`.

From the dashboard, go to **Target Companies** to add companies you want to track, then **Run Job Search** to fetch your first batch of results.

---

## Dashboard pages

### Job Matches
Your scraper results organized into three buckets:
- **⚡ Apply Now** — high-scoring roles with a one-click Apply & Track button that opens the job and logs it in your tracker simultaneously
- **📋 Review Today** — worth a closer look
- **👁 Watch** — lower match, keep an eye on
- **✅ Applied / ❌ Rejected** — your history

### My Applications
Full CRM for tracking your job search pipeline. Add any application (or network conversation, or job fair) and log the complete timeline:
- Status progression from considering → applied → screening → interviewing → offer
- Interview scheduling with round tracking and outcome recording
- Contact log (recruiter, hiring manager, referral, network contact)
- Follow-up reminders — get notified when a follow-up is overdue
- Resume version and cover letter notes per application
- Filter by status (Applied, Interviewing, etc.) or type (Applications / Opportunities / Job Fairs)

**Opportunities** — for networking conversations that might lead to a role. A call with a former manager about a potential position is an Opportunity. Log the conversation, track the follow-up, and convert to a formal Application if it materializes.

**Job Fairs** — log job fair attendance with the companies you spoke to. Shows up as a separate activity in your weekly report.

### Training
Plan and track courses and certifications:
- Set status: Planned → In Progress → Completed (or Paused)
- Track provider (AWS, Snowflake, Coursera, etc.), category, and target completion date
- Hours/week estimate for planning
- Certificate URL once you earn it
- Active and completed training flows into your Weekly Activity Report automatically

### Weekly Report
Generates your job search activity log for any date range.

- Defaults to the current week (Mon–Sun)
- Shows a **compliance indicator**: "✅ 3/3 required activities" or "⚠️ 2/3 — need 1 more"
- Covers: applications submitted, job fairs, networking calls, recruiter contacts, phone screens, interviews, follow-ups, and training
- Bottom of the page: copyable plain-text report formatted for unemployment certification forms

> The 3-activity minimum is Colorado's requirement. Check your state's rules — the number may differ where you are.

### Target Companies
Add and manage the companies being scraped. Key fields:
- **Adapter**: the type of careers system they use (Greenhouse, Lever, Ashby, Workday, or custom)
- **Tier**: 1 = top priority, 2 = standard, 3 = opportunistic
- **Bulk URL Fix tab**: quickly paste in corrected URLs for companies with broken links

Run the ATS Healer from this page to automatically find and fix broken career page URLs.

### Search Settings
Edit your preferences without touching YAML files: salary floors, location policy, keyword weights, scoring thresholds.

### Run Job Search
Runs the scraper and refreshes results. A full run typically takes 5–15 minutes depending on how many companies you have.

---

## Configuring your search

The tool uses two config files in the `config/` folder. Both are gitignored so your personal salary and location data are never uploaded to GitHub.

### `job_search_preferences.yaml` — what to look for

Controls salary floors, location preferences, and the keyword scoring engine that ranks jobs.

**The fastest way to configure this:** use the AI prompts in [docs/AI_SETUP_PROMPTS.md](docs/AI_SETUP_PROMPTS.md) to generate a customized version from your resume.

Key sections:
- `search.compensation.min_salary_usd` — your minimum acceptable salary
- `search.location_policy` — `remote_only`, `remote_or_hybrid`, or `any`
- `titles.positive_keywords` — job titles you're targeting
- `titles.negative_disqualifiers` — titles to always skip
- `keywords.body_positive` — JD keywords that signal a good fit (with weights)
- `keywords.body_negative` — JD keywords that signal a bad fit (with penalty scores)

### `job_search_companies.yaml` — where to look

**A starter list of ~485 companies is already included.** It's weighted toward FinTech, FinServ, and adjacent tech — wealth management, payments, data platforms, insurtech, banking software, and enterprise SaaS. If that overlaps with your background, you may be able to run the scraper on day one with no changes.

Customize it from the **Target Companies** page in the dashboard — add, edit, or remove companies without touching the YAML file directly. The ATS Healer can automatically fix any broken career page URLs.

Each entry in the file looks like:

```yaml
- name: Acme Corp
  domain: acmecorp.com
  adapter: greenhouse           # greenhouse | lever | ashby | workday | custom_manual
  adapter_key: acmecorp         # the slug used by their ATS
  careers_url: https://boards.greenhouse.io/acmecorp
  tier: 1                       # 1=top priority, 2=standard, 3=opportunistic
  status: active
```

**Need companies for a different field?** Use Prompt 2 in [docs/AI_SETUP_PROMPTS.md](docs/AI_SETUP_PROMPTS.md) to generate a targeted list for any industry.

---

## For co-workers sharing this tool

Each person runs their own local copy. Your salary, location, and application data never leave your computer.

**Setup checklist:**
1. Clone the repo and install dependencies (see Quick Start above)
2. Copy `config/job_search_preferences.example.yaml` → `config/job_search_preferences.yaml`
3. Fill in your zip code, salary floor, and target job titles — or use the AI prompts in `docs/AI_SETUP_PROMPTS.md` to generate the whole file from your resume in minutes
4. **The company list is already populated** (~485 companies, mostly FinTech/FinServ/tech). Add, remove, or adjust priorities from the **Target Companies** page in the dashboard
5. Run **Run Job Search** to get your first results
6. Go to **My Applications** and log any applications you've already submitted

If you're in a different field (healthcare, marketing, ops, etc.), use Prompt 2 in [docs/AI_SETUP_PROMPTS.md](docs/AI_SETUP_PROMPTS.md) to generate a company list for your industry — and remove the FinTech companies that aren't relevant to you.

**What gets shared (in the repo):**
- The tool code
- `config/job_search_preferences.example.yaml` — a template showing the structure
- `config/job_search_companies.yaml` — a starter company list (you'll customize this)

**What stays private (gitignored, never uploaded):**
- Your actual preferences file (`config/job_search_preferences.yaml`)
- Your results folder (all scraped jobs, your applications database)
- Any local state files

---

## Troubleshooting

**"No results" after running the scraper**
- Check that `config/job_search_preferences.yaml` exists (not just the `.example.yaml`)
- Your salary floor might be filtering everything out — try lowering `min_salary_usd` temporarily
- Go to **Target Companies** and make sure you have active companies with valid URLs

**Dashboard won't load**
- Make sure you ran `pip install -r requirements.txt`
- Try `streamlit run app.py` from the `JobSearch/` folder

**Companies showing as "broken"**
- Run the ATS Healer from the **Target Companies** page — it automatically finds and fixes most broken URLs

**YAML syntax errors**
- YAML is whitespace-sensitive. Use 2-space indentation, not tabs. Paste your YAML into [yamllint.com](https://www.yamllint.com/) to check it.

---

## Privacy

The following are gitignored and never committed to GitHub:

- `config/job_search_preferences.yaml` — your salary targets and location
- `results/` — all scraped jobs, Excel files, and your SQLite applications database
- Any `*.db` files and runtime state files

---

## Requirements

- Python 3.9+
- See `requirements.txt` for all package versions

Key dependencies: `requests`, `PyYAML`, `beautifulsoup4`, `pandas`, `openpyxl`, `streamlit`

---

## Roadmap / known gaps

- **Interview prep notes** — dedicated space for company research and question prep per application
- **Offer comparison** — side-by-side of salary, equity, benefits across multiple offers
- **Email templates** — thank-you notes, follow-up templates tied to application events
- **Benefit week tracker** — "Week 14 of 26" benefit countdown
- **LinkedIn import** — import existing application history from LinkedIn
