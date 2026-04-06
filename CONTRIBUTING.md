# Contributing to Job Search Automation Platform

Thank you for your interest in contributing! This guide will help you get started.

## Code of Conduct

Be respectful and constructive. We welcome contributors of all skill levels.

## Getting Started

### Prerequisites
- Python 3.11+ (earlier versions may require native builds and fail)
- Git
- A GitHub account

### Local Development Setup

1. **Fork and clone the repository:**
   ```bash
   git clone https://github.com/YOUR_USERNAME/jobsearch.git
   cd jobsearch
   ```

2. **Create a virtual environment:**
   ```bash
   python -m venv venv
   source venv/bin/activate  # On Windows: venv\Scripts\activate
   ```

3. **Install in development mode:**
   ```bash
   pip install -e .
   ```

4. **Install optional dependencies for deep search:**
   ```bash
   # On Windows
   .\deep_search\install_deep_search.bat
   # On Mac/Linux
   pip install playwright
   playwright install chromium
   ```

5. **Verify installation:**
   ```bash
   python -m jobsearch.cli --help
   streamlit run src/jobsearch/app_main.py
   ```

## Project Structure

```
jobsearch/
├── config/                    # Configuration files (YAML)
│   ├── job_search_preferences.yaml   # Scoring weights, preferences
│   ├── job_search_companies_*.yaml   # Target company lists
│   └── ...
├── src/jobsearch/
│   ├── app_main.py           # Main Streamlit dashboard
│   ├── cli.py                # CLI entry point
│   ├── scheduler.py          # Background task scheduling
│   ├── config/
│   │   └── settings.py       # Config loading & validation
│   ├── db/
│   │   ├── schema.py         # SQLite schema & migrations
│   │   ├── connection.py     # Database connection pooling
│   │   └── repository.py     # Data access layer
│   ├── scraper/
│   │   ├── engine.py         # Main scraper orchestration
│   │   ├── scoring.py        # Job scoring & filtering logic
│   │   ├── adapters/         # ATS-specific scrapers
│   │   │   ├── base.py       # Adapter base class
│   │   │   ├── greenhouse_adapter.py
│   │   │   ├── lever_adapter.py
│   │   │   └── ...
│   │   └── jobspy_validation.py  # JobSpy board validation
│   ├── services/
│   │   ├── enrichment_service.py   # LLM job analysis
│   │   ├── profile_service.py      # Resume parsing
│   │   ├── search_service.py       # FTS5 search
│   │   ├── export_service.py       # Excel/CSV export
│   │   ├── healer_service.py       # ATS discovery
│   │   └── llm_client.py           # Multi-LLM abstraction
│   └── views/                # Streamlit page components
│       ├── home_page.py
│       ├── tracker_page.py
│       ├── analytics_page.py
│       └── ...
├── results/                  # Runtime outputs (gitignored)
│   ├── jobsearch.db         # SQLite database
│   ├── job_search_v6.log    # Scraper logs
│   └── ats_heal.log         # Healer logs
├── installer/               # Packaging & distribution
│   ├── msix/                # Windows MSIX installer
│   └── portable/            # Portable ZIP package
├── CLAUDE.md                # Technical specification
├── CHANGELOG.md             # Release notes
├── README.md                # This file
└── pyproject.toml           # Package metadata & dependencies
```

## How to Contribute

### Reporting Bugs

1. **Check existing issues** — Search for duplicates before opening a new one
2. **Describe the problem** — What happened? What did you expect?
3. **Include details** — Python version, OS, steps to reproduce
4. **Attach logs** — Scraper log (`results/job_search_v6.log`), error traceback

Example:
```
**Describe the bug**
The scraper crashes when scraping Workday jobs for Company X.

**Steps to reproduce**
1. Add Company X with adapter=workday to job_search_companies.yaml
2. Run `python -m jobsearch.cli run`
3. Observe error in results/job_search_v6.log

**Error message**
TimeoutError: Workday page took too long to load (60s timeout exceeded)

**Environment**
- Python 3.11.5
- Windows 11
- jobsearch v2.1.0
```

### Adding a New ATS Adapter

1. **Create a new adapter file:**
   ```bash
   cp src/jobsearch/scraper/adapters/base.py src/jobsearch/scraper/adapters/myats_adapter.py
   ```

2. **Implement the adapter:**
   ```python
   from jobsearch.scraper.adapters.base import BaseAdapter
   from jobsearch.scraper.models import Job

   class MyAtsAdapter(BaseAdapter):
       """Scraper for MyAts platform."""

       def scrape(self) -> list[Job]:
           """Scrape jobs from MyAts careers page."""
           # 1. Fetch the careers page
           # 2. Parse job listings
           # 3. Extract Job objects with all required fields
           # 4. Return list of Job objects
           pass

       def scrape_detail(self, job_url: str) -> dict:
           """Fetch full job description if needed."""
           pass
   ```

3. **Register the adapter** in `src/jobsearch/scraper/engine.py`:
   ```python
   from jobsearch.scraper.adapters.myats_adapter import MyAtsAdapter

   ADAPTERS = {
       "myats": MyAtsAdapter,
       # ... existing adapters
   }
   ```

4. **Test it:**
   ```bash
   python -m jobsearch.cli run --test-companies
   ```

5. **Submit a PR** with:
   - Your adapter implementation
   - Test results (JSON output showing parsed jobs)
   - A sample company config for the YAML file

### Improving Scoring Logic

The scoring engine is in `src/jobsearch/scraper/scoring.py`. Key areas:

- **`_score_title()`** — Title matching and fast-track logic
- **`_score_keywords()`** — Positive/negative keyword matching
- **`_score_adjustments()`** — Salary, location, and tier bonuses/penalties
- **`_check_experience_fit()`** — Experience requirement validation

To test changes:
1. Update scoring logic
2. Re-run scraper: `python -m jobsearch.cli run`
3. Compare output in `results/jobsearch.db` and `results/job_search_v6.log`
4. Re-score existing jobs without re-scraping: go to Job Matches → Re-Score All

### Enhancing the Dashboard

The Streamlit app is in `src/jobsearch/app_main.py` with individual page components in `src/jobsearch/views/`.

To add a new page:
1. Create `src/jobsearch/views/my_page.py`:
   ```python
   import streamlit as st

   def render_my_page():
       st.title("My New Page")
       st.write("Page content here")
   ```

2. Import and register in `app_main.py`:
   ```python
   from jobsearch.views.my_page import render_my_page
   
   view_map = {
       # ... existing pages
       "My New Page": render_my_page,
   }
   ```

3. Test: `streamlit run src/jobsearch/app_main.py`

## Code Style

We follow [PEP 8](https://www.python.org/dev/peps/pep-0008/). A few conventions:

- **Type hints** — Use them where helpful, especially for public functions
- **Docstrings** — Include for public methods and modules
- **Logging** — Use `logger.info()`, `logger.warning()`, `logger.error()` instead of `print()`
- **Comments** — Explain *why*, not *what* (the code shows what)

Example:
```python
def upsert_job(conn: sqlite3.Connection, job: Job) -> tuple[bool, int]:
    """
    Insert or update a Job, preserving user-owned fields.
    
    If job content hasn't changed (same hash), skips update entirely.
    Returns (inserted, app_id).
    """
    # Compute content hash for change detection
    new_hash = compute_content_hash(job.role_title_raw, job.url, job.description_excerpt)
    
    existing = conn.execute(
        "SELECT id, content_hash FROM applications WHERE scraper_key = ?",
        (job.id,)
    ).fetchone()
    
    if existing:
        existing_id, existing_hash = existing
        # If hash unchanged, skip update to preserve user annotations
        if existing_hash == new_hash:
            return False, existing_id
        # ... update logic
    
    # ... insert logic
```

## Testing

No formal test suite yet, but verify manually:

```bash
# Test CLI
python -m jobsearch.cli run --test-companies

# Test dashboard
streamlit run src/jobsearch/app_main.py

# Test healer
python -m jobsearch.cli heal --force

# Check database
sqlite3 results/jobsearch.db "SELECT COUNT(*) FROM applications"
```

## Submitting Changes

1. **Create a feature branch:**
   ```bash
   git checkout -b feature/add-new-adapter
   ```

2. **Make your changes** — Keep commits focused and descriptive

3. **Test thoroughly** — Verify scraper output, dashboard, logs

4. **Push to your fork:**
   ```bash
   git push origin feature/add-new-adapter
   ```

5. **Open a PR** with:
   - Clear description of what changed and why
   - Links to related issues
   - Test results / screenshots if applicable

6. **Respond to feedback** — We may ask for clarifications or changes

## Release Process

Maintainers only: To cut a release:

1. **Update version** in `src/jobsearch/__init__.py` and `pyproject.toml`
2. **Update CHANGELOG.md** with new features/fixes
3. **Create a git tag:**
   ```bash
   git tag -a v2.1.0 -m "Release v2.1.0"
   git push origin v2.1.0
   ```
4. **GitHub Actions** automatically builds and publishes Windows installers

## Questions?

- **Documentation** — See [USER_GUIDE.md](USER_GUIDE.md) and [CLAUDE.md](CLAUDE.md)
- **Issues** — Open a GitHub issue with questions or bugs
- **Discussions** — Use GitHub Discussions for general questions

---

**Thank you for contributing!** 🙏
