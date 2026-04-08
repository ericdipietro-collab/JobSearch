# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [2.1.1] - 2026-04-08

### Added
- **Incremental Scraping Logic** — All ATS adapters now support "Last Run" awareness. Skips re-fetching full job descriptions for URLs already in the database and updated within the last 3 days, significantly reducing run time and token usage.
- **In-place URL Editor** — Manual Review cards now include a "Fix URL" text input and "Save URL to Registry" button to permanently repair broken career links directly from the dashboard.
- **Expanded Workday Site Discovery** — Healer now probes for common custom site patterns (e.g., `[Company]Careers`, `[Company]Investments`) when a base Workday URL is provided.

### Fixed
- **Database Stability & Recovery** — Resolved "malformed disk image" errors in AppData by implementing automatic FTS5 index reconstruction and database VACUUM.
- **UI Layout & Readability** — Fixed top content being obscured by the Streamlit header on several pages (Journal, Contacts, Training, etc.) by adding consistent title headers.
- **Scoring Precision** — Fixed title score stacking; logic now uses `max(base_score, title_points)` to prevent score inflation on strong title matches.
- **Light Mode Accessibility** — Fixed text contrast and readability for AI analysis and score breakdown cards in Streamlit Light Mode.
- **Weekly Report Enhancements** — Resolved Session State warnings and fixed metric label truncation on standard screen widths.
- **Data Coercion** — Fixed `ValueError` in company editor when handling string-formatted floats (e.g., "0.0") for integer fields.

### Security & Dependencies
- **Locked Dependencies** — Explicitly pinned `urllib3<2.0.0` and `pydantic==2.9.2` to resolve `RequestsDependencyWarning` and ensure environment stability across systems.

## [2.1.0] - 2026-04-06

### Added

#### Quick Wins (Phase 1)
- **Excel Export with Color-Coded Scoring** — Multi-sheet `.xlsx` reports with fit-band color coding (green ≥85, yellow ≥70, salmon <70). Includes summary sheet with band counts and top companies.
- **LLM Cost Tracking & Budget Management** — Token usage monitoring against configurable daily budget. UI displays total tokens used, API call count, and budget percentage with 80% warning threshold. Prevents budget overruns by halting enrichment when limit reached.
- **Fit Markdown Pre-filtering** — Crawl4AI integration now prioritizes `fit_markdown` output to reduce token consumption and improve extraction quality on complex career pages.

#### Search & Discovery (Phase 2)
- **Full-Text Search (FTS5)** — SQLite FTS5 with BM25 ranking enables sub-15ms semantic search across 10K+ jobs. Integrated search bar in Job Matches page with ranked results and fuzzy matching.
- **Experience Tolerance Gap** — Configurable experience requirement threshold. Hard-drops jobs requiring significantly more experience; applies soft penalty for jobs within tolerance band. Extracted via regex patterns from job title/description.
- **Crawl4AI-Powered ATS Discovery** — Healer enhancement for complex SPA careers pages. Uses Crawl4AI + LLM extraction to detect ATS platforms (Greenhouse, Lever, Ashby, Workday, Rippling, SmartRecruiters). Gracefully falls back to Playwright for JavaScript-rendered content.

#### AI & Scoring Intelligence (Phase 3)
- **Per-Job Missing-Skills Delta Report** — Enrichment service extracts skills gap between user profile and job requirements. Displayed as "Skills Gap" in job cards for actionable interview prep.
- **Configurable Scoring Weights UI** — 11+ interactive sliders in Search Settings for real-time weight tuning:
  - Title match max points
  - Positive/negative keyword caps
  - Salary bonuses (target met, minimum met) and penalties
  - Contract role adjustments
  - Location bonuses (US remote, hybrid)
  - Experience gap penalties
- **Multi-LLM Provider Abstraction** — `LLMClient` interface supporting Gemini and OpenAI with auto-detection and fallback. Enables switching providers without code changes via `GOOGLE_API_KEY` or `OPENAI_API_KEY` environment variables.

#### Reliability & Automation (Phase 4)
- **Content-Hash Sync** — MD5-based change detection preserves user annotations on re-scrape. Only updates scraper-owned fields (score, description, salary); leaves user-owned fields (notes, status, date_applied) untouched.
- **Scheduled Background Refresh** — APScheduler-based auto-refresh runs scraper on configurable intervals (1-24 hours) without manual intervention. Displays last-run timestamp and next-run countdown in UI.
- **High-Score Job Alerts** — Toast notifications in dashboard when new jobs above apply_now threshold are discovered. Alert count tracked in settings table and cleared on Job Matches page visit.

### Improved
- **Token Usage Metrics Display** — Run Job Search page now shows LLM token usage, call count, and budget consumption after pipeline completes.
- **Skill Extraction** — Added `ProfileService.extract_skills()` method to extract technical competencies from resume for gap analysis and preference generation.
- **Experience Requirement Detection** — Comprehensive regex patterns for extracting "X+ years experience" from job titles and descriptions.

### Technical

- **Schema Enhancement** — Added `llm_cost_log` table for token tracking, `content_hash` column in applications for change detection, `jd_needs_review` flag for material JD changes.
- **Scheduler Integration** — `src/jobsearch/scheduler.py` module manages background task scheduling with graceful shutdown.
- **Search Service** — `src/jobsearch/services/search_service.py` provides FTS5-backed job search with BM25 ranking.
- **Export Service** — `src/jobsearch/services/export_service.py` builds multi-sheet Excel reports with styling.
- **LLM Client** — Unified `src/jobsearch/services/llm_client.py` abstracts Gemini and OpenAI providers.

### Dependencies
- Added `APScheduler>=3.10` for background scheduling
- Optional: `openai` package when `OPENAI_API_KEY` is configured
- `openpyxl` explicitly listed (was indirect dependency via pandas)

## [2.0.0] - 2026-03-20

### Added
- Initial unified SQLite database schema with applications, stage_history, events, job_observations tables
- 18 ATS adapters (Greenhouse, Lever, Ashby, Workday, Rippling, SmartRecruiters, Dice, Indeed Connector, Jooble, Adzuna, TheMuse, Generic, USAJOBS, MotionRecruitment, Google Careers, JobSpy, Contractor Jobs, Aggregator sources)
- Modular scoring engine with title matching, keyword weighting, salary adjustments, and location-based bonuses
- Streamlit dashboard with 15+ pages (Job Matches, My Applications, Analytics, Journal, Contacts, Company Profiles, Training, Question Bank, Weekly Report, Templates, Pipeline, Search Settings, Target Companies)
- Job description enrichment via Google Gemini (visa sponsorship, tech stack, IC vs Manager detection)
- ATS Healer for discovering company job boards and detecting ATS platforms
- Deep search capability for complex Cloudflare/anti-bot protected sites via Playwright
- Resume parsing and preference generation via LLM
- Comprehensive scoring metrics and rejected job tracking
- Windows MSIX installer and portable executable packaging

---

## Version History

- **v2.0.0** (2026-03-20) — Unified database, 18 adapters, rich scoring, Streamlit dashboard
- **v2.1.0** (2026-04-06) — Comprehensive automation, search, and AI enhancements; Phase 1–4 implementation
