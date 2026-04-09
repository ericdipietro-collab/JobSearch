# QA Test Report - Job Search Automation Platform
Date: 2026-04-08

## Summary
Comprehensive QA testing following `docs/TEST_PLAN.md`.

## 1. Core Scraper Engine & Logic

| Test Case | Description | Result | Notes |
|-----------|-------------|--------|-------|
| **Incremental Scrape** | Run search twice for the same company. | **PASS** | Confirmed WorkdayAdapter skips detail fetch for jobs seen in last 3 days. |
| **Heuristic Scoring** | Score a job with strong title match vs weak JD match. | **PASS** | Verified max(fast_track, title_points) logic and keyword capping via `test_scoring_behavior.py`. |
| **Deduplication** | Inject the same job URL via manual escape hatch. | **PASS** | Verified API dedup logic via `test_deduplication_api.py`. |
| **Manual Review Lifecycle** | Suggest manual review in dashboard if status is broken. | **FAIL** | `_build_manual_review_items` in `app_main.py` only checks `manual_only` status, missing `broken` status from registry. |

## Bugs Found

### 1. Manual Review Queue Missing 'Broken' Status (Priority: Medium)
- **Description:** Companies marked as `status: broken` or `manual_only_suggested: true` in the company registry YAMLs are not automatically added to the "Manual Review" tab in the dashboard.
- **Impact:** Users may miss companies that the healer has identified as broken and needing manual intervention.
- **Root Cause:** `app_main._build_manual_review_items()` only checks for `manual_only` flag or status.
- **Reproduction:** Set a company to `status: broken` in `config/job_search_companies.yaml` and check the Manual Review tab.

### 2. Missing Dependencies in Environment (Priority: Low)
- **Description:** `APScheduler` and `feedparser` were missing from the environment despite being in `requirements.txt`.
- **Impact:** Dashboard fails to start if scheduler is enabled.
- **Note:** Fixed during testing by running `pip install -r requirements.txt`.

### 3. Logging Not Configured for Adapters (Priority: Low)
- **Description:** `logger.info()` and `logger.debug()` calls in adapters do not output anywhere because `logging` is not configured in `cli.py` or `app_main.py`.
- **Impact:** Harder to debug adapter issues without manually adding `print()` statements.
- **Root Cause:** Standard `logging` library is imported but `basicConfig` or equivalent is never called.
