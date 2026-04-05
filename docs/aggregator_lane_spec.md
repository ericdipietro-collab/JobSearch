# Aggregator Lane - Technical Specification
**Version:** 1.1  
**Status:** Draft for dev team review  
**Scope:** Third job-sourcing pipeline for 3rd-party job boards and aggregators

---

## 1. Overview and Constraints

This spec defines the aggregator lane as a first-class addition to the existing two-lane pipeline (employer ATS + contractor). It preserves full separation between lanes, enforces lower trust for aggregator-sourced jobs, and provides canonical deduplication back to employer ATS postings.

Aggregator sources are supplemental discovery only. The employer ATS lane remains the system of record for salaried/full-time roles, and the contractor lane remains the system of record for contract-specific sources.

### Non-negotiable constraints
- Aggregator jobs must never inflate the ATS lane's metrics
- A job that exists in both the ATS lane and the aggregator lane must produce one DB row, not two
- Aggregator jobs must not reach Apply Now unless they have a verified canonical employer URL and high score
- The healer/heal command must not attempt to process aggregator entries
- No robots.txt-violating scraping; only boards with public indexing or partner API access
- Dedup priority is: `employer_ats > contractor > aggregator`
- If a company or job already exists from a stronger lane, the aggregator copy is ignored or merged as enrichment only

### Out of scope for v1
- LinkedIn (ToS prohibits automated scraping)
- Authenticated/login-walled boards
- Multi-source canonical resolution (one board per adapter in v1)

---

## 2. Architecture Summary

```text
Lane 1: Employer ATS          job_search_companies.yaml              source_lane = "employer_ats"
Lane 2: Contractor            job_search_companies_contract.yaml     source_lane = "contractor"
Lane 3: Aggregator (NEW)      job_search_companies_aggregators.yaml  source_lane = "aggregator"
```

All three lanes write to the same `results/jobsearch.db` / `applications` table.  
Lane isolation is enforced by `source_lane`, `source`, and `canonical_job_url` fields.

Metric isolation must be enforced repo-wide, not just in the sidebar. Aggregator rows must be excluded from ATS-only counts on:
- Home/sidebar lead metrics
- Job Matches ATS-facing counts
- Analytics summaries intended to describe ATS performance
- Weekly/report surfaces unless they explicitly opt into aggregators

---

## 3. Database Migration

### File
`src/jobsearch/db/migrate_v2_to_v3.py`

### New columns on `applications`

```sql
ALTER TABLE applications ADD COLUMN source_lane TEXT NOT NULL DEFAULT 'employer_ats';
ALTER TABLE applications ADD COLUMN canonical_job_url TEXT;
```

### Backfill for existing rows

```sql
UPDATE applications
SET source_lane = 'employer_ats'
WHERE source_lane IS NULL OR source_lane = '';
```

### Migration script structure
Follow the pattern from `migrate_v1_to_v2.py`:
- Accept an optional DB path argument
- Create a backup before altering (`shutil.copy`)
- Run ALTER TABLE statements wrapped in a transaction
- Print row counts before and after to verify
- Be idempotent: check `PRAGMA table_info(applications)` before adding columns

### Schema update
Update:
- `src/jobsearch/db/schema.py`
- `src/jobsearch/ats_db.py`

Both fresh-install schemas must include `source_lane` and `canonical_job_url`.

### Model update
Add to `src/jobsearch/ats_db.py` `Opportunity` / `Job`:

```python
source_lane: str = "employer_ats"
canonical_job_url: str = ""
```

---

## 4. Configuration

### 4a. New config file
`config/job_search_companies_aggregators.yaml`

Schema is a simplified subset of the company config:
- no `heal_*` fields
- no ATS `adapter_key`
- no `manual_only_suggested`

```yaml
version: 1
companies:
  - name: Indeed API Pilot
    source_lane: aggregator
    adapter: indeed_connector
    active: false
    priority: medium
    tier: 4
    industry: job board | aggregator
    notes: API-first pilot lane for third-party discovery. Disabled until the connector-backed adapter is built.
    search_queries:
      - senior product manager fintech
      - solution architect financial services
      - technical program manager data
    location_filter: United States
    max_results_per_query: 50
    concurrency: 1
    cooldown_days: 1
```

Fields specific to aggregators:

| Field | Type | Description |
|---|---|---|
| `source_lane` | string | Always `"aggregator"` for this file |
| `adapter` | string | Maps to a class in `ADAPTER_MAP` |
| `search_queries` | list[str] | Terms submitted to the board source |
| `location_filter` | string | Source-side location filter |
| `max_results_per_query` | int | Hard cap per search term |
| `concurrency` | int | Overrides semaphore cap for this source |
| `cooldown_days` | int | Minimum days between re-scrapes |

### 4b. Settings
In `src/jobsearch/config/settings.py`, add:

```python
self.aggregator_companies_yaml = self.config_dir / "job_search_companies_aggregators.yaml"
```

### 4c. Scoring config
In `config/job_search_preferences.yaml`, add:

```yaml
source_trust:
  employer_ats:
    score_penalty: 0
    apply_now_eligible: true
    cap_bucket: null
  contractor:
    score_penalty: 0
    apply_now_eligible: true
    cap_bucket: null
  aggregator_with_canonical:
    score_penalty: 6
    apply_now_eligible: false
    cap_bucket: "REVIEW TODAY"
  aggregator_without_canonical:
    score_penalty: 12
    apply_now_eligible: false
    cap_bucket: "WATCH"
```

---

## 5. CLI Changes

### File: `src/jobsearch/cli.py`

Add a new `run` flag:

```python
@click.option("--aggregator-sources", is_flag=True, help="Include aggregator job board sources.")
```

Add the parameter to `run(...)` and merge the aggregator config after the contractor merge:

```python
if aggregator_sources:
    agg_path = settings.aggregator_companies_yaml
    if agg_path.exists() and agg_path != comp_path:
        with agg_path.open("r", encoding="utf-8") as handle:
            agg_data = (yaml.safe_load(handle) or {}).get("companies", [])
        comp_data = _merge_company_lists(comp_data, agg_data)
```

`_merge_company_lists()` is sufficient for the first slice because it deduplicates exact `(name, careers_url)` pairs. Later phases must add stronger canonical dedup in persistence.

### Healer command
Aggregator entries must never be processed by `heal`:

```python
companies = [c for c in companies if str(c.get("source_lane") or "") != "aggregator"]
```

---

## 6. Engine Changes

### File: `src/jobsearch/scraper/engine.py`

### 6a. ADAPTER_MAP
Add:

```python
"indeed_connector": IndeedConnectorAdapter,
```

### 6b. Semaphore registration
Add:

```python
"indeed_connector": int(os.getenv("JOBSEARCH_SCRAPE_INDEED_CONNECTOR_CONCURRENCY", "1")),
```

### 6c. Scoring data
Pass `source_lane` and `canonical_job_url` through scoring:

```python
"source_lane": str(company.get("source_lane") or "employer_ats"),
"canonical_job_url": getattr(job, "canonical_job_url", ""),
```

### 6d. Job object metadata
Each aggregator adapter must set:

```python
job.source_lane = "aggregator"
job.canonical_job_url = resolved_url_or_empty
```

---

## 7. Scoring Changes

### File: `src/jobsearch/scraper/scoring.py`

Load `source_trust` in `Scorer.__init__` and apply lane penalties before final score clamp.

Aggregator trust model:
- `aggregator_with_canonical`: smaller penalty, cap at `REVIEW TODAY`
- `aggregator_without_canonical`: larger penalty, cap at `WATCH`

Do not use source trust as a hard drop. Hard-drop logic remains title/location based.

---

## 8. Deduplication Changes

### File: `src/jobsearch/services/opportunity_service.py`

### 8a. Canonical URL dedup
Before the current broad fallback dedup, add a canonical URL lookup:

```python
if existing is None and job.canonical_job_url:
    existing = conn.execute(
        """
        SELECT ...
        FROM applications
        WHERE (job_url = ? OR canonical_job_url = ?)
          AND status != 'rejected'
        LIMIT 1
        """,
        (job.canonical_job_url, job.canonical_job_url),
    ).fetchone()
```

### 8b. Store `source_lane` and `canonical_job_url`
Insert both on create. Preserve existing `source_lane` on update when an aggregator matches an ATS row.

### 8c. Fallback dedup must preserve distinct openings
If canonical resolution fails, fallback dedup must include location context:

```python
(company, role_normalized, normalized_location, status='considering')
```

Do not dedup distinct locations at the same company/title family into one row.

---

## 9. Bucket Function Changes

### File: `src/jobsearch/app_main.py`

Read `source_trust` caps from preferences and enforce them after salary/location gates:
- aggregator with canonical URL: cap at `REVIEW TODAY`
- aggregator without canonical URL: cap at `WATCH`

This must apply consistently in all ATS-facing surfaces that currently bucket rows.

---

## 10. Aggregator Adapter Interface

All aggregator adapters extend `BaseAdapter`. They must:
1. Accept `company_config` including `search_queries`, `location_filter`, `max_results_per_query`
2. Return `List[Job]` with `source_lane = "aggregator"`
3. Attempt canonical URL resolution only for plausible results
4. Set `job.canonical_job_url` when a clean employer ATS URL is found
5. Set `job.source` to the board/provider name
6. Never raise on individual result failures - skip and continue

Plausible-result gating is required for performance. Do not attempt canonical resolution for every raw board result. Resolve only when a result passes a cheap pre-score/title-fit threshold.

Shared helper recommendation:
- `src/jobsearch/scraper/adapters/canonical.py`

---

## 11. First Adapter: Indeed Connector Pilot

### File
`src/jobsearch/scraper/adapters/indeed_connector.py`

### Rationale
Best first implementation path:
- API/connector-first is safer than HTML scraping
- The current product direction is to prefer an API-friendly pilot board
- Indeed has enough volume to validate lane isolation, dedup, and trust scoring quickly

### Search source
Use the connector/API-backed search path rather than HTML scraping.

### Result parsing
Each result should map:
- title
- company
- location
- source URL
- description/snippet when available
- salary text when available

### Canonical resolution
Attempt canonical employer resolution only for plausible results. If the source exposes an employer apply URL, resolve that to known ATS domains.

### Concurrency and rate limiting
- Concurrency: 1 by default
- Respect `cooldown_days` from config
- Canonical resolution must be gated; do not perform one network follow-up per result by default

---

## 12. Dashboard Changes

### File: `src/jobsearch/app_main.py`

### 12a. Run Job Search
Add a checkbox:

```python
r_aggregator = st.checkbox("Include Aggregator Sources", value=False)
```

Add the new config file to the companies list and append `--aggregator-sources` to the CLI command.

### 12b. Tracker / analytics filters
Add source lane filtering:
- All
- Employer ATS
- Contractor
- Aggregator

### 12c. Job cards
For aggregator rows:
- show a `Via <source>` badge
- show canonical URL when present

### 12d. Sidebar metrics
Keep aggregator counts separate. They must not be folded into primary ATS scraped lead metrics.

---

## 13. Rescore Changes

### File: `src/jobsearch/app_main.py`

`_rescore_saved_jobs()` must select and pass:
- `source_lane`
- `canonical_job_url`

so saved aggregator rows can be rescored under lane trust rules.

---

## 14. Build Order and Dependencies

### Phase 1 - Infrastructure
Goal: lane plumbing exists, migration is safe, DB is ready.

Deliverables:
1. Add `source_lane` and `canonical_job_url` to live/fresh schemas
2. Add fields to `Opportunity` / `Job`
3. Add `settings.aggregator_companies_yaml`
4. Add `config/job_search_companies_aggregators.yaml` with the Indeed pilot entry, `active: false`
5. Add CLI/UI toggles and merge plumbing
6. Add healer exclusion for aggregators

Acceptance criteria:
- Fresh DB schema includes both new columns
- Existing rows default/backfill to `employer_ats`
- `run --aggregator-sources` merges the third registry without breaking the other two lanes
- `heal` ignores aggregator entries

### Phase 2 - Scoring and bucketing
Goal: source trust penalties and bucket caps are live.

### Phase 3 - Deduplication
Goal: canonical URL dedup is live; aggregator rows enrich or suppress duplicates instead of creating them.

### Phase 4 - Dashboard surfacing
Goal: lane filters, badges, and metrics are visible and isolated.

### Phase 5 - Indeed connector adapter
Goal: first real aggregator source is live and validated.

Acceptance criteria:
- Aggregator run produces rows with `source_lane = "aggregator"`
- A meaningful subset resolve to canonical employer URLs
- ATS duplicate rows are not created
- Aggregator jobs do not surface as `Apply Now`

---

## 15. What the Healer Must Not Do

The current healer is ATS/company specific. It must not process aggregator entries because:
- aggregators do not have one stable careers URL to validate
- failure detection is per search query, not per company
- `manual_only` semantics do not apply the same way

If aggregator health tracking is needed later, it should be a separate command or service.

---

## 16. Risks and Mitigations

| Risk | Likelihood | Mitigation |
|---|---|---|
| Connector/API response changes break adapter | Medium | Fail gracefully, return empty list, log warning |
| Canonical URL resolution times out at scale | High | Resolve only plausible titles/results; hard short timeout |
| Duplicate insertion if canonical resolution fails | Medium | Fallback dedup must include normalized location |
| Aggregator jobs pollute ATS metrics | Medium | Enforce lane isolation across all ATS-facing metrics and views |
| Healer processes aggregator entries | Medium | Explicit guard in `heal` path |

---

## 17. Board Candidates Beyond v1

| Board | Signal Quality | Canonical URL Rate | Notes |
|---|---|---|---|
| **Indeed (connector/API)** | Medium | Low-Medium | Good first pilot for API-first lane plumbing and dedup validation |
| **eFinancialCareers** | High | High (~70%) | Good future HTML-source candidate after the pilot |
| **Hired.com** | High | High (~90%) | Good next-step API/partner candidate |
| **Builtin** | Low | Medium | Stronger on startup SWE than finance |
| **Wellfound** | Low | High | Startup-focused |
| **Otta** | Low | High | More UK-centric |
| **LinkedIn** | Very High | High | ToS prohibits automated scraping; excluded |

**Recommended next board after the pilot:** Hired.com.
