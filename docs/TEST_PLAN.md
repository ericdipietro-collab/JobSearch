# Comprehensive Test Plan: Job Search Automation Platform (v2.1.1)

This document outlines the testing strategy to ensure the application is production-ready, stable, and bug-free.

## 1. Core Scraper Engine & Logic
| Test Case | Description | Expected Result |
| :--- | :--- | :--- |
| **Incremental Scrape** | Run search twice for the same company. | Second run should skip fetching full descriptions for existing URLs. |
| **Heuristic Scoring** | Score a job with strong title match vs weak JD match. | Score should accurately reflect title points without stacking (max logic). |
| **Deduplication** | Inject the same job URL via manual escape hatch. | Database should contain only one record; return existing ID. |
| **Cooldown Logic** | Run scraper on a company marked 'low_signal'. | Scraper should skip the company if within the cooldown period (e.g., 14 days). |

## 2. Data Source Adapters
| Test Case | Description | Expected Result |
| :--- | :--- | :--- |
| **Workday Discovery** | Run repair on incomplete Workday URLs. | Healer should follow redirects and use "Cheat Code" search to find deep paths. |
| **Aggregator Health** | Run tests for CareerOneStop, Remotive, WWR, etc. | 200 OK responses; correct mapping to `Job` model (id, role_title_raw). |
| **LinkedIn Fallback** | Trigger a 400 error on LinkedIn. | Scraper should switch to 'respectful' mode or skip to prevent IP block. |

## 3. System Integration
| Test Case | Description | Expected Result |
| :--- | :--- | :--- |
| **FTS5 Search** | Search for "Product" in Job Matches. | Sub-15ms results; ranked by relevance (BM25). |
| **AI Enrichment** | Enrich a job description with a configured LLM. | Returns tech stack, visa status, and skills gap analysis. |
| **Gmail Sync** | Classify a marketing email vs an interview invite. | Marketing (e.g. "Sale", "Booking") should be ignored; invites identified. |
| **Manual Escape Hatch** | Click bookmarklet on a Breezy/Greenhouse site. | Company name extracted; job scored and injected into dashboard. |

## 4. UI/UX Stability
| Test Case | Description | Expected Result |
| :--- | :--- | :--- |
| **Header Spacing** | Navigate to Journal, Contacts, and Training. | Content should start below the Streamlit header (fixed padding/title). |
| **Session State** | Change date ranges on the Weekly Report. | No "Session State API" warnings; metrics update correctly. |
| **Dynamic Registry** | Add `job_search_companies_test.yaml` to config. | Radio buttons should automatically show "Test Company List". |

## 5. Distribution & Environment
| Test Case | Description | Expected Result |
| :--- | :--- | :--- |
| **Launcher Setup** | Run `launch.bat` in a fresh directory. | Auto-installs dependencies; seeds runtime AppData correctly. |
| **Installer Build** | Run `build_installer.bat`. | Downloads all wheels (including source fallbacks); builds valid .exe. |
