"""
job_search_v6.py

Automated job search pipeline.

v6 goals:
- domain gating based directly on APPLY / SKIP rules
- cleaner evaluation flow: title gate -> domain gate -> scoring -> action bucket
- salary extraction
- normalized titles
- better exports for daily workflow
- YAML-driven search preferences and scoring overrides

Dependencies:
    pip install beautifulsoup4 pandas requests openpyxl
"""

from __future__ import annotations

import argparse
import hashlib
import json
import logging
import re
import shutil
import subprocess
import threading
import time
import warnings
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, asdict
from datetime import datetime, timezone
from pathlib import Path
from functools import lru_cache
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import urljoin, urlparse

import builtins
import os
import pandas as pd
import requests
from bs4 import BeautifulSoup, FeatureNotFound, XMLParsedAsHTMLWarning
import yaml

# --- Optional deep search add-on (playwright-based) ---
try:
    from deep_search import playwright_adapter as _deep_search_mod
    _DEEP_SEARCH_INSTALLED = True
except ImportError:
    _deep_search_mod = None  # type: ignore[assignment]
    _DEEP_SEARCH_INSTALLED = False


def _early_cli_value(flag_names):
    argv = sys.argv[1:]
    for idx, token in enumerate(argv):
        for flag in flag_names:
            if token == flag and idx + 1 < len(argv):
                return argv[idx + 1]
            if token.startswith(flag + "="):
                return token.split("=", 1)[1]
    return None


def _early_cli_flag(flag_name: str) -> bool:
    argv = sys.argv[1:]
    return any(token == flag_name or token.startswith(flag_name + "=") for token in argv)


CLI_COMPANIES_PATH_OVERRIDE = _early_cli_value(["--companies"])
CLI_PREFERENCES_PATH_OVERRIDE = _early_cli_value(["--prefs", "--preferences"])
if not CLI_COMPANIES_PATH_OVERRIDE and _early_cli_flag("--test-companies"):
    CLI_COMPANIES_PATH_OVERRIDE = str((Path(__file__).resolve().parent / "config" / "job_search_companies_test.yaml"))


# ============================================================
# CONFIG
# ============================================================

REQUEST_TIMEOUT = 25
MAX_DESC_CHARS = 7000
MAX_JOB_AGE_DAYS = 21
TOP_N_PER_COMPANY = 8
MAX_SERVICE_HEAVY_PER_COMPANY = 2
MIN_SCORE_TO_KEEP = 35
MAX_DETAIL_LINKS = 120
MAX_EXTERNAL_BOARD_DETAIL_LINKS = 80
MAX_EXTERNAL_BOARD_SEARCH_URLS = 4
MAX_EXTERNAL_BOARD_PREFILTER_TEXT_CHARS = 5000

REQUIRE_STRONG_TITLE_FOR_APPLY_NOW = True
APPLY_NOW_MIN_ROLE_ALIGNMENT = 6.0
APPLY_NOW_DIRECT_TITLE_MARKERS = [
    "architect", "product manager", "program manager", "business analyst",
    "systems analyst", "solution architect", "product architect",
    "integration architect", "platform", "api", "data architect",
]
ACTION_BUCKET_RANKS = {
    "APPLY NOW": 0,
    "REVIEW TODAY": 1,
    "WATCH": 2,
    "MANUAL REVIEW": 3,
    "IGNORE": 4,
}
ACTION_BUCKET_RULES = [
    {"label": "MANUAL REVIEW", "when": {"manual_review": True}},
    {"label": "APPLY NOW", "when": {"min_score": 88, "eligible": True, "strong_title": True, "known_salary_for_apply": True}},
    {"label": "REVIEW TODAY", "when": {"min_score": 88, "eligible": True, "strong_title": True, "known_salary_for_apply": False}},
    {"label": "REVIEW TODAY", "when": {"min_score": 74, "eligible": True}},
    {"label": "REVIEW TODAY", "when": {"tier_in": [1], "min_score": 67, "eligible": True}},
    {"label": "WATCH", "when": {"min_score": 55, "eligible": True}},
    {"label": "WATCH", "when": {"min_score": 40}},
    {"label": "IGNORE", "when": {}},
]

BASE_DIR = Path(__file__).resolve().parent
RESULTS_DIR = BASE_DIR / "results"
RESULTS_DIR.mkdir(exist_ok=True)
SCRAPE_WORKERS = 6  # parallel company threads; increase carefully (ATS rate limits)

OUTPUT_XLSX = str(RESULTS_DIR / "job_search_v6_results.xlsx")
OUTPUT_CSV = str(RESULTS_DIR / "job_search_v6_results.csv")
OUTPUT_REJECTED_CSV = str(RESULTS_DIR / "job_search_v6_rejected.csv")
OUTPUT_REJECTION_DETAIL_CSV = str(RESULTS_DIR / "job_search_v6_rejection_details.csv")
OUTPUT_MANUAL_CSV = str(RESULTS_DIR / "job_search_v6_manual_targets.csv")
OUTPUT_ADAPTER_HEALTH_CSV = str(RESULTS_DIR / "job_search_v6_adapter_health.csv")
OUTPUT_REGISTRY_HEALTH_CSV = str(RESULTS_DIR / "job_search_registry_health_v6.csv")
OUTPUT_ACTIVE_CONFIG_JSON = str(RESULTS_DIR / "job_search_v6_active_config.json")
DEFAULT_CHECKPOINT_DIR = str(RESULTS_DIR / "job_search_v6_checkpoints")
HISTORY_JSON = str(RESULTS_DIR / "job_search_history_v6.json")
RUN_LOG = str(RESULTS_DIR / "job_search_v6.log")


# ---------------------------------------------------------------------------
# Deep search mode helpers
# ---------------------------------------------------------------------------

def _deep_search_enabled() -> bool:
    """Return True if the Playwright add-on is installed and deep search is enabled."""
    if not _DEEP_SEARCH_INSTALLED:
        return False
    # Set by run_job_search_v6.py (--deep-search flag) or app.py subprocess env
    if getattr(builtins, "DEEP_SEARCH_ENABLED", False):
        return True
    if os.environ.get("JOB_SEARCH_DEEP_MODE", "").lower() in ("1", "true", "yes"):
        return True
    return False


def _convert_raw_jobs(company, raw_jobs, rejected_jobs=None):
    """Convert deep_search raw dicts → Job objects via make_job()."""
    jobs = []
    for raw in raw_jobs:
        job = make_job(
            company=company,
            title=raw.get("title") or "",
            location=raw.get("location") or "",
            url=raw.get("url") or company.careers_url or "",
            source=raw.get("source") or "Deep Search",
            description=raw.get("description") or "",
            posted_dt=parse_date(raw.get("posted_at") or ""),
            rejected_jobs=rejected_jobs,
        )
        if job:
            jobs.append(job)
    return jobs


HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/135.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,application/json;q=0.8,*/*;q=0.7",
    "Accept-Language": "en-US,en;q=0.9",
    "Cache-Control": "no-cache",
    "Pragma": "no-cache",
    "Upgrade-Insecure-Requests": "1",
}
SESSION = requests.Session()
SESSION.headers.update(HEADERS)
CURL_BIN = shutil.which("curl.exe") or shutil.which("curl")
HOST_COOLDOWNS: Dict[str, float] = {}
URL_COOLDOWNS: Dict[str, float] = {}
URL_FAILURE_COUNTS: Dict[str, int] = {}
URL_FAMILY_FAILURE_COUNTS: Dict[str, int] = {}
URL_FAMILY_COOLDOWNS: Dict[str, float] = {}
WORKDAY_DETAIL_FAMILY_COOLDOWNS: Dict[str, float] = {}
_RUN_CTX = threading.local()  # thread-safe run context (replaces shared CURRENT_RUN_CONTEXT)
REGISTRY_HEALTH: Dict[Tuple[str, str, str], Dict[str, Any]] = {}
RUNTIME_COMPANY_STATUS: Dict[str, str] = {}

PERMANENT_URL_FAILURE_TYPES = {"not_found", "dns_failure", "ssl_hostname_mismatch", "ssl_error", "invalid_url", "blacklisted_url"}
TEMP_BLOCK_FAILURE_TYPES = {"forbidden", "rate_limited"}

BLOCKED_URL_HOST_MARKERS = [
    "aetna.com",
    "transparency-in-coverage",
]

BLOCKED_URL_PATH_MARKERS = [
    "transparency-in-coverage",
    "machine-readable",
    "machine_readable",
    "/mrf",
    "sitemap.xml",
    "/sitemap",
    "/feed",
    "/rss",
    "/atom",
    "robots.txt",
]

LOCATION_POLICY = "remote_only"   # remote_only | remote_or_hybrid | in_office | any
ENFORCE_MIN_SALARY = True
MIN_SALARY_USD = 100000
ALLOW_MISSING_SALARY = True
SALARY_FLOOR_BASIS = "midpoint"      # low_end | midpoint | high_end
ENFORCE_JOB_AGE = True
US_ONLY = True
REQUIRE_REMOTE = LOCATION_POLICY == "remote_only"

PRIMARY_SEARCH_ZIP = "00000"
LOCAL_HYBRID_RADIUS_MILES = 30
ALLOW_LOCAL_HYBRID_HIGH_COMP = True
LOCAL_HYBRID_MIN_SALARY_USD = 100000
PREFERRED_REMOTE_MIN_SALARY_USD = 100000
MISSING_SALARY_SCORE_PENALTY = 6
SALARY_ABOVE_TARGET_BONUS = 6
LOCAL_HYBRID_SCORE_BONUS = 4
REMOTE_US_SCORE_BONUS = 14
FLEXIBLE_INDUSTRY_IF_SKILLS_ALIGN = True

logging.basicConfig(
    filename=RUN_LOG,
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)

# Some career/discovery endpoints return XML-like payloads (sitemaps, feeds, XML APIs).
# Treat those with an XML parser when possible and suppress the noisy warning when HTML parsing is intentional.
warnings.filterwarnings("ignore", category=XMLParsedAsHTMLWarning)


def _coerce_markup_to_text(text: Any) -> str:
    if text is None:
        return ""
    if isinstance(text, str):
        return text
    if isinstance(text, (bytes, bytearray)):
        data = bytes(text)
        for enc in ("utf-8", "utf-8-sig", "cp1252", "latin-1"):
            try:
                return data.decode(enc)
            except UnicodeDecodeError:
                continue
        return data.decode("utf-8", errors="replace")
    return str(text)


def make_soup(text: Any) -> BeautifulSoup:
    payload = _coerce_markup_to_text(text).strip()
    probe = payload[:500].lower()
    parser = "html.parser"
    if probe.startswith("<?xml") or "<rss" in probe or "<feed" in probe or "<urlset" in probe or "<sitemapindex" in probe:
        parser = "xml"
    try:
        return BeautifulSoup(payload, parser)
    except FeatureNotFound:
        return BeautifulSoup(payload, "html.parser")



def _early_url_is_blacklisted(url: Optional[str]) -> bool:
    raw = (url or "").strip()
    if not raw:
        return True
    try:
        parsed = urlparse(raw if "://" in raw else "https://" + raw.lstrip("/"))
    except Exception:
        return True
    host = (parsed.netloc or "").lower()
    path = (parsed.path or "").lower()
    full = f"{host}{path}"
    if any(marker in host for marker in BLOCKED_URL_HOST_MARKERS):
        return True
    if any(marker in full for marker in BLOCKED_URL_PATH_MARKERS):
        return True
    return False


def _early_sanitize_url(url: Optional[str]) -> str:
    raw = (url or "").strip()
    if not raw:
        return ""
    if raw.startswith("//"):
        raw = "https:" + raw
    elif not re.match(r"^[a-zA-Z][a-zA-Z0-9+.-]*://", raw):
        raw = "https://" + raw.lstrip("/")
    try:
        parsed = urlparse(raw)
    except Exception:
        return ""
    if parsed.scheme not in {"http", "https"} or not parsed.netloc:
        return ""
    cleaned = parsed._replace(netloc=parsed.netloc.lower(), fragment="")
    normalized = cleaned.geturl()
    if _early_url_is_blacklisted(normalized):
        return ""
    return normalized



# Prioritize: 1. CLI Override, 2. Organized /config folder, 3. Legacy root files
candidates = [
    Path(CLI_COMPANIES_PATH_OVERRIDE).expanduser().resolve() if CLI_COMPANIES_PATH_OVERRIDE else None,
    BASE_DIR / "config" / "job_search_companies.yaml",
    BASE_DIR / "job_search_companies.yaml",
    BASE_DIR / "config" / "companies.yaml",
    BASE_DIR / "companies.yaml",
]

# Identify the first path that exists; default to the /config standard if none are found
COMPANY_REGISTRY_PATH = next(
    (p for p in candidates if p and p.exists()),
    BASE_DIR / "config" / "job_search_companies.yaml"
)

# Allow the launcher to inject overrides via builtins; fall back to local candidates when run directly.
COMPANY_REGISTRY_FILE_CANDIDATES: List[Path] = getattr(
    builtins, "COMPANY_REGISTRY_FILE_CANDIDATES",
    [p for p in candidates if p is not None],
)


def _normalize_company_record(raw: Dict[str, Any]) -> Dict[str, Any]:
    rec = dict(raw)
    rec.setdefault("tier", 4)
    rec.setdefault("priority", "medium")
    rec.setdefault("adapter", "custom_manual")
    rec.setdefault("careers_url", "")
    rec.setdefault("domain", "")
    rec.setdefault("industry", [])
    rec.setdefault("notes", "")
    rec.setdefault("active", True)
    rec.setdefault("sub_industry", (rec.get("industry") or [""])[0] if rec.get("industry") else "")
    rec["careers_url"] = _early_sanitize_url(rec.get("careers_url"))
    discovery_urls = []
    for item in (rec.get("discovery_urls") or []):
        normalized = _early_sanitize_url(item)
        if normalized:
            discovery_urls.append(normalized)
    if discovery_urls:
        rec["discovery_urls"] = list(dict.fromkeys(discovery_urls))
    else:
        rec.pop("discovery_urls", None)
    if not rec.get("domain") and rec.get("careers_url"):
        rec["domain"] = urlparse(rec["careers_url"]).netloc.lower()
    rec.setdefault("render_required", False)
    rec.setdefault("jobs_api_url", None)
    rec.setdefault("iframe_src", None)
    return rec


def _load_company_registry_from_yaml() -> Tuple[Optional[Path], Optional[List[Dict[str, Any]]]]:
    for path in COMPANY_REGISTRY_FILE_CANDIDATES:
        if not path.exists():
            continue
        with path.open("r", encoding="utf-8") as f:
            payload = yaml.safe_load(f) or {}
        raw_companies = payload.get("companies", [])
        if not isinstance(raw_companies, list):
            raise ValueError(f"Invalid company registry format in {path}")
        companies = [_normalize_company_record(item) for item in raw_companies if isinstance(item, dict)]
        if companies:
            return path, companies
    return None, None


# Apply the same logic for preferences (include CLI override at head of list)
pref_candidates = [
    Path(CLI_PREFERENCES_PATH_OVERRIDE).expanduser().resolve() if CLI_PREFERENCES_PATH_OVERRIDE else None,
    BASE_DIR / "config" / "job_search_preferences.yaml",
    BASE_DIR / "job_search_preferences.yaml",
    BASE_DIR / "config" / "preferences.yaml",
]

YAML_PREFERENCES = next(
    (p for p in pref_candidates if p and p.exists()),
    BASE_DIR / "config" / "job_search_preferences.yaml"
)

# Allow the launcher to inject overrides via builtins; fall back to local candidates when run directly.
PREFERENCES_FILE_CANDIDATES: List[Path] = getattr(
    builtins, "PREFERENCES_FILE_CANDIDATES",
    [p for p in pref_candidates if p is not None],
)


def _load_preferences_from_yaml() -> Tuple[Optional[Path], Dict[str, Any]]:
    for path in PREFERENCES_FILE_CANDIDATES:
        if not path.exists():
            continue
        with path.open("r", encoding="utf-8") as f:
            payload = yaml.safe_load(f) or {}
        if isinstance(payload, dict) and payload:
            return path, payload
    return None, {}


def _pref_get(payload: Dict[str, Any], keys: List[str], default: Any) -> Any:
    current: Any = payload
    for key in keys:
        if not isinstance(current, dict) or key not in current:
            return default
        current = current[key]
    return current


def _pref_phrase_list(raw: Any, default: List[str]) -> List[str]:
    if not isinstance(raw, list):
        return list(default)
    out: List[str] = []
    for item in raw:
        value = str(item or "").strip().lower()
        if value:
            out.append(value)
    return list(dict.fromkeys(out)) or list(default)


def _pref_weight_pairs(raw: Any, default: List[Tuple[str, int]]) -> List[Tuple[str, int]]:
    pairs: List[Tuple[str, int]] = []
    if isinstance(raw, dict):
        for key, value in raw.items():
            phrase = str(key or "").strip().lower()
            if not phrase:
                continue
            try:
                weight = int(float(value))
            except Exception:
                continue
            pairs.append((phrase, weight))
    elif isinstance(raw, list):
        for item in raw:
            if not isinstance(item, dict):
                continue
            phrase = str(item.get("keyword") or item.get("phrase") or "").strip().lower()
            if not phrase:
                continue
            try:
                weight = int(float(item.get("weight") or item.get("points") or 0))
            except Exception:
                continue
            pairs.append((phrase, weight))
    return pairs or list(default)


def _pref_action_bucket_ranks(raw: Any, default: Dict[str, int]) -> Dict[str, int]:
    out: Dict[str, int] = {}
    if isinstance(raw, dict):
        for key, value in raw.items():
            label = str(key or "").strip()
            if not label:
                continue
            try:
                out[label] = int(float(value))
            except Exception:
                continue
    return out or dict(default)


def _pref_action_bucket_rules(raw: Any, default: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    if not isinstance(raw, list):
        return [json.loads(json.dumps(item)) for item in default]
    out: List[Dict[str, Any]] = []
    for item in raw:
        if not isinstance(item, dict):
            continue
        label = str(item.get("label") or item.get("bucket") or "").strip()
        if not label:
            continue
        when = item.get("when")
        if not isinstance(when, dict):
            when = {}
        out.append({"label": label, "when": json.loads(json.dumps(when))})
    return out or [json.loads(json.dumps(item)) for item in default]


def _flatten_preferences(payload: Any, prefix: str = "") -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    if isinstance(payload, dict):
        for key, value in payload.items():
            next_prefix = f"{prefix}.{key}" if prefix else str(key)
            rows.extend(_flatten_preferences(value, next_prefix))
        return rows
    if isinstance(payload, list):
        if all(not isinstance(item, (dict, list)) for item in payload):
            rows.append({"path": prefix, "value": ", ".join(str(item) for item in payload)})
            return rows
        for idx, value in enumerate(payload):
            next_prefix = f"{prefix}[{idx}]"
            rows.extend(_flatten_preferences(value, next_prefix))
        return rows
    rows.append({"path": prefix, "value": payload})
    return rows


# ============================================================
# COMPANY REGISTRY
# Full target list from v4
# adapter options:
#   greenhouse | lever | ashby | workday | custom_site
#   workday_manual | custom_manual
#   custom_blackrock | custom_schwab | custom_spglobal
# ============================================================

DEFAULT_COMPANY_REGISTRY: List[Dict[str, Any]] = [

    # ── TIER 1 ────────────────────────────────────────────────
    {
        "name": "Addepar", "tier": 1, "priority": "high",
        "adapter": "greenhouse", "adapter_key": "addepar1",
        "domain": "addepar.com",
        "industry": ["wealth management", "portfolio analytics", "financial data", "investment"],
        "notes": "Wealth mgmt data + portfolio analytics — Aladdin-adjacent, direct domain match",
    },
    {
        "name": "BlackRock", "tier": 1, "priority": "high",
        "adapter": "custom_blackrock",
        "careers_url": "https://careers.blackrock.com/search-jobs",
        "domain": "blackrock.com",
        "industry": ["aladdin", "asset management", "capital markets", "investment", "financial data"],
        "notes": "Aladdin SME is rare — this is the top credential match",
    },
    {
        "name": "Envestnet", "tier": 1, "priority": "high",
        "adapter": "custom_manual",
        "careers_url": "https://careers.envestnet.com/search/jobs",
        "domain": "envestnet.com",
        "industry": ["wealth management", "advisor technology", "financial data", "integration", "platform"],
        "notes": "Advisor tech ecosystem — major platform integrations across wealth management",
    },
    {
        "name": "Affirm", "tier": 1, "priority": "high",
        "adapter": "greenhouse", "adapter_key": "affirm",
        "domain": "affirm.com",
        "industry": ["financial reporting", "capital platform", "reconciliation", "fintech", "payments"],
        "notes": "3 active applications — financial reporting + capital platform",
    },
    {
        "name": "Plaid", "tier": 1, "priority": "high",
        "adapter": "lever", "adapter_key": "plaid",
        "domain": "plaid.com",
        "industry": ["financial data", "api", "integration", "fintech", "brokerage"],
        "notes": "Financial data connectivity + API-first — integration depth maps directly",
    },
    {
        "name": "Bill.com", "tier": 1, "priority": "high",
        "adapter": "greenhouse", "adapter_key": "billcom",
        "domain": "bill.com",
        "industry": ["b2b payments", "financial operations", "ap ar automation", "reconciliation"],
        "notes": "B2B financial ops + AP/AR automation — payments + data integration fit",
    },
    {
        "name": "Gusto", "tier": 1, "priority": "high",
        "adapter": "greenhouse", "adapter_key": "gusto",
        "domain": "gusto.com",
        "industry": ["payroll", "b2b payments", "financial data", "reconciliation", "fintech"],
        "notes": "Payroll infrastructure + financial data + reconciliation — already applied",
    },
    {
        "name": "Marqeta", "tier": 1, "priority": "high",
        "adapter": "greenhouse", "adapter_key": "marqeta",
        "domain": "marqeta.com",
        "industry": ["card issuing", "b2b payments", "api platform", "integration", "fintech"],
        "notes": "Card issuing infrastructure + B2B API platform",
    },
    {
        "name": "iCapital", "tier": 1, "priority": "high",
        "adapter": "custom_site",
        "careers_url": "https://icapital.com/careers/",
        "domain": "icapital.com",
        "industry": ["alternative investments", "wealth management", "alts distribution", "fintech"],
        "notes": "Closest analog to SUBSCRIBE — alts distribution for wealth managers",
    },
    {
        "name": "Clearwater Analytics", "tier": 1, "priority": "high",
        "adapter": "custom_site",
        "careers_url": "https://clearwateranalytics.com/company/careers/",
        "domain": "clearwateranalytics.com",
        "industry": ["investment accounting", "reconciliation", "financial reporting", "investment data"],
        "notes": "Investment accounting + reconciliation + reporting — direct Allstate overlap",
    },

    # ── TIER 2 ────────────────────────────────────────────────
    {
        "name": "Fidelity Investments", "tier": 2, "priority": "medium",
        "adapter": "custom_site",
        "careers_url": "https://jobs.fidelity.com/job-search-results",
        "domain": "fidelity.com",
        "industry": ["brokerage", "custody", "wealth management", "financial services", "nfs"],
        "notes": "NFS platform is primary proof point — custodian PM roles exist",
    },
    {
        "name": "Charles Schwab", "tier": 2, "priority": "medium",
        "adapter": "custom_schwab",
        "careers_url": "https://www.schwabjobs.com/search-jobs",
        "domain": "schwabjobs.com",
        "industry": ["brokerage", "custody", "trading", "wealth management", "financial services"],
        "notes": "Custodian PM — NFS/DTCC background directly relevant post-TDA merger",
    },
    {
        "name": "Morningstar", "tier": 2, "priority": "medium",
        "adapter": "custom_manual",
        "careers_url": "https://careers.morningstar.com/us/en/",
        "domain": "morningstar.com",
        "industry": ["financial data", "investment", "analytics", "advisor tools", "capital markets"],
        "notes": "Data + analytics + advisor tools — strong domain match",
    },
    {
        "name": "FactSet", "tier": 2, "priority": "medium",
        "adapter": "custom_manual",
        "careers_url": "https://www.factset.com/careers",
        "domain": "factset.com",
        "industry": ["capital markets", "financial data", "investment", "portfolio analytics"],
        "notes": "Capital markets data + workflow PM — Aladdin/IDW background applies",
    },
    {
        "name": "S&P Global", "tier": 2, "priority": "medium",
        "adapter": "custom_spglobal",
        "careers_url": "https://careers.spglobal.com/",
        "domain": "spglobal.com",
        "industry": ["financial data", "capital markets", "market intelligence", "investment"],
        "notes": "Market data + financial technology PM — broad FS domain fit",
    },
    {
        "name": "Confluent", "tier": 2, "priority": "medium",
        "adapter": "custom_site",
        "careers_url": "https://careers.confluent.io/jobs/united_states",
        "domain": "confluent.io",
        "industry": ["data streaming", "financial services", "integration", "platform", "messaging"],
        "notes": "JMS/messaging pivot at InvestCloud IS Kafka territory — apply sooner than other T2",
    },
    {
        "name": "Carta", "tier": 2, "priority": "medium",
        "adapter": "greenhouse", "adapter_key": "carta",
        "domain": "carta.com",
        "industry": ["equity management", "fund administration", "financial data", "integration"],
        "notes": "Equity management + fund admin + data/integration PM fit",
    },
    {
        "name": "CAIS", "tier": 2, "priority": "medium",
        "adapter": "greenhouse", "adapter_key": "cais",
        "domain": "caisgroup.com",
        "industry": ["alternative investments", "alts distribution", "wealth management", "platform"],
        "notes": "Alts distribution platform — same buyer/user profile as SUBSCRIBE + iCapital",
    },
    {
        "name": "Melio", "tier": 2, "priority": "medium",
        "adapter": "greenhouse", "adapter_key": "melio",
        "domain": "meliopayments.com",
        "industry": ["b2b payments", "integration", "financial infrastructure", "fintech"],
        "notes": "B2B payments infrastructure — smaller than Bill.com but strong integration fit",
    },
    {
        "name": "Checkout.com", "tier": 2, "priority": "medium",
        "adapter": "custom_site",
        "careers_url": "https://careers.smartrecruiters.com/Checkoutcom1",
        "domain": "checkout.com",
        "industry": ["b2b payments", "api platform", "financial data", "integration", "fintech"],
        "notes": "Enterprise payments + API-first platform + financial data flows",
    },

    # ── TIER 3  (target Q3-Q4 2026 after training) ────────────
    {
        "name": "Snowflake", "tier": 3, "priority": "low",
        "adapter": "custom_manual",
        "careers_url": "https://careers.snowflake.com/us/en/",
        "domain": "snowflake.com",
        "industry": ["data platform", "cloud", "financial services", "data warehouse"],
        "notes": "Target once AWS + Snowflake training complete — financial services PM angle",
    },
    {
        "name": "Informatica", "tier": 3, "priority": "low",
        "adapter": "workday_manual",
        "careers_url": "https://careers.informatica.com/us/en",
        "domain": "informatica.com",
        "industry": ["data integration", "enterprise data", "governance", "etl", "platform"],
        "notes": "Data integration + governance — architecture background maps directly",
    },
    {
        "name": "MuleSoft", "tier": 3, "priority": "low",
        "adapter": "workday_manual",
        "careers_url": "https://careers.salesforce.com/en/jobs/",
        "domain": "salesforce.com",
        "industry": ["integration platform", "api", "enterprise platform", "data integration"],
        "notes": "Integration platform PM — integration architecture work is direct evidence",
    },

    # ── WATCHLIST  (monitor, not primary targets yet) ──────────
    {
        "name": "Koyfin", "tier": 2, "priority": "medium",
        "adapter": "custom_manual",
        "careers_url": "https://www.koyfin.com/careers/",
        "domain": "koyfin.com",
        "industry": ["advisor technology", "portfolio analytics", "wealth management", "financial data"],
        "notes": "Strong domain fit. Eyes open on options/quant depth gap.",
    },
    {
        "name": "FloQast", "tier": 2, "priority": "medium",
        "adapter": "lever", "adapter_key": "floqast",
        "careers_url": "https://floqast.com/company/careers/",
        "domain": "floqast.com",
        "industry": ["accounting close", "financial operations", "reconciliation", "financial reporting"],
        "notes": "Accounting close automation — perfect domain fit, remote-first",
    },
    {
        "name": "Arcesium", "tier": 2, "priority": "medium",
        "adapter": "custom_site",
        "careers_url": "https://www.arcesium.com/careers/",
        "domain": "arcesium.com",
        "industry": ["hedge fund", "data operations", "investment", "capital markets", "financial data"],
        "notes": "Hedge fund / data operations — strong capital markets overlap",
    },
    {
        "name": "Ridgeline", "tier": 2, "priority": "medium",
        "adapter": "greenhouse", "adapter_key": "ridgeline",
        "domain": "ridgeline.com",
        "industry": ["investment management", "portfolio management", "financial data", "platform"],
        "notes": "Investment management platform — monitor for right roles",
    },
]

COMPANY_REGISTRY_PATH, _YAML_COMPANY_REGISTRY = _load_company_registry_from_yaml()
COMPANY_REGISTRY: List[Dict[str, Any]] = _YAML_COMPANY_REGISTRY or [
    _normalize_company_record(item) for item in DEFAULT_COMPANY_REGISTRY
]


# ============================================================
# APPLY / SKIP DOMAIN TAXONOMY
# Directly based on your rules
# ============================================================

APPLY_DOMAINS: Dict[str, List[str]] = {
    "Investment Management": [
        "aladdin", "eagle pace", "charles river", "charles river oms",
        "ss&c maximis", "maximis", "wall street office",
        "investment data warehouse", "idw", "portfolio management system",
        "ibor", "abor", "investment accounting", "investment management", "portfolio management",
        "managed accounts", "separately managed accounts",
    ],
    "Capital Markets": [
        "capital markets", "fixed income", "equities", "derivatives",
        "trading systems", "risk analytics", "portfolio analytics",
        "front office", "middle office", "back office",
    ],
    "Financial Data / Reporting": [
        "gl integration", "subledger", "erp integration",
        "financial reporting", "reconciliation", "month-end close",
        "sox", "sox controls", "journal entry", "journal entries",
        "fund accounting", "general ledger", "gl mapping",
    ],
    "Brokerage / Custody": [
        "nfs", "dtcc", "fis", "custodian", "brokerage",
        "self-directed trading", "tax lots", "settlements",
        "settlement", "clearing", "custody", "custody platforms",
    ],
    "Alternative Investments": [
        "private equity", "private credit", "hedge fund", "hedge funds",
        "real assets", "subscription documents", "fund administration",
        "investor onboarding", "aml", "kyc", "private markets",
    ],
    "Wealth Management Tech": [
        "wealth management platform", "advisor technology", "wealthtech",
        "ria platform", "financial advisor tools", "portfolio reporting",
        "advisor platform", "wealth management", "managed accounts", "separately managed accounts",
    ],
    "Payments Infrastructure (B2B)": [
        "ach", "wire transfers", "wire transfer", "settlement",
        "disbursements", "money movement", "payment rails",
        "financial infrastructure", "b2b payments", "institutional payments",
        "bill pay", "payment operations", "payables", "card issuing",
    ],
    "API Integration (Financial)": [
        "api integration", "data integration", "financial api",
        "custodian api", "market data api", "integration platform",
        "financial data pipeline", "financial data pipelines",
        "partner platform", "partner api",
    ],
}

SKIP_DOMAINS: Dict[str, List[str]] = {
    "Consumer Lending / EWA": [
        "mortgage", "home equity", "earned wage access",
        "consumer loans", "consumer lending", "bnpl",
        "buy now pay later", "personal finance", "credit scoring",
        "underwriting for consumers", "underwriting",
    ],
    "DevOps / IT Operations": [
        "incident management", "sre", "site reliability",
        "reliability engineering", "observability", "monitoring",
        "pagerduty", "datadog", "splunk", "devops tooling",
    ],
    "AI / ML Developer Tools": [
        "vector database", "llm infrastructure", "embedding models",
        "ai agents", "ml platform", "developer tools for ai",
        "pinecone", "weaviate", "hugging face",
    ],
    "Consumer B2C Products": [
        "consumer app", "mobile banking", "retail banking",
        "b2c fintech", "consumer experience", "user acquisition",
        "growth hacking", "nps for consumers",
    ],
    "Cloud Infrastructure / DevOps": [
        "kubernetes", "aws infrastructure", "cloud cost management",
        "ci/cd", "platform engineering", "cloud native",
        "saas infrastructure tooling",
    ],
    "Marketing / Growth Tech": [
        "marketing automation", "crm product", "lifecycle campaigns",
        "email marketing platform", "a/b testing consumer funnels",
        "seo", "sem",
    ],
    "Developer Tools (non-financial)": [
        "developer tools for software engineers", "api platforms for developers",
        "auth platforms", "cli tools", "sdk", "coding platforms",
    ],
    "SMB / HR / Scheduling": [
        "smb software", "hr platform", "payroll for small business",
        "employee scheduling", "restaurant software", "retail ops",
    ],
    "Blockchain / DeFi": [
        "blockchain", "defi", "crypto", "smart contracts",
        "tokenization", "nft", "web3", "dao",
    ],
}


# ============================================================
# TITLE RULES
# ============================================================

TITLE_MUST_INCLUDE_ONE = [
    "product manager",
    "product management",
    "program manager",
    "architect",
    "integration",
    "integrations",
    "solution",
    "platform",
    "business systems analyst",
    "business analyst",
    "bsa",
    "technical product",
    "technical program",
    "product lead",
    "product owner",
    "solutions consultant",
    "integrations manager",
    "integration manager",
    "application manager",
    "business transformation",
    "investment solutions",
    " senior pm",
    " staff pm",
    " principal pm",
    " lead pm",
]

TITLE_HARD_EXCLUDE = [
    "software engineer",
    "engineering manager",
    "data engineer",
    "machine learning engineer",
    "sales engineer",
    "devops engineer",
    "site reliability",
    "security engineer",
    "network engineer",
    "infrastructure engineer",
    "recruiter",
    "talent",
    "human resources",
    " hr ",
    "attorney",
    "counsel",
    "legal",
    "account executive",
    "account manager",
    "customer success",
    "customer support",
    "marketing manager",
    "content ",
    "designer",
    "ux researcher",
    "data scientist",
    "research scientist",
    "finance analyst",
    "financial analyst",
    "controller",
    "accountant",
]

TITLE_NEGATIVE_DISQUALIFIERS = TITLE_HARD_EXCLUDE[:]
TITLE_FAST_TRACK_BASE_SCORE = 50
TITLE_FAST_TRACK_MIN_WEIGHT = 8
SALARY_NEGOTIATION_BUFFER_PCT = 0.05

PM_ALLOWED_MODIFIERS = [
    "staff", "principal", "senior", "lead",
    "integration", "integrations", "platform", "api",
    "data", "infrastructure", "technical", "core",
    "enterprise", "capital", "financial", "payments",
    "investment", "trading", "brokerage", "wealth",
    "reporting", "fund", "alternative", "analytics",
    "ledger", "reconciliation", "solution", "payments",
    "money movement", "billing", "capital", "advisor",
]

BSA_ALLOWED_MODIFIERS = [
    "senior", "lead", "principal",
    "systems", "technical", "investment", "financial",
    "capital markets", "data",
]

SOFT_TITLE_ROLE_MARKERS = [
    "lead", "manager", "director", "head", "owner", "consultant",
    "specialist", "analyst", "strategist", "advisor", "architect",
]

SOFT_TITLE_DOMAIN_MARKERS = [
    "financial systems", "portfolio", "investment", "wealth", "advisor",
    "custody", "brokerage", "reconciliation", "accounting", "ledger",
    "reporting", "payments", "bill pay", "api", "integration",
    "platform", "trading", "alternatives", "fund", "managed accounts",
    "separately managed accounts", "data", "analytics",
]

SOFT_TITLE_RESCUE_ALLOWED_FAILURES = {
    "no_target_signal",
    "generic_pm_no_modifier",
    "generic_ba",
}

TITLE_RESCUE_ADJACENT_TITLE_MARKERS = [
    "strategic insights",
    "risk",
    "implementation",
    "technical programs",
]

TITLE_RESCUE_ANALYST_VARIANT_MARKERS = [
    "strategic insights",
    "strategic insights analyst",
    "credit risk",
    "merchant risk",
    "risk analyst",
    "implementation analyst",
    "implementation specialist",
    "technical analyst",
    "product analyst",
    "compliance analyst",
]

TITLE_RESCUE_ADJACENT_TITLE_AUTO_RESCUE_PATTERNS = [
    "compliance analyst",
    "risk analyst",
    "implementation analyst",
    "implementation specialist",
    "technical program",
]

TITLE_RESCUE_STRONG_BODY_DOMAIN_MARKERS = [
    "fintech",
    "financial services",
    "payments",
    "risk",
    "fraud",
    "money movement",
    "merchant",
    "ledger",
    "banking",
    "platform",
    "api",
    "integrations",
    "compliance",
    "credit",
    "underwriting",
    "reconciliation",
    "capital markets",
    "investment",
    "brokerage",
    "custody",
    "wealth",
    "advisor",
    "merchant risk",
    "credit risk",
]

TITLE_RESCUE_STRONG_BODY_MIN_POSITIVE_POINTS = 4
TITLE_RESCUE_STRONG_BODY_MIN_POSITIVE_TERMS = 1
TITLE_RESCUE_ADJACENT_TITLE_BONUS = 8
TITLE_RESCUE_ADJACENT_TITLE_STRONG_DOMAIN_BONUS = 6
TITLE_RESCUE_ADJACENT_TITLE_MIN_SCORE_TO_KEEP = 26

PREFERRED_ROLE_TITLE_PHRASES = [
    "technical solution architect",
    "solution architect",
    "integration architect",
    "product architect",
    "platform architect",
    "enterprise architect",
    "technical product manager",
    "senior product manager",
    "staff product manager",
    "principal product manager",
    "lead product manager",
    "product manager",
    "technical program manager",
    "program manager",
    "business systems analyst",
    "senior business analyst",
    "systems analyst",
]

PREFERRED_ROLE_MARKERS = [
    "architect",
    "product manager",
    "technical product",
    "platform",
    "integration",
    "integrations",
    "api",
    "business systems analyst",
    "systems analyst",
    "back office",
    "financial systems",
    "reconciliation",
    "reporting",
]

ALLOWED_ANALYST_TITLE_MARKERS = [
    "business analyst",
    "business systems analyst",
    "systems analyst",
    "financial systems analyst",
    "technical analyst",
    "product analyst",
]

NEUTRAL_SERVICE_ROLE_MARKERS = [
    "consultant",
    "consulting",
    "implementation",
    "integration",
    "integrations",
    "data migration",
    "migration",
    "specialist",
    "advisor",
    "strategist",
    "professional services",
    "field services",
    "enablement",
]


HIGH_FIT_FINTECH_TITLE_MARKERS = [
    "technical product manager",
    "product manager",
    "product lead",
    "solutions architect",
    "solution architect",
    "product architect",
    "integration architect",
    "architect",
    "integrations manager",
    "integration manager",
    "solutions consultant",
    "consultant",
    "business systems analyst",
    "business analyst",
    "systems analyst",
    "technical program manager",
    "program manager",
    "application manager",
]

HIGH_FIT_FINTECH_DOMAIN_MARKERS = [
    "api",
    "apis",
    "integration",
    "integrations",
    "data platform",
    "data platforms",
    "data integration",
    "data pipeline",
    "platform",
    "financial platform",
    "capital platform",
    "payments",
    "payment",
    "money movement",
    "funds flow",
    "ledger",
    "ledgers",
    "financial reporting",
    "reporting",
    "billing automation",
    "reconciliation",
    "back office",
    "investment operations",
    "investment solution",
    "investment solutions",
    "portfolio",
    "portfolio workflows",
    "financial advisor",
    "advisor platform",
    "wealth management",
    "wealth",
    "asset management",
    "capital markets",
    "trading systems",
    "trading",
    "custody",
    "brokerage",
    "tax lots",
    "money movement platform",
    "payments experience",
    "card ledgers",
    "tradefi",
    "financial systems",
    "aladdin",
    "developer ecosystem",
    "partner platform",
    "partner api",
]

HIGH_FIT_FINTECH_PHRASES: List[Tuple[str, int]] = [
    ("technical product manager", 5),
    ("senior technical product manager", 7),
    ("technical product manager - data platform", 10),
    ("product lead, data & integrations", 11),
    ("senior product manager", 4),
    ("senior product manager - apis & integrations", 10),
    ("senior product manager - apis and integrations", 10),
    ("financial platform", 9),
    ("capital platform", 9),
    ("card ledgers", 11),
    ("money movement", 10),
    ("funds flow", 10),
    ("ledger", 7),
    ("financial reporting", 9),
    ("payments", 7),
    ("billing automation", 8),
    ("advisor & portfolio workflows", 11),
    ("advisor and portfolio workflows", 11),
    ("financial advisor & portfolio workflows", 12),
    ("financial advisor and portfolio workflows", 12),
    ("investment solutions", 8),
    ("wealth management", 7),
    ("wealth and asset management", 8),
    ("wam", 4),
    ("tradefi", 10),
    ("trading systems", 8),
    ("aladdin", 10),
    ("api integration", 8),
    ("apis & integrations", 9),
    ("apis and integrations", 9),
    ("data & integrations", 9),
    ("data and integrations", 9),
    ("separately managed accounts", 10),
    ("developer ecosystem & partner platform", 8),
    ("developer ecosystem and partner platform", 8),
]

HIGH_FIT_TITLE_DOMAIN_SYNERGIES: List[Tuple[List[str], List[str], int, str]] = [
    (["product manager", "product lead", "technical product manager"], ["payments", "money movement", "ledger", "financial reporting", "capital platform", "financial platform", "billing automation"], 10, "pm_financial_platform"),
    (["architect", "solution architect", "solutions architect", "product architect", "integration architect"], ["aladdin", "trading", "capital markets", "wealth management", "custody", "brokerage", "financial systems", "investment solutions"], 9, "architect_financial_systems"),
    (["consultant", "solutions consultant", "application manager", "integrations manager", "integration manager"], ["aladdin", "trading systems", "capital markets", "wealth management", "custody", "brokerage", "financial systems", "investment solutions", "api", "integrations"], 8, "consulting_financial_systems"),
    (["business analyst", "business systems analyst", "systems analyst"], ["financial reporting", "reconciliation", "ledger", "portfolio", "investment operations", "financial systems", "data platform"], 8, "analyst_financial_data"),
    (["program manager", "technical program manager"], ["financial platform", "capital platform", "payments", "money movement", "portfolio", "wealth management", "trading"], 7, "program_financial_platform"),
]

HIGH_FIT_DOMAIN_BONUS_CAP = 22


# ============================================================
# SCORING
# ============================================================

POSITIVE_KEYWORDS: List[Tuple[str, int]] = [
    ("aladdin", 12),
    ("eagle pace", 12),
    ("charles river", 10),
    ("investment data warehouse", 10),
    ("idw", 8),
    ("ss&c", 8),
    ("maximis", 8),
    ("ibor", 8),
    ("abor", 8),
    ("capital markets", 8),
    ("fixed income", 6),
    ("equities", 5),
    ("derivatives", 5),
    ("trading systems", 7),
    ("portfolio analytics", 7),
    ("risk analytics", 6),
    ("financial reporting", 7),
    ("reconciliation", 7),
    ("subledger", 7),
    ("gl integration", 7),
    ("fund accounting", 7),
    ("month-end close", 6),
    ("sox", 5),
    ("nfs", 8),
    ("dtcc", 7),
    ("custodian", 7),
    ("brokerage", 7),
    ("self-directed trading", 7),
    ("tax lots", 6),
    ("settlements", 5),
    ("clearing", 5),
    ("alternative investments", 8),
    ("private markets", 8),
    ("private equity", 7),
    ("hedge fund", 7),
    ("fund administration", 7),
    ("subscription documents", 8),
    ("investor onboarding", 6),
    ("wealth management", 7),
    ("advisor technology", 7),
    ("wealthtech", 7),
    ("ria platform", 7),
    ("portfolio reporting", 6),
    ("payment rails", 6),
    ("ach", 5),
    ("money movement", 6),
    ("disbursements", 5),
    ("b2b payments", 7),
    ("api integration", 7),
    ("data integration", 6),
    ("custodian api", 8),
    ("market data api", 7),
    ("integration platform", 6),
    ("financial services", 5),
    ("fintech", 4),
    ("data platform", 4),
    ("etl", 4),
    ("data pipeline", 4),
    ("investment management", 7),
    ("portfolio management", 7),
    ("managed accounts", 7),
    ("separately managed accounts", 8),
    ("bill pay", 7),
    ("payment operations", 6),
    ("payables", 5),
    ("card issuing", 7),
    ("partner platform", 5),
    ("partner api", 6),
    ("asset management", 5),
    ("ledger", 6),
    ("double entry", 8),
    ("double-entry", 8),
    ("general ledger", 6),
    ("journal entry", 5),
    ("gl mapping", 6),
]

NEGATIVE_KEYWORDS: List[Tuple[str, int]] = [
    ("earned wage access", 30),
    ("ewa", 20),
    ("buy now pay later", 30),
    ("bnpl", 25),
    ("consumer lending", 25),
    ("mortgage", 20),
    ("home equity", 20),
    ("personal finance", 20),
    ("credit scoring", 15),
    ("consumer loans", 20),
    ("incident management", 25),
    ("site reliability", 30),
    ("kubernetes", 20),
    ("ci/cd", 15),
    ("observability", 20),
    ("pagerduty", 25),
    ("datadog", 20),
    ("vector database", 30),
    ("llm infrastructure", 30),
    ("embedding models", 30),
    ("ml platform", 25),
    ("model training", 20),
    ("deep learning", 25),
    ("feature engineering", 15),
    ("consumer app", 20),
    ("mobile banking", 20),
    ("retail banking", 15),
    ("user acquisition", 20),
    ("growth hacking", 25),
    ("developer tools", 20),
    ("sdk for developers", 25),
    ("cli tool", 20),
    ("auth platform", 15),
    ("small business payroll", 20),
    ("employee scheduling", 25),
    ("restaurant software", 30),
    ("blockchain", 20),
    ("defi", 25),
    ("smart contracts", 25),
    ("web3", 25),
    ("cryptocurrency", 20),
    ("nft", 25),
    ("write production code", 25),
    ("daily coding", 25),
    ("hands-on coding", 25),
    ("golang", 20),
    ("rust language", 20),
    ("c++ development", 20),
    ("marketing automation", 25),
    ("lifecycle campaigns", 20),
    ("a/b testing consumer", 20),
    ("seo/sem", 25),
]

SWEET_SPOT_PHRASES: List[Tuple[str, int]] = [
    ("investment data warehouse", 10),
    ("aladdin", 10),
    ("capital markets data", 8),
    ("alternative investments platform", 8),
    ("subscription document", 8),
    ("custodian integration", 8),
    ("portfolio analytics", 7),
    ("financial data integration", 7),
    ("wealth management platform", 7),
    ("reconciliation platform", 7),
    ("fund accounting", 7),
    ("brokerage api", 8),
    ("trading platform", 6),
    ("investment management platform", 8),
    ("double entry ledger", 10),
    ("double-entry ledger", 10),
    ("event-sourced", 6),
    ("event sourced", 6),
    ("gl mapping", 8),
]

NON_US_MARKERS = [
    "uk", "united kingdom", "london", "edinburgh", "dublin", "ireland",
    "canada", "toronto", "vancouver", "india", "poland", "germany",
    "france", "singapore", "australia", "netherlands", "spain",
    "sweden", "switzerland", "israel",
]
REMOTE_MARKER_PATTERNS = [
    r"\bremote\b",
    r"\bwork from home\b",
    r"\bhome[- ]based\b",
    r"\btelecommut(?:e|ing)?\b",
    r"\bvirtual\b",
    r"\bremote[- ]first\b",
    r"\banywhere in (?:the )?u\.?s\.?\b",
    r"\bus remote\b",
    r"\bremote us\b",
    # "United States" (and variants) as the sole location = remote-anywhere
    r"^united states$",
    r"^u\.?s\.?a?\.?$",
]
HYBRID_MARKERS = ["hybrid", "in-office", "in office", "onsite", "on-site"]

# Local-commute markers — overridden by config/job_search_preferences.yaml at runtime.
# Replace these with city/town names within commuting distance of your location.
LOCAL_COMMUTE_MARKERS = [
    "YOUR_CITY", "NEARBY_CITY_1", "NEARBY_CITY_2",
]

# ============================================================
# YAML PREFERENCES OVERRIDES (v6)
# ============================================================

PREFERENCES_PATH, PREFERENCES = _load_preferences_from_yaml()

TITLE_REQUIRE_ONE_POSITIVE_KEYWORD = True
TITLE_POSITIVE_WEIGHTS: List[Tuple[str, int]] = []
TITLE_RESCUE_ADJACENT_TITLE_MARKERS_CFG: List[str] = []
TITLE_RESCUE_ANALYST_VARIANT_MARKERS_CFG: List[str] = []
TITLE_RESCUE_ADJACENT_TITLE_AUTO_RESCUE_PATTERNS_CFG: List[str] = []
TITLE_RESCUE_STRONG_BODY_DOMAIN_MARKERS_CFG: List[str] = []
TITLE_RESCUE_STRONG_BODY_MIN_POSITIVE_POINTS_CFG = TITLE_RESCUE_STRONG_BODY_MIN_POSITIVE_POINTS
TITLE_RESCUE_STRONG_BODY_MIN_POSITIVE_TERMS_CFG = TITLE_RESCUE_STRONG_BODY_MIN_POSITIVE_TERMS
TITLE_RESCUE_ADJACENT_TITLE_BONUS_CFG = TITLE_RESCUE_ADJACENT_TITLE_BONUS
TITLE_RESCUE_ADJACENT_TITLE_STRONG_DOMAIN_BONUS_CFG = TITLE_RESCUE_ADJACENT_TITLE_STRONG_DOMAIN_BONUS
TITLE_RESCUE_ADJACENT_TITLE_MIN_SCORE_TO_KEEP_CFG = TITLE_RESCUE_ADJACENT_TITLE_MIN_SCORE_TO_KEEP
PM_REQUIRES_MODIFIER = True
BSA_REQUIRES_MODIFIER = True
COUNT_UNIQUE_KEYWORD_MATCHES_ONLY = True
POSITIVE_KEYWORD_CAP = 999
NEGATIVE_KEYWORD_CAP = 999

DEFAULT_LANE_EXCLUSION_TITLE_ONLY = [
    "customer experience",
    "vendor management",
    "channel experience",
    "common experiences",
]

DEFAULT_LANE_EXCLUSION_BODY_STRICT = [
    "salesforce",
    "tax credits",
    "tax resolution",
    "contractor products",
]

DEFAULT_LANE_EXCLUSION_BODY_ROLE_EVIDENCE = {
    "salesforce": ["salesforce", "sfdc", "crm", "revops", "go to market", "gtm"],
    "tax credits": ["tax", "credits", "credit"],
    "tax resolution": ["tax", "resolution"],
    "contractor products": ["contractor", "trade", "home services"],
}

ROLE_LANE_EXCLUDE_TITLE_ONLY = list(DEFAULT_LANE_EXCLUSION_TITLE_ONLY)
ROLE_LANE_EXCLUDE_BODY_STRICT = list(DEFAULT_LANE_EXCLUSION_BODY_STRICT)
ROLE_LANE_EXCLUDE_BODY_ROLE_EVIDENCE = dict(DEFAULT_LANE_EXCLUSION_BODY_ROLE_EVIDENCE)
ROLE_LANE_EXCLUDE_BODY_MIN_OCCURRENCES = 2

if PREFERENCES:
    LOCATION_POLICY = str(_pref_get(PREFERENCES, ["search", "location_policy"], LOCATION_POLICY)).strip() or LOCATION_POLICY
    US_ONLY = bool(_pref_get(PREFERENCES, ["search", "geography", "us_only"], US_ONLY))
    ENFORCE_JOB_AGE = bool(_pref_get(PREFERENCES, ["search", "recency", "enforce_job_age"], ENFORCE_JOB_AGE))
    MAX_JOB_AGE_DAYS = int(_pref_get(PREFERENCES, ["search", "recency", "max_job_age_days"], MAX_JOB_AGE_DAYS))
    REQUIRE_REMOTE = LOCATION_POLICY == "remote_only"

    ALLOW_LOCAL_HYBRID_HIGH_COMP = bool(
        _pref_get(PREFERENCES, ["search", "location_preferences", "local_hybrid", "enabled"], ALLOW_LOCAL_HYBRID_HIGH_COMP)
    )
    PRIMARY_SEARCH_ZIP = str(
        _pref_get(PREFERENCES, ["search", "location_preferences", "local_hybrid", "primary_zip"], PRIMARY_SEARCH_ZIP)
    ).strip() or PRIMARY_SEARCH_ZIP
    LOCAL_HYBRID_RADIUS_MILES = int(
        _pref_get(PREFERENCES, ["search", "location_preferences", "local_hybrid", "radius_miles"], LOCAL_HYBRID_RADIUS_MILES)
    )
    LOCAL_COMMUTE_MARKERS = _pref_phrase_list(
        _pref_get(PREFERENCES, ["search", "location_preferences", "local_hybrid", "markers"], LOCAL_COMMUTE_MARKERS),
        LOCAL_COMMUTE_MARKERS,
    )

    ENFORCE_MIN_SALARY = bool(
        _pref_get(PREFERENCES, ["search", "compensation", "enforce_min_salary"], ENFORCE_MIN_SALARY)
    )
    MIN_SALARY_USD = int(_pref_get(PREFERENCES, ["search", "compensation", "min_salary_usd"], MIN_SALARY_USD))
    PREFERRED_REMOTE_MIN_SALARY_USD = int(
        _pref_get(PREFERENCES, ["search", "compensation", "preferred_remote_min_salary_usd"], PREFERRED_REMOTE_MIN_SALARY_USD)
    )
    ALLOW_MISSING_SALARY = bool(
        _pref_get(PREFERENCES, ["search", "compensation", "allow_missing_salary"], ALLOW_MISSING_SALARY)
    )
    SALARY_FLOOR_BASIS = str(
        _pref_get(PREFERENCES, ["search", "compensation", "salary_floor_basis"], SALARY_FLOOR_BASIS)
    ).strip() or SALARY_FLOOR_BASIS
    LOCAL_HYBRID_MIN_SALARY_USD = int(
        _pref_get(PREFERENCES, ["search", "location_preferences", "local_hybrid", "allow_if_salary_at_least_usd"], LOCAL_HYBRID_MIN_SALARY_USD)
    )

    MIN_SCORE_TO_KEEP = int(_pref_get(PREFERENCES, ["scoring", "minimum_score_to_keep"], MIN_SCORE_TO_KEEP))
    REQUIRE_STRONG_TITLE_FOR_APPLY_NOW = bool(
        _pref_get(PREFERENCES, ["scoring", "apply_now", "require_strong_title"], REQUIRE_STRONG_TITLE_FOR_APPLY_NOW)
    )
    APPLY_NOW_MIN_ROLE_ALIGNMENT = float(
        _pref_get(PREFERENCES, ["scoring", "apply_now", "min_role_alignment"], APPLY_NOW_MIN_ROLE_ALIGNMENT)
    )
    APPLY_NOW_DIRECT_TITLE_MARKERS = _pref_phrase_list(
        _pref_get(PREFERENCES, ["scoring", "apply_now", "direct_title_markers"], APPLY_NOW_DIRECT_TITLE_MARKERS),
        APPLY_NOW_DIRECT_TITLE_MARKERS,
    )
    ACTION_BUCKET_RANKS = _pref_action_bucket_ranks(
        _pref_get(PREFERENCES, ["scoring", "action_buckets", "ranks"], ACTION_BUCKET_RANKS),
        ACTION_BUCKET_RANKS,
    )
    ACTION_BUCKET_RULES = _pref_action_bucket_rules(
        _pref_get(PREFERENCES, ["scoring", "action_buckets", "rules"], ACTION_BUCKET_RULES),
        ACTION_BUCKET_RULES,
    )

    MISSING_SALARY_SCORE_PENALTY = int(
        _pref_get(PREFERENCES, ["scoring", "adjustments", "missing_salary_penalty"], MISSING_SALARY_SCORE_PENALTY)
    )
    SALARY_ABOVE_TARGET_BONUS = int(
        _pref_get(PREFERENCES, ["scoring", "adjustments", "salary_at_or_above_target_bonus"], SALARY_ABOVE_TARGET_BONUS)
    )
    LOCAL_HYBRID_SCORE_BONUS = int(
        _pref_get(PREFERENCES, ["search", "location_preferences", "local_hybrid", "bonus"], LOCAL_HYBRID_SCORE_BONUS)
    )
    REMOTE_US_SCORE_BONUS = int(
        _pref_get(PREFERENCES, ["search", "location_preferences", "remote_us", "bonus"], REMOTE_US_SCORE_BONUS)
    )

    TITLE_REQUIRE_ONE_POSITIVE_KEYWORD = bool(
        _pref_get(PREFERENCES, ["titles", "require_one_positive_keyword"], TITLE_REQUIRE_ONE_POSITIVE_KEYWORD)
    )
    TITLE_MUST_INCLUDE_ONE = _pref_phrase_list(
        _pref_get(PREFERENCES, ["titles", "positive_keywords"], TITLE_MUST_INCLUDE_ONE),
        TITLE_MUST_INCLUDE_ONE,
    )
    TITLE_HARD_EXCLUDE = _pref_phrase_list(
        _pref_get(PREFERENCES, ["titles", "negative_disqualifiers"], TITLE_HARD_EXCLUDE),
        TITLE_HARD_EXCLUDE,
    )
    TITLE_POSITIVE_WEIGHTS = _pref_weight_pairs(
        _pref_get(PREFERENCES, ["titles", "positive_weights"], {}),
        TITLE_POSITIVE_WEIGHTS,
    )
    # Single source of truth for title disqualifiers.
    # Prefer titles.negative_disqualifiers and do not require a second duplicated YAML key.
    TITLE_NEGATIVE_DISQUALIFIERS = _pref_phrase_list(
        _pref_get(PREFERENCES, ["titles", "negative_disqualifiers"], TITLE_NEGATIVE_DISQUALIFIERS),
        TITLE_NEGATIVE_DISQUALIFIERS,
    )
    TITLE_FAST_TRACK_BASE_SCORE = int(
        _pref_get(PREFERENCES, ["titles", "fast_track_base_score"], TITLE_FAST_TRACK_BASE_SCORE)
    )
    TITLE_FAST_TRACK_MIN_WEIGHT = int(
        _pref_get(PREFERENCES, ["titles", "fast_track_min_weight"], TITLE_FAST_TRACK_MIN_WEIGHT)
    )
    SALARY_NEGOTIATION_BUFFER_PCT = float(
        _pref_get(PREFERENCES, ["search", "compensation", "negotiation_buffer_pct"], SALARY_NEGOTIATION_BUFFER_PCT)
    )
    TITLE_RESCUE_ADJACENT_TITLE_MARKERS_CFG = _pref_phrase_list(
        _pref_get(PREFERENCES, ["policy", "title_rescue", "adjacent_title_markers"], TITLE_RESCUE_ADJACENT_TITLE_MARKERS),
        TITLE_RESCUE_ADJACENT_TITLE_MARKERS,
    )
    TITLE_RESCUE_ANALYST_VARIANT_MARKERS_CFG = _pref_phrase_list(
        _pref_get(PREFERENCES, ["policy", "title_rescue", "analyst_variant_markers"], TITLE_RESCUE_ANALYST_VARIANT_MARKERS),
        TITLE_RESCUE_ANALYST_VARIANT_MARKERS,
    )
    TITLE_RESCUE_ADJACENT_TITLE_AUTO_RESCUE_PATTERNS_CFG = _pref_phrase_list(
        _pref_get(PREFERENCES, ["policy", "title_rescue", "adjacent_title_auto_rescue_patterns"], TITLE_RESCUE_ADJACENT_TITLE_AUTO_RESCUE_PATTERNS),
        TITLE_RESCUE_ADJACENT_TITLE_AUTO_RESCUE_PATTERNS,
    )
    TITLE_RESCUE_STRONG_BODY_DOMAIN_MARKERS_CFG = _pref_phrase_list(
        _pref_get(PREFERENCES, ["policy", "title_rescue", "strong_body_domain_markers"], TITLE_RESCUE_STRONG_BODY_DOMAIN_MARKERS),
        TITLE_RESCUE_STRONG_BODY_DOMAIN_MARKERS,
    )
    TITLE_RESCUE_STRONG_BODY_MIN_POSITIVE_POINTS_CFG = int(
        _pref_get(PREFERENCES, ["policy", "title_rescue", "strong_body_min_positive_points"], TITLE_RESCUE_STRONG_BODY_MIN_POSITIVE_POINTS)
    )
    TITLE_RESCUE_STRONG_BODY_MIN_POSITIVE_TERMS_CFG = int(
        _pref_get(PREFERENCES, ["policy", "title_rescue", "strong_body_min_positive_terms"], TITLE_RESCUE_STRONG_BODY_MIN_POSITIVE_TERMS)
    )
    TITLE_RESCUE_ADJACENT_TITLE_BONUS_CFG = int(
        _pref_get(PREFERENCES, ["policy", "title_rescue", "adjacent_title_bonus"], TITLE_RESCUE_ADJACENT_TITLE_BONUS)
    )
    TITLE_RESCUE_ADJACENT_TITLE_STRONG_DOMAIN_BONUS_CFG = int(
        _pref_get(PREFERENCES, ["policy", "title_rescue", "adjacent_title_strong_domain_bonus"], TITLE_RESCUE_ADJACENT_TITLE_STRONG_DOMAIN_BONUS)
    )
    TITLE_RESCUE_ADJACENT_TITLE_MIN_SCORE_TO_KEEP_CFG = int(
        _pref_get(PREFERENCES, ["policy", "title_rescue", "adjacent_title_min_score_to_keep"], TITLE_RESCUE_ADJACENT_TITLE_MIN_SCORE_TO_KEEP)
    )

    PM_REQUIRES_MODIFIER = bool(
        _pref_get(PREFERENCES, ["titles", "constraints", "product_manager_requires_modifier"], PM_REQUIRES_MODIFIER)
    )
    PM_ALLOWED_MODIFIERS = _pref_phrase_list(
        _pref_get(PREFERENCES, ["titles", "constraints", "product_manager_allowed_modifiers"], PM_ALLOWED_MODIFIERS),
        PM_ALLOWED_MODIFIERS,
    )
    BSA_REQUIRES_MODIFIER = bool(
        _pref_get(PREFERENCES, ["titles", "constraints", "business_analyst_requires_modifier"], BSA_REQUIRES_MODIFIER)
    )
    BSA_ALLOWED_MODIFIERS = _pref_phrase_list(
        _pref_get(PREFERENCES, ["titles", "constraints", "business_analyst_allowed_modifiers"], BSA_ALLOWED_MODIFIERS),
        BSA_ALLOWED_MODIFIERS,
    )

    POSITIVE_KEYWORDS = _pref_weight_pairs(
        _pref_get(PREFERENCES, ["keywords", "body_positive"], {}),
        POSITIVE_KEYWORDS,
    )
    NEGATIVE_KEYWORDS = _pref_weight_pairs(
        _pref_get(PREFERENCES, ["keywords", "body_negative"], {}),
        NEGATIVE_KEYWORDS,
    )
    COUNT_UNIQUE_KEYWORD_MATCHES_ONLY = bool(
        _pref_get(PREFERENCES, ["scoring", "keyword_matching", "count_unique_matches_only"], COUNT_UNIQUE_KEYWORD_MATCHES_ONLY)
    )
    POSITIVE_KEYWORD_CAP = int(
        _pref_get(PREFERENCES, ["scoring", "keyword_matching", "positive_keyword_cap"], POSITIVE_KEYWORD_CAP)
    )
    NEGATIVE_KEYWORD_CAP = int(
        _pref_get(PREFERENCES, ["scoring", "keyword_matching", "negative_keyword_cap"], NEGATIVE_KEYWORD_CAP)
    )

    ROLE_LANE_EXCLUDE_TITLE_ONLY = _pref_phrase_list(
        _pref_get(PREFERENCES, ["policy", "lane_exclusions", "title_only_markers"], DEFAULT_LANE_EXCLUSION_TITLE_ONLY),
        DEFAULT_LANE_EXCLUSION_TITLE_ONLY,
    )
    ROLE_LANE_EXCLUDE_BODY_STRICT = _pref_phrase_list(
        _pref_get(PREFERENCES, ["policy", "lane_exclusions", "body_markers_strict"], DEFAULT_LANE_EXCLUSION_BODY_STRICT),
        DEFAULT_LANE_EXCLUSION_BODY_STRICT,
    )
    ROLE_LANE_EXCLUDE_BODY_MIN_OCCURRENCES = int(
        _pref_get(PREFERENCES, ["policy", "lane_exclusions", "body_strict_min_occurrences"], ROLE_LANE_EXCLUDE_BODY_MIN_OCCURRENCES)
    )
    raw_role_evidence = _pref_get(
        PREFERENCES,
        ["policy", "lane_exclusions", "body_marker_role_evidence"],
        DEFAULT_LANE_EXCLUSION_BODY_ROLE_EVIDENCE,
    )
    if isinstance(raw_role_evidence, dict) and raw_role_evidence:
        ROLE_LANE_EXCLUDE_BODY_ROLE_EVIDENCE = {
            str(k or "").strip().lower(): _pref_phrase_list(v, DEFAULT_LANE_EXCLUSION_BODY_ROLE_EVIDENCE.get(str(k or "").strip().lower(), []))
            for k, v in raw_role_evidence.items()
            if str(k or "").strip()
        }
    else:
        ROLE_LANE_EXCLUDE_BODY_ROLE_EVIDENCE = dict(DEFAULT_LANE_EXCLUSION_BODY_ROLE_EVIDENCE)

CONTEXT_FALLBACK_TITLE_MARKERS = [
    "solution architect",
    "solutions architect",
    "product architect",
    "integration architect",
    "platform architect",
    "solutions consultant",
    "solution consultant",
    "integration consultant",
    "integration product manager",
    "platform product manager",
]

NON_US_MARKERS.extend([
    "brazil", "sao paulo", "são paulo", "dubai", "uae", "united arab emirates",
])



# ============================================================
# DATA MODELS
# ============================================================

@dataclass
class Company:
    name: str
    tier: int
    priority: str
    adapter: str
    domain: str
    industry: List[str]
    adapter_key: Optional[str] = None
    careers_url: Optional[str] = None
    discovery_urls: Optional[List[str]] = None
    notes: str = ""
    # Optional fields set manually or by the healer after discovery
    render_required: bool = False       # JS-rendered page; generic scraping won't work
    jobs_api_url: Optional[str] = None  # direct JSON endpoint bypassing the frontend
    iframe_src: Optional[str] = None    # ATS board embedded via iframe


@dataclass
class Job:
    company: str
    tier: int
    priority: str
    title: str
    normalized_title: str
    role_family: str
    role_alignment_score: float
    role_alignment_label: str
    source_trust_label: str
    source_trust_adjustment: float
    lane_fit_score: float
    lane_fit_label: str
    benchmark_lane: str
    benchmark_match_score: float
    benchmark_match_reason: str
    lane_cohort: str
    location: str
    url: str
    canonical_url: str
    source: str
    posted_at: str
    age_days: Optional[int]
    score: float
    fit_band: str
    action_bucket: str
    decision_reason: str
    is_remote: bool
    is_hybrid: bool
    is_non_us: bool
    apply_domains_hit: str
    skip_domains_hit: str
    primary_apply_domain: str
    matched_keywords: str
    penalized_keywords: str
    salary_range: str
    salary_currency: str
    salary_low: Optional[float]
    salary_high: Optional[float]
    salary_period: str
    description_excerpt: str
    company_notes: str
    keep: bool = True
    drop_stage: str = ""
    drop_reason: str = ""
    decision_score: float = 0.0
    title_gate_matched_keywords: str = ""
    title_rescue_bucket: str = ""
    title_rescue_trigger: str = ""
    adjacent_title_marker_hit: str = ""
    adjacent_domain_terms_hit: str = ""
    normalized_title_for_adjacent_match: str = ""
    adjacent_title_patterns_checked: str = ""
    adjacent_title_patterns_hit: str = ""
    adjacent_title_matched_token_slice: str = ""
    adjacent_title_tokens_debug: str = ""
    adjacent_pattern_tokens_debug: str = ""
    adjacent_match_attempts: str = ""
    score_threshold_used: Optional[float] = None
    base_score_before_rescue: Optional[float] = None
    adjacent_rescue_bonus_applied: Optional[float] = None
    final_score_after_rescue: Optional[float] = None
    is_new: bool = True
    seen_first_at: str = ""
    seen_last_at: str = ""
    manual_review: bool = False


@dataclass
class RejectedJob:
    company: str
    tier: int
    priority: str
    title: str
    normalized_title: str
    role_family: str
    location: str
    url: str
    canonical_url: str
    source: str
    posted_at: str
    age_days: Optional[int]
    drop_stage: str
    drop_reason_code: str
    drop_reason_detail: str
    drop_reason: str
    title_gate_matched_keywords: str = ""
    title_rescue_bucket: str = ""
    title_rescue_trigger: str = ""
    adjacent_title_marker_hit: str = ""
    adjacent_domain_terms_hit: str = ""
    normalized_title_for_adjacent_match: str = ""
    adjacent_title_patterns_checked: str = ""
    adjacent_title_patterns_hit: str = ""
    adjacent_title_matched_token_slice: str = ""
    adjacent_title_tokens_debug: str = ""
    adjacent_pattern_tokens_debug: str = ""
    adjacent_match_attempts: str = ""
    decision_reason: str = ""
    score: Optional[float] = None
    decision_score: Optional[float] = None
    score_threshold_used: Optional[float] = None
    base_score_before_rescue: Optional[float] = None
    adjacent_rescue_bonus_applied: Optional[float] = None
    final_score_after_rescue: Optional[float] = None
    fit_band: str = ""
    action_bucket: str = ""
    is_remote: bool = False
    is_hybrid: bool = False
    is_non_us: bool = False
    apply_domains_hit: str = ""
    skip_domains_hit: str = ""
    primary_apply_domain: str = ""
    matched_keywords: str = ""
    penalized_keywords: str = ""
    salary_range: str = ""
    salary_currency: str = ""
    salary_low: Optional[float] = None
    salary_high: Optional[float] = None
    salary_period: str = ""
    description_excerpt: str = ""
    company_notes: str = ""
    role_alignment_score: float = 0.0
    role_alignment_label: str = ""
    source_trust_label: str = ""
    source_trust_adjustment: float = 0.0
    lane_fit_score: float = 0.0
    lane_fit_label: str = ""
    benchmark_lane: str = ""
    benchmark_match_score: float = 0.0
    benchmark_match_reason: str = ""
    lane_cohort: str = ""


# ============================================================
# HELPERS
# ============================================================

def now_utc() -> datetime:
    return datetime.now(timezone.utc)

def iso_now() -> str:
    return now_utc().isoformat()

def browser_headers(
    url: str,
    referer: Optional[str] = None,
    accept_json: bool = False,
    method: str = "GET",
) -> Dict[str, str]:
    method = (method or "GET").upper()
    headers = dict(HEADERS)
    parsed = urlparse(url)
    origin = f"{parsed.scheme}://{parsed.netloc}" if parsed.scheme and parsed.netloc else ""
    # GET requests with synthetic Origin headers tend to trip some career-site WAFs.
    if method != "GET" and origin:
        headers["Origin"] = origin
    if referer and referer != url:
        headers["Referer"] = referer
    if accept_json:
        headers["Accept"] = "application/json,text/plain,*/*"
    headers.update({
        "Sec-Fetch-Site": "same-origin" if referer else "none",
        "Sec-Fetch-Mode": "cors" if accept_json else "navigate",
        "Sec-Fetch-Dest": "empty" if accept_json else "document",
    })
    if method == "GET":
        headers["Upgrade-Insecure-Requests"] = "1"
    return headers



def set_run_context(kind: str, name: str, adapter: str = "", careers_url: str = "") -> None:
    _RUN_CTX.context = {
        "kind": clean(kind),
        "name": clean(name),
        "adapter": clean(adapter),
        "careers_url": clean(careers_url),
    }


def clear_run_context() -> None:
    _RUN_CTX.context = {}


def url_family_key(url: str) -> str:
    try:
        parsed = urlparse(url)
    except Exception:
        return ""
    host = (parsed.netloc or "").lower()
    segments = [seg for seg in (parsed.path or "").split("/") if seg]
    lowered = [seg.lower() for seg in segments]

    if not host:
        return ""

    # Board APIs need narrower family keys so one bad token does not block the whole host.
    if host == "boards-api.greenhouse.io" and len(lowered) >= 4 and lowered[0] == "v1" and lowered[1] == "boards":
        return f"{host}/v1/boards/{lowered[2]}"
    if host == "api.lever.co" and len(lowered) >= 3 and lowered[0] == "v0" and lowered[1] == "postings":
        return f"{host}/v0/postings/{lowered[2]}"
    if "myworkdayjobs.com" in host and len(lowered) >= 5 and lowered[0] == "wday" and lowered[1] == "cxs":
        return f"{host}/wday/cxs/{lowered[2]}/{lowered[3]}"

    first = lowered[0] if lowered else ""
    return f"{host}/{first}" if host else first


def url_family_is_blocked(url: str) -> bool:
    key = url_family_key(url)
    until_ts = URL_FAMILY_COOLDOWNS.get(key, 0)
    return bool(key and until_ts and time.time() < until_ts)


def block_url_family(url: str, seconds: int) -> None:
    key = url_family_key(url)
    if not key:
        return
    URL_FAMILY_COOLDOWNS[key] = max(URL_FAMILY_COOLDOWNS.get(key, 0), time.time() + max(1, seconds))


def workday_detail_family_key(host: str, site: str) -> str:
    host_key = clean(host).lower()
    site_key = clean(site).lower()
    if not host_key or not site_key:
        return ""
    return f"{host_key}::workday-detail::{site_key}"


def workday_detail_family_is_blocked(host: str, site: str) -> bool:
    key = workday_detail_family_key(host, site)
    until_ts = WORKDAY_DETAIL_FAMILY_COOLDOWNS.get(key, 0)
    return bool(key and until_ts and time.time() < until_ts)


def block_workday_detail_family(host: str, site: str, seconds: int) -> None:
    key = workday_detail_family_key(host, site)
    if not key:
        return
    WORKDAY_DETAIL_FAMILY_COOLDOWNS[key] = max(
        WORKDAY_DETAIL_FAMILY_COOLDOWNS.get(key, 0),
        time.time() + max(1, seconds),
    )


def classify_failure(url: str, status: Optional[int], exc: Optional[Exception] = None) -> str:
    if status == 404:
        return "not_found"
    if status == 429:
        return "rate_limited"
    if status == 403:
        return "forbidden"
    if status in {401, 406, 409, 410, 451}:
        return "blocked"
    lower = clean(str(exc or "")).lower()
    if any(marker in lower for marker in ["name resolution", "getaddrinfo failed", "nodename nor servname", "failed to resolve", "name or service not known"]):
        return "dns_failure"
    if "sec_e_wrong_principal" in lower or "wrong principal" in lower:
        return "ssl_hostname_mismatch"
    if ("hostname" in lower and "certificate" in lower) or ("certificate verify failed" in lower):
        return "ssl_error"
    if "invalid or blacklisted url" in lower:
        return "invalid_url"
    if "blacklisted" in lower:
        return "blacklisted_url"
    if "host temporarily blocked" in lower:
        return "host_blocked"
    if "url temporarily blocked" in lower:
        return "url_blocked"
    return "request_error"


def suggested_action_for_failure(failure_type: str) -> str:
    if failure_type in {"not_found", "dns_failure", "ssl_hostname_mismatch", "ssl_error", "invalid_url", "blacklisted_url"}:
        return "correct_registry_url"
    if failure_type == "rate_limited":
        return "reduce_retries_or_keep_manual"
    if failure_type == "forbidden":
        return "check_blocking_or_use_manual"
    return "review"


def record_registry_health(
    url: str,
    status: Optional[int] = None,
    failure_type: str = "",
    action_taken: str = "",
    note: str = "",
) -> None:
    safe = sanitize_url(url) or clean(url)
    if not safe:
        return
    ctx = dict(getattr(_RUN_CTX, 'context', {}))
    key = (ctx.get("kind", ""), ctx.get("name", ""), safe)
    rec = REGISTRY_HEALTH.get(key)
    if rec is None:
        rec = {
            "context_kind": ctx.get("kind", ""),
            "name": ctx.get("name", ""),
            "adapter": ctx.get("adapter", ""),
            "careers_url": ctx.get("careers_url", ""),
            "url": safe,
            "host": (urlparse(safe).netloc or "").lower() if "://" in safe else "",
            "last_status": "",
            "failure_type": "",
            "failure_count": 0,
            "action_taken": "",
            "suggested_action": "",
            "note": "",
        }
        REGISTRY_HEALTH[key] = rec
    rec["failure_count"] = int(rec.get("failure_count", 0)) + 1
    if status is not None:
        rec["last_status"] = status
    if failure_type:
        rec["failure_type"] = failure_type
        rec["suggested_action"] = suggested_action_for_failure(failure_type)
    if action_taken:
        rec["action_taken"] = action_taken
    if note:
        rec["note"] = clean(note)[:500]


def host_is_blocked(url: str) -> bool:
    host = urlparse(url).netloc.lower()
    until_ts = HOST_COOLDOWNS.get(host, 0)
    return bool(until_ts and time.time() < until_ts)


def block_host(url: str, seconds: int) -> None:
    host = urlparse(url).netloc.lower()
    if not host:
        return
    HOST_COOLDOWNS[host] = max(HOST_COOLDOWNS.get(host, 0), time.time() + max(1, seconds))


def sanitize_url(url: Optional[str]) -> str:
    raw = (url or "").strip()
    if not raw:
        return ""
    if raw.startswith("//"):
        raw = "https:" + raw
    elif not re.match(r"^[a-zA-Z][a-zA-Z0-9+.-]*://", raw):
        raw = "https://" + raw.lstrip("/")
    try:
        parsed = urlparse(raw)
    except Exception:
        return ""
    if parsed.scheme not in {"http", "https"} or not parsed.netloc:
        return ""
    cleaned = parsed._replace(netloc=parsed.netloc.lower(), fragment="")
    normalized = cleaned.geturl()
    if url_is_blacklisted(normalized):
        return ""
    return normalized


def url_is_blacklisted(url: Optional[str]) -> bool:
    raw = (url or "").strip()
    if not raw:
        return True
    try:
        parsed = urlparse(raw if "://" in raw else "https://" + raw.lstrip("/"))
    except Exception:
        return True
    host = (parsed.netloc or "").lower()
    path = (parsed.path or "").lower()
    full = f"{host}{path}"
    if any(marker in host for marker in BLOCKED_URL_HOST_MARKERS):
        return True
    if any(marker in full for marker in BLOCKED_URL_PATH_MARKERS):
        return True
    return False


def url_is_blocked(url: str) -> bool:
    key = sanitize_url(url)
    until_ts = URL_COOLDOWNS.get(key, 0)
    return bool(key and until_ts and time.time() < until_ts)


def block_url(url: str, seconds: int) -> None:
    key = sanitize_url(url)
    if not key:
        return
    URL_COOLDOWNS[key] = max(URL_COOLDOWNS.get(key, 0), time.time() + max(1, seconds))



def note_url_failure(url: str, status: Optional[int], failure_type: str = "") -> int:
    key = sanitize_url(url) or (url or "").strip()
    URL_FAILURE_COUNTS[key] = URL_FAILURE_COUNTS.get(key, 0) + 1
    if status == 404 or failure_type == "not_found":
        family_key = url_family_key(url)
        if family_key:
            URL_FAMILY_FAILURE_COUNTS[family_key] = URL_FAMILY_FAILURE_COUNTS.get(family_key, 0) + 1
            if URL_FAMILY_FAILURE_COUNTS[family_key] >= 3:
                block_url_family(url, 8 * 60 * 60)
                record_registry_health(url, status=status, failure_type="not_found", action_taken="url_family_quarantine", note=f"family={family_key}")
    return URL_FAILURE_COUNTS[key]


def is_workday_detail_url(url: str) -> bool:
    raw = (url or "").lower()
    return "myworkdayjobs.com" in raw and "/wday/cxs/" in raw and "/job/" in raw


def retry_policy(url: str, status: Optional[int], attempt: int, failure_count: int, failure_type: str = "") -> Tuple[bool, float]:
    if failure_type in {"invalid_url", "blacklisted_url", "dns_failure", "ssl_hostname_mismatch", "ssl_error"}:
        block_url(url, 8 * 60 * 60)
        return True, 0.0
    if status == 404 or failure_type == "not_found":
        block_url(url, 8 * 60 * 60)
        return True, 0.0
    if status == 429 or failure_type == "rate_limited":
        block_host(url, 8 * 60 * 60)
        block_url(url, 8 * 60 * 60)
        block_url_family(url, 8 * 60 * 60)
        return True, 0.0
    if status == 403 or failure_type == "forbidden":
        if failure_count >= 2 or attempt >= 1:
            block_url(url, 2 * 60 * 60)
            return True, 0.0
        return False, 1.5 * (attempt + 1)
    if status in {500, 502, 503, 504}:
        if is_workday_detail_url(url):
            block_url(url, 60 * 60)
            block_url_family(url, 60 * 60)
            return True, 0.0
        if failure_count >= 2 or attempt >= 1:
            block_url(url, 30 * 60)
            return True, 0.0
        return False, 0.75 * (attempt + 1)
    if status == 422 and "myworkdayjobs.com" in (url or "").lower():
        block_url(url, 8 * 60 * 60)
        block_url_family(url, 8 * 60 * 60)
        return True, 0.0
    if status in {401, 406, 409, 410, 451}:
        block_url(url, 2 * 60 * 60)
        return True, 0.0
    return False, 1.25 * (attempt + 1)

def exception_status_code(exc: Exception) -> Optional[int]:
    response = getattr(exc, "response", None)
    if response is not None:
        try:
            return int(response.status_code)
        except Exception:
            return None
    return None


def should_try_curl_fallback(exc: Exception) -> bool:
    if not CURL_BIN:
        return False
    status = exception_status_code(exc)
    if status in {401, 403, 406, 409, 503}:
        return True
    err_text = str(exc).lower()
    network_markers = [
        "ssl", "certificate verify failed", "max retries exceeded",
        "name resolution", "getaddrinfo failed", "connection aborted",
        "connection reset", "forbidden", "too many requests",
    ]
    return any(marker in err_text for marker in network_markers)


def _decode_subprocess_output(data: bytes) -> str:
    if not data:
        return ""
    for enc in ("utf-8", "utf-8-sig", "cp1252", "latin-1"):
        try:
            return data.decode(enc)
        except UnicodeDecodeError:
            continue
    return data.decode("utf-8", errors="replace")


def curl_fetch(url: str, referer: Optional[str] = None, accept_json: bool = False, payload: Optional[Dict[str, Any]] = None) -> str:
    if not CURL_BIN:
        raise RuntimeError("curl not available")
    cmd = [
        CURL_BIN, "-sS", "-L", "--compressed", "--max-time", str(REQUEST_TIMEOUT),
        "-A", HEADERS["User-Agent"],
        "-H", f"Accept: {'application/json,text/plain,*/*' if accept_json else HEADERS['Accept']}",
        "-H", f"Accept-Language: {HEADERS['Accept-Language']}",
        "-H", "Cache-Control: no-cache",
        "-H", "Pragma: no-cache",
        url,
    ]
    if referer and referer != url:
        cmd[1:1] = ["-e", referer]
    if payload is not None:
        cmd[1:1] = ["-X", "POST", "-H", "Content-Type: application/json", "--data", json.dumps(payload)]
    proc = subprocess.run(cmd, capture_output=True, text=False, timeout=REQUEST_TIMEOUT + 5)
    stdout_text = _decode_subprocess_output(proc.stdout)
    stderr_text = _decode_subprocess_output(proc.stderr)
    if proc.returncode != 0:
        raise RuntimeError((stderr_text or stdout_text or "curl failed").strip())
    return stdout_text



def fetch_text(url: str, retries: int = 3, referer: Optional[str] = None) -> str:
    raw_url = clean(url)
    url = sanitize_url(url)
    if not url:
        record_registry_health(raw_url, failure_type="invalid_url", action_taken="quarantine_url", note="invalid_or_blacklisted")
        raise RuntimeError("invalid or blacklisted url")
    if host_is_blocked(url):
        record_registry_health(url, failure_type="rate_limited", action_taken="skip_host_blocked", note="host_temporarily_blocked")
        raise RuntimeError(f"host temporarily blocked: {urlparse(url).netloc}")
    if url_is_blocked(url):
        record_registry_health(url, failure_type="not_found", action_taken="skip_url_blocked", note="url_temporarily_blocked")
        raise RuntimeError(f"url temporarily blocked: {url}")
    if url_family_is_blocked(url):
        record_registry_health(url, failure_type="not_found", action_taken="skip_url_family_blocked", note="url_family_temporarily_blocked")
        raise RuntimeError(f"url family temporarily blocked: {url_family_key(url)}")
    last_err = None
    for attempt in range(retries):
        try:
            r = SESSION.get(url, headers=browser_headers(url, referer=referer, method="GET"), timeout=REQUEST_TIMEOUT)
            r.raise_for_status()
            if not r.encoding:
                r.encoding = r.apparent_encoding or "utf-8"
            return r.text
        except Exception as e:
            last_err = e
            status = exception_status_code(e)
            failure_type = classify_failure(url, status, e)
            failure_count = note_url_failure(url, status, failure_type)
            record_registry_health(
                url,
                status=status,
                failure_type=failure_type,
                action_taken="retry" if attempt + 1 < retries else "fallback_or_fail",
                note=str(e),
            )
            if attempt == 0 and should_try_curl_fallback(e) and status != 429:
                try:
                    return curl_fetch(url, referer=referer, accept_json=False)
                except Exception as curl_e:
                    curl_failure_type = classify_failure(url, None, curl_e)
                    record_registry_health(url, failure_type=curl_failure_type, action_taken="curl_fallback_failed", note=str(curl_e))
                    logging.warning("curl text fallback failed (%s) url=%s", curl_e, url)
            should_break, sleep_for = retry_policy(url, status, attempt, failure_count, failure_type)
            logging.warning("fetch_text failed status=%s failure_type=%s attempt=%s url=%s err=%s", status, failure_type, attempt + 1, url, e)
            if should_break:
                break
            time.sleep(sleep_for)
    raise last_err


def fetch_json(url: str, retries: int = 3, referer: Optional[str] = None) -> Any:
    raw_url = clean(url)
    url = sanitize_url(url)
    if not url:
        record_registry_health(raw_url, failure_type="invalid_url", action_taken="quarantine_url", note="invalid_or_blacklisted")
        raise RuntimeError("invalid or blacklisted url")
    if host_is_blocked(url):
        record_registry_health(url, failure_type="rate_limited", action_taken="skip_host_blocked", note="host_temporarily_blocked")
        raise RuntimeError(f"host temporarily blocked: {urlparse(url).netloc}")
    if url_is_blocked(url):
        record_registry_health(url, failure_type="not_found", action_taken="skip_url_blocked", note="url_temporarily_blocked")
        raise RuntimeError(f"url temporarily blocked: {url}")
    if url_family_is_blocked(url):
        record_registry_health(url, failure_type="not_found", action_taken="skip_url_family_blocked", note="url_family_temporarily_blocked")
        raise RuntimeError(f"url family temporarily blocked: {url_family_key(url)}")
    last_err = None
    for attempt in range(retries):
        try:
            r = SESSION.get(url, headers=browser_headers(url, referer=referer, accept_json=True, method="GET"), timeout=REQUEST_TIMEOUT)
            r.raise_for_status()
            return r.json()
        except Exception as e:
            last_err = e
            status = exception_status_code(e)
            failure_type = classify_failure(url, status, e)
            failure_count = note_url_failure(url, status, failure_type)
            record_registry_health(
                url,
                status=status,
                failure_type=failure_type,
                action_taken="retry" if attempt + 1 < retries else "fallback_or_fail",
                note=str(e),
            )
            if attempt == 0 and should_try_curl_fallback(e) and status != 429:
                try:
                    return json.loads(curl_fetch(url, referer=referer, accept_json=True))
                except Exception as curl_e:
                    curl_failure_type = classify_failure(url, None, curl_e)
                    record_registry_health(url, failure_type=curl_failure_type, action_taken="curl_fallback_failed", note=str(curl_e))
                    logging.warning("curl json fallback failed (%s) url=%s", curl_e, url)
            should_break, sleep_for = retry_policy(url, status, attempt, failure_count, failure_type)
            logging.warning("fetch_json failed status=%s failure_type=%s attempt=%s url=%s err=%s", status, failure_type, attempt + 1, url, e)
            if should_break:
                break
            time.sleep(sleep_for)
    raise last_err


def fetch_json_post(url: str, payload: Optional[Dict[str, Any]] = None, retries: int = 3, referer: Optional[str] = None) -> Any:
    raw_url = clean(url)
    url = sanitize_url(url)
    if not url:
        record_registry_health(raw_url, failure_type="invalid_url", action_taken="quarantine_url", note="invalid_or_blacklisted")
        raise RuntimeError("invalid or blacklisted url")
    if host_is_blocked(url):
        record_registry_health(url, failure_type="rate_limited", action_taken="skip_host_blocked", note="host_temporarily_blocked")
        raise RuntimeError(f"host temporarily blocked: {urlparse(url).netloc}")
    if url_is_blocked(url):
        record_registry_health(url, failure_type="not_found", action_taken="skip_url_blocked", note="url_temporarily_blocked")
        raise RuntimeError(f"url temporarily blocked: {url}")
    if url_family_is_blocked(url):
        record_registry_health(url, failure_type="not_found", action_taken="skip_url_family_blocked", note="url_family_temporarily_blocked")
        raise RuntimeError(f"url family temporarily blocked: {url_family_key(url)}")
    last_err = None
    req_headers = browser_headers(url, referer=referer, accept_json=True, method="POST")
    req_headers["Content-Type"] = "application/json"
    for attempt in range(retries):
        try:
            r = SESSION.post(url, headers=req_headers, json=payload or {}, timeout=REQUEST_TIMEOUT)
            r.raise_for_status()
            return r.json()
        except Exception as e:
            last_err = e
            status = exception_status_code(e)
            failure_type = classify_failure(url, status, e)
            failure_count = note_url_failure(url, status, failure_type)
            record_registry_health(
                url,
                status=status,
                failure_type=failure_type,
                action_taken="retry" if attempt + 1 < retries else "fallback_or_fail",
                note=str(e),
            )
            if attempt == 0 and should_try_curl_fallback(e) and status != 429:
                try:
                    return json.loads(curl_fetch(url, referer=referer, accept_json=True, payload=payload or {}))
                except Exception as curl_e:
                    curl_failure_type = classify_failure(url, None, curl_e)
                    record_registry_health(url, failure_type=curl_failure_type, action_taken="curl_fallback_failed", note=str(curl_e))
                    logging.warning("curl post fallback failed (%s) url=%s", curl_e, url)
            should_break, sleep_for = retry_policy(url, status, attempt, failure_count, failure_type)
            logging.warning("fetch_json_post failed status=%s failure_type=%s attempt=%s/%s url=%s err=%s", status, failure_type, attempt + 1, retries, url, e)
            if attempt + 1 >= retries or should_break:
                break
            if sleep_for > 0:
                time.sleep(sleep_for)
    raise last_err

def clean(value: Optional[str]) -> str:
    if not value:
        return ""
    text = re.sub(r"<[^>]+>", " ", str(value))
    return re.sub(r"\s+", " ", text).strip()

def parse_human_date(value: Optional[str]) -> Optional[datetime]:
    if not value:
        return None
    raw = clean(value)
    raw = re.sub(r"\b(Posted|Published|Date Posted|Updated|Open Until Filled)\b\s*:?\s*", "", raw, flags=re.I)
    raw = re.sub(r"\s+", " ", raw).strip(" -")
    patterns = [
        "%Y-%m-%d",
        "%Y/%m/%d",
        "%b %d, %Y",
        "%B %d, %Y",
        "%d %b %Y",
        "%d %B %Y",
        "%b %d %Y",
        "%B %d %Y",
        "%m/%d/%Y",
    ]
    for fmt in patterns:
        try:
            dt = datetime.strptime(raw, fmt)
            return dt.replace(tzinfo=timezone.utc)
        except Exception:
            pass
    m = re.search(r"(\b(?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)[a-z]*\.?\s+\d{1,2},\s+\d{4})", raw, flags=re.I)
    if m:
        return parse_human_date(m.group(1))
    m = re.search(r"(\b\d{1,2}\s+(?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)[a-z]*\.?\s+\d{4})", raw, flags=re.I)
    if m:
        return parse_human_date(m.group(1))
    return None

def parse_date(value: Optional[str]) -> Optional[datetime]:
    if not value:
        return None
    try:
        if isinstance(value, str) and value.endswith("Z"):
            return datetime.fromisoformat(value.replace("Z", "+00:00"))
        if isinstance(value, str):
            return datetime.fromisoformat(value)
    except Exception:
        pass
    return parse_human_date(value)

def from_ms(ms: Optional[int]) -> Optional[datetime]:
    if ms is None:
        return None
    try:
        return datetime.fromtimestamp(ms / 1000, tz=timezone.utc)
    except Exception:
        return None

def job_age(dt: Optional[datetime]) -> Optional[int]:
    if not dt:
        return None
    return max(0, (now_utc() - dt.astimezone(timezone.utc)).days)

def canonicalize(url: str) -> str:
    url = re.sub(r"#.*$", "", url.strip())
    url = re.sub(r"\?.*$", "", url)
    return url.rstrip("/")


def normalize_title(title: str) -> str:
    t = f" {clean(title).lower()} "
    replacements = {
        "sr. ": "senior ",
        " sr ": " senior ",
        "pm,": "product manager,",
        " pm ": " product manager ",
        "tpm": "technical product manager",
        "tpgm": "technical program manager",
        "prod mgr": "product manager",
        "prod. mgr": "product manager",
        "&": " and ",
        "–": "-",
        "—": "-",
    }
    for old, new in replacements.items():
        t = t.replace(old, new)
    t = re.sub(r"\s+", " ", t).strip()
    return t


def _singularize_title_token(token: str) -> str:
    token = clean(token).lower().strip()
    if len(token) > 4 and token.endswith("ies"):
        return token[:-3] + "y"
    if len(token) > 4 and token.endswith("s") and not token.endswith("ss"):
        return token[:-1]
    return token


ADJACENT_TITLE_IGNORABLE_TOKENS = {
    "i", "ii", "iii", "iv", "v", "vi", "vii", "viii", "ix", "x",
    "senior", "sr", "lead", "principal", "staff", "manager", "director",
    "associate", "intern", "junior", "jr", "head", "vp", "svp", "avp",
}


def normalize_adjacent_title_text(title: str) -> str:
    t = normalize_title(title)
    t = re.sub(r"[^a-z0-9]+", " ", t.lower())
    t = re.sub(r"\s+", " ", t).strip()
    tokens = [_singularize_title_token(tok) for tok in t.split() if tok]
    return " ".join(tokens)


def adjacent_title_tokens(text: str, ignore_level_tokens: bool = True) -> Tuple[List[str], List[str], str]:
    normalized = normalize_adjacent_title_text(text)
    raw_tokens = [tok for tok in normalized.split() if tok]
    if not ignore_level_tokens:
        return raw_tokens, [], normalized
    kept: List[str] = []
    ignored: List[str] = []
    for tok in raw_tokens:
        if tok in ADJACENT_TITLE_IGNORABLE_TOKENS:
            ignored.append(tok)
        else:
            kept.append(tok)
    return kept, ignored, " ".join(kept).strip()


def _find_filtered_title_pattern_slice(title_tokens: List[str], pattern_tokens: List[str]) -> Optional[List[str]]:
    if not title_tokens or not pattern_tokens:
        return None
    plen = len(pattern_tokens)
    for idx in range(len(title_tokens) - plen + 1):
        candidate = title_tokens[idx: idx + plen]
        if candidate == pattern_tokens:
            return candidate
    return None


@lru_cache(maxsize=64)
def _pretokenize_adjacent_patterns(patterns_tuple: Tuple[str, ...]) -> Tuple[Tuple[str, str, Tuple[str, ...]], ...]:
    specs: List[Tuple[str, str, Tuple[str, ...]]] = []
    for pattern in patterns_tuple:
        pattern_tokens, _, normalized_pattern = adjacent_title_tokens(pattern, ignore_level_tokens=True)
        if not normalized_pattern or not pattern_tokens:
            continue
        specs.append((pattern, normalized_pattern, tuple(pattern_tokens)))
    return tuple(specs)


@lru_cache(maxsize=64)
def _normalized_adjacent_pattern_set(patterns_tuple: Tuple[str, ...]) -> Set[str]:
    return {normalized for _original, normalized, _tokens in _pretokenize_adjacent_patterns(patterns_tuple)}


def adjacent_title_pattern_debug(title: str, patterns: List[str]) -> Dict[str, Any]:
    title_tokens, ignored_tokens, normalized_title = adjacent_title_tokens(title, ignore_level_tokens=True)
    checked: List[str] = []
    hits: List[str] = []
    matched_slices: List[str] = []
    pattern_tokens_debug: List[str] = []
    match_attempts: List[str] = []
    matched_pattern: str = ""
    matched_token_slice: str = ""
    title_token_str = " ".join(title_tokens)
    for original_pattern, normalized_pattern, pattern_tokens_tuple in _pretokenize_adjacent_patterns(tuple(patterns)):
        pattern_tokens = list(pattern_tokens_tuple)
        checked.append(normalized_pattern)
        pattern_token_str = " ".join(pattern_tokens)
        pattern_tokens_debug.append(f"{normalized_pattern} => [{pattern_token_str}]")
        matched_slice = _find_filtered_title_pattern_slice(title_tokens, pattern_tokens)
        if matched_slice is not None:
            hits.append(normalized_pattern)
            matched_slice_str = " ".join(matched_slice)
            matched_slices.append(matched_slice_str)
            if not matched_pattern:
                matched_pattern = normalized_pattern
                matched_token_slice = matched_slice_str
            match_attempts.append(f"[{title_token_str}] ~ [{pattern_token_str}] => hit")
        else:
            match_attempts.append(f"[{title_token_str}] ~ [{pattern_token_str}] => miss")
    return {
        "normalized_title": normalized_title,
        "title_tokens": unique_preserve(title_tokens),
        "ignored_tokens": unique_preserve(ignored_tokens),
        "patterns_checked": unique_preserve(checked),
        "pattern_tokens_debug": unique_preserve(pattern_tokens_debug),
        "patterns_hit": unique_preserve(hits),
        "matched_token_slices": unique_preserve(matched_slices),
        "match_attempts": unique_preserve(match_attempts),
        "matched": bool(matched_pattern),
        "matched_pattern": matched_pattern,
        "matched_token_slice": matched_token_slice,
    }

def canonical_adjacent_match_result(title: str) -> Dict[str, Any]:
    combined_patterns = unique_preserve(TITLE_RESCUE_ADJACENT_TITLE_MARKERS_CFG + TITLE_RESCUE_ANALYST_VARIANT_MARKERS_CFG)
    combined_debug = adjacent_title_pattern_debug(title, combined_patterns)
    adjacent_debug = adjacent_title_pattern_debug(title, TITLE_RESCUE_ADJACENT_TITLE_MARKERS_CFG)
    analyst_debug = adjacent_title_pattern_debug(title, TITLE_RESCUE_ANALYST_VARIANT_MARKERS_CFG)
    hits = unique_preserve((combined_debug.get("patterns_hit", []) or []))
    matched_pattern = clean(str(combined_debug.get("matched_pattern", "")))
    matched_token_slice = clean(str(combined_debug.get("matched_token_slice", "")))
    if not matched_pattern and hits:
        matched_pattern = hits[0]
    if not matched_token_slice:
        slices = combined_debug.get("matched_token_slices", []) or []
        if slices:
            matched_token_slice = clean(str(slices[0]))
    return {
        "matched": bool(combined_debug.get("matched", False) or matched_pattern),
        "matched_pattern": matched_pattern,
        "matched_token_slice": matched_token_slice,
        "normalized_title": clean(str(combined_debug.get("normalized_title", ""))),
        "title_tokens": unique_preserve(combined_debug.get("title_tokens", []) or []),
        "ignored_tokens": unique_preserve(combined_debug.get("ignored_tokens", []) or []),
        "patterns_checked": unique_preserve(combined_debug.get("patterns_checked", []) or []),
        "pattern_tokens_debug": unique_preserve(combined_debug.get("pattern_tokens_debug", []) or []),
        "patterns_hit": hits,
        "match_attempts": unique_preserve(combined_debug.get("match_attempts", []) or []),
        "adjacent_patterns_hit": unique_preserve(adjacent_debug.get("patterns_hit", []) or []),
        "analyst_patterns_hit": unique_preserve(analyst_debug.get("patterns_hit", []) or []),
    }

def job_id(company: str, title: str, url: str) -> str:
    base = f"{company.lower()}|{normalize_title(title)}|{canonicalize(url).lower()}"
    return hashlib.sha256(base.encode()).hexdigest()[:24]

def classify_role_family(title: str) -> str:
    t = normalize_title(title)
    if "architect" in t:
        return "Architect"
    if "technical product manager" in t or "product manager" in t or "product lead" in t or "product owner" in t:
        return "Product"
    if "program manager" in t or "technical program manager" in t:
        return "Program"
    if "business systems analyst" in t or "business analyst" in t or "bsa" in t or "systems analyst" in t:
        return "BSA"
    if "consultant" in t:
        return "Consulting"
    if "specialist" in t:
        return "Specialist"
    if "analyst" in t:
        return "Analyst"
    return "Other"


def role_alignment(title: str) -> Tuple[float, str, str]:
    t = normalize_title(title)
    bonus = 0
    reasons: List[str] = []

    for phrase in PREFERRED_ROLE_TITLE_PHRASES:
        if phrase in t:
            if "architect" in phrase:
                bonus += 18
                reasons.append("architect_phrase")
            elif "product manager" in phrase:
                bonus += 14
                reasons.append("product_phrase")
            elif "program manager" in phrase:
                bonus += 10
                reasons.append("program_phrase")
            elif "analyst" in phrase:
                bonus += 10
                reasons.append("analyst_phrase")
            else:
                bonus += 8
                reasons.append("preferred_phrase")
            break

    marker_hits = sum(1 for marker in PREFERRED_ROLE_MARKERS if marker in t)
    if marker_hits:
        bonus += min(marker_hits * 3, 12)
        reasons.append(f"preferred_markers:{marker_hits}")

    if any(marker in t for marker in NEUTRAL_SERVICE_ROLE_MARKERS):
        reasons.append("service_role_neutral")

    generic_analyst = "analyst" in t and not any(marker in t for marker in ALLOWED_ANALYST_TITLE_MARKERS)
    if generic_analyst:
        reasons.append("generic_analyst_neutral")

    alignment = float(max(0, min(25, bonus)))
    if alignment >= 15:
        label = "core_target"
    elif alignment >= 6:
        label = "good_target"
    else:
        label = "adjacent"

    return alignment, label, "; ".join(reasons)


def high_fit_fintech_lane_score(title: str, description: str) -> Tuple[float, str, str]:
    t = normalize_title(title)
    d = clean(description).lower()
    blob = f"{t} {d}"
    bonus = 0
    reasons: List[str] = []

    for phrase, pts in HIGH_FIT_FINTECH_PHRASES:
        if _kw_match(phrase, blob):
            bonus += pts
            reasons.append(f"lane_phrase:{phrase}")

    title_hits = [marker for marker in HIGH_FIT_FINTECH_TITLE_MARKERS if marker in t]
    if title_hits:
        bonus += min(len(title_hits) * 2, 6)
        reasons.append(f"lane_title_markers:{len(title_hits)}")

    domain_hits = [marker for marker in HIGH_FIT_FINTECH_DOMAIN_MARKERS if _kw_match(marker, blob)]
    if domain_hits:
        bonus += min(len(domain_hits), 8)
        reasons.append(f"lane_domain_markers:{min(len(domain_hits), 8)}")

    if title_hits and domain_hits:
        combo_bonus = 4
        if any(pm in t for pm in ["product manager", "technical product manager", "product lead"]):
            combo_bonus += 2
        if any(fin in blob for fin in ["payments", "money movement", "ledger", "financial reporting", "capital platform", "financial platform"]):
            combo_bonus += 2
        bonus += min(combo_bonus, 8)
        reasons.append("lane_title_domain_combo")

    for title_markers, domain_markers, pts, label in HIGH_FIT_TITLE_DOMAIN_SYNERGIES:
        if any(marker in t for marker in title_markers) and any(_kw_match(marker, blob) for marker in domain_markers):
            bonus += pts
            reasons.append(label)

    bonus = float(max(0, min(HIGH_FIT_DOMAIN_BONUS_CAP, bonus)))
    if bonus >= 18:
        label = "high_fit_fintech"
    elif bonus >= 10:
        label = "good_fit_fintech"
    elif bonus >= 4:
        label = "adjacent_fintech"
    else:
        label = "generic"

    reason_str = "; ".join(unique_preserve(reasons[:8]))
    return bonus, label, reason_str


CORE_LANE_TITLE_MARKERS = [
    "technical product manager", "product manager", "product lead", "product owner",
    "architect", "solution architect", "product architect", "integration architect",
    "data architect", "solutions architect", "platform", "api", "integration",
    "business analyst", "systems analyst", "application manager", "integrations manager",
]

SERVICES_LANE_TITLE_MARKERS = [
    "consultant", "implementation", "migration", "field services",
    "enablement", "professional services", "solution consultant", "solutions consultant",
]

ADJACENT_LANE_TITLE_MARKERS = [
    "analyst", "specialist", "advisor", "strategist", "manager",
]

BENCHMARK_LANE_PROFILES: Dict[str, Dict[str, Any]] = {
    "core": {
        "max_bonus": 12,
        "phrase_points": [
            ("technical product manager", 5),
            ("senior technical product manager", 6),
            ("product architect", 6),
            ("financial platform", 6),
            ("capital platform", 6),
            ("financial reporting", 6),
            ("money movement", 6),
            ("card ledgers", 6),
            ("payments", 4),
            ("payment", 3),
            ("api integrations", 5),
            ("apis & integrations", 6),
            ("data platform", 5),
            ("portfolio workflows", 6),
            ("financial advisor", 4),
            ("advisor workflows", 5),
            ("investment solutions", 5),
            ("trading systems", 5),
            ("aladdin", 6),
            ("separately managed accounts", 6),
            ("billing automation", 5),
            ("data & integrations", 5),
        ],
        "title_markers": [
            "technical product manager", "product manager", "product lead",
            "product architect", "architect", "solution architect",
            "platform", "api", "integration", "integrations",
            "application manager", "business analyst", "systems analyst",
        ],
        "domain_markers": [
            "financial platform", "capital platform", "financial reporting",
            "payments", "money movement", "ledger", "ledgers",
            "api", "apis", "integration", "integrations",
            "data platform", "portfolio", "advisor", "wealth management",
            "investment solution", "portfolio workflows", "trading systems",
            "reconciliation", "back office", "custody", "brokerage",
            "tax lots", "private markets", "alternative investments",
        ],
    },
    "adjacent": {
        "max_bonus": 9,
        "phrase_points": [
            ("solutions consultant", 5),
            ("solution architect", 5),
            ("solutions architect", 5),
            ("integration architect", 5),
            ("implementation", 3),
            ("data migration", 4),
            ("business transformation", 5),
            ("advisor platform", 5),
            ("data & integrations", 5),
            ("professional services", 4),
            ("trading systems", 4),
            ("investment solutions", 4),
        ],
        "title_markers": [
            "solutions consultant", "solution consultant", "consultant",
            "solutions architect", "solution architect", "architect",
            "implementation", "integration", "integrations",
            "data migration", "application manager", "business transformation",
            "business analyst", "systems analyst",
        ],
        "domain_markers": [
            "financial systems", "trading systems", "advisor platform",
            "wealth management", "investment solutions", "api", "integration",
            "data platform", "portfolio", "implementation", "migration",
            "reconciliation", "reporting", "back office", "custody",
        ],
    },
    "exploratory": {
        "max_bonus": 5,
        "phrase_points": [
            ("healthcare data", 4),
            ("developer tools", 4),
            ("modern data stack", 3),
            ("reverse etl", 4),
            ("cloud database", 3),
            ("startup platform", 3),
            ("billing automation", 3),
        ],
        "title_markers": [
            "technical product manager", "product manager", "platform",
            "api", "integration", "integrations", "solutions architect",
            "consultant",
        ],
        "domain_markers": [
            "healthcare", "developer tools", "modern data stack",
            "reverse etl", "cloud", "database", "startup",
            "billing", "data platform",
        ],
    },
}

BENCHMARK_LANE_LABELS = {
    "core": "core_history_match",
    "adjacent": "adjacent_history_match",
    "exploratory": "exploratory_history_match",
    "": "",
}


def benchmark_lane_match(title: str, description: str) -> Tuple[float, str, str]:
    t = normalize_title(title)
    d = clean(description).lower()
    blob = f"{t} {d}"

    best_lane = ""
    best_score = 0.0
    best_reasons: List[str] = []

    for lane_name, profile in BENCHMARK_LANE_PROFILES.items():
        lane_score = 0
        reasons: List[str] = []

        phrase_hits = []
        for phrase, pts in profile["phrase_points"]:
            if _kw_match(phrase, blob):
                lane_score += pts
                phrase_hits.append(phrase)
        if phrase_hits:
            reasons.append(f"phrase_hits:{','.join(phrase_hits[:4])}")

        title_hits = [marker for marker in profile["title_markers"] if marker in t]
        if title_hits:
            lane_score += min(len(title_hits) * 2, 6)
            reasons.append(f"title_hits:{len(title_hits)}")

        domain_hits = [marker for marker in profile["domain_markers"] if _kw_match(marker, blob)]
        if domain_hits:
            lane_score += min(len(domain_hits), 5)
            reasons.append(f"domain_hits:{len(domain_hits)}")

        if title_hits and domain_hits:
            lane_score += 3
            reasons.append("title_domain_combo")

        lane_score = float(max(0, min(profile["max_bonus"], lane_score)))
        if lane_score > best_score:
            best_lane = lane_name
            best_score = lane_score
            best_reasons = reasons

    if best_lane == "core" and best_score < 7:
        return 0.0, "", ""
    if best_lane == "adjacent" and best_score < 6:
        return 0.0, "", ""
    if best_lane == "exploratory" and best_score < 5:
        return 0.0, "", ""

    return best_score, BENCHMARK_LANE_LABELS.get(best_lane, ""), "; ".join(best_reasons[:5])


def lane_cohort_profile(
    title: str,
    role_family: str,
    role_alignment_score: float,
    role_alignment_label: str,
    lane_fit_score: float,
    lane_fit_label: str,
    benchmark_lane: str = "",
    benchmark_match_score: float = 0.0,
) -> Tuple[str, int]:
    t = normalize_title(title)
    core_title = any(marker in t for marker in CORE_LANE_TITLE_MARKERS)
    services_title = any(marker in t for marker in SERVICES_LANE_TITLE_MARKERS)
    adjacent_title = any(marker in t for marker in ADJACENT_LANE_TITLE_MARKERS)

    strong_lane = lane_fit_score >= 14 or lane_fit_label == "high_fit_fintech"
    good_lane = lane_fit_score >= 8 or lane_fit_label in {"good_fit_fintech", "high_fit_fintech"}
    strong_role = role_alignment_score >= 14 or role_alignment_label == "core_target"
    good_role = role_alignment_score >= 8 or role_alignment_label in {"core_target", "good_target"}
    core_history = benchmark_lane == "core_history_match" and benchmark_match_score >= 7
    adjacent_history = benchmark_lane == "adjacent_history_match" and benchmark_match_score >= 6
    exploratory_history = benchmark_lane == "exploratory_history_match" and benchmark_match_score >= 5

    if (core_title and (good_lane or good_role or core_history)) or (strong_lane and not services_title) or core_history:
        return "core_fintech_platform", 0
    if services_title and adjacent_history:
        return "adjacent_fintech", 1
    if services_title:
        return "services_implementation", 2
    if adjacent_history or exploratory_history:
        return "adjacent_fintech", 1
    if good_lane or good_role or adjacent_title or role_family in {"Architect", "Product", "Program", "BSA", "Analyst"}:
        return "adjacent_fintech", 1
    return "adjacent_fintech", 1


def lane_cohort_rank(label: str) -> int:
    ranks = {
        "core_fintech_platform": 0,
        "adjacent_fintech": 1,
        "services_implementation": 2,
        "manual_target": 3,
        "": 9,
    }
    return ranks.get(label, 9)


def display_sort_key(job: Job) -> Tuple[Any, ...]:
    salary_basis = salary_basis_value(job.salary_low, job.salary_high, job.salary_period)
    location_rank = 0 if (job.is_remote and not job.is_non_us) else 1 if (job.is_hybrid and is_local_commute_location(job.location)) else 2
    return (
        lane_cohort_rank(getattr(job, "lane_cohort", "")),
        location_rank,
        -1 if salary_basis is None else -salary_basis,
        bucket_rank(job.action_bucket),
        0 if job.is_new else 1,
        -getattr(job, "benchmark_match_score", 0.0),
        -job.source_trust_adjustment,
        -job.lane_fit_score,
        -job.score,
        job.company.lower(),
        job.title.lower(),
    )


def _kw_match(kw: str, blob: str) -> bool:
    """
    Boundary-aware keyword matching.

    Uses word boundaries for normal alphanumeric terms and non-word lookarounds
    for odd tech-stack tokens such as .NET or C++.
    """
    if not kw or not blob:
        return False

    keyword = clean(kw).lower().strip()
    haystack = clean(blob).lower()

    if not keyword or not haystack:
        return False

    escaped = re.escape(keyword).replace(r"\ ", r"\s+")

    if re.match(r"^\w", keyword) and re.search(r"\w$", keyword):
        pattern = rf"\b{escaped}\b"
    else:
        pattern = rf"(?<!\w){escaped}(?!\w)"

    return re.search(pattern, haystack, flags=re.IGNORECASE) is not None



SALARY_PATTERNS = [
    re.compile(
        r'(?P<currency>\$|USD|CAD)\s?(?P<low>[\d,]+)\s*(?:-|to|–)\s*(?P<currency2>\$|USD|CAD)?\s?(?P<high>[\d,]+)',
        re.I,
    ),
    re.compile(
        r'(?P<currency>\$|USD|CAD)\s?(?P<low>[\d,]+(?:\.\d+)?)\s*[kK]\s*(?:-|to|–)\s*(?P<currency2>\$|USD|CAD)?\s?(?P<high>[\d,]+(?:\.\d+)?)\s*[kK]',
        re.I,
    ),
]

HOURLY_MARKERS = ["/hr", "per hour", "an hour", "hourly", "/hour"]

def _parse_salary_number(raw: str, has_k: bool) -> Optional[float]:
    try:
        value = float(raw.replace(",", "").strip())
        if has_k and value < 1000:
            value *= 1000
        return value
    except Exception:
        return None


def extract_salary_info(text: str) -> Dict[str, Any]:
    blob = clean(text)
    for pattern in SALARY_PATTERNS:
        m = pattern.search(blob)
        if not m:
            continue

        matched_text = m.group(0)
        has_k = "k" in matched_text.lower()
        currency = (m.group("currency") or m.group("currency2") or "").upper().replace("$", "USD")
        low_raw = m.group("low")
        high_raw = m.group("high")
        low = _parse_salary_number(low_raw, has_k)
        high = _parse_salary_number(high_raw, has_k)

        window = blob[max(0, m.start() - 30): min(len(blob), m.end() + 30)].lower()
        period = "hourly" if any(marker in window for marker in HOURLY_MARKERS) else "annual"

        if has_k:
            salary_range = f"{currency} {low_raw}k - {high_raw}k".strip()
        else:
            salary_range = f"{currency} {low_raw} - {high_raw}".strip()

        return {
            "salary_range": salary_range,
            "salary_currency": currency,
            "salary_low": low,
            "salary_high": high,
            "salary_period": period,
        }

    return {
        "salary_range": "",
        "salary_currency": "",
        "salary_low": None,
        "salary_high": None,
        "salary_period": "",
    }

def extract_salary(text: str) -> Tuple[str, str]:
    info = extract_salary_info(text)
    return info["salary_range"], info["salary_currency"]

def annualize_salary(value: Optional[float], period: str) -> Optional[float]:
    if value is None:
        return None
    if period == "hourly":
        return value * 2080
    return value

def salary_basis_value(low: Optional[float], high: Optional[float], period: str) -> Optional[float]:
    low_annual = annualize_salary(low, period)
    high_annual = annualize_salary(high, period)
    basis = SALARY_FLOOR_BASIS.lower().strip()

    if basis == "high_end":
        return high_annual if high_annual is not None else low_annual
    if basis == "midpoint":
        if low_annual is not None and high_annual is not None:
            return (low_annual + high_annual) / 2.0
        return low_annual if low_annual is not None else high_annual
    return low_annual if low_annual is not None else high_annual

def detect_domains(blob: str) -> Tuple[List[str], List[str]]:
    apply_hits = []
    skip_hits = []

    for label, kws in APPLY_DOMAINS.items():
        if any(_kw_match(kw, blob) for kw in kws):
            apply_hits.append(label)

    for label, kws in SKIP_DOMAINS.items():
        if any(_kw_match(kw, blob) for kw in kws):
            skip_hits.append(label)

    return apply_hits, skip_hits

def dominant_apply_domain(apply_hits: List[str]) -> str:
    return apply_hits[0] if apply_hits else ""

def _phrase_occurrence_count(phrase: str, blob: str) -> int:
    phrase = clean(phrase).lower()
    blob = clean(blob).lower()
    if not phrase or not blob:
        return 0
    exact = {
        "api", "apis", "etl", "crm", "sre", "bsa", "sql", "aws", "gcp", "ml",
        "pm", "po", "ux", "ui", "nfs", "dtcc", "fis", "ach", "sox", "kyc", "aml",
        "bnpl", "ewa", "wam", "api integration", "data integration",
    }
    if phrase in exact:
        return len(re.findall(r"\b" + re.escape(phrase) + r"\b", blob))
    return blob.count(phrase)


def excluded_role_lane(title: str, description: str = "") -> str:
    title_blob = normalize_title(title)
    body_blob = clean(description).lower()

    for kw in ROLE_LANE_EXCLUDE_TITLE_ONLY:
        if _kw_match(kw, title_blob):
            return kw

    for kw in ROLE_LANE_EXCLUDE_BODY_STRICT:
        if _kw_match(kw, title_blob):
            return kw

        body_count = _phrase_occurrence_count(kw, body_blob)
        if body_count >= max(1, ROLE_LANE_EXCLUDE_BODY_MIN_OCCURRENCES):
            return kw

        role_markers = ROLE_LANE_EXCLUDE_BODY_ROLE_EVIDENCE.get(kw, [])
        if body_count > 0 and role_markers and any(_kw_match(marker, title_blob) for marker in role_markers):
            return kw

    return ""

def should_use_context_fallback(title: str) -> bool:
    t = normalize_title(title)
    return any(marker in t for marker in CONTEXT_FALLBACK_TITLE_MARKERS)

def location_flags(location: str) -> Tuple[bool, bool, bool]:
    loc_l = clean(location).lower()
    is_remote = any(re.search(pattern, loc_l) for pattern in REMOTE_MARKER_PATTERNS)
    is_hybrid = any(k in loc_l for k in HYBRID_MARKERS)
    is_non_us = any(k in loc_l for k in NON_US_MARKERS)
    # If the job is remote AND explicitly mentions "United States" or "USA", treat it as a
    # US-available role even when other countries (Canada, etc.) also appear in the string.
    # e.g. "remote within Canada or United States" should not be rejected as non-US.
    if is_non_us and is_remote:
        if re.search(r"\bunited states\b|\bu\.?s\.?a?\.?\b", loc_l):
            is_non_us = False
    return is_remote, is_hybrid, is_non_us


def is_local_commute_location(location: str) -> bool:
    loc_l = clean(location).lower()
    if not loc_l:
        return False
    return any(marker in loc_l for marker in LOCAL_COMMUTE_MARKERS)


def location_policy_reason(
    is_remote: bool,
    is_hybrid: bool,
    is_non_us: bool,
    location: str = "",
    salary_low: Optional[float] = None,
    salary_high: Optional[float] = None,
    salary_period: str = "",
    salary_currency: str = "",
) -> str:
    if US_ONLY and is_non_us:
        return "non_us_location"

    mode = LOCATION_POLICY.lower().strip()
    salary_basis = salary_basis_value(salary_low, salary_high, salary_period)
    local_commute = is_local_commute_location(location)

    # Blank location = ATS didn't populate the field; pass through to scoring
    # rather than auto-rejecting. Score will be lower (no remote bonus) but
    # the job stays visible for the user to judge.
    if not location.strip() and mode != "any":
        return ""

    if mode == "remote_only":
        if is_remote:
            return ""
        if ALLOW_LOCAL_HYBRID_HIGH_COMP and is_hybrid and local_commute:
            if salary_currency and salary_currency != "USD":
                return f"hybrid_local_salary_currency_not_supported:{salary_currency}"
            if salary_basis is None:
                return "hybrid_local_missing_salary"
            if salary_basis < LOCAL_HYBRID_MIN_SALARY_USD:
                return f"hybrid_local_salary_below_floor:{int(round(salary_basis))}<{int(LOCAL_HYBRID_MIN_SALARY_USD)}"
            return ""
        return "not_remote"

    if mode == "remote_or_hybrid":
        if is_remote:
            return ""
        if is_hybrid:
            if not local_commute:
                return "hybrid_outside_commute"
            if salary_currency and salary_currency != "USD":
                return f"hybrid_local_salary_currency_not_supported:{salary_currency}"
            if salary_basis is None:
                return "hybrid_local_missing_salary"
            if salary_basis < LOCAL_HYBRID_MIN_SALARY_USD:
                return f"hybrid_local_salary_below_floor:{int(round(salary_basis))}<{int(LOCAL_HYBRID_MIN_SALARY_USD)}"
            return ""
        return "not_remote_or_hybrid"

    if mode == "in_office":
        if is_remote:
            return "not_in_office"
        if is_hybrid:
            if not local_commute:
                return "in_office_outside_commute"
            return ""
        return ""   # fully in-office jobs pass

    return ""  # mode == "any" — no location filtering


def location_is_eligible(
    is_remote: bool,
    is_hybrid: bool,
    is_non_us: bool,
    location: str = "",
    salary_low: Optional[float] = None,
    salary_high: Optional[float] = None,
    salary_period: str = "",
    salary_currency: str = "",
) -> bool:
    return location_policy_reason(
        is_remote,
        is_hybrid,
        is_non_us,
        location=location,
        salary_low=salary_low,
        salary_high=salary_high,
        salary_period=salary_period,
        salary_currency=salary_currency,
    ) == ""

def source_trust_profile(source: str, url: str = "", company_domain: str = "") -> Tuple[str, float, int]:
    src_l = clean(source).lower()
    host = (urlparse(url).netloc or "").lower()
    company_domain = clean(company_domain).lower()
    combined = f"{src_l} {host}"

    if "efinancialcareers" in combined:
        return "external_finance_board", -1.5, 3
    if "wellfound" in combined:
        return "external_startup_board", -4.0, 4
    if "builtin" in combined:
        return "external_aggregator", -3.5, 4
    if "welcometothejungle" in combined:
        return "external_aggregator", -3.0, 4

    if any(token in combined for token in ["greenhouse", "lever", "ashby", "smartrecruiters", "myworkdayjobs", "workday"]):
        return "official_ats", 4.0, 1

    if company_domain and host and (host == company_domain or host.endswith("." + company_domain) or company_domain.endswith("." + host)):
        return "official_company_site", 2.5, 2

    if host and is_known_job_host(host):
        return "third_party_ats", 2.0, 2

    return "web_source", 0.0, 5


def should_rescue_no_apply_domain(title: str, description: str) -> Tuple[bool, str]:
    if not FLEXIBLE_INDUSTRY_IF_SKILLS_ALIGN:
        return False, ""
    norm_title = normalize_title(title)
    desc_l = clean(description).lower()
    role_score, _, _ = role_alignment(norm_title)
    lane_score, lane_label, _ = high_fit_fintech_lane_score(norm_title, desc_l)
    benchmark_score, benchmark_lane, _ = benchmark_lane_match(norm_title, desc_l)

    strong_history = benchmark_lane in {"core_history_match", "adjacent_history_match"} and benchmark_score >= 6
    strong_title = role_score >= 14 and any(
        marker in norm_title
        for marker in ["product manager", "technical product", "architect", "integration", "platform", "api", "solutions", "consultant", "analyst", "manager"]
    )
    strong_lane = lane_score >= 10
    if strong_history and (strong_title or strong_lane):
        return True, "cross_industry_skill_alignment"
    if strong_title and lane_score >= 8:
        return True, f"cross_industry_skill_alignment:{lane_label or 'lane_fit'}"
    return False, ""


def apply_preference_adjustments(
    scored: Dict[str, Any],
    location: str,
    salary_low: Optional[float],
    salary_high: Optional[float],
    salary_period: str,
    salary_currency: str,
) -> Dict[str, Any]:
    score = float(scored.get("score", 0.0))
    reasons: List[str] = []
    salary_basis = salary_basis_value(salary_low, salary_high, salary_period)
    local_commute = is_local_commute_location(location)

    if scored.get("is_remote") and not scored.get("is_non_us"):
        score += REMOTE_US_SCORE_BONUS
        reasons.append("priority_remote_us")
    elif scored.get("is_hybrid") and local_commute:
        score += LOCAL_HYBRID_SCORE_BONUS
        reasons.append("priority_local_hybrid")

    if salary_basis is None:
        score -= MISSING_SALARY_SCORE_PENALTY
        reasons.append("salary_missing")
    elif salary_currency and salary_currency != "USD":
        score -= 4
        reasons.append(f"salary_currency:{salary_currency}")
    else:
        if salary_basis >= max(MIN_SALARY_USD, PREFERRED_REMOTE_MIN_SALARY_USD):
            score += SALARY_ABOVE_TARGET_BONUS
            reasons.append("salary_at_or_above_target")
        elif salary_basis >= MIN_SALARY_USD:
            score += 2
            reasons.append("salary_meets_floor")
        else:
            score -= 12
            reasons.append("salary_below_target")

    decision_score = max(0.0, min(100.0, score))
    scored["decision_score"] = decision_score
    scored["score"] = decision_score
    scored["fit_band"] = fit_band(decision_score)
    scored["preference_adjustments"] = ", ".join(reasons)
    scored["local_commute"] = local_commute
    scored["salary_basis_value"] = salary_basis
    return scored


def fit_band(score: float) -> str:
    return "A" if score >= 80 else "B" if score >= 60 else "C" if score >= 40 else "D"

def bucket_rank(bucket: str) -> int:
    return ACTION_BUCKET_RANKS.get(bucket, 9)


def qualifies_for_apply_now(role_alignment_score: float, title: str) -> bool:
    if not REQUIRE_STRONG_TITLE_FOR_APPLY_NOW:
        return True
    t = normalize_title(title)
    if role_alignment_score >= APPLY_NOW_MIN_ROLE_ALIGNMENT:
        return True
    return any(marker in t for marker in APPLY_NOW_DIRECT_TITLE_MARKERS)


def _action_bucket_rule_matches(rule_when: Dict[str, Any], state: Dict[str, Any]) -> bool:
    if not isinstance(rule_when, dict):
        return False

    exact_fields = [
        "manual_review",
        "eligible",
        "strong_title",
        "known_salary_for_apply",
        "is_remote",
        "is_hybrid",
        "is_non_us",
    ]
    for field in exact_fields:
        if field in rule_when and bool(state.get(field)) != bool(rule_when.get(field)):
            return False

    numeric_min_map = {
        "min_score": "score",
        "min_role_alignment": "role_alignment_score",
        "min_tier": "tier",
    }
    for rule_key, state_key in numeric_min_map.items():
        if rule_key in rule_when and float(state.get(state_key, 0.0)) < float(rule_when.get(rule_key, 0.0)):
            return False

    numeric_max_map = {
        "max_score": "score",
        "max_role_alignment": "role_alignment_score",
        "max_tier": "tier",
    }
    for rule_key, state_key in numeric_max_map.items():
        if rule_key in rule_when and float(state.get(state_key, 0.0)) > float(rule_when.get(rule_key, 0.0)):
            return False

    if "tier_in" in rule_when:
        allowed = {int(x) for x in (rule_when.get("tier_in") or [])}
        if int(state.get("tier", 0)) not in allowed:
            return False

    if "tier_not_in" in rule_when:
        blocked = {int(x) for x in (rule_when.get("tier_not_in") or [])}
        if int(state.get("tier", 0)) in blocked:
            return False

    return True


def action_bucket(
    score: float,
    is_remote: bool,
    is_hybrid: bool,
    is_non_us: bool,
    manual_review: bool,
    tier: int,
    role_alignment_score: float,
    title: str,
    location: str = "",
    salary_low: Optional[float] = None,
    salary_high: Optional[float] = None,
    salary_period: str = "",
    salary_currency: str = "",
) -> str:
    eligible = location_is_eligible(
        is_remote,
        is_hybrid,
        is_non_us,
        location=location,
        salary_low=salary_low,
        salary_high=salary_high,
        salary_period=salary_period,
        salary_currency=salary_currency,
    )
    strong_title = qualifies_for_apply_now(role_alignment_score, title)
    salary_basis = salary_basis_value(salary_low, salary_high, salary_period)
    known_salary_for_apply = salary_basis is not None and (not salary_currency or salary_currency == "USD")

    state = {
        "score": float(score),
        "is_remote": bool(is_remote),
        "is_hybrid": bool(is_hybrid),
        "is_non_us": bool(is_non_us),
        "manual_review": bool(manual_review),
        "tier": int(tier),
        "role_alignment_score": float(role_alignment_score),
        "eligible": bool(eligible),
        "strong_title": bool(strong_title),
        "known_salary_for_apply": bool(known_salary_for_apply),
    }

    for rule in ACTION_BUCKET_RULES:
        label = str(rule.get("label") or "").strip()
        when = rule.get("when") or {}
        if not label:
            continue
        if _action_bucket_rule_matches(when, state):
            return label

    return "IGNORE"


def matched_title_gate_keywords(title: str) -> List[str]:
    t = normalize_title(title)
    return [kw for kw in TITLE_MUST_INCLUDE_ONE if kw in t]


def matched_title_positive_weight_terms(title: str) -> List[str]:
    t = normalize_title(title)
    return [phrase for phrase, _pts in TITLE_POSITIVE_WEIGHTS if _kw_match(phrase, t)]


def matched_positive_body_terms(title: str, description: str) -> List[str]:
    blob = f"{normalize_title(title)} {clean(description).lower()}"
    seen: set[str] = set()
    terms: List[str] = []
    for kw, _pts in POSITIVE_KEYWORDS:
        if not _kw_match(kw, blob):
            continue
        if COUNT_UNIQUE_KEYWORD_MATCHES_ONLY and kw in seen:
            continue
        seen.add(kw)
        terms.append(kw)
    return terms


def positive_body_points_from_terms(terms: List[str]) -> int:
    weight_map = {kw: int(pts) for kw, pts in POSITIVE_KEYWORDS}
    points = sum(weight_map.get(term, 0) for term in terms)
    if POSITIVE_KEYWORD_CAP >= 0:
        points = min(points, POSITIVE_KEYWORD_CAP)
    return points


def title_passes(title: str) -> Tuple[bool, str]:
    t = normalize_title(title)

    for kw in TITLE_HARD_EXCLUDE:
        if kw in t:
            return False, f"excluded:{kw}"

    if TITLE_REQUIRE_ONE_POSITIVE_KEYWORD and not any(k in t for k in TITLE_MUST_INCLUDE_ONE):
        return False, "no_target_signal"

    if BSA_REQUIRES_MODIFIER and "business analyst" in t and not any(m in t for m in BSA_ALLOWED_MODIFIERS):
        return False, "generic_ba"

    if PM_REQUIRES_MODIFIER and ("product manager" in t or "product management" in t):
        if not any(m in t for m in PM_ALLOWED_MODIFIERS):
            return False, "generic_pm_no_modifier"

    return True, ""


def should_soft_rescue_title(
    title: str,
    description: str,
    apply_hits: List[str],
    skip_hits: List[str],
    fail_reason: str,
) -> Dict[str, Any]:
    canonical_adjacent = canonical_adjacent_match_result(title)
    normalized_adjacent_title = canonical_adjacent["normalized_title"]
    title_tokens = canonical_adjacent["title_tokens"]
    adjacent_title_ignored_tokens = canonical_adjacent["ignored_tokens"]

    result = {
        "rescued": False,
        "bucket": "",
        "trigger": "",
        "adjacent_domain_terms_hit": [],
        "strong_domain": False,
        "positive_body_terms": [],
        "positive_body_points": 0,
        "normalized_title_for_adjacent_match": normalized_adjacent_title,
        "adjacent_title_patterns_checked": canonical_adjacent["patterns_checked"],
        "adjacent_title_patterns_hit": canonical_adjacent["patterns_hit"],
        "adjacent_title_matched_token_slice": [canonical_adjacent["matched_token_slice"]] if canonical_adjacent["matched_token_slice"] else [],
        "adjacent_title_tokens_debug": " | ".join(canonical_adjacent["title_tokens"]),
        "adjacent_pattern_tokens_debug": " || ".join(canonical_adjacent["pattern_tokens_debug"]),
        "adjacent_match_attempts": " || ".join(canonical_adjacent["match_attempts"]),
        "adjacent_title_marker_hit": [canonical_adjacent["matched_pattern"]] if canonical_adjacent["matched_pattern"] else [],
    }
    if fail_reason not in SOFT_TITLE_RESCUE_ALLOWED_FAILURES:
        return result
    t = normalize_title(title)
    d = clean(description).lower()
    blob = f"{t} {d}"
    if any(kw in t for kw in TITLE_HARD_EXCLUDE):
        return result
    if skip_hits and not apply_hits:
        return result

    role_signal = any(marker in t for marker in SOFT_TITLE_ROLE_MARKERS)
    domain_signal = any(marker in blob for marker in SOFT_TITLE_DOMAIN_MARKERS)
    strong_apply_domains = {
        "Investment Management", "Brokerage / Custody",
        "Financial Data / Reporting", "API Integration (Financial)",
        "Wealth Management Tech", "Alternative Investments",
        "Capital Markets", "Payments / Money Movement",
    }
    strong_apply = len(apply_hits) >= 2 or any(hit in strong_apply_domains for hit in apply_hits)

    positive_body_terms = matched_positive_body_terms(title, description)
    positive_body_points = positive_body_points_from_terms(positive_body_terms)
    adjacent_domain_terms_hit = [marker for marker in TITLE_RESCUE_STRONG_BODY_DOMAIN_MARKERS_CFG if marker in d]
    strong_body_domain = bool(adjacent_domain_terms_hit)
    strong_body = (
        len(positive_body_terms) >= max(1, TITLE_RESCUE_STRONG_BODY_MIN_POSITIVE_TERMS_CFG)
        and positive_body_points >= max(1, TITLE_RESCUE_STRONG_BODY_MIN_POSITIVE_POINTS_CFG)
    ) or strong_body_domain or strong_apply

    adjacent_marker_hits = canonical_adjacent.get("adjacent_patterns_hit", []) or []
    analyst_marker_hits = canonical_adjacent.get("analyst_patterns_hit", []) or []
    canonical_adjacent_hit = clean(str(canonical_adjacent.get("matched_pattern", "")))
    adjacent_signal = bool(canonical_adjacent.get("matched", False))
    analyst_adjacent_signal = bool(analyst_marker_hits)
    auto_rescue_pattern_set = _normalized_adjacent_pattern_set(tuple(TITLE_RESCUE_ADJACENT_TITLE_AUTO_RESCUE_PATTERNS_CFG))
    canonical_adjacent_candidates = unique_preserve(
        ([canonical_adjacent_hit] if canonical_adjacent_hit else [])
        + [clean(str(x)) for x in (canonical_adjacent.get("patterns_hit", []) or [])]
    )
    adjacent_auto_rescue_hit = next((p for p in canonical_adjacent_candidates if p in auto_rescue_pattern_set), "")
    auto_rescue_signal = bool(adjacent_auto_rescue_hit)

    if adjacent_signal or analyst_adjacent_signal:
        trigger_parts = []
        if auto_rescue_signal:
            trigger_parts.append("adjacent_auto_rescue")
        if adjacent_signal:
            trigger_parts.append("adjacent_title_marker")
        if analyst_adjacent_signal:
            trigger_parts.append("analyst_variant")
        if strong_body_domain:
            trigger_parts.append("strong_domain_terms")
        if strong_apply:
            trigger_parts.append("strong_apply_domain")
        if positive_body_points >= max(1, TITLE_RESCUE_STRONG_BODY_MIN_POSITIVE_POINTS_CFG):
            trigger_parts.append("positive_body_score")
        rescue_allowed = auto_rescue_signal or strong_body
        if rescue_allowed:
            result.update({
                "rescued": True,
                "bucket": "adjacent_title",
                "trigger": "+".join(unique_preserve(trigger_parts)) or "adjacent_title",
                "adjacent_domain_terms_hit": unique_preserve(adjacent_domain_terms_hit),
                "strong_domain": bool(strong_body_domain or strong_apply),
                "positive_body_terms": positive_body_terms,
                "positive_body_points": positive_body_points,
                "adjacent_title_marker_hit": [canonical_adjacent_hit] if canonical_adjacent_hit else unique_preserve(adjacent_marker_hits + analyst_marker_hits),
                "adjacent_title_patterns_hit": [canonical_adjacent_hit] if canonical_adjacent_hit else unique_preserve(adjacent_marker_hits + analyst_marker_hits),
                "adjacent_title_matched_token_slice": [clean(str(canonical_adjacent.get("matched_token_slice", "")))] if clean(str(canonical_adjacent.get("matched_token_slice", ""))) else [],
            })
            return result

    if role_signal and (domain_signal or strong_apply):
        result.update({
            "rescued": True,
            "bucket": "soft_title",
            "trigger": "role_signal+domain_or_apply",
            "strong_domain": bool(domain_signal or strong_apply),
            "adjacent_domain_terms_hit": unique_preserve(adjacent_domain_terms_hit),
            "positive_body_terms": positive_body_terms,
            "positive_body_points": positive_body_points,
            "adjacent_title_marker_hit": [canonical_adjacent_hit] if canonical_adjacent_hit else unique_preserve(adjacent_marker_hits + analyst_marker_hits),
        })
        return result

    result.update({
        "adjacent_domain_terms_hit": unique_preserve(adjacent_domain_terms_hit),
        "strong_domain": bool(strong_body_domain),
        "positive_body_terms": positive_body_terms,
        "positive_body_points": positive_body_points,
        "adjacent_title_marker_hit": unique_preserve(adjacent_marker_hits + analyst_marker_hits),
    })
    return result

def split_reason(reason: str) -> Tuple[str, str]:
    if not reason:
        return "", ""
    code, _, detail = reason.partition(":")
    return code.strip(), detail.strip()

def make_rejected_job(
    company: Company,
    title: str,
    location: str,
    url: str,
    source: str,
    description: str,
    posted_dt: Optional[datetime],
    drop_stage: str,
    drop_reason: str,
    evaluation: Optional[Dict[str, Any]] = None,
) -> RejectedJob:
    evaluation = evaluation or {}
    reason_code, reason_detail = split_reason(drop_reason)
    norm_title = evaluation.get("normalized_title") or normalize_title(title)
    salary_info = extract_salary_info(description)
    trust_label, trust_adjustment, _ = source_trust_profile(source, url, company.domain)
    lane_cohort, _ = lane_cohort_profile(
        norm_title,
        evaluation.get("role_family") or classify_role_family(norm_title),
        evaluation.get("role_alignment_score", 0.0),
        evaluation.get("role_alignment_label", ""),
        evaluation.get("lane_fit_score", 0.0),
        evaluation.get("lane_fit_label", ""),
        evaluation.get("benchmark_lane", ""),
        evaluation.get("benchmark_match_score", 0.0),
    )

    return RejectedJob(
        company=company.name,
        tier=company.tier,
        priority=company.priority,
        title=clean(title),
        normalized_title=norm_title,
        role_family=evaluation.get("role_family") or classify_role_family(norm_title),
        role_alignment_score=evaluation.get("role_alignment_score", 0.0),
        role_alignment_label=evaluation.get("role_alignment_label", ""),
        source_trust_label=evaluation.get("source_trust_label", trust_label),
        source_trust_adjustment=evaluation.get("source_trust_adjustment", trust_adjustment),
        lane_fit_score=evaluation.get("lane_fit_score", 0.0),
        lane_fit_label=evaluation.get("lane_fit_label", ""),
        benchmark_lane=evaluation.get("benchmark_lane", ""),
        benchmark_match_score=evaluation.get("benchmark_match_score", 0.0),
        benchmark_match_reason=evaluation.get("benchmark_match_reason", ""),
        lane_cohort=evaluation.get("lane_cohort", lane_cohort),
        location=clean(location),
        url=url,
        canonical_url=canonicalize(url),
        source=source,
        posted_at=posted_dt.isoformat() if posted_dt else "",
        age_days=evaluation.get("age_days", job_age(posted_dt)),
        drop_stage=drop_stage,
        drop_reason_code=reason_code,
        drop_reason_detail=reason_detail,
        drop_reason=drop_reason,
        title_gate_matched_keywords=evaluation.get("title_gate_matched_keywords", ""),
        title_rescue_bucket=evaluation.get("title_rescue_bucket", ""),
        title_rescue_trigger=evaluation.get("title_rescue_trigger", ""),
        adjacent_title_marker_hit=evaluation.get("adjacent_title_marker_hit", ""),
        adjacent_domain_terms_hit=evaluation.get("adjacent_domain_terms_hit", ""),
        normalized_title_for_adjacent_match=evaluation.get("normalized_title_for_adjacent_match", ""),
        adjacent_title_patterns_checked=evaluation.get("adjacent_title_patterns_checked", ""),
        adjacent_title_patterns_hit=evaluation.get("adjacent_title_patterns_hit", ""),
        adjacent_title_matched_token_slice=evaluation.get("adjacent_title_matched_token_slice", ""),
        adjacent_title_tokens_debug=evaluation.get("adjacent_title_tokens_debug", ""),
        adjacent_pattern_tokens_debug=evaluation.get("adjacent_pattern_tokens_debug", ""),
        adjacent_match_attempts=evaluation.get("adjacent_match_attempts", ""),
        decision_reason=evaluation.get("decision_reason", ""),
        score=evaluation.get("score"),
        decision_score=evaluation.get("decision_score", evaluation.get("score")),
        score_threshold_used=evaluation.get("score_threshold_used"),
        base_score_before_rescue=evaluation.get("base_score_before_rescue"),
        adjacent_rescue_bonus_applied=evaluation.get("adjacent_rescue_bonus_applied"),
        final_score_after_rescue=evaluation.get("final_score_after_rescue"),
        fit_band=evaluation.get("fit_band", ""),
        action_bucket=evaluation.get("action_bucket", ""),
        is_remote=evaluation.get("is_remote", False),
        is_hybrid=evaluation.get("is_hybrid", False),
        is_non_us=evaluation.get("is_non_us", False),
        apply_domains_hit=evaluation.get("apply_domains_hit", ""),
        skip_domains_hit=evaluation.get("skip_domains_hit", ""),
        primary_apply_domain=evaluation.get("primary_apply_domain", ""),
        matched_keywords=evaluation.get("matched_keywords", ""),
        penalized_keywords=evaluation.get("penalized_keywords", ""),
        salary_range=evaluation.get("salary_range", salary_info["salary_range"]),
        salary_currency=evaluation.get("salary_currency", salary_info["salary_currency"]),
        salary_low=evaluation.get("salary_low", salary_info["salary_low"]),
        salary_high=evaluation.get("salary_high", salary_info["salary_high"]),
        salary_period=evaluation.get("salary_period", salary_info["salary_period"]),
        description_excerpt=clean(description)[:600],
        company_notes=company.notes,
    )

def _unique_weighted_hits(blob: str, weighted_terms: List[Tuple[str, int]]) -> List[Tuple[str, int]]:
    """
    Unique weighted hits without duplicate inflation.
    """
    hits: List[Tuple[str, int]] = []
    seen: set[str] = set()

    for term, pts in weighted_terms:
        key = normalize_title(term)
        if key in seen:
            continue
        if _kw_match(term, blob):
            seen.add(key)
            hits.append((term, int(pts)))

    return hits


def _decision_reason_parts(
    title_fast_track_hits: List[Tuple[str, int]],
    title_weight_hits: List[Tuple[str, int]],
    jd_positive_hits: List[Tuple[str, int]],
    jd_negative_hits: List[Tuple[str, int]],
    *,
    title_points: float,
    jd_positive_points: float,
    jd_negative_points: float,
    jd_positive_multiplier: float,
    location_label: str = "",
    extra_parts: Optional[List[str]] = None,
) -> str:
    parts: List[str] = []

    if title_fast_track_hits:
        titles = ", ".join(term for term, _ in title_fast_track_hits[:3])
        parts.append(f"Title Fast-Track +{title_points:g} ({titles})")
    elif title_weight_hits:
        titles = ", ".join(term for term, _ in title_weight_hits[:4])
        parts.append(f"Title Match +{title_points:g} ({titles})")

    if jd_positive_hits:
        kws = ", ".join(term for term, _ in jd_positive_hits[:6])
        if jd_positive_multiplier != 1.0:
            parts.append(f"JD Keywords +{jd_positive_points:g} ({jd_positive_multiplier:.1f}x; {kws})")
        else:
            parts.append(f"JD Keywords +{jd_positive_points:g} ({kws})")

    if jd_negative_hits:
        kws = ", ".join(term for term, _ in jd_negative_hits[:6])
        parts.append(f"Negative JD -{jd_negative_points:g} ({kws})")

    if location_label:
        parts.append(f"Location: {location_label}")

    for part in extra_parts or []:
        if part:
            parts.append(part)

    return "; ".join(parts)


def _filtered_out_result(
    *,
    company: Company,
    title: str,
    location: str,
    description: str,
    posted_dt: Optional[datetime],
    salary_info: Dict[str, Any],
    role_family: str,
    normalized_title: str,
    drop_stage: str,
    drop_reason: str,
    reason_code: Optional[str] = None,
    matched_keywords: str = "",
    title_gate_matched_keywords: str = "",
) -> Dict[str, Any]:
    is_remote, is_hybrid, is_non_us = location_flags(location)

    return {
        "keep": False,
        "reason": reason_code or drop_reason,
        "drop_stage": drop_stage,
        "drop_reason": drop_reason,
        "score": 0.0,
        "decision_score": 0.0,
        "fit_band": "FILTERED",
        "action_bucket": "FILTERED_OUT",
        "decision_reason": f"{drop_stage}: {drop_reason}",
        "apply_domains_hit": "",
        "skip_domains_hit": "",
        "primary_apply_domain": "",
        "salary_range": salary_info["salary_range"],
        "salary_currency": salary_info["salary_currency"],
        "salary_low": salary_info["salary_low"],
        "salary_high": salary_info["salary_high"],
        "salary_period": salary_info["salary_period"],
        "is_remote": is_remote,
        "is_hybrid": is_hybrid,
        "is_non_us": is_non_us,
        "matched_keywords": matched_keywords,
        "penalized_keywords": "",
        "age_days": job_age(posted_dt),
        "role_family": role_family,
        "role_alignment_score": 0.0,
        "role_alignment_label": "",
        "lane_fit_score": 0.0,
        "lane_fit_label": "",
        "benchmark_lane": "",
        "benchmark_match_score": 0.0,
        "benchmark_match_reason": "",
        "lane_cohort": "",
        "source_trust_label": "",
        "source_trust_adjustment": 0.0,
        "normalized_title": normalized_title,
        "title_gate_matched_keywords": title_gate_matched_keywords,
        "title_rescue_bucket": "",
        "title_rescue_trigger": "",
        "adjacent_title_marker_hit": "",
        "adjacent_domain_terms_hit": "",
        "normalized_title_for_adjacent_match": "",
        "adjacent_title_patterns_checked": "",
        "adjacent_title_patterns_hit": "",
        "adjacent_title_matched_token_slice": "",
        "adjacent_title_tokens_debug": "",
        "adjacent_pattern_tokens_debug": "",
        "adjacent_match_attempts": "",
        "score_threshold_used": None,
        "base_score_before_rescue": None,
        "adjacent_rescue_bonus_applied": None,
        "final_score_after_rescue": None,
    }


def score_job(
    company: Company,
    title: str,
    location: str,
    description: str,
    posted_dt: Optional[datetime],
    is_manual: bool,
    apply_hits: List[str],
    skip_hits: List[str],
    source: str = "",
    url: str = "",
) -> Dict[str, Any]:
    """
    Tiered Funnel Scoring:
    - hard title fast-track for high-confidence titles
    - deep JD-only positive/negative keyword scan
    - anti-double-dipping multiplier on JD positives after fast-track
    - category caps to avoid runaway scoring
    """
    title_l = normalize_title(title)
    desc_l = clean(description).lower()
    job_blob = f"{title_l} {desc_l}"
    jd_blob = desc_l

    role_alignment_score, role_alignment_label, role_alignment_reason = role_alignment(title_l)
    lane_fit_score, lane_fit_label, lane_fit_reason = high_fit_fintech_lane_score(title_l, desc_l)
    benchmark_match_score, benchmark_lane, benchmark_match_reason = benchmark_lane_match(title_l, desc_l)
    source_trust_label, source_trust_adjustment, source_trust_rank = source_trust_profile(source, url, company.domain)

    score = 0.0

    # ---------- Title scoring ----------
    title_weight_hits = _unique_weighted_hits(title_l, TITLE_POSITIVE_WEIGHTS)
    max_title_weight = max((pts for _, pts in title_weight_hits), default=0)
    title_fast_track_hits: List[Tuple[str, int]] = []
    title_points = 0.0

    if max_title_weight >= TITLE_FAST_TRACK_MIN_WEIGHT:
        title_fast_track_hits = [(term, pts) for term, pts in title_weight_hits if pts == max_title_weight]
        title_points = float(TITLE_FAST_TRACK_BASE_SCORE)
    else:
        title_points = float(min(sum(pts for _, pts in title_weight_hits), 20))

    score += title_points

    # ---------- Existing structural signals ----------
    score += role_alignment_score
    score += lane_fit_score
    score += benchmark_match_score

    # ---------- JD-only keyword scoring ----------
    jd_positive_hits = _unique_weighted_hits(jd_blob, POSITIVE_KEYWORDS)
    jd_negative_hits = _unique_weighted_hits(jd_blob, NEGATIVE_KEYWORDS)

    jd_positive_multiplier = 0.5 if title_fast_track_hits else 1.0
    jd_positive_points_raw = float(sum(pts for _, pts in jd_positive_hits))
    jd_negative_points_raw = float(sum(pts for _, pts in jd_negative_hits))

    positive_cap = float(POSITIVE_KEYWORD_CAP) if POSITIVE_KEYWORD_CAP >= 0 else 999.0
    negative_cap = float(NEGATIVE_KEYWORD_CAP) if NEGATIVE_KEYWORD_CAP >= 0 else 999.0

    jd_positive_points = min(jd_positive_points_raw * jd_positive_multiplier, float(positive_cap))
    jd_negative_points = min(jd_negative_points_raw, float(negative_cap))

    score += jd_positive_points
    score -= jd_negative_points

    for phrase, pts in SWEET_SPOT_PHRASES:
        if _kw_match(phrase, job_blob):
            score += pts

    context_overlap = sum(1 for kw in company.industry if _kw_match(kw, jd_blob))
    score += min(context_overlap * 2, 6)

    if apply_hits:
        score += min(len(apply_hits) * 5, 20)
    if skip_hits:
        score -= min(len(skip_hits) * 8, 20)

    is_remote, is_hybrid, is_non_us = location_flags(location)
    if is_remote:
        score += 10
        location_label = "Remote"
    elif is_hybrid:
        score -= 8
        location_label = "Hybrid"
    elif is_non_us:
        score -= 40
        location_label = "Non-US"
    else:
        location_label = "Onsite/Unknown"

    tier_bonus = {1: 8, 2: 4, 3: 0}.get(company.tier, 0)
    score += tier_bonus

    days = job_age(posted_dt)
    if days is not None:
        if days <= 3:
            score += 12
        elif days <= 7:
            score += 8
        elif days <= 14:
            score += 4
        elif days <= MAX_JOB_AGE_DAYS:
            score += 1
        else:
            score -= 20

    if is_manual:
        score -= 15

    score += source_trust_adjustment
    if source_trust_rank <= 2 and lane_fit_score >= 18:
        score += 4
    elif source_trust_rank <= 2 and lane_fit_score >= 10:
        score += 3

    score = max(0.0, min(100.0, score))

    matched_terms = [(f"title:{term}", pts) for term, pts in title_weight_hits] + jd_positive_hits
    penalized_terms = jd_negative_hits

    matched_str = ", ".join(f"{k}(+{p})" for k, p in sorted(matched_terms, key=lambda x: -x[1])[:12])
    penalized_str = ", ".join(f"{k}(-{p})" for k, p in sorted(penalized_terms, key=lambda x: -x[1])[:8])

    decision_reason = _decision_reason_parts(
        title_fast_track_hits=title_fast_track_hits,
        title_weight_hits=title_weight_hits,
        jd_positive_hits=jd_positive_hits,
        jd_negative_hits=jd_negative_hits,
        title_points=title_points,
        jd_positive_points=jd_positive_points,
        jd_negative_points=jd_negative_points,
        jd_positive_multiplier=jd_positive_multiplier,
        location_label=location_label,
    )

    return {
        "score": score,
        "decision_score": score,
        "fit_band": fit_band(score),
        "age_days": days,
        "is_remote": is_remote,
        "is_hybrid": is_hybrid,
        "is_non_us": is_non_us,
        "matched_keywords": matched_str,
        "penalized_keywords": penalized_str,
        "decision_reason": decision_reason,
        "role_alignment_score": role_alignment_score,
        "role_alignment_label": role_alignment_label,
        "role_alignment_reason": role_alignment_reason,
        "lane_fit_score": lane_fit_score,
        "lane_fit_label": lane_fit_label,
        "lane_fit_reason": lane_fit_reason,
        "benchmark_lane": benchmark_lane,
        "benchmark_match_score": benchmark_match_score,
        "benchmark_match_reason": benchmark_match_reason,
        "source_trust_label": source_trust_label,
        "source_trust_adjustment": source_trust_adjustment,
        "source_trust_rank": source_trust_rank,
    }

def evaluate_job(
    company: Company,
    title: str,
    location: str,
    description: str,
    posted_dt: Optional[datetime],
    is_manual: bool = False,
    source: str = "",
    url: str = "",
) -> Dict[str, Any]:
    norm_title = normalize_title(title)
    desc = clean(description)
    job_blob = f"{norm_title} {desc.lower()}"
    company_blob = " ".join(company.industry).lower()
    role_family = classify_role_family(norm_title)
    salary_info = extract_salary_info(desc)
    salary_basis = salary_basis_value(
        salary_info["salary_low"],
        salary_info["salary_high"],
        salary_info["salary_period"],
    )

    apply_hits, skip_hits = detect_domains(job_blob)
    company_apply_hits, _ = detect_domains(company_blob)
    context_fallback_used = False
    title_rescue_bucket = ""
    title_rescue_trigger = ""

    if is_manual:
        manual_apply_hits = apply_hits or company_apply_hits
        trust_label, trust_adjustment, _ = source_trust_profile(source, url, company.domain)
        return {
            "keep": True,
            "drop_stage": "",
            "drop_reason": "",
            "reason": "",
            "score": 50.0,
            "decision_score": 50.0,
            "fit_band": "C",
            "action_bucket": "MANUAL REVIEW",
            "decision_reason": "manual review company",
            "apply_domains_hit": ", ".join(manual_apply_hits),
            "skip_domains_hit": ", ".join(skip_hits),
            "primary_apply_domain": dominant_apply_domain(manual_apply_hits),
            "salary_range": salary_info["salary_range"],
            "salary_currency": salary_info["salary_currency"],
            "salary_low": salary_info["salary_low"],
            "salary_high": salary_info["salary_high"],
            "salary_period": salary_info["salary_period"],
            "is_remote": False,
            "is_hybrid": False,
            "is_non_us": False,
            "matched_keywords": "",
            "penalized_keywords": "",
            "age_days": job_age(posted_dt),
            "role_family": role_family,
            "role_alignment_score": 0.0,
            "role_alignment_label": "manual_target",
            "lane_fit_score": 0.0,
            "lane_fit_label": "manual_target",
            "benchmark_lane": "",
            "benchmark_match_score": 0.0,
            "benchmark_match_reason": "",
            "lane_cohort": "manual_target",
            "source_trust_label": trust_label,
            "source_trust_adjustment": trust_adjustment,
            "normalized_title": norm_title,
            "title_gate_matched_keywords": "",
            "title_rescue_bucket": "",
            "title_rescue_trigger": "",
        }

    # ---------------------------------------------------------
    # Strict gating / soft-drop output
    # ---------------------------------------------------------
    for term in TITLE_NEGATIVE_DISQUALIFIERS:
        if _kw_match(term, norm_title):
            return _filtered_out_result(
                company=company,
                title=title,
                location=location,
                description=desc,
                posted_dt=posted_dt,
                salary_info=salary_info,
                role_family=role_family,
                normalized_title=norm_title,
                drop_stage="Title Blacklist",
                drop_reason=f"Matched disqualifier: {term}",
                reason_code=f"title_blacklist:{term}",
            )

    is_remote, is_hybrid, is_non_us = location_flags(location)
    loc_reason = location_policy_reason(
        is_remote, is_hybrid, is_non_us,
        location=location,
        salary_low=salary_info["salary_low"],
        salary_high=salary_info["salary_high"],
        salary_period=salary_info["salary_period"],
        salary_currency=salary_info["salary_currency"],
    )
    if loc_reason:
        return _filtered_out_result(
            company=company,
            title=title,
            location=location,
            description=desc,
            posted_dt=posted_dt,
            salary_info=salary_info,
            role_family=role_family,
            normalized_title=norm_title,
            drop_stage="Location Policy",
            drop_reason=loc_reason,
            reason_code=f"location_policy:{loc_reason}",
        )

    if ENFORCE_MIN_SALARY and salary_basis is not None and (salary_info["salary_currency"] in ("", "USD")):
        base_floor = (
            LOCAL_HYBRID_MIN_SALARY_USD
            if (is_hybrid and is_local_commute_location(location))
            else MIN_SALARY_USD
        )
        buffered_floor = base_floor * (1.0 - SALARY_NEGOTIATION_BUFFER_PCT)
        if salary_basis < buffered_floor:
            floor_label = "local_hybrid_buffered_floor" if (is_hybrid and is_local_commute_location(location)) else "buffered_floor"
            return _filtered_out_result(
                company=company,
                title=title,
                location=location,
                description=desc,
                posted_dt=posted_dt,
                salary_info=salary_info,
                role_family=role_family,
                normalized_title=norm_title,
                drop_stage="Salary Floor",
                drop_reason=f"{salary_basis:,.0f} < {floor_label} {buffered_floor:,.0f}",
                reason_code=f"salary_floor:{int(round(salary_basis))}<{int(round(buffered_floor))}",
            )

    passed_title, fail_reason = title_passes(norm_title)
    if not passed_title:
        rescue = should_soft_rescue_title(norm_title, desc, apply_hits, skip_hits, fail_reason)
        if rescue.get("rescued"):
            passed_title = True
            context_fallback_used = True
            title_rescue_bucket = rescue.get("bucket", "")
            title_rescue_trigger = rescue.get("trigger", "soft_title_rescue")
        else:
            title_gate_terms = unique_preserve(
                matched_title_gate_keywords(norm_title) + matched_title_positive_weight_terms(norm_title)
            )
            filtered = _filtered_out_result(
                company=company,
                title=title,
                location=location,
                description=desc,
                posted_dt=posted_dt,
                salary_info=salary_info,
                role_family=role_family,
                normalized_title=norm_title,
                drop_stage="Title Gate",
                drop_reason=f"title_fail:{fail_reason}",
                reason_code=f"title_fail:{fail_reason}",
                matched_keywords=", ".join(f"title:{term}(+0)" for term in title_gate_terms),
                title_gate_matched_keywords=" | ".join(title_gate_terms),
            )
            filtered.update({
                "title_rescue_bucket": rescue.get("bucket", ""),
                "title_rescue_trigger": rescue.get("trigger", ""),
                "adjacent_title_marker_hit": " | ".join(rescue.get("adjacent_title_marker_hit", [])) if isinstance(rescue.get("adjacent_title_marker_hit"), list) else str(rescue.get("adjacent_title_marker_hit", "")),
                "adjacent_domain_terms_hit": " | ".join(rescue.get("adjacent_domain_terms_hit", [])) if isinstance(rescue.get("adjacent_domain_terms_hit"), list) else str(rescue.get("adjacent_domain_terms_hit", "")),
                "normalized_title_for_adjacent_match": rescue.get("normalized_title_for_adjacent_match", ""),
                "adjacent_title_patterns_checked": rescue.get("adjacent_title_patterns_checked", ""),
                "adjacent_title_patterns_hit": " | ".join(rescue.get("adjacent_title_patterns_hit", [])) if isinstance(rescue.get("adjacent_title_patterns_hit"), list) else str(rescue.get("adjacent_title_patterns_hit", "")),
                "adjacent_title_matched_token_slice": " | ".join(rescue.get("adjacent_title_matched_token_slice", [])) if isinstance(rescue.get("adjacent_title_matched_token_slice"), list) else str(rescue.get("adjacent_title_matched_token_slice", "")),
                "adjacent_title_tokens_debug": rescue.get("adjacent_title_tokens_debug", ""),
                "adjacent_pattern_tokens_debug": rescue.get("adjacent_pattern_tokens_debug", ""),
                "adjacent_match_attempts": rescue.get("adjacent_match_attempts", ""),
            })
            return filtered

    lane_exclusion = excluded_role_lane(norm_title, desc)
    if lane_exclusion:
        return _filtered_out_result(
            company=company,
            title=title,
            location=location,
            description=desc,
            posted_dt=posted_dt,
            salary_info=salary_info,
            role_family=role_family,
            normalized_title=norm_title,
            drop_stage="Lane Exclusion",
            drop_reason=lane_exclusion,
            reason_code=f"role_lane_exclude:{lane_exclusion}",
        )

    if skip_hits and not apply_hits:
        return _filtered_out_result(
            company=company,
            title=title,
            location=location,
            description=desc,
            posted_dt=posted_dt,
            salary_info=salary_info,
            role_family=role_family,
            normalized_title=norm_title,
            drop_stage="Domain Gate",
            drop_reason=f"Skip-only domain match: {', '.join(skip_hits)}",
            reason_code=f"skip_domain_only:{', '.join(skip_hits)}",
        )

    if not apply_hits and not skip_hits and should_use_context_fallback(norm_title):
        apply_hits = company_apply_hits[:1]
        context_fallback_used = bool(apply_hits)

    if not apply_hits:
        rescued, rescue_reason = should_rescue_no_apply_domain(norm_title, desc)
        if rescued:
            apply_hits = ["Cross-Industry Skill Alignment"]
            context_fallback_used = True
            title_rescue_trigger = title_rescue_trigger or "no_apply_domain_rescue"
        else:
            return _filtered_out_result(
                company=company,
                title=title,
                location=location,
                description=desc,
                posted_dt=posted_dt,
                salary_info=salary_info,
                role_family=role_family,
                normalized_title=norm_title,
                drop_stage="Domain Gate",
                drop_reason="No apply-domain hit",
                reason_code="no_apply_domain",
            )

    scored = score_job(
        company=company,
        title=norm_title,
        location=location,
        description=desc,
        posted_dt=posted_dt,
        is_manual=is_manual,
        apply_hits=apply_hits,
        skip_hits=skip_hits,
        source=source,
        url=url,
    )

    lane_cohort, _ = lane_cohort_profile(
        norm_title,
        role_family,
        scored["role_alignment_score"],
        scored["role_alignment_label"],
        scored.get("lane_fit_score", 0.0),
        scored.get("lane_fit_label", ""),
        scored.get("benchmark_lane", ""),
        scored.get("benchmark_match_score", 0.0),
    )

    scored = apply_preference_adjustments(
        scored,
        location=location,
        salary_low=salary_info["salary_low"],
        salary_high=salary_info["salary_high"],
        salary_period=salary_info["salary_period"],
        salary_currency=salary_info["salary_currency"],
    )

    bucket = action_bucket(
        score=scored["score"],
        is_remote=scored["is_remote"],
        is_hybrid=scored["is_hybrid"],
        is_non_us=scored["is_non_us"],
        manual_review=is_manual,
        tier=company.tier,
        role_alignment_score=scored["role_alignment_score"],
        title=title,
        location=location,
        salary_low=salary_info["salary_low"],
        salary_high=salary_info["salary_high"],
        salary_period=salary_info["salary_period"],
        salary_currency=salary_info["salary_currency"],
    )

    extra_reasons: List[str] = []
    if company.tier == 1:
        extra_reasons.append("tier1_company")
    if apply_hits:
        extra_reasons.append(f"Apply Domain: {dominant_apply_domain(apply_hits)}")
    if context_fallback_used:
        extra_reasons.append("Context fallback")
    if title_rescue_bucket:
        extra_reasons.append(f"Title Rescue: {title_rescue_bucket}")
    if title_rescue_trigger:
        extra_reasons.append(f"Rescue Trigger: {title_rescue_trigger}")
    if scored["is_remote"]:
        extra_reasons.append("Remote")
    if scored["age_days"] is not None and scored["age_days"] <= 7:
        extra_reasons.append("Fresh Posting")
    if salary_info["salary_range"]:
        extra_reasons.append("Salary Found")

    decision_reason = _decision_reason_parts(
        title_fast_track_hits=[],
        title_weight_hits=[],
        jd_positive_hits=[],
        jd_negative_hits=[],
        extra_parts=[scored.get("decision_reason", "")] + extra_reasons,
    )

    decision_score = float(scored.get("decision_score", scored.get("score", 0.0)))
    keep = decision_score >= MIN_SCORE_TO_KEEP

    return {
        "keep": keep,
        "drop_stage": "" if keep else "Score Threshold",
        "drop_reason": "" if keep else f"{decision_score:.1f} < {MIN_SCORE_TO_KEEP}",
        "reason": "" if keep else f"score_below_threshold:{int(MIN_SCORE_TO_KEEP)}",
        "score": decision_score,
        "decision_score": decision_score,
        "fit_band": scored["fit_band"],
        "action_bucket": bucket,
        "decision_reason": decision_reason,
        "apply_domains_hit": ", ".join(apply_hits),
        "skip_domains_hit": ", ".join(skip_hits),
        "primary_apply_domain": dominant_apply_domain(apply_hits),
        "title_gate_matched_keywords": " | ".join(unique_preserve(matched_title_gate_keywords(norm_title) + matched_title_positive_weight_terms(norm_title))),
        "title_rescue_bucket": title_rescue_bucket,
        "title_rescue_trigger": title_rescue_trigger,
        "salary_range": salary_info["salary_range"],
        "salary_currency": salary_info["salary_currency"],
        "salary_low": salary_info["salary_low"],
        "salary_high": salary_info["salary_high"],
        "salary_period": salary_info["salary_period"],
        "is_remote": scored["is_remote"],
        "is_hybrid": scored["is_hybrid"],
        "is_non_us": scored["is_non_us"],
        "matched_keywords": scored["matched_keywords"],
        "penalized_keywords": scored["penalized_keywords"],
        "age_days": scored["age_days"],
        "role_family": role_family,
        "role_alignment_score": scored["role_alignment_score"],
        "role_alignment_label": scored["role_alignment_label"],
        "lane_fit_score": scored.get("lane_fit_score", 0.0),
        "lane_fit_label": scored.get("lane_fit_label", ""),
        "benchmark_lane": scored.get("benchmark_lane", ""),
        "benchmark_match_score": scored.get("benchmark_match_score", 0.0),
        "benchmark_match_reason": scored.get("benchmark_match_reason", ""),
        "lane_cohort": lane_cohort,
        "source_trust_label": scored.get("source_trust_label", ""),
        "source_trust_adjustment": scored.get("source_trust_adjustment", 0.0),
        "normalized_title": norm_title,
        "score_threshold_used": MIN_SCORE_TO_KEEP,
        "base_score_before_rescue": None,
        "adjacent_rescue_bonus_applied": None,
        "final_score_after_rescue": decision_score,
    }
# ============================================================
# JOB FACTORY
# ============================================================

def make_job(
    company: Company,
    title: str,
    location: str,
    url: str,
    source: str,
    description: str,
    posted_dt: Optional[datetime],
    is_manual: bool = False,
    rejected_jobs: Optional[List[RejectedJob]] = None,
) -> Optional[Job]:
    desc = clean(description)[:MAX_DESC_CHARS]
    title = clean(title)

    result = evaluate_job(
        company=company,
        title=title,
        location=location,
        description=desc,
        posted_dt=posted_dt,
        is_manual=is_manual,
        source=source,
        url=url,
    )

    if not result.get("keep", False):
        if rejected_jobs is not None:
            rejected_jobs.append(
                make_rejected_job(
                    company=company,
                    title=title,
                    location=location,
                    url=url,
                    source=source,
                    description=desc,
                    posted_dt=posted_dt,
                    drop_stage=result.get("drop_stage", "evaluate_job"),
                    drop_reason=result.get("drop_reason") or result.get("reason", "evaluation_reject"),
                    evaluation=result,
                )
            )
        return None

    return Job(
        company=company.name,
        tier=company.tier,
        priority=company.priority,
        title=title,
        normalized_title=result["normalized_title"],
        role_family=result["role_family"],
        role_alignment_score=result.get("role_alignment_score", 0.0),
        role_alignment_label=result.get("role_alignment_label", ""),
        source_trust_label=result.get("source_trust_label", ""),
        source_trust_adjustment=result.get("source_trust_adjustment", 0.0),
        lane_fit_score=result.get("lane_fit_score", 0.0),
        lane_fit_label=result.get("lane_fit_label", ""),
        benchmark_lane=result.get("benchmark_lane", ""),
        benchmark_match_score=result.get("benchmark_match_score", 0.0),
        benchmark_match_reason=result.get("benchmark_match_reason", ""),
        lane_cohort=result.get("lane_cohort", ""),
        location=clean(location),
        url=url,
        canonical_url=canonicalize(url),
        source=source,
        posted_at=posted_dt.isoformat() if posted_dt else "",
        age_days=result["age_days"],
        score=round(result["score"], 1),
        fit_band=result["fit_band"],
        action_bucket=result["action_bucket"],
        decision_reason=result["decision_reason"],
        is_remote=result["is_remote"],
        is_hybrid=result["is_hybrid"],
        is_non_us=result["is_non_us"],
        apply_domains_hit=result["apply_domains_hit"],
        skip_domains_hit=result["skip_domains_hit"],
        primary_apply_domain=result["primary_apply_domain"],
        matched_keywords=result["matched_keywords"],
        penalized_keywords=result["penalized_keywords"],
        salary_range=result["salary_range"],
        salary_currency=result["salary_currency"],
        salary_low=result.get("salary_low"),
        salary_high=result.get("salary_high"),
        salary_period=result.get("salary_period", ""),
        description_excerpt=desc[:600],
        company_notes=company.notes,
        keep=result.get("keep", True),
        drop_stage=result.get("drop_stage", ""),
        drop_reason=result.get("drop_reason", ""),
        decision_score=round(result.get("decision_score", result["score"]), 1),
        title_gate_matched_keywords=result.get("title_gate_matched_keywords", ""),
        title_rescue_bucket=result.get("title_rescue_bucket", ""),
        title_rescue_trigger=result.get("title_rescue_trigger", ""),
        adjacent_title_marker_hit=result.get("adjacent_title_marker_hit", ""),
        adjacent_domain_terms_hit=result.get("adjacent_domain_terms_hit", ""),
        normalized_title_for_adjacent_match=result.get("normalized_title_for_adjacent_match", ""),
        adjacent_title_patterns_checked=result.get("adjacent_title_patterns_checked", ""),
        adjacent_title_patterns_hit=result.get("adjacent_title_patterns_hit", ""),
        adjacent_title_matched_token_slice=result.get("adjacent_title_matched_token_slice", ""),
        adjacent_title_tokens_debug=result.get("adjacent_title_tokens_debug", ""),
        adjacent_pattern_tokens_debug=result.get("adjacent_pattern_tokens_debug", ""),
        adjacent_match_attempts=result.get("adjacent_match_attempts", ""),
        score_threshold_used=result.get("score_threshold_used"),
        base_score_before_rescue=result.get("base_score_before_rescue"),
        adjacent_rescue_bonus_applied=result.get("adjacent_rescue_bonus_applied"),
        final_score_after_rescue=result.get("final_score_after_rescue"),
        is_new=True,
        seen_first_at="",
        seen_last_at="",
        manual_review=is_manual,
    )


# ============================================================
# KEEP FILTER
# ============================================================

def salary_filter_reason(job: Job) -> str:
    if job.manual_review or not ENFORCE_MIN_SALARY:
        return ""

    basis_value = salary_basis_value(job.salary_low, job.salary_high, job.salary_period)
    # Safe salary gating: only gate when salary is actually present.
    if basis_value is None:
        return ""

    if job.salary_currency and job.salary_currency != "USD":
        return f"salary_currency_not_supported:{job.salary_currency}"

    floor = LOCAL_HYBRID_MIN_SALARY_USD if (job.is_hybrid and is_local_commute_location(job.location)) else MIN_SALARY_USD
    buffered_floor = floor * (1.0 - SALARY_NEGOTIATION_BUFFER_PCT)
    if basis_value < buffered_floor:
        return f"salary_below_buffered_floor:{int(round(basis_value))}<{int(buffered_floor)}"

    return ""


def keep_filter_reason(job: Job) -> str:
    if not job.title or not job.url:
        return "missing_required_fields"
    if job.manual_review:
        return ""

    loc_reason = location_policy_reason(
        job.is_remote,
        job.is_hybrid,
        job.is_non_us,
        location=job.location,
        salary_low=job.salary_low,
        salary_high=job.salary_high,
        salary_period=job.salary_period,
        salary_currency=job.salary_currency,
    )
    if loc_reason:
        return loc_reason

    if ENFORCE_JOB_AGE and job.age_days is not None and job.age_days > MAX_JOB_AGE_DAYS:
        return f"stale_posting:{MAX_JOB_AGE_DAYS}"

    salary_reason = salary_filter_reason(job)
    if salary_reason:
        return salary_reason

    if job.action_bucket == "IGNORE":
        return "ignore_bucket"

    score_threshold_used = job.score_threshold_used if job.score_threshold_used is not None else MIN_SCORE_TO_KEEP
    decision_score = job.decision_score if job.decision_score is not None else job.score
    if float(decision_score) < float(score_threshold_used):
        threshold_label = (
            f"adjacent({int(score_threshold_used)})"
            if getattr(job, "title_rescue_bucket", "") == "adjacent_title"
            else f"{int(score_threshold_used)}"
        )
        return f"score_below_threshold:{threshold_label}"
    return ""

def should_keep(job: Job) -> bool:
    return keep_filter_reason(job) == ""

def filter_kept_jobs(
    company: Optional[Company],
    jobs: List[Job],
    rejected_jobs: List[RejectedJob],
) -> List[Job]:
    kept: List[Job] = []
    for job in jobs:
        drop_reason = keep_filter_reason(job)
        if not drop_reason:
            kept.append(job)
            continue

        rejected_jobs.append(
            make_rejected_job(
                company=company or company_from_job(job),
                title=job.title,
                location=job.location,
                url=job.url,
                source=job.source,
                description=job.description_excerpt,
                posted_dt=parse_date(job.posted_at),
                drop_stage="keep_filter",
                drop_reason=drop_reason,
                evaluation=asdict(job),
            )
        )

    return kept



# ============================================================
# BOARD / CAREER SITE DISCOVERY HELPERS
# ============================================================

DISCOVERY_URLS_BY_COMPANY: Dict[str, List[str]] = {
    "Confluent": [
        "https://careers.confluent.io/jobs/united_states",
        "https://careers.confluent.io/",
    ],
    "Checkout.com": [
        "https://careers.smartrecruiters.com/Checkoutcom1",
        "https://www.checkout.com/jobs",
    ],
    "Fidelity Investments": [
        "https://jobs.fidelity.com/en/jobs/",
        "https://jobs.fidelity.com/job-search-results",
        "https://jobs.fidelity.com/",
    ],
    "Fidelity (NFS Parent)": [
        "https://jobs.fidelity.com/en/jobs/",
        "https://jobs.fidelity.com/job-search-results",
        "https://jobs.fidelity.com/",
    ],
    "Clearwater Analytics": [
        "https://clearwateranalytics.com/company/careers/",
        "https://clearwateranalytics.com/careers/",
    ],
    "Snowflake": [
        "https://careers.snowflake.com/us/en/",
        "https://careers.snowflake.com/us/en/search-results",
        "https://careers.snowflake.com/us/en/c/product-jobs",
    ],
    "Morningstar": [
        "https://careers.morningstar.com/us/en",
        "https://careers.morningstar.com/us/en/search-results",
    ],
    "FactSet": [
        "https://factset.wd108.myworkdayjobs.com/FactSetCareers",
    ],
    "Envestnet": [
        "https://careers.envestnet.com/",
        "https://careers.envestnet.com/search/jobs",
    ],
    "MuleSoft": ["https://careers.salesforce.com/en/jobs/"],
}

EXTERNAL_BOARD_REGISTRY: List[Dict[str, Any]] = []

EXTERNAL_BOARD_COMMON_DOMAIN_MARKERS = [
    "fintech", "financial services", "financial infrastructure", "capital markets",
    "wealth management", "asset management", "alternative investments", "investment",
    "portfolio", "portfolio accounting", "investment accounting", "reconciliation",
    "brokerage", "custody", "clearing", "trading", "trader", "advisor",
    "advisor platform", "fund administration", "fund accounting", "operations",
    "back office", "middle office", "money movement", "payments", "bill pay",
    "ledger", "ledgers", "treasury", "financial systems", "api", "integration",
    "tax lot", "tax lots", "market data", "order management",
]

EXTERNAL_BOARD_COMMON_ROLE_MARKERS = [
    "architect", "product manager", "product owner", "program manager", "manager",
    "analyst", "business analyst", "systems analyst", "consultant", "specialist",
    "solutions", "solution", "integration", "platform", "api", "operations",
    "migration", "implementation", "portfolio", "trading", "back office",
]

EXTERNAL_BOARD_BLOCK_MARKERS = [
    "crypto", "cryptocurrency", "blockchain", "web3", "defi", "nft", "token",
    "consumer lending", "earned wage access", "ewa", "loan officer", "loan originator",
    "mortgage sales", "mortgage originator", "collections agent",
]

EXTERNAL_BOARD_PREFILTERS: Dict[str, Dict[str, List[str]]] = {
    "eFinancialCareers": {
        "domain_markers": [
            "capital markets", "asset management", "wealth management", "brokerage",
            "custody", "investment operations", "fund accounting", "investment banking",
        ],
    },
    "Wellfound": {
        "domain_markers": [
            "fintech", "payments", "money movement", "ledger", "ledgers",
            "financial infrastructure", "banking", "treasury",
        ],
    },
    "Built In": {
        "domain_markers": [
            "fintech", "payments", "money movement", "financial services",
            "financial infrastructure", "reconciliation",
        ],
    },
    "Welcome to the Jungle": {
        "domain_markers": [
            "fintech", "payments", "money movement", "banking",
            "financial services", "financial infrastructure",
        ],
    },
}

BOARD_LINK_PATTERNS: Dict[str, List[str]] = {
    "eFinancialCareers": [
        r'https?://www\.efinancialcareers\.com/jobs-[^"\'\s?#]+',
        r'(?<![A-Za-z0-9])(/jobs-[^"\'\s?#]+)',
    ],
    "Wellfound": [
        r'https?://wellfound\.com/jobs/\d+[^"\'\s?#]*',
        r'(?<![A-Za-z0-9])(/jobs/\d+[^"\'\s?#]*)',
    ],
    "Built In": [
        r'https?://builtin\.com/job/[^"\'\s?#]+',
        r'(?<![A-Za-z0-9])(/job/[^"\'\s?#]+)',
    ],
    "Welcome to the Jungle": [
        r'https?://app\.welcometothejungle\.com/jobs/[^"\'\s?#/]+(?:/company)?',
        r'(?<![A-Za-z0-9])(/jobs/[^"\'\s?#/]+(?:/company)?)',
    ],
}

JOB_TITLE_ANCHOR_WORDS = (
    "manager", "architect", "product", "analyst", "consultant", "specialist",
    "director", "lead", "principal", "platform", "integration", "operations",
    "engineer", "solutions", "administrator", "owner", "strategy", "technical",
)
NON_JOB_LINK_RE = re.compile(
    r"/(benefits|culture|locations|teams|events|students|faq|saved-jobs|talent-network|"
    r"job-alerts|join|apply|sign-in|login|privacy|people|about|pages|categories|departments|"
    r"leadership|executives|executive|our-team|meet-the-team|meet-our-team|board-of-directors|"
    r"board|bios|bio|author|profile|person|team-member|management|management-team|"
    r"press|news|blog|investor|media|contact|support|products|solutions|platform)(?:/|$)",
    re.I,
)
JOB_LINK_RE = re.compile(
    r"/(job|jobs|position|positions|opening|openings|opportunit|vacanc|requisition)(?:/|$)",
    re.I,
)

KNOWN_JOB_HOST_TOKENS = (
    "greenhouse.io", "boards.greenhouse.io", "job-boards.greenhouse.io",
    "lever.co", "myworkdayjobs.com", "workday.com", "ashbyhq.com",
    "smartrecruiters.com", "jobvite.com", "jobs.jobvite.com", "icims.com",
    "workable.com", "recruitee.com", "bamboohr.com", "comeet.co",
    "applytojob.com", "careers-page.com", "phenompeople.com",
)
SKIP_FILE_EXTENSIONS = {".pdf", ".jpg", ".jpeg", ".png", ".gif", ".svg", ".zip", ".doc", ".docx"}

def unique_preserve(values: List[str]) -> List[str]:
    out: List[str] = []
    seen = set()
    for value in values:
        if not value:
            continue
        if value not in seen:
            seen.add(value)
            out.append(value)
    return out

def company_discovery_urls(company: Company) -> List[str]:
    urls: List[str] = []
    if company.careers_url:
        urls.append(company.careers_url)
    # iframe_src is a known-good ATS embed URL — treat it as a first-class discovery target
    if company.iframe_src:
        urls.append(company.iframe_src)
    if company.discovery_urls:
        urls.extend(company.discovery_urls)
    urls.extend(DISCOVERY_URLS_BY_COMPANY.get(company.name, []))
    normalized: List[str] = []
    for raw in urls:
        safe = sanitize_url(raw)
        if safe:
            normalized.append(safe)
    return unique_preserve(normalized)


def jobs_api_json(company: Company, rejected_jobs: Optional[List[RejectedJob]] = None) -> List[Job]:
    """Fetch company.jobs_api_url as JSON and extract jobs from common list structures."""
    if not company.jobs_api_url:
        return []
    data = fetch_json(company.jobs_api_url)
    if not data:
        return []
    # Unwrap common wrapper keys
    if isinstance(data, dict):
        for key in ("jobs", "results", "data", "items", "postings", "positions",
                    "requisitions", "openings", "jobPostings"):
            if isinstance(data.get(key), list):
                data = data[key]
                break
    if not isinstance(data, list):
        return []
    jobs: List[Job] = []
    for raw in data:
        if not isinstance(raw, dict):
            continue
        title = clean(
            raw.get("title") or raw.get("name") or raw.get("jobTitle") or
            raw.get("job_title") or raw.get("position") or ""
        )
        if not title:
            continue
        loc_raw = raw.get("location") or raw.get("locationName") or raw.get("location_name") or ""
        location = clean(loc_raw.get("name", "") if isinstance(loc_raw, dict) else loc_raw)
        url = clean(
            raw.get("url") or raw.get("link") or raw.get("applyUrl") or
            raw.get("apply_url") or raw.get("absolute_url") or raw.get("absoluteUrl") or
            company.jobs_api_url or ""
        )
        posted_dt = parse_date(
            raw.get("posted_at") or raw.get("createdAt") or raw.get("created_at") or
            raw.get("datePosted") or raw.get("date_posted") or ""
        )
        description = clean(
            raw.get("description") or raw.get("content") or raw.get("body") or ""
        )
        job = make_job(company, title, location, url, "Jobs API", description, posted_dt,
                       rejected_jobs=rejected_jobs)
        if job:
            jobs.append(job)
    return jobs

def host_label(url: str) -> str:
    try:
        host = urlparse(url).netloc.lower()
    except Exception:
        host = ""
    if "greenhouse" in host:
        return "Greenhouse"
    if "lever" in host:
        return "Lever"
    if "ashby" in host:
        return "Ashby"
    if "smartrecruiters" in host:
        return "SmartRecruiters"
    if "workday" in host or "myworkdayjobs" in host:
        return "Workday"
    if host:
        return host
    return "Career Site"


def company_hosts(company: Company) -> List[str]:
    hosts: List[str] = []
    for raw in ([company.careers_url] if company.careers_url else []) + (company.discovery_urls or []) + DISCOVERY_URLS_BY_COMPANY.get(company.name, []):
        try:
            host = urlparse(raw).netloc.lower()
        except Exception:
            host = ""
        if host:
            hosts.append(host)
    if company.domain:
        hosts.append(company.domain.lower())
        hosts.append(f"www.{company.domain.lower()}")
    return unique_preserve(hosts)


def is_known_job_host(host: str) -> bool:
    return any(token in host for token in KNOWN_JOB_HOST_TOKENS)


def is_allowed_detail_url(company: Company, listing_url: str, detail_url: str) -> bool:
    if url_is_blacklisted(detail_url):
        return False
    parsed = urlparse(detail_url)
    host = parsed.netloc.lower()
    path_l = parsed.path.lower()
    if parsed.scheme not in {"http", "https", ""}:
        return False
    if any(path_l.endswith(ext) for ext in SKIP_FILE_EXTENSIONS):
        return False
    if NON_JOB_LINK_RE.search(path_l):
        return False
    if not host:
        return True
    listing_host = urlparse(listing_url).netloc.lower()
    allowed_hosts = company_hosts(company) + ([listing_host] if listing_host else [])
    if host == listing_host or any(allowed and (host == allowed or host.endswith("." + allowed) or allowed.endswith("." + host)) for allowed in allowed_hosts):
        return True
    return is_known_job_host(host)

def safe_json_loads(value: str) -> Any:
    try:
        return json.loads(value)
    except Exception:
        return None

def iter_jobposting_objects(value: Any):
    if isinstance(value, dict):
        type_value = value.get("@type")
        if type_value == "JobPosting" or (isinstance(type_value, list) and "JobPosting" in type_value):
            yield value
        for nested in value.values():
            yield from iter_jobposting_objects(nested)
    elif isinstance(value, list):
        for item in value:
            yield from iter_jobposting_objects(item)

def extract_jsonld_jobpostings(soup: BeautifulSoup) -> List[Dict[str, Any]]:
    jobs: List[Dict[str, Any]] = []
    for script in soup.find_all("script", attrs={"type": "application/ld+json"}):
        raw = script.string or script.get_text(" ", strip=True)
        if not raw:
            continue
        data = safe_json_loads(raw)
        if data is None:
            continue
        for obj in iter_jobposting_objects(data):
            jobs.append(obj)
    return jobs

def jsonld_location(obj: Dict[str, Any]) -> str:
    locations = obj.get("jobLocation") or obj.get("jobLocationText") or []
    if isinstance(locations, str):
        return clean(locations)
    if isinstance(locations, dict):
        locations = [locations]
    out: List[str] = []
    for loc in locations or []:
        if isinstance(loc, str):
            out.append(clean(loc))
            continue
        if not isinstance(loc, dict):
            continue
        addr = loc.get("address") or {}
        parts = [
            addr.get("addressLocality"),
            addr.get("addressRegion"),
            addr.get("addressCountry"),
        ]
        joined = ", ".join([clean(p) for p in parts if p])
        if joined:
            out.append(joined)
    return " / ".join(unique_preserve(out))

def jsonld_description(obj: Dict[str, Any]) -> str:
    parts = [
        clean(obj.get("description")),
        clean(obj.get("employmentType")),
        clean(obj.get("qualifications")),
        clean(obj.get("responsibilities")),
    ]
    base_salary = obj.get("baseSalary") or {}
    if isinstance(base_salary, dict):
        salary_value = base_salary.get("value") or {}
        if isinstance(salary_value, dict):
            low = salary_value.get("minValue")
            high = salary_value.get("maxValue")
            unit = salary_value.get("unitText") or base_salary.get("unitText")
            if low or high:
                parts.append(f"Salary {low or ''} - {high or ''} {unit or ''}")
    return clean(" ".join([p for p in parts if p]))

def jsonld_company(obj: Dict[str, Any]) -> str:
    org = obj.get("hiringOrganization") or obj.get("hiringOrganizationName") or {}
    if isinstance(org, str):
        return clean(org)
    if isinstance(org, dict):
        return clean(org.get("name") or org.get("legalName") or "")
    return ""


def normalize_company_key(name: str) -> str:
    return re.sub(r"[^a-z0-9]+", "", clean(name).lower())


def registry_company_lookup() -> Dict[str, Dict[str, Any]]:
    lookup: Dict[str, Dict[str, Any]] = {}
    for raw in COMPANY_REGISTRY:
        lookup[normalize_company_key(raw.get("name", ""))] = raw
    aliases = {
        "billcom": "Bill.com",
        "fidelity": "Fidelity Investments",
        "schwab": "Charles Schwab",
        "builtin": "Built In",
    }
    name_to_raw = {raw.get("name"): raw for raw in COMPANY_REGISTRY}
    for alias, canonical in aliases.items():
        if canonical in name_to_raw:
            lookup[alias] = name_to_raw[canonical]
    return lookup


REGISTRY_COMPANY_LOOKUP = registry_company_lookup()


def external_company_for_job(company_name: str, board_name: str, job_url: str, context_text: str = "") -> Company:
    clean_name = clean(company_name) or f"Unknown via {board_name}"
    match = REGISTRY_COMPANY_LOOKUP.get(normalize_company_key(clean_name))
    if match:
        return Company(**{k: v for k, v in match.items() if k in Company.__dataclass_fields__})

    context = clean(context_text)
    tags = [t for t in re.split(r"[•|/,]", context) if clean(t)]
    industries = unique_preserve([clean(t) for t in tags][:8])
    blob = f"{clean_name} {context}".lower()
    tier = 2 if any(k in blob for k in [
        "fintech", "capital markets", "payments", "financial services",
        "investment", "brokerage", "custody", "advisor", "wealth", "banking", "financial data",
    ]) else 3
    priority = "medium" if tier == 2 else "low"
    return Company(
        name=clean_name,
        tier=tier,
        priority=priority,
        adapter=f"external_{normalize_company_key(board_name)}",
        domain=(urlparse(job_url).netloc or "").lower(),
        industry=industries,
        notes=f"Discovered via {board_name}",
    )


def company_from_job(job: Job) -> Company:
    return Company(
        name=job.company,
        tier=job.tier,
        priority=job.priority,
        adapter="from_job",
        domain=(urlparse(job.url).netloc or "").lower(),
        industry=[],
        notes=job.company_notes,
    )


def meta_content(soup: BeautifulSoup, attr_name: str, attr_value: str) -> str:
    node = soup.select_one(f'meta[{attr_name}="{attr_value}"]')
    if node and node.get("content"):
        return clean(node.get("content"))
    return ""


def split_title_company_pair(raw: str, board_name: str) -> Tuple[str, str]:
    raw = clean(raw)
    if not raw:
        return "", ""
    patterns = []
    if board_name == "Wellfound":
        patterns = [r"^(.*?)\s+at\s+(.+?)(?:\s+[•|]\s+|\s+\|\s+Wellfound|$)"]
    elif board_name == "Built In":
        patterns = [r"^(.*?)\s+-\s+(.+?)(?:\s+\|\s+Built In.*|$)"]
    elif board_name == "eFinancialCareers":
        patterns = [r"^(.*?)\s+(?:job\s+with|at)\s+(.+?)(?:\s+\||$)"]
    elif board_name == "Welcome to the Jungle":
        patterns = [
            r"^(.*?),\s+(.+?)(?:\s+\|\s+Welcome to the Jungle.*|$)",
            r"^(.+?)\s+(.*?)\s+\|\s+Welcome to the Jungle.*$",
        ]
    for pattern in patterns:
        m = re.match(pattern, raw, flags=re.I)
        if m:
            return clean(m.group(1)), clean(m.group(2))
    return raw, ""


def board_detail_company(board_name: str, soup: BeautifulSoup, page_text: str) -> str:
    jsonld_jobs = extract_jsonld_jobpostings(soup)
    for obj in jsonld_jobs:
        name = jsonld_company(obj)
        if name:
            return name

    for raw in [
        meta_content(soup, "property", "og:title"),
        clean((soup.select_one("title") or soup.new_tag("title")).get_text(" ", strip=True)),
    ]:
        title_guess, company_guess = split_title_company_pair(raw, board_name)
        if company_guess:
            return company_guess

    h1 = clean((soup.select_one("h1") or soup.new_tag("h1")).get_text(" ", strip=True))
    if board_name == "Welcome to the Jungle" and "," in h1:
        left, _, right = h1.rpartition(",")
        if left and 1 <= len(right.split()) <= 6:
            return clean(right)
    if board_name == "Built In":
        m = re.search(r"The Company\s+(.+?)(?:\s+HQ:|\s+What We Do|\s+Benefits|$)", page_text, flags=re.I)
        if m:
            return clean(m.group(1))
    return ""


def board_detail_title(board_name: str, soup: BeautifulSoup) -> str:
    jsonld_jobs = extract_jsonld_jobpostings(soup)
    for obj in jsonld_jobs:
        title = clean(obj.get("title") or obj.get("name"))
        if title:
            return title
    h1 = clean((soup.select_one("h1") or soup.new_tag("h1")).get_text(" ", strip=True))
    if board_name == "Welcome to the Jungle" and "," in h1:
        left, _, right = h1.rpartition(",")
        if left and 1 <= len(right.split()) <= 6:
            return clean(left)
    raw = meta_content(soup, "property", "og:title") or clean((soup.select_one("title") or soup.new_tag("title")).get_text(" ", strip=True))
    title_guess, _ = split_title_company_pair(raw, board_name)
    return clean(title_guess)


def board_detail_context(board_name: str, page_text: str) -> str:
    if board_name == "Welcome to the Jungle":
        m = re.search(r"Open for applications\s+(.*?)\s+(?:Open for applications|Job no longer available|## Company mission|$)", page_text, flags=re.I)
        if m:
            return clean(m.group(1))
    if board_name == "Built In":
        m = re.search(r"(Fintech[^\n]{0,120}|Financial Services[^\n]{0,120}|Payments[^\n]{0,120})", page_text, flags=re.I)
        if m:
            return clean(m.group(1))
    return ""


def external_board_prefilter(
    board_name: str,
    title: str,
    company_name: str,
    location: str,
    page_text: str,
    url: str,
) -> Tuple[bool, str]:
    title_l = normalize_title(title)
    blob = clean(" ".join([title, company_name, location, page_text[:MAX_EXTERNAL_BOARD_PREFILTER_TEXT_CHARS], url])).lower()
    apply_hits, skip_hits = detect_domains(blob)
    known_target_company = normalize_company_key(company_name) in REGISTRY_COMPANY_LOOKUP

    for marker in EXTERNAL_BOARD_BLOCK_MARKERS:
        if _kw_match(marker, blob):
            return False, f"blocked:{marker}"

    if skip_hits and not apply_hits:
        return False, f"skip_domain_only:{','.join(skip_hits)}"

    role_signal = any(marker in title_l for marker in EXTERNAL_BOARD_COMMON_ROLE_MARKERS)
    role_signal = role_signal or any(marker in title_l for marker in TITLE_MUST_INCLUDE_ONE)
    role_signal = role_signal or any(marker in title_l for marker in SOFT_TITLE_ROLE_MARKERS)

    board_cfg = EXTERNAL_BOARD_PREFILTERS.get(board_name, {})
    domain_markers = unique_preserve(EXTERNAL_BOARD_COMMON_DOMAIN_MARKERS + board_cfg.get("domain_markers", []))
    domain_signal = bool(apply_hits) or any(_kw_match(marker, blob) for marker in domain_markers)
    lane_fit_score, lane_fit_label, _ = high_fit_fintech_lane_score(title_l, blob)

    if lane_fit_score >= 10:
        return True, f"high_fit_lane:{lane_fit_label}"
    if known_target_company and role_signal:
        return True, "target_company_role"
    if role_signal and domain_signal:
        return True, "role_plus_domain"
    if domain_signal and any(marker in title_l for marker in ["consultant", "specialist", "analyst", "manager", "architect"]):
        return True, "domain_plus_general_role"
    if not role_signal and not domain_signal:
        return False, "missing_role_and_domain_signal"
    if not role_signal:
        return False, "missing_role_signal"
    return False, "missing_domain_signal"


def extract_board_detail_links(board_name: str, listing_url: str, html: str, limit: int = MAX_EXTERNAL_BOARD_DETAIL_LINKS) -> List[str]:
    urls: List[str] = []
    seen: set[str] = set()
    patterns = BOARD_LINK_PATTERNS.get(board_name, [])
    for pattern in patterns:
        for m in re.finditer(pattern, html, flags=re.I):
            full = urljoin(listing_url, m.group(0))
            if full in seen:
                continue
            seen.add(full)
            urls.append(full)
            if len(urls) >= limit:
                return urls
    soup = make_soup(html)
    for a in soup.find_all("a", href=True):
        href = a.get("href") or ""
        text_value = clean(a.get_text(" ", strip=True))
        full = urljoin(listing_url, href)
        if board_name == "Wellfound" and re.search(r"/jobs/\d+", full):
            pass
        elif board_name == "Built In" and "/job/" in full:
            pass
        elif board_name == "eFinancialCareers" and re.search(r"/jobs-[A-Za-z0-9_-]", full):
            pass
        elif board_name == "Welcome to the Jungle" and re.search(r"app\.welcometothejungle\.com/jobs/", full):
            pass
        else:
            continue
        if full not in seen and text_value:
            seen.add(full)
            urls.append(full)
        if len(urls) >= limit:
            break
    return urls


def build_job_from_external_board_page(board_name: str, url: str, html: str, rejected_jobs: Optional[List[RejectedJob]] = None) -> Optional[Job]:
    if not html or not clean(html):
        return None
    soup = make_soup(html)
    page_text = clean(soup.get_text(" ", strip=True))
    title = board_detail_title(board_name, soup)
    company_name = board_detail_company(board_name, soup, page_text)
    jsonld_jobs = extract_jsonld_jobpostings(soup)
    location = ""
    posted_dt: Optional[datetime] = None
    description = page_text
    if jsonld_jobs:
        obj = jsonld_jobs[0]
        location = jsonld_location(obj)
        posted_dt = parse_date(obj.get("datePosted"))
        description = clean(" ".join([jsonld_description(obj), page_text]))
    if not location:
        location = guess_location_from_text(page_text)
    if not posted_dt:
        posted_dt = guess_posted_from_text(page_text)

    if not title:
        return None

    company_context = board_detail_context(board_name, page_text)
    prefilter_ok, prefilter_reason = external_board_prefilter(
        board_name=board_name,
        title=title,
        company_name=company_name,
        location=location,
        page_text=f"{company_context} {page_text}",
        url=url,
    )
    company = external_company_for_job(company_name, board_name, url, company_context)
    if not prefilter_ok:
        if rejected_jobs is not None:
            rejected_jobs.append(
                make_rejected_job(
                    company=company,
                    title=title,
                    location=location,
                    url=url,
                    source=board_name,
                    description=page_text[:MAX_DESC_CHARS],
                    posted_dt=posted_dt,
                    drop_stage="board_prefilter",
                    drop_reason=f"board_prefilter:{prefilter_reason}",
                    evaluation={
                        "normalized_title": normalize_title(title),
                        "role_family": classify_role_family(normalize_title(title)),
                    },
                )
            )
        return None
    return make_job(
        company=company,
        title=title,
        location=location,
        url=url,
        source=board_name,
        description=description,
        posted_dt=posted_dt,
        rejected_jobs=rejected_jobs,
    )


def efinancialcareers_adapter(search_urls: List[str], rejected_jobs: Optional[List[RejectedJob]] = None) -> List[Job]:
    jobs: List[Job] = []
    seen: set[str] = set()
    for listing_url in search_urls[:MAX_EXTERNAL_BOARD_SEARCH_URLS]:
        try:
            html = fetch_text(listing_url, referer="https://www.efinancialcareers.com/jobs")
        except Exception as e:
            logging.warning("eFinancialCareers listing fetch failed url=%s err=%s", listing_url, e)
            continue
        for detail_url in extract_board_detail_links("eFinancialCareers", listing_url, html):
            if detail_url in seen:
                continue
            seen.add(detail_url)
            try:
                detail_html = fetch_text(detail_url, referer=listing_url)
            except Exception as e:
                logging.warning("eFinancialCareers detail fetch failed url=%s err=%s", detail_url, e)
                continue
            job = build_job_from_external_board_page("eFinancialCareers", detail_url, detail_html, rejected_jobs=rejected_jobs)
            if job:
                jobs.append(job)
    return dedupe(jobs)


def wellfound_adapter(search_urls: List[str], rejected_jobs: Optional[List[RejectedJob]] = None) -> List[Job]:
    jobs: List[Job] = []
    seen: set[str] = set()
    for listing_url in search_urls[:MAX_EXTERNAL_BOARD_SEARCH_URLS]:
        try:
            html = fetch_text(listing_url, referer="https://wellfound.com/jobs")
        except Exception as e:
            logging.warning("Wellfound listing fetch failed url=%s err=%s", listing_url, e)
            continue
        for detail_url in extract_board_detail_links("Wellfound", listing_url, html):
            if detail_url in seen:
                continue
            seen.add(detail_url)
            try:
                detail_html = fetch_text(detail_url, referer=listing_url)
            except Exception as e:
                logging.warning("Wellfound detail fetch failed url=%s err=%s", detail_url, e)
                continue
            job = build_job_from_external_board_page("Wellfound", detail_url, detail_html, rejected_jobs=rejected_jobs)
            if job:
                jobs.append(job)
    return dedupe(jobs)


def builtin_adapter(search_urls: List[str], rejected_jobs: Optional[List[RejectedJob]] = None) -> List[Job]:
    jobs: List[Job] = []
    seen: set[str] = set()
    for listing_url in search_urls[:MAX_EXTERNAL_BOARD_SEARCH_URLS]:
        try:
            html = fetch_text(listing_url, referer="https://builtin.com/jobs")
        except Exception as e:
            logging.warning("Built In listing fetch failed url=%s err=%s", listing_url, e)
            continue
        for detail_url in extract_board_detail_links("Built In", listing_url, html):
            if detail_url in seen:
                continue
            seen.add(detail_url)
            try:
                detail_html = fetch_text(detail_url, referer=listing_url)
            except Exception as e:
                logging.warning("Built In detail fetch failed url=%s err=%s", detail_url, e)
                continue
            job = build_job_from_external_board_page("Built In", detail_url, detail_html, rejected_jobs=rejected_jobs)
            if job:
                jobs.append(job)
    return dedupe(jobs)


def welcometothejungle_adapter(search_urls: List[str], rejected_jobs: Optional[List[RejectedJob]] = None) -> List[Job]:
    jobs: List[Job] = []
    seen: set[str] = set()
    for listing_url in search_urls[:MAX_EXTERNAL_BOARD_SEARCH_URLS]:
        try:
            html = fetch_text(listing_url, referer="https://app.welcometothejungle.com/")
        except Exception as e:
            logging.warning("Welcome to the Jungle listing fetch failed url=%s err=%s", listing_url, e)
            continue
        for detail_url in extract_board_detail_links("Welcome to the Jungle", listing_url, html):
            if detail_url in seen:
                continue
            seen.add(detail_url)
            try:
                detail_html = fetch_text(detail_url, referer=listing_url)
            except Exception as e:
                logging.warning("Welcome to the Jungle detail fetch failed url=%s err=%s", detail_url, e)
                continue
            job = build_job_from_external_board_page("Welcome to the Jungle", detail_url, detail_html, rejected_jobs=rejected_jobs)
            if job:
                jobs.append(job)
    return dedupe(jobs)



def run_external_board(board: Dict[str, Any], rejected_jobs: Optional[List[RejectedJob]] = None) -> List[Job]:
    set_run_context("board", board.get("name", ""), board.get("adapter", ""), "")
    try:
        adapter = board.get("adapter")
        urls = board.get("search_urls") or []
        if adapter == "efinancialcareers":
            return efinancialcareers_adapter(urls, rejected_jobs=rejected_jobs)
        if adapter == "wellfound":
            return wellfound_adapter(urls, rejected_jobs=rejected_jobs)
        if adapter == "builtin":
            return builtin_adapter(urls, rejected_jobs=rejected_jobs)
        if adapter == "welcometothejungle":
            return welcometothejungle_adapter(urls, rejected_jobs=rejected_jobs)
        return []
    finally:
        clear_run_context()

# Vocabulary signals that indicate a page is actually a job posting.
# Requiring 2+ of these before accepting non-structured-data pages filters
# out leadership bios, product pages, press releases, and navigation pages
# that pass URL/link heuristics but aren't job listings.
_JOB_CONTENT_SIGNALS = [
    "responsibilities", "qualifications", "requirements",
    "job description", "about the role", "what you'll do",
    "what you will do", "you will be", "we are looking for",
    "minimum qualifications", "preferred qualifications",
    "key responsibilities", "essential functions", "basic qualifications",
    "this role", "in this role", "the ideal candidate",
    "apply now", "submit your application", "to apply",
]

def _has_job_content_signals(text: str) -> bool:
    """Return True if text contains at least 2 job-posting vocabulary signals."""
    t = text.lower()
    return sum(1 for s in _JOB_CONTENT_SIGNALS if s in t) >= 2


def guess_title_from_soup(soup: BeautifulSoup) -> str:
    h1 = soup.select_one("h1")
    if h1:
        txt = clean(h1.get_text(" ", strip=True))
        if txt:
            return txt
    og = soup.select_one("meta[property='og:title']")
    if og and og.get("content"):
        return clean(og.get("content"))
    title_tag = soup.select_one("title")
    if title_tag:
        return clean(title_tag.get_text(" ", strip=True)).split(" | ")[0]
    return ""

def guess_location_from_text(page_text: str) -> str:
    if not page_text:
        return ""
    patterns = [
        r"(Remote,?\s+United States)",
        r"(US Remote\s+[A-Za-z]+)",
        r"(United States(?:\s*/\s*[A-Za-z .'-]+)?)",
        r"([A-Z][A-Za-z .'-]+,\s*[A-Z]{2})",
    ]
    for pattern in patterns:
        m = re.search(pattern, page_text, flags=re.I)
        if m:
            return clean(m.group(1))
    return ""

def guess_posted_from_text(page_text: str) -> Optional[datetime]:
    if not page_text:
        return None
    patterns = [
        r"(?:Date Posted|Posted|Published)\s*:?\s*([A-Za-z]{3,9}\.?\s+\d{1,2},\s+\d{4})",
        r"(?:Date Posted|Posted|Published)\s*:?\s*(\d{1,2}\s+[A-Za-z]{3,9}\.?\s+\d{4})",
        r"(\d{4}-\d{2}-\d{2})",
    ]
    for pattern in patterns:
        m = re.search(pattern, page_text, flags=re.I)
        if m:
            return parse_date(m.group(1))
    return None

def is_probable_job_link(text_value: str, href: str) -> bool:
    href_l = (href or "").lower()
    text_l = clean(text_value).lower()
    if not href or href.startswith("#") or href.startswith("javascript:") or href.startswith("mailto:"):
        return False
    if NON_JOB_LINK_RE.search(href_l):
        return False
    if text_l in {"apply", "apply now", "view all", "view more jobs", "search jobs", "save job", "learn more"}:
        return False
    if JOB_LINK_RE.search(href_l):
        return True
    if any(word in text_l for word in JOB_TITLE_ANCHOR_WORDS) and 3 <= len(text_l) <= 140:
        return True
    if re.search(r"(jr\d{5,}|_\w?\d{4,}|/\d{6,})", href_l):
        return True
    return False

def extract_job_links(company: Company, listing_url: str, html: str, limit: int = MAX_DETAIL_LINKS) -> List[str]:
    if html is None:
        return []
    soup = make_soup(html)
    links: List[str] = []
    for a in soup.find_all("a", href=True):
        href = a["href"]
        text_value = a.get_text(" ", strip=True)
        if not is_probable_job_link(text_value, href):
            continue
        full = urljoin(listing_url, href)
        if not is_allowed_detail_url(company, listing_url, full):
            continue
        if full not in links:
            links.append(full)
        if len(links) >= limit:
            break
    return links

def build_job_from_detail_page(
    company: Company,
    url: str,
    html: str,
    source: str,
    rejected_jobs: Optional[List[RejectedJob]] = None,
) -> Optional[Job]:
    if html is None:
        logging.debug("build_job_from_detail_page received None markup company=%s url=%s", company.name, url)
        return None
    if not html:
        return None
    soup = make_soup(html)
    jsonld_jobs = extract_jsonld_jobpostings(soup)
    if jsonld_jobs:
        obj = jsonld_jobs[0]
        title = clean(obj.get("title") or obj.get("name") or guess_title_from_soup(soup))
        location = jsonld_location(obj)
        description = jsonld_description(obj) or clean(soup.get_text(" ", strip=True))
        posted_dt = parse_date(obj.get("datePosted")) or guess_posted_from_text(clean(soup.get_text(" ", strip=True)))
        job_url = clean(obj.get("url") or url)
        if title:
            return make_job(
                company=company,
                title=title,
                location=location,
                url=job_url,
                source=source,
                description=description,
                posted_dt=posted_dt,
                rejected_jobs=rejected_jobs,
            )

    title = guess_title_from_soup(soup)
    if not title:
        return None
    page_text = clean(soup.get_text(" ", strip=True))
    # Guard against scraping non-job pages (leadership bios, product pages, etc.).
    # JSON-LD JobPosting structured data is trusted unconditionally; the heuristic
    # fallback requires at least 2 job-posting vocabulary signals in the page text.
    if not _has_job_content_signals(page_text):
        logging.debug(
            "build_job_from_detail_page: rejected non-job page company=%s url=%s title=%r",
            company.name, url, title,
        )
        return None
    location = guess_location_from_text(page_text)
    posted_dt = guess_posted_from_text(page_text)
    return make_job(
        company=company,
        title=title,
        location=location,
        url=url,
        source=source,
        description=page_text,
        posted_dt=posted_dt,
        rejected_jobs=rejected_jobs,
    )

def extract_jobs_from_listing_blocks(
    company: Company,
    listing_url: str,
    html: str,
    source: str,
    rejected_jobs: Optional[List[RejectedJob]] = None,
) -> List[Job]:
    if html is None:
        return []
    soup = make_soup(html)
    jobs: List[Job] = []
    seen: set[str] = set()
    for a in soup.find_all("a", href=True):
        href = a.get("href") or ""
        title = clean(a.get_text(" ", strip=True))
        if not is_probable_job_link(title, href):
            continue
        full = urljoin(listing_url, href)
        if full in seen or not is_allowed_detail_url(company, listing_url, full):
            continue
        container = a
        for _ in range(5):
            parent = getattr(container, "parent", None)
            if not parent or getattr(parent, "name", None) in {"html", "body"}:
                break
            text_len = len(clean(parent.get_text(" ", strip=True)))
            if 40 <= text_len <= 1200:
                container = parent
            else:
                break
        block_text = clean(container.get_text(" ", strip=True))
        if not block_text or len(block_text) < len(title):
            block_text = title
        posted_dt = guess_posted_from_text(block_text)
        location = ""
        loc_m = re.search(r"Location\s*:?\s*([^|•]+?)(?:Date Posted|Posted|Remote|Hybrid|Onsite|$)", block_text, flags=re.I)
        if loc_m:
            location = clean(loc_m.group(1))
        if not location:
            location = guess_location_from_text(block_text)
        seen.add(full)
        job = make_job(
            company=company,
            title=title,
            location=location,
            url=full,
            source=source,
            description=block_text,
            posted_dt=posted_dt,
            rejected_jobs=rejected_jobs,
        )
        if job:
            jobs.append(job)
    return jobs


def generic_site(company: Company, rejected_jobs: Optional[List[RejectedJob]] = None) -> List[Job]:
    jobs: List[Job] = []
    seen_urls = set()
    for listing_url in company_discovery_urls(company):
        if url_is_blacklisted(listing_url):
            logging.info("generic_site skipping blacklisted listing url company=%s url=%s", company.name, listing_url)
            continue
        try:
            html = fetch_text(listing_url, referer=company.careers_url)
        except Exception as e:
            logging.warning("generic_site listing fetch failed company=%s url=%s err=%s", company.name, listing_url, e)
            continue

        if not html or not clean(html):
            logging.warning("generic_site listing empty company=%s url=%s", company.name, listing_url)
            continue
        source = host_label(listing_url)
        if html is None:
            logging.warning("generic_site listing returned None company=%s url=%s", company.name, listing_url)
            continue
        soup = make_soup(html)
        for obj in extract_jsonld_jobpostings(soup):
            title = clean(obj.get("title") or obj.get("name"))
            if not title:
                continue
            url = clean(obj.get("url") or listing_url)
            if url in seen_urls:
                continue
            seen_urls.add(url)
            job = make_job(
                company=company,
                title=title,
                location=jsonld_location(obj),
                url=url,
                source=source,
                description=jsonld_description(obj),
                posted_dt=parse_date(obj.get("datePosted")),
                rejected_jobs=rejected_jobs,
            )
            if job:
                jobs.append(job)

        listing_jobs = extract_jobs_from_listing_blocks(company, listing_url, html, source, rejected_jobs=rejected_jobs)
        if listing_jobs:
            for job in listing_jobs:
                if job.url in seen_urls:
                    continue
                seen_urls.add(job.url)
                jobs.append(job)
            continue

        for detail_url in extract_job_links(company, listing_url, html):
            if detail_url in seen_urls:
                continue
            seen_urls.add(detail_url)
            try:
                detail_html = fetch_text(detail_url, referer=listing_url)
                job = build_job_from_detail_page(company, detail_url, detail_html, source, rejected_jobs=rejected_jobs)
                if job:
                    jobs.append(job)
            except Exception as e:
                logging.warning("generic_site detail fetch failed company=%s url=%s err=%s", company.name, detail_url, e)
                continue
    return jobs

def extract_assignment_json(script_text: str) -> List[Any]:
    candidates: List[Any] = []
    patterns = [
        r"__NEXT_DATA__\s*=\s*({.*?})\s*;",
        r"__INITIAL_STATE__\s*=\s*({.*?})\s*;",
        r"__PRELOADED_STATE__\s*=\s*({.*?})\s*;",
        r"phApp\.ddo\s*=\s*({.*?})\s*;",
        r"phApp\.state\s*=\s*({.*?})\s*;",
    ]
    for pattern in patterns:
        for m in re.finditer(pattern, script_text, flags=re.S):
            payload = safe_json_loads(m.group(1))
            if payload is not None:
                candidates.append(payload)
    return candidates


def discover_workday_jobs_endpoints(careers_url: str) -> List[str]:
    endpoints: List[str] = []
    try:
        html = fetch_text(careers_url, retries=1, referer=careers_url)
    except Exception:
        html = ""
    if html:
        for match in re.finditer(r"https?://[^\"'\s]+/wday/cxs/[^\"'\s]+/jobs", html, flags=re.I):
            endpoints.append(match.group(0))
        for match in re.finditer(r"(/wday/cxs/[^\"'\s]+/jobs)", html, flags=re.I):
            endpoints.append(urljoin(careers_url, match.group(1)))
        soup = make_soup(html)
        for script in soup.find_all("script"):
            raw = script.string or script.get_text(" ", strip=True)
            if not raw:
                continue
            for payload in extract_assignment_json(raw):
                text_blob = json.dumps(payload)
                for match in re.finditer(r"https?://[^\"'\s]+/wday/cxs/[^\"'\s]+/jobs", text_blob, flags=re.I):
                    endpoints.append(match.group(0))
                for match in re.finditer(r"(/wday/cxs/[^\"'\s]+/jobs)", text_blob, flags=re.I):
                    endpoints.append(urljoin(careers_url, match.group(1)))
    if endpoints:
        cleaned = [sanitize_url(ep) for ep in endpoints]
        return unique_preserve([ep for ep in cleaned if ep])

    host, tenant, site_candidates = workday_context(careers_url)
    for site in site_candidates:
        endpoints.append(f"https://{host}/wday/cxs/{tenant}/{site}/jobs")
    return unique_preserve(endpoints)


def workday_detail_url_from_endpoint(endpoint: str, external_path: str) -> str:
    parsed = urlparse(endpoint)
    parts = [p for p in parsed.path.split('/') if p]
    if len(parts) >= 5 and parts[0] == 'wday' and parts[1] == 'cxs':
        site = parts[4]
        return f"https://{parsed.netloc}/en-US/{site}/job/{external_path.lstrip('/')}"
    return f"https://{parsed.netloc}/job/{external_path.lstrip('/')}"


def workday_context(careers_url: str) -> Tuple[str, str, List[str]]:
    parsed = urlparse(careers_url)
    host = parsed.netloc.split(":")[0]
    tenant = host.split(".")[0]
    raw_segments = [seg for seg in parsed.path.split("/") if seg]
    segments = []
    for seg in raw_segments:
        if re.fullmatch(r"[a-z]{2}(?:-[A-Z]{2})?", seg):
            continue
        if seg.lower() in {"job", "jobs", "search-results"}:
            continue
        segments.append(seg)
    sites = unique_preserve(segments + [tenant])
    return host, tenant, sites

def workday_listing_payload_variants(limit: int, offset: int) -> List[Dict[str, Any]]:
    return [
        {"limit": limit, "offset": offset, "appliedFacets": {}},
        {"limit": limit, "offset": offset, "appliedFacets": {}, "searchText": ""},
        {"limit": limit, "offset": offset, "searchText": ""},
        {"limit": limit, "offset": offset},
    ]


def fetch_workday_listing(endpoint: str, limit: int, offset: int, referer: Optional[str], company_name: str, site: str) -> Any:
    last_err: Optional[Exception] = None
    for idx, payload in enumerate(workday_listing_payload_variants(limit, offset), start=1):
        try:
            return fetch_json_post(endpoint, payload, retries=1, referer=referer)
        except Exception as e:
            last_err = e
            status = exception_status_code(e)
            logging.debug(
                "workday listing variant failed company=%s site=%s variant=%s/%s status=%s endpoint=%s payload_keys=%s err=%s",
                company_name,
                site,
                idx,
                len(workday_listing_payload_variants(limit, offset)),
                status,
                endpoint,
                sorted(payload.keys()),
                e,
            )
            if status == 422:
                continue
            raise
    if last_err is not None:
        block_url_family(endpoint, 8 * 60 * 60)
        record_registry_health(
            endpoint,
            status=exception_status_code(last_err),
            failure_type="request_error",
            action_taken="workday_listing_family_blocked",
            note=f"company={company_name};site={site};reason=422_after_payload_variants",
        )
        raise last_err
    raise RuntimeError(f"workday listing failed for {endpoint}")


def workday(company: Company, rejected_jobs: Optional[List[RejectedJob]] = None) -> List[Job]:
    if not company.careers_url or "myworkdayjobs" not in company.careers_url:
        return []
    host, tenant, site_candidates = workday_context(company.careers_url)
    jobs: List[Job] = []
    seen_urls = set()

    for site in site_candidates:
        endpoint = f"https://{host}/wday/cxs/{tenant}/{site}/jobs"
        offset = 0
        limit = 20
        success = False
        while offset < 400:
            try:
                data = fetch_workday_listing(endpoint, limit, offset, company.careers_url or endpoint, company.name, site)
            except Exception as e:
                logging.warning("workday listing fetch failed company=%s endpoint=%s err=%s", company.name, endpoint, e)
                break
            postings = data.get("jobPostings") or []
            total = int(data.get("total") or len(postings) or 0)
            if not postings:
                break
            success = True
            for raw in postings:
                title = clean(raw.get("title") or raw.get("jobTitle") or raw.get("postingTitle"))
                location = clean(raw.get("locationsText") or raw.get("location") or " / ".join(raw.get("locations") or []))
                external_path = clean(raw.get("externalPath") or raw.get("externalUrl") or "")
                posted_dt = parse_date(raw.get("postedOn")) or parse_date(raw.get("postedDate")) or parse_date(raw.get("startDate"))
                detail_desc = ""
                detail_url = ""
                if external_path:
                    # Strip leading '/job/' if present — some Workday instances return
                    # externalPath as '/job/<slug>' which would produce a double '/job//job/' URL.
                    _ep = external_path.lstrip('/')
                    if _ep.startswith('job/'):
                        _ep = _ep[4:]
                    detail_endpoint = f"https://{host}/wday/cxs/{tenant}/{site}/job/{_ep}"
                    if not workday_detail_family_is_blocked(host, site):
                        try:
                            detail = fetch_json_post(detail_endpoint, {}, retries=1, referer=endpoint)
                            info = detail.get("jobPostingInfo") or detail.get("jobPosting") or detail
                            detail_desc = clean(" ".join([
                                clean(info.get("jobDescription")),
                                clean(info.get("jobResponsibilities")),
                                clean(info.get("requiredQualifications")),
                                clean(info.get("preferredQualifications")),
                            ]))
                        except Exception as e:
                            status = exception_status_code(e)
                            if status in {500, 502, 503, 504}:
                                block_workday_detail_family(host, site, 8 * 60 * 60)
                                record_registry_health(
                                    detail_endpoint,
                                    status=status,
                                    failure_type="server_error",
                                    action_taken="workday_detail_family_blocked",
                                    note=f"company={company.name};site={site}",
                                )
                            logging.debug("workday detail fetch failed company=%s site=%s path=%s err=%s", company.name, site, external_path, e)
                    detail_url = f"https://{host}/en-US/{site}/job/{_ep}"
                desc = clean(" ".join([
                    title,
                    location,
                    " ".join([clean(x) for x in (raw.get("bulletFields") or [])]),
                    detail_desc,
                ]))
                if not title:
                    continue
                job_url = detail_url or company.careers_url or endpoint
                if job_url in seen_urls:
                    continue
                seen_urls.add(job_url)
                job = make_job(
                    company=company,
                    title=title,
                    location=location,
                    url=job_url,
                    source="Workday API",
                    description=desc,
                    posted_dt=posted_dt,
                    rejected_jobs=rejected_jobs,
                )
                if job:
                    jobs.append(job)
            offset += limit
            if total and offset >= total:
                break
        if success and jobs:
            break
    return jobs

def ashby(company: Company, rejected_jobs: Optional[List[RejectedJob]] = None) -> List[Job]:
    if not company.adapter_key:
        return []
    url = f"https://api.ashbyhq.com/posting-api/job-board/{company.adapter_key}?includeCompensation=true"
    data = fetch_json(url)
    jobs: List[Job] = []
    for raw in data.get("jobs", []):
        locations = [clean(raw.get("location"))]
        for item in raw.get("secondaryLocations") or []:
            locations.append(clean(item.get("location")))
        location = " / ".join([x for x in unique_preserve(locations) if x])
        # Honour Ashby's dedicated remote flags — a job can be remote even if the
        # location field only lists an office city.
        is_remote_flag = raw.get("isRemote") or (raw.get("workplaceType") or "").lower() == "remote"
        if is_remote_flag and "remote" not in location.lower():
            location = f"{location} / Remote" if location else "Remote"
        comp = raw.get("compensation") or {}
        desc = " ".join([
            clean(raw.get("descriptionHtml") or raw.get("descriptionPlain")),
            clean(comp.get("summary")),
        ])
        posted_dt = parse_date(raw.get("publishedAt"))
        job = make_job(
            company=company,
            title=clean(raw.get("title")),
            location=location,
            url=clean(raw.get("jobUrl")),
            source="Ashby",
            description=desc,
            posted_dt=posted_dt,
            rejected_jobs=rejected_jobs,
        )
        if job:
            jobs.append(job)
    return jobs

def auto_discover(company: Company, rejected_jobs: Optional[List[RejectedJob]] = None) -> List[Job]:
    jobs: List[Job] = []
    all_urls = company_discovery_urls(company)
    if company.careers_url and "myworkdayjobs" in company.careers_url:
        jobs.extend(workday(company, rejected_jobs=rejected_jobs))
    if company.adapter == "ashby" or any("ashbyhq.com" in url for url in all_urls):
        jobs.extend(ashby(company, rejected_jobs=rejected_jobs))
    jobs.extend(generic_site(company, rejected_jobs=rejected_jobs))
    return dedupe(jobs)

# ============================================================
# ADAPTERS
# ============================================================

def greenhouse(company: Company, rejected_jobs: Optional[List[RejectedJob]] = None) -> List[Job]:
    url = f"https://boards-api.greenhouse.io/v1/boards/{company.adapter_key}/jobs?content=true"
    data = fetch_json(url)
    jobs: List[Job] = []

    for raw in data.get("jobs", []):
        job = make_job(
            company=company,
            title=raw.get("title", ""),
            location=(raw.get("location") or {}).get("name", ""),
            url=raw.get("absolute_url", ""),
            source="Greenhouse",
            description=raw.get("content", ""),
            posted_dt=parse_date(raw.get("updated_at")) or parse_date(raw.get("created_at")),
            rejected_jobs=rejected_jobs,
        )
        if job:
            jobs.append(job)

    return jobs

def lever(company: Company, rejected_jobs: Optional[List[RejectedJob]] = None) -> List[Job]:
    url = f"https://api.lever.co/v0/postings/{company.adapter_key}?mode=json"
    data = fetch_json(url)
    jobs: List[Job] = []

    for raw in data:
        lever_location = (raw.get("categories") or {}).get("location", "")
        # Lever's workplaceType field ("remote"/"hybrid"/"on-site") is more reliable
        # than the location string for remote detection.
        lever_workplace = (raw.get("workplaceType") or "").lower()
        if lever_workplace == "remote" and "remote" not in lever_location.lower():
            lever_location = f"{lever_location} / Remote" if lever_location else "Remote"
        job = make_job(
            company=company,
            title=raw.get("text", ""),
            location=lever_location,
            url=raw.get("hostedUrl", ""),
            source="Lever",
            description=raw.get("descriptionPlain", ""),
            posted_dt=from_ms(raw.get("createdAt")),
            rejected_jobs=rejected_jobs,
        )
        if job:
            jobs.append(job)

    return jobs

def blackrock(company: Company, rejected_jobs: Optional[List[RejectedJob]] = None) -> List[Job]:
    html = fetch_text(company.careers_url or "")
    soup = make_soup(html)
    jobs: List[Job] = []

    for a in soup.find_all("a", href=True):
        href = a["href"]
        text = clean(a.get_text(" ", strip=True))
        if not text or "save for later" in text.lower():
            continue
        if href.startswith("/") and "/jobs/" in href:
            full = urljoin(company.careers_url or "", href)
            title = text.split(" Location: ")[0]
            loc = text.split(" Location: ")[1] if " Location: " in text else ""
            job = make_job(company, title, loc, full, "BlackRock Careers", text, None, rejected_jobs=rejected_jobs)
            if job:
                jobs.append(job)

    return jobs

def schwab(company: Company, rejected_jobs: Optional[List[RejectedJob]] = None) -> List[Job]:
    html = fetch_text(company.careers_url or "")
    soup = make_soup(html)
    links: List[str] = []
    seen = set()

    for a in soup.find_all("a", href=True):
        href = a["href"]
        if re.search(r"/job/|/jobs/", href):
            full = urljoin(company.careers_url or "", href)
            if full not in seen:
                seen.add(full)
                links.append(full)

    jobs: List[Job] = []

    for url in links[:60]:
        try:
            jhtml = fetch_text(url)
            jsoup = make_soup(jhtml)
            title = clean((jsoup.select_one("h1") or jsoup.select_one("title") or jsoup.new_tag("x")).get_text(" ", strip=True))
            if not title:
                continue
            page = clean(jsoup.get_text(" ", strip=True))
            loc = ""
            m = re.search(r"([A-Za-z .'-]+,\s*[A-Z]{2})", page)
            if m:
                loc = clean(m.group(1))
            job = make_job(company, title, loc, url, "Schwab Careers", page[:2500], None, rejected_jobs=rejected_jobs)
            if job:
                jobs.append(job)
        except Exception as e:
            logging.warning("Schwab detail page failed: %s", e)
            continue

    return jobs

def spglobal(company: Company, rejected_jobs: Optional[List[RejectedJob]] = None) -> List[Job]:
    html = fetch_text(company.careers_url or "")
    soup = make_soup(html)
    links: List[str] = []
    seen = set()

    for a in soup.find_all("a", href=True):
        href = a["href"]
        if re.search(r"/jobs/\d+", href):
            full = urljoin(company.careers_url or "", href)
            if full not in seen:
                seen.add(full)
                links.append(full)

    jobs: List[Job] = []

    for url in links[:80]:
        try:
            jhtml = fetch_text(url)
            jsoup = make_soup(jhtml)
            title = clean((jsoup.select_one("h1") or jsoup.new_tag("x")).get_text(" ", strip=True))
            if not title:
                continue
            page = clean(jsoup.get_text(" ", strip=True))
            job = make_job(company, title, "", url, "S&P Global Careers", page[:2500], None, rejected_jobs=rejected_jobs)
            if job:
                jobs.append(job)
        except Exception as e:
            logging.warning("S&P Global detail page failed: %s", e)
            continue

    return jobs

def manual_flag(company: Company, rejected_jobs: Optional[List[RejectedJob]] = None) -> List[Job]:
    url = company.careers_url or f"https://{company.domain}"
    label = "Workday" if company.adapter == "workday_manual" else "Manual"

    job = make_job(
        company=company,
        title=f"[{label} — manual check] {company.name}",
        location="Check career page",
        url=url,
        source=f"{label} Manual",
        description=f"Manual review required. Career page: {url}. Notes: {company.notes}",
        posted_dt=None,
        is_manual=True,
        rejected_jobs=rejected_jobs,
    )
    return [job] if job else []


def smartrecruiters(company: Company, rejected_jobs: Optional[List[RejectedJob]] = None) -> List[Job]:
    """SmartRecruiters public JSON API. adapter_key = company slug from careers.smartrecruiters.com URL."""
    slug = company.adapter_key or company.name.lower().replace(" ", "")
    url = f"https://api.smartrecruiters.com/v1/companies/{slug}/postings"
    try:
        data = fetch_json(url)
    except Exception as e:
        logging.warning("SmartRecruiters API failed company=%s err=%s", company.name, e)
        return []
    jobs: List[Job] = []
    for raw in data.get("content", []):
        loc_obj = raw.get("location") or {}
        city    = loc_obj.get("city") or ""
        country = loc_obj.get("country") or ""
        remote  = loc_obj.get("remote") or False
        location = "Remote" if remote else ", ".join(filter(None, [city, country]))
        job = make_job(
            company     = company,
            title       = raw.get("name", ""),
            location    = location,
            url         = raw.get("ref", ""),
            source      = "SmartRecruiters",
            description = "",
            posted_dt   = parse_date(raw.get("releasedDate")),
            rejected_jobs = rejected_jobs,
        )
        if job:
            jobs.append(job)
    return jobs


def icims(company: Company, rejected_jobs: Optional[List[RejectedJob]] = None) -> List[Job]:
    """iCIMS HTML scraper. careers_url should be the company's iCIMS search page."""
    base_url = company.careers_url or ""
    if not base_url:
        return []
    # Try common iCIMS search URL patterns
    search_urls = [base_url]
    if "icims.com" in base_url and "search" not in base_url:
        search_urls.append(base_url.rstrip("/") + "/jobs/search")
    jobs: List[Job] = []
    seen: set = set()
    for search_url in search_urls[:2]:
        try:
            html = fetch_text(search_url, referer=search_url)
        except Exception as e:
            logging.warning("iCIMS listing failed company=%s url=%s err=%s", company.name, search_url, e)
            continue
        soup = make_soup(html)
        for a in soup.find_all("a", href=True):
            href = str(a["href"])
            # iCIMS job detail links match /jobs/{id}/job
            if re.search(r"/jobs/\d+/job", href):
                full_url = urljoin(search_url, href)
                if full_url in seen:
                    continue
                seen.add(full_url)
                if len(seen) > MAX_DETAIL_LINKS:
                    break
                try:
                    detail_html = fetch_text(full_url, referer=search_url)
                except Exception as e:
                    logging.warning("iCIMS detail failed url=%s err=%s", full_url, e)
                    continue
                detail_soup = make_soup(detail_html)
                # Extract title — iCIMS job title is usually in h1 or .iCIMS_JobTitle
                title_el = (detail_soup.find(class_=re.compile(r"iCIMS_JobTitle", re.I))
                            or detail_soup.find("h1"))
                title = clean(title_el.get_text(" ", strip=True)) if title_el else ""
                if not title:
                    continue
                # Location
                loc_el = detail_soup.find(class_=re.compile(r"iCIMS_JobHeaderField.*location|jobLocation", re.I))
                location = clean(loc_el.get_text(" ", strip=True)) if loc_el else ""
                # Description
                desc_el = detail_soup.find(class_=re.compile(r"iCIMS_JobContent|iCIMS_Expandable_Text", re.I))
                description = clean(desc_el.get_text(" ", strip=True)) if desc_el else ""
                job = make_job(
                    company     = company,
                    title       = title,
                    location    = location,
                    url         = full_url,
                    source      = "iCIMS",
                    description = description,
                    posted_dt   = None,
                    rejected_jobs = rejected_jobs,
                )
                if job:
                    jobs.append(job)
        if jobs:
            break  # Got results from first working URL
    return dedupe(jobs)


def indeed_search(company: Company, rejected_jobs: Optional[List[RejectedJob]] = None) -> List[Job]:
    """
    Searches Indeed for jobs at a specific company.
    careers_url should be the Indeed company jobs page:
      https://www.indeed.com/cmp/{slug}/jobs
    If careers_url is missing, builds a URL from company.name.
    """
    if company.careers_url and "indeed.com" in company.careers_url:
        search_url = company.careers_url
    else:
        slug = re.sub(r"[^a-z0-9-]", "-", company.name.lower()).strip("-")
        search_url = f"https://www.indeed.com/cmp/{slug}/jobs"
    try:
        html = fetch_text(search_url, referer="https://www.indeed.com")
    except Exception as e:
        logging.warning("Indeed search failed company=%s url=%s err=%s", company.name, search_url, e)
        return []
    soup = make_soup(html)
    jobs: List[Job] = []
    seen: set = set()
    # Indeed job links: /viewjob?jk=... or /rc/clk?jk=...
    for a in soup.find_all("a", href=True):
        href = str(a["href"])
        if not re.search(r"/(viewjob|rc/clk)\?.*jk=", href) and "/jobs/view" not in href:
            continue
        full_url = urljoin("https://www.indeed.com", href)
        # Normalise: strip tracking params, keep jk=
        m_jk = re.search(r"jk=([a-f0-9]+)", full_url)
        canonical = f"https://www.indeed.com/viewjob?jk={m_jk.group(1)}" if m_jk else full_url
        if canonical in seen:
            continue
        seen.add(canonical)
        if len(seen) > 40:
            break
        # Title from link text or data attribute
        title = clean(a.get_text(" ", strip=True))
        if not title or len(title) < 4:
            parent = a.find_parent(class_=re.compile(r"jobCard|job_seen_beacon|tapItem", re.I))
            if parent:
                t_el = parent.find(class_=re.compile(r"jobTitle|title", re.I))
                if t_el:
                    title = clean(t_el.get_text(" ", strip=True))
        if not title:
            continue
        job = make_job(
            company     = company,
            title       = title,
            location    = "",
            url         = canonical,
            source      = "Indeed",
            description = "",
            posted_dt   = None,
            rejected_jobs = rejected_jobs,
        )
        if job:
            jobs.append(job)
    return dedupe(jobs)


def run_adapter(company: Company, rejected_jobs: Optional[List[RejectedJob]] = None) -> List[Job]:
    set_run_context("company", company.name, company.adapter, company.careers_url or "")
    try:
        if company.adapter == "greenhouse":
            jobs = greenhouse(company, rejected_jobs=rejected_jobs)
            RUNTIME_COMPANY_STATUS[company.name] = "✓"
            return jobs
        if company.adapter == "lever":
            jobs = lever(company, rejected_jobs=rejected_jobs)
            RUNTIME_COMPANY_STATUS[company.name] = "✓"
            return jobs
        if company.adapter == "ashby":
            jobs = ashby(company, rejected_jobs=rejected_jobs)
            RUNTIME_COMPANY_STATUS[company.name] = "✓"
            return jobs
        if company.adapter == "workday":
            jobs = workday(company, rejected_jobs=rejected_jobs)
            if jobs:
                RUNTIME_COMPANY_STATUS[company.name] = "✓"
                return jobs
            RUNTIME_COMPANY_STATUS[company.name] = "manual fallback"
            return manual_flag(company, rejected_jobs=rejected_jobs)
        if company.adapter == "custom_site":
            jobs = generic_site(company, rejected_jobs=rejected_jobs)
            if jobs:
                RUNTIME_COMPANY_STATUS[company.name] = "custom scraper"
                return jobs
            # Deep search fallback for JS-rendered custom sites
            if _deep_search_enabled():
                logging.info("deep_search fallback for custom_site company=%s", company.name)
                raw = _deep_search_mod.scrape_jobs_generic(company.careers_url or "", company.name)
                jobs = _convert_raw_jobs(company, raw, rejected_jobs)
                if jobs:
                    RUNTIME_COMPANY_STATUS[company.name] = "deep search"
                    return jobs
            RUNTIME_COMPANY_STATUS[company.name] = "manual fallback: bad URL" if company_health_rows(company.name) else "manual fallback"
            return manual_flag(company, rejected_jobs=rejected_jobs)
        if company.adapter == "custom_blackrock":
            if _deep_search_enabled():
                raw = _deep_search_mod.scrape_jobs_blackrock(company.careers_url or "", company.name)
                jobs = _convert_raw_jobs(company, raw, rejected_jobs)
                if jobs:
                    RUNTIME_COMPANY_STATUS[company.name] = "deep search"
                    return jobs
            jobs = blackrock(company, rejected_jobs=rejected_jobs)
            RUNTIME_COMPANY_STATUS[company.name] = "custom scraper"
            return jobs
        if company.adapter == "custom_schwab":
            if _deep_search_enabled():
                raw = _deep_search_mod.scrape_jobs_schwab(company.careers_url or "", company.name)
                jobs = _convert_raw_jobs(company, raw, rejected_jobs)
                if jobs:
                    RUNTIME_COMPANY_STATUS[company.name] = "deep search"
                    return jobs
            jobs = schwab(company, rejected_jobs=rejected_jobs)
            RUNTIME_COMPANY_STATUS[company.name] = "custom scraper"
            return jobs
        if company.adapter == "custom_spglobal":
            if _deep_search_enabled():
                raw = _deep_search_mod.scrape_jobs_spglobal(company.careers_url or "", company.name)
                jobs = _convert_raw_jobs(company, raw, rejected_jobs)
                if jobs:
                    RUNTIME_COMPANY_STATUS[company.name] = "deep search"
                    return jobs
            jobs = spglobal(company, rejected_jobs=rejected_jobs)
            RUNTIME_COMPANY_STATUS[company.name] = "custom scraper"
            return jobs
        if company.adapter == "smartrecruiters":
            jobs = smartrecruiters(company, rejected_jobs=rejected_jobs)
            if jobs:
                RUNTIME_COMPANY_STATUS[company.name] = "✓"
                return jobs
            RUNTIME_COMPANY_STATUS[company.name] = "manual fallback"
            return manual_flag(company, rejected_jobs=rejected_jobs)
        if company.adapter == "icims":
            jobs = icims(company, rejected_jobs=rejected_jobs)
            if jobs:
                RUNTIME_COMPANY_STATUS[company.name] = "✓"
                return jobs
            RUNTIME_COMPANY_STATUS[company.name] = "manual fallback"
            return manual_flag(company, rejected_jobs=rejected_jobs)
        if company.adapter == "indeed_search":
            jobs = indeed_search(company, rejected_jobs=rejected_jobs)
            if jobs:
                RUNTIME_COMPANY_STATUS[company.name] = "✓ (Indeed)"
                return jobs
            RUNTIME_COMPANY_STATUS[company.name] = "manual fallback"
            return manual_flag(company, rejected_jobs=rejected_jobs)
        # --- jobs_api_url: hit the discovered JSON endpoint directly ---
        if company.jobs_api_url:
            jobs = jobs_api_json(company, rejected_jobs=rejected_jobs)
            if jobs:
                RUNTIME_COMPANY_STATUS[company.name] = "✓ (Jobs API)"
                return jobs
            # API returned nothing — fall through to normal adapter logic

        # --- render_required: JS-heavy page, use deep search if available ---
        if company.render_required:
            if _deep_search_enabled():
                logging.info("render_required + deep_search enabled for %s", company.name)
                raw = _deep_search_mod.scrape_jobs_generic(company.careers_url or "", company.name)
                jobs = _convert_raw_jobs(company, raw, rejected_jobs)
                if jobs:
                    RUNTIME_COMPANY_STATUS[company.name] = "deep search"
                    return jobs
            logging.info("render_required set for %s — skipping generic scraping", company.name)
            RUNTIME_COMPANY_STATUS[company.name] = "render required (skipped)"
            return manual_flag(company, rejected_jobs=rejected_jobs)

        if company.adapter in ("workday_manual", "custom_manual"):
            jobs = auto_discover(company, rejected_jobs=rejected_jobs)
            if jobs:
                RUNTIME_COMPANY_STATUS[company.name] = "custom scraper"
                return jobs
            RUNTIME_COMPANY_STATUS[company.name] = "manual fallback: bad URL" if company_health_rows(company.name) else "manual placeholder"
            return manual_flag(company, rejected_jobs=rejected_jobs)
        jobs = auto_discover(company, rejected_jobs=rejected_jobs)
        if jobs:
            RUNTIME_COMPANY_STATUS[company.name] = "custom scraper"
            return jobs
        RUNTIME_COMPANY_STATUS[company.name] = "manual fallback: bad URL" if company_health_rows(company.name) else "manual fallback"
        return manual_flag(company, rejected_jobs=rejected_jobs)
    except Exception as e:
        record_registry_health(company.careers_url or "", failure_type=classify_failure(company.careers_url or "", None, e), action_taken="adapter_error", note=str(e))
        logging.error("adapter error for %s: %s", company.name, e)
        RUNTIME_COMPANY_STATUS[company.name] = "manual fallback: adapter error"
        return manual_flag(company, rejected_jobs=rejected_jobs)
    finally:
        clear_run_context()

def dedupe_key(job: Job) -> Tuple[str, str, str]:
    return (
        job.company.lower().strip(),
        job.normalized_title,
        job.canonical_url,
    )

def dedupe(jobs: List[Job]) -> List[Job]:
    jobs = sorted(
        jobs,
        key=lambda j: (
            j.company.lower(),
            j.normalized_title,
            -j.source_trust_adjustment,
            -j.score,
        ),
    )

    seen = set()
    out: List[Job] = []

    for j in jobs:
        key = dedupe_key(j)
        if key not in seen:
            seen.add(key)
            out.append(j)

    return sorted(out, key=display_sort_key)

def cap_per_company(jobs: List[Job], n: int) -> List[Job]:
    grouped: Dict[str, List[Job]] = {}
    for j in jobs:
        grouped.setdefault(j.company, []).append(j)

    out: List[Job] = []
    for items in grouped.values():
        items.sort(key=display_sort_key)
        selected: List[Job] = []
        service_count = 0
        for job in items:
            if len(selected) >= n:
                break
            if getattr(job, "lane_cohort", "") == "services_implementation":
                if service_count >= MAX_SERVICE_HEAVY_PER_COMPANY:
                    continue
                service_count += 1
            selected.append(job)
        out.extend(selected)

    return sorted(out, key=display_sort_key)


# ============================================================
# HISTORY
# ============================================================

def load_history() -> Dict[str, Dict]:
    p = Path(HISTORY_JSON)
    if not p.exists():
        return {}
    try:
        return json.loads(p.read_text(encoding="utf-8"))
    except Exception:
        return {}

def save_history(history: Dict) -> None:
    Path(HISTORY_JSON).write_text(json.dumps(history, indent=2), encoding="utf-8")

def apply_history(jobs: List[Job], history: Dict) -> None:
    now = iso_now()
    for j in jobs:
        jid = job_id(j.company, j.title, j.canonical_url)
        if jid in history:
            j.is_new = False
            j.seen_first_at = history[jid].get("seen_first_at", "")
            j.seen_last_at = history[jid].get("seen_last_at", "")
        else:
            j.is_new = True
            j.seen_first_at = now
            j.seen_last_at = now

def update_history(jobs: List[Job], history: Dict) -> Dict:
    now = iso_now()
    out = dict(history)

    for j in jobs:
        jid = job_id(j.company, j.title, j.canonical_url)
        if jid not in out:
            out[jid] = {
                "company": j.company,
                "title": j.title,
                "canonical_url": j.canonical_url,
                "seen_first_at": now,
                "seen_last_at": now,
            }
        else:
            out[jid]["seen_last_at"] = now

    return out


# ============================================================
# OUTPUT
# ============================================================

TRACKER_COLS = [
    "is_new", "action_bucket", "fit_band", "score",
    "company", "tier", "priority",
    "title", "normalized_title", "role_family", "role_alignment_score", "role_alignment_label", "lane_fit_score", "lane_fit_label", "benchmark_lane", "benchmark_match_score", "benchmark_match_reason", "lane_cohort", "source_trust_label", "source_trust_adjustment",
    "primary_apply_domain", "apply_domains_hit", "skip_domains_hit",
    "decision_reason",
    "location", "is_remote", "is_hybrid", "is_non_us",
    "salary_range", "salary_currency", "salary_low", "salary_high", "salary_period",
    "age_days", "posted_at",
    "source", "url",
    "matched_keywords", "penalized_keywords",
    "description_excerpt", "company_notes",
    "seen_first_at", "seen_last_at",
    "applied", "applied_date", "stage", "notes",
]

def to_df(jobs: List[Job]) -> pd.DataFrame:
    rows = []
    for j in jobs:
        d = asdict(j)
        d["applied"] = ""
        d["applied_date"] = ""
        d["stage"] = ""
        d["notes"] = ""
        rows.append({col: d.get(col, "") for col in TRACKER_COLS})
    return pd.DataFrame(rows, columns=TRACKER_COLS)


REJECTED_COLS = [
    "drop_stage", "drop_reason_code", "drop_reason_detail", "drop_reason", "title_gate_matched_keywords", "title_rescue_bucket", "title_rescue_trigger", "adjacent_title_marker_hit", "adjacent_domain_terms_hit", "normalized_title_for_adjacent_match", "adjacent_title_patterns_checked", "adjacent_title_patterns_hit", "adjacent_title_matched_token_slice", "adjacent_title_tokens_debug", "adjacent_pattern_tokens_debug", "adjacent_match_attempts", "decision_score", "score_threshold_used", "base_score_before_rescue", "adjacent_rescue_bonus_applied", "final_score_after_rescue",
    "company", "tier", "priority",
    "title", "normalized_title", "role_family", "role_alignment_score", "role_alignment_label", "lane_fit_score", "lane_fit_label", "benchmark_lane", "benchmark_match_score", "benchmark_match_reason", "lane_cohort", "source_trust_label", "source_trust_adjustment",
    "location", "is_remote", "is_hybrid", "is_non_us",
    "score", "fit_band", "action_bucket",
    "apply_domains_hit", "skip_domains_hit", "primary_apply_domain",
    "salary_range", "salary_currency", "salary_low", "salary_high", "salary_period",
    "age_days", "posted_at",
    "source", "url",
    "matched_keywords", "penalized_keywords",
    "description_excerpt", "company_notes",
]

def rejected_to_df(rejected_jobs: List[RejectedJob]) -> pd.DataFrame:
    rows = []
    for r in rejected_jobs:
        d = asdict(r)
        rows.append({col: d.get(col, "") for col in REJECTED_COLS})
    return pd.DataFrame(rows, columns=REJECTED_COLS)

def rejection_summary(rejected_jobs: List[RejectedJob]) -> pd.DataFrame:
    if not rejected_jobs:
        return pd.DataFrame(columns=["drop_stage", "drop_reason_code", "drop_reason_detail", "count"])

    df = rejected_to_df(rejected_jobs)
    summary = (
        df.groupby(["drop_stage", "drop_reason_code", "drop_reason_detail"], dropna=False)
        .size()
        .reset_index(name="count")
        .sort_values(["count", "drop_stage", "drop_reason_code"], ascending=[False, True, True])
    )
    return summary


def partition_manual_jobs(jobs: List[Job]) -> Tuple[List[Job], List[Job]]:
    real_jobs = [j for j in jobs if not j.manual_review]
    manual_jobs = [j for j in jobs if j.manual_review]
    return real_jobs, manual_jobs



def company_health_rows(company_name: str) -> List[Dict[str, Any]]:
    rows = [row for row in REGISTRY_HEALTH.values() if row.get("context_kind") == "company" and row.get("name") == company_name]
    return sorted(rows, key=lambda r: (int(r.get("failure_count", 0)), clean(str(r.get("failure_type", "")))), reverse=True)


def company_status_label(company: Company, jobs: List[Job], real_kept: List[Job], manual_kept: List[Job]) -> str:
    failures = {row.get("failure_type", "") for row in company_health_rows(company.name)}
    if company.name in RUNTIME_COMPANY_STATUS:
        return RUNTIME_COMPANY_STATUS[company.name]
    if company.adapter in {"greenhouse", "lever", "ashby", "workday"} and not manual_kept:
        return "✓"
    if manual_kept and failures & PERMANENT_URL_FAILURE_TYPES:
        return "manual fallback: bad URL"
    if manual_kept and "rate_limited" in failures:
        return "manual fallback: rate-limited"
    if manual_kept and "forbidden" in failures:
        return "manual fallback: blocked"
    if company.adapter in {"custom_site", "custom_blackrock", "custom_schwab", "custom_spglobal"}:
        return "custom scraper"
    if company.adapter in {"custom_manual", "workday_manual"} and manual_kept and not real_kept:
        return "manual placeholder"
    if manual_kept:
        return "manual fallback"
    return "⚙ custom"


def registry_health_df() -> pd.DataFrame:
    cols = [
        "context_kind", "name", "adapter", "careers_url", "url", "host",
        "last_status", "failure_type", "failure_count", "action_taken",
        "suggested_action", "note",
    ]
    if not REGISTRY_HEALTH:
        return pd.DataFrame(columns=cols)
    rows = list(REGISTRY_HEALTH.values())
    df = pd.DataFrame(rows)
    for col in cols:
        if col not in df.columns:
            df[col] = ""
    return df[cols].sort_values(
        ["failure_count", "name", "failure_type", "url"],
        ascending=[False, True, True, True],
    )

def split_views(jobs: List[Job]) -> Dict[str, List[Job]]:
    return {
        "Apply Now": [j for j in jobs if j.action_bucket == "APPLY NOW"],
        "Review Today": [j for j in jobs if j.action_bucket == "REVIEW TODAY"],
        "Watch": [j for j in jobs if j.action_bucket == "WATCH"],
        "Manual Review": [j for j in jobs if j.action_bucket == "MANUAL REVIEW"],
    }

def company_summary(jobs: List[Job]) -> pd.DataFrame:
    grouped: Dict[str, List[Job]] = {}
    for j in jobs:
        grouped.setdefault(j.company, []).append(j)

    rows = []
    for company, items in grouped.items():
        best = max(items, key=lambda j: j.score)
        rows.append({
            "company": company,
            "tier": best.tier,
            "priority": best.priority,
            "jobs_found": len(items),
            "new_jobs": sum(1 for x in items if x.is_new),
            "best_score": best.score,
            "best_bucket": best.action_bucket,
            "best_title": best.title,
            "primary_apply_domain": best.primary_apply_domain,
            "salary_range": best.salary_range,
            "source": best.source,
            "notes": best.company_notes,
        })

    return pd.DataFrame(sorted(rows, key=lambda r: (-r["best_score"], r["company"])))


def _group_label_from_rejection(row: pd.Series) -> str:
    stage = str(row.get("drop_stage", ""))
    code = str(row.get("drop_reason_code", ""))
    if stage == "keep_filter":
        return "policy_filter"
    if stage == "board_prefilter" or code.startswith("board_prefilter"):
        return "board_prefilter"
    if code.startswith("title_fail"):
        return "title_gate"
    if code in {"no_apply_domain", "skip_domain_only"}:
        return "domain_gate"
    if code.startswith("role_lane_exclude"):
        return "role_lane_exclude"
    if code.startswith("score_below_threshold"):
        return "score_threshold"
    return stage or "other"


def funnel_summary(
    raw_intake_count: int,
    unique_evaluated_jobs: List[Job],
    kept_jobs: List[Job],
    manual_jobs: List[Job],
    rejected_jobs: List[RejectedJob],
) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    evaluated_df = to_df(unique_evaluated_jobs)
    kept_df = to_df(kept_jobs)
    manual_df = to_df(manual_jobs)
    rejected_df = rejected_to_df(rejected_jobs)

    unique_evaluated_count = len(evaluated_df)
    total_kept = len(kept_df)
    total_rejected = len(rejected_df)
    total_manual = len(manual_df)

    funnel_df = pd.DataFrame([{
        "raw_intake_count": raw_intake_count,
        "unique_evaluated_count": unique_evaluated_count,
        "total_rejected": total_rejected,
        "total_real_jobs_kept": total_kept,
        "total_manual_targets": total_manual,
        "kept_rate_of_unique_evaluated": round((total_kept / unique_evaluated_count) if unique_evaluated_count else 0.0, 4),
        "unique_from_raw_rate": round((unique_evaluated_count / raw_intake_count) if raw_intake_count else 0.0, 4),
    }])

    def summarize_by(col: str) -> pd.DataFrame:
        def _empty_count_series(name: str) -> pd.Series:
            return pd.Series(dtype="int64", name=name)

        evaluated = evaluated_df.groupby(col).size().rename("evaluated") if not evaluated_df.empty else _empty_count_series("evaluated")
        kept = kept_df.groupby(col).size().rename("kept") if not kept_df.empty else _empty_count_series("kept")
        manual = manual_df.groupby(col).size().rename("manual_targets") if not manual_df.empty else _empty_count_series("manual_targets")
        rejected = rejected_df.groupby(col).size().rename("rejected") if not rejected_df.empty else _empty_count_series("rejected")
        summary = pd.concat([evaluated, kept, manual, rejected], axis=1).fillna(0).reset_index()
        if summary.empty:
            return pd.DataFrame(columns=[col, "evaluated", "kept", "manual_targets", "rejected", "kept_rate"])
        summary = summary.reindex(columns=[col, "evaluated", "kept", "manual_targets", "rejected"], fill_value=0)
        for metric in ["evaluated", "kept", "manual_targets", "rejected"]:
            summary[metric] = pd.to_numeric(summary[metric], errors="coerce").fillna(0).astype(int)
        summary["kept_rate"] = summary.apply(lambda r: round((r["kept"] / r["evaluated"]) if r["evaluated"] else 0.0, 4), axis=1)
        return summary.sort_values(["kept_rate", "kept", "evaluated", col], ascending=[False, False, False, True])

    source_summary_df = summarize_by("source")
    company_summary_df = summarize_by("company")

    if rejected_df.empty:
        reason_group_df = pd.DataFrame(columns=["reason_group", "rejected", "rejection_rate_of_all_rejections"])
    else:
        tmp = rejected_df.copy()
        tmp["reason_group"] = tmp.apply(_group_label_from_rejection, axis=1)
        reason_group_df = (
            tmp.groupby("reason_group")
            .size()
            .reset_index(name="rejected")
            .sort_values(["rejected", "reason_group"], ascending=[False, True])
        )
        total = reason_group_df["rejected"].sum()
        reason_group_df["rejection_rate_of_all_rejections"] = reason_group_df["rejected"].apply(lambda x: round((x / total) if total else 0.0, 4))

    return funnel_df, source_summary_df, company_summary_df, reason_group_df


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run the v6 job search pipeline.")
    parser.add_argument("--companies", default=CLI_COMPANIES_PATH_OVERRIDE or "", help="Path to a company-registry YAML file.")
    parser.add_argument("--test-companies", action="store_true", help="Use job_search_companies_test.yaml from the script directory.")
    parser.add_argument("--prefs", "--preferences", dest="prefs", default=CLI_PREFERENCES_PATH_OVERRIDE or "", help="Path to a preferences YAML file.")
    parser.add_argument("--company-limit", type=int, default=None, help="Process only the first N companies after filters are applied.")
    parser.add_argument("--stop-after", type=int, default=None, help="Alias for company-limit when doing shorter test runs.")
    parser.add_argument("--company-starts-after", default="", help="Skip companies until after this company name.")
    parser.add_argument("--company-allowlist", default="", help="Comma-separated list of company names to run.")
    parser.add_argument("--debug-company", default="", help="Print detailed rejection output for one company.")
    parser.add_argument("--debug-rejections-limit", type=int, default=20, help="How many rejected rows to print in debug-company mode.")
    parser.add_argument("--checkpoint-every", type=int, default=20, help="Write partial checkpoint files every N companies. Use 0 to disable.")
    parser.add_argument("--checkpoint-dir", default=DEFAULT_CHECKPOINT_DIR, help="Directory for partial checkpoint exports.")
    parser.add_argument("--skip-external-boards", action="store_true", help="Skip external board scraping during the run.")
    parser.add_argument("--deep-search", action="store_true", help="Enable Playwright-based deep search for JS-heavy careers pages (requires deep search add-on).")
    return parser.parse_args()


def _parse_name_csv(raw: str) -> List[str]:
    if not raw:
        return []
    parts = re.split(r"[,;\n]+", raw)
    out: List[str] = []
    seen: set[str] = set()
    for part in parts:
        value = clean(part).lower()
        if value and value not in seen:
            out.append(value)
            seen.add(value)
    return out


def select_companies_for_run(companies: List[Company], args: argparse.Namespace) -> List[Company]:
    selected = list(companies)

    start_after = clean(args.company_starts_after).lower()
    if start_after:
        matched_index = None
        for idx, company in enumerate(selected):
            cname = clean(company.name).lower()
            if cname == start_after or start_after in cname:
                matched_index = idx + 1
                break
        if matched_index is not None:
            selected = selected[matched_index:]

    allowlist = _parse_name_csv(args.company_allowlist)
    debug_company = clean(args.debug_company).lower()
    if debug_company and debug_company not in allowlist:
        allowlist.append(debug_company)

    if allowlist:
        allowset = set(allowlist)
        selected = [
            company for company in selected
            if clean(company.name).lower() in allowset
            or any(token in clean(company.name).lower() for token in allowset)
        ]

    limit_candidates = [value for value in [args.company_limit, args.stop_after] if isinstance(value, int) and value > 0]
    if limit_candidates:
        selected = selected[: min(limit_candidates)]

    return selected


def active_config_df() -> pd.DataFrame:
    config_df = pd.DataFrame(_flatten_preferences(PREFERENCES)) if PREFERENCES else pd.DataFrame(
        [{"path": "preferences", "value": "No YAML preferences file found. Using in-script defaults."}]
    )
    meta_rows = pd.DataFrame(
        [
            {"path": "meta.preferences_path", "value": str(PREFERENCES_PATH) if PREFERENCES_PATH else ""},
            {"path": "meta.loaded_from_yaml", "value": bool(PREFERENCES_PATH)},
            {"path": "meta.company_registry_path", "value": str(COMPANY_REGISTRY_PATH) if COMPANY_REGISTRY_PATH else ""},
            {"path": "meta.company_registry_from_yaml", "value": bool(COMPANY_REGISTRY_PATH)},
        ]
    )
    return pd.concat([meta_rows, config_df], ignore_index=True)


def _extract_keyword_terms(raw: Any) -> List[str]:
    text = clean(str(raw or ""))
    if not text:
        return []
    terms: List[str] = []
    for piece in text.split(","):
        value = piece.strip()
        if not value:
            continue
        term = re.sub(r"\([+-]?\d+\)$", "", value).strip()
        if term:
            terms.append(term)
    return terms


def rejection_detail_df(rejected_jobs: List[RejectedJob]) -> pd.DataFrame:
    base = rejected_to_df(rejected_jobs)
    if base.empty:
        return pd.DataFrame(
            columns=[
                "company", "title", "url", "drop_stage", "drop_reason", "drop_reason_code",
                "drop_reason_detail", "decision_reason", "title_gate_passed", "title_gate_reason", "title_gate_matched_keywords", "title_rescue_bucket", "title_rescue_trigger", "adjacent_title_marker_hit", "adjacent_domain_terms_hit", "normalized_title_for_adjacent_match", "adjacent_title_patterns_checked", "adjacent_title_patterns_hit", "adjacent_title_matched_token_slice", "adjacent_title_tokens_debug", "adjacent_pattern_tokens_debug", "adjacent_match_attempts",
                "title_rescue_trigger", "adjacent_domain_terms_hit",
                "matched_positive_title_terms", "matched_positive_body_terms", "matched_negative_terms",
                "apply_domains_hit", "skip_domains_hit", "primary_apply_domain", "role_family",
                "role_alignment_score", "lane_fit_score", "benchmark_lane", "benchmark_match_score",
                "source_trust_label", "action_bucket", "score", "decision_score", "score_threshold_used", "base_score_before_rescue", "adjacent_rescue_bonus_applied", "final_score_after_rescue", "location", "salary_range",
            ]
        )

    positive_title_terms: List[str] = []
    positive_body_terms: List[str] = []
    negative_terms: List[str] = []
    title_gate_passed: List[bool] = []
    title_gate_reason: List[str] = []

    for row in base.itertuples(index=False):
        matched_terms = _extract_keyword_terms(getattr(row, "matched_keywords", ""))
        penalized_terms = _extract_keyword_terms(getattr(row, "penalized_keywords", ""))
        title_terms = [term.replace("title:", "", 1) for term in matched_terms if term.startswith("title:")]
        stored_gate_terms = [piece.strip() for piece in clean(str(getattr(row, "title_gate_matched_keywords", ""))).split("|") if piece.strip()]
        if stored_gate_terms:
            title_terms = unique_preserve(title_terms + stored_gate_terms)
        body_terms = [term for term in matched_terms if not term.startswith("title:")]
        reason_code = clean(str(getattr(row, "drop_reason_code", "")))
        reason_detail = clean(str(getattr(row, "drop_reason_detail", "")))
        title_failed = reason_code == "title_fail"

        positive_title_terms.append(" | ".join(title_terms))
        positive_body_terms.append(" | ".join(body_terms))
        negative_terms.append(" | ".join(penalized_terms))
        title_gate_passed.append(not title_failed)
        title_gate_reason.append(reason_detail if title_failed else "")

    detail = base.copy()
    detail["title_gate_passed"] = title_gate_passed
    detail["title_gate_reason"] = title_gate_reason
    if "title_gate_matched_keywords" not in detail.columns:
        detail["title_gate_matched_keywords"] = ""
    if "title_rescue_bucket" not in detail.columns:
        detail["title_rescue_bucket"] = ""
    if "title_rescue_trigger" not in detail.columns:
        detail["title_rescue_trigger"] = ""
    if "adjacent_title_marker_hit" not in detail.columns:
        detail["adjacent_title_marker_hit"] = ""
    if "adjacent_domain_terms_hit" not in detail.columns:
        detail["adjacent_domain_terms_hit"] = ""
    if "normalized_title_for_adjacent_match" not in detail.columns:
        detail["normalized_title_for_adjacent_match"] = ""
    if "adjacent_title_patterns_checked" not in detail.columns:
        detail["adjacent_title_patterns_checked"] = ""
    if "adjacent_title_patterns_hit" not in detail.columns:
        detail["adjacent_title_patterns_hit"] = ""
    if "adjacent_title_matched_token_slice" not in detail.columns:
        detail["adjacent_title_matched_token_slice"] = ""
    if "adjacent_title_tokens_debug" not in detail.columns:
        detail["adjacent_title_tokens_debug"] = ""
    if "adjacent_pattern_tokens_debug" not in detail.columns:
        detail["adjacent_pattern_tokens_debug"] = ""
    if "adjacent_match_attempts" not in detail.columns:
        detail["adjacent_match_attempts"] = ""
    if "decision_score" not in detail.columns:
        detail["decision_score"] = ""
    if "score_threshold_used" not in detail.columns:
        detail["score_threshold_used"] = ""
    if "base_score_before_rescue" not in detail.columns:
        detail["base_score_before_rescue"] = ""
    if "adjacent_rescue_bonus_applied" not in detail.columns:
        detail["adjacent_rescue_bonus_applied"] = ""
    if "final_score_after_rescue" not in detail.columns:
        detail["final_score_after_rescue"] = ""
    detail["matched_positive_title_terms"] = positive_title_terms
    detail["matched_positive_body_terms"] = positive_body_terms
    detail["matched_negative_terms"] = negative_terms

    preferred = [
        "company", "title", "url", "drop_stage", "drop_reason", "drop_reason_code",
        "drop_reason_detail", "decision_reason", "title_gate_passed", "title_gate_reason", "title_gate_matched_keywords", "title_rescue_bucket", "title_rescue_trigger", "adjacent_title_marker_hit", "adjacent_domain_terms_hit", "normalized_title_for_adjacent_match", "adjacent_title_patterns_checked", "adjacent_title_patterns_hit", "adjacent_title_matched_token_slice", "adjacent_title_tokens_debug", "adjacent_pattern_tokens_debug", "adjacent_match_attempts",
        "title_rescue_trigger", "adjacent_domain_terms_hit",
        "matched_positive_title_terms", "matched_positive_body_terms", "matched_negative_terms",
        "apply_domains_hit", "skip_domains_hit", "primary_apply_domain", "role_family",
        "role_alignment_score", "lane_fit_score", "benchmark_lane", "benchmark_match_score",
        "source_trust_label", "action_bucket", "score", "decision_score", "score_threshold_used", "base_score_before_rescue", "adjacent_rescue_bonus_applied", "final_score_after_rescue", "location", "salary_range",
    ]
    for col in preferred:
        if col not in detail.columns:
            detail[col] = ""
    ordered = preferred + [col for col in detail.columns if col not in preferred]
    return detail[ordered]


def adapter_health_df(rows: List[Dict[str, Any]]) -> pd.DataFrame:
    if not rows:
        return pd.DataFrame(
            columns=[
                "context_kind", "name", "tier", "adapter", "status", "evaluated", "kept",
                "manual_targets", "rejected", "fallback_triggered", "failure_types",
                "failure_count_total", "top_failure_url", "notes",
            ]
        )
    df = pd.DataFrame(rows)
    preferred = [
        "context_kind", "name", "tier", "adapter", "status", "evaluated", "kept",
        "manual_targets", "rejected", "fallback_triggered", "failure_types",
        "failure_count_total", "top_failure_url", "notes",
    ]
    for col in preferred:
        if col not in df.columns:
            df[col] = ""
    ordered = preferred + [col for col in df.columns if col not in preferred]
    return df[ordered].sort_values(["fallback_triggered", "rejected", "evaluated", "name"], ascending=[False, False, False, True])


def build_adapter_health_row(
    *,
    context_kind: str,
    name: str,
    tier: Optional[int],
    adapter: str,
    status: str,
    evaluated_count: int,
    kept_count: int,
    manual_count: int,
    rejected_count: int,
    notes: str = "",
) -> Dict[str, Any]:
    failures = company_health_rows(name) if context_kind == "company" else []
    failure_types = sorted({clean(str(row.get("failure_type", ""))) for row in failures if row.get("failure_type")})
    top_failure_url = ""
    if failures:
        top_failure = failures[0]
        top_failure_url = clean(str(top_failure.get("url", "") or top_failure.get("careers_url", "")))

    return {
        "context_kind": context_kind,
        "name": name,
        "tier": tier if tier is not None else "",
        "adapter": adapter,
        "status": status,
        "evaluated": evaluated_count,
        "kept": kept_count,
        "manual_targets": manual_count,
        "rejected": rejected_count,
        "fallback_triggered": "manual" in status.lower() or "blocked" in status.lower() or "rate-limited" in status.lower(),
        "failure_types": " | ".join(failure_types),
        "failure_count_total": sum(int(row.get("failure_count", 0) or 0) for row in failures),
        "top_failure_url": top_failure_url,
        "notes": notes,
    }


def write_checkpoint_snapshot(
    *,
    checkpoint_dir: Path,
    label: str,
    raw_intake_total: int,
    all_evaluated_jobs: List[Job],
    all_jobs: List[Job],
    all_manual_targets: List[Job],
    all_rejected: List[RejectedJob],
    adapter_rows: List[Dict[str, Any]],
) -> Path:
    checkpoint_dir.mkdir(parents=True, exist_ok=True)
    evaluated = dedupe(all_evaluated_jobs)
    kept = dedupe(all_jobs)
    manual = dedupe(all_manual_targets)
    views = split_views(kept)

    all_df = to_df(kept)
    manual_df = to_df(manual)
    rejected_df = rejected_to_df(all_rejected)
    rejection_detail = rejection_detail_df(all_rejected)
    summary_df = company_summary(kept)
    funnel_df, source_yield_df, company_yield_df, reason_group_df = funnel_summary(
        raw_intake_total,
        evaluated,
        kept,
        manual,
        all_rejected,
    )
    adapter_df = adapter_health_df(adapter_rows)
    active_df = active_config_df()

    prefix = checkpoint_dir / label
    all_df.to_csv(prefix.with_name(prefix.name + "_all_jobs.csv"), index=False)
    manual_df.to_csv(prefix.with_name(prefix.name + "_manual_targets.csv"), index=False)
    rejected_df.to_csv(prefix.with_name(prefix.name + "_rejected.csv"), index=False)
    rejection_detail.to_csv(prefix.with_name(prefix.name + "_rejection_details.csv"), index=False)
    summary_df.to_csv(prefix.with_name(prefix.name + "_company_summary.csv"), index=False)
    funnel_df.to_csv(prefix.with_name(prefix.name + "_funnel_summary.csv"), index=False)
    source_yield_df.to_csv(prefix.with_name(prefix.name + "_source_yield.csv"), index=False)
    company_yield_df.to_csv(prefix.with_name(prefix.name + "_company_yield.csv"), index=False)
    reason_group_df.to_csv(prefix.with_name(prefix.name + "_reason_groups.csv"), index=False)
    adapter_df.to_csv(prefix.with_name(prefix.name + "_adapter_health.csv"), index=False)
    active_df.to_csv(prefix.with_name(prefix.name + "_active_config.csv"), index=False)
    Path(prefix.with_name(prefix.name + "_summary.json")).write_text(
        json.dumps(
            {
                "label": label,
                "raw_intake_total": raw_intake_total,
                "unique_evaluated": len(evaluated),
                "kept": len(kept),
                "manual_targets": len(manual),
                "rejected": len(all_rejected),
                "apply_now": len(views["Apply Now"]),
                "review_today": len(views["Review Today"]),
                "watch": len(views["Watch"]),
                "preferences_path": str(PREFERENCES_PATH) if PREFERENCES_PATH else "",
                "company_registry_path": str(COMPANY_REGISTRY_PATH) if COMPANY_REGISTRY_PATH else "",
            },
            indent=2,
            default=str,
        ),
        encoding="utf-8",
    )
    return prefix


def print_debug_company_snapshot(company_name: str, rejected_rows: List[RejectedJob], limit: int = 20) -> None:
    detail = rejection_detail_df([row for row in rejected_rows if clean(row.company).lower() == clean(company_name).lower()])
    if detail.empty:
        print(f"  Debug {company_name}: no rejected rows captured.")
        return

    print(f"  Debug {company_name}: top rejected rows")
    preview_cols = [
        "drop_stage", "drop_reason", "score", "decision_score", "action_bucket",
        "title_gate_matched_keywords", "title_rescue_bucket", "title_rescue_trigger", "adjacent_title_marker_hit", "adjacent_domain_terms_hit", "normalized_title_for_adjacent_match", "adjacent_title_patterns_checked", "adjacent_title_patterns_hit", "adjacent_title_matched_token_slice", "adjacent_title_tokens_debug", "adjacent_pattern_tokens_debug", "adjacent_match_attempts", "decision_score", "score_threshold_used", "base_score_before_rescue", "adjacent_rescue_bonus_applied", "final_score_after_rescue",
        "matched_positive_title_terms", "matched_positive_body_terms", "matched_negative_terms", "title",
    ]
    for row in detail.head(limit).itertuples(index=False):
        print(
            f"    [{getattr(row, 'drop_stage', '')}] {getattr(row, 'drop_reason', ''):<36} "
            f"score={getattr(row, 'decision_score', getattr(row, 'score', ''))} bucket={getattr(row, 'action_bucket', '')} title={getattr(row, 'title', '')[:60]}"
        )
        gate_terms = getattr(row, "title_gate_matched_keywords", "")
        rescue_bucket = getattr(row, "title_rescue_bucket", "")
        rescue_trigger = getattr(row, "title_rescue_trigger", "")
        adjacent_markers = getattr(row, "adjacent_title_marker_hit", "")
        adjacent_terms = getattr(row, "adjacent_domain_terms_hit", "")
        adj_norm = getattr(row, "normalized_title_for_adjacent_match", "")
        adj_checked = getattr(row, "adjacent_title_patterns_checked", "")
        adj_hits = getattr(row, "adjacent_title_patterns_hit", "")
        adj_slice = getattr(row, "adjacent_title_matched_token_slice", "")
        adj_tokens = getattr(row, "adjacent_title_tokens_debug", "")
        adj_pattern_tokens = getattr(row, "adjacent_pattern_tokens_debug", "")
        adj_attempts = getattr(row, "adjacent_match_attempts", "")
        threshold_used = getattr(row, "score_threshold_used", "")
        base_before = getattr(row, "base_score_before_rescue", "")
        rescue_bonus = getattr(row, "adjacent_rescue_bonus_applied", "")
        final_after = getattr(row, "final_score_after_rescue", "")
        title_terms = getattr(row, "matched_positive_title_terms", "")
        body_terms = getattr(row, "matched_positive_body_terms", "")
        neg_terms = getattr(row, "matched_negative_terms", "")
        if gate_terms or rescue_bucket or rescue_trigger or adjacent_markers or adjacent_terms or title_terms or body_terms or neg_terms or adj_norm or adj_checked or adj_hits or adj_tokens or adj_pattern_tokens or adj_attempts:
            print(
                f"      gate={gate_terms or '-'} | rescue={rescue_bucket or '-'} | trigger={rescue_trigger or '-'} | "
                f"adj_title={adjacent_markers or '-'} | adj_domain={adjacent_terms or '-'} | adj_slice={adj_slice or '-'} | thresh={threshold_used or '-'} | "
                f"base={base_before or '-'} | bonus={rescue_bonus or '-'} | final={final_after or '-'} | "
                f"+title={title_terms or '-'} | +body={body_terms or '-'} | -terms={neg_terms or '-'}"
            )
            print(
                f"      adj_norm={adj_norm or '-'} | adj_checked={adj_checked or '-'} | adj_hit={adj_hits or '-'}"
            )
            print(
                f"      adj_tokens={adj_tokens or '-'} | adj_pattern_tokens={adj_pattern_tokens or '-'}"
            )
            print(
                f"      adj_attempts={adj_attempts or '-'}"
            )


# ============================================================
# MAIN
# ============================================================
def main() -> None:
    args = parse_args()
    companies = [
        Company(**{k: v for k, v in c.items() if k in Company.__dataclass_fields__})
        for c in COMPANY_REGISTRY
    ]
    companies = select_companies_for_run(companies, args)

    if not companies:
        print("No companies selected for this run. Adjust --company-allowlist, --company-starts-after, or --company-limit.")
        return

    checkpoint_dir = Path(args.checkpoint_dir).expanduser()
    if not checkpoint_dir.is_absolute():
        checkpoint_dir = BASE_DIR / checkpoint_dir

    all_evaluated_jobs: List[Job] = []
    all_jobs: List[Job] = []
    all_manual_targets: List[Job] = []
    all_rejected: List[RejectedJob] = []
    adapter_rows: List[Dict[str, Any]] = []
    raw_intake_total = 0
    processed_company_count = 0

    # Load history before scraping so already-seen jobs can be skipped.
    history = load_history()

    print("=" * 68)
    print("Job Search v6")
    print(f"Run time: {iso_now()}")
    print(f"Preferences: {PREFERENCES_PATH if PREFERENCES_PATH else 'in-script defaults'}")
    print(f"Company registry: {COMPANY_REGISTRY_PATH if COMPANY_REGISTRY_PATH else 'in-script defaults'}")
    print(f"Companies selected: {len(companies)} of {len(COMPANY_REGISTRY)}")
    print("=" * 68)

    try:
        print_lock = threading.Lock()

        def _scrape_company(company: "Company"):
            local_rejected: List[RejectedJob] = []
            jobs = run_adapter(company, rejected_jobs=local_rejected)
            kept = filter_kept_jobs(company, jobs, local_rejected)
            kept = [j for j in kept if job_id(j.company, j.title, j.canonical_url) not in history]
            real_kept, manual_kept = partition_manual_jobs(kept)
            rejected_count = len(local_rejected)
            evaluated_count = len(jobs) + rejected_count
            status = company_status_label(company, jobs, real_kept, manual_kept)
            manual_str = f"  manual={len(manual_kept):3d}" if manual_kept else ""
            with print_lock:
                print(
                    f"  {company.name:<28} tier={company.tier}  "
                    f"evaluated={evaluated_count:3d}  kept={len(real_kept):3d}{manual_str}  rejected={rejected_count:3d}  [{status}]"
                )
                logging.info(
                    "company=%s evaluated=%s kept=%s manual=%s rejected=%s",
                    company.name, evaluated_count, len(real_kept), len(manual_kept), rejected_count,
                )
            return {
                "company": company,
                "jobs": jobs,
                "real_kept": real_kept,
                "manual_kept": manual_kept,
                "rejected": local_rejected,
                "evaluated_count": evaluated_count,
                "status": status,
            }

        with ThreadPoolExecutor(max_workers=SCRAPE_WORKERS) as pool:
            futures = {pool.submit(_scrape_company, co): co for co in companies}
            for future in as_completed(futures):
                result = future.result()
                company = result["company"]
                all_evaluated_jobs.extend(result["jobs"])
                all_jobs.extend(result["real_kept"])
                all_manual_targets.extend(result["manual_kept"])
                all_rejected.extend(result["rejected"])
                raw_intake_total += result["evaluated_count"]
                processed_company_count += 1
                adapter_rows.append(
                    build_adapter_health_row(
                        context_kind="company",
                        name=company.name,
                        tier=company.tier,
                        adapter=company.adapter,
                        status=result["status"],
                        evaluated_count=result["evaluated_count"],
                        kept_count=len(result["real_kept"]),
                        manual_count=len(result["manual_kept"]),
                        rejected_count=len(result["rejected"]),
                        notes=company.notes,
                    )
                )
                if args.debug_company and clean(company.name).lower() == clean(args.debug_company).lower():
                    company_rejected = [row for row in result["rejected"]]
                    print_debug_company_snapshot(company.name, company_rejected, limit=args.debug_rejections_limit)

        if not args.skip_external_boards:
            for board in EXTERNAL_BOARD_REGISTRY:
                rejected_before = len(all_rejected)
                jobs = run_external_board(board, rejected_jobs=all_rejected)
                all_evaluated_jobs.extend(jobs)
                kept = filter_kept_jobs(None, jobs, all_rejected)
                kept = [j for j in kept if job_id(j.company, j.title, j.canonical_url) not in history]
                real_kept, manual_kept = partition_manual_jobs(kept)
                all_jobs.extend(real_kept)
                all_manual_targets.extend(manual_kept)

                rejected_count = len(all_rejected) - rejected_before
                evaluated_count = len(jobs) + rejected_count
                raw_intake_total += evaluated_count
                manual_str = f"  manual={len(manual_kept):3d}" if manual_kept else ""
                print(
                    f"  {board['name']:<28} board    "
                    f"evaluated={evaluated_count:3d}  kept={len(real_kept):3d}{manual_str}  rejected={rejected_count:3d}  [↗ external]"
                )
                logging.info(
                    "external_board=%s evaluated=%s kept=%s manual=%s rejected=%s",
                    board["name"],
                    evaluated_count,
                    len(real_kept),
                    len(manual_kept),
                    rejected_count,
                )
                adapter_rows.append(
                    build_adapter_health_row(
                        context_kind="external_board",
                        name=board["name"],
                        tier=None,
                        adapter=board.get("adapter", "external_board"),
                        status="external board",
                        evaluated_count=evaluated_count,
                        kept_count=len(real_kept),
                        manual_count=len(manual_kept),
                        rejected_count=rejected_count,
                        notes=clean(str(board.get("search_urls", "")))[:300],
                    )
                )
                time.sleep(0.3)
    except KeyboardInterrupt:
        label = f"interrupted_{processed_company_count:03d}_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        prefix = write_checkpoint_snapshot(
            checkpoint_dir=checkpoint_dir,
            label=label,
            raw_intake_total=raw_intake_total,
            all_evaluated_jobs=all_evaluated_jobs,
            all_jobs=all_jobs,
            all_manual_targets=all_manual_targets,
            all_rejected=all_rejected,
            adapter_rows=adapter_rows,
        )
        print()
        print(f"Interrupted. Partial checkpoint written to {prefix.with_name(prefix.name + '_summary.json')}")
        return

    all_evaluated_jobs = dedupe(all_evaluated_jobs)
    all_jobs = dedupe(all_jobs)
    all_manual_targets = dedupe(all_manual_targets)
    unique_evaluated_total = len(all_evaluated_jobs)

    # history already loaded at top of main(); just apply timestamps to kept jobs.
    apply_history(all_jobs, history)
    apply_history(all_manual_targets, history)

    all_jobs = cap_per_company(all_jobs, TOP_N_PER_COMPANY)

    views = split_views(all_jobs)
    save_history(update_history(all_jobs + all_manual_targets, history))

    all_df = to_df(all_jobs)
    apply_df = to_df(views["Apply Now"])
    review_df = to_df(views["Review Today"])
    watch_df = to_df(views["Watch"])
    manual_df = to_df(all_manual_targets)
    rejected_df = rejected_to_df(all_rejected)
    rejection_summary_df = rejection_summary(all_rejected)
    rejection_detail_export_df = rejection_detail_df(all_rejected)
    keep_filter_rejection_df = rejection_summary([r for r in all_rejected if r.drop_stage == "keep_filter"])
    summary_df = company_summary(all_jobs)
    funnel_df, source_yield_df, company_yield_df, reason_group_df = funnel_summary(
        raw_intake_total,
        all_evaluated_jobs,
        all_jobs,
        all_manual_targets,
        all_rejected,
    )
    adapter_health_export_df = adapter_health_df(adapter_rows)
    registry_health_export_df = registry_health_df()
    active_config_export_df = active_config_df()

    with pd.ExcelWriter(OUTPUT_XLSX, engine="openpyxl") as writer:
        apply_df.to_excel(writer, sheet_name="Apply Now", index=False)
        review_df.to_excel(writer, sheet_name="Review Today", index=False)
        watch_df.to_excel(writer, sheet_name="Watch", index=False)
        manual_df.to_excel(writer, sheet_name="Manual Review", index=False)
        all_df.to_excel(writer, sheet_name="All Jobs", index=False)
        rejected_df.to_excel(writer, sheet_name="Rejected Jobs", index=False)
        rejection_detail_export_df.to_excel(writer, sheet_name="Rejection Details", index=False)
        rejection_summary_df.to_excel(writer, sheet_name="Rejection Summary", index=False)
        keep_filter_rejection_df.to_excel(writer, sheet_name="Keep Filter Rejects", index=False)
        summary_df.to_excel(writer, sheet_name="Company Summary", index=False)
        funnel_df.to_excel(writer, sheet_name="Funnel Summary", index=False)
        source_yield_df.to_excel(writer, sheet_name="Source Yield", index=False)
        company_yield_df.to_excel(writer, sheet_name="Company Yield", index=False)
        reason_group_df.to_excel(writer, sheet_name="Reason Groups", index=False)
        adapter_health_export_df.to_excel(writer, sheet_name="Adapter Health", index=False)
        registry_health_export_df.to_excel(writer, sheet_name="Registry Health", index=False)
        active_config_export_df.to_excel(writer, sheet_name="Active Config", index=False)

    all_df.to_csv(OUTPUT_CSV, index=False)
    manual_df.to_csv(OUTPUT_MANUAL_CSV, index=False)
    rejected_df.to_csv(OUTPUT_REJECTED_CSV, index=False)
    rejection_detail_export_df.to_csv(OUTPUT_REJECTION_DETAIL_CSV, index=False)
    adapter_health_export_df.to_csv(OUTPUT_ADAPTER_HEALTH_CSV, index=False)
    registry_health_export_df.to_csv(OUTPUT_REGISTRY_HEALTH_CSV, index=False)
    Path(OUTPUT_ACTIVE_CONFIG_JSON).write_text(
        json.dumps(
            {
                "preferences_path": str(PREFERENCES_PATH) if PREFERENCES_PATH else "",
                "loaded_from_yaml": bool(PREFERENCES_PATH),
                "company_registry_path": str(COMPANY_REGISTRY_PATH) if COMPANY_REGISTRY_PATH else "",
                "company_registry_from_yaml": bool(COMPANY_REGISTRY_PATH),
                "preferences": PREFERENCES,
                "selected_company_count": len(companies),
                "skip_external_boards": bool(args.skip_external_boards),
            },
            indent=2,
            default=str,
        ),
        encoding="utf-8",
    )

    print()
    print(f"  Raw intake:           {raw_intake_total}")
    print(f"  Unique evaluated:     {unique_evaluated_total}")
    print(f"  Total real jobs kept: {len(all_jobs)}")
    print(f"  Total jobs rejected:  {len(all_rejected)}")
    print(f"  Apply now:            {len(views['Apply Now'])}")
    print(f"  Review today:         {len(views['Review Today'])}")
    print(f"  Watch:                {len(views['Watch'])}")
    print(f"  Manual targets:       {len(all_manual_targets)}")
    print()
    print(f"  Output: {OUTPUT_XLSX}")
    print(f"  Output: {OUTPUT_CSV}")
    print(f"  Output: {OUTPUT_MANUAL_CSV}")
    print(f"  Output: {OUTPUT_REJECTED_CSV}")
    print(f"  Output: {OUTPUT_REJECTION_DETAIL_CSV}")
    print(f"  Output: {OUTPUT_ADAPTER_HEALTH_CSV}")
    print(f"  Output: {OUTPUT_REGISTRY_HEALTH_CSV}")
    print(f"  Output: {OUTPUT_ACTIVE_CONFIG_JSON}")
    if args.checkpoint_every and args.checkpoint_every > 0:
        print(f"  Checkpoints: {checkpoint_dir}")
    print(f"  History: {HISTORY_JSON}")
    print(f"  Log:     {RUN_LOG}")
    print()

    top = sorted(
        [j for j in all_jobs if j.action_bucket in ("APPLY NOW", "REVIEW TODAY")],
        key=display_sort_key,
    )[:10]

    if top:
        print("  Top jobs:")
        for j in top:
            print(f"    [{j.action_bucket:<12}] {j.score:5.1f}  {j.company:<24} {j.title[:50]}")

    if all_rejected:
        print("  Top rejection reasons:")
        for row in rejection_summary_df.head(8).itertuples(index=False):
            detail = f":{row.drop_reason_detail}" if row.drop_reason_detail else ""
            print(f"    {row.count:3d}  {row.drop_stage}/{row.drop_reason_code}{detail}")

        if not keep_filter_rejection_df.empty:
            print("  Top keep-filter rejections:")
            for row in keep_filter_rejection_df.head(5).itertuples(index=False):
                detail = f":{row.drop_reason_detail}" if row.drop_reason_detail else ""
                print(f"    {row.count:3d}  {row.drop_stage}/{row.drop_reason_code}{detail}")

    if not source_yield_df.empty:
        print("  Kept rate by source:")
        for row in source_yield_df.head(8).itertuples(index=False):
            print(f"    {str(row.source)[:24]:24} eval={row.evaluated:4d} kept={row.kept:3d} manual={row.manual_targets:3d} rate={row.kept_rate:.1%}")

    if not company_yield_df.empty:
        print("  Kept rate by company:")
        for row in company_yield_df.head(8).itertuples(index=False):
            print(f"    {str(row.company)[:24]:24} eval={row.evaluated:4d} kept={row.kept:3d} manual={row.manual_targets:3d} rate={row.kept_rate:.1%}")

    if not reason_group_df.empty:
        print("  Rejection rate by reason group:")
        for row in reason_group_df.head(8).itertuples(index=False):
            print(f"    {row.reason_group:24} rejected={row.rejected:4d} rate={row.rejection_rate_of_all_rejections:.1%}")

    if not source_yield_df.empty:
        highest_yield = source_yield_df[source_yield_df["evaluated"] >= 5].head(5)
        if not highest_yield.empty:
            print("  Top sources with highest yield:")
            for row in highest_yield.itertuples(index=False):
                print(f"    {str(row.source)[:24]:24} kept={row.kept:3d}/{row.evaluated:<3d} rate={row.kept_rate:.1%}")

    print("=" * 68)

if __name__ == "__main__":
    main()