from __future__ import annotations

import hashlib
import json
import re
from typing import Any, Dict, Iterable, List
from urllib.parse import urljoin

from jobsearch.scraper.models import Job


_JSONLD_PATTERN = re.compile(
    r'<script[^>]+type=["\']application/ld\+json["\'][^>]*>(.*?)</script>',
    re.IGNORECASE | re.DOTALL,
)


def extract_jobposting_objects(html: str) -> List[Dict[str, Any]]:
    results: List[Dict[str, Any]] = []
    seen: set[str] = set()
    for match in _JSONLD_PATTERN.finditer(str(html or "")):
        raw = (match.group(1) or "").strip()
        if not raw:
            continue
        try:
            payload = json.loads(raw)
        except (json.JSONDecodeError, ValueError, TypeError):
            continue
        for item in _collect_jobposting_objects(payload):
            key = json.dumps(item, sort_keys=True, default=str)
            if key in seen:
                continue
            seen.add(key)
            results.append(item)
    return results


def jsonld_jobs_to_canonical(
    html: str,
    *,
    base_url: str,
    company_name: str,
    adapter: str,
    tier: int | str = 4,
    source: str = "JSON-LD",
) -> List[Job]:
    jobs: List[Job] = []
    seen_urls: set[str] = set()
    for obj in extract_jobposting_objects(html):
        title = _clean(obj.get("title") or obj.get("name") or "")
        url = _absolute_url(base_url, obj.get("url") or obj.get("sameAs") or "")
        if not title:
            continue
        if not url:
            slug = re.sub(r"[^a-z0-9]+", "-", title.lower()).strip("-")
            url = _absolute_url(base_url, slug or "")
        if url in seen_urls:
            continue
        seen_urls.add(url)
        description = _description_text(obj)
        salary_text = _salary_text(obj)
        salary_min, salary_max = _salary_range(obj)
        job_id = hashlib.md5(f"{company_name}{title}{url}".encode()).hexdigest()
        jobs.append(
            Job(
                id=job_id,
                company=company_name,
                role_title_raw=title,
                location=_job_location(obj),
                is_remote="remote" in _job_location(obj).lower(),
                url=url,
                source=source,
                adapter=adapter,
                tier=str(tier),
                description_excerpt=description,
                salary_text=salary_text,
                salary_min=salary_min,
                salary_max=salary_max,
            )
        )
    return jobs


def _collect_jobposting_objects(payload: Any) -> List[Dict[str, Any]]:
    found: List[Dict[str, Any]] = []
    if isinstance(payload, list):
        for item in payload:
            found.extend(_collect_jobposting_objects(item))
        return found
    if not isinstance(payload, dict):
        return found

    types = payload.get("@type") or payload.get("type") or ""
    if isinstance(types, list):
        type_blob = " ".join(str(part) for part in types)
    else:
        type_blob = str(types)
    if "jobposting" in type_blob.lower():
        found.append(payload)

    for key in ("@graph", "graph", "itemListElement", "mainEntity", "hasPart"):
        value = payload.get(key)
        if value is not None:
            found.extend(_collect_jobposting_objects(value))

    for value in payload.values():
        if isinstance(value, (dict, list)):
            found.extend(_collect_jobposting_objects(value))
    return found


def _job_location(obj: Dict[str, Any]) -> str:
    locations = obj.get("jobLocation") or obj.get("joblocation") or obj.get("applicantLocationRequirements") or []
    if not isinstance(locations, list):
        locations = [locations]
    parts: List[str] = []
    for item in locations:
        if isinstance(item, str):
            cleaned = _clean(item)
            if cleaned:
                parts.append(cleaned)
            continue
        if not isinstance(item, dict):
            continue
        address = item.get("address") if isinstance(item.get("address"), dict) else item
        values = [
            _clean(address.get("addressLocality") or ""),
            _clean(address.get("addressRegion") or ""),
            _clean(address.get("addressCountry") or address.get("name") or ""),
        ]
        rendered = ", ".join(value for value in values if value)
        if rendered:
            parts.append(rendered)
    if not parts and str(obj.get("jobLocationType") or "").upper() == "TELECOMMUTE":
        parts.append("Remote")
    return " / ".join(dict.fromkeys(parts))


def _description_text(obj: Dict[str, Any]) -> str:
    raw = obj.get("description") or obj.get("descriptionHtml") or obj.get("summary") or ""
    raw = re.sub(r"<[^>]+>", " ", str(raw))
    return _clean(raw)[:8000]


def _salary_text(obj: Dict[str, Any]) -> str:
    salary = obj.get("baseSalary") or obj.get("estimatedSalary") or {}
    if isinstance(salary, dict):
        currency = _clean(salary.get("currency") or "")
        value = salary.get("value") if isinstance(salary.get("value"), dict) else salary
        unit = _clean(value.get("unitText") or value.get("unitCode") or "")
        low = value.get("minValue")
        high = value.get("maxValue")
        if low is not None or high is not None:
            low_txt = f"{low:,.0f}" if isinstance(low, (int, float)) else str(low or "")
            high_txt = f"{high:,.0f}" if isinstance(high, (int, float)) else str(high or "")
            if low_txt and high_txt:
                return f"{currency} {low_txt} - {high_txt} {unit}".strip()
            return f"{currency} {low_txt or high_txt} {unit}".strip()
    return ""


def _salary_range(obj: Dict[str, Any]) -> tuple[float | None, float | None]:
    salary = obj.get("baseSalary") or obj.get("estimatedSalary") or {}
    if isinstance(salary, dict):
        value = salary.get("value") if isinstance(salary.get("value"), dict) else salary
        low = value.get("minValue")
        high = value.get("maxValue")
        try:
            low_val = float(low) if low is not None else None
        except (TypeError, ValueError):
            low_val = None
        try:
            high_val = float(high) if high is not None else None
        except (TypeError, ValueError):
            high_val = None
        return low_val, high_val
    return None, None


def _absolute_url(base_url: str, href: str) -> str:
    target = str(href or "").strip()
    if not target:
        return ""
    return urljoin(base_url, target)


def _clean(text: Any) -> str:
    return re.sub(r"\s+", " ", str(text or "")).strip()
