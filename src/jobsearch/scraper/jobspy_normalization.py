from __future__ import annotations

import re
from typing import Any, Dict, Iterable, List


def _clean_text(value: Any) -> str:
    return re.sub(r"\s+", " ", str(value or "").strip()).lower()


def _cluster_key(record: Dict[str, Any]) -> str:
    canonical = str(record.get("canonical_job_url") or "").strip().lower()
    if canonical:
        return f"canonical::{canonical}"
    company = _clean_text(record.get("company") or record.get("company_name"))
    title = _clean_text(record.get("title") or record.get("job_title") or record.get("name"))
    location = _clean_text(record.get("location") or record.get("job_location"))
    return f"fallback::{company}|{title}|{location}"


def _description_len(record: Dict[str, Any]) -> int:
    return len(str(record.get("description") or record.get("description_text") or record.get("snippet") or ""))


def cluster_jobspy_records(records: Iterable[Dict[str, Any]]) -> List[Dict[str, Any]]:
    clusters: dict[str, Dict[str, Any]] = {}
    for record in records:
        if not isinstance(record, dict):
            continue
        key = _cluster_key(record)
        site = str(record.get("_source_site") or "").strip().lower()
        existing = clusters.get(key)
        if existing is None:
            copy = dict(record)
            copy["_source_site_variants"] = [site] if site else []
            clusters[key] = copy
            continue
        existing_variants = list(existing.get("_source_site_variants") or [])
        if site and site not in existing_variants:
            existing_variants.append(site)
        existing["_source_site_variants"] = existing_variants
        existing_score = (1 if existing.get("canonical_job_url") else 0, _description_len(existing))
        incoming_score = (1 if record.get("canonical_job_url") else 0, _description_len(record))
        if incoming_score > existing_score:
            merged = dict(record)
            merged["_source_site_variants"] = existing_variants
            clusters[key] = merged
    output: list[Dict[str, Any]] = []
    for record in clusters.values():
        variants = list(record.get("_source_site_variants") or [])
        record["_source_site"] = variants[0] if variants else ""
        record["_source_site_variants"] = variants
        record["_source_site_count"] = len(variants)
        record["_direct_apply_confidence"] = "high" if record.get("canonical_job_url") else "low"
        output.append(record)
    return output
