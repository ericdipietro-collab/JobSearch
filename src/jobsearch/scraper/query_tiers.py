from __future__ import annotations

import re
from typing import Any


_TIER_PREFIX_RE = re.compile(r"^\s*(\d+)\s*:\s*(.+?)\s*$")


def _as_positive_int(value: Any, default: int) -> int:
    try:
        parsed = int(value)
    except (TypeError, ValueError):
        return default
    return parsed if parsed > 0 else default


def normalize_search_queries(raw: Any) -> list[dict[str, Any]]:
    if raw in (None, "", []):
        return []

    if isinstance(raw, str):
        items = [part.strip() for part in re.split(r"[\n|]+", raw.replace("\r", "\n")) if part.strip()]
    elif isinstance(raw, (list, tuple)):
        items = list(raw)
    else:
        items = [raw]

    normalized: list[dict[str, Any]] = []
    for item in items:
        if isinstance(item, dict):
            query = str(item.get("query") or item.get("text") or "").strip()
            if not query:
                continue
            normalized.append({"query": query, "tier": _as_positive_int(item.get("tier"), 1)})
            continue

        text = str(item or "").strip()
        if not text:
            continue
        match = _TIER_PREFIX_RE.match(text)
        if match:
            normalized.append({"query": match.group(2).strip(), "tier": _as_positive_int(match.group(1), 1)})
        else:
            normalized.append({"query": text, "tier": 1})
    return normalized


def search_query_text_lines(raw: Any) -> list[str]:
    lines: list[str] = []
    for item in normalize_search_queries(raw):
        tier = _as_positive_int(item.get("tier"), 1)
        query = str(item.get("query") or "").strip()
        if not query:
            continue
        lines.append(f"{tier}: {query}" if tier != 1 else query)
    return lines


def search_queries_for_tier(raw: Any, max_tier: int | None = None) -> list[str]:
    queries: list[str] = []
    tier_limit = _as_positive_int(max_tier, 99) if max_tier is not None else None
    for item in normalize_search_queries(raw):
        tier = _as_positive_int(item.get("tier"), 1)
        if tier_limit is not None and tier > tier_limit:
            continue
        query = str(item.get("query") or "").strip()
        if query:
            queries.append(query)
    return queries


def max_query_tier(preferences: dict[str, Any], lane: str, default: int = 3) -> int:
    cfg = (((preferences or {}).get("search") or {}).get("query_tiers") or {})
    if lane == "jobspy_experimental":
        return _as_positive_int(cfg.get("jobspy_max_tier"), default)
    if lane == "aggregator":
        return _as_positive_int(cfg.get("aggregator_max_tier"), default)
    return _as_positive_int(cfg.get("default_max_tier"), default)
