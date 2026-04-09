from __future__ import annotations

import argparse
import glob
import re
from collections import defaultdict
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Tuple

import yaml


_CORP_STOPWORDS = {
    "inc",
    "incorporated",
    "llc",
    "l",
    "ltd",
    "limited",
    "corp",
    "corporation",
    "co",
    "company",
    "holdings",
    "group",
    "technologies",
    "technology",
}


def _norm_name(value: Any) -> str:
    text = str(value or "").strip().lower().replace("&", " and ")
    text = re.sub(r"[^a-z0-9]+", " ", text)
    parts = [p for p in text.split() if p and p not in _CORP_STOPWORDS]
    return " ".join(parts).strip()


def _norm_url(value: Any) -> str:
    url = str(value or "").strip()
    if not url:
        return ""
    # Drop fragments and normalize trailing slash
    url = url.split("#", 1)[0].strip()
    url = url.rstrip("/")
    return url.lower()


def _shallow_merge_keep_left(left: Dict[str, Any], right: Dict[str, Any]) -> Dict[str, Any]:
    """Prefer left values; fill missing/empty fields from right. Avoid overwriting user edits."""
    out = dict(left)
    for k, v in (right or {}).items():
        if k not in out:
            out[k] = v
            continue
        lv = out.get(k)
        if lv in (None, "", [], {}):
            out[k] = v
            continue
        # For booleans, preserve True if either side is True.
        if isinstance(lv, bool) and isinstance(v, bool):
            out[k] = bool(lv or v)
    return out


@dataclass
class _DedupeStats:
    before: int = 0
    after: int = 0
    removed: int = 0
    merged: int = 0


def dedupe_companies(companies: List[Dict[str, Any]]) -> Tuple[List[Dict[str, Any]], _DedupeStats]:
    stats = _DedupeStats(before=len(companies))
    kept: List[Dict[str, Any]] = []

    seen_by_name: Dict[str, int] = {}
    seen_by_url: Dict[str, List[int]] = defaultdict(list)

    for row in companies:
        if not isinstance(row, dict):
            continue
        name_raw = row.get("name")
        url_raw = row.get("careers_url")
        nname = _norm_name(name_raw)
        nurl = _norm_url(url_raw)

        if nname and nname in seen_by_name:
            idx = seen_by_name[nname]
            kept[idx] = _shallow_merge_keep_left(kept[idx], row)
            stats.removed += 1
            stats.merged += 1
            continue

        if nurl and seen_by_url.get(nurl):
            # Same careers URL repeated inside a registry is almost always an accidental duplicate.
            # Keep the first occurrence, fill any missing fields from later entries, and drop the rest.
            idx = seen_by_url[nurl][0]
            kept[idx] = _shallow_merge_keep_left(kept[idx], row)
            stats.removed += 1
            stats.merged += 1
            continue

        kept.append(row)
        idx = len(kept) - 1
        if nname:
            seen_by_name[nname] = idx
        if nurl:
            seen_by_url[nurl].append(idx)

    stats.after = len(kept)
    return kept, stats


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--glob", default="config/job_search_comp*.yaml")
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    paths = [Path(p) for p in sorted(glob.glob(args.glob))]
    if not paths:
        print(f"No registries found for glob: {args.glob}")
        return 1

    total_removed = 0
    for path in paths:
        data = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
        companies = data.get("companies") or []
        if not isinstance(companies, list):
            continue
        deduped, stats = dedupe_companies(companies)
        if stats.removed <= 0:
            continue

        total_removed += stats.removed
        print(f"{path.name}: {stats.before} -> {stats.after} (removed={stats.removed}, merged={stats.merged})")
        if not args.dry_run:
            data["companies"] = deduped
            path.write_text(yaml.safe_dump(data, sort_keys=False, allow_unicode=True), encoding="utf-8")

    if total_removed == 0:
        print("No duplicates found.")
    else:
        print(f"Total removed: {total_removed}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
