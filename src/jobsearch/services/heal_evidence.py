"""Structured per-company JSONL evidence writer for heal runs.

Appends one JSON line per company attempt to results/heal_evidence.jsonl,
enabling post-hoc audit of which companies failed, why, and what was tried.
"""

from __future__ import annotations

import json
import logging
import threading
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)

_DEFAULT_EVIDENCE_PATH = Path("results") / "heal_evidence.jsonl"


class HealEvidenceWriter:
    """Thread-safe appender for per-company heal evidence records."""

    def __init__(self, path: Path | str | None = None) -> None:
        self._path = Path(path) if path else _DEFAULT_EVIDENCE_PATH
        self._lock = threading.Lock()
        self._path.parent.mkdir(parents=True, exist_ok=True)

    def write(
        self,
        *,
        company: str,
        input_url: str,
        adapter: Optional[str],
        adapter_key: Optional[str],
        final_url: Optional[str],
        status: str,
        failure_reason: Optional[str],
        jobs_found: int,
        elapsed_ms: float,
        candidates_tried: Optional[List[Dict[str, Any]]] = None,
        ats_family: Optional[str] = None,
        extraction_method: str = "",
        extraction_confidence: float = 0.0,
        route_decision: str = "",
        board_state: str = "",
        screenshot_path: str = "",
        html_snapshot_path: str = "",
        top_network_response_urls: Optional[List[str]] = None,
        timing_metrics: Optional[Dict[str, Any]] = None,
        detail: str = "",
    ) -> None:
        """Append one evidence record for a company heal attempt."""
        record = {
            "ts": datetime.now(timezone.utc).isoformat(timespec="seconds"),
            "company": company,
            "input_url": input_url,
            "candidates_tried": candidates_tried or [],
            "final_url": final_url,
            "adapter": adapter,
            "adapter_key": adapter_key,
            "ats_family": ats_family,
            "status": status,
            "board_state": board_state,
            "extraction_method": extraction_method,
            "extraction_confidence": round(extraction_confidence, 3),
            "route_decision": route_decision,
            "failure_reason": failure_reason,
            "jobs_found": jobs_found,
            "elapsed_ms": round(elapsed_ms, 1),
            "screenshot_path": screenshot_path,
            "html_snapshot_path": html_snapshot_path,
            "top_network_response_urls": list(top_network_response_urls or []),
            "timing_metrics": timing_metrics or {},
            "detail": detail,
        }
        line = json.dumps(record, ensure_ascii=False)
        try:
            with self._lock:
                with self._path.open("a", encoding="utf-8") as fh:
                    fh.write(line + "\n")
        except OSError as exc:
            logger.warning("HealEvidenceWriter: failed to write evidence for %s: %s", company, exc)
