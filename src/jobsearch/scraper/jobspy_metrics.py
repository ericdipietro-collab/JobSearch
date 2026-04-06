from __future__ import annotations

from dataclasses import dataclass, field
from typing import Dict


@dataclass
class _BoardStats:
    requested: int = 0
    attempted: int = 0
    success: int = 0
    failed: int = 0
    skipped: int = 0
    runtime_ms: float = 0.0
    raw_results: int = 0
    normalized_results: int = 0
    deduped_results: int = 0


@dataclass
class JobSpyMetrics:
    boards: Dict[str, _BoardStats] = field(default_factory=dict)

    def _board(self, name: str) -> _BoardStats:
        return self.boards.setdefault(name, _BoardStats())

    def mark_requested(self, name: str) -> None:
        self._board(name).requested += 1

    def mark_attempted(self, name: str) -> None:
        self._board(name).attempted += 1

    def mark_success(self, name: str, runtime_ms: float, raw_results: int, normalized_results: int, deduped_results: int) -> None:
        board = self._board(name)
        board.success += 1
        board.runtime_ms += max(0.0, float(runtime_ms or 0.0))
        board.raw_results += max(0, int(raw_results or 0))
        board.normalized_results += max(0, int(normalized_results or 0))
        board.deduped_results += max(0, int(deduped_results or 0))

    def mark_failure(self, name: str, runtime_ms: float) -> None:
        board = self._board(name)
        board.failed += 1
        board.runtime_ms += max(0.0, float(runtime_ms or 0.0))

    def mark_skipped(self, name: str) -> None:
        self._board(name).skipped += 1

    def summary_text(self) -> str:
        parts = []
        for name, stats in self.boards.items():
            parts.append(
                f"{name}: requested={stats.requested} attempted={stats.attempted} "
                f"success={stats.success} failed={stats.failed} skipped={stats.skipped} "
                f"runtime_ms={round(stats.runtime_ms,1)} raw={stats.raw_results} "
                f"normalized={stats.normalized_results} deduped={stats.deduped_results}"
            )
        return " | ".join(parts)
