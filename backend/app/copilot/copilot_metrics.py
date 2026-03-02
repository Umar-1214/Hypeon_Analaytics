"""Simple in-process metrics for Copilot V2 (planner attempts, fallback success, empty results)."""
from __future__ import annotations

import threading

_metrics = {
    "copilot.planner_attempts_total": 0,
    "copilot.fallback_success_total": 0,
    "copilot.query_empty_results_total": 0,
}
_lock = threading.Lock()


def increment(name: str, value: int = 1) -> None:
    if name in _metrics:
        with _lock:
            _metrics[name] += value


def get(name: str) -> int:
    with _lock:
        return _metrics.get(name, 0)


def reset() -> None:
    with _lock:
        for k in _metrics:
            _metrics[k] = 0
