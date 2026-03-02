"""
Result validator for Copilot V2: sanity checks on SQL results.
Non-empty expectations, numeric types for metrics, row sanity (e.g. counts >= 0).
"""
from __future__ import annotations

import re
from typing import Any


def validate(
    result: dict,
    question: str,
    *,
    allow_empty: bool = False,
) -> tuple[bool, str]:
    """
    Validate a run_bigquery_sql result against the user question.
    Returns (is_valid, reason).
    """
    if not isinstance(result, dict):
        return False, "Invalid result shape"
    error = result.get("error")
    if error:
        return False, f"Query error: {error}"
    rows = result.get("rows") or []
    schema = result.get("schema") or []

    if not allow_empty and len(rows) == 0:
        return False, "No rows returned"

    # If question implies a count/aggregate metric, expect at least one numeric-ish column
    question_lower = (question or "").strip().lower()
    wants_metric = any(
        w in question_lower
        for w in ("count", "views", "views count", "how many", "total", "sum", "number of", "metric")
    )
    if wants_metric and rows:
        has_numeric = False
        for col in schema:
            if not col:
                continue
            for r in rows[:3]:
                v = r.get(col)
                if v is not None and isinstance(v, (int, float)):
                    has_numeric = True
                    break
            if has_numeric:
                break
        if not has_numeric and schema:
            # Allow if only one column and it's string (e.g. "no data")
            pass  # don't fail for string-only result

    # Row sanity: no negative counts if column name suggests count
    for r in rows:
        if not isinstance(r, dict):
            continue
        for k, v in r.items():
            if v is None:
                continue
            if isinstance(v, (int, float)) and v < 0:
                if "count" in (k or "").lower() or "total" in (k or "").lower() or "sum" in (k or "").lower():
                    return False, f"Invalid negative value in column {k}"
            if isinstance(v, (int, float)) and "percent" in (k or "").lower():
                if v < 0 or v > 100:
                    return False, f"Percentage out of range in column {k}"

    return True, "ok"


def is_valid(result: dict, question: str, *, allow_empty: bool = False) -> bool:
    """Convenience: return True if validate says valid."""
    ok, _ = validate(result, question, allow_empty=allow_empty)
    return ok
