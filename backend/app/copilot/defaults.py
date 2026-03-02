"""Copilot V2 configurable constants: retries, schema cache TTL, planner settings."""
from __future__ import annotations

import os


def get_max_retries() -> int:
    """Max replan/retry attempts when SQL returns no rows (default 3)."""
    try:
        return max(1, min(int(os.environ.get("COPILOT_MAX_RETRIES", "3")), 10))
    except (TypeError, ValueError):
        return 3


def get_schema_cache_ttl_seconds() -> int:
    """Schema discovery cache TTL in seconds (default 3600 = 1 hour)."""
    try:
        return max(60, min(int(os.environ.get("COPILOT_SCHEMA_CACHE_TTL", "3600")), 86400))
    except (TypeError, ValueError):
        return 3600


def get_discover_tables_limit() -> int:
    """Max candidate tables to return from discover_tables (default 20)."""
    try:
        return max(5, min(int(os.environ.get("COPILOT_DISCOVER_TABLES_LIMIT", "20")), 50))
    except (TypeError, ValueError):
        return 20


PLANNER_SETTINGS = {
    "max_sql_templates_per_plan": 5,
    "intent_max_tokens": 100,
}
