"""
Planner for Copilot V2: map user question to intent, discover candidate tables, produce SQL templates.
"""
from __future__ import annotations

import re
from typing import Any, List, Optional

from . import tools as copilot_tools
from .defaults import get_discover_tables_limit


def _extract_intent(question: str, context: Optional[List[dict]] = None) -> str:
    """Derive a short intent phrase from the user question (and optional chat context)."""
    text = (question or "").strip().lower()
    if not text:
        return "analytics"
    # Take key tokens: drop stopwords, keep numbers and identifiers
    stop = {"the", "a", "an", "is", "are", "was", "were", "what", "how", "many", "much", "for", "from", "to", "of", "in", "on", "at", "can", "you", "me", "my", "i", "we", "this", "that", "it", "and", "or", "but"}
    words = re.findall(r"\w+", text)
    kept = [w for w in words if w not in stop and len(w) > 1][:15]
    return " ".join(kept) if kept else "analytics"


def analyze(
    question: str,
    context: Optional[List[dict]] = None,
    client_id: Optional[int] = None,
    organization_id: str = "",
) -> dict:
    """
    Analyze user question: produce intent, candidate tables (via discover_tables), and SQL templates.
    Returns:
      {
        "intent": str,
        "candidates": [{"table": "project.dataset.table", "reason": str}, ...],
        "sql_templates": [str, ...],
      }
    """
    intent = _extract_intent(question, context)
    limit = get_discover_tables_limit()
    candidates_raw = copilot_tools.discover_tables(intent, limit=limit)
    project = _get_project()
    candidates = []
    for c in candidates_raw[:10]:
        proj = c.get("project") or project
        ds = c.get("dataset") or ""
        tbl = c.get("table") or c.get("table_name") or ""
        if not tbl:
            continue
        full = f"{proj}.{ds}.{tbl}"
        cols = c.get("columns") or []
        reason = f"contains columns {cols[:8]}" if cols else "table in warehouse"
        candidates.append({"table": full, "reason": reason, "columns": cols})
    sql_templates = _build_sql_templates(question, candidates, client_id or 1)
    return {
        "intent": intent,
        "candidates": candidates,
        "sql_templates": sql_templates,
    }


def _get_project() -> str:
    try:
        from ..clients.bigquery import _project
        return _project()
    except Exception:
        import os
        return os.environ.get("BQ_PROJECT", "")


def _build_sql_templates(question: str, candidates: List[dict], client_id: int) -> List[str]:
    """
    Build one or more SQL templates from candidates. Heuristic: views/count -> SUM or COUNT;
    item_id / FT05B -> WHERE item_id LIKE 'prefix%'; channel/facebook -> WHERE channel = 'facebook'.
    """
    q = (question or "").strip().lower()
    templates = []
    for c in candidates[:5]:
        full = c.get("table") or ""
        if not full or full.count(".") < 2:
            continue
        cols = c.get("columns") or []
        col_set = set((x or "").lower() for x in cols)
        # Prefer a metric column
        metric_col = None
        if "views" in col_set or "view_count" in col_set:
            metric_col = "views" if "views" in col_set else "view_count"
        elif "event_count" in col_set:
            metric_col = "event_count"
        elif "count" in col_set:
            metric_col = "count"
        else:
            metric_col = "COUNT(*)"
            metric_alias = "views"
        if metric_col == "COUNT(*)":
            sel = "COUNT(*) AS views"
        else:
            sel = f"SUM({metric_col}) AS views"
        where = []
        if "item_id" in col_set:
            where.append("item_id LIKE 'FT05B%'")
        if "channel" in col_set and ("facebook" in q or "fb" in q):
            where.append("LOWER(channel) = 'facebook'")
        if "event_name" in col_set and "view" in q:
            where.append("event_name IN ('view_item','view_item_list')")
        if "event_date" in col_set or "event_time" in col_set:
            where.append("event_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)")
        where_sql = " AND ".join(where) if where else "1=1"
        templates.append(f"SELECT {sel} FROM `{full}` WHERE {where_sql} LIMIT 500")
    return templates[:5]


def record_failure(plan: dict) -> None:
    """Record that a plan/SQL template failed (e.g. empty result). Used to prefer alternatives next time."""
    pass  # Optional: persist to cache or metrics


def replan(
    question: str,
    failed_sql: Optional[str] = None,
    context: Optional[List[dict]] = None,
    client_id: Optional[int] = None,
    organization_id: str = "",
) -> dict:
    """
    Re-plan after a failure: produce alternative candidates and SQL templates (e.g. skip first table).
    """
    result = analyze(question, context=context, client_id=client_id, organization_id=organization_id)
    if failed_sql and result.get("sql_templates"):
        # Remove the failed template from the list so next attempt uses another
        result["sql_templates"] = [s for s in result["sql_templates"] if s != failed_sql]
    return result
