"""
Copilot tools: V1 run_sql/run_sql_raw; V2 discover_tables + run_bigquery_sql.
"""
from __future__ import annotations

import json
import math
import re
from typing import Any, List, Optional

COPILOT_TOOLS = [
    {
        "name": "run_sql",
        "description": "Run a single SELECT (or WITH ... SELECT) against hypeon_marts or hypeon_marts_ads only. Allowed tables: hypeon_marts.fct_sessions (events, item_id, utm_source), hypeon_marts_ads.fct_ad_spend (channel, cost, clicks). Use backtick-quoted names: `project.hypeon_marts.fct_sessions`, `project.hypeon_marts_ads.fct_ad_spend`. Returns JSON with 'rows' and optional 'error'. Use when marts have the data.",
        "input_schema": {
            "type": "object",
            "properties": {
                "query": {
                    "type": "string",
                    "description": "A single SELECT SQL query. Only tables from the injected schema (hypeon_marts, hypeon_marts_ads) are allowed.",
                },
            },
            "required": ["query"],
            "additionalProperties": False,
        },
    },
    {
        "name": "run_sql_raw",
        "description": "Run a read-only SELECT against raw GA4 or Ads tables when marts (run_sql) don't have the needed data or returned empty. Allowed: GA4 events_* tables, Ads ads_AccountBasicStats_* tables. Use backtick-quoted names. Always include LIMIT and, for GA4, filter by event_date. Returns same JSON shape as run_sql (rows, error).",
        "input_schema": {
            "type": "object",
            "properties": {
                "query": {
                    "type": "string",
                    "description": "A single SELECT SQL query. Only GA4 events_* and Ads ads_AccountBasicStats_* tables are allowed. Include LIMIT and date filter for GA4.",
                },
            },
            "required": ["query"],
            "additionalProperties": False,
        },
    },
]

# V2: schema discovery + unified SELECT tool (no dataset whitelist in code)
COPILOT_TOOLS_V2 = [
    {
        "name": "discover_tables",
        "description": "Get a ranked list of candidate tables for a given question intent. Returns project, dataset, table, columns, last_updated, and optional sample_row. Use this to choose the best table(s) before writing SQL.",
        "input_schema": {
            "type": "object",
            "properties": {
                "intent": {
                    "type": "string",
                    "description": "Short intent phrase derived from the user question (e.g. 'views count item_id Facebook', 'ad spend by channel').",
                },
                "limit": {
                    "type": "integer",
                    "description": "Max number of candidate tables to return (default 20).",
                },
            },
            "required": ["intent"],
            "additionalProperties": False,
        },
    },
    {
        "name": "run_bigquery_sql",
        "description": "Execute a single read-only SELECT (or WITH ... SELECT) against the data warehouse. No hard-coded dataset; access is enforced by IAM. Returns rows, schema, row_count, and stats. Do not use INSERT/UPDATE/DELETE/CREATE/DROP/ALTER/MERGE/EXPORT.",
        "input_schema": {
            "type": "object",
            "properties": {
                "query": {
                    "type": "string",
                    "description": "A single SELECT SQL query. Use backtick-quoted table names: `project.dataset.table`.",
                },
                "dry_run": {
                    "type": "boolean",
                    "description": "If true, validate query without executing (default false).",
                },
            },
            "required": ["query"],
            "additionalProperties": False,
        },
    },
]


def _normalize_tool_arguments(arguments: Any) -> dict:
    """Ensure tool arguments are always a dict (API may return a JSON string)."""
    if arguments is None:
        return {}
    if isinstance(arguments, dict):
        return arguments
    if isinstance(arguments, str):
        try:
            parsed = json.loads(arguments)
            return parsed if isinstance(parsed, dict) else {}
        except (json.JSONDecodeError, TypeError):
            return {}
    return {}


def _serialize_rows(rows: list[dict]) -> list[dict]:
    """Serialize BigQuery row dicts for JSON (dates, NaN)."""
    serialized = []
    for r in rows:
        row = {}
        for k, v in r.items():
            if hasattr(v, "isoformat"):
                row[k] = v.isoformat()
            elif v is not None and isinstance(v, float) and (math.isnan(v) or math.isinf(v)):
                row[k] = None
            else:
                row[k] = v
        serialized.append(row)
    return serialized


def _rank_tables_by_intent(tables: List[dict], intent: str) -> List[dict]:
    """Rank candidate tables by keyword overlap with intent (table name + column names)."""
    intent_lower = (intent or "").strip().lower()
    tokens = set(re.findall(r"\w+", intent_lower))
    if not tokens:
        return tables[:20]
    scored: List[tuple[float, dict]] = []
    for t in tables:
        table_name = (t.get("table_name") or "").lower()
        cols = t.get("columns") or []
        col_names = " ".join((c.get("name") or "").lower() for c in cols).split()
        all_text = table_name + " " + " ".join(col_names)
        all_tokens = set(re.findall(r"\w+", all_text))
        match = len(tokens & all_tokens)
        col_match = sum(1 for c in col_names if c in tokens)
        score = match + 0.5 * col_match
        scored.append((score, t))
    scored.sort(key=lambda x: -x[0])
    return [t for _, t in scored]


def discover_tables(intent: str, limit: int = 20) -> List[dict]:
    """
    Return ranked candidate tables for the given intent. Uses schema cache; on miss
    queries INFORMATION_SCHEMA and ranks by keyword match.
    """
    from .defaults import get_discover_tables_limit
    from .schema_cache import schema_cache_get, schema_cache_set
    from ..clients.bigquery import list_tables_for_discovery

    limit = min(limit or get_discover_tables_limit(), 50)
    cached = schema_cache_get(intent)
    if cached is not None:
        return cached[:limit]
    raw = list_tables_for_discovery()
    ranked = _rank_tables_by_intent(raw, intent)
    result = []
    for t in ranked[:limit]:
        row = {
            "project": t.get("project"),
            "dataset": t.get("dataset"),
            "table": t.get("table_name"),
            "columns": [c.get("name") for c in (t.get("columns") or []) if c.get("name")],
            "last_updated": t.get("last_updated"),
            "sample_row": {},
        }
        result.append(row)
    schema_cache_set(intent, result)
    return result


def run_bigquery_sql(
    sql: str,
    organization_id: str,
    client_id: int,
    dry_run: bool = False,
    max_rows: int = 500,
    timeout_sec: float = 20.0,
) -> dict:
    """
    Execute read-only BigQuery SQL. Returns dict with rows, schema, row_count, stats, error.
    """
    from ..clients.bigquery import run_bigquery_sql_readonly

    out = run_bigquery_sql_readonly(
        sql=sql,
        client_id=client_id,
        organization_id=organization_id,
        max_rows=max_rows,
        timeout_sec=timeout_sec,
        dry_run=dry_run,
    )
    rows = out.get("rows") or []
    out["rows"] = _serialize_rows(rows)
    out["row_count"] = len(out["rows"])
    return out


def execute_tool(
    organization_id: str,
    client_id: int,
    tool_name: str,
    arguments: Optional[dict] = None,
) -> str:
    """
    Execute a Copilot tool: V1 run_sql/run_sql_raw; V2 discover_tables/run_bigquery_sql.
    """
    args = _normalize_tool_arguments(arguments)
    cid = int(client_id) if client_id is not None else 1

    if tool_name == "run_sql":
        from ..clients.bigquery import run_readonly_query
        query = (args.get("query") or "").strip()
        if not query:
            return json.dumps({"rows": [], "error": "Missing query."})
        out = run_readonly_query(
            sql=query,
            client_id=cid,
            organization_id=organization_id,
            max_rows=500,
            timeout_sec=15.0,
        )
        rows = out.get("rows") or []
        serialized = _serialize_rows(rows)
        return json.dumps({"rows": serialized, "error": out.get("error"), "row_count": len(serialized)})

    if tool_name == "run_sql_raw":
        from ..clients.bigquery import run_readonly_query_raw
        query = (args.get("query") or "").strip()
        if not query:
            return json.dumps({"rows": [], "error": "Missing query."})
        out = run_readonly_query_raw(
            sql=query,
            client_id=cid,
            organization_id=organization_id,
            max_rows=500,
            timeout_sec=20.0,
        )
        rows = out.get("rows") or []
        serialized = _serialize_rows(rows)
        return json.dumps({"rows": serialized, "error": out.get("error"), "row_count": len(serialized)})

    if tool_name == "discover_tables":
        intent = (args.get("intent") or "").strip() or "analytics"
        limit = args.get("limit")
        if limit is not None:
            try:
                limit = min(50, max(1, int(limit)))
            except (TypeError, ValueError):
                limit = 20
        else:
            from .defaults import get_discover_tables_limit
            limit = get_discover_tables_limit()
        candidates = discover_tables(intent, limit=limit)
        return json.dumps({"candidates": candidates})

    if tool_name == "run_bigquery_sql":
        query = (args.get("query") or "").strip()
        if not query:
            return json.dumps({"rows": [], "schema": [], "row_count": 0, "stats": {}, "error": "Missing query."})
        dry_run = bool(args.get("dry_run", False))
        out = run_bigquery_sql(
            sql=query,
            organization_id=organization_id,
            client_id=cid,
            dry_run=dry_run,
        )
        return json.dumps(out)

    return json.dumps({"error": f"Unknown tool: {tool_name}"})
