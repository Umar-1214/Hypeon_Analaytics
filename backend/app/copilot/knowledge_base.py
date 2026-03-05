"""
Knowledge base for Copilot: schema ONLY from hypeon_marts and hypeon_marts_ads INFORMATION_SCHEMA.
No static table names. No fallback to discovery or raw datasets. If schema fetch fails, return error.
"""
from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Optional

# In-process cache for get_schema_for_copilot() to avoid re-reading on every request.
_SCHEMA_CACHE: str | None = None


def get_bq_project() -> str:
    return os.environ.get("BQ_PROJECT", "braided-verve-459208-i6")


def get_bq_source_project() -> str:
    """Project where GA4 raw events live (BQ_SOURCE_PROJECT or BQ_PROJECT)."""
    return os.environ.get("BQ_SOURCE_PROJECT") or get_bq_project()


def get_analytics_dataset() -> str:
    return os.environ.get("ANALYTICS_DATASET", "analytics")


def get_ads_dataset() -> str:
    """Dataset for Ads (from .env ADS_DATASET, e.g. 146568). Never delete."""
    return os.environ.get("ADS_DATASET", "146568")


def get_ga4_dataset() -> str:
    """Dataset for GA4 (from .env GA4_DATASET, e.g. analytics_444259275). Never delete."""
    return os.environ.get("GA4_DATASET", "analytics_444259275")


def get_marts_dataset() -> str:
    """Marts dataset (hypeon_marts). Primary schema source for Copilot."""
    return os.environ.get("MARTS_DATASET", "hypeon_marts")


def _discovery_path() -> Path:
    """Path to bigquery_discovery.json (repo root / bigquery_schema / bigquery_discovery.json)."""
    env_path = os.environ.get("BQ_DISCOVERY_PATH", "").strip()
    if env_path:
        return Path(env_path)
    # backend/app/copilot/knowledge_base.py -> parents[3] = repo root
    repo_root = Path(__file__).resolve().parents[3]
    return repo_root / "bigquery_schema" / "bigquery_discovery.json"


def _samples_path() -> Path:
    """Path to copilot_samples.json (written by scripts/copilot_fetch_samples.py)."""
    env_path = os.environ.get("BQ_COPILOT_SAMPLES_PATH", "").strip()
    if env_path:
        return Path(env_path)
    repo_root = Path(__file__).resolve().parents[3]
    return repo_root / "bigquery_schema" / "copilot_samples.json"


def _raw_schema_path() -> Path:
    """Path to raw_copilot_schema.json (written by scripts/copilot_fetch_raw_schema.py)."""
    env_path = os.environ.get("BQ_RAW_COPILOT_SCHEMA_PATH", "").strip()
    if env_path:
        return Path(env_path)
    repo_root = Path(__file__).resolve().parents[3]
    return repo_root / "bigquery_schema" / "raw_copilot_schema.json"


def _marts_catalog_path() -> Path:
    """Path to copilot_marts_catalog.json (written by scripts/copilot_fetch_marts_catalog.py)."""
    env_path = os.environ.get("BQ_MARTS_CATALOG_PATH", "").strip()
    if env_path:
        return Path(env_path)
    repo_root = Path(__file__).resolve().parents[3]
    return repo_root / "bigquery_schema" / "copilot_marts_catalog.json"


def _all_schemas_path() -> Path:
    """Path to all_schemas_and_samples.json (written by scripts/fetch_all_schemas_and_samples.py)."""
    env_path = os.environ.get("BQ_ALL_SCHEMAS_PATH", "").strip()
    if env_path:
        return Path(env_path)
    repo_root = Path(__file__).resolve().parents[3]
    return repo_root / "bigquery_schema" / "all_schemas_and_samples.json"


# Cache for unified schema file: (path_str, mtime) -> parsed dict. Invalidated when file changes.
_ALL_SCHEMAS_CACHE: tuple[str, float, dict] | None = None


def _load_all_schemas_and_samples() -> dict | None:
    """Load all_schemas_and_samples.json if present; return None if missing or invalid. Cached by path + mtime."""
    global _ALL_SCHEMAS_CACHE
    path = _all_schemas_path()
    if not path.is_file():
        return None
    try:
        mtime = path.stat().st_mtime
        path_str = str(path)
        if _ALL_SCHEMAS_CACHE is not None and _ALL_SCHEMAS_CACHE[0] == path_str and _ALL_SCHEMAS_CACHE[1] == mtime:
            return _ALL_SCHEMAS_CACHE[2]
        data = json.loads(path.read_text(encoding="utf-8"))
        if not isinstance(data, dict) or "datasets" not in data:
            return None
        _ALL_SCHEMAS_CACHE = (path_str, mtime, data)
        return data
    except (json.JSONDecodeError, OSError):
        return None


def _format_datasets_catalog(
    datasets_dict: dict,
    project: str,
    max_cols: int = 50,
    max_sample_snippet: int = 500,
) -> str:
    """Build catalog text from datasets_dict (ds_id -> { tables: { table_id -> { schema, sample_rows } } })."""
    parts: list[str] = []
    for ds_id, ds_obj in sorted(datasets_dict.items()):
        if not isinstance(ds_obj, dict):
            continue
        tables = ds_obj.get("tables") or {}
        for table_id, tbl in sorted(tables.items()):
            if not isinstance(tbl, dict) or tbl.get("error"):
                continue
            schema = tbl.get("schema") or []
            flat = _flatten_schema(schema)
            col_list = ", ".join(f"{n} ({t})" for n, t in flat[:max_cols])
            if len(flat) > max_cols:
                col_list += f", ... +{len(flat) - max_cols} more"
            parts.append(f"- **{ds_id}.{table_id}** (project: {project})")
            parts.append(f"  Columns: {col_list}")
            for i, row in enumerate((tbl.get("sample_rows") or [])[:2]):
                if isinstance(row, dict):
                    parts.append(f"  Sample {i + 1}: {json.dumps(row, default=str)[:max_sample_snippet]}")
            parts.append("")
    return "\n".join(parts).strip()


# Max chars for raw schema section to avoid blowing the prompt.
_RAW_SCHEMA_MAX_CHARS = 15_000
_MARTS_CATALOG_MAX_CHARS = 12_000


def _load_samples_section() -> str:
    """Load optional sample rows and return a short 'Sample rows (for reference)' section, or empty string."""
    path = _samples_path()
    if not path.is_file():
        return ""
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, OSError):
        return ""
    if not isinstance(data, dict):
        return ""
    parts = ["", "## Sample rows (for reference)", ""]
    for ds_id, tables in sorted(data.items()):
        if not isinstance(tables, dict):
            continue
        for table_id, rows in sorted(tables.items()):
            if not isinstance(rows, list) or not rows:
                continue
            parts.append(f"### {ds_id}.{table_id}")
            for i, row in enumerate(rows[:2]):
                if isinstance(row, dict):
                    parts.append(json.dumps(row, default=str)[:800])
            parts.append("")
    if len(parts) <= 3:
        return ""
    return "\n".join(parts)


def _flatten_schema(schema: list[dict], prefix: str = "") -> list[tuple[str, str]]:
    """Flatten schema to (name, type) including nested RECORD/STRUCT fields for LLM readability."""
    out: list[tuple[str, str]] = []
    for col in schema or []:
        name = prefix + (col.get("name") or "?")
        typ = col.get("type") or "?"
        out.append((name, typ))
        if col.get("fields"):
            for sub in _flatten_schema(col["fields"], prefix=name + "."):
                out.append(sub)
    return out


# Max chars for schema section to avoid blowing the prompt (leave room for system text + tools).
_SCHEMA_MAX_CHARS = 55_000


def _format_live_marts_schema(rows: list[dict], project: str, marts: str) -> str:
    """Format live INFORMATION_SCHEMA rows into schema text (marts datasets, dynamic)."""
    from collections import defaultdict
    by_key = defaultdict(list)  # key = (dataset, table_name)
    for r in rows:
        ds = (r.get("dataset") or marts).strip()
        tn = (r.get("table_name") or "").strip()
        cn = (r.get("column_name") or "").strip()
        if tn and cn:
            by_key[(ds, tn)].append(cn)
    datasets = sorted(set(ds for (ds, tn) in by_key.keys())) or [marts]
    parts = [
        "## Database: BigQuery (read-only). Schema from configured marts datasets, live.",
        f"- Project: {project}. Datasets: {', '.join(datasets)}.",
        "- Use backtick-quoted names: `project.dataset.table`. Use only the tables and columns listed below.",
        "",
        "## Tables and columns (INFORMATION_SCHEMA)",
        "",
    ]
    for (ds, table_name) in sorted(by_key.keys()):
        cols = by_key[(ds, table_name)][:80]
        col_lines = [f"  - {c}" for c in cols]
        if len(by_key[(ds, table_name)]) > 80:
            col_lines.append(f"  - ... and {len(by_key[(ds, table_name)]) - 80} more")
        parts.append(f"- **{ds}.{table_name}**")
        parts.extend(col_lines)
        parts.append("")
    return "\n".join(parts)


def get_schema_for_copilot(use_cache: bool = True) -> str:
    """
    Schema ONLY from hypeon_marts and hypeon_marts_ads INFORMATION_SCHEMA (dynamic).
    No fallback. If fetch fails, return explicit error so the assistant tells the user.
    """
    global _SCHEMA_CACHE
    if use_cache and _SCHEMA_CACHE is not None:
        return _SCHEMA_CACHE
    project = get_bq_project()
    marts_ds = get_marts_dataset().strip()
    marts_ads_ds = os.environ.get("MARTS_ADS_DATASET", "hypeon_marts_ads").strip()

    try:
        from ..clients.bigquery import get_marts_schema_live
        live_rows = get_marts_schema_live()
        if not live_rows:
            err = _schema_error_message("Marts schema returned no tables. Ensure the configured marts datasets exist and contain base tables or views.")
            _SCHEMA_CACHE = err
            return err
        schema_text = _format_live_marts_schema(live_rows, project, marts_ds)
        schema_text += _marts_only_rules(project, marts_ds, marts_ads_ds)
        catalog_section = get_marts_catalog_for_copilot()
        if catalog_section:
            schema_text += "\n" + catalog_section
        _SCHEMA_CACHE = schema_text
        return schema_text
    except Exception as e:
        err = _schema_error_message(f"Could not load marts schema: {str(e)[:200]}. Copilot uses only hypeon_marts and hypeon_marts_ads.")
        _SCHEMA_CACHE = err
        return err


def _schema_error_message(detail: str) -> str:
    """When schema fetch fails, return instructions so the assistant responds with a clear error."""
    return f"""## Database schema unavailable
- {detail}
- Do NOT use ads_daily_staging, ga4_daily_staging, analytics_cache, decision_store, or raw datasets.
- Tell the user: "The analytics schema could not be loaded. Please try again later or contact support."
"""


def get_marts_catalog_for_copilot() -> str:
    """
    Load schema + sample rows for marts (hypeon_marts, hypeon_marts_ads).
    Prefers all_schemas_and_samples.json when present; falls back to copilot_marts_catalog.json.
    """
    data = _load_all_schemas_and_samples()
    if data:
        marts_ds = get_marts_dataset()
        marts_ads_ds = os.environ.get("MARTS_ADS_DATASET", "hypeon_marts_ads")
        ds = data.get("datasets") or {}
        subset = {k: v for k, v in ds.items() if k in (marts_ds, marts_ads_ds)}
        if subset:
            project = data.get("bq_project") or get_bq_project()
            hints = (
                "Prefer marts tables (sessions, orders, ad spend, funnel, dimensions—use whatever tables exist in the schema). "
                "Date filters required for event/session tables."
            )
            body = _format_datasets_catalog(subset, project, max_cols=50, max_sample_snippet=500)
            if body:
                result = "\n".join([
                    "",
                    "## Data catalog (schema + sample rows for accurate queries)",
                    "Use this to see column types and example values. Prefer run_sql against these tables.",
                    "",
                    f"Catalog hints: {hints}",
                    "",
                    body,
                ]).strip()
                if len(result) > _MARTS_CATALOG_MAX_CHARS:
                    result = result[:_MARTS_CATALOG_MAX_CHARS] + "\n... (truncated)"
                return result
    # Fallback: copilot_marts_catalog.json
    path = _marts_catalog_path()
    if not path.is_file():
        return ""
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, OSError):
        return ""
    if not isinstance(data, dict):
        return ""
    project = data.get("project") or get_bq_project()
    hints = (data.get("hints") or "").strip()
    datasets = data.get("datasets") or {}
    parts = [
        "",
        "## Data catalog (schema + sample rows for accurate queries)",
        "Use this to see column types and example values. Prefer run_sql against these tables.",
        "",
    ]
    if hints:
        parts.append(f"Catalog hints: {hints}")
        parts.append("")
    for ds_id, ds_obj in sorted(datasets.items()):
        if not isinstance(ds_obj, dict):
            continue
        tables = ds_obj.get("tables") or {}
        for table_id, tbl in sorted(tables.items()):
            if not isinstance(tbl, dict):
                continue
            if tbl.get("error"):
                continue
            schema = tbl.get("schema") or []
            flat = _flatten_schema(schema)
            col_list = ", ".join(f"{n} ({t})" for n, t in flat[:50])
            if len(flat) > 50:
                col_list += f", ... +{len(flat) - 50} more"
            parts.append(f"- **{ds_id}.{table_id}** (project: {project})")
            parts.append(f"  Columns: {col_list}")
            for i, row in enumerate((tbl.get("sample_rows") or [])[:2]):
                if isinstance(row, dict):
                    parts.append(f"  Sample {i + 1}: {json.dumps(row, default=str)[:500]}")
            parts.append("")
    result = "\n".join(parts).strip()
    if len(result) > _MARTS_CATALOG_MAX_CHARS:
        result = result[:_MARTS_CATALOG_MAX_CHARS] + "\n... (truncated)"
    return result


def _marts_only_rules(project: str, marts: str, marts_ads: str) -> str:
    """Rules for Copilot: schema-agnostic. Use tables/columns from the schema above; no hardcoded table names."""
    return f"""

## Allowed tables
- Use ONLY the tables and columns listed in the schema/catalog above (from datasets {marts}, {marts_ads} or as discovered).
- Do NOT reference ads_daily_staging, ga4_daily_staging, analytics_cache, decision_store, or raw datasets unless present in the schema.

## Query behavior (user intent -> SQL)
- **Views / item views**: Use the table(s) that have event_name and item_id; filter event_name IN ('view_item','view_item_list'), item_id LIKE 'prefix%' when those columns exist.
- **Item views from a source (e.g. Google)**: Same as above and filter by utm_source (or source column) e.g. utm_source LIKE '%google%'.
- **Channel / ad spend**: Use the table(s) that have channel, cost, clicks, conversions (or similar); filter by channel. Channel values vary by org (e.g. google_ads, meta_ads, pinterest_ads).

## Date filter (required for event/session tables)
- Add a date filter to queries that use event or session tables (e.g. event_time >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 30 DAY) or event_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)) to avoid exceeding the bytes limit.

## Unavailable channel
- If the user asks for a channel that is NOT in the data: run SELECT DISTINCT channel (or equivalent) on the ad/spend table from the schema to get the actual list; then respond with "[Channel] channel data is not currently present. Available channels: [list from your query result]. Once [Channel] data is integrated, this query will be supported."

## Query guidelines
- Use only SELECT. Use the exact table and column names from the schema above.
- Filter by date when relevant.
"""


def get_raw_schema_for_copilot(organization_id: Optional[str] = None) -> str:
    """
    Load schema + sample rows for raw datasets (GA4, Ads).
    When organization_id is set, uses org BQ config from Firestore for dataset names; else env.
    When organization_id is set and org has no config, returns a clear message (no shared env fallback).
    Prefers all_schemas_and_samples.json when present; falls back to raw_copilot_schema.json.
    Capped at _RAW_SCHEMA_MAX_CHARS.
    """
    ctx = None
    if organization_id:
        try:
            from ..auth.firestore_user import get_org_bq_context
            ctx = get_org_bq_context(organization_id)
        except Exception:
            pass
    o = (organization_id or "").strip()
    if o and o.lower() != "default" and not ctx:
        return "Datasets are not configured for your organization. Please ask your administrator to set up BigQuery data sources in your organization settings."
    data = _load_all_schemas_and_samples()
    if data:
        ga4_ds = (ctx.get("ga4_dataset") if ctx else None) or get_ga4_dataset()
        ads_ds = (ctx.get("ads_dataset") if ctx else None) or get_ads_dataset()
        ds = data.get("datasets") or {}
        subset = {k: v for k, v in ds.items() if k in (ga4_ds, ads_ds)}
        if subset:
            project = (ctx.get("bq_source_project") if ctx else None) or data.get("bq_source_project") or get_bq_source_project()
            hints = (
                "GA4 events_*: use UNNEST(event_params), UNNEST(items); filter by event_date. "
                "Ads: filter by segments_date. Funnel: event_name IN ('view_item','add_to_cart','begin_checkout','purchase','session_start'). "
                "Landing page: page_location and first event per session."
            )
            body = _format_datasets_catalog(subset, project, max_cols=40, max_sample_snippet=400)
            if body:
                result = "\n".join([
                    "",
                    "## Fallback: raw data (run_sql_raw)",
                    "Use run_sql_raw only when marts (run_sql) don't have the needed data or returned no rows.",
                    "Allowed: GA4 events_* tables, Ads ads_* tables. Always include LIMIT and, for GA4, a date filter (event_date).",
                    "",
                    f"Query hints: {hints}",
                    "",
                    body,
                ]).strip()
                if len(result) > _RAW_SCHEMA_MAX_CHARS:
                    result = result[:_RAW_SCHEMA_MAX_CHARS] + "\n... (truncated)"
                return result if result else "Raw data schema not available; use marts only."
    # Fallback: raw_copilot_schema.json
    path = _raw_schema_path()
    if not path.is_file():
        return "Raw data schema not available; use marts only (run_sql)."
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, OSError):
        return "Raw data schema not available; use marts only."
    if not isinstance(data, dict):
        return "Raw data schema not available; use marts only."
    project = data.get("project") or get_bq_source_project()
    hints = (data.get("hints") or "").strip()
    datasets = data.get("datasets") or {}
    parts = [
        "",
        "## Fallback: raw data (run_sql_raw)",
        "Use run_sql_raw only when marts (run_sql) don't have the needed data or returned no rows.",
        "Allowed: GA4 events_* tables, Ads ads_AccountBasicStats_* tables. Always include LIMIT and, for GA4, a date filter (event_date).",
        "Funnel/checkout: use event_name IN ('view_item','add_to_cart','begin_checkout','purchase','session_start'). Landing page: use page_location and first event per session.",
        "",
    ]
    if hints:
        parts.append(f"Query hints: {hints}")
        parts.append("")
    for ds_id, ds_obj in sorted(datasets.items()):
        if not isinstance(ds_obj, dict):
            continue
        tables = ds_obj.get("tables") or {}
        for table_id, table_info in sorted(tables.items()):
            if not isinstance(table_info, dict):
                continue
            schema = table_info.get("schema") or []
            sample_rows = table_info.get("sample_rows") or []
            flat = _flatten_schema(schema)
            col_list = ", ".join(f"{n} ({t})" for n, t in flat[:40])
            if len(flat) > 40:
                col_list += f", ... and {len(flat) - 40} more"
            parts.append(f"- **{ds_id}.{table_id}** (project: {project})")
            parts.append(f"  Columns: {col_list}")
            for i, row in enumerate(sample_rows[:2]):
                if isinstance(row, dict):
                    snippet = json.dumps(row, default=str)[:400]
                    parts.append(f"  Sample {i + 1}: {snippet}")
            parts.append("")
    result = "\n".join(parts).strip()
    if len(result) > _RAW_SCHEMA_MAX_CHARS:
        result = result[:_RAW_SCHEMA_MAX_CHARS] + "\n... (truncated)"
    return result if result else "Raw data schema not available; use marts only."
