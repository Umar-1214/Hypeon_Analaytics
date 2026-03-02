"""BigQuery client for HypeOn Analytics V1. Enterprise: organization_id, workspace_id, scoped queries."""
from __future__ import annotations

import logging
import math
import os
import uuid
from datetime import date, timedelta
from typing import Any, Optional

import pandas as pd


def _is_table_not_found(exc: BaseException) -> bool:
    """True if the exception indicates a missing table (404 / not found)."""
    msg = (str(exc) or "").lower()
    return "not found" in msg or "404" in msg or "notfound" in msg

_client: Any = None


def get_client():
    global _client
    if _client is None:
        from google.cloud import bigquery
        project = os.environ.get("BQ_PROJECT", "braided-verve-459208-i6")
        location = os.environ.get("BQ_LOCATION")
        _client = bigquery.Client(project=project, location=location) if location else bigquery.Client(project=project)
    return _client


def get_analytics_dataset() -> str:
    return os.environ.get("ANALYTICS_DATASET", "analytics")


def get_ads_dataset() -> str:
    """Dataset for Ads raw/staging (from .env ADS_DATASET, e.g. 146568). Used by Copilot run_sql."""
    return os.environ.get("ADS_DATASET", "146568")


def get_ga4_dataset() -> str:
    """Dataset for GA4 raw/staging (from .env GA4_DATASET, e.g. analytics_444259275). Used by Copilot run_sql."""
    return os.environ.get("GA4_DATASET", "analytics_444259275")


def get_marts_dataset() -> str:
    """Marts dataset (hypeon_marts, europe-north2). Copilot primary schema and queries."""
    return os.environ.get("MARTS_DATASET", "hypeon_marts")


def get_marts_ads_dataset() -> str:
    """Ads marts dataset (hypeon_marts_ads, EU). For fct_ad_spend."""
    return os.environ.get("MARTS_ADS_DATASET", "hypeon_marts_ads")


def _project() -> str:
    return os.environ.get("BQ_PROJECT", "braided-verve-459208-i6")


def _source_project() -> str:
    """Project where GA4/Ads raw data lives (e.g. events_*). Used for item views count."""
    return os.environ.get("BQ_SOURCE_PROJECT") or _project()


# Copilot run_sql: ONLY hypeon_marts and hypeon_marts_ads. No raw/staging. No fallback.
def _copilot_allowed_datasets() -> frozenset[str]:
    """Only marts datasets allowed. No ads_daily_staging, ga4_daily_staging, analytics_cache, raw."""
    marts = get_marts_dataset().strip().lower()
    marts_ads = get_marts_ads_dataset().strip().lower()
    return frozenset({marts, marts_ads})


def _copilot_allowed_tables() -> frozenset[tuple[str, str]] | None:
    """Set of (dataset, table_name) from marts INFORMATION_SCHEMA. None if schema fetch fails."""
    rows = get_marts_schema_live()
    if not rows:
        return None
    out: set[tuple[str, str]] = set()
    for r in rows:
        ds = (r.get("dataset") or "").strip().lower()
        tn = (r.get("table_name") or "").strip().lower()
        if ds and tn:
            out.add((ds, tn))
    return frozenset(out) if out else None


def run_readonly_query(
    sql: str,
    client_id: int,
    organization_id: str,
    max_rows: int = 500,
    timeout_sec: float = 15.0,
) -> dict:
    """
    Run a read-only BigQuery query for Copilot. Validates SELECT only and allowed tables.
    Returns {"rows": [...], "error": None} or {"rows": [], "error": "message"}.
    """
    import re
    from google.cloud import bigquery

    sql = (sql or "").strip()
    if not sql:
        return {"rows": [], "error": "Empty query."}

    # Single statement only: no semicolon (except trailing)
    sql_normalized = sql.rstrip(";").strip()
    if ";" in sql_normalized:
        return {"rows": [], "error": "Only a single SELECT statement is allowed."}

    # Must be SELECT only (allow WITH ... SELECT)
    upper = sql_normalized.upper()
    if not upper.startswith("SELECT") and not upper.startswith("WITH"):
        return {"rows": [], "error": "Only SELECT (or WITH ... SELECT) queries are allowed."}
    for verb in ("INSERT", "UPDATE", "DELETE", "DROP", "CREATE", "ALTER", "TRUNCATE", "GRANT", "REVOKE"):
        if verb in upper:
            return {"rows": [], "error": f"Only read-only SELECT is allowed (no {verb})."}

    project = _project().lower()
    allowed_datasets = _copilot_allowed_datasets()
    # Runtime validation: only tables that exist in marts INFORMATION_SCHEMA
    allowed_tables = _copilot_allowed_tables()
    if allowed_tables is None:
        return {"rows": [], "error": "Could not load marts schema. Copilot uses only hypeon_marts and hypeon_marts_ads."}
    pattern = r"`([^`]+)`"
    for match in re.finditer(pattern, sql):
        ref = match.group(1).strip().lower()
        parts = ref.split(".")
        if len(parts) != 3:
            continue
        ref_project, ref_dataset, table_part = parts
        if ref_project != project:
            return {"rows": [], "error": f"Only tables in project {_project()} are allowed."}
        if ref_dataset not in allowed_datasets:
            return {"rows": [], "error": f"Dataset not allowed: {ref_dataset}. Use only hypeon_marts or hypeon_marts_ads."}
        if (ref_dataset, table_part) not in allowed_tables:
            return {"rows": [], "error": f"Table {ref_dataset}.{table_part} is not in marts schema. Allowed: fct_sessions (hypeon_marts), fct_ad_spend (hypeon_marts_ads), and related views."}

    # Enforce LIMIT if not present (BigQuery allows no LIMIT but we want to cap rows)
    if "LIMIT" not in upper:
        sql_normalized = f"{sql_normalized} LIMIT {max_rows}"

    # Configurable bytes cap so fct_sessions with date filter can succeed (default 300 MB)
    try:
        max_mb = int(os.environ.get("BQ_COPILOT_MAX_BYTES_BILLED_MB", "300"))
        max_mb = max(50, min(max_mb, 1024))  # clamp 50 MB–1 GB
    except (TypeError, ValueError):
        max_mb = 300
    max_bytes_billed = max_mb * 1024 * 1024

    client = get_client()
    job_config = bigquery.QueryJobConfig(maximum_bytes_billed=max_bytes_billed)
    try:
        query_job = client.query(sql_normalized, job_config=job_config)
        # Wait with timeout; then fetch up to max_rows
        iterator = query_job.result(max_results=max_rows, timeout=timeout_sec)
        rows = []
        for row in iterator:
            rows.append(dict(row.items()))
        return {"rows": rows, "error": None}
    except Exception as e:
        return {"rows": [], "error": str(e)[:300]}


# Copilot run_sql_raw: allowed raw datasets and table patterns (GA4 events_*, Ads ads_AccountBasicStats_*).
def _copilot_raw_allowed_datasets() -> frozenset[str]:
    """Datasets allowed for run_sql_raw: GA4_DATASET and ADS_DATASET."""
    ga4 = get_ga4_dataset().strip().lower()
    ads = get_ads_dataset().strip().lower()
    return frozenset({ga4, ads})


def _copilot_raw_table_allowed(dataset: str, table_name: str) -> bool:
    """True if (dataset, table_name) is in the raw allowlist. GA4: events_* (excl. events_intraday_); Ads: ads_AccountBasicStats_*."""
    ds = (dataset or "").strip().lower()
    tn = (table_name or "").strip().lower()
    ga4_ds = get_ga4_dataset().strip().lower()
    ads_ds = get_ads_dataset().strip().lower()
    if ds == ga4_ds:
        return tn.startswith("events_") and not tn.startswith("events_intraday_")
    if ds == ads_ds:
        return tn.startswith("ads_accountbasicstats_")
    return False


def run_readonly_query_raw(
    sql: str,
    client_id: int,
    organization_id: str,
    max_rows: int = 500,
    timeout_sec: float = 20.0,
    max_bytes_billed: Optional[int] = None,
) -> dict:
    """
    Run a read-only BigQuery query for Copilot raw fallback. Only GA4 events_* and Ads ads_AccountBasicStats_*.
    Same SELECT-only checks as run_readonly_query; validates tables against raw allowlist. Enforces LIMIT and max_bytes_billed.
    Returns {"rows": [...], "error": None} or {"rows": [], "error": "message"}.
    """
    import re
    from google.cloud import bigquery

    if max_bytes_billed is None:
        try:
            max_mb = int(os.environ.get("BQ_COPILOT_RAW_MAX_BYTES_BILLED_MB", "200"))
            max_mb = max(50, min(max_mb, 1024))
        except (TypeError, ValueError):
            max_mb = 200
        max_bytes_billed = max_mb * 1024 * 1024

    sql = (sql or "").strip()
    if not sql:
        return {"rows": [], "error": "Empty query."}

    sql_normalized = sql.rstrip(";").strip()
    if ";" in sql_normalized:
        return {"rows": [], "error": "Only a single SELECT statement is allowed."}

    upper = sql_normalized.upper()
    if not upper.startswith("SELECT") and not upper.startswith("WITH"):
        return {"rows": [], "error": "Only SELECT (or WITH ... SELECT) queries are allowed."}
    for verb in ("INSERT", "UPDATE", "DELETE", "DROP", "CREATE", "ALTER", "TRUNCATE", "GRANT", "REVOKE"):
        if verb in upper:
            return {"rows": [], "error": f"Only read-only SELECT is allowed (no {verb})."}

    project = _source_project().lower()
    allowed_raw_datasets = _copilot_raw_allowed_datasets()
    pattern = r"`([^`]+)`"
    for match in re.finditer(pattern, sql):
        ref = match.group(1).strip().lower()
        parts = ref.split(".")
        if len(parts) != 3:
            continue
        ref_project, ref_dataset, table_part = parts
        if ref_project != project:
            return {"rows": [], "error": f"Only tables in project {_source_project()} are allowed for raw queries."}
        if ref_dataset not in allowed_raw_datasets:
            return {"rows": [], "error": f"Dataset not allowed for raw: {ref_dataset}. Use only GA4 or Ads raw datasets."}
        if not _copilot_raw_table_allowed(ref_dataset, table_part):
            return {"rows": [], "error": f"Table {ref_dataset}.{table_part} not in raw allowlist. GA4: events_*; Ads: ads_AccountBasicStats_*."}

    if "LIMIT" not in upper:
        sql_normalized = f"{sql_normalized} LIMIT {max_rows}"

    location = os.environ.get("BQ_LOCATION", "europe-north2")
    client = bigquery.Client(project=project, location=location)
    job_config = bigquery.QueryJobConfig(maximum_bytes_billed=max_bytes_billed)
    try:
        query_job = client.query(sql_normalized, job_config=job_config)
        iterator = query_job.result(max_results=max_rows, timeout=timeout_sec)
        rows = []
        for row in iterator:
            rows.append(dict(row.items()))
        return {"rows": rows, "error": None}
    except Exception as e:
        return {"rows": [], "error": str(e)[:300]}


# ----- Copilot V2: schema discovery and unified run_bigquery_sql (no dataset whitelist) -----


def _forbidden_sql_keywords() -> tuple[str, ...]:
    """Keywords that make a query non-read-only."""
    return (
        "INSERT", "UPDATE", "DELETE", "DROP", "CREATE", "ALTER",
        "TRUNCATE", "GRANT", "REVOKE", "MERGE", "EXPORT",
    )


def run_bigquery_sql_readonly(
    sql: str,
    client_id: int,
    organization_id: str,
    max_rows: int = 500,
    timeout_sec: float = 20.0,
    dry_run: bool = False,
) -> dict:
    """
    Run a read-only BigQuery query for Copilot V2. Validates SELECT-only (no DML/DDL).
    No hard-coded dataset whitelist; access is enforced by IAM.
    Returns {"rows": [...], "schema": [...], "row_count": int, "stats": {...}, "error": None or str}.
    """
    from google.cloud import bigquery

    sql = (sql or "").strip()
    if not sql:
        return {"rows": [], "schema": [], "row_count": 0, "stats": {}, "error": "Empty query."}

    sql_normalized = sql.rstrip(";").strip()
    if ";" in sql_normalized:
        return {"rows": [], "schema": [], "row_count": 0, "stats": {}, "error": "Only a single SELECT statement is allowed."}

    upper = sql_normalized.upper()
    if not upper.startswith("SELECT") and not upper.startswith("WITH"):
        return {"rows": [], "schema": [], "row_count": 0, "stats": {}, "error": "Only SELECT (or WITH ... SELECT) queries are allowed."}
    for verb in _forbidden_sql_keywords():
        if verb in upper:
            return {"rows": [], "schema": [], "row_count": 0, "stats": {}, "error": f"Only read-only SELECT is allowed (no {verb})."}

    if "LIMIT" not in upper:
        sql_normalized = f"{sql_normalized} LIMIT {max_rows}"

    try:
        max_mb = int(os.environ.get("BQ_COPILOT_MAX_BYTES_BILLED_MB", "300"))
        max_mb = max(50, min(max_mb, 1024))
    except (TypeError, ValueError):
        max_mb = 300
    max_bytes_billed = max_mb * 1024 * 1024

    client = get_client()
    job_config = bigquery.QueryJobConfig(maximum_bytes_billed=max_bytes_billed)

    if dry_run:
        try:
            dry_config = bigquery.QueryJobConfig(dry_run=True)
            client.query(sql_normalized, job_config=dry_config)
            return {"rows": [], "schema": [], "row_count": 0, "stats": {"dry_run": True}, "error": None}
        except Exception as e:
            return {"rows": [], "schema": [], "row_count": 0, "stats": {}, "error": str(e)[:300]}

    try:
        query_job = client.query(sql_normalized, job_config=job_config)
        iterator = query_job.result(max_results=max_rows, timeout=timeout_sec)
        rows = []
        schema_names = [f.name for f in query_job.schema] if query_job.schema else []
        for row in iterator:
            rows.append(dict(row.items()))
        total = getattr(iterator, "total_rows", None) or len(rows)
        return {
            "rows": rows,
            "schema": schema_names,
            "row_count": len(rows),
            "stats": {"total_rows": total, "job_id": getattr(query_job, "job_id", None)},
            "error": None,
        }
    except Exception as e:
        return {"rows": [], "schema": [], "row_count": 0, "stats": {}, "error": str(e)[:300]}


def list_tables_for_discovery(
    project: str | None = None,
    datasets: list[str] | None = None,
    location: str | None = None,
) -> list[dict]:
    """
    List BASE TABLEs from INFORMATION_SCHEMA for the given project/datasets.
    Returns list of {"project", "dataset", "table_name", "columns": [{"name", "data_type"}], "last_updated": ...}.
    If datasets is None, uses MARTS_DATASET, MARTS_ADS_DATASET, GA4_DATASET, ADS_DATASET from env.
    """
    from google.cloud import bigquery

    project = project or _project()
    if datasets is None:
        datasets = [
            get_marts_dataset(),
            get_marts_ads_dataset(),
            get_ga4_dataset(),
            get_ads_dataset(),
        ]
    location = location or os.environ.get("BQ_LOCATION", "europe-north2")
    client = bigquery.Client(project=project, location=location)
    out: list[dict] = []

    for dataset in datasets:
        dataset = (dataset or "").strip()
        if not dataset:
            continue
        try:
            tables_sql = f"""
            SELECT table_catalog, table_schema, table_name
            FROM `{project}.{dataset}.INFORMATION_SCHEMA.TABLES`
            WHERE table_type = 'BASE TABLE'
            """
            tbl_job = client.query(tables_sql)
            for row in tbl_job.result():
                catalog = row.get("table_catalog") or project
                schema = row.get("table_schema") or dataset
                table_name = (row.get("table_name") or "").strip()
                if not table_name:
                    continue
                cols_sql = f"""
                SELECT column_name, data_type
                FROM `{project}.{dataset}.INFORMATION_SCHEMA.COLUMNS`
                WHERE table_name = @tname
                ORDER BY ordinal_position
                """
                job_config = bigquery.QueryJobConfig(
                    query_parameters=[bigquery.ScalarQueryParameter("tname", "STRING", table_name)]
                )
                col_job = client.query(cols_sql, job_config=job_config)
                columns = [{"name": r.get("column_name"), "data_type": r.get("data_type")} for r in col_job.result()]
                out.append({
                    "project": catalog,
                    "dataset": schema,
                    "table_name": table_name,
                    "columns": columns,
                    "last_updated": None,
                })
        except Exception:
            continue
    return out


def get_marts_schema_live() -> list[dict] | None:
    """
    Fetch live schema from hypeon_marts and hypeon_marts_ads for Copilot.
    Returns list of {"table_name": str, "column_name": str} or None on error.
    """
    logger = logging.getLogger(__name__)
    project = _project()
    marts = get_marts_dataset()
    marts_ads = get_marts_ads_dataset()
    out: list[dict] = []
    location = os.environ.get("BQ_LOCATION", "europe-north2")
    location_ads = os.environ.get("BQ_LOCATION_ADS", "EU")
    try:
        from google.cloud import bigquery
        for ds, loc in [(marts, location), (marts_ads, location_ads)]:
            client = bigquery.Client(project=project, location=loc)
            q = f"SELECT table_name, column_name FROM `{project}.{ds}.INFORMATION_SCHEMA.COLUMNS` ORDER BY table_name, ordinal_position"
            df = client.query(q).to_dataframe()
            if not df.empty:
                for r in df.to_dict("records"):
                    r["dataset"] = ds
                    out.append(r)
        return out if out else None
    except Exception as e:
        logger.warning("get_marts_schema_live failed (project=%s, marts=%s, marts_ads=%s): %s", project, marts, marts_ads, e, exc_info=True)
        return None


# GA4 events that represent a product/item view (for get_item_views_count)
_VIEW_ITEM_EVENTS = ("view_item", "view_item_list")


def get_item_views_count(prefix: str = "FT05B") -> dict:
    """
    Return views count for item_id starting with prefix from GA4 raw events_*.
    Returns {"views_count": int, "item_id_prefix": str} or {"error": str}.
    Uses BQ_SOURCE_PROJECT, GA4_DATASET, BQ_LOCATION from env.
    """
    from google.cloud import bigquery

    prefix = (prefix or "FT05B").strip() or "FT05B"
    project = _source_project()
    dataset = get_ga4_dataset()
    location = os.environ.get("BQ_LOCATION", "europe-north2")
    table_ref = f"`{project}.{dataset}.events_*`"
    query = f"""
    SELECT COUNT(*) AS views_count
    FROM {table_ref},
    UNNEST(COALESCE(items, [])) AS it
    WHERE event_date IS NOT NULL
      AND event_name IN {_VIEW_ITEM_EVENTS}
      AND STARTS_WITH(COALESCE(it.item_id, ''), @prefix)
    """
    # events_* scan can exceed 100 MB; allow 200 MB for this specific query
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("prefix", "STRING", prefix),
        ],
        maximum_bytes_billed=200 * 1024 * 1024,
    )
    try:
        client = bigquery.Client(project=project, location=location)
        job = client.query(query, job_config=job_config)
        rows = list(job.result(timeout=30))
        count = int(rows[0][0]) if rows else 0
        return {"views_count": count, "item_id_prefix": prefix}
    except Exception as e:
        return {"error": str(e)[:300], "views_count": None, "item_id_prefix": prefix}


def load_marketing_performance(
    client_id: int,
    as_of_date: date,
    days: int = 28,
    organization_id: Optional[str] = None,
    workspace_id: Optional[str] = None,
    since_date: Optional[date] = None,
) -> pd.DataFrame:
    """Load marketing_performance_daily for client. If since_date set, only rows with date > since_date (incremental)."""
    client = get_client()
    dataset = get_analytics_dataset()
    project = _project()
    if since_date:
        start = since_date
        end = as_of_date
    else:
        start = as_of_date - timedelta(days=days)
        end = as_of_date
    query = f"""
    SELECT *
    FROM `{project}.{dataset}.marketing_performance_daily`
    WHERE client_id = {client_id}
      AND date >= '{start.isoformat()}'
      AND date <= '{end.isoformat()}'
    """
    return client.query(query).to_dataframe()


def load_ads_staging(
    client_id: int,
    start_date: date,
    end_date: date,
    organization_id: Optional[str] = None,
) -> pd.DataFrame:
    """Load Google Ads data from marketing_performance_daily (channel = google_ads)."""
    client = get_client()
    dataset = get_analytics_dataset()
    project = _project()
    query = f"""
    SELECT client_id, date, campaign_id, ad_group_id, device,
           spend, clicks, impressions, conversions, revenue, sessions
    FROM `{project}.{dataset}.marketing_performance_daily`
    WHERE client_id = {client_id}
      AND date >= '{start_date.isoformat()}'
      AND date <= '{end_date.isoformat()}'
      AND channel = 'google_ads'
    ORDER BY date
    """
    return client.query(query).to_dataframe()


def load_ga4_staging(
    client_id: int,
    start_date: date,
    end_date: date,
    organization_id: Optional[str] = None,
) -> pd.DataFrame:
    """Load GA4 data from marketing_performance_daily (channel = ga4)."""
    client = get_client()
    dataset = get_analytics_dataset()
    project = _project()
    query = f"""
    SELECT client_id, date, campaign_id, ad_group_id, device,
           spend, clicks, impressions, conversions, revenue, sessions
    FROM `{project}.{dataset}.marketing_performance_daily`
    WHERE client_id = {client_id}
      AND date >= '{start_date.isoformat()}'
      AND date <= '{end_date.isoformat()}'
      AND channel = 'ga4'
    ORDER BY date
    """
    return client.query(query).to_dataframe()


def _sanitize_for_json(obj: Any) -> Any:
    """Replace NaN/Inf and non-JSON-serializable values so insert_rows_json succeeds."""
    if obj is None:
        return None
    if isinstance(obj, dict):
        return {k: _sanitize_for_json(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [_sanitize_for_json(x) for x in obj]
    if isinstance(obj, (int, str, bool)):
        return obj
    if isinstance(obj, float):
        return None if (math.isnan(obj) or math.isinf(obj)) else obj
    try:
        f = float(obj)
        return None if (math.isnan(f) or math.isinf(f)) else f
    except (TypeError, ValueError):
        pass
    return obj


def insert_insights(rows: list[dict[str, Any]]) -> None:
    """Insert insight rows into analytics_insights. Caller ensures idempotency (insight_hash)."""
    if not rows:
        return
    client = get_client()
    table_id = f"{_project()}.{get_analytics_dataset()}.analytics_insights"
    sanitized = [_sanitize_for_json(r) for r in rows]
    errors = client.insert_rows_json(table_id, sanitized)
    if errors:
        raise RuntimeError(f"BigQuery insert errors: {errors}")


def list_insights(
    organization_id: str,
    client_id: Optional[int] = None,
    workspace_id: Optional[str] = None,
    status: Optional[str] = None,
    limit: int = 100,
    offset: int = 0,
    min_created_date: Optional[date] = None,
) -> list[dict]:
    """List insights scoped by organization_id; no cross-tenant leakage. Use min_created_date for partition pruning."""
    # Local fallback: serve from JSON file when INSIGHTS_JSON_PATH is set
    json_path = os.environ.get("INSIGHTS_JSON_PATH")
    if json_path and os.path.isfile(json_path):
        try:
            import json
            with open(json_path) as f:
                rows = json.load(f)
            if not isinstance(rows, list):
                rows = [rows]
            out = []
            for r in rows:
                if (r.get("organization_id") or "") != organization_id:
                    continue
                if client_id is not None and r.get("client_id") != client_id:
                    continue
                if workspace_id and (r.get("workspace_id") or "") != workspace_id:
                    continue
                if status and (r.get("status") or "") != status:
                    continue
                if min_created_date and r.get("created_at"):
                    try:
                        from datetime import datetime
                        created = datetime.fromisoformat(r["created_at"].replace("Z", "+00:00")).date()
                        if created < min_created_date:
                            continue
                    except Exception:
                        pass
                out.append(r)
            out.sort(key=lambda x: x.get("created_at") or "", reverse=True)
            return out[offset : offset + limit]
        except Exception:
            pass
    client = get_client()
    project = _project()
    dataset = get_analytics_dataset()

    def esc(s: str) -> str:
        return (s or "").replace("'", "''")
    where = [f"organization_id = '{esc(organization_id)}'"]
    if client_id is not None:
        where.append(f"client_id = {client_id}")
    if workspace_id:
        where.append(f"workspace_id = '{esc(workspace_id)}'")
    if status:
        where.append(f"status = '{esc(status)}'")
    if min_created_date:
        where.append(f"DATE(created_at) >= '{min_created_date.isoformat()}'")
    if not where:
        where.append("1=1")
    q = f"""
    SELECT * FROM `{project}.{dataset}.analytics_insights`
    WHERE {' AND '.join(where)}
    ORDER BY created_at DESC
    LIMIT {limit} OFFSET {offset}
    """
    try:
        df = client.query(q).to_dataframe()
    except Exception as e:
        if _is_table_not_found(e):
            import logging
            logging.getLogger(__name__).debug("analytics_insights table not found; returning empty list")
            return []
        raise
    if df.empty:
        return []
    return [dict(row) for _, row in df.iterrows()]


def get_insight_by_id(insight_id: str, organization_id: Optional[str] = None) -> Optional[dict]:
    json_path = os.environ.get("INSIGHTS_JSON_PATH")
    if json_path and os.path.isfile(json_path):
        try:
            import json
            with open(json_path) as f:
                rows = json.load(f)
            if not isinstance(rows, list):
                rows = [rows]
            for r in rows:
                if r.get("insight_id") == insight_id:
                    if organization_id and (r.get("organization_id") or "") != organization_id:
                        continue
                    return r
            return None
        except Exception:
            pass
    client = get_client()
    project = _project()
    dataset = get_analytics_dataset()

    def esc(s: str) -> str:
        return (s or "").replace("'", "''")
    where = [f"insight_id = '{esc(insight_id)}'"]
    if organization_id:
        where.append(f"organization_id = '{esc(organization_id)}'")
    q = f"SELECT * FROM `{project}.{dataset}.analytics_insights` WHERE {' AND '.join(where)} LIMIT 1"
    try:
        df = client.query(q).to_dataframe()
    except Exception as e:
        if _is_table_not_found(e):
            return None
        try:
            q_fallback = f"SELECT * FROM `{project}.{dataset}.analytics_insights` WHERE insight_id = '{esc(insight_id)}' LIMIT 1"
            df = client.query(q_fallback).to_dataframe()
        except Exception:
            raise e
    if df.empty:
        return None
    return dict(df.iloc[0])


def get_supporting_metrics_snapshot(organization_id: str, client_id: int, insight_id: str) -> Optional[dict]:
    client = get_client()
    project = _project()
    dataset = get_analytics_dataset()
    q = f"""
    SELECT metrics_json FROM `{project}.{dataset}.supporting_metrics_snapshot`
    WHERE organization_id = '{organization_id.replace("'", "''")}' AND client_id = {client_id} AND insight_id = '{insight_id.replace("'", "''")}'
    ORDER BY created_at DESC LIMIT 1
    """
    try:
        df = client.query(q).to_dataframe()
    except Exception:
        return None
    if df.empty:
        return None
    import json
    raw = df.iloc[0].get("metrics_json")
    if not raw:
        return None
    try:
        return json.loads(raw)
    except Exception:
        return None


def get_recent_insight_hashes(
    organization_id: str,
    client_id: str,
    since_days: int = 7,
) -> list[tuple[str, Any, str]]:
    """Return list of (insight_hash, created_at, severity) for repeat/cooldown detection."""
    client = get_client()
    project = _project()
    dataset = get_analytics_dataset()
    esc = (lambda s: (s or "").replace("'", "''"))
    cid = int(client_id) if client_id else 0
    q = f"""
    SELECT insight_hash, created_at, severity
    FROM `{project}.{dataset}.analytics_insights`
    WHERE organization_id = '{esc(organization_id)}' AND client_id = {cid}
      AND created_at >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {since_days} DAY)
    ORDER BY created_at DESC
    """
    try:
        df = client.query(q).to_dataframe()
    except Exception:
        return []
    if df.empty:
        return []
    out = []
    for _, r in df.iterrows():
        h = r.get("insight_hash") or r.get("insight_id")
        if h:
            out.append((str(h), r.get("created_at"), str(r.get("severity") or "medium")))
    return out


def insert_audit_log(
    organization_id: str,
    event_type: str,
    entity_id: Optional[str] = None,
    user_id: Optional[str] = None,
    payload: Optional[str] = None,
) -> None:
    client = get_client()
    project = _project()
    dataset = get_analytics_dataset()
    now = __import__("datetime").datetime.now(__import__("datetime").timezone.utc)
    row = {
        "audit_id": str(uuid.uuid4()),
        "organization_id": organization_id,
        "event_type": event_type,
        "entity_id": entity_id or "",
        "user_id": user_id or "",
        "payload": payload or "{}",
        "created_at": now.isoformat(),
    }
    table_id = f"{_project()}.{get_analytics_dataset()}.audit_log"
    try:
        errors = client.insert_rows_json(table_id, [row])
        if errors:
            raise RuntimeError(f"BigQuery insert errors: {errors}")
    except Exception:
        pass
