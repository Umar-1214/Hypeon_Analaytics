"""
Tests for Copilot run_sql, run_sql_raw, knowledge base, and chat_handler.
Covers run_readonly_query, run_readonly_query_raw, execute_tool, get_schema_for_copilot,
get_raw_schema_for_copilot, and _build_system_template.
"""
from __future__ import annotations

import json
import math
import sys
from datetime import date, datetime, timezone
from pathlib import Path
from unittest.mock import patch, MagicMock

import pytest

# Repo root
ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))


# ----- run_readonly_query (bigquery.py) -----


@pytest.fixture
def env_bq():
    """BQ_PROJECT, hypeon_marts, hypeon_marts_ads only. No raw/staging for Copilot."""
    with patch.dict(
        "os.environ",
        {
            "BQ_PROJECT": "test-proj",
            "MARTS_DATASET": "hypeon_marts",
            "MARTS_ADS_DATASET": "hypeon_marts_ads",
        },
        clear=False,
    ):
        yield


@pytest.fixture
def env_marts_and_allowed_tables(env_bq):
    """Copilot marts-only: mock _copilot_allowed_tables so marts tables pass validation."""
    with patch("backend.app.clients.bigquery._copilot_allowed_tables") as mock_tables:
        mock_tables.return_value = frozenset({
            ("hypeon_marts", "fct_sessions"),
            ("hypeon_marts", "stg_ga4__events"),
            ("hypeon_marts_ads", "fct_ad_spend"),
            ("hypeon_marts_ads", "stg_google_ads__performance"),
        })
        yield


def test_run_readonly_query_empty_sql(env_bq):
    from backend.app.clients.bigquery import run_readonly_query
    out = run_readonly_query("", client_id=1, organization_id="default")
    assert out["rows"] == []
    assert out["error"] == "Empty query."


def test_run_readonly_query_whitespace_only(env_bq):
    from backend.app.clients.bigquery import run_readonly_query
    out = run_readonly_query("   \n\t  ", client_id=1, organization_id="default")
    assert out["rows"] == []
    assert "Empty" in (out["error"] or "")


def test_run_readonly_query_multi_statement_rejected(env_bq):
    from backend.app.clients.bigquery import run_readonly_query
    sql = "SELECT 1; SELECT 2"
    out = run_readonly_query(sql, client_id=1, organization_id="default")
    assert out["rows"] == []
    assert "single" in (out["error"] or "").lower() and "select" in (out["error"] or "").lower()


def test_run_readonly_query_trailing_semicolon_allowed(env_bq):
    from backend.app.clients.bigquery import run_readonly_query
    with patch("backend.app.clients.bigquery._copilot_allowed_tables") as m:
        m.return_value = frozenset({("hypeon_marts", "fct_sessions")})
        sql = "SELECT 1 AS x;"
        with patch("backend.app.clients.bigquery.get_client") as mock_get:
            mock_job = MagicMock()
            mock_job.result.return_value = [MagicMock(items=lambda: [("x", 1)])]
            mock_get.return_value.query.return_value = mock_job
            out = run_readonly_query(sql, client_id=1, organization_id="default")
        assert out["error"] is None
        assert len(out["rows"]) == 1


def test_run_readonly_query_insert_rejected(env_bq):
    from backend.app.clients.bigquery import run_readonly_query
    sql = "INSERT INTO `test-proj.analytics.foo` (a) VALUES (1)"
    out = run_readonly_query(sql, client_id=1, organization_id="default")
    assert out["rows"] == []
    err = (out["error"] or "").lower()
    assert "insert" in err or "read-only" in err or "only select" in err


def test_run_readonly_query_update_rejected(env_bq):
    from backend.app.clients.bigquery import run_readonly_query
    sql = "UPDATE `test-proj.analytics.marketing_performance_daily` SET spend=0"
    out = run_readonly_query(sql, client_id=1, organization_id="default")
    assert out["rows"] == []
    err = (out["error"] or "").lower()
    assert "update" in err or "read-only" in err or "only select" in err


def test_run_readonly_query_drop_rejected(env_bq):
    from backend.app.clients.bigquery import run_readonly_query
    sql = "DROP TABLE `test-proj.analytics.marketing_performance_daily`"
    out = run_readonly_query(sql, client_id=1, organization_id="default")
    assert out["rows"] == []
    err = (out["error"] or "").lower()
    assert "drop" in err or "read-only" in err or "only select" in err


def test_run_readonly_query_disallowed_dataset_rejected(env_bq):
    """Dataset not in hypeon_marts/hypeon_marts_ads is rejected. No raw/staging."""
    from backend.app.clients.bigquery import run_readonly_query
    with patch("backend.app.clients.bigquery._copilot_allowed_tables") as m:
        m.return_value = frozenset({("hypeon_marts", "fct_sessions")})
        sql = "SELECT * FROM `test-proj.other_dataset.some_table` LIMIT 1"
        out = run_readonly_query(sql, client_id=1, organization_id="default")
    assert out["rows"] == []
    assert "not allowed" in (out["error"] or "").lower() or "hypeon_marts" in (out["error"] or "")


def test_run_readonly_query_analytics_and_staging_rejected(env_bq):
    """analytics, ads_daily_staging, ga4_daily_staging are never allowed. Marts only."""
    from backend.app.clients.bigquery import run_readonly_query
    with patch("backend.app.clients.bigquery._copilot_allowed_tables") as m:
        m.return_value = frozenset({("hypeon_marts", "fct_sessions")})
        for bad in ("analytics.marketing_performance_daily", "analytics.ads_daily_staging", "analytics.ga4_daily_staging"):
            sql = f"SELECT * FROM `test-proj.{bad}` LIMIT 1"
            out = run_readonly_query(sql, client_id=1, organization_id="default")
            assert out["rows"] == [], bad
            assert "not allowed" in (out["error"] or "").lower() or "hypeon_marts" in (out["error"] or ""), bad


def test_run_readonly_query_wrong_project_or_dataset_rejected(env_bq):
    from backend.app.clients.bigquery import run_readonly_query
    with patch("backend.app.clients.bigquery._copilot_allowed_tables") as m:
        m.return_value = frozenset({("hypeon_marts", "fct_sessions")})
        sql = "SELECT * FROM `other-project.hypeon_marts.fct_sessions` LIMIT 1"
        out = run_readonly_query(sql, client_id=1, organization_id="default")
    assert out["rows"] == []
    assert "Only tables" in (out["error"] or "") or "project" in (out["error"] or "").lower()


def test_run_readonly_query_marts_table_passes_validation(env_marts_and_allowed_tables):
    """Table in hypeon_marts.fct_sessions passes when in allowed_tables."""
    from backend.app.clients.bigquery import run_readonly_query
    sql = "SELECT COUNT(*) AS n FROM `test-proj.hypeon_marts.fct_sessions` WHERE event_name = 'view_item' LIMIT 1"
    with patch("backend.app.clients.bigquery.get_client") as mock_get:
        mock_job = MagicMock()
        mock_job.result.return_value = [MagicMock(items=lambda: [("n", 14200)])]
        mock_get.return_value.query.return_value = mock_job
        out = run_readonly_query(sql, client_id=1, organization_id="default")
    assert out["error"] is None
    assert len(out["rows"]) == 1
    assert out["rows"][0]["n"] == 14200


def test_run_readonly_query_hypeon_marts_allowed(env_marts_and_allowed_tables):
    """hypeon_marts.fct_sessions is allowed when in _copilot_allowed_tables."""
    from backend.app.clients.bigquery import run_readonly_query
    sql = "SELECT COUNT(*) AS n FROM `test-proj.hypeon_marts.fct_sessions` WHERE event_name = 'view_item' LIMIT 1"
    with patch("backend.app.clients.bigquery.get_client") as mock_get:
        mock_job = MagicMock()
        mock_job.result.return_value = [MagicMock(items=lambda: [("n", 14)])]
        mock_get.return_value.query.return_value = mock_job
        out = run_readonly_query(sql, client_id=1, organization_id="default")
    assert out["error"] is None
    assert len(out["rows"]) == 1


def test_run_readonly_query_with_cte_passes_validation(env_bq):
    from backend.app.clients.bigquery import run_readonly_query
    with patch("backend.app.clients.bigquery._copilot_allowed_tables") as m:
        m.return_value = frozenset({("hypeon_marts", "fct_sessions")})
        sql = "WITH t AS (SELECT 1 AS x) SELECT * FROM t LIMIT 1"
        with patch("backend.app.clients.bigquery.get_client") as mock_get:
            mock_job = MagicMock()
            mock_job.result.return_value = [MagicMock(items=lambda: [("x", 1)])]
            mock_get.return_value.query.return_value = mock_job
            out = run_readonly_query(sql, client_id=1, organization_id="default")
        assert out["error"] is None
        assert len(out["rows"]) == 1


def test_run_readonly_query_adds_limit_when_missing(env_marts_and_allowed_tables):
    from backend.app.clients.bigquery import run_readonly_query
    sql = "SELECT * FROM `test-proj.hypeon_marts.fct_sessions` WHERE event_name = 'view_item'"
    with patch("backend.app.clients.bigquery.get_client") as mock_get:
        mock_job = MagicMock()
        mock_job.result.return_value = []
        mock_get.return_value.query.return_value = mock_job
        run_readonly_query(sql, client_id=1, organization_id="default", max_rows=99)
    call_args = mock_get.return_value.query.call_args
    assert call_args is not None
    assert "LIMIT 99" in (call_args[0][0] if call_args[0] else "")


def test_run_readonly_query_bq_exception_returns_error(env_marts_and_allowed_tables):
    from backend.app.clients.bigquery import run_readonly_query
    sql = "SELECT * FROM `test-proj.hypeon_marts.fct_sessions` LIMIT 1"
    with patch("backend.app.clients.bigquery.get_client") as mock_get:
        mock_get.return_value.query.side_effect = Exception("Table not found: 404")
        out = run_readonly_query(sql, client_id=1, organization_id="default")
    assert out["rows"] == []
    assert "error" in out and out["error"]
    assert "404" in out["error"] or "not found" in out["error"].lower()


def test_run_readonly_query_fct_ad_spend_allowed(env_marts_and_allowed_tables):
    """hypeon_marts_ads.fct_ad_spend passes when in allowed_tables."""
    from backend.app.clients.bigquery import run_readonly_query
    sql = "SELECT DISTINCT channel FROM `test-proj.hypeon_marts_ads.fct_ad_spend` LIMIT 10"
    with patch("backend.app.clients.bigquery.get_client") as mock_get:
        mock_job = MagicMock()
        mock_job.result.return_value = [MagicMock(items=lambda: [("channel", "google_ads")])]
        mock_get.return_value.query.return_value = mock_job
        out = run_readonly_query(sql, client_id=1, organization_id="default")
    assert out["error"] is None
    assert len(out["rows"]) == 1


def test_run_readonly_query_raw_ga4_events_rejected(env_bq):
    """Raw GA4 events_* is rejected. Marts only."""
    from backend.app.clients.bigquery import run_readonly_query
    with patch("backend.app.clients.bigquery._copilot_allowed_tables") as m:
        m.return_value = frozenset({("hypeon_marts", "fct_sessions")})
        sql = "SELECT event_name FROM `test-proj.analytics_444259275.events_*` WHERE event_date = '20240201' LIMIT 1"
        out = run_readonly_query(sql, client_id=1, organization_id="default")
    assert out["rows"] == []
    assert "not allowed" in (out["error"] or "").lower()


def test_run_readonly_query_wrong_project_rejected(env_bq):
    """Table in wrong project is rejected. Marts only in BQ_PROJECT."""
    from backend.app.clients.bigquery import run_readonly_query
    with patch("backend.app.clients.bigquery._copilot_allowed_tables") as m:
        m.return_value = frozenset({("hypeon_marts", "fct_sessions")})
        sql = "SELECT * FROM `other-project.hypeon_marts.fct_sessions` LIMIT 1"
        out = run_readonly_query(sql, client_id=1, organization_id="default")
    assert out["rows"] == []
    assert "Only tables" in (out["error"] or "") or "project" in (out["error"] or "").lower()


# ----- run_readonly_query_raw (bigquery.py) -----


@pytest.fixture
def env_raw():
    """BQ_SOURCE_PROJECT, GA4_DATASET, ADS_DATASET for run_sql_raw allowlist."""
    with patch.dict(
        "os.environ",
        {
            "BQ_PROJECT": "test-proj",
            "BQ_SOURCE_PROJECT": "test-proj",
            "GA4_DATASET": "ga4_dataset",
            "ADS_DATASET": "146568",
        },
        clear=False,
    ):
        yield


def test_run_readonly_query_raw_empty_sql(env_raw):
    from backend.app.clients.bigquery import run_readonly_query_raw
    out = run_readonly_query_raw("", client_id=1, organization_id="default")
    assert out["rows"] == []
    assert "Empty" in (out["error"] or "")


def test_run_readonly_query_raw_insert_rejected(env_raw):
    from backend.app.clients.bigquery import run_readonly_query_raw
    sql = "INSERT INTO `test-proj.ga4_dataset.events_20250101` (a) VALUES (1)"
    out = run_readonly_query_raw(sql, client_id=1, organization_id="default")
    assert out["rows"] == []
    assert "SELECT" in (out["error"] or "") or "read-only" in (out["error"] or "").lower()


def test_run_readonly_query_raw_marts_table_rejected(env_raw):
    """Marts tables are not allowed in run_readonly_query_raw."""
    from backend.app.clients.bigquery import run_readonly_query_raw
    sql = "SELECT * FROM `test-proj.hypeon_marts.fct_sessions` LIMIT 1"
    out = run_readonly_query_raw(sql, client_id=1, organization_id="default")
    assert out["rows"] == []
    assert "not allowed" in (out["error"] or "").lower() or "raw" in (out["error"] or "").lower()


def test_run_readonly_query_raw_ga4_events_allowed(env_raw):
    """GA4 events_* table in allowed dataset passes validation."""
    from backend.app.clients.bigquery import run_readonly_query_raw
    sql = "SELECT event_date, event_name FROM `test-proj.ga4_dataset.events_20250101` WHERE event_date >= '2025-01-01' LIMIT 5"
    with patch("google.cloud.bigquery.Client") as MockClient:
        mock_job = MagicMock()
        mock_job.result.return_value = [
            MagicMock(items=lambda: [("event_date", date(2025, 1, 1)), ("event_name", "page_view")]),
        ]
        MockClient.return_value.query.return_value = mock_job
        out = run_readonly_query_raw(sql, client_id=1, organization_id="default")
    assert out["error"] is None
    assert len(out["rows"]) == 1


def test_run_readonly_query_raw_ads_table_allowed(env_raw):
    """Ads ads_AccountBasicStats_* table in allowed dataset passes validation."""
    from backend.app.clients.bigquery import run_readonly_query_raw
    sql = "SELECT * FROM `test-proj.146568.ads_accountbasicstats_4221201460` LIMIT 2"
    with patch("google.cloud.bigquery.Client") as MockClient:
        mock_job = MagicMock()
        mock_job.result.return_value = [MagicMock(items=lambda: [("segments_date", date(2025, 1, 1))])]
        MockClient.return_value.query.return_value = mock_job
        out = run_readonly_query_raw(sql, client_id=1, organization_id="default")
    assert out["error"] is None
    assert len(out["rows"]) >= 0


def test_run_readonly_query_raw_events_intraday_rejected(env_raw):
    """events_intraday_* is not in allowlist."""
    from backend.app.clients.bigquery import run_readonly_query_raw
    sql = "SELECT * FROM `test-proj.ga4_dataset.events_intraday_20250101` LIMIT 1"
    out = run_readonly_query_raw(sql, client_id=1, organization_id="default")
    assert out["rows"] == []
    assert "not in raw allowlist" in (out["error"] or "").lower() or "not allowed" in (out["error"] or "").lower()


def test_run_readonly_query_raw_adds_limit(env_raw):
    """run_readonly_query_raw adds LIMIT when missing."""
    from backend.app.clients.bigquery import run_readonly_query_raw
    sql = "SELECT 1 AS x FROM `test-proj.ga4_dataset.events_20250101`"
    with patch("google.cloud.bigquery.Client") as MockClient:
        mock_job = MagicMock()
        mock_job.result.return_value = [MagicMock(items=lambda: [("x", 1)])]
        MockClient.return_value.query.return_value = mock_job
        out = run_readonly_query_raw(sql, client_id=1, organization_id="default", max_rows=10)
    assert out["error"] is None
    call_sql = MockClient.return_value.query.call_args[0][0]
    assert "LIMIT 10" in call_sql


# ----- execute_tool (single-flow: discover_tables + run_bigquery_sql only; run_sql/run_sql_raw removed) -----


@pytest.mark.skip(reason="run_sql removed; Copilot uses run_bigquery_sql only")
def test_execute_tool_run_sql_empty_query():
    from backend.app.copilot.tools import execute_tool
    result = execute_tool("org", 1, "run_sql", {"query": ""})
    data = json.loads(result)
    assert data["rows"] == []
    assert "Missing" in (data.get("error") or "")


@pytest.mark.skip(reason="run_sql removed; Copilot uses run_bigquery_sql only")
def test_execute_tool_run_sql_missing_query_key():
    from backend.app.copilot.tools import execute_tool
    result = execute_tool("org", 1, "run_sql", {})
    data = json.loads(result)
    assert data["rows"] == []
    assert data.get("error") == "Missing query."


@pytest.mark.skip(reason="run_sql removed; Copilot uses run_bigquery_sql only")
def test_execute_tool_run_sql_delegates_to_run_readonly_query(env_bq):
    from backend.app.copilot.tools import execute_tool
    sql = "SELECT * FROM `test-proj.146568.ads_AccountBasicStats_4221201460` LIMIT 1"
    with patch("backend.app.clients.bigquery.run_readonly_query") as mock_run:
        mock_run.return_value = {"rows": [{"spend": 10.5, "revenue": 100}], "error": None}
        result = execute_tool("org", 1, "run_sql", {"query": sql})
    mock_run.assert_called_once()
    assert mock_run.call_args[1]["client_id"] == 1
    assert mock_run.call_args[1]["organization_id"] == "org"
    data = json.loads(result)
    assert data["row_count"] == 1
    assert data["rows"][0]["spend"] == 10.5
    assert data["error"] is None


@pytest.mark.skip(reason="run_sql removed; Copilot uses run_bigquery_sql only")
def test_execute_tool_run_sql_serializes_date_and_nan(env_bq):
    from backend.app.copilot.tools import execute_tool
    with patch("backend.app.clients.bigquery.run_readonly_query") as mock_run:
        mock_run.return_value = {
            "rows": [
                {"date": date(2025, 2, 27), "value": math.nan, "normal": 42},
            ],
            "error": None,
        }
        result = execute_tool("org", 1, "run_sql", {"query": "SELECT 1"})
    data = json.loads(result)
    assert data["rows"][0]["date"] == "2025-02-27"
    assert data["rows"][0]["value"] is None
    assert data["rows"][0]["normal"] == 42


@pytest.mark.skip(reason="run_sql removed; Copilot uses run_bigquery_sql only")
def test_execute_tool_run_sql_propagates_error():
    from backend.app.copilot.tools import execute_tool
    with patch("backend.app.clients.bigquery.run_readonly_query") as mock_run:
        mock_run.return_value = {"rows": [], "error": "Only SELECT is allowed."}
        result = execute_tool("org", 1, "run_sql", {"query": "DELETE FROM x"})
    data = json.loads(result)
    assert data["rows"] == []
    assert "Only SELECT" in (data.get("error") or "")


def test_execute_tool_unknown_tool_returns_error():
    """Unknown tool names (e.g. get_business_overview, run_sql) return error."""
    from backend.app.copilot.tools import execute_tool
    result = execute_tool("org", 1, "get_business_overview", {})
    data = json.loads(result)
    assert "Unknown tool" in (data.get("error") or "")


@pytest.mark.skip(reason="run_sql_raw removed; Copilot uses run_bigquery_sql only")
def test_execute_tool_run_sql_raw_empty_query():
    from backend.app.copilot.tools import execute_tool
    result = execute_tool("org", 1, "run_sql_raw", {"query": ""})
    data = json.loads(result)
    assert data["rows"] == []
    assert "Missing" in (data.get("error") or "")


@pytest.mark.skip(reason="run_sql_raw removed; Copilot uses run_bigquery_sql only")
def test_execute_tool_run_sql_raw_delegates_to_run_readonly_query_raw(env_raw):
    from backend.app.copilot.tools import execute_tool
    sql = "SELECT * FROM `test-proj.ga4_dataset.events_20250101` LIMIT 1"
    with patch("backend.app.clients.bigquery.run_readonly_query_raw") as mock_run:
        mock_run.return_value = {"rows": [{"event_name": "page_view"}], "error": None}
        result = execute_tool("org", 1, "run_sql_raw", {"query": sql})
    mock_run.assert_called_once()
    assert mock_run.call_args[1]["client_id"] == 1
    assert mock_run.call_args[1]["organization_id"] == "org"
    data = json.loads(result)
    assert data["row_count"] == 1
    assert data["rows"][0]["event_name"] == "page_view"
    assert data["error"] is None


# ----- knowledge_base -----


def test_knowledge_base_schema_contains_project_and_marts():
    """Schema is built from marts only; contains project and marts dataset names."""
    with patch.dict(
        "os.environ",
        {"BQ_PROJECT": "my-proj", "MARTS_DATASET": "hypeon_marts", "MARTS_ADS_DATASET": "hypeon_marts_ads"},
        clear=False,
    ):
        with patch("backend.app.clients.bigquery.get_marts_schema_live") as mock_live:
            mock_live.return_value = [
                {"dataset": "hypeon_marts", "table_name": "fct_sessions", "column_name": "event_name"},
                {"dataset": "hypeon_marts_ads", "table_name": "fct_ad_spend", "column_name": "channel"},
            ]
            from backend.app.copilot.knowledge_base import get_schema_for_copilot
            schema = get_schema_for_copilot(use_cache=False)
    assert "my-proj" in schema
    assert "hypeon_marts" in schema
    assert "fct_sessions" in schema or "fct_ad_spend" in schema


def test_knowledge_base_schema_contains_datasets_and_guidance():
    with patch("backend.app.clients.bigquery.get_marts_schema_live") as mock_live:
        mock_live.return_value = [{"dataset": "hypeon_marts", "table_name": "fct_sessions", "column_name": "event_name"}]
        from backend.app.copilot.knowledge_base import get_schema_for_copilot
        schema = get_schema_for_copilot(use_cache=False)
    assert "Query" in schema or "SELECT" in schema or "fct_sessions" in schema
    assert "hypeon_marts" in schema or "fct_sessions" in schema


def test_knowledge_base_schema_read_only_guidance():
    with patch("backend.app.clients.bigquery.get_marts_schema_live") as mock_live:
        mock_live.return_value = [{"dataset": "hypeon_marts", "table_name": "fct_sessions", "column_name": "event_name"}]
        from backend.app.copilot.knowledge_base import get_schema_for_copilot
        schema = get_schema_for_copilot(use_cache=False)
    assert "SELECT" in schema
    assert "read-only" in schema.lower() or "INSERT" in schema or "DROP" in schema


def test_knowledge_base_schema_contains_marts_rules():
    """Schema includes marts-only rules (view_item, item_id, channel)."""
    with patch("backend.app.clients.bigquery.get_marts_schema_live") as mock_live:
        mock_live.return_value = [{"dataset": "hypeon_marts", "table_name": "fct_sessions", "column_name": "event_name"}]
        from backend.app.copilot.knowledge_base import get_schema_for_copilot
        schema = get_schema_for_copilot(use_cache=False)
    assert "view_item" in schema
    assert "item_id" in schema or "fct_sessions" in schema
    assert "channel" in schema or "utm_source" in schema


def test_knowledge_base_no_fallback_when_marts_schema_fails():
    """When marts schema fetch fails, return error message. No discovery fallback."""
    from backend.app.copilot import knowledge_base
    with patch("backend.app.clients.bigquery.get_marts_schema_live", return_value=None):
        schema = knowledge_base.get_schema_for_copilot(use_cache=False)
    assert "unavailable" in schema.lower() or "could not load" in schema.lower() or "marts" in schema.lower()
    assert "do not" in schema.lower() or "staging" not in schema.lower()


def test_knowledge_base_no_static_table_names():
    """Success path: live schema must not list staging/cache as allowed tables."""
    from backend.app.copilot import knowledge_base
    with patch("backend.app.clients.bigquery.get_marts_schema_live") as mock_live:
        mock_live.return_value = [{"dataset": "hypeon_marts", "table_name": "fct_sessions", "column_name": "event_name"}]
        schema = knowledge_base.get_schema_for_copilot(use_cache=False)
    # Schema is built from marts only; must not list staging/cache as queryable tables
    assert "fct_sessions" in schema
    assert "ads_daily_staging" not in schema or "Do NOT" in schema
    assert "ga4_daily_staging" not in schema or "Do NOT" in schema


# ----- get_marts_catalog_for_copilot (knowledge_base.py) -----


def test_get_marts_catalog_for_copilot_missing_file_returns_empty():
    """When copilot_marts_catalog.json is missing, return empty string."""
    from backend.app.copilot.knowledge_base import get_marts_catalog_for_copilot
    with patch("backend.app.copilot.knowledge_base._marts_catalog_path") as mock_path:
        mock_path.return_value = Path(ROOT) / "nonexistent_marts_catalog_12345.json"
        result = get_marts_catalog_for_copilot()
    assert result == ""


def test_get_marts_catalog_for_copilot_valid_file_contains_tables_and_hints():
    """When copilot_marts_catalog.json exists, return string with Data catalog, tables, hints."""
    import tempfile
    from backend.app.copilot.knowledge_base import get_marts_catalog_for_copilot
    catalog_data = {
        "project": "proj",
        "hints": "fct_sessions: event_name, item_id, utm_source; add date filter.",
        "datasets": {
            "hypeon_marts": {
                "tables": {
                    "fct_sessions": {
                        "schema": [{"name": "event_name", "type": "STRING"}, {"name": "item_id", "type": "STRING"}, {"name": "utm_source", "type": "STRING"}],
                        "sample_rows": [{"event_name": "view_item", "item_id": "FT05B1", "utm_source": "google"}],
                    },
                },
            },
        },
    }
    with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
        json.dump(catalog_data, f)
        path = Path(f.name)
    try:
        with patch("backend.app.copilot.knowledge_base._marts_catalog_path", return_value=path):
            result = get_marts_catalog_for_copilot()
        assert "Data catalog" in result or "catalog" in result.lower()
        assert "fct_sessions" in result
        assert "event_name" in result or "item_id" in result or "utm_source" in result
        assert "date filter" in result or "hints" in result.lower()
    finally:
        path.unlink(missing_ok=True)


# ----- get_raw_schema_for_copilot (knowledge_base.py) -----


def test_get_raw_schema_for_copilot_missing_file_returns_use_marts_only():
    """When raw_copilot_schema.json is missing, return short message to use marts only."""
    from backend.app.copilot.knowledge_base import get_raw_schema_for_copilot
    with patch("backend.app.copilot.knowledge_base._raw_schema_path") as mock_path:
        mock_path.return_value = Path(ROOT) / "nonexistent_raw_schema_12345.json"
        result = get_raw_schema_for_copilot()
    assert "marts only" in result.lower() or "not available" in result.lower()


def test_get_raw_schema_for_copilot_valid_file_contains_tables_and_schema():
    """When raw_copilot_schema.json exists, return string with dataset/table and schema info."""
    import tempfile
    from backend.app.copilot.knowledge_base import get_raw_schema_for_copilot
    raw_data = {
        "project": "proj",
        "hints": "Use UNNEST for event_params.",
        "datasets": {
            "ga4_ds": {
                "tables": {
                    "events_20250101": {
                        "schema": [{"name": "event_date", "type": "DATE"}, {"name": "event_name", "type": "STRING"}],
                        "sample_rows": [{"event_date": "2025-01-01", "event_name": "page_view"}],
                    },
                },
            },
        },
    }
    with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
        json.dump(raw_data, f)
        path = Path(f.name)
    try:
        with patch("backend.app.copilot.knowledge_base._raw_schema_path", return_value=path):
            result = get_raw_schema_for_copilot()
        assert "run_sql_raw" in result or "Fallback" in result
        assert "ga4_ds" in result or "events_20250101" in result
        assert "event_date" in result or "event_name" in result
    finally:
        path.unlink(missing_ok=True)


# ----- chat_handler: single-flow Copilot (planner + run_bigquery_sql). V1 _build_system_template removed. -----
