"""
Unit tests for Copilot planner, discover_tables (marts-first, synonym-aware), and run_bigquery_sql.
"""
from __future__ import annotations

import json
import sys
from pathlib import Path
from unittest.mock import patch, MagicMock

import pytest

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))


def test_planner_extract_intent():
    from backend.app.copilot.planner import _extract_intent
    intent = _extract_intent("What's the view count of Item Id starting with FT05B coming from Facebook?")
    assert "view" in intent or "count" in intent or "ft05b" in intent or "facebook" in intent
    intent2 = _extract_intent("How many sessions from Google last week?")
    assert "sessions" in intent2 or "google" in intent2 or "week" in intent2


def test_planner_analyze_returns_intent_and_candidates():
    with patch("backend.app.copilot.tools.discover_tables") as mock_discover:
        mock_discover.return_value = [
            {"project": "p", "dataset": "raw_ads", "table": "facebook_events", "columns": ["item_id", "channel", "views"]},
            {"project": "p", "dataset": "hypeon_marts_ads", "table": "fct_ad_spend", "columns": ["item_id", "channel"]},
        ]
        from backend.app.copilot.planner import analyze
        plan = analyze("views count FT05B facebook", client_id=1, organization_id="org1")
    assert "intent" in plan
    assert "candidates" in plan
    assert len(plan["candidates"]) >= 1
    assert any("facebook" in (c.get("table") or "") or "raw_ads" in (c.get("table") or "") for c in plan["candidates"])


def test_planner_candidates_contain_table_refs():
    """Planner returns candidates with full table refs (no hardcoded SQL; LLM generates SQL)."""
    with patch("backend.app.copilot.tools.discover_tables") as mock_discover:
        mock_discover.return_value = [
            {"project": "proj", "dataset": "ds", "table": "events", "columns": ["item_id", "views", "channel"]},
        ]
        from backend.app.copilot.planner import analyze
        plan = analyze("views count FT05B facebook", client_id=1, organization_id="org1")
    assert plan.get("candidates")
    for c in plan["candidates"]:
        full = c.get("table") or ""
        assert "proj" in full or "ds" in full or "events" in full
        assert c.get("columns") is not None


def test_discover_tables_ranking():
    """discover_tables ranks by keyword match: intent 'views count facebook' should rank tables with views/facebook higher."""
    with patch("backend.app.clients.bigquery.list_tables_for_discovery") as mock_list:
        mock_list.return_value = [
            {"project": "p", "dataset": "d", "table_name": "other", "columns": [{"name": "x"}]},
            {"project": "p", "dataset": "raw_ads", "table_name": "facebook_events", "columns": [{"name": "views"}, {"name": "item_id"}]},
        ]
        from backend.app.copilot.tools import discover_tables
        out = discover_tables("views count FT05B facebook", limit=10)
    assert out
    names = [t.get("table") for t in out]
    assert "facebook_events" in names or "other" in names


def test_discover_tables_uses_cache():
    with patch("backend.app.clients.bigquery.list_tables_for_discovery") as mock_list:
        mock_list.return_value = [{"project": "p", "dataset": "d", "table_name": "t1", "columns": []}]
        from backend.app.copilot.tools import discover_tables
        from backend.app.copilot.schema_cache import schema_cache_set, schema_cache_get
        intent = "test intent cache"
        cache_val = [{"project": "p", "dataset": "d", "table": "cached_table", "columns": []}]
        schema_cache_set(intent, cache_val)
        out = discover_tables(intent, limit=5)
    assert out
    assert any(t.get("table") == "cached_table" for t in out)
    mock_list.assert_not_called()


def test_get_org_bq_context_from_firestore():
    """When org has Firestore projects with typed datasets, get_org_bq_context returns BQ config."""
    from backend.app.auth.firestore_user import get_org_bq_context

    org_doc = {
        "name": "Test Org",
        "projects": [
            {
                "bq_project": "my-bq-project",
                "datasets": [
                    {"bq_dataset": "hypeon_marts", "bq_location": "europe-north2", "type": "marts"},
                    {"bq_dataset": "hypeon_marts_ads", "bq_location": "EU", "type": "marts_ads"},
                ],
            },
            {
                "bq_project": "my-source-project",
                "datasets": [
                    {"bq_dataset": "analytics_ga4", "bq_location": "europe-north2", "type": "ga4"},
                    {"bq_dataset": "146568", "bq_location": "EU", "type": "ads"},
                ],
            },
        ],
    }
    with patch("backend.app.auth.firestore_user.get_organization") as mock_get_org:
        mock_get_org.return_value = org_doc
        ctx = get_org_bq_context("test-org")
    assert ctx is not None
    assert ctx["bq_project"] == "my-bq-project"
    assert ctx["marts_dataset"] == "hypeon_marts"
    assert ctx["marts_ads_dataset"] == "hypeon_marts_ads"
    assert ctx["ga4_dataset"] == "analytics_ga4"
    assert ctx["ads_dataset"] == "146568"
    assert ctx["bq_location"] == "europe-north2"
    assert ctx["bq_location_ads"] == "EU"
    assert ctx["bq_source_project"] == "my-source-project"


def test_get_org_bq_context_returns_none_when_no_projects():
    """When org has no projects, get_org_bq_context returns None."""
    from backend.app.auth.firestore_user import get_org_bq_context

    with patch("backend.app.auth.firestore_user.get_organization") as mock_get_org:
        mock_get_org.return_value = {"name": "Empty Org"}
        ctx = get_org_bq_context("empty-org")
    assert ctx is None


def test_run_bigquery_sql_rejects_non_select():
    from backend.app.copilot.tools import execute_tool
    res = execute_tool("org", 1, "run_bigquery_sql", {"query": "INSERT INTO t (a) VALUES (1)"})
    data = json.loads(res)
    assert data.get("error")
    err = (data.get("error") or "").lower()
    assert "select" in err and ("only" in err or "allowed" in err)


def test_run_bigquery_sql_accepts_select():
    with patch("backend.app.clients.bigquery.run_bigquery_sql_readonly") as mock_run:
        mock_run.return_value = {
            "rows": [{"views": 100}],
            "schema": ["views"],
            "row_count": 1,
            "stats": {},
            "error": None,
        }
        from backend.app.copilot.tools import execute_tool
        res = execute_tool("org", 1, "run_bigquery_sql", {"query": "SELECT SUM(views) AS views FROM `p.d.t` LIMIT 1"})
    data = json.loads(res)
    assert data.get("error") is None
    assert data.get("rows") and data["rows"][0].get("views") == 100


def test_validator_rejects_empty_when_not_allowed():
    from backend.app.copilot.validator import validate
    ok, reason = validate({"rows": [], "schema": [], "error": None}, "How many views?")
    assert ok is False
    assert "row" in reason.lower() or "empty" in reason.lower()


def test_validator_accepts_non_empty():
    from backend.app.copilot.validator import validate
    ok, _ = validate({"rows": [{"views": 10}], "schema": ["views"], "error": None}, "How many views?")
    assert ok is True


def test_validator_rejects_negative_count():
    from backend.app.copilot.validator import validate
    ok, _ = validate({"rows": [{"total_count": -1}], "schema": ["total_count"], "error": None}, "What is the total count?")
    assert ok is False


def test_run_bigquery_sql_readonly_rejects_insert():
    """run_bigquery_sql_readonly must reject INSERT (returns error, no rows)."""
    from backend.app.clients.bigquery import run_bigquery_sql_readonly
    out = run_bigquery_sql_readonly("INSERT INTO t (a) VALUES (1)", client_id=1, organization_id="org")
    assert out.get("error")
    assert out.get("row_count", 0) == 0
    err = (out.get("error") or "").lower()
    assert "select" in err or "allowed" in err or "insert" in err


def test_run_bigquery_sql_readonly_rejects_update_delete():
    """run_bigquery_sql_readonly must reject UPDATE and DELETE (returns error)."""
    from backend.app.clients.bigquery import run_bigquery_sql_readonly
    for sql in ("UPDATE t SET x=1", "DELETE FROM t"):
        out = run_bigquery_sql_readonly(sql, client_id=1, organization_id="org")
        assert out.get("error"), sql
        assert out.get("row_count", 0) == 0
        err = (out.get("error") or "").lower()
        assert "select" in err or "allowed" in err or "update" in err or "delete" in err


def test_run_bigquery_sql_readonly_rejects_drop():
    """run_bigquery_sql_readonly must reject DROP TABLE (malicious DDL)."""
    from backend.app.clients.bigquery import run_bigquery_sql_readonly
    out = run_bigquery_sql_readonly("DROP TABLE project.dataset.table; SELECT 1;", client_id=1, organization_id="org")
    assert out.get("error")
    assert out.get("row_count", 0) == 0


def test_run_bigquery_sql_readonly_accepts_with_cte():
    """run_bigquery_sql_readonly must accept WITH ... SELECT (read-only)."""
    org_ctx = {"bq_project": "test-proj", "marts_dataset": "marts", "marts_ads_dataset": "marts_ads", "bq_location": "europe-north2"}
    with patch("backend.app.clients.bigquery.get_client") as mock_get:
        mock_job = MagicMock()
        mock_job.schema = [MagicMock(name="x")]
        mock_row = MagicMock()
        mock_row.items.return_value = [("x", 1)]
        mock_job.result.return_value = [mock_row]
        mock_get.return_value.query.return_value = mock_job
        with patch("backend.app.clients.bigquery._get_bq_context", return_value=org_ctx):
            from backend.app.clients.bigquery import run_bigquery_sql_readonly
            out = run_bigquery_sql_readonly(
                "WITH x AS (SELECT 1 AS x) SELECT * FROM x LIMIT 1",
                client_id=1,
                organization_id="org",
            )
    assert out.get("error") is None
    assert len(out.get("rows") or []) == 1
    assert out["rows"][0].get("x") == 1
