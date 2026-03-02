"""
Integration test: Copilot V2 fallback when first plan returns 0 rows, second returns data.
"""
from __future__ import annotations

import os
import sys
from pathlib import Path
from unittest.mock import patch, MagicMock

import pytest

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))


@pytest.fixture
def env_copilot_v2():
    with patch.dict(os.environ, {"COPILOT_V2": "true"}, clear=False):
        yield


def test_fallback_to_second_plan_when_first_returns_empty(env_copilot_v2):
    """When first SQL returns 0 rows and second returns rows, handler should return answer with data from second."""
    call_count = 0

    def mock_run_bigquery_sql(sql, organization_id, client_id, **kwargs):
        nonlocal call_count
        call_count += 1
        if call_count == 1:
            return {"rows": [], "schema": [], "row_count": 0, "stats": {}, "error": None}
        return {"rows": [{"views": 100}], "schema": ["views"], "row_count": 1, "stats": {}, "error": None}

    with patch("backend.app.copilot.chat_handler.run_bigquery_sql", side_effect=mock_run_bigquery_sql):
        with patch("backend.app.copilot.planner.analyze") as mock_analyze:
            mock_analyze.return_value = {
                "intent": "views count FT05B facebook",
                "candidates": [
                    {"table": "proj.hypeon_marts_ads.fct", "reason": "marts", "columns": ["item_id"]},
                    {"table": "proj.raw_ads.facebook_events", "reason": "raw", "columns": ["item_id", "views"]},
                ],
                "sql_templates": [
                    "SELECT SUM(views) AS views FROM `proj.hypeon_marts_ads.fct` WHERE item_id LIKE 'FT05B%' LIMIT 500",
                    "SELECT SUM(views) AS views FROM `proj.raw_ads.facebook_events` WHERE item_id LIKE 'FT05B%' LIMIT 500",
                ],
            }
            from backend.app.copilot.chat_handler import _chat_v2
            store = MagicMock()
            out = _chat_v2("org1", "What's the view count of Item Id starting with FT05B from Facebook?", "sess1", 1, store)
    assert call_count == 2
    assert out.get("answer") or out.get("text")
    assert "100" in (out.get("answer") or "") or "100" in (out.get("text") or "") or out.get("data")
    if out.get("data"):
        assert out["data"][0].get("views") == 100


def test_chat_v2_returns_failure_message_when_all_plans_empty(env_copilot_v2):
    with patch("backend.app.copilot.chat_handler.run_bigquery_sql") as mock_run:
        mock_run.return_value = {"rows": [], "schema": [], "row_count": 0, "stats": {}, "error": None}
        with patch("backend.app.copilot.planner.analyze") as mock_analyze:
            mock_analyze.return_value = {
                "intent": "test",
                "candidates": [],
                "sql_templates": ["SELECT 1 FROM `p.d.t` LIMIT 500"],
            }
            with patch("backend.app.copilot.planner.replan") as mock_replan:
                mock_replan.return_value = {"sql_templates": []}
                from backend.app.copilot.chat_handler import _chat_v2
                store = MagicMock()
                out = _chat_v2("org1", "Views for FT05B?", "sess1", 1, store)
    assert "couldn't find" in (out.get("answer") or "").lower() or "tried" in (out.get("answer") or "").lower()
    assert out.get("data") == []


def test_copilot_chat_uses_v2_when_flag_set(env_copilot_v2):
    """When COPILOT_V2=true, chat() should use V2 path and return answer from run_bigquery_sql result."""
    with patch("backend.app.copilot.chat_handler._chat_v2") as mock_v2:
        mock_v2.return_value = {"answer": "Total views: 50.", "data": [{"views": 50}], "text": "Total views: 50.", "session_id": "s1"}
        from backend.app.copilot.chat_handler import chat
        out = chat("org1", "What are the views for FT05B?", session_id="s1", client_id=1)
    mock_v2.assert_called_once()
    assert out.get("answer") == "Total views: 50."
    assert out.get("data") and out["data"][0].get("views") == 50
