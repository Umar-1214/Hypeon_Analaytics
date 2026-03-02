"""
Integration tests: Copilot discover → LLM SQL generation → run_bigquery_sql → validate (retry on empty/invalid).
"""
from __future__ import annotations

import sys
from pathlib import Path
from unittest.mock import patch

import pytest

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))

_CANDIDATES = [
    {"table": "proj.marts.fct", "columns": ["item_id", "views"]},
    {"table": "proj.raw_ads.fb_events", "columns": ["item_id", "views", "channel"]},
]


def test_fallback_to_second_sql_when_first_returns_empty():
    """First LLM SQL returns 0 rows, second returns data; answer uses second."""
    call_count = 0

    def mock_run_bigquery_sql(sql, organization_id, client_id, **kwargs):
        nonlocal call_count
        call_count += 1
        if call_count == 1:
            return {"rows": [], "schema": [], "row_count": 0, "stats": {}, "error": None}
        return {"rows": [{"views": 100}], "schema": ["views"], "row_count": 1, "stats": {}, "error": None}

    def mock_llm_generate_sql(system, user_content):
        nonlocal call_count
        if call_count == 0:
            return "SELECT SUM(views) AS views FROM `proj.marts.fct` WHERE 1=1 LIMIT 500"
        return "SELECT SUM(views) AS views FROM `proj.raw_ads.fb_events` WHERE 1=1 LIMIT 500"

    with patch("backend.app.copilot.chat_handler.run_bigquery_sql", side_effect=mock_run_bigquery_sql):
        with patch("backend.app.copilot.chat_handler._llm_generate_sql", side_effect=mock_llm_generate_sql):
            with patch("backend.app.copilot.planner.analyze") as mock_analyze:
                mock_analyze.return_value = {"intent": "views count", "candidates": _CANDIDATES}
                from backend.app.copilot.chat_handler import chat
                out = chat("org1", "What's the view count of Item Id from Facebook?", session_id="sess1", client_id=1)
    assert call_count == 2
    assert out.get("answer") or out.get("text")
    assert "100" in (out.get("answer") or "") or "100" in (out.get("text") or "") or (out.get("data") and out["data"][0].get("views") == 100)


def test_chat_returns_failure_message_when_no_data():
    """When run_bigquery_sql always returns empty, user gets couldn't find message."""
    with patch("backend.app.copilot.chat_handler.run_bigquery_sql") as mock_run:
        mock_run.return_value = {"rows": [], "schema": [], "row_count": 0, "stats": {}, "error": None}
        with patch("backend.app.copilot.chat_handler._llm_generate_sql") as mock_llm:
            mock_llm.return_value = "SELECT 1 FROM `p.d.t` LIMIT 500"
            with patch("backend.app.copilot.planner.analyze") as mock_analyze:
                mock_analyze.return_value = {"intent": "test", "candidates": _CANDIDATES}
                from backend.app.copilot.chat_handler import chat
                out = chat("org1", "Views for FT05B?", session_id="sess1", client_id=1)
    assert "couldn't find" in (out.get("answer") or "").lower() or "tried" in (out.get("answer") or "").lower()
    assert out.get("data") == []


def test_chat_uses_llm_sql_and_returns_data():
    """Chat gets candidates → LLM generates SQL → run_bigquery_sql returns data."""
    with patch("backend.app.copilot.chat_handler.run_bigquery_sql") as mock_run:
        mock_run.return_value = {"rows": [{"views": 50}], "schema": ["views"], "row_count": 1, "stats": {}, "error": None}
        with patch("backend.app.copilot.chat_handler._llm_generate_sql") as mock_llm:
            mock_llm.return_value = "SELECT 50 AS views FROM `p.d.t` LIMIT 500"
            with patch("backend.app.copilot.planner.analyze") as mock_analyze:
                mock_analyze.return_value = {"intent": "views", "candidates": _CANDIDATES}
                from backend.app.copilot.chat_handler import chat
                out = chat("org1", "What are the views for FT05B?", session_id="s1", client_id=1)
    mock_analyze.assert_called_once()
    mock_llm.assert_called()
    assert out.get("answer") or out.get("text")
    assert out.get("data") and out["data"][0].get("views") == 50


def test_chat_retries_on_invalid_then_succeeds():
    """First run empty, second invalid (negative), third valid; final answer uses third."""
    call_count = 0

    def mock_run_bigquery_sql(sql, organization_id, client_id, **kwargs):
        nonlocal call_count
        call_count += 1
        if call_count == 1:
            return {"rows": [], "schema": [], "row_count": 0, "stats": {}, "error": None}
        if call_count == 2:
            return {"rows": [{"total_count": -1}], "schema": ["total_count"], "row_count": 1, "stats": {}, "error": None}
        return {"rows": [{"views": 42}], "schema": ["views"], "row_count": 1, "stats": {}, "error": None}

    with patch("backend.app.copilot.chat_handler.run_bigquery_sql", side_effect=mock_run_bigquery_sql):
        with patch("backend.app.copilot.chat_handler._llm_generate_sql") as mock_llm:
            mock_llm.return_value = "SELECT 42 AS views FROM `p.d.t3` LIMIT 500"
            with patch("backend.app.copilot.planner.analyze") as mock_analyze:
                mock_analyze.return_value = {"intent": "views", "candidates": _CANDIDATES}
                from backend.app.copilot.chat_handler import chat
                out = chat("org1", "What are the views?", session_id="sess1", client_id=1)
    assert call_count >= 2
    assert out.get("data") and len(out["data"]) == 1 and out["data"][0].get("views") == 42


def test_chat_no_candidates_returns_helpful_message():
    """When planner returns no candidates, user gets message about no tables."""
    with patch("backend.app.copilot.planner.analyze") as mock_analyze:
        mock_analyze.return_value = {"intent": "test", "candidates": []}
        from backend.app.copilot.chat_handler import chat
        out = chat("org1", "Something obscure?", session_id="s1", client_id=1)
    assert "couldn't find any tables" in (out.get("answer") or "").lower() or "no tables" in (out.get("answer") or "").lower()
    assert out.get("data") == []
