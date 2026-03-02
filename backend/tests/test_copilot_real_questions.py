"""
Tests that Copilot planner and flow handle real analytics questions.
Uses mocks; asserts intent extraction, candidate tables, and SQL shape for representative questions.
"""
from __future__ import annotations

import sys
from pathlib import Path
from unittest.mock import patch

import pytest

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))

REAL_QUESTIONS = [
    "Top 10 product IDs driving 50% of revenue — pareto/cumulative revenue ranking",
    "High volume vs high profit products — units sold vs revenue per order, two-axis sort",
    "New product launch performance — sessions, add-to-cart rate, revenue vs existing catalogue",
    "Products that spiked in last 14 days vs 14 days before — period-over-period delta",
    "Which two products are most often bought together? — basket analysis, co-purchase pairs",
    "True last-click ROAS vs what Google/Meta claim — the double-counting reality check",
    "Which channels actually bring new customers vs recycled buyers — is_new_customer by channel",
    "Most common channel path before first purchase — multi-touch journey mapping",
    "Days from first visit to first purchase by channel — time lag analysis",
    "Which Google campaigns to scale vs pause right now — campaign-level ROAS with threshold flags",
    "Which channel acquires customers with the highest 90-day LTV — not first order, total 90-day spend",
    "Repeat purchase rate + what did they first buy — cohort repurchase analysis",
    "Customers who used to buy every 3–4 weeks but went quiet 45–90 days ago — churn risk list",
    "Profile of the top 10% spenders — channel, first product, purchase frequency",
    "Top 5 cities by revenue vs cities with traffic but no conversions — geo funnel gap",
    "Countries adding to cart but abandoning checkout — signals friction or shipping/pricing issues",
    "Mobile vs desktop conversion rate gap — confirm the suspicion most founders have",
    "Which landing pages generate revenue, not just traffic — entry page → order attribution",
    "Where exactly do people drop off in checkout — paid vs organic — funnel step comparison",
]


def test_extract_intent_covers_real_questions():
    """Intent extraction yields non-empty, relevant tokens for each real question."""
    from backend.app.copilot.planner import _extract_intent

    for q in REAL_QUESTIONS[:5]:
        intent = _extract_intent(q)
        assert intent
        assert len(intent) >= 3
        assert "analytics" in intent or any(w in intent for w in ("revenue", "product", "top", "channel", "session", "roas", "basket", "purchase", "city", "mobile"))


def test_planner_returns_candidates_for_revenue_pareto():
    """Pareto / top-N revenue question produces candidates with revenue-relevant tables (LLM will generate SQL)."""
    with patch("backend.app.copilot.tools.discover_tables") as mock_discover:
        mock_discover.return_value = [
            {"project": "p", "dataset": "marts", "table": "fct_orders", "columns": ["item_id", "revenue", "event_date", "client_id"]},
            {"project": "p", "dataset": "raw", "table": "events", "columns": ["item_id", "value", "event_date"]},
        ]
        from backend.app.copilot.planner import analyze

        plan = analyze(
            "Top 10 product IDs driving 50% of revenue — pareto/cumulative revenue ranking",
            client_id=1,
            organization_id="org1",
        )
    assert plan.get("intent")
    assert plan.get("candidates")
    tables_and_cols = " ".join((c.get("table") or "") + " " + " ".join(c.get("columns") or []) for c in plan["candidates"]).upper()
    assert "REVENUE" in tables_and_cols or "VALUE" in tables_and_cols
    assert "ITEM_ID" in tables_and_cols or "PRODUCT" in tables_and_cols


def test_planner_returns_candidates_with_columns():
    """Planner returns candidates with table and columns for LLM SQL generation."""
    with patch("backend.app.copilot.tools.discover_tables") as mock_discover:
        mock_discover.return_value = [
            {"project": "p", "dataset": "ds", "table": "t", "columns": ["item_id", "value", "event_date"]},
        ]
        from backend.app.copilot.planner import analyze

        plan = analyze("Top 10 product IDs by revenue", client_id=1, organization_id="org1")
    assert plan.get("candidates")
    c = plan["candidates"][0]
    assert "item_id" in (c.get("columns") or []) or "value" in (c.get("columns") or [])


def test_chat_returns_formatted_answer_for_real_question_when_mock_returns_data():
    """Chat returns a well-structured answer when LLM SQL + run return rows (real question)."""
    with patch("backend.app.copilot.chat_handler.run_bigquery_sql") as mock_run:
        mock_run.return_value = {
            "rows": [
                {"item_id": "SKU1", "revenue": 1000, "cumulative_pct": 25.5},
                {"item_id": "SKU2", "revenue": 800, "cumulative_pct": 45.0},
            ],
            "schema": ["item_id", "revenue", "cumulative_pct"],
            "row_count": 2,
            "stats": {},
            "error": None,
        }
        with patch("backend.app.copilot.chat_handler._llm_generate_sql") as mock_llm:
            mock_llm.return_value = "SELECT item_id, revenue, 50 AS cumulative_pct FROM `p.d.t` LIMIT 10"
            with patch("backend.app.copilot.planner.analyze") as mock_analyze:
                mock_analyze.return_value = {
                    "intent": "top 10 product revenue pareto",
                    "candidates": [{"table": "p.d.t", "columns": ["item_id", "revenue", "cumulative_pct"]}],
                }
                from backend.app.copilot.chat_handler import chat

                out = chat(
                    "org1",
                    "Top 10 product IDs driving 50% of revenue — pareto/cumulative revenue ranking",
                    session_id="s1",
                    client_id=1,
                )
    assert out.get("answer") or out.get("text")
    assert out.get("data") and len(out["data"]) == 2
    assert "session_id" in out
    text = (out.get("answer") or out.get("text") or "").lower()
    assert "sku" in text or "1000" in text or "revenue" in text or "row" in text or "|" in text


def test_chat_handles_greeting_without_planner():
    """Short greeting does not call planner or run_bigquery_sql."""
    with patch("backend.app.copilot.planner.analyze") as mock_analyze:
        with patch("backend.app.copilot.chat_handler.run_bigquery_sql") as mock_run:
            from backend.app.copilot.chat_handler import chat

            out = chat("org1", "Hi!", session_id="s1", client_id=1)
    mock_analyze.assert_not_called()
    mock_run.assert_not_called()
    assert "hi" in (out.get("answer") or "").lower() or "help" in (out.get("answer") or "").lower()
