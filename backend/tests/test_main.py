"""Tests for FastAPI main (mocked BigQuery)."""
import sys
from pathlib import Path
ROOT = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(ROOT))

import pytest
from unittest.mock import patch, MagicMock
from fastapi.testclient import TestClient


@pytest.fixture
def client():
    import os
    os.environ.setdefault("API_KEY", "test-key")
    from backend.app.main import app
    return TestClient(app)


def test_health(client):
    # Cache may not be ready when using TestClient (lifespan not run); mark ready for test
    with patch("backend.app.analytics_cache.get_cache_ready", return_value=True):
        r = client.get("/health")
    assert r.status_code == 200
    assert r.json()["status"] == "ok"


def test_insights_mocked_bq(client):
    with patch("backend.app.main._list_insights_scoped", return_value=[]):
        r = client.get("/insights", headers={"X-API-Key": "test-key", "X-Organization-Id": "test-org"})
    assert r.status_code == 200
    assert "items" in r.json()
    assert r.json()["count"] == 0


def test_copilot_query_mocked(client):
    with patch("backend.app.main.copilot_synthesize") as mock_synth:
        mock_synth.return_value = {"error": "insight not found", "insight_id": "nope"}
        r = client.post("/copilot_query", json={"insight_id": "nope"}, headers={"X-API-Key": "test-key"})
    assert r.status_code == 404


def test_copilot_chat_returns_answer_and_data(client):
    """POST /api/v1/copilot/chat returns 200 with answer, data, text, session_id (marts/raw fallback)."""
    def _mock_chat(org, msg, *, session_id=None, client_id=None, user_id=None):
        return {
            "answer": "Test answer.",
            "data": [{"metric": "sessions", "value": 100}],
            "text": "Test answer.",
            "session_id": session_id or "test-session",
        }
    with patch("backend.app.copilot.chat_handler.chat", side_effect=_mock_chat):
        r = client.post(
            "/api/v1/copilot/chat",
            json={"message": "Show last 7 days performance"},
            headers={"X-API-Key": "test-key", "X-Organization-Id": "default"},
        )
    assert r.status_code == 200
    data = r.json()
    assert "answer" in data and "text" in data and "session_id" in data and "data" in data
    assert isinstance(data["data"], list)
    assert len(data["session_id"]) > 0


def test_copilot_sessions_and_history_display_from_store(client):
    """Sessions list and chat history from store are returned so the frontend can display them."""
    from backend.app.copilot import session_memory

    mem = session_memory.SessionMemoryStore()
    mem.append("default", "sid-1", "user", "Hello", meta=None, user_id=None)
    mem.append("default", "sid-1", "assistant", "Hi there.", meta=None, user_id=None)

    with patch.object(session_memory, "get_session_store", return_value=mem):
        # List sessions – frontend loadSessions()
        r = client.get(
            "/api/v1/copilot/sessions",
            headers={"X-API-Key": "test-key", "X-Organization-Id": "default"},
        )
    assert r.status_code == 200
    data = r.json()
    assert "sessions" in data
    sessions = data["sessions"]
    assert len(sessions) >= 1
    assert any(s.get("session_id") == "sid-1" for s in sessions)

    with patch.object(session_memory, "get_session_store", return_value=mem):
        # Load history for session – frontend loadSession(sid)
        r = client.get(
            "/api/v1/copilot/chat/history",
            params={"session_id": "sid-1"},
            headers={"X-API-Key": "test-key", "X-Organization-Id": "default"},
        )
    assert r.status_code == 200
    hist = r.json()
    assert "messages" in hist
    assert len(hist["messages"]) >= 2
    roles = [m.get("role") for m in hist["messages"]]
    assert "user" in roles and "assistant" in roles


@pytest.mark.skip(reason="Endpoint /simulate_budget_shift not implemented in main.py")
def test_simulate_budget_shift_structure(client):
    r = client.post(
        "/simulate_budget_shift",
        json={"client_id": 1, "date": "2025-02-22", "from_campaign": "c1", "to_campaign": "c2", "amount": 100},
        headers={"X-API-Key": "test-key"},
    )
    assert r.status_code == 200
    data = r.json()
    assert "low" in data and "median" in data and "high" in data
    assert "expected_delta" in data and "confidence" in data
