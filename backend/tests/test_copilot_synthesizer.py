"""Tests for copilot_synthesizer."""
import json
import sys
from pathlib import Path
ROOT = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(ROOT))

from backend.app.copilot_synthesizer import (
    build_prompt_grounded,
    _parse_llm_response,
    synthesize,
    set_llm_client,
    get_llm_client,
)


def test_build_prompt_grounded():
    insight = {"insight_id": "abc", "summary": "Test", "evidence": [{"metric": "revenue", "value": 100, "baseline": 80, "period": "28d"}]}
    prompt = build_prompt_grounded(insight, None)
    assert "abc" in prompt
    assert "revenue" in prompt or "Test" in prompt


def test_parse_llm_response():
    raw = json.dumps({"summary": "S", "explanation": "E", "action_steps": [], "expected_impact": {}, "provenance": "p", "confidence": 0.9, "tldr": "T"})
    out = _parse_llm_response(raw)
    assert out["summary"] == "S"
    assert out["confidence"] == 0.9


def test_synthesize_mock_llm():
    def mock_load(iid):
        if iid != "test-id":
            return None
        return {"insight_id": "test-id", "summary": "Test insight", "evidence": []}

    def mock_llm(prompt):
        return json.dumps({"summary": "Synth", "explanation": "Expl", "action_steps": ["A"], "expected_impact": {"metric": "rev", "estimate": 1.0, "units": "USD"}, "provenance": "rules", "confidence": 0.8, "tldr": "TLDR"})

    out = synthesize("test-id", load_insight=mock_load, llm_client=mock_llm)
    assert "error" not in out
    assert out["summary"] == "Synth"
    assert out["insight_id"] == "test-id"

    out_miss = synthesize("missing", load_insight=mock_load, llm_client=mock_llm)
    assert out_miss.get("error") == "insight not found"
