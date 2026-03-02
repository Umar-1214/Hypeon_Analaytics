#!/usr/bin/env python3
"""
Test Claude path used by Copilot chat: load .env, then call chat_completion_with_tools.
Run from repo root: python backend/scripts/test_claude_copilot.py
"""
import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
BACKEND = ROOT / "backend"
sys.path.insert(0, str(BACKEND))

try:
    from dotenv import load_dotenv
    load_dotenv(ROOT / ".env")
    print("Loaded .env from", ROOT / ".env")
except Exception as e:
    print("Could not load .env:", e)

key = os.environ.get("ANTHROPIC_API_KEY")
if not key or not key.strip():
    print("ANTHROPIC_API_KEY is not set. Add it to .env (repo root).")
    sys.exit(1)
print("ANTHROPIC_API_KEY is set (masked).")

# Minimal tools like Copilot uses
from app.copilot.tools import COPILOT_TOOLS
from app.copilot.chat_handler import _build_system_template

def test_claude_tools():
    from app.llm_claude import (
        is_claude_configured,
        chat_completion_with_tools as claude_tools_chat,
    )
    if not is_claude_configured():
        print("is_claude_configured() returned False (ANTHROPIC_API_KEY not visible to llm_claude).")
        return False
    system_template = _build_system_template(1)
    messages = [{"role": "user", "content": "Say hello in one short sentence. Do not use any tools."}]
    print("Calling chat_completion_with_tools (Claude with tools)...")
    try:
        result = claude_tools_chat(messages, COPILOT_TOOLS, system=system_template)
        if not isinstance(result, dict):
            print("Unexpected result type:", type(result))
            return False
        text = result.get("text", "").strip()
        # Avoid Windows console Unicode errors (emoji in reply)
        raw = (text[:300] if text else "(empty)")
        safe = "".join(c if ord(c) < 128 else "?" for c in raw)
        if result.get("tool_calls"):
            print("LLM requested tools (ok for this test). First reply:", safe)
        else:
            print("Claude reply (text only):", safe)
        print("SUCCESS: Claude with tools path works.")
        return True
    except Exception as e:
        print("FAILED:", type(e).__name__, str(e)[:500])
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    ok = test_claude_tools()
    sys.exit(0 if ok else 1)
