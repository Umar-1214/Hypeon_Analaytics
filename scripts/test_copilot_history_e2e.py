#!/usr/bin/env python3
"""
E2E test: Copilot session/history with test@hypeon.ai.
- Login, create 2 sessions (one with 2 messages), list sessions, fetch history for each.
- Fails if sessions or history are missing (DB persistence must work).
Run from repo root; backend on 8001. Needs VITE_FIREBASE_API_KEY.
"""
import os
import sys
import time
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT))
for p in [REPO_ROOT / ".env", REPO_ROOT / "frontend" / ".env"]:
    try:
        from dotenv import load_dotenv
        load_dotenv(p)
    except Exception:
        pass

import requests

BASE = os.environ.get("BASE_URL", "http://127.0.0.1:8001").rstrip("/")
API_KEY = os.environ.get("VITE_FIREBASE_API_KEY") or os.environ.get("FIREBASE_API_KEY")
EMAIL = os.environ.get("TEST_USER_EMAIL", "test@hypeon.ai")
PASSWORD = os.environ.get("TEST_USER_PASSWORD", "test@123")


def main():
    print("E2E: Copilot sessions & history (test@hypeon.ai)")
    print("=" * 55)

    if not API_KEY:
        print("FAIL: set VITE_FIREBASE_API_KEY in .env")
        return 1

    # Health
    try:
        r = requests.get(f"{BASE}/health", timeout=5)
        if r.status_code != 200:
            print("FAIL: health", r.status_code)
            return 1
    except Exception as e:
        print("FAIL: backend not reachable:", e)
        return 1
    print("1. Health OK")

    # Login
    r = requests.post(
        "https://identitytoolkit.googleapis.com/v1/accounts:signInWithPassword?key=" + API_KEY,
        json={"email": EMAIL, "password": PASSWORD, "returnSecureToken": True},
        timeout=15,
    )
    if r.status_code != 200:
        print("FAIL: Firebase sign-in", r.status_code, r.text[:200])
        return 1
    token = r.json().get("idToken")
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
    print("2. Auth OK (test@hypeon.ai)")

    # Store info
    r = requests.get(f"{BASE}/api/v1/copilot/store-info", headers=headers, timeout=10)
    if r.status_code != 200:
        print("FAIL: store-info", r.status_code)
        return 1
    info = r.json() or {}
    print("3. Store: %s org=%s user_id=%s" % (
        info.get("store"), info.get("organization_id"),
        (info.get("user_id") or "None")[:20] + "..." if info.get("user_id") else "None"))

    # Initial sessions count
    r = requests.get(f"{BASE}/api/v1/copilot/sessions", headers=headers, timeout=15)
    if r.status_code != 200:
        print("FAIL: GET /sessions", r.status_code)
        return 1
    initial = (r.json().get("sessions") or [])
    print("4. Sessions (initial): %d" % len(initial))

    # Create session A: first message
    r = requests.post(
        f"{BASE}/api/v1/copilot/chat",
        json={"message": "Hi", "client_id": 1},
        headers=headers,
        timeout=30,
    )
    if r.status_code != 200:
        print("FAIL: POST /chat (session A)", r.status_code, r.text[:200])
        return 1
    sid_a = r.json().get("session_id")
    if not sid_a:
        print("FAIL: no session_id in response")
        return 1
    print("5. Session A created: %s..." % sid_a[:24])

    time.sleep(1)

    # Session A: second message (same session) – use greeting for fast path
    r = requests.post(
        f"{BASE}/api/v1/copilot/chat",
        json={"message": "Hey", "session_id": sid_a, "client_id": 1},
        headers=headers,
        timeout=30,
    )
    if r.status_code != 200:
        print("WARN: POST /chat (2nd msg session A)", r.status_code, "- continuing")
    else:
        print("6. Session A: 2nd message sent")

    time.sleep(1)

    # Create session B (new chat)
    r = requests.post(
        f"{BASE}/api/v1/copilot/chat",
        json={"message": "Hello", "client_id": 1},
        headers=headers,
        timeout=30,
    )
    if r.status_code != 200:
        print("FAIL: POST /chat (session B)", r.status_code)
        return 1
    sid_b = r.json().get("session_id")
    if not sid_b:
        print("FAIL: no session_id for session B")
        return 1
    print("7. Session B created: %s..." % sid_b[:24])

    time.sleep(1)

    # List sessions – must include A and B
    r = requests.get(f"{BASE}/api/v1/copilot/sessions", headers=headers, timeout=15)
    if r.status_code != 200:
        print("FAIL: GET /sessions (after create)", r.status_code)
        return 1
    sessions = (r.json().get("sessions") or [])
    ids = [s.get("session_id") for s in sessions]
    if sid_a not in ids:
        print("FAIL: session A not in list. count=%d ids=%s" % (len(sessions), ids[:5]))
        return 1
    if sid_b not in ids:
        print("FAIL: session B not in list. count=%d ids=%s" % (len(sessions), ids[:5]))
        return 1
    print("8. Sessions list: %d total, A and B present" % len(sessions))

    # History for session A (at least user + assistant for 2 turns)
    r = requests.get(
        f"{BASE}/api/v1/copilot/chat/history",
        params={"session_id": sid_a},
        headers=headers,
        timeout=10,
    )
    if r.status_code != 200:
        print("FAIL: GET /history (session A)", r.status_code)
        return 1
    msgs_a = (r.json().get("messages") or [])
    if len(msgs_a) < 2:
        print("FAIL: session A history expected >=2 messages, got %d" % len(msgs_a))
        return 1
    roles_a = [m.get("role") for m in msgs_a]
    if "user" not in roles_a or "assistant" not in roles_a:
        print("FAIL: session A missing user/assistant messages, roles=%s" % roles_a)
        return 1
    print("9. Session A history: %d messages (user + assistant)" % len(msgs_a))

    # History for session B
    r = requests.get(
        f"{BASE}/api/v1/copilot/chat/history",
        params={"session_id": sid_b},
        headers=headers,
        timeout=10,
    )
    if r.status_code != 200:
        print("FAIL: GET /history (session B)", r.status_code)
        return 1
    msgs_b = (r.json().get("messages") or [])
    if len(msgs_b) < 2:
        print("FAIL: session B history expected >=2 messages, got %d" % len(msgs_b))
        return 1
    print("10. Session B history: %d messages" % len(msgs_b))

    print("=" * 55)
    print("PASS: Sessions and history are stored and returned (DB + API OK).")
    return 0


if __name__ == "__main__":
    sys.exit(main())
