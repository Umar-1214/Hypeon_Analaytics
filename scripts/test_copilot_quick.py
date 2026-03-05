#!/usr/bin/env python3
"""Quick test: health, auth, store-info, sessions, history for one known session. Uses test@hypeon.ai / test@123."""
import os
import sys
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
KNOWN_SESSION_ID = "928d439c-c703-4b50-9410-a80892592b57"  # from user's Firestore

def main():
    print("Quick Copilot test (test@hypeon.ai)")
    print("=" * 50)

    # 1. Health
    try:
        r = requests.get(f"{BASE}/health", timeout=5)
        print("1. Health:", r.status_code, r.json() if r.ok else r.text[:80])
        if r.status_code != 200:
            return 1
    except Exception as e:
        print("1. Health FAIL:", e)
        return 1

    if not API_KEY:
        print("2. SKIP auth (VITE_FIREBASE_API_KEY not set)")
        return 0

    # 2. Firebase token
    r = requests.post(
        "https://identitytoolkit.googleapis.com/v1/accounts:signInWithPassword?key=" + API_KEY,
        json={"email": EMAIL, "password": PASSWORD, "returnSecureToken": True},
        timeout=15,
    )
    if r.status_code != 200:
        print("2. Firebase sign-in FAIL:", r.status_code, r.text[:200])
        return 1
    token = r.json().get("idToken")
    print("2. Firebase auth OK")

    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}

    # 3. Store info
    try:
        r = requests.get(f"{BASE}/api/v1/copilot/store-info", headers=headers, timeout=10)
        print("3. Store-info:", r.status_code, r.json() if r.ok else r.text[:150])
        if r.ok:
            info = r.json()
            print("   -> store=%s database_id=%s organization_id=%s" % (
                info.get("store"), info.get("database_id"), info.get("organization_id")))
    except requests.RequestException as e:
        print("3. Store-info FAIL:", e)
        return 1

    # 4. Sessions list (backend has 10s timeout; use 15s client)
    try:
        r = requests.get(f"{BASE}/api/v1/copilot/sessions", headers=headers, timeout=15)
        print("4. Sessions:", r.status_code, end="")
        if r.ok:
            sessions = (r.json() or {}).get("sessions") or []
            print(" count=%d" % len(sessions))
            for s in sessions[:10]:
                print("   -", s.get("session_id"), "|", (s.get("title") or "New chat")[:50])
        else:
            print("", r.text[:150])
    except requests.RequestException as e:
        print("4. Sessions FAIL (timeout?):", e)
        return 1

    # 5. History for known session
    try:
        r = requests.get(
            f"{BASE}/api/v1/copilot/chat/history",
            params={"session_id": KNOWN_SESSION_ID},
            headers=headers,
            timeout=15,
        )
        print("5. History for", KNOWN_SESSION_ID[:20] + "...:", r.status_code, end="")
        if r.ok:
            msgs = (r.json() or {}).get("messages") or []
            print(" messages=%d" % len(msgs))
            for i, m in enumerate(msgs[:3]):
                print("   [%d] %s: %s" % (i, m.get("role"), (m.get("content") or "")[:60]))
        else:
            print("", r.text[:100])
    except requests.RequestException as e:
        print("5. History FAIL:", e)

    # 6. Chat "Hi" (quick greeting, no BQ)
    print("6. POST /chat (Hi)...")
    try:
        r = requests.post(
            f"{BASE}/api/v1/copilot/chat",
            json={"message": "Hi", "client_id": 1},
            headers=headers,
            timeout=30,
        )
        print("   ", r.status_code, end="")
        if r.ok:
            out = r.json()
            print(" session_id=%s" % (out.get("session_id") or "")[:20])
            print("   answer:", (out.get("text") or out.get("answer") or "")[:120])
        else:
            print("", r.text[:200])
    except requests.RequestException as e:
        print("   FAIL:", e)

    print("=" * 50)
    print("Done.")
    return 0

if __name__ == "__main__":
    sys.exit(main())
