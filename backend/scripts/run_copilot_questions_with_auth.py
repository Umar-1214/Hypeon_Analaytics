#!/usr/bin/env python3
"""
POST Copilot questions to the API using Firebase auth.
Credentials: test@hypeon.ai / test@123 (default; overridable via env).
Run from repo root with backend on port 8001:
  python -m backend.scripts.run_copilot_questions_with_auth
  # or: TEST_USER_EMAIL=test@hypeon.ai TEST_USER_PASSWORD=test@123 python -m backend.scripts.run_copilot_questions_with_auth
"""
from __future__ import annotations

import os
import sys
from pathlib import Path

try:
    if sys.platform == "win32":
        sys.stdout.reconfigure(encoding="utf-8")
except (AttributeError, OSError):
    pass

REPO_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO_ROOT))
try:
    from dotenv import load_dotenv
    load_dotenv(REPO_ROOT / ".env")
    load_dotenv(REPO_ROOT / "frontend" / ".env")
except Exception:
    pass

import requests

BASE = os.environ.get("BASE_URL", "http://127.0.0.1:8001").rstrip("/")
API_KEY = os.environ.get("VITE_FIREBASE_API_KEY") or os.environ.get("FIREBASE_API_KEY")
EMAIL = os.environ.get("TEST_USER_EMAIL", "test@hypeon.ai")
PASSWORD = os.environ.get("TEST_USER_PASSWORD", "test@123")
MAX_QUESTIONS = int(os.environ.get("MAX_QUESTIONS", "0") or "0")  # 0 = all

QUESTIONS = [
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


def get_token():
    r = requests.post(
        "https://identitytoolkit.googleapis.com/v1/accounts:signInWithPassword?key=" + API_KEY,
        json={"email": EMAIL, "password": PASSWORD, "returnSecureToken": True},
        timeout=15,
    )
    if r.status_code != 200:
        return None
    return r.json().get("idToken")


def main():
    if not API_KEY:
        print("Set VITE_FIREBASE_API_KEY in .env")
        return 1
    token = get_token()
    if not token:
        print("Firebase sign-in failed. Check TEST_USER_EMAIL / TEST_USER_PASSWORD.")
        return 1
    qs = QUESTIONS[:MAX_QUESTIONS] if MAX_QUESTIONS > 0 else QUESTIONS
    print(f"Signed in as {EMAIL}. Sending {len(qs)} questions to Copilot...\n")
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
    url = f"{BASE}/api/v1/copilot/chat"
    artifacts_dir = REPO_ROOT / "artifacts"
    out_file = None
    if artifacts_dir.is_dir():
        from datetime import datetime
        out_file = artifacts_dir / f"copilot_answers_{datetime.now().strftime('%Y%m%d_%H%M')}.txt"
    for i, q in enumerate(qs, 1):
        print(f"--- Q{i}/{len(qs)}: {q[:70]}{'...' if len(q) > 70 else ''}")
        try:
            r = requests.post(
                url,
                json={"message": q, "session_id": f"auth-test-{i}", "client_id": 1},
                headers=headers,
                timeout=180,
            )
            out = r.json() if r.content else {}
            text = (out.get("text") or out.get("answer") or "").strip()
            data = out.get("data") or []
            print(f"  Status: {r.status_code} | Rows: {len(data)}")
            print(f"  Answer: {(text[:300] + '...') if len(text) > 300 else text}")
            if out_file is not None:
                with open(out_file, "a", encoding="utf-8") as f:
                    f.write(f"\n\n--- Q{i}: {q}\n\n")
                    f.write(f"Status: {r.status_code} | Rows: {len(data)}\n\n")
                    f.write(text + "\n")
        except requests.RequestException as e:
            print(f"  Error: {e}")
            if out_file is not None:
                with open(out_file, "a", encoding="utf-8") as f:
                    f.write(f"\n\n--- Q{i}: {q}\n\nError: {e}\n")
        except Exception as e:
            print(f"  Error: {e}")
            if out_file is not None:
                with open(out_file, "a", encoding="utf-8") as f:
                    f.write(f"\n\n--- Q{i}: {q}\n\nError: {e}\n")
        print()
    if out_file is not None:
        print(f"Full answers written to {out_file}")
    print("Done.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
