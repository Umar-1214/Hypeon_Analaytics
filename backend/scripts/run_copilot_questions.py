#!/usr/bin/env python3
"""
Run the list of Copilot user questions in-process (chat_handler.chat) or via API.
Writes results to artifacts/copilot_questions_report.json and artifacts/copilot_questions_logs/.
"""
from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO_ROOT))

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


def _is_success(out: dict) -> bool:
    has_data = bool(out.get("data"))
    answer = (out.get("answer") or out.get("text") or "").lower()
    return has_data or "couldn't find" not in answer[:200]


def run_in_process(questions: list[str], organization_id: str = "default", client_id: int = 1):
    """Call chat_handler.chat() in-process for each question."""
    from backend.app.copilot.chat_handler import chat

    results = []
    for i, q in enumerate(questions):
        print(f"[{i+1}/{len(questions)}] {q[:60]}...")
        try:
            out = chat(organization_id, q, session_id=f"sim-{i}", client_id=client_id)
            results.append({
                "question": q,
                "answer": out.get("answer") or out.get("text") or "",
                "data_rows": len(out.get("data") or []),
                "session_id": out.get("session_id"),
                "success": _is_success(out),
            })
        except Exception as e:
            results.append({
                "question": q,
                "answer": f"Error: {str(e)[:300]}",
                "data_rows": 0,
                "session_id": f"sim-{i}",
                "success": False,
            })
    return results


def run_via_api(questions: list[str], base_url: str, organization_id: str = "default", client_id: int = 1, token: str | None = None):
    """POST each question to /api/v1/copilot/chat."""
    try:
        import requests
    except ImportError:
        print("Install requests to use --api: pip install requests")
        return []

    results = []
    url = f"{base_url.rstrip('/')}/api/v1/copilot/chat"
    headers = {"Content-Type": "application/json"}
    if token:
        headers["Authorization"] = f"Bearer {token}"

    for i, q in enumerate(questions):
        print(f"[{i+1}/{len(questions)}] {q[:60]}...")
        try:
            r = requests.post(
                url,
                json={"message": q, "session_id": f"sim-{i}", "client_id": client_id},
                headers=headers,
                timeout=120,
            )
            r.raise_for_status()
            out = r.json()
            results.append({
                "question": q,
                "answer": out.get("answer") or out.get("text") or "",
                "data_rows": len(out.get("data") or []),
                "session_id": out.get("session_id"),
                "success": _is_success(out),
            })
        except Exception as e:
            results.append({
                "question": q,
                "answer": f"Error: {str(e)[:300]}",
                "data_rows": 0,
                "session_id": f"sim-{i}",
                "success": False,
            })
    return results


def main():
    parser = argparse.ArgumentParser(description="Run Copilot questions in-process or via API")
    parser.add_argument("--api", action="store_true", help="Use API (default: in-process)")
    parser.add_argument("--base-url", default="http://localhost:8001", help="Base URL when using --api")
    parser.add_argument("--token", default=os.environ.get("COPILOT_TEST_TOKEN"), help="Auth token for API")
    parser.add_argument("--org", default="default", help="Organization ID")
    parser.add_argument("--client-id", type=int, default=1, help="Client ID")
    parser.add_argument("--limit", type=int, default=0, help="Limit number of questions (0 = all)")
    args = parser.parse_args()

    questions = QUESTIONS[: args.limit] if args.limit else list(QUESTIONS)

    if args.api:
        results = run_via_api(questions, args.base_url, args.org, args.client_id, args.token)
    else:
        results = run_in_process(questions, args.org, args.client_id)

    out_dir = REPO_ROOT / "artifacts"
    out_dir.mkdir(parents=True, exist_ok=True)
    logs_dir = out_dir / "copilot_questions_logs"
    logs_dir.mkdir(parents=True, exist_ok=True)

    report = {
        "total": len(results),
        "success": sum(1 for r in results if r.get("success")),
        "results": results,
    }
    report_path = out_dir / "copilot_questions_report.json"
    report_path.write_text(json.dumps(report, indent=2), encoding="utf-8")
    print(f"Wrote {report_path} (success: {report['success']}/{report['total']})")

    for i, r in enumerate(results):
        (logs_dir / f"q_{i+1:02d}.json").write_text(json.dumps(r, indent=2), encoding="utf-8")
    print(f"Wrote {len(results)} logs to {logs_dir}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
