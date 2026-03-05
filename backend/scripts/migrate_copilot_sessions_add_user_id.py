#!/usr/bin/env python3
"""
One-time migration: set user_id on existing copilot_sessions documents that don't have it.
Use when you've added per-user session scoping and want existing sessions to appear for a specific user.

  python -m backend.scripts.migrate_copilot_sessions_add_user_id --uid <firebase_uid>
  # Or with org filter (only update docs with this organization_id):
  python -m backend.scripts.migrate_copilot_sessions_add_user_id --uid <firebase_uid> --org default

Run from repo root. Requires GOOGLE_APPLICATION_CREDENTIALS or gcloud auth application-default login.
"""
from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO_ROOT))
for p in [REPO_ROOT / ".env", REPO_ROOT / "frontend" / ".env"]:
    try:
        from dotenv import load_dotenv
        load_dotenv(p)
    except Exception:
        pass


def main():
    ap = argparse.ArgumentParser(description="Set user_id on copilot_sessions docs that lack it")
    ap.add_argument("--uid", required=True, help="Firebase uid to assign to legacy sessions")
    ap.add_argument("--org", default=None, help="Only update docs with this organization_id (default: all)")
    ap.add_argument("--dry-run", action="store_true", help="Only print what would be updated")
    args = ap.parse_args()
    uid = (args.uid or "").strip()
    if not uid:
        print("--uid is required")
        return 1

    try:
        from backend.app.auth.firebase import init_firebase
        from backend.app.auth.firestore_user import _get_firestore
        from backend.app.copilot.session_memory import COPLIOT_SESSIONS_COLLECTION
    except Exception as e:
        print("Import failed:", e)
        return 1

    init_firebase()
    db = _get_firestore()
    if not db:
        print("Firestore not available")
        return 1

    col = db.collection(COPLIOT_SESSIONS_COLLECTION)
    updated = 0
    for doc in col.stream():
        d = doc.to_dict() or {}
        if d.get("user_id"):
            continue
        if args.org is not None and (d.get("organization_id") or "default") != args.org:
            continue
        if args.dry_run:
            print("Would set user_id=%s on %s (org=%s)" % (uid, doc.id, d.get("organization_id")))
        else:
            doc.reference.update({"user_id": uid})
            print("Updated %s with user_id=%s" % (doc.id, uid))
        updated += 1

    print("Done. Updated %d document(s)." % updated)
    return 0


if __name__ == "__main__":
    sys.exit(main())
