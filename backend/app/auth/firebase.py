"""
Firebase Admin SDK: initialize app and verify ID tokens.
"""
from __future__ import annotations

import logging
import os
from typing import Any, Optional

logger = logging.getLogger(__name__)

_firebase_app = None


def init_firebase() -> None:
    """Initialize Firebase Admin SDK. Safe to call multiple times; uses default credentials or GOOGLE_APPLICATION_CREDENTIALS."""
    global _firebase_app
    if _firebase_app is not None:
        return
    try:
        import firebase_admin
        from firebase_admin import credentials

        if firebase_admin._apps:
            _firebase_app = firebase_admin.get_app()
            logger.info("Firebase already initialized")
            return

        cred_path = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS")
        project_id = (
            os.environ.get("FIREBASE_PROJECT_ID")
            or os.environ.get("GOOGLE_CLOUD_PROJECT")
            or os.environ.get("BQ_PROJECT", "")
        )
        project_id = (project_id or "").strip() or "hypeon-ai-prod"
        if cred_path and os.path.isfile(cred_path):
            _firebase_app = firebase_admin.initialize_app(credentials.Certificate(cred_path))
        else:
            # gcloud Application Default Credentials (project required for Firestore)
            opts = {"projectId": project_id} if project_id else None
            _firebase_app = firebase_admin.initialize_app(options=opts)
        logger.info("Firebase Admin initialized")
    except Exception as e:
        logger.warning("Firebase Admin init skipped or failed: %s", e)
        _firebase_app = False  # mark as attempted


def is_initialized() -> bool:
    return _firebase_app is not None and _firebase_app is not False


def verify_id_token(token: str) -> Optional[dict[str, Any]]:
    """
    Verify Firebase ID token and return decoded claims (uid, email, etc.).
    Returns None if Firebase not initialized, token invalid, or expired.
    """
    if not is_initialized():
        return None
    try:
        from firebase_admin import auth
        return auth.verify_id_token(token)
    except Exception as e:
        logger.debug("Firebase token verification failed: %s", e)
        return None
