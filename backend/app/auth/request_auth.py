"""
Resolve organization_id and role from request: Firebase token + Firestore when Bearer present, else headers/API key.
Require auth (Bearer or API key) for all protected routes including local.
"""
from __future__ import annotations

from typing import Optional, Tuple

from starlette.requests import Request

from .firebase import verify_id_token
from .firestore_user import get_user

# Lazy import to avoid circular dependency
def _get_api_key():
    from ..config import get_api_key
    return get_api_key()


def require_any_auth(request: Request) -> None:
    """
    Raise 401 if request has no valid auth (Bearer token or X-API-Key).
    Use as Depends(require_any_auth) on routes that must require auth for local and prod.
    """
    if (request.headers.get("Authorization") or "").strip().startswith("Bearer "):
        return
    api_key = _get_api_key()
    if api_key and request.headers.get("X-API-Key") == api_key:
        return
    from fastapi import HTTPException
    raise HTTPException(
        401,
        detail={"code": "UNAUTHORIZED", "message": "Authentication required. Use Bearer token (Firebase) or X-API-Key."},
    )


def _get_firebase_context(request: Request) -> Tuple[Optional[str], Optional[dict]]:
    """
    Verify Bearer token and load user from Firestore; cache on request.state.
    Returns (uid, user_doc) or (None, None).
    """
    if hasattr(request.state, "_firebase_user"):
        return getattr(request.state, "_firebase_uid", None), getattr(request.state, "_firebase_user", None)

    request.state._firebase_uid = None
    request.state._firebase_user = None

    auth = request.headers.get("Authorization")
    if not auth or not auth.startswith("Bearer "):
        return None, None

    token = auth[7:].strip()
    if not token:
        return None, None

    decoded = verify_id_token(token)
    if not decoded:
        return None, None

    uid = decoded.get("uid") or decoded.get("user_id") or decoded.get("sub")
    if not uid:
        import logging
        logging.getLogger(__name__).debug("Firebase token decoded but no uid/user_id; keys=%s", list(decoded.keys()))
        return None, None

    user = get_user(uid)
    request.state._firebase_uid = uid
    request.state._firebase_user = user
    return uid, user


def get_organization_id(request: Request) -> str:
    """
    When Bearer token is present and valid, return organization_id from Firestore user doc.
    Otherwise return X-Organization-Id or X-Org-Id header, or "default".
    """
    _, user = _get_firebase_context(request)
    if user and isinstance(user.get("organization_id"), str):
        return user["organization_id"]
    return (
        request.headers.get("X-Organization-Id")
        or request.headers.get("X-Org-Id")
        or "default"
    )


def get_user_id(request: Request) -> Optional[str]:
    """
    When Bearer token is present and valid, return Firebase uid from the token.
    Used to scope Copilot sessions per user so the same user sees their chats when they log in again.
    """
    uid, _ = _get_firebase_context(request)
    return uid


def get_role_from_token(request: Request, get_api_key_fn=None) -> str:
    """
    When Bearer token is present and valid, return role from Firestore user doc (or "analyst" if missing).
    Otherwise: X-API-Key match -> "admin", Bearer present -> "analyst", else "viewer".
    """
    if get_api_key_fn:
        api_key = get_api_key_fn()
        if api_key and request.headers.get("X-API-Key") == api_key:
            return "admin"

    uid, user = _get_firebase_context(request)
    if uid and user:
        role = (user.get("role") or "analyst").strip().lower()
        if role in ("admin", "analyst", "viewer"):
            return role
        return "analyst"

    auth = request.headers.get("Authorization")
    if auth and auth.startswith("Bearer "):
        return "analyst"
    return "viewer"
