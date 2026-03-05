"""
Firebase Auth + Firestore for user/org context.
Exposes init_firebase, verify_id_token, get_organization_id, get_role_from_token.
"""
from __future__ import annotations

from .firebase import init_firebase, verify_id_token
from .firestore_user import (
    get_bq_config_for_client,
    get_org_bq_context,
    get_organization,
    get_org_projects_flat,
    get_user,
    parse_org_projects,
)
from .request_auth import get_organization_id, get_role_from_token, get_user_id, require_any_auth

__all__ = [
    "get_bq_config_for_client",
    "get_org_bq_context",
    "get_organization",
    "get_org_projects_flat",
    "get_user",
    "init_firebase",
    "parse_org_projects",
    "get_organization_id",
    "get_role_from_token",
    "get_user_id",
    "require_any_auth",
    "verify_id_token",
]
